"""One-shot, deterministic mock billing from Bronze event and tariff evidence.

This intentionally supports only the current flat-EUR smoke scenario. It does
not read expected/actual ledgers or existing CDRs. The bad candidate contains an
explicit EUR 0.87 surcharge bug; the corrected candidate uses healthy billing.
The manifest's SHA fields contain code/variant fingerprints, NOT Git commit IDs:
SHA-1 of this module's UTF-8 bytes with CRLF normalized to LF plus a variant tag.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import hashlib
import json
from pathlib import Path


SOURCE_RUN_ID = "smoke-run-v1"
MOCK_RUN_IDS = ("mock-amount-bad-v1", "mock-amount-fixed-v1")
_CENT = Decimal("0.01")
_MICRO = Decimal("0.000001")
_MAX_DECIMAL = Decimal("999999999999.999999")


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _payload(row):
    body = row["payload"]
    _require(isinstance(body, str), "Payload must be JSON text")
    _require(row["payload_hash"] == _sha256(body), "Payload hash mismatch")
    value = json.loads(body, parse_float=Decimal, object_pairs_hook=_unique_object)
    _require(isinstance(value, dict), "Payload must be a JSON object")
    return value


def _utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _require(isinstance(value, datetime), "Expected a timestamp")
    # Spark collects TIMESTAMP columns as naive datetimes. The notebook wrapper
    # sets the Spark session timezone to UTC before reading these Bronze rows.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp(value):
    return value.isoformat().replace("+00:00", "Z")


def _json(value):
    """Serialize Decimal as plain JSON numbers without a binary-float round trip."""
    if isinstance(value, Decimal):
        _require(value.is_finite(), "JSON number must be finite")
        return format(value, "f")
    if isinstance(value, dict):
        return "{" + ",".join(
            json.dumps(key) + ":" + _json(value[key]) for key in sorted(value)
        ) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_json(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _validated_inputs(events, tariff, manifest):
    _require(manifest["run_id"] == SOURCE_RUN_ID, "Expected the smoke source run")
    _require(type(manifest["seed"]) is int and manifest["seed"] >= 0, "Invalid seed")
    _utc(manifest["created_at"])
    _require(len(events) == 3, "Require exactly three smoke transaction events")
    _require(len({row["event_id"] for row in events}) == 3, "Duplicate event ID")
    _require(all(isinstance(row["event_id"], str) and row["event_id"] for row in events),
             "Invalid event ID")
    stations = {row["charging_station_id"] for row in events}
    _require(len(stations) == 1 and all(isinstance(item, str) and item for item in stations),
             "Events must belong to one charging station")
    ordered = sorted(events, key=lambda row: row["sequence_number"])
    parsed = []
    for sequence, (row, event_type) in enumerate(zip(ordered, ("Started", "Updated", "Ended"))):
        body = _payload(row)
        _require(row["run_id"] == SOURCE_RUN_ID, "Event belongs to another run")
        _require(type(row["sequence_number"]) is int and row["sequence_number"] == sequence
                 and type(body["seqNo"]) is int and body["seqNo"] == sequence,
                 "Require unique consecutive event sequence numbers 0, 1, 2")
        _require(row["event_type"] == body["eventType"] == event_type,
                 "Require Started, Updated, Ended events in sequence")
        _require(row["transaction_id"] == body["transactionInfo"]["transactionId"] == "txn-smoke-v1",
                 "Unsupported session or inconsistent transaction metadata")
        event_time = _utc(body["timestamp"])
        _require(_utc(row["event_time"]) == event_time, "Event timestamp metadata mismatch")
        _utc(row["ingest_time"])
        _require(len(body["meterValue"]) == 1, "Require one meter sample per event")
        meter = body["meterValue"][0]
        _require(_utc(meter["timestamp"]) == event_time, "Meter timestamp mismatch")
        _require(len(meter["sampledValue"]) == 1, "Require one meter register per event")
        sample = meter["sampledValue"][0]
        unit = sample["unitOfMeasure"]
        _require(sample["measurand"] == "Energy.Active.Import.Register"
                 and sample["location"] == "Outlet" and unit["unit"] == "Wh"
                 and type(unit["multiplier"]) is int and unit["multiplier"] == 0,
                 "Only the cumulative Outlet energy register in whole Wh is supported")
        _require(type(sample["value"]) is int and 0 <= sample["value"] <= 9223372036854775807,
                 "Meter value must be a nonnegative BIGINT in Wh")
        parsed.append((event_time, sample["value"]))
    _require(parsed[0][0] < parsed[1][0] < parsed[2][0], "Event times must increase")
    _require(parsed[0][1] <= parsed[1][1] <= parsed[2][1], "Meter reset/decrease is unsupported")

    body = _payload(tariff)
    _require(tariff["run_id"] == SOURCE_RUN_ID, "Tariff belongs to another run")
    _require(tariff["tariff_id"] == body["id"] == "tariff-smoke-v1", "Unsupported tariff ID")
    _require(tariff["currency"] == body["currency"] == "EUR", "Only EUR is supported")
    _require(set(body) == {"id", "currency", "elements"}, "Unsupported tariff fields")
    _require(len(body["elements"]) == 1, "Require one flat tariff element")
    element = body["elements"][0]
    _require(set(element) == {"price_components"} and len(element["price_components"]) == 1,
             "Require one unrestricted price component")
    component = element["price_components"][0]
    _require(set(component) == {"type", "price", "step_size"}
             and component["type"] == "ENERGY"
             and type(component["step_size"]) is int and component["step_size"] == 1,
             "Only ENERGY pricing with step_size 1 and no VAT is supported")
    _require(type(component["price"]) in (int, Decimal), "Price must be a JSON number")
    price = Decimal(component["price"])
    _require(price.is_finite() and 0 <= price <= _MAX_DECIMAL
             and price == price.quantize(_MICRO), "Price must fit nonnegative DECIMAL(18,6)")
    valid_from = _utc(tariff["valid_from"])
    valid_to = _utc(tariff["valid_to"]) if tariff["valid_to"] is not None else None
    _require(valid_from <= parsed[0][0] and (valid_to is None or parsed[-1][0] <= valid_to),
             "Tariff must cover the complete session")
    return ordered, parsed, body, price


def _healthy_amount(energy, price):
    return (energy * price).quantize(_CENT, rounding=ROUND_HALF_UP)


def _faulty_amount(energy, price):
    # Deliberate executable candidate defect: an extra EUR 0.87 per session.
    return _healthy_amount(energy, price) + Decimal("0.87")


def _cdr(parsed, tariff_body, price, faulty):
    start, start_wh = parsed[0]
    end, end_wh = parsed[-1]
    energy = Decimal(end_wh - start_wh) / Decimal(1000)
    elapsed = end - start
    microseconds = (elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds
    duration = (Decimal(microseconds) / Decimal(3600000000)).quantize(_MICRO, rounding=ROUND_HALF_UP)
    amount = (_faulty_amount if faulty else _healthy_amount)(energy, price)
    _require(all(0 <= value <= _MAX_DECIMAL for value in (energy, duration, amount)),
             "Generated billing values exceed DECIMAL(18,6)")
    body = {
        "country_code": "FR", "party_id": "CAS", "id": "cdr-mock-v1",
        "session_id": "txn-smoke-v1", "start_date_time": _timestamp(start),
        "end_date_time": _timestamp(end), "currency": "EUR", "tariffs": [tariff_body],
        "charging_periods": [{"start_date_time": _timestamp(start), "dimensions": [
            {"type": "ENERGY", "volume": energy}, {"type": "TIME", "volume": duration}
        ], "tariff_id": "tariff-smoke-v1"}],
        "total_cost": {"excl_vat": amount}, "total_energy": energy, "total_time": duration,
        "credit": False, "last_updated": _timestamp(end + timedelta(seconds=1)),
    }
    payload = _json(body)
    return {
        "country_code": "FR", "party_id": "CAS", "cdr_id": "cdr-mock-v1",
        "session_id": "txn-smoke-v1", "cdr_type": "FINAL", "currency": "EUR",
        "total_cost": amount, "payload": payload, "payload_hash": _sha256(payload),
        "ingest_time": (end + timedelta(seconds=2)).replace(tzinfo=None),
    }


def generate_mock_runs(events, tariff, manifest):
    """Return rows for four Bronze tables; perform no database writes or scheduling.

    Inputs are dictionaries collected from the smoke Bronze tables. Both runs
    receive byte-identical event/tariff inputs. Each release executes the billing
    function, and only the bad run's candidate enables the deliberate surcharge.
    Callers must preserve immutable prior evidence and reject conflicting reruns.
    """
    try:
        with localcontext() as context:
            context.prec = 60
            ordered, parsed, tariff_body, price = _validated_inputs(events, tariff, manifest)
            source_bytes = Path(__file__).read_bytes().replace(b"\r\n", b"\n")

            def fingerprint(variant):
                return hashlib.sha1(source_bytes + b"\0" + variant.encode("ascii")).hexdigest()

            output = {name: [] for name in (
                "run_manifest", "ocpp_transaction_events_raw", "tariffs_raw", "ocpi_cdrs_raw"
            )}
            for run_id in MOCK_RUN_IDS:
                faulty_candidate = run_id == MOCK_RUN_IDS[0]
                output["run_manifest"].append({
                    "run_id": run_id, "scenario_id": "mock-amount-mismatch-v1", "seed": manifest["seed"],
                    "baseline_sha": fingerprint("healthy-v1"),
                    "candidate_sha": fingerprint("surcharge-bug-v1" if faulty_candidate else "healthy-v1"),
                    "tariff_hash": _sha256(tariff["payload"]), "created_at": manifest["created_at"],
                })
                output["ocpp_transaction_events_raw"].extend(dict(row, run_id=run_id) for row in ordered)
                output["tariffs_raw"].append(dict(tariff, run_id=run_id))
                for role in ("baseline", "candidate"):
                    output["ocpi_cdrs_raw"].append(dict(
                        _cdr(parsed, tariff_body, price, faulty_candidate and role == "candidate"),
                        run_id=run_id, release_role=role,
                    ))
            return output
    except (KeyError, IndexError, TypeError, InvalidOperation) as error:
        raise ValueError(f"Malformed or unsupported mock billing input: {error}") from error
