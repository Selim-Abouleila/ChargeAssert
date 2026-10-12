"""Validate immutable sessions-v1 landing evidence and execute bounded mock billing.

Only event-only files from the original producer are supported. The independent
SQL oracle never supplies these bills. SHA fields fingerprint this complete
module's normalized UTF-8 source plus its executable mock variant; they are not
Git commit IDs. Arrival metadata stays in landing and is not billing identity.
This module performs no database or filesystem writes.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import hashlib
import json
from pathlib import Path
import re


MAX_SESSIONS = 1000
MAX_LANDED_ROWS = 10000
_BATCH_ID = re.compile(r"[a-z][a-z0-9_-]{0,31}")
_HASH = re.compile(r"[0-9a-f]{64}")
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})")
_EVENT_FIELDS = {"schema_version", "run_id", "event_id", "charging_station_id", "payload", "generator"}
_EVENT_TYPES = ("Started", "Updated", "Ended")
_CENT = Decimal("0.01")
_MICRO = Decimal("0.000001")
_MAX_DECIMAL = Decimal("999999999999.999999")


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _sha256(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError(f"Non-finite JSON number: {value}")


def _object(value, label):
    _require(isinstance(value, str), f"{label} must be exact JSON text.")
    parsed = json.loads(value, parse_float=Decimal, parse_constant=_invalid_constant,
                        object_pairs_hook=_unique_object)
    _require(isinstance(parsed, dict), f"{label} must be a JSON object.")
    return parsed


def _json(value):
    """Canonical JSON retaining plain exact decimal numbers without floats."""
    if isinstance(value, Decimal):
        _require(value.is_finite(), "JSON decimal must be finite.")
        return format(value, "f")
    if isinstance(value, dict):
        return "{" + ",".join(json.dumps(key) + ":" + _json(value[key])
                              for key in sorted(value)) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_json(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"))


def _utc(value):
    _require(isinstance(value, str) and _TIMESTAMP.fullmatch(value) is not None,
             "Event timestamps must be explicit RFC3339 timestamps with at most six fractional digits.")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _arrival(value):
    if isinstance(value, str):
        value = _utc(value)
    _require(isinstance(value, datetime), "Landing ingested_at must be a valid timestamp.")
    # Spark collects TIMESTAMP as a naive datetime under the caller's UTC session.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp(value):
    return value.isoformat().replace("+00:00", "Z")


def _naive(value):
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _metadata(value, batch_id):
    _require(isinstance(value, dict) and set(value) == {
        "version", "batch_id", "session_count", "seed", "tariff"
    }, "Require the complete sessions-v1 generator metadata.")
    _require(value["version"] == "sessions-v1" and value["batch_id"] == batch_id,
             "Generator version or batch_id differs from the selected batch.")
    _require(type(value["session_count"]) is int and 1 <= value["session_count"] <= MAX_SESSIONS,
             f"Declared session_count must be an integer from 1 to {MAX_SESSIONS}.")
    _require(type(value["seed"]) is int and 0 <= value["seed"] <= 4294967295,
             "Generator seed must be an unsigned 32-bit integer.")
    _require(value["tariff"] == {
        "id": "generated-energy-v1", "currency": "EUR", "price_per_kwh": "0.45"
    }, "sessions-v1 requires its declared flat EUR 0.45/kWh tariff.")
    return value


def _event(envelope, identities):
    body = _object(envelope["payload"], "OCPP payload")
    sequence = body["seqNo"]
    _require(type(sequence) is int and 0 <= sequence <= 2, "Require event sequence numbers 0, 1, 2.")
    event_type = _EVENT_TYPES[sequence]
    _require(body["eventType"] == event_type, "Event type does not match its sequence number.")
    transaction = body["transactionInfo"]
    session_id = transaction["transactionId"]
    _require(isinstance(session_id, str) and session_id in identities,
             "Session identity is outside the full declared generator inventory.")
    identity = identities[session_id]
    _require(envelope["event_id"] == f"msg-{identity}-{event_type.lower()}"
             and envelope["charging_station_id"] == f"cs-{identity}",
             "Event or station identity does not match the generated session.")
    event_time = _utc(body["timestamp"])
    _require(isinstance(body["meterValue"], list) and len(body["meterValue"]) == 1,
             "Require one meter sample per event.")
    meter = body["meterValue"][0]
    _require(_utc(meter["timestamp"]) == event_time, "Meter timestamp differs from its event.")
    _require(isinstance(meter["sampledValue"], list) and len(meter["sampledValue"]) == 1,
             "Require one meter register per event.")
    sample = meter["sampledValue"][0]
    unit = sample["unitOfMeasure"]
    _require(sample["measurand"] == "Energy.Active.Import.Register"
             and sample["location"] == "Outlet" and unit["unit"] == "Wh"
             and type(unit["multiplier"]) is int and unit["multiplier"] == 0,
             "Require the cumulative Outlet energy register in whole Wh with multiplier 0.")
    _require(type(sample["value"]) is int and 0 <= sample["value"] <= 9223372036854775807,
             "Meter readings must be nonnegative BIGINT values in whole Wh.")
    if sequence:
        _require(type(transaction["timeSpentCharging"]) is int
                 and transaction["timeSpentCharging"] > 0,
                 "Updated and Ended events require integer timeSpentCharging.")
    return {
        "envelope": envelope, "body": body, "session_id": session_id,
        "sequence": sequence, "time": event_time, "meter_wh": sample["value"],
    }


def _validated(rows, batch_id):
    _require(isinstance(rows, (list, tuple)) and 1 <= len(rows) <= MAX_LANDED_ROWS,
             f"Require between 1 and {MAX_LANDED_ROWS} landed rows for the selected batch.")
    metadata, identities, unique = None, None, {}
    for row in rows:
        _require(isinstance(row, dict), "Landing rows must be dictionaries.")
        raw = row["raw_record"]
        _require(isinstance(raw, str) and row["record_hash"] == _sha256(raw),
                 "Landing whole-line record_hash mismatch; preserve and inspect the original evidence.")
        envelope = _object(raw, "Landing record")
        _require(not ({"tariff_id", "cdr_id", "release_role"} & set(envelope)),
                 "Mixed tariff/CDR records are unsupported sessions-v1 evidence. Preserve this file and use a fresh batch_id with the event-only generator.")
        _require(set(envelope) == _EVENT_FIELDS, "Require an event-only sessions-v1 envelope.")
        _require(type(envelope["schema_version"]) is int and envelope["schema_version"] == 1,
                 "Only integer schema_version 1 is supported.")
        _require(envelope["run_id"] == f"generated-v1-{batch_id}",
                 "Landing run_id differs from the selected generated batch.")
        settings = _metadata(envelope["generator"], batch_id)
        if metadata is None:
            metadata = settings
            identities = {}
            for index in range(metadata["session_count"]):
                identity = _sha256(f"sessions-v1:{batch_id}:{index}")[:24]
                identities[f"txn-{identity}"] = identity
        _require(settings == metadata, "Conflicting generator metadata within one batch.")
        event = _event(envelope, identities)
        event["ingested_at"] = _arrival(row["ingested_at"])
        key = envelope["event_id"]
        if key in unique:
            _require(unique[key]["envelope"] == envelope,
                     f"Conflicting event evidence for {key}; existing evidence is immutable.")
            unique[key]["ingested_at"] = min(unique[key]["ingested_at"], event["ingested_at"])
        else:
            unique[key] = event
    _require(len(unique) == metadata["session_count"] * 3,
             "Incomplete declared session inventory: require three unique events for every declared session.")
    sessions = {session_id: [] for session_id in identities}
    for event in unique.values():
        sessions[event["session_id"]].append(event)
    for session_id, events in sessions.items():
        events.sort(key=lambda item: item["sequence"])
        _require([event["sequence"] for event in events] == [0, 1, 2],
                 f"Incomplete or duplicated event sequence for {session_id}.")
        first, middle, last = events
        _require(first["time"] < middle["time"] < last["time"], "Event timestamps must increase.")
        _require(first["meter_wh"] <= middle["meter_wh"] <= last["meter_wh"],
                 "Meter reset/decrease is unsupported.")
        for event in events[1:]:
            elapsed = event["time"] - first["time"]
            _require(elapsed == timedelta(seconds=event["body"]["transactionInfo"]["timeSpentCharging"]),
                     "timeSpentCharging differs from the event timestamps.")
    return metadata, unique, sessions


def _bill(events, tariff_body, role, mode, run_id):
    """Execute one mock release using raw validated readings and tariff data."""
    first, _, last = events
    start, end = first["time"], last["time"]
    energy = Decimal(last["meter_wh"] - first["meter_wh"]) / Decimal(1000)
    elapsed = end - start
    microseconds = (elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds
    duration = (Decimal(microseconds) / Decimal(3600000000)).quantize(_MICRO, rounding=ROUND_HALF_UP)
    price = tariff_body["elements"][0]["price_components"][0]["price"]
    amount = (energy * price).quantize(_CENT, rounding=ROUND_HALF_UP)
    if role == "candidate" and mode == "amount_error":
        amount += Decimal("0.87")
    _require(all(0 <= value <= _MAX_DECIMAL for value in (energy, duration, amount)),
             "Generated billing values exceed DECIMAL(18,6).")
    session_id = first["session_id"]
    cdr_id = "cdr-" + session_id.removeprefix("txn-")
    payload = _json({
        "country_code": "FR", "party_id": "CAS", "id": cdr_id,
        "session_id": session_id, "start_date_time": _timestamp(start),
        "end_date_time": _timestamp(end), "currency": tariff_body["currency"],
        "tariffs": [tariff_body],
        "charging_periods": [{"start_date_time": _timestamp(start), "dimensions": [
            {"type": "ENERGY", "volume": energy}, {"type": "TIME", "volume": duration}
        ], "tariff_id": tariff_body["id"]}],
        "total_cost": {"excl_vat": amount}, "total_energy": energy, "total_time": duration,
        "credit": False, "last_updated": _timestamp(end + timedelta(seconds=1)),
    })
    return {
        "run_id": run_id, "release_role": role, "country_code": "FR", "party_id": "CAS",
        "cdr_id": cdr_id, "session_id": session_id, "cdr_type": "FINAL", "currency": "EUR",
        "total_cost": amount, "payload": payload, "payload_hash": _sha256(payload),
        "ingest_time": _naive(end + timedelta(seconds=2)),
    }


def prepare_batch(rows, batch_id, candidate_mode, pipeline_hash):
    """Return immutable Bronze rows and a complete input-session inventory.

    rows is the bounded selected landing list, each dictionary containing exact
    raw_record, record_hash and ingested_at. Arrival metadata is excluded from
    business identity. Equivalent duplicates retain the earliest supplied arrival
    time per event; adding an earlier copy changes only that ingest_time. Exact
    OCPP payload bytes are preserved. Mock CDR ingest timestamps are end + 2s.
    The caller must include this module in the supplied full pipeline hash and
    persist returned rows with conflict checking, never overwrite prior evidence.
    """
    _require(isinstance(batch_id, str) and _BATCH_ID.fullmatch(batch_id) is not None,
             "batch_id must contain 1-32 lowercase letters, digits, '-' or '_' and start with a letter.")
    _require(candidate_mode in ("healthy", "amount_error"), "candidate_mode must be healthy or amount_error.")
    _require(isinstance(pipeline_hash, str) and _HASH.fullmatch(pipeline_hash) is not None,
             "pipeline_hash must contain 64 lowercase hexadecimal characters.")
    try:
        with localcontext() as context:
            context.prec = 60
            settings, unique, sessions = _validated(rows, batch_id)
            input_hash = _sha256(_json([unique[key]["envelope"] for key in sorted(unique)]))
            suffix = _sha256(input_hash + pipeline_hash)[:16]
            run_id = f"generated-check-v1-{batch_id}-{candidate_mode}-{suffix}"
            earliest = min(event["time"] for event in unique.values())
            tariff_body = {
                "id": settings["tariff"]["id"], "currency": settings["tariff"]["currency"],
                "elements": [{"price_components": [{
                    "type": "ENERGY", "price": Decimal(settings["tariff"]["price_per_kwh"]), "step_size": 1
                }]}],
            }
            tariff_payload = _json(tariff_body)
            source = Path(__file__).read_bytes().replace(b"\r\n", b"\n")

            def fingerprint(variant):
                return hashlib.sha1(source + b"\0" + variant.encode("ascii")).hexdigest()

            tables = {
                "run_manifest": [{
                    "run_id": run_id, "scenario_id": f"generated-mock-v1-{candidate_mode}-{batch_id}",
                    "seed": settings["seed"], "baseline_sha": fingerprint("healthy-v1"),
                    "candidate_sha": fingerprint("amount-error-v1" if candidate_mode == "amount_error" else "healthy-v1"),
                    "tariff_hash": _sha256(tariff_payload), "created_at": _naive(earliest),
                }],
                "input_session": [], "ocpp_transaction_events_raw": [],
                "tariffs_raw": [{
                    "run_id": run_id, "tariff_id": tariff_body["id"], "valid_from": _naive(earliest),
                    "valid_to": None, "currency": "EUR", "payload": tariff_payload,
                    "payload_hash": _sha256(tariff_payload),
                }],
                "ocpi_cdrs_raw": [],
            }
            for session_id in sorted(sessions):
                events = sessions[session_id]
                tables["input_session"].append({
                    "run_id": run_id, "session_id": session_id, "tariff_id": tariff_body["id"],
                })
                for event in events:
                    envelope = event["envelope"]
                    tables["ocpp_transaction_events_raw"].append({
                        "run_id": run_id, "event_id": envelope["event_id"],
                        "charging_station_id": envelope["charging_station_id"], "transaction_id": session_id,
                        "event_type": event["body"]["eventType"], "sequence_number": event["sequence"],
                        "event_time": _naive(event["time"]),
                        "ingest_time": _naive(event["ingested_at"]),
                        "payload": envelope["payload"], "payload_hash": _sha256(envelope["payload"]),
                    })
                for role in ("baseline", "candidate"):
                    tables["ocpi_cdrs_raw"].append(_bill(events, tariff_body, role, candidate_mode, run_id))
            return {
                "run_id": run_id, "session_ids": sorted(sessions), "input_hash": input_hash,
                "metadata": dict(settings, billing_kind="mock", candidate_mode=candidate_mode,
                                 pipeline_hash=pipeline_hash, expected_assertions=14 * settings["session_count"]),
                "tables": tables,
            }
    except (KeyError, IndexError, TypeError, AttributeError, InvalidOperation, OverflowError) as error:
        raise ValueError(f"Malformed or unsupported generated batch: {error}") from error
