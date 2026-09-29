"""Create repeatable synthetic charging events and publish one immutable batch.

This producer only writes files for the existing Auto Loader landing job. It
creates no bills and does not run Silver/Gold. Consumers must run after this
job succeeds; dbutils.fs does not establish an atomic handoff to a reader.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import random
import re

GENERATOR_VERSION = "sessions-v1"
MAX_SESSIONS = 1000
MAX_BATCH_BYTES = 4 * 1024 * 1024
BASE_TIME = datetime(2026, 9, 1, tzinfo=timezone.utc)
_BATCH_ID = re.compile(r"[a-z][a-z0-9_-]{0,31}")
_INPUT_PATH = re.compile(
    r"/Volumes/[A-Za-z_][A-Za-z0-9_-]*/chargeassert_dev_bronze/ocpp_ingestion/incoming"
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _timestamp(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _validate_parameters(batch_id, session_count, seed):
    if not isinstance(batch_id, str) or not _BATCH_ID.fullmatch(batch_id):
        raise ValueError("batch_id must start with a lowercase letter and contain 1-32 lowercase letters, digits, '-' or '_'.")
    if type(session_count) is not int or not 1 <= session_count <= MAX_SESSIONS:
        raise ValueError(f"session_count must be an integer from 1 to {MAX_SESSIONS}.")
    if type(seed) is not int or not 0 <= seed <= 4294967295:
        raise ValueError("seed must be an integer from 0 to 4294967295.")


def generate_batch(batch_id, session_count, seed):
    """Return JSONL with three complete events per session and stable LF bytes.

    A local seeded RNG and fixed UTC clock make retries independent of wall
    time and global random state. Batch identity namespaces event/session IDs.
    Retained settings bind parameters even if different seeds produce the same
    random values. The tariff metadata is input for a future billing adapter.
    """
    _validate_parameters(batch_id, session_count, seed)
    rng = random.Random(seed)
    metadata = {
        "version": GENERATOR_VERSION,
        "batch_id": batch_id,
        "session_count": session_count,
        "seed": seed,
        "tariff": {"id": "generated-energy-v1", "currency": "EUR", "price_per_kwh": "0.45"},
    }
    rows = []
    
    # 1. Generate the Tariff record
    tariff_payload = {
        "id": "generated-energy-v1",
        "currency": "EUR",
        "elements": [{"price_components": [{"type": "ENERGY", "price": 0.45, "step_size": 1}]}]
    }
    rows.append(_json({
        "schema_version": 1,
        "run_id": f"generated-v1-{batch_id}",
        "tariff_id": "generated-energy-v1",
        "payload": _json(tariff_payload),
        "generator": metadata,
    }))

    for index in range(session_count):
        identity = hashlib.sha256(f"{GENERATOR_VERSION}:{batch_id}:{index}".encode()).hexdigest()[:24]
        started = BASE_TIME + timedelta(minutes=rng.randint(0, 7 * 24 * 60 - 1))
        duration_minutes = rng.randint(20, 180)
        duration_seconds = duration_minutes * 60
        power_watts = rng.choice((3600, 7200, 11000, 22000))
        energy_wh = power_watts * duration_minutes // 60
        meter_start = rng.randint(10000, 1000000)
        event_specs = (
            ("Started", "Authorized", "Transaction.Begin", 0, meter_start),
            ("Updated", "MeterValuePeriodic", "Sample.Periodic", duration_seconds // 2, meter_start + energy_wh // 2),
            ("Ended", "StopAuthorized", "Transaction.End", duration_seconds, meter_start + energy_wh),
        )
        for sequence, (event_type, trigger, context, elapsed, meter) in enumerate(event_specs):
            timestamp = _timestamp(started + timedelta(seconds=elapsed))
            transaction = {
                "transactionId": f"txn-{identity}",
                "chargingState": "Idle" if event_type == "Ended" else "Charging",
            }
            if elapsed:
                transaction["timeSpentCharging"] = elapsed
            if event_type == "Ended":
                transaction["stoppedReason"] = "Local"
            payload = {
                "eventType": event_type,
                "timestamp": timestamp,
                "triggerReason": trigger,
                "seqNo": sequence,
                "transactionInfo": transaction,
                "meterValue": [{
                    "timestamp": timestamp,
                    "sampledValue": [{
                        "value": meter,
                        "context": context,
                        "measurand": "Energy.Active.Import.Register",
                        "location": "Outlet",
                        "unitOfMeasure": {"unit": "Wh", "multiplier": 0},
                    }],
                }],
                "evse": {"id": 1, "connectorId": 1},
            }
            if event_type == "Started":
                payload["idToken"] = {"idToken": f"TEST-{identity}", "type": "ISO14443"}
            rows.append(_json({
                "schema_version": 1,
                "run_id": f"generated-v1-{batch_id}",
                "event_id": f"msg-{identity}-{event_type.lower()}",
                "charging_station_id": f"cs-{identity}",
                "payload": _json(payload),
                "generator": metadata,
            }))

        # 2. Generate a perfectly matching fake CDR (Actual Ledger) for the Gold Layer
        cdr_amount = round((energy_wh / 1000.0) * 0.45, 2)
        cdr_payload = {
            "country_code": "FR",
            "party_id": "CAS",
            "id": f"cdr-{identity}",
            "session_id": f"txn-{identity}",
            "currency": "EUR",
            "credit": False,
            "total_cost": {"excl_vat": cdr_amount}
        }
        rows.append(_json({
            "schema_version": 1,
            "run_id": f"generated-v1-{batch_id}",
            "cdr_id": f"cdr-{identity}",
            "release_role": "candidate",
            "payload": _json(cdr_payload),
            "generator": metadata,
        }))

    content = "\n".join(rows) + "\n"
    if len(content.encode("utf-8")) > MAX_BATCH_BYTES:
        raise ValueError("Generated batch exceeds the 4 MiB publisher limit.")
    return content


def _find_file(fs, directory, filename):
    # Listing/permission errors must propagate rather than imply an absent file.
    matches = [item for item in fs.ls(directory) if item.name.rstrip("/") == filename]
    if len(matches) > 1:
        raise ValueError(f"Ambiguous generated batch file: {filename}")
    return matches[0] if matches else None


def _verify_file(fs, path, item, expected):
    if item is None or item.name.endswith("/") or item.size != len(expected):
        raise ValueError(f"Batch file is incomplete or differs; refusing to overwrite: {path}")
    if fs.head(path, len(expected) + 1).encode("utf-8") != expected:
        raise ValueError(f"Batch file differs; refusing to overwrite: {path}. Use a new batch_id for changed settings.")


def publish_batch(fs, input_path, batch_id, session_count, seed):
    """Publish once and leave byte-identical retries unchanged.

    fs is dbutils.fs. Never overwrite/delete a file or reset a checkpoint.
    Truncated/conflicting files fail for investigation. Do not start ingestion
    after a failed publication or concurrently with this generator.
    """
    if not isinstance(input_path, str) or not _INPUT_PATH.fullmatch(input_path):
        raise ValueError("input_path must be /Volumes/<catalog>/chargeassert_dev_bronze/ocpp_ingestion/incoming.")
    # Finish validating and building the bounded batch before filesystem writes.
    content = generate_batch(batch_id, session_count, seed)
    expected = content.encode("utf-8")
    filename = f"generated-{batch_id}.jsonl"
    path = f"{input_path}/{filename}"
    if fs.mkdirs(input_path) is not True:
        raise RuntimeError(f"Could not create incoming directory: {input_path}")
    existing = _find_file(fs, input_path, filename)
    status = "unchanged"
    if existing is None:
        if fs.put(path, content, overwrite=False) is not True:
            raise RuntimeError(f"Generated batch write did not succeed: {path}")
        status = "published"
        existing = _find_file(fs, input_path, filename)
    _verify_file(fs, path, existing, expected)
    return {
        "status": status,
        "path": path,
        "run_id": f"generated-v1-{batch_id}",
        "sessions": session_count,
        "records": session_count * 3,
        "file_sha256": hashlib.sha256(expected).hexdigest(),
    }
