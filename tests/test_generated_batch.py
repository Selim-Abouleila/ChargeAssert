"""Validate generated input evidence and executable mock billing without Spark."""

import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from notebooks import generated_batch as adapter
from notebooks.session_generator import generate_batch


PIPELINE_HASH = "a" * 64
ARRIVED = datetime(2026, 9, 30, 12, 0, 0)


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def landing(batch_id="audit", count=2, seed=42):
    return [{
        "raw_record": line, "record_hash": sha256(line), "ingested_at": ARRIVED,
        "source_file_name": f"generated-{batch_id}.jsonl",
        "source_file_path": f"/incoming/generated-{batch_id}.jsonl",
    } for line in generate_batch(batch_id, count, seed).splitlines()]


def rewrite(row, edit, *, payload=False):
    envelope = json.loads(row["raw_record"])
    if payload:
        body = json.loads(envelope["payload"])
        edit(body)
        envelope["payload"] = json.dumps(body, separators=(",", ":"))
    else:
        edit(envelope)
    row["raw_record"] = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    row["record_hash"] = sha256(row["raw_record"])


class GeneratedBatchTests(unittest.TestCase):
    def setUp(self):
        self.rows = landing()

    def prepare(self, mode="healthy", rows=None, pipeline_hash=PIPELINE_HASH):
        return adapter.prepare_batch(self.rows if rows is None else rows, "audit", mode, pipeline_hash)

    def test_two_sessions_have_complete_bronze_and_28_assertion_inventory(self):
        result = self.prepare()
        tables = result["tables"]
        self.assertEqual({key: len(value) for key, value in tables.items()}, {
            "run_manifest": 1, "input_session": 2, "ocpp_transaction_events_raw": 6,
            "tariffs_raw": 1, "ocpi_cdrs_raw": 4,
        })
        self.assertEqual(result["metadata"]["expected_assertions"], 28)
        self.assertEqual(result["metadata"]["billing_kind"], "mock")
        self.assertEqual(result["session_ids"], sorted(row["session_id"] for row in tables["input_session"]))
        for rows in tables.values():
            self.assertEqual({row["run_id"] for row in rows}, {result["run_id"]})
        self.assertEqual(set(tables["run_manifest"][0]), {
            "run_id", "scenario_id", "seed", "baseline_sha", "candidate_sha", "tariff_hash", "created_at",
        })
        self.assertEqual(set(tables["input_session"][0]), {"run_id", "session_id", "tariff_id"})
        self.assertEqual(set(tables["ocpp_transaction_events_raw"][0]), {
            "run_id", "event_id", "charging_station_id", "transaction_id", "event_type",
            "sequence_number", "event_time", "ingest_time", "payload", "payload_hash",
        })
        self.assertEqual(set(tables["tariffs_raw"][0]), {
            "run_id", "tariff_id", "valid_from", "valid_to", "currency", "payload", "payload_hash",
        })
        self.assertEqual(set(tables["ocpi_cdrs_raw"][0]), {
            "run_id", "release_role", "country_code", "party_id", "cdr_id", "session_id",
            "cdr_type", "currency", "total_cost", "payload", "payload_hash", "ingest_time",
        })
        events = tables["ocpp_transaction_events_raw"]
        originals = {json.loads(row["raw_record"])["event_id"]: json.loads(row["raw_record"])["payload"]
                     for row in self.rows}
        for row in events:
            self.assertEqual(row["payload"], originals[row["event_id"]])
            self.assertEqual(row["payload_hash"], sha256(row["payload"]))
            self.assertIsNone(row["event_time"].tzinfo)
            self.assertEqual(row["ingest_time"], ARRIVED)
        self.assertEqual(tables["run_manifest"][0]["created_at"], min(row["event_time"] for row in events))
        tariff = tables["tariffs_raw"][0]
        self.assertEqual(tariff["valid_from"], tables["run_manifest"][0]["created_at"])
        self.assertIsNone(tariff["valid_to"])
        self.assertEqual(tariff["payload_hash"], sha256(tariff["payload"]))
        self.assertEqual(tables["run_manifest"][0]["tariff_hash"], tariff["payload_hash"])
        self.assertEqual({row["tariff_id"] for row in tables["input_session"]}, {tariff["tariff_id"]})
        for session in result["session_ids"]:
            cdrs = [row for row in tables["ocpi_cdrs_raw"] if row["session_id"] == session]
            self.assertEqual([row["release_role"] for row in cdrs], ["baseline", "candidate"])
            self.assertEqual(cdrs[0]["payload"], cdrs[1]["payload"])
            session_events = [row for row in events if row["transaction_id"] == session]
            start, _, end = session_events
            meters = [json.loads(row["payload"])["meterValue"][0]["sampledValue"][0]["value"]
                      for row in session_events]
            body = json.loads(cdrs[0]["payload"], parse_float=Decimal)
            self.assertEqual(body["total_energy"], Decimal(meters[-1] - meters[0]) / Decimal(1000))
            self.assertEqual(body["total_cost"]["excl_vat"], cdrs[0]["total_cost"])
            self.assertEqual(body["start_date_time"], start["event_time"].isoformat() + "Z")
            self.assertEqual(body["end_date_time"], end["event_time"].isoformat() + "Z")
            self.assertEqual(body["last_updated"], (end["event_time"] + timedelta(seconds=1)).isoformat() + "Z")
            self.assertEqual(cdrs[0]["ingest_time"], end["event_time"] + timedelta(seconds=2))
            self.assertEqual(len(body["charging_periods"]), 1)
            self.assertEqual(body["charging_periods"][0]["tariff_id"], tariff["tariff_id"])
            self.assertEqual(body["tariffs"], [json.loads(tariff["payload"], parse_float=Decimal)])
            self.assertFalse(body["credit"])
            self.assertLessEqual(len(body["id"]), 36)
            self.assertLessEqual(len(body["session_id"]), 36)
            self.assertEqual(cdrs[0]["payload_hash"], sha256(cdrs[0]["payload"]))

    def test_exact_half_cent_uses_half_up_and_plain_six_decimal_duration(self):
        # 3.3 kWh * EUR 0.45 = EUR 1.485; float/banker's round gave 1.48.
        start = json.loads(json.loads(self.rows[0]["raw_record"])["payload"])
        start_wh = start["meterValue"][0]["sampledValue"][0]["value"]
        rewrite(self.rows[1], lambda body: body["meterValue"][0]["sampledValue"][0].update(value=start_wh + 1650), payload=True)
        rewrite(self.rows[2], lambda body: body["meterValue"][0]["sampledValue"][0].update(value=start_wh + 3300), payload=True)
        session_id = start["transactionInfo"]["transactionId"]
        cdrs = [row for row in self.prepare()["tables"]["ocpi_cdrs_raw"] if row["session_id"] == session_id]
        self.assertEqual([row["total_cost"] for row in cdrs], [Decimal("1.49"), Decimal("1.49")])
        for row in cdrs:
            self.assertIn('"total_energy":3.3', row["payload"])
            self.assertRegex(row["payload"], r'"total_time":[0-9]+\.[0-9]{6}')
            self.assertIn('"excl_vat":1.49', row["payload"])

    def test_amount_error_changes_only_candidate_reported_amount(self):
        healthy, faulty = self.prepare(), self.prepare("amount_error")
        self.assertEqual(healthy["input_hash"], faulty["input_hash"])
        self.assertNotEqual(healthy["run_id"], faulty["run_id"])
        healthy_cdrs = healthy["tables"]["ocpi_cdrs_raw"]
        faulty_cdrs = faulty["tables"]["ocpi_cdrs_raw"]
        for clean, bad in zip(healthy_cdrs, faulty_cdrs):
            clean_body = json.loads(clean["payload"], parse_float=Decimal)
            bad_body = json.loads(bad["payload"], parse_float=Decimal)
            delta = Decimal("0.87") if bad["release_role"] == "candidate" else Decimal(0)
            self.assertEqual(bad["total_cost"] - clean["total_cost"], delta)
            clean_body["total_cost"]["excl_vat"] += delta
            self.assertEqual(clean_body, bad_body)
        baseline = healthy["tables"]["run_manifest"][0]["baseline_sha"]
        self.assertEqual(healthy["tables"]["run_manifest"][0]["candidate_sha"], baseline)
        self.assertEqual(faulty["tables"]["run_manifest"][0]["baseline_sha"], baseline)
        self.assertNotEqual(faulty["tables"]["run_manifest"][0]["candidate_sha"], baseline)

    def test_fingerprints_cover_full_normalized_module_and_variant(self):
        source = Path(adapter.__file__).read_bytes().replace(b"\r\n", b"\n")
        expected = self.prepare("amount_error")
        manifest = expected["tables"]["run_manifest"][0]
        for field, variant in (("baseline_sha", b"healthy-v1"), ("candidate_sha", b"amount-error-v1")):
            self.assertEqual(manifest[field], hashlib.sha1(source + b"\0" + variant).hexdigest())
        self.assertIn("mock", manifest["scenario_id"])
        with patch("notebooks.generated_batch.Path.read_bytes", return_value=source.replace(b"\n", b"\r\n")):
            self.assertEqual(self.prepare("amount_error"), expected)
        with patch("notebooks.generated_batch.Path.read_bytes", return_value=source + b"\n# modified\n"):
            changed = self.prepare("amount_error")
        self.assertNotEqual(changed["tables"]["run_manifest"][0]["baseline_sha"], manifest["baseline_sha"])

    def test_input_hash_and_run_identity_are_explicit_and_ignore_arrival(self):
        result = self.prepare()
        envelopes = sorted([json.loads(row["raw_record"]) for row in self.rows], key=lambda row: row["event_id"])
        input_hash = sha256(json.dumps(envelopes, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
        self.assertEqual(result["input_hash"], input_hash)
        self.assertEqual(result["run_id"], "generated-check-v1-audit-healthy-" + sha256(input_hash + PIPELINE_HASH)[:16])
        changed_pipeline = self.prepare(pipeline_hash="b" * 64)
        self.assertEqual(changed_pipeline["input_hash"], result["input_hash"])
        self.assertNotEqual(changed_pipeline["run_id"], result["run_id"])
        changed_seed = self.prepare(rows=landing(seed=43))
        self.assertNotEqual(changed_seed["input_hash"], result["input_hash"])
        self.assertNotEqual(changed_seed["run_id"], result["run_id"])
        self.assertEqual(self.prepare(rows=landing(seed=43)), changed_seed)

    def test_duplicates_other_files_and_order_are_no_ops_and_do_not_mutate_inputs(self):
        original = copy.deepcopy(self.rows)
        expected = self.prepare()
        duplicates = copy.deepcopy(self.rows)
        for row in duplicates:
            row.update(source_file_name="duplicate.jsonl", source_file_path="/elsewhere/duplicate.jsonl",
                       ingested_at=ARRIVED + timedelta(days=1))
            # Outer JSON whitespace can differ while the original payload text stays exact.
            row["raw_record"] = "  " + row["raw_record"] + " "
            row["record_hash"] = sha256(row["raw_record"])
        self.assertEqual(self.prepare(rows=list(reversed(self.rows + duplicates))), expected)
        self.assertEqual(self.rows, original)

    def test_earlier_duplicate_changes_only_raw_arrival_time_and_accepts_spark_utc(self):
        expected = self.prepare()
        duplicates = copy.deepcopy(self.rows)
        early = datetime(2026, 9, 29, 12, tzinfo=timezone.utc)
        for row in duplicates:
            row["ingested_at"] = early
        result = self.prepare(rows=self.rows + duplicates)
        for row in expected["tables"]["ocpp_transaction_events_raw"]:
            row["ingest_time"] = early.replace(tzinfo=None)
        self.assertEqual(result, expected)
        for row in duplicates:
            row["ingested_at"] = "2026-09-29T14:00:00+02:00"
        self.assertEqual(self.prepare(rows=duplicates), expected)

    def test_payload_whitespace_is_preserved_and_changes_input_identity(self):
        expected = self.prepare()
        envelope = json.loads(self.rows[0]["raw_record"])
        changed_payload = "  " + envelope["payload"] + " "
        rewrite(self.rows[0], lambda row: row.update(payload=changed_payload))
        result = self.prepare()
        raw = next(row for row in result["tables"]["ocpp_transaction_events_raw"] if row["event_id"] == envelope["event_id"])
        self.assertEqual(raw["payload"], changed_payload)
        self.assertEqual(raw["payload_hash"], sha256(changed_payload))
        self.assertNotEqual(result["input_hash"], expected["input_hash"])
        self.assertNotEqual(result["run_id"], expected["run_id"])

    def test_conflicting_duplicate_event_is_rejected(self):
        extra = copy.deepcopy(self.rows[0])
        rewrite(extra, lambda body: body["meterValue"][0]["sampledValue"][0].update(value=1), payload=True)
        with self.assertRaisesRegex(ValueError, "Conflicting event evidence"):
            self.prepare(rows=self.rows + [extra])

    def test_missing_whole_session_is_rejected_using_declared_inventory(self):
        for rows in (self.rows[:3], self.rows[:-1], self.rows[:3] * 2):
            with self.subTest(count=len(rows)), self.assertRaisesRegex(ValueError, "Incomplete declared session inventory"):
                self.prepare(rows=rows)

    def test_maximum_declared_sessions_and_landing_row_bounds(self):
        result = self.prepare(rows=landing(count=1000))
        self.assertEqual(len(result["session_ids"]), 1000)
        self.assertEqual(len(result["tables"]["ocpi_cdrs_raw"]), 2000)
        self.assertEqual(result["metadata"]["expected_assertions"], 14000)
        self.assertEqual(self.prepare(rows=(self.rows * 1667)[:10000])["input_hash"], self.prepare()["input_hash"])
        for rows in ([], [self.rows[0]] * 10001, iter(self.rows)):
            with self.assertRaises(ValueError):
                self.prepare(rows=rows)
        with self.assertRaises(ValueError):
            adapter.prepare_batch(None, "audit", "healthy", PIPELINE_HASH)

    def test_mixed_tariff_or_cdr_rows_request_fresh_batch_without_mutation(self):
        for marker in ("tariff_id", "cdr_id", "release_role"):
            mixed = copy.deepcopy(self.rows)
            rewrite(mixed[0], lambda row: row.update({marker: "legacy-mixed-evidence"}))
            before = copy.deepcopy(mixed)
            with self.assertRaisesRegex(ValueError, "fresh batch_id"):
                self.prepare(rows=mixed)
            self.assertEqual(mixed, before)

    def test_whole_line_hash_and_malformed_json_are_rejected(self):
        row = copy.deepcopy(self.rows[0])
        row["record_hash"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "whole-line record_hash mismatch"):
            self.prepare(rows=[row] + self.rows[1:])
        for raw in ('{', '[]', '{"x":NaN}', '{"x":Infinity}', '{"x":1,"x":2}'):
            row = dict(self.rows[0], raw_record=raw, record_hash=sha256(raw))
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.prepare(rows=[row] + self.rows[1:])

    def test_bad_envelope_metadata_and_arrival_types_are_rejected(self):
        edits = [
            lambda row: row.update(schema_version=True),
            lambda row: row.update(schema_version=2),
            lambda row: row.update(run_id="generated-v1-other"),
            lambda row: row.update(event_id="other"),
            lambda row: row.update(charging_station_id="other"),
            lambda row: row.update(payload={}),
            lambda row: row.update(extra="unsupported"),
            lambda row: row["generator"].update(version="sessions-v2"),
            lambda row: row["generator"].update(batch_id="other"),
            lambda row: row["generator"].update(session_count=True),
            lambda row: row["generator"].update(session_count=0),
            lambda row: row["generator"].update(session_count=1001),
            lambda row: row["generator"].update(seed=True),
            lambda row: row["generator"].update(seed=-1),
            lambda row: row["generator"].update(seed=2**32),
            lambda row: row["generator"]["tariff"].update(price_per_kwh="0.46"),
            lambda row: row["generator"]["tariff"].update(currency="USD"),
            lambda row: row["generator"]["tariff"].update(id="other"),
        ]
        for edit in edits:
            rows = copy.deepcopy(self.rows)
            rewrite(rows[0], edit)
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                self.prepare(rows=rows)
        for arrival in (None, "2026-09-01", 42, "invalid"):
            rows = copy.deepcopy(self.rows)
            rows[0]["ingested_at"] = arrival
            with self.subTest(arrival=arrival), self.assertRaises(ValueError):
                self.prepare(rows=rows)
        rows = copy.deepcopy(self.rows)
        del rows[0]["ingested_at"]
        with self.assertRaises(ValueError):
            self.prepare(rows=rows)
        rows = copy.deepcopy(self.rows)
        rewrite(rows[1], lambda row: row["generator"].update(seed=43))
        with self.assertRaisesRegex(ValueError, "Conflicting generator metadata"):
            self.prepare(rows=rows)

    def test_bad_payload_types_times_units_and_sequences_are_rejected(self):
        def sample(body):
            return body["meterValue"][0]["sampledValue"][0]
        edits = [
            lambda body: body.update(seqNo=True),
            lambda body: body.update(seqNo=2),
            lambda body: body.update(eventType="Updated"),
            lambda body: body.update(timestamp="2026-09-01T12:00:00"),
            lambda body: body.update(timestamp="2026-99-01T12:00:00Z"),
            lambda body: body["meterValue"][0].update(timestamp="2026-09-01T12:00:00Z"),
            lambda body: body.update(meterValue=[]),
            lambda body: body["meterValue"][0].update(sampledValue=[]),
            lambda body: sample(body).update(value=True),
            lambda body: sample(body).update(value=-1),
            lambda body: sample(body).update(value=2**63),
            lambda body: sample(body).update(value=1.5),
            lambda body: sample(body).update(measurand="other"),
            lambda body: sample(body).update(location="Inlet"),
            lambda body: sample(body)["unitOfMeasure"].update(unit="kWh"),
            lambda body: sample(body)["unitOfMeasure"].update(multiplier=True),
            lambda body: sample(body)["unitOfMeasure"].update(multiplier=1),
            lambda body: body["transactionInfo"].update(transactionId="txn-unknown"),
        ]
        for edit in edits:
            rows = copy.deepcopy(self.rows)
            rewrite(rows[0], edit, payload=True)
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                self.prepare(rows=rows)
        for edit in (lambda body: body["transactionInfo"].update(timeSpentCharging=True),
                     lambda body: body["transactionInfo"].update(timeSpentCharging=1),
                     lambda body: sample(body).update(value=0)):
            rows = copy.deepcopy(self.rows)
            rewrite(rows[2], edit, payload=True)
            with self.assertRaises(ValueError):
                self.prepare(rows=rows)

    def test_duplicate_payload_keys_and_overflow_are_rejected(self):
        rows = copy.deepcopy(self.rows)
        rewrite(rows[0], lambda row: row.update(payload=row["payload"].replace('"seqNo":0', '"seqNo":0,"seqNo":0')))
        with self.assertRaisesRegex(ValueError, "Duplicate JSON key"):
            self.prepare(rows=rows)
        rows = copy.deepcopy(self.rows)
        rewrite(rows[2], lambda body: body["meterValue"][0]["sampledValue"][0].update(value=2**63 - 1), payload=True)
        with self.assertRaisesRegex(ValueError, "DECIMAL"):
            self.prepare(rows=rows)

    def test_invalid_parameters_are_rejected(self):
        for batch in (None, "", "../path", "Upper", "a" * 33, "batch\n", 12):
            with self.subTest(batch=batch), self.assertRaises(ValueError):
                adapter.prepare_batch(self.rows, batch, "healthy", PIPELINE_HASH)
        for mode in (None, "", "candidate", "amount-error", True, []):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.prepare(mode)
        for digest in (None, "", "a" * 63, "g" * 64, "A" * 64, 42):
            with self.subTest(digest=digest), self.assertRaises(ValueError):
                self.prepare(pipeline_hash=digest)


if __name__ == "__main__":
    unittest.main()
