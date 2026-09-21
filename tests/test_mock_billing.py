"""Exercise executable mock billing without Spark, Silver, or canned CDRs."""

import copy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import unittest
from unittest.mock import patch

from notebooks.mock_billing import generate_mock_runs, MOCK_RUN_IDS


ROOT = Path(__file__).resolve().parents[1]


def sha256(text):
    return hashlib.sha256(text.encode()).hexdigest()


class MockBillingTests(unittest.TestCase):
    def setUp(self):
        # Reuse the production input evidence, never the canned output CDR.
        bodies = re.findall(r"'(\{\"eventType\":[^'\r\n]+})'",
                            (ROOT / "sql/02_create_ocpp_transaction_events_raw.sql").read_text())
        self.assertEqual(len(bodies), 3)
        self.events = []
        for payload in bodies:
            body = json.loads(payload)
            self.events.append({
                "run_id": "smoke-run-v1", "event_id": f"event-{body['seqNo']}",
                "charging_station_id": "cs-smoke-001", "transaction_id": "txn-smoke-v1",
                "event_type": body["eventType"], "sequence_number": body["seqNo"],
                "event_time": body["timestamp"], "ingest_time": body["timestamp"],
                "payload": payload, "payload_hash": sha256(payload),
            })
        payload = re.findall(r"'(\{\"id\":\"tariff-smoke-v1\"[^'\r\n]+})'",
                             (ROOT / "sql/04_create_tariffs_raw.sql").read_text())[0]
        self.tariff = {
            "run_id": "smoke-run-v1", "tariff_id": "tariff-smoke-v1",
            "valid_from": "2026-01-01T00:00:00Z", "valid_to": "2026-12-31T23:59:59Z",
            "currency": "EUR", "payload": payload, "payload_hash": sha256(payload),
        }
        self.manifest = {
            "run_id": "smoke-run-v1", "scenario_id": "smoke-scenario-v1", "seed": 42,
            "baseline_sha": "ignored-source-label", "candidate_sha": "ignored-source-label",
            "tariff_hash": "legacy-smoke-id-hash", "created_at": "2026-08-22T00:00:00Z",
        }

    def generate(self):
        return generate_mock_runs(self.events, self.tariff, self.manifest)

    def edit_payload(self, row, edit):
        body = json.loads(row["payload"])
        edit(body)
        row["payload"] = json.dumps(body, separators=(",", ":"))
        row["payload_hash"] = sha256(row["payload"])

    def test_computes_baseline_bad_and_fixed_outputs(self):
        result = self.generate()
        self.assertEqual({key: len(rows) for key, rows in result.items()}, {
            "run_manifest": 2, "ocpp_transaction_events_raw": 6,
            "tariffs_raw": 2, "ocpi_cdrs_raw": 4,
        })
        outputs = {(row["run_id"], row["release_role"]): row for row in result["ocpi_cdrs_raw"]}
        for (run_id, role), row in outputs.items():
            faulty = run_id == MOCK_RUN_IDS[0] and role == "candidate"
            body = json.loads(row["payload"], parse_float=Decimal)
            self.assertEqual(row["total_cost"], Decimal("6.50" if faulty else "5.63"))
            self.assertEqual(body["total_cost"]["excl_vat"], row["total_cost"])
            self.assertEqual(body["total_energy"], Decimal("12.5"))
            self.assertEqual(body["total_time"], Decimal("1.000000"))
            self.assertEqual(body["session_id"], "txn-smoke-v1")
            self.assertEqual(body["charging_periods"][0]["tariff_id"], "tariff-smoke-v1")
            self.assertEqual(row["payload_hash"], sha256(row["payload"]))
            self.assertEqual(row["ingest_time"], datetime(2026, 8, 22, 11, 0, 2))
            self.assertEqual(body["last_updated"], "2026-08-22T11:00:01Z")
        baseline = outputs[(MOCK_RUN_IDS[0], "baseline")]
        fixed = outputs[(MOCK_RUN_IDS[1], "candidate")]
        self.assertEqual(baseline["payload"], fixed["payload"])

    def test_repeated_and_reordered_inputs_are_deterministic_and_not_mutated(self):
        before = copy.deepcopy((self.events, self.tariff, self.manifest))
        first = self.generate()
        self.assertEqual(self.generate(), first)
        self.assertEqual(generate_mock_runs(list(reversed(self.events)), self.tariff, self.manifest), first)
        self.assertEqual((self.events, self.tariff, self.manifest), before)

    def test_clones_exact_input_evidence_and_fingerprints_executable_variants(self):
        result = self.generate()
        for run_id in MOCK_RUN_IDS:
            events = [row for row in result["ocpp_transaction_events_raw"] if row["run_id"] == run_id]
            self.assertEqual([dict(row, run_id="smoke-run-v1") for row in events], self.events)
            tariff = next(row for row in result["tariffs_raw"] if row["run_id"] == run_id)
            self.assertEqual(dict(tariff, run_id="smoke-run-v1"), self.tariff)
        bad, fixed = result["run_manifest"]
        self.assertEqual(bad["baseline_sha"], fixed["baseline_sha"])
        self.assertEqual(fixed["candidate_sha"], fixed["baseline_sha"])
        self.assertNotEqual(bad["candidate_sha"], bad["baseline_sha"])
        for row in result["run_manifest"]:
            self.assertRegex(row["baseline_sha"], r"^[0-9a-f]{40}$")
            self.assertEqual(row["seed"], 42)
            self.assertEqual(row["tariff_hash"], sha256(self.tariff["payload"]))

    def test_meter_evidence_changes_computed_charge(self):
        self.edit_payload(self.events[-1], lambda body:
                          body["meterValue"][0]["sampledValue"][0].update(value=113000))
        rows = self.generate()["ocpi_cdrs_raw"]
        self.assertEqual([row["total_cost"] for row in rows],
                         [Decimal("5.85"), Decimal("6.72"), Decimal("5.85"), Decimal("5.85")])
        self.assertEqual(json.loads(rows[0]["payload"])["total_energy"], 13)

    def test_code_fingerprints_ignore_checkout_line_endings(self):
        source = (ROOT / "notebooks/mock_billing.py").read_bytes().replace(b"\r\n", b"\n")
        expected = self.generate()
        with patch("notebooks.mock_billing.Path.read_bytes", return_value=source.replace(b"\n", b"\r\n")):
            self.assertEqual(self.generate(), expected)
        with patch("notebooks.mock_billing.Path.read_bytes", return_value=source + b"\n# source change\n"):
            changed = self.generate()
        self.assertNotEqual(changed["run_manifest"][0]["baseline_sha"],
                            expected["run_manifest"][0]["baseline_sha"])

    def test_tariff_evidence_changes_charge_and_half_up_rounds_cents(self):
        self.assertEqual(self.generate()["ocpi_cdrs_raw"][0]["total_cost"], Decimal("5.63"))
        self.edit_payload(self.tariff, lambda body:
                          body["elements"][0]["price_components"][0].update(price=0.5))
        self.assertEqual(self.generate()["ocpi_cdrs_raw"][0]["total_cost"], Decimal("6.25"))

    def test_duration_comes_from_event_clock(self):
        def change_end(body):
            body["timestamp"] = "2026-08-22T11:30:00Z"
            body["meterValue"][0]["timestamp"] = body["timestamp"]
        self.edit_payload(self.events[-1], change_end)
        self.events[-1]["event_time"] = "2026-08-22T11:30:00Z"
        row = self.generate()["ocpi_cdrs_raw"][0]
        self.assertEqual(json.loads(row["payload"])["total_time"], 1.5)
        self.assertEqual(row["total_cost"], Decimal("5.63"))

    def test_spark_datetime_inputs_are_accepted(self):
        for row in self.events:
            for key in ("event_time", "ingest_time"):
                row[key] = datetime.fromisoformat(row[key].replace("Z", "+00:00")).replace(tzinfo=None)
        self.tariff["valid_from"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.manifest["created_at"] = datetime(2026, 8, 22)
        self.assertEqual(self.generate()["ocpi_cdrs_raw"][0]["total_cost"], Decimal("5.63"))

    def test_rejects_corrupt_input_hash(self):
        for target in (self.events[0], self.tariff):
            original = target["payload_hash"]
            target["payload_hash"] = "wrong"
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                self.generate()
            target["payload_hash"] = original

    def test_rejects_missing_duplicate_and_mismatched_events(self):
        original = copy.deepcopy(self.events)
        invalid_events = [original[:2], original + [original[-1]],
                          [original[0], original[0], original[-1]],
                          [dict(original[0], event_type="Ended"), *original[1:]],
                          [dict(original[0], transaction_id="other"), *original[1:]],
                          [dict(original[0], sequence_number=2), *original[1:]],
                          [dict(original[0], event_time="2026-08-22T09:00:00Z"), *original[1:]],
                          [dict(original[0], charging_station_id="other"), *original[1:]]]
        for events in invalid_events:
            with self.subTest(events=events):
                with self.assertRaises(ValueError):
                    generate_mock_runs(events, self.tariff, self.manifest)

    def test_rejects_meter_reset_unsupported_units_and_fractional_wh(self):
        original = copy.deepcopy(self.events)
        for change in (lambda sample: sample.update(value=99999),
                       lambda sample: sample.update(value=112500.5),
                       lambda sample: sample["unitOfMeasure"].update(unit="kWh"),
                       lambda sample: sample["unitOfMeasure"].update(multiplier=1)):
            self.events = copy.deepcopy(original)
            self.edit_payload(self.events[-1], lambda body: change(body["meterValue"][0]["sampledValue"][0]))
            with self.assertRaises(ValueError):
                self.generate()

    def test_rejects_unsupported_tariffs(self):
        original = copy.deepcopy(self.tariff)
        changes = [lambda body: body.update(currency="USD"),
                   lambda body: body.update(min_price={"excl_vat": 1}),
                   lambda body: body["elements"][0].update(restrictions={"min_kwh": 1}),
                   lambda body: body["elements"][0]["price_components"][0].update(type="TIME"),
                   lambda body: body["elements"][0]["price_components"][0].update(step_size=1000),
                   lambda body: body["elements"][0]["price_components"][0].update(vat=20),
                   lambda body: body["elements"][0]["price_components"][0].update(price=-0.1),
                   lambda body: body["elements"][0]["price_components"][0].update(price=0.4500001)]
        for change in changes:
            self.tariff = copy.deepcopy(original)
            self.edit_payload(self.tariff, change)
            with self.assertRaises(ValueError):
                self.generate()

    def test_rejects_tariff_outside_session_and_duplicate_json_keys(self):
        self.tariff["valid_to"] = "2026-08-22T10:30:00Z"
        with self.assertRaisesRegex(ValueError, "cover the complete session"):
            self.generate()
        self.tariff["valid_to"] = None
        self.assertEqual(self.generate()["ocpi_cdrs_raw"][0]["total_cost"], Decimal("5.63"))
        self.tariff["payload"] = self.tariff["payload"].replace('"currency":"EUR"',
                                                             '"currency":"USD","currency":"EUR"')
        self.tariff["payload_hash"] = sha256(self.tariff["payload"])
        with self.assertRaisesRegex(ValueError, "Duplicate JSON key"):
            self.generate()


if __name__ == "__main__":
    unittest.main()
