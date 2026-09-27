"""Test absent CDR evidence and preserve already-deployed amount fixtures."""

import copy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from notebooks.generate_mock_billing import verify_stored_rows
from notebooks.mock_billing import generate_mock_runs, MOCK_RUN_IDS
from notebooks.mock_missing_cdr import (
    generate_all_mock_runs, generate_missing_cdr_runs, MISSING_CDR_RUN_IDS,
)
import test_mock_billing as billing_fixtures


ROOT = Path(__file__).resolve().parents[1]


class MissingCdrMockTests(unittest.TestCase):
    def setUp(self):
        fixture = billing_fixtures.MockBillingTests()
        fixture.setUp()
        self.events, self.tariff, self.manifest = fixture.events, fixture.tariff, fixture.manifest

    def generate(self):
        return generate_missing_cdr_runs(self.events, self.tariff, self.manifest)

    def test_bad_candidate_emits_zero_rows_and_other_roles_bill_correctly(self):
        result = self.generate()
        self.assertEqual({table: len(rows) for table, rows in result.items()}, {
            "run_manifest": 2, "ocpp_transaction_events_raw": 6,
            "tariffs_raw": 2, "ocpi_cdrs_raw": 3,
        })
        self.assertEqual([(row["run_id"], row["release_role"]) for row in result["ocpi_cdrs_raw"]], [
            (MISSING_CDR_RUN_IDS[0], "baseline"),
            (MISSING_CDR_RUN_IDS[1], "baseline"),
            (MISSING_CDR_RUN_IDS[1], "candidate"),
        ])
        for row in result["ocpi_cdrs_raw"]:
            body = json.loads(row["payload"], parse_float=Decimal)
            self.assertEqual(row["total_cost"], Decimal("5.63"))
            self.assertEqual(body["total_cost"]["excl_vat"], Decimal("5.63"))
            self.assertEqual(body["total_energy"], Decimal("12.5"))
            self.assertEqual(body["total_time"], Decimal("1.000000"))
            self.assertEqual(body["session_id"], "txn-smoke-v1")
            self.assertEqual(body["charging_periods"][0]["tariff_id"], "tariff-smoke-v1")
            self.assertEqual(body["last_updated"], "2026-08-22T11:00:01Z")
            self.assertEqual(row["payload_hash"], hashlib.sha256(row["payload"].encode()).hexdigest())
        self.assertEqual(len({row["payload"] for row in result["ocpi_cdrs_raw"]}), 1)

    def test_identical_inputs_clock_seed_and_repeatable_outputs(self):
        before = copy.deepcopy((self.events, self.tariff, self.manifest))
        result = self.generate()
        self.assertEqual(result, self.generate())
        self.assertEqual(result, generate_missing_cdr_runs(list(reversed(self.events)), self.tariff, self.manifest))
        self.assertEqual((self.events, self.tariff, self.manifest), before)
        for run_id in MISSING_CDR_RUN_IDS:
            self.assertEqual([
                dict(row, run_id="smoke-run-v1") for row in result["ocpp_transaction_events_raw"]
                if row["run_id"] == run_id
            ], self.events)
            tariff = next(row for row in result["tariffs_raw"] if row["run_id"] == run_id)
            self.assertEqual(dict(tariff, run_id="smoke-run-v1"), self.tariff)
        bad, fixed = result["run_manifest"]
        self.assertEqual(bad["baseline_sha"], fixed["baseline_sha"])
        self.assertEqual(fixed["candidate_sha"], fixed["baseline_sha"])
        self.assertNotEqual(bad["candidate_sha"], fixed["candidate_sha"])
        for manifest in (bad, fixed):
            self.assertEqual(manifest["scenario_id"], "mock-missing-cdr-v1")
            self.assertEqual(manifest["seed"], 42)
            self.assertEqual(manifest["created_at"], self.manifest["created_at"])
            self.assertEqual(manifest["tariff_hash"], self.tariff["payload_hash"])

    def test_healthy_outputs_are_calculated_from_meter_inputs(self):
        body = json.loads(self.events[-1]["payload"])
        body["meterValue"][0]["sampledValue"][0]["value"] = 113000
        self.events[-1]["payload"] = json.dumps(body, separators=(",", ":"))
        self.events[-1]["payload_hash"] = hashlib.sha256(self.events[-1]["payload"].encode()).hexdigest()
        result = self.generate()
        self.assertEqual(len(result["ocpi_cdrs_raw"]), 3)
        for row in result["ocpi_cdrs_raw"]:
            self.assertEqual(row["total_cost"], Decimal("5.85"))
            self.assertEqual(json.loads(row["payload"])["total_energy"], 13)

    def test_missing_candidate_does_not_bypass_input_validation(self):
        self.events[-1]["payload_hash"] = "corrupt"
        with self.assertRaisesRegex(ValueError, "Payload hash mismatch"):
            self.generate()

    def test_combined_generation_preserves_original_amount_evidence_and_fingerprints(self):
        old = generate_mock_runs(self.events, self.tariff, self.manifest)
        combined = generate_all_mock_runs(self.events, self.tariff, self.manifest)
        for table, rows in old.items():
            self.assertEqual([row for row in combined[table] if row["run_id"] in MOCK_RUN_IDS], rows)
        # These are the code/variant fingerprints already persisted by the v1
        # amount mock. Editing that module would invalidate immutable reruns.
        bad, fixed = old["run_manifest"]
        self.assertEqual(bad["baseline_sha"], "740cb29180c1bf2e2d6f17d62a48e2ca99f2ebbc")
        self.assertEqual(bad["candidate_sha"], "a85de3a21c6cf36bdd11aa7c8e63096311919970")
        self.assertEqual(fixed["candidate_sha"], bad["baseline_sha"])
        self.assertEqual({table: len(rows) for table, rows in combined.items()}, {
            "run_manifest": 4, "ocpp_transaction_events_raw": 12,
            "tariffs_raw": 4, "ocpi_cdrs_raw": 7,
        })

    def test_new_fingerprints_cover_both_modules_and_ignore_line_endings(self):
        original_read = Path.read_bytes
        expected = self.generate()["run_manifest"]

        def crlf_bytes(path):
            return original_read(path).replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")

        with patch.object(Path, "read_bytes", crlf_bytes):
            self.assertEqual(self.generate()["run_manifest"], expected)
        for name in ("mock_billing.py", "mock_missing_cdr.py"):
            def changed_bytes(path):
                return original_read(path) + (b"\n# source change\n" if path.name == name else b"")
            with self.subTest(module=name), patch.object(Path, "read_bytes", changed_bytes):
                changed = self.generate()["run_manifest"]
                for old, new in zip(expected, changed):
                    self.assertNotEqual(old["baseline_sha"], new["baseline_sha"])
                    self.assertNotEqual(old["candidate_sha"], new["candidate_sha"])

    def test_immutable_replay_accepts_absence_and_rejects_unexpected_candidate_cdr(self):
        expected = generate_all_mock_runs(self.events, self.tariff, self.manifest)
        for table, rows in expected.items():
            verify_stored_rows(table, rows, rows, require_complete=True)
        cdrs = expected["ocpi_cdrs_raw"]
        baseline = next(row for row in cdrs if row["run_id"] == MISSING_CDR_RUN_IDS[0])
        unexpected = dict(baseline, release_role="candidate")
        with self.assertRaisesRegex(ValueError, "Stored mock evidence differs"):
            verify_stored_rows("ocpi_cdrs_raw", cdrs + [unexpected], cdrs)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            verify_stored_rows("ocpi_cdrs_raw", cdrs[:-1], cdrs, require_complete=True)


if __name__ == "__main__":
    unittest.main()
