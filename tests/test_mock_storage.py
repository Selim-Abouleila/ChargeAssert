"""Evidence retention/retry checks; these do not execute Spark or Delta MERGE."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "notebooks"))
from generate_mock_billing import verify_stored_rows


class MockStorageTests(unittest.TestCase):
    def setUp(self):
        self.expected = [
            {"run_id": "mock-bad", "candidate_sha": "bad-code", "created_at": datetime(2026, 8, 22)},
            {"run_id": "mock-fixed", "candidate_sha": "fixed-code", "created_at": datetime(2026, 8, 22)},
        ]

    def test_empty_partial_and_identical_reruns_can_resume(self):
        for rows in ([], self.expected[:1], self.expected):
            verify_stored_rows("run_manifest", rows, self.expected)
        verify_stored_rows("run_manifest", self.expected[::-1], self.expected, require_complete=True)

    def test_changed_code_or_input_is_rejected_without_modifying_evidence(self):
        existing = [{**self.expected[0], "candidate_sha": "old-code"}]
        original = [dict(row) for row in existing]
        with self.assertRaisesRegex(ValueError, "new versioned run IDs"):
            verify_stored_rows("run_manifest", existing, self.expected)
        self.assertEqual(existing, original)

    def test_duplicate_unexpected_and_incomplete_rows_are_rejected(self):
        for rows in (self.expected + self.expected[:1], [{**self.expected[0], "run_id": "unexpected"}]):
            with self.assertRaisesRegex(ValueError, "Stored mock evidence differs"):
                verify_stored_rows("run_manifest", rows, self.expected)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            verify_stored_rows("run_manifest", self.expected[:1], self.expected, require_complete=True)

    def test_empty_or_duplicate_generation_is_rejected(self):
        for rows in ([], self.expected + self.expected[:1]):
            with self.assertRaisesRegex(ValueError, "empty or duplicate"):
                verify_stored_rows("run_manifest", [], rows)

    def test_timestamp_round_trip_compares_instants(self):
        aware = [{**row, "created_at": datetime(2026, 8, 22, 2, tzinfo=timezone(timedelta(hours=2)))}
                 for row in self.expected]
        verify_stored_rows("run_manifest", aware, self.expected, require_complete=True)

    def test_extra_cdr_version_cannot_be_silently_added_on_rerun(self):
        expected = [{"run_id": "mock-bad", "release_role": "candidate", "payload_hash": "current", "payload": "new"}]
        existing = [{**expected[0], "payload_hash": "original", "payload": "old"}]
        with self.assertRaisesRegex(ValueError, "Stored mock evidence differs"):
            verify_stored_rows("ocpi_cdrs_raw", existing, expected)


if __name__ == "__main__":
    unittest.main()
