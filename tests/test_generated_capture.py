"""Check generated-batch capture against the production Gold SQL adapters.

The fixtures run the existing assertion/verdict SELECTs with two Silver sessions.
These tests check capture consistency and declared inventory, not Spark or Delta.
"""

from copy import deepcopy
from pathlib import Path
import unittest
from unittest.mock import patch

import test_assertion_result as assertion_fixture
import test_release_verdict as gold_fixture
from notebooks import generated_capture as capture


ROOT = Path(__file__).resolve().parents[1]
ASSERTIONS_SQL = (ROOT / "sql/generated/09_assertion_result.sql").read_text(encoding="utf-8")
VERDICT_SQL = (ROOT / "sql/generated/10_release_verdict.sql").read_text(encoding="utf-8")
RUN = "generated-v1-capture"
SESSIONS = ("txn-capture-001", "txn-capture-002")


class GeneratedCaptureTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = gold_fixture.ReleaseVerdictTests()
        self.pipeline.setUp()
        self.addCleanup(self.pipeline.tearDown)
        self.db = self.pipeline.db
        for table in ("run_manifest", "expected_ledger", "session_lifecycle", "actual_ledger", "assertion_result"):
            self.db.execute(f"DELETE FROM {table}")
        self.pipeline.add_manifest(run=RUN)
        for session in SESSIONS:
            self.pipeline.fixture.add_oracle(run=RUN, session=session)
            for role in ("baseline", "candidate"):
                self.pipeline.fixture.add_actual(run=RUN, session=session, role=role, cdr_id="cdr-" + session)
        manifest = dict(self.db.execute("SELECT * FROM run_manifest").fetchone())
        self.prepared = {
            "run_id": RUN, "session_ids": list(SESSIONS), "input_hash": "a" * 64,
            "metadata": {"session_count": 2, "pipeline_hash": "b" * 64,
                         "candidate_mode": "healthy", "expected_assertions": 28},
            "tables": {"run_manifest": [manifest]},
        }
        self.evaluate_gold()

    def evaluate_gold(self):
        with patch.object(assertion_fixture, "SQL", ASSERTIONS_SQL):
            self.assertions = [dict(row) for row in self.db.execute(
                assertion_fixture.source_query(), {"run_id": RUN})]
        self.db.execute("DELETE FROM assertion_result")
        for row in self.assertions:
            columns = list(row)
            self.db.execute(f"INSERT INTO assertion_result ({','.join(columns)}) "
                            f"VALUES ({','.join('?' for _ in columns)})", tuple(row[name] for name in columns))
        query = VERDICT_SQL.split("USING (\n", 1)[1].split("\n) AS source", 1)[0]
        for table in ("run_manifest", "assertion_result", "expected_ledger", "session_lifecycle", "actual_ledger"):
            query = query.replace(f"IDENTIFIER(:{table}_table_name)", table)
        query = query.replace(" AS STRING)", " AS TEXT)").replace("current_timestamp()", "CURRENT_TIMESTAMP")
        self.verdicts = [dict(row) for row in self.db.execute(query, {"run_id": RUN})]

    def capture(self):
        return capture.capture_result(self.prepared, self.verdicts, self.assertions)

    def test_two_healthy_sessions_preserve_all_28_checks_and_sorted_evidence(self):
        self.assertions.reverse()
        before = deepcopy((self.prepared, self.verdicts, self.assertions))
        result = self.capture()
        self.assertEqual((result["session_count"], result["assertion_count"]), (2, 28))
        self.assertEqual((result["baseline_verdict"], result["candidate_verdict"], result["verdict"]),
                         ("PASS", "PASS", "PASS"))
        self.assertEqual(result["verdict_row"], self.verdicts[0])
        self.assertEqual(result["assertions"], sorted(self.assertions, key=lambda row:
                         (row["release_role"], row["session_id"], row["assertion_id"])))
        self.assertEqual((self.prepared, self.verdicts, self.assertions), before)
        result["verdict_row"]["verdict"] = "FAIL"
        result["assertions"][0]["status"] = "FAIL"
        self.assertEqual((self.prepared, self.verdicts, self.assertions), before)

    def test_two_candidate_amount_failures_are_valid_financial_evidence(self):
        self.db.execute("UPDATE actual_ledger SET actual_amount = 6.50 WHERE release_role = 'candidate'")
        self.evaluate_gold()
        result = self.capture()
        self.assertEqual((result["baseline_verdict"], result["candidate_verdict"], result["verdict"]),
                         ("PASS", "FAIL", "FAIL"))
        self.assertEqual(result["verdict_row"]["passed_assertions"], 26)
        self.assertEqual(result["verdict_row"]["failed_assertions"], 2)
        failures = [row for row in result["assertions"] if row["status"] == "FAIL"]
        self.assertEqual({(row["session_id"], row["assertion_id"]) for row in failures},
                         {(session, "amount_match") for session in SESSIONS})
        self.assertNotIn("status", result)  # Runtime completion is a separate decision.

    def test_missing_cdr_with_blocked_values_is_valid_captured_failure(self):
        self.db.execute("DELETE FROM actual_ledger WHERE release_role = 'candidate' AND session_id = ?", (SESSIONS[1],))
        self.evaluate_gold()
        result = self.capture()
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["verdict_row"]["passed_assertions"], 22)
        self.assertEqual(result["verdict_row"]["failed_assertions"], 1)
        self.assertEqual(result["verdict_row"]["blocked_assertions"], 5)

    def test_missing_whole_session_cannot_shrink_to_a_plausible_all_pass_result(self):
        for table in ("expected_ledger", "session_lifecycle", "actual_ledger"):
            self.db.execute(f"DELETE FROM {table} WHERE session_id = ?", (SESSIONS[1],))
        self.evaluate_gold()
        self.assertEqual(self.verdicts[0]["verdict"], "PASS")
        self.assertEqual(self.verdicts[0]["required_assertions"], 14)
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.capture()

    def test_missing_release_is_rejected_even_when_supplied_counts_are_reduced(self):
        self.assertions = [row for row in self.assertions if row["release_role"] == "baseline"]
        self.verdicts[0]["required_assertions"] = 14
        self.verdicts[0]["passed_assertions"] = 14
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.capture()

    def test_duplicate_key_cannot_replace_missing_rule_at_same_total_count(self):
        self.assertions[1] = deepcopy(self.assertions[0])
        self.assertEqual(len(self.assertions), 28)
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.capture()

    def test_unknown_session_role_or_rule_is_rejected(self):
        original = deepcopy(self.assertions)
        for field, value in (("session_id", "txn-other"), ("release_role", "other"),
                             ("assertion_id", "unknown_rule")):
            with self.subTest(field=field):
                self.assertions = deepcopy(original)
                self.assertions[0][field] = value
                with self.assertRaisesRegex(ValueError, "coverage"):
                    self.capture()

    def test_unknown_run_is_not_filtered_out_of_the_supplied_evidence(self):
        self.assertions[0]["run_id"] = "another-run"
        with self.assertRaisesRegex(ValueError, "another run"):
            self.capture()

    def test_verdict_must_be_unique_and_for_the_requested_run(self):
        original = deepcopy(self.verdicts)
        for verdicts in ([], original * 2, [{**original[0], "run_id": "another-run"}]):
            with self.subTest(rows=len(verdicts)):
                self.verdicts = verdicts
                with self.assertRaises(ValueError):
                    self.capture()

    def test_all_manifest_provenance_fields_must_match(self):
        original = deepcopy(self.verdicts)
        for field in capture.PROVENANCE_FIELDS:
            with self.subTest(field=field):
                self.verdicts = deepcopy(original)
                self.verdicts[0][field] = 43 if field == "seed" else "changed-" + original[0][field]
                with self.assertRaisesRegex(ValueError, "provenance"):
                    self.capture()

    def test_counts_must_be_integers_and_agree_with_complete_rows(self):
        original = deepcopy(self.verdicts)
        for field in capture.COUNT_FIELDS:
            for value in (-1, True, float(original[0][field]), str(original[0][field]), original[0][field] + 1):
                with self.subTest(field=field, value=value):
                    self.verdicts = deepcopy(original)
                    self.verdicts[0][field] = value
                    with self.assertRaises(ValueError):
                        self.capture()
        self.verdicts = deepcopy(original)
        del self.verdicts[0]["failed_assertions"]
        with self.assertRaisesRegex(ValueError, "count"):
            self.capture()

    def test_invalid_assertion_status_never_becomes_pass(self):
        original = deepcopy(self.assertions)
        for value in (None, "", "SUCCESS", "SUCCEEDED", "pass", 1):
            with self.subTest(status=value):
                self.assertions = deepcopy(original)
                self.assertions[0]["status"] = value
                with self.assertRaisesRegex(ValueError, "status"):
                    self.capture()

    def test_release_and_overall_verdicts_must_match_assertion_statuses(self):
        original = deepcopy(self.verdicts)
        for field in ("baseline_verdict", "candidate_verdict", "verdict"):
            for value in (None, "FAIL", "BLOCKED", "SUCCEEDED"):
                with self.subTest(field=field, value=value):
                    self.verdicts = deepcopy(original)
                    self.verdicts[0][field] = value
                    with self.assertRaisesRegex(ValueError, "verdict"):
                        self.capture()
        self.db.execute("UPDATE actual_ledger SET actual_amount = 6.50 WHERE release_role = 'candidate'")
        self.evaluate_gold()
        self.verdicts[0]["candidate_verdict"] = "PASS"
        self.verdicts[0]["verdict"] = "PASS"
        with self.assertRaisesRegex(ValueError, "candidate_verdict"):
            self.capture()

    def test_prepared_inventory_cannot_be_empty_duplicate_or_inconsistent(self):
        original = deepcopy(self.prepared)
        variants = [
            {**original, "session_ids": []},
            {**original, "session_ids": [SESSIONS[0], SESSIONS[0]]},
            {**original, "metadata": {**original["metadata"], "session_count": 1}},
            {**original, "metadata": {**original["metadata"], "session_count": True}},
            {**original, "tables": {"run_manifest": []}},
            {**original, "tables": {"run_manifest": original["tables"]["run_manifest"] * 2}},
            {**original, "input_hash": None},
        ]
        for prepared in variants:
            with self.subTest(prepared=prepared):
                self.prepared = prepared
                with self.assertRaises(ValueError):
                    self.capture()


if __name__ == "__main__":
    unittest.main()
