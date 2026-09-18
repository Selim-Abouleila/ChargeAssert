"""Exercise the production verdict SELECT using deliberately incomplete results.

The assertion SELECT is evaluated first against the shared Silver fixture. SQLite
adapters check grouping, required-key coverage, fail-closed decisions and evidence.
They do not execute Delta DDL/MERGE or establish Spark/Databricks compatibility.
"""

import json
from pathlib import Path
import unittest

import test_assertion_result as assertion_fixture


ROOT = Path(__file__).resolve().parents[1]


def source_query():
    sql = (ROOT / "sql/10_create_release_verdict.sql").read_text(encoding="utf-8")
    query = sql.split("USING (\n", 1)[1].split("\n) AS source", 1)[0]
    for table in ("run_manifest", "assertion_result", "expected_ledger",
                  "session_lifecycle", "actual_ledger"):
        query = query.replace(f"IDENTIFIER(:{table}_table_name)", table)
    return query.replace(" AS STRING)", " AS TEXT)").replace("current_timestamp()", "CURRENT_TIMESTAMP")


def named_struct(*args):
    # SQLite represents nested Spark structs as JSON text. Keep them as objects
    # when testing the final evidence rather than accepting double-encoded JSON.
    fields = json.loads(assertion_fixture.named_struct(*args))
    for key in ("baseline", "candidate", "first_problem"):
        if isinstance(fields.get(key), str):
            fields[key] = json.loads(fields[key])
    return json.dumps(fields)


class ReleaseVerdictTests(unittest.TestCase):
    def setUp(self):
        # Composition reuses the Silver fixture without rediscovering its tests.
        self.fixture = assertion_fixture.AssertionResultTests()
        self.fixture.setUp()
        self.db = self.fixture.db
        self.db.create_function("named_struct", -1, named_struct)
        self.db.create_function("concat", -1, lambda *values:
                                None if any(value is None for value in values)
                                else "".join(str(value) for value in values))
        self.db.executescript("""
            CREATE TABLE run_manifest (
                run_id TEXT, scenario_id TEXT, seed INTEGER,
                baseline_sha TEXT, candidate_sha TEXT, tariff_hash TEXT,
                created_at TEXT
            );
            CREATE TABLE assertion_result (
                run_id TEXT, release_role TEXT, session_id TEXT,
                assertion_id TEXT, expected_value TEXT, actual_value TEXT,
                difference NUMERIC, status TEXT, severity TEXT,
                message TEXT, evidence TEXT
            );
        """)
        self.add_manifest()
        self.refresh_assertions()

    def tearDown(self):
        self.fixture.tearDown()

    def add_manifest(self, run="smoke-run-v1"):
        self.db.execute("INSERT INTO run_manifest VALUES (?,?,?,?,?,?,?)", (
            run, "smoke-scenario-v1", 42, "baseline-sha", "candidate-sha",
            "tariff-hash", "2026-08-22T00:00:00Z",
        ))

    def refresh_assertions(self):
        self.db.execute("DELETE FROM assertion_result")
        columns = ("run_id", "release_role", "session_id", "assertion_id",
                   "expected_value", "actual_value", "difference", "status",
                   "severity", "message", "evidence")
        self.db.executemany("INSERT INTO assertion_result VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            [tuple(row[column] for column in columns) for row in self.fixture.rows()])

    def rows(self):
        return [dict(row) for row in self.db.execute(source_query()).fetchall()]

    def verdict(self, run="smoke-run-v1"):
        results = [row for row in self.rows() if row["run_id"] == run]
        self.assertEqual(len(results), 1)
        return results[0]

    def assert_failed(self, row, baseline="PASS", candidate="FAIL"):
        self.assertEqual(row["verdict"], "FAIL")
        self.assertEqual(row["baseline_verdict"], baseline)
        self.assertEqual(row["candidate_verdict"], candidate)
        self.assertTrue(row["reason"])
        self.assertIsInstance(json.loads(row["first_problem"]), dict)

    def test_healthy_run_has_one_complete_verdict_with_manifest_provenance(self):
        row = self.verdict()
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(row["verdict"], "PASS")
        self.assertEqual(row["baseline_verdict"], "PASS")
        self.assertEqual(row["candidate_verdict"], "PASS")
        self.assertEqual(row["required_assertions"], 14)
        self.assertEqual(row["passed_assertions"], 14)
        for count in ("failed_assertions", "blocked_assertions", "missing_assertions",
                      "duplicate_assertion_keys", "unexpected_assertions", "invalid_assertions"):
            self.assertEqual(row[count], 0, count)
        self.assertIsNone(row["first_problem"])
        self.assertEqual((row["scenario_id"], row["seed"], row["baseline_sha"],
                          row["candidate_sha"], row["tariff_hash"]),
                         ("smoke-scenario-v1", 42, "baseline-sha", "candidate-sha", "tariff-hash"))
        evidence = json.loads(row["evidence"])
        self.assertIsInstance(evidence["baseline"], dict)
        self.assertIsInstance(evidence["candidate"], dict)
        self.assertTrue(row["evaluated_at"])

    def test_missing_one_required_check_fails_with_reference(self):
        self.db.execute("DELETE FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'amount_match'")
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["required_assertions"], 14)
        self.assertEqual(row["passed_assertions"], 13)
        self.assertEqual(row["missing_assertions"], 1)
        problem = json.loads(row["first_problem"])
        self.assertEqual(problem["release_role"], "candidate")
        self.assertEqual(problem["session_id"], "txn-smoke-v1")
        self.assertEqual(problem["assertion_id"], "amount_match")

    def test_entire_missing_release_cannot_pass_from_other_release(self):
        self.db.execute("DELETE FROM assertion_result WHERE release_role = 'candidate'")
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["required_assertions"], 14)
        self.assertEqual(row["passed_assertions"], 7)
        self.assertEqual(row["missing_assertions"], 7)

    def test_missing_whole_session_is_discovered_from_silver(self):
        self.fixture.add_oracle(session="second-session")
        row = self.verdict()
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["required_assertions"], 28)
        self.assertEqual(row["missing_assertions"], 14)
        self.assertEqual(json.loads(row["first_problem"])["session_id"], "second-session")

    def test_completed_session_without_expected_ledger_still_requires_checks(self):
        self.db.execute("INSERT INTO session_lifecycle SELECT run_id, 'second-session', started_at, ended_at, status FROM session_lifecycle")
        row = self.verdict()
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["required_assertions"], 28)
        self.assertEqual(row["missing_assertions"], 14)

    def test_duplicate_pass_does_not_replace_missing_required_check(self):
        self.db.execute("DELETE FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'amount_match'")
        self.db.execute("INSERT INTO assertion_result SELECT * FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'energy_match'")
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["required_assertions"], 14)
        self.assertEqual(row["passed_assertions"], 14)
        self.assertEqual(row["duplicate_assertion_keys"], 1)
        self.assertEqual(row["missing_assertions"], 1)

    def test_duplicate_key_with_conflicting_status_cannot_pass(self):
        self.db.execute("INSERT INTO assertion_result SELECT run_id, release_role, session_id, assertion_id, expected_value, actual_value, difference, 'FAIL', severity, message, evidence FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'amount_match'")
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["duplicate_assertion_keys"], 1)
        self.assertEqual(row["failed_assertions"], 1)

    def test_unknown_rule_is_rejected_even_when_required_checks_pass(self):
        self.db.execute("INSERT INTO assertion_result SELECT run_id, release_role, session_id, 'unregistered_rule', expected_value, actual_value, difference, status, severity, message, evidence FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'amount_match'")
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["passed_assertions"], 15)
        self.assertEqual(row["unexpected_assertions"], 1)

    def test_unknown_role_is_not_silently_discarded(self):
        self.db.execute("INSERT INTO assertion_result SELECT run_id, 'unknown', session_id, assertion_id, expected_value, actual_value, difference, status, severity, message, evidence FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'amount_match'")
        row = self.verdict()
        self.assertEqual(row["verdict"], "FAIL")
        self.assertEqual(row["unexpected_assertions"], 1)
        self.assertEqual(row["passed_assertions"], 15)
        self.assertEqual(json.loads(row["first_problem"])["release_role"], "unknown")

    def test_unknown_or_null_status_fails_closed(self):
        for status in ("UNKNOWN", "pass", "", None):
            with self.subTest(status=status):
                self.db.execute("UPDATE assertion_result SET status = ? WHERE release_role = 'candidate' AND assertion_id = 'amount_match'", (status,))
                row = self.verdict()
                self.assert_failed(row)
                self.assertEqual(row["invalid_assertions"], 1)
                self.assertEqual(row["missing_assertions"], 0)
                self.assertEqual(row["passed_assertions"], 13)

    def test_unexpected_assertion_session_cannot_expand_required_keys(self):
        self.db.execute("INSERT INTO assertion_result SELECT run_id, release_role, 'invented-session', assertion_id, expected_value, actual_value, difference, status, severity, message, evidence FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'amount_match'")
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["required_assertions"], 14)
        self.assertEqual(row["unexpected_assertions"], 1)

    def test_actual_only_session_requires_its_own_checks_and_cannot_pass(self):
        self.fixture.add_actual(session="unexpected-session", cdr_id="unexpected-cdr")
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["required_assertions"], 21)
        self.assertEqual(row["missing_assertions"], 7)
        self.refresh_assertions()
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["missing_assertions"], 0)
        self.assertEqual(row["failed_assertions"], 1)
        self.assertEqual(row["blocked_assertions"], 6)

    def test_manifest_without_sessions_fails_instead_of_vacuous_pass(self):
        self.add_manifest(run="empty-run")
        row = self.verdict(run="empty-run")
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["required_assertions"], 0)
        self.assertEqual(row["passed_assertions"], 0)

    def test_orphan_data_without_manifest_fails_both_roles(self):
        self.db.execute("DELETE FROM run_manifest")
        row = self.verdict()
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["passed_assertions"], 14)
        self.assertIsNone(row["baseline_sha"])
        self.assertIsNone(row["candidate_sha"])

    def test_orphan_assertion_run_is_not_omitted(self):
        self.db.execute("INSERT INTO assertion_result SELECT 'orphan-run', release_role, session_id, assertion_id, expected_value, actual_value, difference, status, severity, message, evidence FROM assertion_result WHERE release_role = 'candidate' AND assertion_id = 'amount_match'")
        row = self.verdict(run="orphan-run")
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["unexpected_assertions"], 1)
        self.assertEqual(row["required_assertions"], 0)

    def test_duplicate_manifest_fails_both_roles_without_multiplying_counts(self):
        self.db.execute("INSERT INTO run_manifest SELECT * FROM run_manifest")
        row = self.verdict()
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["required_assertions"], 14)
        self.assertEqual(row["passed_assertions"], 14)

    def test_baseline_failure_blocks_release_even_when_candidate_passes(self):
        self.db.execute("UPDATE actual_ledger SET actual_amount = 6.5 WHERE release_role = 'baseline'")
        self.refresh_assertions()
        row = self.verdict()
        self.assert_failed(row, baseline="FAIL", candidate="PASS")
        self.assertEqual(row["failed_assertions"], 1)
        self.assertEqual(json.loads(row["first_problem"])["release_role"], "baseline")

    def test_matching_bad_releases_fail_the_independent_oracle(self):
        self.db.execute("UPDATE actual_ledger SET actual_amount = 6.5")
        self.refresh_assertions()
        row = self.verdict()
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["failed_assertions"], 2)
        self.assertEqual(row["passed_assertions"], 12)

    def test_blocked_checks_are_counted_and_fail_closed(self):
        self.db.execute("DELETE FROM actual_ledger WHERE release_role = 'candidate'")
        self.refresh_assertions()
        row = self.verdict()
        self.assert_failed(row)
        self.assertEqual(row["failed_assertions"], 1)
        self.assertEqual(row["blocked_assertions"], 5)
        self.assertEqual(row["passed_assertions"], 8)
        self.assertEqual(row["missing_assertions"], 0)

    def test_corrected_candidate_passes_same_manifest_and_oracle(self):
        original_manifest = [tuple(row) for row in self.db.execute("SELECT * FROM run_manifest")]
        original_oracle = [tuple(row) for row in self.db.execute("SELECT * FROM expected_ledger")]
        self.db.execute("UPDATE actual_ledger SET actual_amount = 6.5 WHERE release_role = 'candidate'")
        self.refresh_assertions()
        self.assert_failed(self.verdict())
        self.db.execute("UPDATE actual_ledger SET actual_amount = 5.63 WHERE release_role = 'candidate'")
        self.refresh_assertions()
        row = self.verdict()
        self.assertEqual(row["verdict"], "PASS")
        self.assertEqual(row["passed_assertions"], 14)
        self.assertIsNone(row["first_problem"])
        self.assertEqual([tuple(row) for row in self.db.execute("SELECT * FROM run_manifest")], original_manifest)
        self.assertEqual([tuple(row) for row in self.db.execute("SELECT * FROM expected_ledger")], original_oracle)

    def test_zero_assertions_fail_even_with_valid_manifest_and_silver(self):
        self.db.execute("DELETE FROM assertion_result")
        row = self.verdict()
        self.assert_failed(row, baseline="FAIL")
        self.assertEqual(row["required_assertions"], 14)
        self.assertEqual(row["missing_assertions"], 14)
        self.assertEqual(row["passed_assertions"], 0)

    def test_runs_are_isolated_and_each_has_one_verdict(self):
        self.add_manifest(run="other-run")
        self.fixture.add_oracle(run="other-run")
        self.fixture.add_actual(run="other-run", role="baseline")
        self.fixture.add_actual(run="other-run", role="candidate", amount=6.5)
        self.refresh_assertions()
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(self.verdict()["verdict"], "PASS")
        other = self.verdict(run="other-run")
        self.assert_failed(other)
        self.assertEqual(other["failed_assertions"], 1)
        self.assertEqual(other["required_assertions"], 14)


if __name__ == "__main__":
    unittest.main()
