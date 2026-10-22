"""Run dashboard SELECTs with SQLite JSON adapters and actual captured Gold fixtures.

Tests cover execution selection, receipt/snapshot safety and saved bill details.
Spark JSON/DECIMAL parsing and dashboard rendering still need Databricks validation.
"""
from copy import deepcopy
import json
from pathlib import Path
import re
import sqlite3
import unittest

import test_generated_capture as captured_fixture

ROOT = Path(__file__).resolve().parents[1]
SQL = {name: (ROOT / "sql/dashboard" / (name + ".sql")).read_text(encoding="utf-8")
       for name in ("execution_summary", "session_bills", "execution_choices")}


def safe_array(value):
    try:
        rows = json.loads(value)
        return json.dumps(rows) if isinstance(rows, list) else "[]"
    except (TypeError, ValueError):
        return "[]"


def snapshot_field(value, field):
    try:
        row = json.loads(value)
        return row.get(field) if isinstance(row, dict) else None
    except (TypeError, ValueError):
        return None


def adapt(sql):
    sql = sql.replace("chargeassert_dev_bronze.generated_job_execution", "receipts_table")
    sql = sql.replace("chargeassert_dev_gold.generated_execution_verdict", "snapshots_table")
    sql = re.sub(r"parsed_assertions AS \(.*?\n\), assertions AS \(",
                 """parsed_assertions AS (
  SELECT c.execution_id, j.value AS a
  FROM context c, json_each(CASE WHEN c.receipt_usable
    THEN safe_array(c.assertions_snapshot) ELSE '[]' END) j
), assertions AS (""", sql, flags=re.DOTALL)
    # Only the from_json/explode output projection differs from Spark SQL.
    start = sql.find(", assertions AS (")
    end = sql.find(", session_results AS (")
    if start >= 0:
        projection = re.sub(r"a\.([a-z_]+)", r"snapshot_field(a, '\1')", sql[start:end])
        sql = sql[:start] + projection + sql[end:]
    return re.sub(r"TRY_CAST\((s\.[a-z_]+) AS DECIMAL\(38,6\)\)",
                  r"CAST(\1 AS NUMERIC)", sql)


class DashboardQueryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = captured_fixture.GeneratedCaptureTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("safe_array", 1, safe_array)
        self.db.create_function("snapshot_field", 2, snapshot_field)
        self.db.execute("""CREATE TABLE receipts_table (
          execution_id TEXT, batch_id TEXT, candidate_mode TEXT, pipeline_hash TEXT,
          status TEXT, reason TEXT, run_id TEXT, started_at TEXT, finished_at TEXT)""")
        self.db.execute("""CREATE TABLE snapshots_table (
          execution_id TEXT, run_id TEXT, pipeline_hash TEXT, session_count INTEGER,
          assertion_count INTEGER, baseline_verdict TEXT, candidate_verdict TEXT,
          verdict TEXT, assertions_snapshot TEXT, captured_at TEXT)""")
        self.save_attempt()

    def save_attempt(self, execution="job:run:0", status="SUCCEEDED", started="2026-10-15T10:00:00Z"):
        captured = self.fixture.capture()
        self.db.execute("INSERT INTO receipts_table VALUES (?,?,?,?,?,?,?,?,?)",
                        (execution, "batch-one", "healthy", "code-hash", status,
                         "Captured all checks." if status == "SUCCEEDED" else "Attempt " + status,
                         captured_fixture.RUN, started,
                         None if status == "RUNNING" else started))
        if status == "SUCCEEDED":
            self.db.execute("INSERT INTO snapshots_table VALUES (?,?,?,?,?,?,?,?,?,?)",
                            (execution, captured_fixture.RUN, "code-hash", captured["session_count"],
                             captured["assertion_count"], captured["baseline_verdict"],
                             captured["candidate_verdict"], captured["verdict"],
                             json.dumps(captured["assertions"]), started))

    def rows(self, name, execution=""):
        return [dict(row) for row in self.db.execute(adapt(SQL[name]), {"execution_id": execution})]

    def summary(self, execution=""):
        rows = self.rows("execution_summary", execution)
        self.assertEqual(len(rows), 1)
        return rows[0]

    def assert_blocked(self, execution=""):
        result = self.summary(execution)
        self.assertFalse(result["usable"])
        for field in ("verdict", "baseline_verdict", "candidate_verdict"):
            self.assertEqual(result[field], "BLOCKED")
        for field in ("session_count", "assertion_count", "passed_checks", "failed_checks", "blocked_checks"):
            self.assertIsNone(result[field])
        self.assertEqual(self.rows("session_bills", execution), [])
        return result

    def test_healthy_snapshot_has_summary_and_four_saved_bills(self):
        result = self.summary()
        self.assertEqual((result["session_count"], result["assertion_count"], result["passed_checks"],
                          result["failed_checks"], result["blocked_checks"]), (2, 28, 28, 0, 0))
        self.assertEqual((result["evaluation_status"], result["verdict"]), ("SUCCEEDED", "PASS"))
        bills = self.rows("session_bills")
        self.assertEqual(len(bills), 4)
        for bill in bills:
            self.assertEqual((bill["currency"], bill["amount_status"], bill["verdict"]), ("EUR", "PASS", "PASS"))
            self.assertEqual(bill["expected_amount"], bill["actual_amount"])
            self.assertEqual(bill["difference"], 0)

    def test_bad_candidate_bills_are_financial_failures_with_successful_evaluation(self):
        self.fixture.db.execute("UPDATE actual_ledger SET actual_amount = actual_amount + 0.87 WHERE release_role = 'candidate'")
        self.fixture.evaluate_gold()
        self.save_attempt("job:bad:0", started="2026-10-15T11:00:00Z")
        result = self.summary("latest")
        self.assertTrue(result["usable"])
        self.assertEqual((result["evaluation_status"], result["baseline_verdict"], result["candidate_verdict"],
                          result["verdict"], result["passed_checks"], result["failed_checks"]),
                         ("SUCCEEDED", "PASS", "FAIL", "FAIL", 26, 2))
        bills = self.rows("session_bills", "latest")
        self.assertEqual([bill["release_role"] for bill in bills[:2]], ["candidate", "candidate"])
        for bill in bills[:2]:
            self.assertAlmostEqual(bill["difference"], 0.87)
            self.assertEqual((bill["amount_status"], bill["verdict"]), ("FAIL", "FAIL"))

    def test_latest_failed_or_running_attempt_never_reuses_older_pass(self):
        for status in ("FAILED", "RUNNING"):
            with self.subTest(status=status):
                self.db.execute("SAVEPOINT scenario")
                self.save_attempt("job:new:0", status=status, started="2026-10-15T12:00:00Z")
                result = self.assert_blocked("latest")
                self.assertEqual((result["execution_id"], result["evaluation_status"]), ("job:new:0", status))
                self.assertEqual(self.summary("job:run:0")["verdict"], "PASS")
                self.db.execute("ROLLBACK TO scenario")
                self.db.execute("RELEASE scenario")

    def test_missing_explicit_attempt_and_empty_history_return_one_blocked_row(self):
        self.assertEqual(self.assert_blocked("missing")["evaluation_status"], "MISSING")
        self.db.execute("DELETE FROM receipts_table")
        self.db.execute("DELETE FROM snapshots_table")
        self.assertEqual(self.assert_blocked()["evaluation_status"], "MISSING")

    def test_missing_duplicate_or_mismatched_evidence_is_blocked(self):
        statements = (
            "DELETE FROM snapshots_table",
            "INSERT INTO receipts_table SELECT * FROM receipts_table",
            "INSERT INTO snapshots_table SELECT * FROM snapshots_table",
            "UPDATE snapshots_table SET run_id = 'another-run'",
            "UPDATE snapshots_table SET pipeline_hash = 'another-code'",
            "UPDATE snapshots_table SET assertion_count = 14",
            "UPDATE snapshots_table SET session_count = 0",
            "UPDATE snapshots_table SET candidate_verdict = 'FAIL'",
            "UPDATE snapshots_table SET verdict = 'FAIL'",
            "UPDATE snapshots_table SET assertions_snapshot = 'malformed-json'",
            "UPDATE snapshots_table SET assertions_snapshot = '[]'",
        )
        for statement in statements:
            with self.subTest(statement=statement):
                self.db.execute("SAVEPOINT scenario")
                self.db.execute(statement)
                self.assert_blocked()
                self.db.execute("ROLLBACK TO scenario")
                self.db.execute("RELEASE scenario")

    def test_saved_assertion_coverage_cannot_hide_duplicate_missing_or_unknown_checks(self):
        original = self.fixture.capture()["assertions"]
        cases = []
        duplicate = deepcopy(original)
        duplicate[1] = deepcopy(duplicate[0])
        cases.append(duplicate)
        cases.append(deepcopy(original[:-1]))
        for field, value in (("run_id", "other-run"), ("session_id", ""), ("release_role", "other"),
                             ("assertion_id", "invented"), ("status", "UNKNOWN")):
            changed = deepcopy(original)
            changed[0][field] = value
            cases.append(changed)
        for assertions in cases:
            with self.subTest(assertions=assertions[0]):
                self.db.execute("UPDATE snapshots_table SET assertions_snapshot = ?", (json.dumps(assertions),))
                self.assert_blocked()

    def test_missing_cdr_keeps_blocked_checks_and_failed_financial_verdict(self):
        self.fixture.db.execute("DELETE FROM actual_ledger WHERE release_role = 'candidate' AND session_id = ?",
                                (captured_fixture.SESSIONS[1],))
        self.fixture.evaluate_gold()
        self.save_attempt("job:missing-cdr:0", started="2026-10-15T13:00:00Z")
        result = self.summary()
        self.assertTrue(result["usable"])
        self.assertEqual((result["verdict"], result["passed_checks"], result["failed_checks"],
                          result["blocked_checks"]), ("FAIL", 22, 1, 5))
        failed = [row for row in self.rows("session_bills") if row["verdict"] == "FAIL"]
        self.assertEqual(len(failed), 1)
        self.assertIsNone(failed[0]["actual_amount"])

    def test_execution_choices_include_failed_and_running_receipts(self):
        self.save_attempt("job:failed:0", status="FAILED")
        self.save_attempt("job:running:0", status="RUNNING")
        self.assertEqual({row["execution_id"] for row in self.rows("execution_choices")},
                         {"latest", "job:run:0", "job:failed:0", "job:running:0"})

    def test_summary_and_bills_share_the_same_selection_and_safety_gate(self):
        common_summary = SQL["execution_summary"].split("\nSELECT execution_id, batch_id", 1)[0]
        common_detail = SQL["session_bills"].split("\nSELECT s.execution_id", 1)[0]
        self.assertEqual(common_summary, common_detail)
        for name, query in SQL.items():
            self.assertNotIn("workspace.", query)
            self.assertNotIn("generated_assertion_result", query)
            self.assertNotIn("generated_actual_ledger", query)


if __name__ == "__main__":
    unittest.main()
