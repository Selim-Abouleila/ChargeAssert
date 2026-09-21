"""Exercise attempt publication and the actual SQL consumption policy.

The financial rows come from the existing production SQL SELECT adapters. The
tracking helpers run directly as Python, and the consumption query runs against
SQLite with identifier/string-type adapters only. These tests do not execute
Databricks task dynamic references, Spark schemas, or Delta transactions.
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import unittest

import test_amount_scenarios as amount_fixture
from notebooks import execution_tracking as tracking


ROOT = Path(__file__).resolve().parents[1]
START = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
EVALUATED = START + timedelta(minutes=2)


def registration(run="101", *, repair=0, status="RUNNING", started_at=START):
    return {
        "execution_id": tracking.execution_identity("7", run, repair),
        "job_id": "7", "job_run_id": str(run), "repair_count": repair,
        "status": status,
        "expected_run_ids_json": tracking.canonical_json(list(tracking.EXPECTED_RUN_IDS)),
        "started_at": started_at, "finished_at": None, "task_states_json": None,
        "reason": "Execution registered; waiting for all required tasks.",
    }


def successful_states(keys):
    return {key: "success" for key in keys}


class ExecutionFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reuse the full five-run pipeline, not handwritten PASS fixtures. This
        # includes two intentionally failing candidates and their actual checks.
        fixture = amount_fixture.AmountScenarioTests()
        fixture.setUp()
        try:
            cls.production_verdicts = fixture.fixture.rows()
            cls.production_assertions = fixture.query_rows("SELECT * FROM assertion_result")
        finally:
            fixture.tearDown()

    def setUp(self):
        self.verdicts = deepcopy(self.production_verdicts)
        self.assertions = deepcopy(self.production_assertions)
        for row in self.verdicts:
            row["evaluated_at"] = EVALUATED
        self.registration = registration()
        self.capture_states = successful_states(tracking.CAPTURE_TASK_KEYS)
        self.finish_states = successful_states(tracking.FINISH_TASK_KEYS)

    def capture(self, registration_row=None):
        return tracking.validate_capture(
            self.registration if registration_row is None else registration_row,
            self.verdicts, self.assertions, self.capture_states,
        )

    def assert_cannot_finish(self, registration_row, snapshots, states=None, repair=0):
        status, reason = tracking.evaluate_finish(
            registration_row, snapshots,
            self.finish_states if states is None else states, repair,
        )
        self.assertEqual(status, "FAILED")
        self.assertTrue(reason)


class ExecutionTrackingTests(ExecutionFixture):
    def test_execution_identity_distinguishes_job_run_and_repair(self):
        identities = {
            tracking.execution_identity("7", "101", "0"),
            tracking.execution_identity("7", "102", "0"),
            tracking.execution_identity("8", "101", "0"),
            tracking.execution_identity("7", "101", "1"),
        }
        self.assertEqual(len(identities), 4)
        self.assertEqual(tracking.execution_identity("7", "101", "0"), "7:101:0")

    def test_execution_identity_rejects_missing_or_unresolved_job_context(self):
        for values in (("", "101", "0"), ("7", "", "0"),
                       ("7", "101", "{{job.repair_count}}"),
                       ("{{job.id}}", "101", "0"), ("7", "101", "-1")):
            with self.subTest(values=values), self.assertRaises(ValueError):
                tracking.execution_identity(*values)

    def test_complete_snapshot_preserves_real_financial_failures_and_succeeds(self):
        snapshots = self.capture()
        self.assertEqual({row["run_id"] for row in snapshots}, set(tracking.EXPECTED_RUN_IDS))
        self.assertEqual(len(snapshots), 5)
        self.assertEqual({row["execution_id"] for row in snapshots}, {"7:101:0"})
        verdicts = {row["run_id"]: row["verdict"] for row in snapshots}
        self.assertEqual(verdicts["mock-amount-bad-v1"], "FAIL")
        self.assertEqual(verdicts["mock-amount-fixed-v1"], "PASS")
        self.assertEqual(tracking.evaluate_finish(
            self.registration, snapshots, self.finish_states, 0,
        )[0], "SUCCEEDED")
        for row in snapshots:
            assertions = json.loads(row["assertions_snapshot"])
            self.assertEqual(len(assertions), 14)
            self.assertEqual({item["run_id"] for item in assertions}, {row["run_id"]})
            self.assertEqual(json.loads(row["verdict_snapshot"])["run_id"], row["run_id"])

    def test_every_required_task_needs_explicit_success(self):
        snapshots = self.capture()
        for key in tracking.CAPTURE_TASK_KEYS:
            for state in ("failed", "canceled", "excluded", "upstream_failed", "", None,
                          "{{tasks.some_task.result_state}}", "SUCCESS"):
                with self.subTest(key=key, state=state):
                    states = {**self.capture_states, key: state}
                    with self.assertRaises(ValueError):
                        tracking.validate_capture(self.registration, self.verdicts, self.assertions, states)
            with self.subTest(key=key, state="missing"):
                states = dict(self.capture_states)
                del states[key]
                with self.assertRaises(ValueError):
                    tracking.validate_capture(self.registration, self.verdicts, self.assertions, states)
        for key in tracking.FINISH_TASK_KEYS:
            with self.subTest(finish_key=key):
                self.assert_cannot_finish(self.registration, snapshots,
                                         {**self.finish_states, key: "failed"})

    def test_partial_missing_duplicate_and_foreign_verdicts_cannot_publish(self):
        alternatives = (
            [], self.verdicts[:-1], self.verdicts + self.verdicts[:1],
            self.verdicts + [{**self.verdicts[0], "run_id": "unregistered-run"}],
        )
        for rows in alternatives:
            with self.subTest(run_ids=[row["run_id"] for row in rows]), self.assertRaises(ValueError):
                tracking.validate_capture(self.registration, rows, self.assertions, self.capture_states)

    def test_missing_duplicate_and_foreign_assertions_cannot_publish(self):
        alternatives = (
            [], self.assertions[:-1], self.assertions + self.assertions[:1],
            self.assertions + [{**self.assertions[0], "run_id": "unregistered-run"}],
            [{**row, "assertion_id": "unknown-rule"} if i == 0 else row
             for i, row in enumerate(self.assertions)],
        )
        for rows in alternatives:
            with self.subTest(count=len(rows)), self.assertRaises(ValueError):
                tracking.validate_capture(self.registration, self.verdicts, rows, self.capture_states)

    def test_stale_gold_from_before_registration_cannot_publish(self):
        for timestamp in (START - timedelta(seconds=1), None):
            rows = [{**row, "evaluated_at": timestamp} for row in self.verdicts]
            with self.subTest(timestamp=timestamp), self.assertRaises(ValueError):
                tracking.validate_capture(self.registration, rows, self.assertions, self.capture_states)

    def test_missing_start_partial_publication_and_failed_capture_never_succeed(self):
        snapshots = self.capture()
        self.assert_cannot_finish(None, snapshots)
        for rows in ([], snapshots[:-1], snapshots + snapshots[:1]):
            with self.subTest(count=len(rows)):
                self.assert_cannot_finish(self.registration, rows)
        for state in ("failed", "excluded", "canceled", None, ""):
            with self.subTest(capture=state):
                self.assert_cannot_finish(self.registration, snapshots,
                                         {**self.finish_states, "capture_execution": state})

    def test_repair_attempt_does_not_reuse_successful_earlier_attempt(self):
        snapshots = self.capture()
        self.assert_cannot_finish(self.registration, snapshots, repair=1)
        self.assert_cannot_finish(registration(repair=1), snapshots, repair=1)

    def test_idempotent_capture_is_order_independent_and_freezes_full_evidence(self):
        first = self.capture()
        self.verdicts.reverse()
        self.assertions.reverse()
        second = self.capture()
        tracking.verify_snapshots(first, second, require_complete=True)
        tracking.verify_snapshots([], first)
        tracking.verify_snapshots(first[:2], first)
        with self.assertRaises(ValueError):
            tracking.verify_snapshots(first[:2], first, require_complete=True)

        original = deepcopy(first)
        for field, value in (("verdict", "BLOCKED"), ("assertions_snapshot", "[]"),
                             ("verdict_snapshot", "{}"), ("reason", "changed evidence")):
            with self.subTest(field=field):
                changed = [{**first[0], field: value}, *first[1:]]
                with self.assertRaises(ValueError):
                    tracking.verify_snapshots(changed, first)
        self.assertEqual(first, original)

    def test_foreign_and_duplicate_snapshot_keys_cannot_be_reused(self):
        snapshots = self.capture()
        alternatives = (
            snapshots + snapshots[:1],
            [{**snapshots[0], "execution_id": "7:100:0"}, *snapshots[1:]],
            [{**snapshots[0], "run_id": "unregistered-run"}, *snapshots[1:]],
        )
        for rows in alternatives:
            with self.subTest(count=len(rows)), self.assertRaises(ValueError):
                tracking.verify_snapshots(rows, snapshots)

    def test_assertion_and_verdict_content_disagreement_is_rejected(self):
        assertions = deepcopy(self.assertions)
        target = next(row for row in assertions if row["run_id"] == "mock-amount-fixed-v1"
                      and row["release_role"] == "candidate" and row["assertion_id"] == "amount_match")
        target["status"] = "FAIL"
        with self.assertRaises(ValueError):
            tracking.validate_capture(self.registration, self.verdicts, assertions, self.capture_states)

    def test_claimed_pass_cannot_override_preserved_failing_checks(self):
        verdicts = deepcopy(self.verdicts)
        bad = next(row for row in verdicts if row["run_id"] == "mock-amount-bad-v1")
        bad.update(baseline_verdict="PASS", candidate_verdict="PASS", verdict="PASS")
        with self.assertRaises(ValueError):
            tracking.validate_capture(self.registration, verdicts, self.assertions, self.capture_states)

    def test_registered_inventory_and_failed_status_cannot_be_replaced_on_retry(self):
        snapshots = self.capture()
        failed = {**self.registration, "status": "FAILED"}
        self.assert_cannot_finish(failed, snapshots)
        with self.assertRaises(ValueError):
            tracking.validate_capture(failed, self.verdicts, self.assertions, self.capture_states)
        for changed in (
            {**self.registration, "expected_run_ids_json": "[]"},
            {**self.registration, "execution_id": "7:999:0"},
            {**self.registration, "started_at": None},
        ):
            self.assert_cannot_finish(changed, snapshots)

    def test_timestamp_round_trip_accepts_same_instants_from_spark(self):
        self.registration["started_at"] = START.replace(tzinfo=None)
        for row in self.verdicts:
            row["evaluated_at"] = EVALUATED.astimezone(timezone(timedelta(hours=2)))
        snapshots = self.capture()
        self.assertEqual(tracking.evaluate_finish(
            self.registration, snapshots, self.finish_states, 0,
        )[0], "SUCCEEDED")

    def test_finish_revalidates_captured_evidence_before_publishing_success(self):
        snapshots = self.capture()
        for field, value in (("verdict_snapshot", "{}"), ("assertions_snapshot", "[]"),
                             ("verdict", "PASS"), ("passed_assertions", 14)):
            changed = deepcopy(snapshots)
            bad = next(row for row in changed if row["run_id"] == "mock-amount-bad-v1")
            bad[field] = value
            with self.subTest(field=field):
                self.assert_cannot_finish(self.registration, changed)


class ExecutionConsumptionQueryTests(ExecutionFixture):
    """Read actual production SQL, including unknown IDs anchored to one row."""

    def setUp(self):
        super().setUp()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.create_function("concat", -1, lambda *items: "".join(str(item) for item in items))
        self.db.executescript("""
            CREATE TABLE job_execution (execution_id TEXT, status TEXT, reason TEXT);
            CREATE TABLE execution_verdict (
                execution_id TEXT, run_id TEXT, baseline_verdict TEXT,
                candidate_verdict TEXT, verdict TEXT, reason TEXT,
                required_assertions INTEGER, passed_assertions INTEGER,
                failed_assertions INTEGER, blocked_assertions INTEGER,
                missing_assertions INTEGER, duplicate_assertion_keys INTEGER,
                unexpected_assertions INTEGER, invalid_assertions INTEGER
            );
        """)
        self.query = (ROOT / "sql/12_check_execution.sql").read_text(encoding="utf-8")
        self.query = self.query.replace("IDENTIFIER(:job_execution_table_name)", "job_execution")
        self.query = self.query.replace("IDENTIFIER(:execution_verdict_table_name)", "execution_verdict")
        self.query = self.query.replace(" AS STRING)", " AS TEXT)")

    def tearDown(self):
        self.db.close()

    def store(self, registration_row, snapshots, *, states=None):
        status, reason = tracking.evaluate_finish(
            registration_row, snapshots, self.finish_states if states is None else states, 0,
        )
        self.db.execute("INSERT INTO job_execution VALUES (?,?,?)",
                        (registration_row["execution_id"], status, reason))
        fields = ("execution_id", "run_id", "baseline_verdict", "candidate_verdict",
                  "verdict", "reason", *tracking.COUNT_FIELDS)
        placeholders = ",".join("?" for _ in fields)
        self.db.executemany(f"INSERT INTO execution_verdict ({','.join(fields)}) VALUES ({placeholders})",
                            [tuple(row[field] for field in fields) for row in snapshots])
        return status

    def check(self, execution_id, run="mock-amount-fixed-v1"):
        rows = self.db.execute(self.query, {"execution_id": execution_id, "run_id": run}).fetchall()
        self.assertEqual(len(rows), 1)
        return dict(rows[0])

    def test_A_pass_B_fails_before_gold_C_pass_cannot_reuse_prior_pass(self):
        first = self.capture()
        self.assertEqual(self.store(self.registration, first), "SUCCEEDED")
        self.assertEqual(self.check("7:101:0")["verdict"], "PASS")

        # B starts later than A's Gold evaluation. Gold still contains A's PASS;
        # downstream publication must stop, and the read must not fall back to A.
        second = registration("102", started_at=EVALUATED + timedelta(minutes=1))
        failed_tasks = {**self.finish_states, "create_assertion_result": "failed",
                        "create_release_verdict": "upstream_failed", "capture_execution": "excluded"}
        with self.assertRaises(ValueError):
            tracking.validate_capture(second, self.verdicts, self.assertions,
                                      {key: failed_tasks[key] for key in tracking.CAPTURE_TASK_KEYS})
        self.assertEqual(self.store(second, [], states=failed_tasks), "FAILED")
        blocked = self.check("7:102:0")
        self.assertEqual((blocked["execution_status"], blocked["verdict"]), ("FAILED", "BLOCKED"))
        self.assertIsNone(blocked["passed_assertions"])
        self.assertEqual(self.check("7:101:0")["verdict"], "PASS")

        third = registration("103", started_at=EVALUATED + timedelta(minutes=2))
        for row in self.verdicts:
            row["evaluated_at"] = EVALUATED + timedelta(minutes=3)
        self.assertEqual(self.store(third, self.capture(third)), "SUCCEEDED")
        self.assertEqual(self.check("7:103:0")["verdict"], "PASS")
        self.assertEqual(self.check("7:102:0")["verdict"], "BLOCKED")

    def test_unknown_execution_or_unknown_scenario_returns_one_blocked_row(self):
        self.store(self.registration, self.capture())
        missing = self.check("7:999:0")
        self.assertEqual((missing["execution_status"], missing["verdict"]), ("MISSING", "BLOCKED"))
        self.assertEqual(self.check("7:101:0", "unknown-scenario")["verdict"], "BLOCKED")

    def test_running_failed_or_unrecognized_state_hides_even_present_pass_snapshot(self):
        self.store(self.registration, self.capture())
        for state in ("RUNNING", "FAILED", "success", "CANCELLED", None):
            with self.subTest(state=state):
                self.db.execute("UPDATE job_execution SET status = ?", (state,))
                row = self.check("7:101:0")
                self.assertEqual(row["verdict"], "BLOCKED")
                self.assertIsNone(row["baseline_verdict"])
                self.assertIsNone(row["candidate_verdict"])

    def test_duplicate_registration_or_snapshot_blocks_instead_of_selecting_pass(self):
        self.store(self.registration, self.capture())
        for table in ("job_execution", "execution_verdict"):
            with self.subTest(table=table):
                self.db.execute("SAVEPOINT duplicate_record")
                self.db.execute(f"INSERT INTO {table} SELECT * FROM {table}")
                self.assertEqual(self.check("7:101:0")["verdict"], "BLOCKED")
                self.db.execute("ROLLBACK TO duplicate_record")
                self.db.execute("RELEASE duplicate_record")

    def test_completed_execution_preserves_financial_fail_and_corruption_is_blocked(self):
        self.store(self.registration, self.capture())
        bad = self.check("7:101:0", "mock-amount-bad-v1")
        self.assertEqual((bad["execution_status"], bad["baseline_verdict"],
                          bad["candidate_verdict"], bad["verdict"]),
                         ("SUCCEEDED", "PASS", "FAIL", "FAIL"))
        self.db.execute("UPDATE execution_verdict SET verdict = 'PASS' WHERE run_id = 'mock-amount-bad-v1'")
        self.assertEqual(self.check("7:101:0", "mock-amount-bad-v1")["verdict"], "BLOCKED")

    def test_all_pass_labels_still_block_when_any_coverage_count_is_invalid(self):
        self.store(self.registration, self.capture())
        for field, value in (
            ("required_assertions", 0), ("required_assertions", None),
            ("passed_assertions", 13), ("passed_assertions", None),
            ("failed_assertions", 1), ("blocked_assertions", 1),
            ("missing_assertions", 1), ("duplicate_assertion_keys", 1),
            ("unexpected_assertions", 1), ("invalid_assertions", 1),
        ):
            with self.subTest(field=field, value=value):
                self.db.execute("SAVEPOINT inconsistent_count")
                self.db.execute(f"UPDATE execution_verdict SET {field} = ? "
                                "WHERE run_id = 'mock-amount-fixed-v1'", (value,))
                self.assertEqual(self.check("7:101:0")["verdict"], "BLOCKED")
                self.db.execute("ROLLBACK TO inconsistent_count")
                self.db.execute("RELEASE inconsistent_count")

    def test_production_pre_gold_failure_guard_stops_before_changing_existing_gold(self):
        sql = (ROOT / "sql/09_create_assertion_result.sql").read_text(encoding="utf-8")
        guard = next(amount_fixture.statements(sql))
        self.assertTrue(guard.startswith("SELECT assert_true("),
                        "The deliberate failure must happen before any Gold DDL or MERGE.")
        self.db.executescript("""
            CREATE TABLE assertion_result (run_id TEXT, release_role TEXT, assertion_id TEXT, status TEXT);
            CREATE TABLE release_verdict (run_id TEXT, verdict TEXT);
        """)
        self.db.executemany("INSERT INTO assertion_result VALUES (?,?,?,?)", [
            tuple(row[field] for field in ("run_id", "release_role", "assertion_id", "status"))
            for row in self.assertions
        ])
        self.db.executemany("INSERT INTO release_verdict VALUES (?,?)", [
            (row["run_id"], row["verdict"]) for row in self.verdicts
        ])

        def assert_true(condition, message):
            if condition != 1:
                raise ValueError(message)

        self.db.create_function("assert_true", 2, assert_true)
        before = {table: self.db.execute(f"SELECT * FROM {table}").fetchall()
                  for table in ("assertion_result", "release_verdict")}
        self.db.execute(guard, {"fail_before_gold": "false"}).fetchall()
        for value in ("true", "FALSE", "invalid", "", None):
            with self.subTest(value=value):
                with self.assertRaises(sqlite3.OperationalError):
                    self.db.execute(guard, {"fail_before_gold": value}).fetchall()
                for table, original in before.items():
                    self.assertEqual(self.db.execute(f"SELECT * FROM {table}").fetchall(), original)


if __name__ == "__main__":
    unittest.main()
