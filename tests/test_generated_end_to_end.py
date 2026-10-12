"""Exercise actual generated files through mock billing, SQL rules and capture.

The production generated SELECTs run with the existing SQLite function adapters.
Only Spark's typed tariff/CDR JSON projections are replaced by Python parsing;
prepared CDR payloads are used unchanged. Table writes emulate keyed insert and
run-scoped replacement. This does not validate Auto Loader, Spark DECIMAL type
analysis, from_json, Delta MERGE, job orchestration or runtime execution receipts.
"""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import unittest
from unittest.mock import patch

import test_actual_ledger as actual_fixture
import test_amount_scenarios as amount_fixture
import test_assertion_result as assertion_fixture
from test_generated_sql import SQL, adapt, source
from notebooks.generated_batch import prepare_batch
from notebooks.generated_capture import capture_result
from notebooks.session_generator import generate_batch


PIPELINE_HASH = hashlib.sha256(b"local generated SQL integration test").hexdigest()
ARRIVAL = datetime(2026, 9, 30, 12)


class GeneratedEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.fixture = amount_fixture.AmountScenarioTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.db = self.fixture.db
        self.db.create_function("decimal_product", 2, lambda left, right:
                                format(Decimal(str(left)) * Decimal(str(right)), "f"))
        self.db.execute("CREATE TABLE input_sessions (run_id TEXT, session_id TEXT, tariff_id TEXT)")
        self.db.execute("CREATE TABLE release_verdict AS SELECT * FROM (" + adapt(source(SQL["10"]))
                        + ") WHERE 0", {"run_id": "not-loaded"})

    def prepare(self, batch="e2e", count=2, mode="healthy", seed=42):
        lines = generate_batch(batch, count, seed).splitlines()
        landed = [{
            "raw_record": line, "record_hash": hashlib.sha256(line.encode("utf-8")).hexdigest(),
            "ingested_at": ARRIVAL, "source_file_path": f"/incoming/generated-{batch}.jsonl",
        } for line in lines]
        prepared = prepare_batch(landed, batch, mode, PIPELINE_HASH)
        self.assertEqual(len(lines), count * 3)
        return prepared

    def rows(self, query, run_id):
        return [dict(row) for row in self.db.execute(query, {"run_id": run_id})]

    def insert_once(self, table, rows, key_fields):
        # Emulate insert-only key handling for retries; this is not a Delta test.
        where = " AND ".join(f"{field} = ?" for field in key_fields)
        for row in rows:
            exists = self.db.execute(f"SELECT 1 FROM {table} WHERE {where}",
                                     tuple(row[field] for field in key_fields)).fetchone()
            if exists is None:
                self.fixture.insert_rows(table, [row])

    def replace_run(self, table, rows, run_id):
        self.db.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))
        self.fixture.insert_rows(table, rows)

    def evaluate(self, prepared):
        run_id = prepared["run_id"]
        tables = prepared["tables"]
        for source_table, destination, key in (
            ("run_manifest", "run_manifest", ("run_id",)),
            ("input_session", "input_sessions", ("run_id", "session_id")),
            ("ocpp_transaction_events_raw", "raw_events", ("run_id", "event_id")),
            ("tariffs_raw", "raw_tariffs", ("run_id", "tariff_id")),
            ("ocpi_cdrs_raw", "raw_cdrs", ("run_id", "release_role", "payload_hash")),
        ):
            self.insert_once(destination, tables[source_table], key)
        sessions = self.rows(adapt(source(SQL["03"])), run_id)
        self.replace_run("session_lifecycle", sessions, run_id)

        # Substitute only SQL05's Spark typed JSON projection, using the actual
        # prepared raw tariff payload/hash and effective timestamps.
        tariffs = []
        for raw in self.rows("SELECT * FROM raw_tariffs WHERE run_id = :run_id", run_id):
            body = json.loads(raw["payload"])
            tariffs.append({
                "run_id": raw["run_id"], "tariff_id": raw["tariff_id"],
                "valid_from": raw["valid_from"], "valid_to": raw["valid_to"],
                "currency": raw["currency"],
                "price_components": json.dumps(body["elements"][0]["price_components"]),
                "source_payload_hash": raw["payload_hash"],
            })
        self.replace_run("tariff_history", tariffs, run_id)
        for statement in amount_fixture.statements(SQL["06"]):
            if statement.startswith("MERGE INTO"):
                break
            if "assert_true(" in statement:
                self.rows(adapt(statement), run_id)
        expected = self.rows(adapt(source(SQL["06"])), run_id)
        self.replace_run("expected_ledger", expected, run_id)

        normalization = actual_fixture.ActualLedgerTests()
        normalization.setUp()
        try:
            for raw in self.rows("SELECT * FROM raw_cdrs WHERE run_id = :run_id", run_id):
                # No fixture CDR generator: these exact bytes come from the
                # real generated_batch.prepare_batch billing calculation.
                normalization.add(run=raw["run_id"], role=raw["release_role"], body=raw["payload"],
                                  corrupt_hash=raw["payload_hash"] != hashlib.sha256(raw["payload"].encode()).hexdigest())
            with patch.object(actual_fixture, "SQL", SQL["08"]):
                actual = [dict(row) for row in normalization.db.execute(
                    actual_fixture.source_query(), {"run_id": run_id})]
            self.replace_run("actual_ledger", actual, run_id)
        finally:
            normalization.tearDown()
        with patch.object(assertion_fixture, "SQL", SQL["09"]):
            assertions = self.rows(assertion_fixture.source_query(), run_id)
        self.replace_run("assertion_result", assertions, run_id)
        verdicts = self.rows(adapt(source(SQL["10"])), run_id)
        self.replace_run("release_verdict", verdicts, run_id)
        return capture_result(prepared, verdicts, assertions)

    def assert_financial_counts(self, result, *, sessions, passed, failed):
        self.assertEqual((result["session_count"], result["assertion_count"]), (sessions, sessions * 14))
        verdict = result["verdict_row"]
        self.assertEqual((verdict["required_assertions"], verdict["passed_assertions"],
                          verdict["failed_assertions"]), (sessions * 14, passed, failed))
        for field in ("blocked_assertions", "missing_assertions", "duplicate_assertion_keys",
                      "unexpected_assertions", "invalid_assertions"):
            self.assertEqual(verdict[field], 0, field)

    def test_actual_default_file_reaches_28_passing_checks_and_capture(self):
        prepared = self.prepare()
        result = self.evaluate(prepared)
        self.assert_financial_counts(result, sessions=2, passed=28, failed=0)
        self.assertEqual((result["baseline_verdict"], result["candidate_verdict"], result["verdict"]),
                         ("PASS", "PASS", "PASS"))
        self.assertEqual({row["status"] for row in result["assertions"]}, {"PASS"})
        self.assertEqual(result["verdict_row"]["tariff_hash"],
                         prepared["tables"]["tariffs_raw"][0]["payload_hash"])
        for field in ("run_id", "scenario_id", "seed", "baseline_sha", "candidate_sha", "tariff_hash"):
            self.assertEqual(result["verdict_row"][field], prepared["tables"]["run_manifest"][0][field])

    def test_actual_candidate_defect_reaches_26_pass_and_two_amount_failures(self):
        prepared = self.prepare(mode="amount_error")
        result = self.evaluate(prepared)
        self.assert_financial_counts(result, sessions=2, passed=26, failed=2)
        self.assertEqual((result["baseline_verdict"], result["candidate_verdict"], result["verdict"]),
                         ("PASS", "FAIL", "FAIL"))
        failures = [row for row in result["assertions"] if row["status"] == "FAIL"]
        self.assertEqual({(row["session_id"], row["release_role"], row["assertion_id"]) for row in failures},
                         {(session, "candidate", "amount_match") for session in prepared["session_ids"]})
        for row in failures:
            self.assertAlmostEqual(row["difference"], 0.87)

    def test_twenty_generated_sessions_round_half_cents_and_pass_all_280_checks(self):
        prepared = self.prepare(batch="e2e-ties", count=20)
        result = self.evaluate(prepared)
        self.assert_financial_counts(result, sessions=20, passed=280, failed=0)
        self.assertEqual(result["verdict"], "PASS")
        events = prepared["tables"]["ocpp_transaction_events_raw"]
        expected = {row["session_id"]: row for row in self.rows(
            "SELECT * FROM expected_ledger WHERE run_id = :run_id", prepared["run_id"])}
        reported = {(row["session_id"], row["release_role"]): json.loads(row["payload"], parse_float=Decimal)
                    for row in prepared["tables"]["ocpi_cdrs_raw"]}
        exact_ties = {}
        for session in prepared["session_ids"]:
            ordered = sorted((row for row in events if row["transaction_id"] == session),
                             key=lambda row: row["sequence_number"])
            meters = [json.loads(row["payload"])["meterValue"][0]["sampledValue"][0]["value"]
                      for row in ordered]
            unrounded = Decimal(meters[-1] - meters[0]) / Decimal(1000) * Decimal("0.45")
            rounded = unrounded.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            self.assertEqual(Decimal(str(expected[session]["expected_amount"])), rounded)
            for role in ("baseline", "candidate"):
                self.assertEqual(reported[(session, role)]["total_cost"]["excl_vat"], rounded)
            if unrounded % Decimal("0.01") == Decimal("0.005"):
                exact_ties[unrounded] = rounded
        self.assertEqual(exact_ties[Decimal("1.485")], Decimal("1.49"))
        self.assertEqual(exact_ties[Decimal("26.235")], Decimal("26.24"))
        self.assertEqual(exact_ties[Decimal("7.425")], Decimal("7.43"))

    def test_reprepare_and_replay_keep_run_identity_counts_and_financial_evidence(self):
        prepared = self.prepare()
        first = self.evaluate(prepared)
        replay = self.prepare()
        self.assertEqual(replay, prepared)
        second = self.evaluate(replay)
        self.assert_financial_counts(second, sessions=2, passed=28, failed=0)
        self.assertEqual(first["assertions"], second["assertions"])
        self.assertEqual(first["verdict"], second["verdict"])
        for table, count in (("run_manifest", 1), ("input_sessions", 2), ("raw_events", 6),
                             ("raw_tariffs", 1), ("raw_cdrs", 4), ("session_lifecycle", 2),
                             ("expected_ledger", 2), ("actual_ledger", 4),
                             ("assertion_result", 28), ("release_verdict", 1)):
            self.assertEqual(self.db.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = ?",
                                            (replay["run_id"],)).fetchone()[0], count, table)

    def test_healthy_and_faulty_share_inputs_but_keep_separate_gold_and_fixture_runs(self):
        original_fixture_verdicts = {row["run_id"]: row["verdict"] for row in self.fixture.fixture.rows()}
        healthy, faulty = self.prepare(), self.prepare(mode="amount_error")
        self.assertEqual(healthy["input_hash"], faulty["input_hash"])
        self.assertEqual(healthy["session_ids"], faulty["session_ids"])
        self.assertNotEqual(healthy["run_id"], faulty["run_id"])
        for table in ("input_session", "ocpp_transaction_events_raw", "tariffs_raw"):
            remove_run = lambda rows: [{key: value for key, value in row.items() if key != "run_id"}
                                       for row in rows]
            self.assertEqual(remove_run(healthy["tables"][table]), remove_run(faulty["tables"][table]))
        healthy_capture = self.evaluate(healthy)
        faulty_capture = self.evaluate(faulty)
        self.assertEqual((healthy_capture["verdict"], faulty_capture["verdict"]), ("PASS", "FAIL"))
        self.assertEqual(self.rows("SELECT * FROM release_verdict WHERE run_id = :run_id", healthy["run_id"]),
                         [healthy_capture["verdict_row"]])
        self.assertEqual(self.rows("SELECT * FROM release_verdict WHERE run_id = :run_id", faulty["run_id"]),
                         [faulty_capture["verdict_row"]])
        self.assertEqual({row["run_id"]: row["verdict"] for row in self.fixture.fixture.rows()},
                         original_fixture_verdicts)
        self.assertEqual(set(original_fixture_verdicts), amount_fixture.ALL_RUNS)
        # Capture enforces the chosen manifest/session identity; another mode's
        # otherwise complete Gold rows cannot be substituted for this run.
        with self.assertRaisesRegex(ValueError, "provenance"):
            capture_result(healthy, [faulty_capture["verdict_row"]], faulty_capture["assertions"])


if __name__ == "__main__":
    unittest.main()
