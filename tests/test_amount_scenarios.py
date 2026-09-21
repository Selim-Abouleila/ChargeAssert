"""Replay the production fixture SELECTs and executable mock through SQL adapters.

These checks execute the new Bronze seed queries/guards, session aggregation,
expected-charge queries/guards, actual normalization, assertions and verdicts.
JSON parsing into typed tariff/CDR fields is adapted in Python. Insert-only MERGE
is emulated by its declared keys; this is NOT a Delta MERGE test. SQLite numbers
are not Spark DECIMALs, and these tests do not prove Databricks DDL/type behavior.
The complete Databricks job and its fixture assertions still need to succeed.
"""

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unittest

import test_actual_ledger as actual_fixture
import test_release_verdict as verdict_fixture
from notebooks.mock_billing import generate_mock_runs


ROOT = Path(__file__).resolve().parents[1]
SEED_PATH = ROOT / "sql/11_seed_amount_scenarios.sql"
EXPECTED_PATH = ROOT / "sql/06_create_expected_ledger.sql"
VERDICT_PATH = ROOT / "sql/10_create_release_verdict.sql"
TABLES = {
    "run_manifest_table_name": "run_manifest",
    "raw_events_table_name": "raw_events",
    "raw_tariffs_table_name": "raw_tariffs",
    "raw_cdrs_table_name": "raw_cdrs",
    "session_lifecycle_table_name": "session_lifecycle",
    "tariff_history_table_name": "tariff_history",
    "assertion_result_table_name": "assertion_result",
}
MOCK_RUNS = ("mock-amount-bad-v1", "mock-amount-fixed-v1")
ALL_RUNS = {"smoke-run-v1", "amount-bad-v1", "amount-fixed-v1", *MOCK_RUNS}


def statements(sql):
    """Split statements without treating quoted messages/comments as SQL."""
    sql = re.sub(r"'(?:''|[^'])*'|--[^\n]*", lambda match:
                 "" if match[0].startswith("--") else match[0], sql)
    start = 0
    for match in re.finditer(r"'(?:''|[^'])*'|;", sql):
        if match[0] == ";":
            if sql[start:match.start()].strip():
                yield sql[start:match.start()].strip()
            start = match.end()
    if sql[start:].strip():
        yield sql[start:].strip()


def adapt_query(sql):
    for parameter, table in TABLES.items():
        sql = sql.replace(f"IDENTIFIER(:{parameter})", table)
    sql = sql.replace(" AS TIMESTAMP)", " AS TEXT)")
    sql = sql.replace(" AS STRING)", " AS TEXT)").replace("try_cast(", "CAST(")
    sql = re.sub(r"try_element_at\((\w+)\.price_components, 1\)\.(\w+)",
                 r"json_extract(\1.price_components, '$[0].\2')", sql)
    sql = re.sub(r"size\((\w+)\.price_components\)",
                 r"json_array_length(\1.price_components)", sql)

    def values_table(match):
        # SQLite's VALUES columns are named column1, column2, etc. Spark instead
        # permits naming them directly in the following table alias.
        fields = [field.strip() for field in match[3].split(",")]
        projection = ", ".join(f"column{i} AS {name}" for i, name in enumerate(fields, 1))
        return f"FROM (SELECT {projection} FROM (VALUES {match[1]})) AS {match[2]}"

    return re.sub(r"FROM VALUES\s+(.*?)\s+AS (\w+)\s*\(([\w,\s]+)\)",
                  values_table, sql, flags=re.DOTALL)


def source_query(sql):
    return adapt_query(sql.split("USING (\n", 1)[1].split("\n) AS source", 1)[0])


def get_json_object(body, path):
    if body is None:
        return None
    value = json.loads(body)
    for name, index in re.findall(r"\.([^\.\[\]]+)|\[(\d+)\]", path[1:]):
        if name:
            if not isinstance(value, dict):
                return None
            value = value.get(name)
        else:
            if not isinstance(value, list) or int(index) >= len(value):
                return None
            value = value[int(index)]
        if value is None:
            return None
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


class CountIf:
    def __init__(self):
        self.value = 0

    def step(self, value):
        self.value += bool(value)

    def finalize(self):
        return self.value


class AmountScenarioTests(unittest.TestCase):
    def setUp(self):
        self.fixture = verdict_fixture.ReleaseVerdictTests()
        self.fixture.setUp()
        self.db = self.fixture.db
        for table in ("run_manifest", "expected_ledger", "session_lifecycle",
                      "actual_ledger", "assertion_result"):
            self.db.execute(f"DELETE FROM {table}")
        self.db.execute("ALTER TABLE session_lifecycle ADD COLUMN meter_start_wh INTEGER")
        self.db.execute("ALTER TABLE session_lifecycle ADD COLUMN meter_end_wh INTEGER")
        self.db.execute("ALTER TABLE expected_ledger ADD COLUMN price_per_kwh NUMERIC")
        self.db.execute("ALTER TABLE expected_ledger ADD COLUMN expected_amount_unrounded NUMERIC")
        self.db.execute("""CREATE TABLE tariff_history (
            run_id TEXT, tariff_id TEXT, valid_from TEXT, valid_to TEXT,
            currency TEXT, price_components TEXT, source_payload_hash TEXT
        )""")
        self.db.create_aggregate("count_if", 1, CountIf)
        self.db.create_function("sha1", 1, lambda value: hashlib.sha1(value.encode()).hexdigest())
        self.db.create_function("sha2", 2, lambda value, bits: hashlib.sha256(value.encode()).hexdigest())
        self.db.create_function("get_json_object", 2, get_json_object)
        self.guard_error = None

        def assert_true(condition, message):
            if condition != 1:
                self.guard_error = message
                raise ValueError(message)
            return None

        self.db.create_function("assert_true", 2, assert_true)
        self.load_smoke()
        self.run_seed()
        self.load_mock()
        self.refresh_silver()

    def tearDown(self):
        self.fixture.tearDown()

    def insert_rows(self, table, rows):
        def bind(value):
            if isinstance(value, Decimal):
                return str(value)
            if isinstance(value, datetime):
                return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            return value

        columns = [row[1] for row in self.db.execute(f"PRAGMA table_info({table})")]
        for row in rows:
            fields = [field for field in columns if field in row]
            self.db.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                            tuple(bind(row[field]) for field in fields))

    def query_rows(self, query):
        return [dict(row) for row in self.db.execute(query).fetchall()]

    def load_smoke(self):
        for filename, table in (("01_create_run_manifest.sql", "run_manifest"),
                                ("02_create_ocpp_transaction_events_raw.sql", "raw_events"),
                                ("04_create_tariffs_raw.sql", "raw_tariffs"),
                                ("07_create_ocpi_cdrs_raw.sql", "raw_cdrs")):
            query = source_query((ROOT / "sql" / filename).read_text(encoding="utf-8"))
            if table != "run_manifest":
                self.db.execute(f"CREATE TABLE {table} AS SELECT * FROM ({query}) WHERE 0")
            self.insert_rows(table, self.query_rows(query))

    def run_seed(self):
        for statement in statements(SEED_PATH.read_text(encoding="utf-8")):
            if not statement.startswith("MERGE INTO"):
                self.db.execute(adapt_query(statement)).fetchall()
                continue
            parameter = re.match(r"MERGE INTO IDENTIFIER\(:(\w+)\)", statement)[1]
            table = TABLES[parameter]
            # Use the production ON key, not a separately invented dedupe rule.
            tail = statement.split("\n) AS source", 1)[1]
            on_clause = tail.split("WHEN NOT MATCHED", 1)[0]
            keys = re.findall(r"target\.(\w+) = source\.\1", on_clause)
            self.assertTrue(keys)
            self.assertNotIn("WHEN MATCHED", tail)
            rows = self.query_rows(source_query(statement))
            for row in rows:
                exists = self.db.execute(f"SELECT 1 FROM {table} WHERE " +
                                         " AND ".join(f"{key} = ?" for key in keys),
                                         tuple(row[key] for key in keys)).fetchone()
                if exists is None:
                    self.insert_rows(table, [row])

    def load_mock(self):
        """Run actual billing code, then feed its raw rows into the SQL adapters."""
        generated = generate_mock_runs(
            self.query_rows("SELECT * FROM raw_events WHERE run_id = 'smoke-run-v1'"),
            self.query_rows("SELECT * FROM raw_tariffs WHERE run_id = 'smoke-run-v1'")[0],
            self.query_rows("SELECT * FROM run_manifest WHERE run_id = 'smoke-run-v1'")[0],
        )
        table_names = {"run_manifest": "run_manifest", "ocpp_transaction_events_raw": "raw_events",
                       "tariffs_raw": "raw_tariffs", "ocpi_cdrs_raw": "raw_cdrs"}
        for logical_table, rows in generated.items():
            self.insert_rows(table_names[logical_table], rows)

    def check_oracle_inputs(self):
        for statement in statements(EXPECTED_PATH.read_text(encoding="utf-8")):
            if statement.startswith("MERGE INTO"):
                break
            if "assert_true(" in statement:
                self.db.execute(adapt_query(statement)).fetchall()

    def refresh_expected(self):
        self.check_oracle_inputs()
        self.db.execute("DELETE FROM expected_ledger")
        query = source_query(EXPECTED_PATH.read_text(encoding="utf-8"))
        self.insert_rows("expected_ledger", self.query_rows(query))

    def refresh_silver(self):
        self.db.execute("DELETE FROM session_lifecycle")
        query = source_query((ROOT / "sql/03_create_session_lifecycle.sql").read_text(encoding="utf-8"))
        self.insert_rows("session_lifecycle", self.query_rows(query))
        self.db.execute("DELETE FROM tariff_history")
        for tariff in self.query_rows("SELECT * FROM raw_tariffs"):
            # Adapt from_json's typed tariff projection; this does not replace or
            # claim to test SQL05's Spark parser and tariff-validation behavior.
            tariff["price_components"] = json.dumps(json.loads(tariff["payload"])["elements"][0]["price_components"])
            tariff["source_payload_hash"] = tariff["payload_hash"]
            self.insert_rows("tariff_history", [tariff])
        self.refresh_expected()
        normalization = actual_fixture.ActualLedgerTests()
        normalization.setUp()
        try:
            for raw in self.query_rows("SELECT * FROM raw_cdrs"):
                normalization.add(run=raw["run_id"], role=raw["release_role"], body=raw["payload"],
                                  corrupt_hash=raw["payload_hash"] != hashlib.sha256(raw["payload"].encode()).hexdigest())
            self.db.execute("DELETE FROM actual_ledger")
            self.insert_rows("actual_ledger", normalization.rows())
        finally:
            normalization.tearDown()
        self.fixture.refresh_assertions()

    def test_production_pipeline_detects_bad_amount_and_accepts_fix_and_smoke(self):
        verdicts = {row["run_id"]: row for row in self.fixture.rows()}
        self.assertEqual(set(verdicts), ALL_RUNS)
        for run in ("amount-bad-v1", "mock-amount-bad-v1"):
            bad = verdicts[run]
            self.assertEqual((bad["baseline_verdict"], bad["candidate_verdict"], bad["verdict"]), ("PASS", "FAIL", "FAIL"))
            self.assertEqual((bad["required_assertions"], bad["passed_assertions"], bad["failed_assertions"]), (14, 13, 1))
            self.assertEqual(json.loads(bad["first_problem"])["assertion_id"], "amount_match")
        for run in ("smoke-run-v1", "amount-fixed-v1", "mock-amount-fixed-v1"):
            self.assertEqual((verdicts[run]["verdict"], verdicts[run]["passed_assertions"], verdicts[run]["failed_assertions"]), ("PASS", 14, 0))
        failures = self.query_rows("SELECT * FROM assertion_result WHERE status <> 'PASS'")
        self.assertEqual({(row["run_id"], row["release_role"], row["assertion_id"]) for row in failures},
                         {("amount-bad-v1", "candidate", "amount_match"),
                          ("mock-amount-bad-v1", "candidate", "amount_match")})
        for failure in failures:
            self.assertAlmostEqual(failure["difference"], 0.87)
        for row in verdicts.values():
            for count in ("blocked_assertions", "missing_assertions", "duplicate_assertion_keys",
                          "unexpected_assertions", "invalid_assertions"):
                self.assertEqual(row[count], 0, count)

    def test_production_postguards_accept_both_pairs_and_require_each_mock_run(self):
        self.db.execute(f"CREATE TABLE release_verdict AS {verdict_fixture.source_query()}")
        guard_queries = []
        for path, table in ((EXPECTED_PATH, "expected_ledger"), (VERDICT_PATH, "release_verdict")):
            after_merge = False
            for statement in statements(path.read_text(encoding="utf-8")):
                if statement.startswith("MERGE INTO"):
                    after_merge = True
                elif after_merge and "assert_true(" in statement:
                    query = adapt_query(statement).replace("IDENTIFIER(:table_name)", table)
                    # SQLite subtraction is binary floating point; normalize to
                    # the declared six decimals before exact guard comparison.
                    query = query.replace("difference = CAST(", "round(difference, 6) = CAST(")
                    self.db.execute(query).fetchall()
                    guard_queries.append(query)
        for run in MOCK_RUNS:
            with self.subTest(run=run):
                self.db.execute("SAVEPOINT missing_mock")
                self.db.execute("DELETE FROM release_verdict WHERE run_id = ?", (run,))
                with self.assertRaises(sqlite3.OperationalError):
                    for query in guard_queries:
                        self.db.execute(query).fetchall()
                self.db.execute("ROLLBACK TO missing_mock")
                self.db.execute("RELEASE missing_mock")

    def test_mock_pair_replays_identical_inputs_and_independent_oracle(self):
        for table in ("raw_events", "raw_tariffs", "session_lifecycle", "expected_ledger"):
            by_run = []
            for run in MOCK_RUNS:
                rows = self.query_rows(f"SELECT * FROM {table} WHERE run_id = '{run}'")
                for row in rows:
                    row.pop("run_id")
                by_run.append(rows)
            self.assertTrue(by_run[0], table)
            self.assertEqual(*by_run, table)
        rows = self.query_rows("SELECT * FROM raw_cdrs WHERE run_id LIKE 'mock-%' ORDER BY run_id, release_role")
        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertEqual(row["cdr_id"], "cdr-mock-v1")
            self.assertEqual(row["payload_hash"], hashlib.sha256(row["payload"].encode()).hexdigest())
            bad = (row["run_id"], row["release_role"]) == ("mock-amount-bad-v1", "candidate")
            self.assertEqual(row["total_cost"], 6.5 if bad else 5.63)

    def test_mock_and_oracle_recalculate_changed_energy_separately(self):
        event = self.query_rows("SELECT * FROM raw_events WHERE run_id = 'smoke-run-v1' AND event_type = 'Ended'")[0]
        payload = json.loads(event["payload"])
        payload["meterValue"][0]["sampledValue"][0]["value"] = 113000
        body = json.dumps(payload, separators=(",", ":"))
        self.db.execute("UPDATE raw_events SET payload = ?, payload_hash = ? WHERE run_id = 'smoke-run-v1' AND event_type = 'Ended'",
                        (body, hashlib.sha256(body.encode()).hexdigest()))
        for table in ("run_manifest", "raw_events", "raw_tariffs", "raw_cdrs"):
            self.db.execute(f"DELETE FROM {table} WHERE run_id LIKE 'mock-%'")
        self.load_mock()
        self.refresh_silver()
        for run in MOCK_RUNS:
            expected = self.query_rows(f"SELECT expected_energy_kwh, expected_amount FROM expected_ledger WHERE run_id = '{run}'")[0]
            self.assertEqual((expected["expected_energy_kwh"], expected["expected_amount"]), (13, 5.85))
            checks = self.query_rows(f"SELECT * FROM assertion_result WHERE run_id = '{run}' AND assertion_id = 'amount_match' ORDER BY release_role")
            self.assertEqual(checks[0]["status"], "PASS")
            self.assertAlmostEqual(float(checks[0]["actual_value"]), 5.85)
            self.assertAlmostEqual(float(checks[1]["actual_value"]), 6.72 if run.endswith("bad-v1") else 5.85)
        self.assertEqual(self.fixture.verdict(run=MOCK_RUNS[0])["verdict"], "FAIL")
        self.assertEqual(self.fixture.verdict(run=MOCK_RUNS[1])["verdict"], "PASS")

    def test_paired_runs_have_identical_inputs_and_independent_expectations(self):
        for table in ("raw_events", "raw_tariffs", "session_lifecycle", "expected_ledger"):
            bad = self.query_rows(f"SELECT * FROM {table} WHERE run_id = 'amount-bad-v1'")
            fixed = self.query_rows(f"SELECT * FROM {table} WHERE run_id = 'amount-fixed-v1'")
            for rows in (bad, fixed):
                for row in rows:
                    row.pop("run_id")
            self.assertEqual(bad, fixed, table)
        manifests = self.query_rows("SELECT * FROM run_manifest WHERE run_id LIKE 'amount-%' ORDER BY run_id")
        self.assertNotEqual(manifests[0]["candidate_sha"], manifests[1]["candidate_sha"])
        for manifest in manifests:
            run = manifest.pop("run_id")
            self.assertEqual(manifest.pop("candidate_sha"), hashlib.sha1(f"candidate-{run}".encode()).hexdigest())
            self.assertEqual(manifest["scenario_id"], "amount-mismatch-v1")
            tariff = self.query_rows(f"SELECT payload_hash FROM raw_tariffs WHERE run_id = '{run}'")[0]
            self.assertEqual(manifest["tariff_hash"], tariff["payload_hash"])
        self.assertEqual(manifests[0], manifests[1])

    def test_only_bad_candidate_payload_amount_changes_and_hash_is_recomputed(self):
        raw = self.query_rows("SELECT * FROM raw_cdrs WHERE run_id NOT LIKE 'mock-%' ORDER BY run_id, release_role")
        self.assertEqual(len(raw), 6)
        healthy_body = next(row["payload"] for row in raw if row["run_id"] == "smoke-run-v1")
        for row in raw:
            bad = (row["run_id"], row["release_role"]) == ("amount-bad-v1", "candidate")
            self.assertEqual(row["payload"], healthy_body.replace('"excl_vat":5.63', '"excl_vat":6.50') if bad else healthy_body)
            self.assertEqual(row["payload_hash"], hashlib.sha256(row["payload"].encode()).hexdigest())
            self.assertEqual(row["total_cost"], 6.5 if bad else 5.63)

    def test_repeated_insert_only_seed_preserves_cardinality_and_bad_evidence(self):
        tables = ("run_manifest", "raw_events", "raw_tariffs", "raw_cdrs")
        original = {table: self.query_rows(f"SELECT * FROM {table}") for table in tables}
        self.run_seed()
        self.run_seed()
        self.assertEqual([len(original[table]) for table in tables], [5, 15, 5, 10])
        for table in tables:
            self.assertEqual(self.query_rows(f"SELECT * FROM {table}"), original[table])
        self.refresh_silver()
        self.assertEqual(self.fixture.verdict(run="amount-bad-v1")["verdict"], "FAIL")
        self.assertEqual(self.fixture.verdict(run="amount-fixed-v1")["verdict"], "PASS")

    def test_oracle_recalculates_from_meter_and_tariff_inputs_not_reported_charge(self):
        self.db.execute("UPDATE session_lifecycle SET meter_end_wh = 113000 WHERE run_id = 'amount-bad-v1'")
        self.refresh_expected()
        row = self.query_rows("SELECT * FROM expected_ledger WHERE run_id = 'amount-bad-v1'")[0]
        self.assertEqual((row["expected_energy_kwh"], row["expected_amount"]), (13, 5.85))
        self.db.execute("UPDATE tariff_history SET price_components = ? WHERE run_id = 'amount-bad-v1'",
                        (json.dumps([{"type": "ENERGY", "price": 0.5, "step_size": 1}]),))
        self.refresh_expected()
        row = self.query_rows("SELECT * FROM expected_ledger WHERE run_id = 'amount-bad-v1'")[0]
        self.assertEqual((row["expected_energy_kwh"], row["expected_amount"]), (13, 6.5))
        self.db.execute("UPDATE actual_ledger SET actual_amount = 99 WHERE run_id = 'amount-bad-v1'")
        self.refresh_expected()
        self.assertEqual(self.query_rows("SELECT expected_amount FROM expected_ledger WHERE run_id = 'amount-bad-v1'")[0]["expected_amount"], 6.5)
        self.assertEqual(self.query_rows("SELECT expected_amount FROM expected_ledger WHERE run_id = 'amount-fixed-v1'")[0]["expected_amount"], 5.63)

    def test_oracle_guards_reject_missing_duplicate_and_invalid_sessions(self):
        mutations = (
            "DELETE FROM session_lifecycle WHERE run_id = 'amount-bad-v1'",
            "DELETE FROM session_lifecycle WHERE run_id = 'mock-amount-bad-v1'",
            "INSERT INTO session_lifecycle SELECT * FROM session_lifecycle WHERE run_id = 'amount-bad-v1'",
            "UPDATE session_lifecycle SET status = 'In Progress' WHERE run_id = 'amount-bad-v1'",
            "UPDATE session_lifecycle SET ended_at = NULL WHERE run_id = 'amount-bad-v1'",
            "UPDATE session_lifecycle SET ended_at = '2026-08-22T09:00:00Z' WHERE run_id = 'amount-bad-v1'",
            "UPDATE session_lifecycle SET meter_start_wh = -1 WHERE run_id = 'amount-bad-v1'",
            "UPDATE session_lifecycle SET meter_end_wh = 99999 WHERE run_id = 'amount-bad-v1'",
        )
        self.assert_rejected_mutations(mutations, self.check_oracle_inputs, "completed session")

    def test_oracle_guards_reject_missing_ambiguous_and_unsupported_tariffs(self):
        mutations = [
            "DELETE FROM tariff_history WHERE run_id = 'amount-fixed-v1'",
            "DELETE FROM tariff_history WHERE run_id = 'mock-amount-fixed-v1'",
            "INSERT INTO tariff_history SELECT * FROM tariff_history WHERE run_id = 'amount-fixed-v1'",
            "UPDATE tariff_history SET valid_from = '2026-08-22T10:01:00Z' WHERE run_id = 'amount-fixed-v1'",
            "UPDATE tariff_history SET valid_to = '2026-08-22T10:30:00Z' WHERE run_id = 'amount-fixed-v1'",
            "UPDATE tariff_history SET currency = 'USD' WHERE run_id = 'amount-fixed-v1'",
            "UPDATE tariff_history SET source_payload_hash = NULL WHERE run_id = 'amount-fixed-v1'",
        ]
        for component in ({"type": "TIME", "price": 0.45, "step_size": 1},
                          {"type": "ENERGY", "price": -1, "step_size": 1},
                          {"type": "ENERGY", "price": 0.45, "step_size": 2}):
            mutations.append("UPDATE tariff_history SET price_components = '" + json.dumps([component]) + "' WHERE run_id = 'amount-fixed-v1'")
        self.assert_rejected_mutations(mutations, self.check_oracle_inputs, "flat EUR ENERGY tariff")

    def test_seed_guards_reject_drift_and_preserve_existing_conflicting_evidence(self):
        mutations = (
            "DELETE FROM raw_events WHERE run_id = 'smoke-run-v1' AND event_type = 'Ended'",
            "UPDATE raw_cdrs SET payload_hash = 'broken' WHERE run_id = 'smoke-run-v1'",
            "UPDATE run_manifest SET candidate_sha = 'different' WHERE run_id = 'amount-fixed-v1'",
            "UPDATE raw_cdrs SET total_cost = 1 WHERE run_id = 'amount-bad-v1' AND release_role = 'candidate'",
        )
        self.assert_rejected_mutations(mutations, self.run_seed, "Amount scenario")

    def assert_rejected_mutations(self, mutations, action, error):
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.db.execute("SAVEPOINT invalid")
                self.db.execute(mutation)
                self.guard_error = None
                with self.assertRaises(sqlite3.OperationalError):
                    action()
                self.assertIn(error, self.guard_error)
                self.db.execute("ROLLBACK TO invalid")
                self.db.execute("RELEASE invalid")


if __name__ == "__main__":
    unittest.main()
