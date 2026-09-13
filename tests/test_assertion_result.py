"""Exercise the production Gold SELECT against healthy and faulty Silver inputs.

SQLite adapters provide Spark JSON/array/timestamp functions and string casts.
SQLite numeric arithmetic is NOT Databricks DECIMAL arithmetic. These tests check
rule logic, joins, cardinality and evidence, not Delta DDL/MERGE or Spark analysis.
The Databricks job's smoke assertions and workspace verification remain required.
"""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import sqlite3
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql/09_create_assertion_result.sql").read_text(encoding="utf-8")


def source_query():
    query = SQL.split("USING (\n", 1)[1].split("\n) AS source", 1)[0]
    for table in ("expected_ledger", "actual_ledger", "session_lifecycle"):
        query = query.replace(f"IDENTIFIER(:{table}_table_name)", table)
    return (query.replace(" AS STRING)", " AS TEXT)")
            .replace("timestampdiff(MICROSECOND,", "timestampdiff('MICROSECOND',")
            # SQLite stores fixture timestamps as text, not typed timestamps.
            .replace("ended_at >= started_at", "timestampdiff('MICROSECOND', started_at, ended_at) >= 0")
            # SQLite divides integers as integers; Spark DECIMAL division does not.
            .replace("CAST(3600000000 AS DECIMAL(10,0))", "3600000000.0"))


def named_struct(*args):
    fields = dict(zip(args[::2], args[1::2]))
    for key in ("actual_cdrs", "session", "expected_tariff", "source_payload_hashes"):
        if fields.get(key) is not None:
            fields[key] = json.loads(fields[key])
    return json.dumps(fields)


def sort_array(value):
    if value is None:
        return None
    values = json.loads(value)
    return json.dumps(sorted(values, key=lambda item: json.dumps(item)))


class CollectList:
    def __init__(self):
        self.values = []

    def step(self, value):
        if value is not None:
            self.values.append(json.loads(value))

    def finalize(self):
        return json.dumps(self.values)


def timestampdiff(unit, start, end):
    assert unit == "MICROSECOND"
    if start is None or end is None:
        return None
    elapsed = datetime.fromisoformat(end) - datetime.fromisoformat(start)
    return (elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds


class AssertionResultTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE expected_ledger (
                run_id TEXT, session_id TEXT, expected_energy_kwh NUMERIC,
                expected_amount NUMERIC, currency TEXT, tariff_id TEXT,
                tariff_valid_from TEXT, tariff_payload_hash TEXT
            );
            CREATE TABLE session_lifecycle (
                run_id TEXT, session_id TEXT, started_at TEXT, ended_at TEXT,
                status TEXT
            );
            CREATE TABLE actual_ledger (
                run_id TEXT, release_role TEXT, session_id TEXT,
                country_code TEXT, party_id TEXT, cdr_id TEXT, cdr_type TEXT,
                actual_energy_kwh NUMERIC, actual_duration_hours NUMERIC,
                actual_amount NUMERIC, currency TEXT, tariff_id TEXT,
                source_payload_hashes TEXT
            );
        """)
        self.db.create_function("named_struct", -1, named_struct)
        self.db.create_function("to_json", 1, lambda value: value)
        self.db.create_function("sort_array", 1, sort_array)
        self.db.create_function("array", 0, lambda: "[]")
        self.db.create_function("timestampdiff", 3, timestampdiff)
        # Spark uses HALF_UP on DECIMAL; SQLite's binary round differs at ties.
        self.db.create_function("round", 2, lambda value, scale: None if value is None else
                                float(Decimal(str(value)).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)))
        self.db.create_aggregate("collect_list", 1, CollectList)
        self.add_oracle()
        self.add_actual(role="baseline")
        self.add_actual(role="candidate")

    def tearDown(self):
        self.db.close()

    def add_oracle(self, run="smoke-run-v1", session="txn-smoke-v1"):
        self.db.execute("INSERT INTO expected_ledger VALUES (?,?,?,?,?,?,?,?)", (
            run, session, 12.5, 5.63, "EUR", "tariff-smoke-v1",
            "2026-01-01T00:00:00Z", "tariff-evidence-hash",
        ))
        self.db.execute("INSERT INTO session_lifecycle VALUES (?,?,?,?,?)", (
            run, session, "2026-08-22T10:00:00Z", "2026-08-22T11:00:00Z", "Completed",
        ))

    def add_actual(self, role="candidate", run="smoke-run-v1", session="txn-smoke-v1",
                   cdr_id="cdr-smoke-v1", amount=5.63, hashes=None):
        self.db.execute("INSERT INTO actual_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            run, role, session, "FR", "CAS", cdr_id, "FINAL", 12.5, 1,
            amount, "EUR", "tariff-smoke-v1", json.dumps(hashes or [f"hash-{cdr_id}"]),
        ))

    def rows(self):
        return [dict(row) for row in self.db.execute(source_query()).fetchall()]

    def rules(self, role="candidate", run="smoke-run-v1", session="txn-smoke-v1"):
        return {row["assertion_id"]: row for row in self.rows()
                if (row["run_id"], row["release_role"], row["session_id"]) == (run, role, session)}

    def test_healthy_fixture_has_fourteen_unique_passes(self):
        rows = self.rows()
        self.assertEqual(len(rows), 14)
        keys = {(r["run_id"], r["release_role"], r["session_id"], r["assertion_id"]) for r in rows}
        self.assertEqual(len(keys), 14)
        self.assertEqual({r["status"] for r in rows}, {"PASS"})
        amount = self.rules()["amount_match"]
        self.assertEqual(Decimal(amount["expected_value"]), Decimal("5.63"))
        self.assertEqual(Decimal(amount["actual_value"]), Decimal("5.63"))
        self.assertEqual(amount["difference"], 0)

    def test_missing_candidate_is_not_lost_in_join(self):
        self.db.execute("DELETE FROM actual_ledger WHERE release_role = 'candidate'")
        candidate = self.rules()
        self.assertEqual(candidate["oracle_available"]["status"], "PASS")
        self.assertEqual(candidate["final_cdr_count"]["status"], "FAIL")
        self.assertEqual(Decimal(candidate["final_cdr_count"]["actual_value"]), 0)
        self.assertEqual(candidate["final_cdr_count"]["difference"], -1)
        self.assertIn("MISSING_CDR", candidate["final_cdr_count"]["message"])
        for rule in ("energy_match", "duration_match", "currency_match", "tariff_match", "amount_match"):
            self.assertEqual(candidate[rule]["status"], "BLOCKED")
            self.assertIsNone(candidate[rule]["actual_value"])
            self.assertIsNone(candidate[rule]["difference"])
        self.assertTrue(all(r["status"] == "PASS" for r in self.rules(role="baseline").values()))

    def test_both_missing_releases_still_have_results(self):
        self.db.execute("DELETE FROM actual_ledger")
        rows = self.rows()
        self.assertEqual(len(rows), 14)
        self.assertEqual(sum(r["status"] == "FAIL" for r in rows), 2)
        self.assertEqual(json.loads(rows[0]["evidence"])["actual_cdrs"], [])

    def test_duplicate_cdrs_fail_even_if_amounts_sum_to_expectation(self):
        self.db.execute("UPDATE actual_ledger SET actual_amount = 2 WHERE release_role = 'candidate'")
        self.add_actual(cdr_id="second-cdr", amount=3.63)
        rules = self.rules()
        self.assertEqual(rules["final_cdr_count"]["status"], "FAIL")
        self.assertEqual(rules["final_cdr_count"]["difference"], 1)
        self.assertIn("DUPLICATE_CDR", rules["final_cdr_count"]["message"])
        self.assertEqual(rules["amount_match"]["status"], "BLOCKED")
        self.assertIsNone(rules["amount_match"]["actual_value"])
        self.assertIsNone(rules["amount_match"]["difference"])
        records = json.loads(rules["final_cdr_count"]["evidence"])["actual_cdrs"]
        self.assertEqual({r["cdr_id"] for r in records}, {"cdr-smoke-v1", "second-cdr"})

    def test_numeric_mismatches_keep_direction_and_precision(self):
        for field, value, rule, difference in [
            ("actual_energy_kwh", 12.4, "energy_match", -0.1),
            ("actual_duration_hours", 0.75, "duration_match", -0.25),
            ("actual_amount", 6.50, "amount_match", 0.87),
            ("actual_amount", 5.00, "amount_match", -0.63),
            ("actual_amount", 5.625, "amount_match", -0.005),
            ("actual_amount", 5.630001, "amount_match", 0.000001),
        ]:
            with self.subTest(field=field, value=value):
                self.db.execute("SAVEPOINT faulty")
                self.db.execute(f"UPDATE actual_ledger SET {field} = ? WHERE release_role = 'candidate'", (value,))
                result = self.rules()[rule]
                self.assertEqual(result["status"], "FAIL")
                self.assertAlmostEqual(result["difference"], difference, places=10)
                self.assertTrue(all(r["status"] == "PASS" for r in self.rules(role="baseline").values()))
                self.db.execute("ROLLBACK TO faulty")
                self.db.execute("RELEASE faulty")

    def test_currency_mismatch_blocks_monetary_comparison(self):
        self.db.execute("UPDATE actual_ledger SET currency = 'USD' WHERE release_role = 'candidate'")
        rules = self.rules()
        self.assertEqual(rules["currency_match"]["status"], "FAIL")
        self.assertEqual(rules["currency_match"]["actual_value"], "USD")
        self.assertEqual(rules["amount_match"]["status"], "BLOCKED")
        self.assertIsNone(rules["amount_match"]["difference"])
        self.assertEqual(Decimal(rules["amount_match"]["actual_value"]), Decimal("5.63"))

    def test_wrong_tariff_fails_even_when_amount_is_right(self):
        self.db.execute("UPDATE actual_ledger SET tariff_id = 'wrong-tariff' WHERE release_role = 'candidate'")
        self.assertEqual(self.rules()["tariff_match"]["status"], "FAIL")
        self.assertEqual(self.rules()["amount_match"]["status"], "PASS")

    def test_expected_tariff_id_uses_normalized_identity(self):
        self.db.execute("UPDATE expected_ledger SET tariff_id = 'TARIFF-SMOKE-V1'")
        self.assertEqual(self.rules()["tariff_match"]["status"], "PASS")

    def test_matching_bad_baseline_and_candidate_both_fail_oracle(self):
        self.db.execute("UPDATE actual_ledger SET actual_amount = 6.5")
        for role in ("baseline", "candidate"):
            self.assertEqual(self.rules(role=role)["amount_match"]["status"], "FAIL")

    def test_corrected_candidate_passes_same_independent_inputs(self):
        original = [tuple(r) for r in self.db.execute("SELECT * FROM expected_ledger")]
        healthy = self.rows()
        self.db.execute("UPDATE actual_ledger SET actual_amount = 6.5 WHERE release_role = 'candidate'")
        self.assertEqual(self.rules()["amount_match"]["status"], "FAIL")
        self.db.execute("UPDATE actual_ledger SET actual_amount = 5.63 WHERE release_role = 'candidate'")
        self.assertEqual(self.rows(), healthy)
        self.assertEqual([tuple(r) for r in self.db.execute("SELECT * FROM expected_ledger")], original)

    def test_completed_sessions_missing_oracle_fail_even_without_outputs(self):
        self.db.execute("DELETE FROM expected_ledger")
        self.db.execute("DELETE FROM actual_ledger")
        self.assertEqual(len(self.rows()), 14)
        for role in ("baseline", "candidate"):
            rules = self.rules(role=role)
            self.assertEqual(rules["oracle_available"]["status"], "FAIL")
            self.assertEqual(sum(r["status"] == "BLOCKED" for r in rules.values()), 6)

    def test_unexpected_candidate_session_is_reported(self):
        self.add_actual(session="unexpected-session", cdr_id="unexpected-cdr")
        rules = self.rules(session="unexpected-session")
        self.assertEqual(rules["oracle_available"]["status"], "FAIL")
        self.assertEqual(rules["amount_match"]["status"], "BLOCKED")
        evidence = json.loads(rules["oracle_available"]["evidence"])
        self.assertEqual(evidence["expected_rows"], 0)
        self.assertEqual(evidence["actual_cdrs"][0]["cdr_id"], "unexpected-cdr")
        self.assertEqual(self.rules(role="baseline", session="unexpected-session"), {})
        self.assertEqual(len(self.rows()), 21)

    def test_duplicate_oracle_or_lifecycle_cannot_multiply_or_pass_results(self):
        for table in ("expected_ledger", "session_lifecycle"):
            with self.subTest(table=table):
                self.db.execute("SAVEPOINT duplicate")
                self.db.execute(f"INSERT INTO {table} SELECT * FROM {table}")
                self.assertEqual(len(self.rows()), 14)
                self.assertEqual(self.rules()["oracle_available"]["status"], "FAIL")
                self.assertEqual(self.rules()["amount_match"]["status"], "BLOCKED")
                self.db.execute("ROLLBACK TO duplicate")
                self.db.execute("RELEASE duplicate")

    def test_missing_invalid_or_incomplete_session_blocks_comparison(self):
        for mutation in (
            "DELETE FROM session_lifecycle",
            "UPDATE session_lifecycle SET ended_at = NULL",
            "UPDATE session_lifecycle SET ended_at = '2026-08-22T09:00:00Z'",
            "UPDATE session_lifecycle SET status = 'In Progress'",
        ):
            with self.subTest(mutation=mutation):
                self.db.execute("SAVEPOINT invalid")
                self.db.execute(mutation)
                self.assertEqual(self.rules()["oracle_available"]["status"], "FAIL")
                self.assertEqual(self.rules()["duration_match"]["status"], "BLOCKED")
                self.db.execute("ROLLBACK TO invalid")
                self.db.execute("RELEASE invalid")

    def test_duration_uses_fractional_elapsed_hours_at_six_decimals(self):
        for end, hours in (("2026-08-22T10:00:01Z", 0.000278),
                           ("2026-08-22T10:00:00.001800Z", 0.000001)):
            with self.subTest(end=end):
                self.db.execute("UPDATE session_lifecycle SET ended_at = ?", (end,))
                self.db.execute("UPDATE actual_ledger SET actual_duration_hours = ?", (hours,))
                self.assertEqual(self.rules()["duration_match"]["status"], "PASS")

    def test_zero_values_are_compared_not_treated_as_missing(self):
        self.db.execute("UPDATE expected_ledger SET expected_energy_kwh = 0, expected_amount = 0")
        self.db.execute("UPDATE session_lifecycle SET ended_at = started_at")
        self.db.execute("UPDATE actual_ledger SET actual_energy_kwh = 0, actual_duration_hours = 0, actual_amount = 0")
        self.assertTrue(all(r["status"] == "PASS" for r in self.rows()))

    def test_runs_and_releases_are_isolated(self):
        self.add_oracle(run="other-run")
        self.add_actual(run="other-run", role="baseline")
        self.add_actual(run="other-run", role="candidate", amount=6.5)
        self.assertTrue(all(r["status"] == "PASS" for r in self.rules().values()))
        self.assertTrue(all(r["status"] == "PASS" for r in self.rules(run="other-run", role="baseline").values()))
        self.assertEqual(self.rules(run="other-run")["amount_match"]["status"], "FAIL")
        self.assertEqual(len(self.rows()), 28)

    def test_evidence_retains_oracle_and_all_retry_hashes_without_duplicate_charge(self):
        self.db.execute("DELETE FROM actual_ledger WHERE release_role = 'candidate'")
        self.add_actual(hashes=["hash-retry", "hash-original"])
        rules = self.rules()
        self.assertEqual(rules["final_cdr_count"]["status"], "PASS")
        evidence = json.loads(rules["amount_match"]["evidence"])
        self.assertEqual(evidence["expected_tariff"]["payload_hash"], "tariff-evidence-hash")
        self.assertEqual(evidence["expected_tariff"]["currency"], "EUR")
        self.assertEqual(evidence["session"]["ended_at"], "2026-08-22T11:00:00Z")
        self.assertEqual(evidence["actual_cdrs"][0]["source_payload_hashes"], ["hash-original", "hash-retry"])
        self.assertEqual(self.rows(), self.rows())


if __name__ == "__main__":
    unittest.main()
