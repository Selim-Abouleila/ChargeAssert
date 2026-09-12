"""Local regression checks for the actual-ledger source query.

Runs the production validation/grouping/conflict SQL in SQLite after replacing
Spark struct/array accesses with fixture columns and supplying small function
adapters. This does NOT validate Databricks from_json, DDL or Delta MERGE. The
job's SQL smoke assertions must still be run in the Databricks workspace.

Run: python -m unittest discover -s tests -v
"""

import copy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql/08_create_actual_ledger.sql").read_text(encoding="utf-8")
RAW_SQL = (ROOT / "sql/07_create_ocpi_cdrs_raw.sql").read_text(encoding="utf-8")
BODY = re.search(r"'(\{[^\r\n]+\})' AS payload", RAW_SQL).group(1)
FIXTURE = json.loads(BODY)


def get_json_object(body, path):
    value = json.loads(body, parse_float=Decimal)
    for key in path.removeprefix("$.").split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def utc(value):
    if value is None:
        return None
    return datetime.fromisoformat(value).astimezone(timezone.utc).isoformat()


class CollectSet:
    def __init__(self):
        self.values = set()

    def step(self, value):
        if value is not None:
            self.values.add(value)

    def finalize(self):
        return json.dumps(sorted(self.values))


def source_query():
    start = SQL.index("  ), validated_cdrs AS (")
    end = SQL.index("\n) AS source", start)
    query = "WITH validated_cdrs AS (" + SQL[start + len("  ), validated_cdrs AS ("):end]
    replacements = {
        "try_element_at(cdr.charging_periods, 1).tariff_id": "reported_tariff_id",
        "size(cdr.charging_periods)": "period_count",
        "cdr.total_cost.excl_vat": "reported_amount",
        "cdr.country_code": "reported_country_code",
        "cdr.party_id": "reported_party_id",
        "cdr.session_id": "reported_session_id",
        "cdr.start_date_time": "reported_start",
        "cdr.end_date_time": "reported_end",
        "cdr.currency": "reported_currency",
        "cdr.total_energy": "reported_energy",
        "cdr.total_time": "reported_duration",
        "cdr.credit": "reported_credit",
        "cdr.id": "reported_cdr_id",
    }
    for original, replacement in replacements.items():
        query = query.replace(original, replacement)
    return query.replace(" RLIKE ", " REGEXP ")


class ActualLedgerTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute("""CREATE TABLE parsed_cdrs (
            run_id TEXT, release_role TEXT, payload TEXT, payload_hash TEXT,
            reported_country_code TEXT, reported_party_id TEXT,
            reported_cdr_id TEXT, reported_session_id TEXT,
            reported_start TEXT, reported_end TEXT, reported_currency TEXT,
            reported_energy NUMERIC, reported_duration NUMERIC,
            reported_amount NUMERIC, reported_credit INTEGER,
            period_count INTEGER, reported_tariff_id TEXT
        )""")
        self.db.create_function("regexp", 2, lambda pattern, value: None if value is None else int(re.search(pattern, str(value)) is not None))
        self.db.create_function("sha2", 2, lambda body, bits: hashlib.sha256(body.encode()).hexdigest())
        self.db.create_function("get_json_object", 2, get_json_object)
        self.db.create_function("concat", -1, lambda *args: "".join(str(arg) for arg in args))
        self.db.create_function("sort_array", 1, lambda value: json.dumps(sorted(json.loads(value))))
        self.db.create_aggregate("collect_set", 1, CollectSet)
        self.last_error = None

        def raise_error(message):
            self.last_error = message
            raise ValueError(message)

        self.db.create_function("raise_error", 1, raise_error)

    def tearDown(self):
        self.db.close()

    def add(self, changes=None, role="candidate", run="smoke-run-v1", body=None, corrupt_hash=False):
        if body is None:
            cdr = copy.deepcopy(FIXTURE)
            cdr.update(changes or {})
            body = json.dumps(cdr, separators=(",", ":"))
        cdr = json.loads(body)
        periods = cdr.get("charging_periods")
        digest = hashlib.sha256(body.encode()).hexdigest()
        self.db.execute("INSERT INTO parsed_cdrs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            run, role, body, "broken" if corrupt_hash else digest,
            cdr.get("country_code"), cdr.get("party_id"), cdr.get("id"),
            cdr.get("session_id"), utc(cdr.get("start_date_time")),
            utc(cdr.get("end_date_time")), cdr.get("currency"),
            cdr.get("total_energy"), cdr.get("total_time"),
            (cdr.get("total_cost") or {}).get("excl_vat"), cdr.get("credit"),
            None if periods is None else len(periods),
            periods[0].get("tariff_id") if periods else None,
        ))
        return digest

    def rows(self):
        return [dict(row) for row in self.db.execute(source_query()).fetchall()]

    def test_healthy_baseline_and_candidate_are_separate(self):
        self.add(role="baseline")
        self.add(role="candidate")
        rows = self.rows()
        self.assertEqual({row["release_role"] for row in rows}, {"baseline", "candidate"})
        for row in rows:
            self.assertEqual((row["actual_energy_kwh"], row["actual_duration_hours"], row["actual_amount"]), (12.5, 1, 5.63))

    def test_exact_retries_collapse(self):
        digest = self.add()
        self.add()
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0]["source_payload_hashes"]), [digest])

    def test_equivalent_json_keeps_all_evidence_hashes(self):
        hashes = [self.add(body=BODY), self.add(body=json.dumps(FIXTURE, indent=2))]
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0]["source_payload_hashes"]), sorted(hashes))

    def test_distinct_cdr_ids_for_one_session_are_preserved(self):
        self.add()
        self.add({"id": "cdr-second-charge"})
        self.assertEqual(len(self.rows()), 2)

    def test_runs_and_owners_are_isolated(self):
        self.add()
        self.add(run="other-run")
        self.add({"party_id": "ALT"})
        self.add({"country_code": "DE"})
        self.assertEqual(len(self.rows()), 4)

    def test_case_insensitive_record_identity_is_normalized(self):
        self.add()
        self.add({"id": "CDR-SMOKE-V1", "country_code": "fr", "party_id": "cas"})
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["country_code"], rows[0]["party_id"], rows[0]["cdr_id"]), ("FR", "CAS", "cdr-smoke-v1"))

    def test_conflicting_amount_is_rejected(self):
        self.add()
        self.add({"total_cost": {"excl_vat": 6.50}})
        with self.assertRaises(sqlite3.OperationalError):
            self.rows()
        self.assertIn("Conflicting normalized CDR values", self.last_error)
        self.assertIn("smoke-run-v1/candidate/FR/CAS/cdr-smoke-v1", self.last_error)

    def test_conflicting_session_is_rejected(self):
        self.add()
        self.add({"session_id": "other-session"})
        with self.assertRaises(sqlite3.OperationalError):
            self.rows()
        self.assertIn("Conflicting normalized CDR values", self.last_error)

    def test_reported_values_are_not_replaced_with_expectations(self):
        self.add({"total_energy": 10, "total_time": 2, "currency": "USD", "total_cost": {"excl_vat": 6.50}, "charging_periods": [{"tariff_id": "wrong-tariff"}]})
        row = self.rows()[0]
        self.assertEqual((row["actual_energy_kwh"], row["actual_duration_hours"], row["actual_amount"], row["currency"], row["tariff_id"]), (10, 2, 6.50, "USD", "wrong-tariff"))

    def test_fractional_cent_is_preserved(self):
        self.add({"total_cost": {"excl_vat": 5.625}})
        self.assertEqual(self.rows()[0]["actual_amount"], 5.625)

    def test_invalid_records_fail_with_evidence_reference(self):
        cases = [
            {"id": None}, {"id": " "}, {"id": "x" * 37},
            {"session_id": None}, {"party_id": None}, {"country_code": "FRA"},
            {"start_date_time": None}, {"end_date_time": None},
            {"end_date_time": "2026-08-22T09:00:00Z"},
            {"total_energy": None}, {"total_energy": -1},
            {"total_time": -1}, {"total_cost": {"excl_vat": -1}},
            {"total_cost": {}}, {"currency": "EURO"}, {"credit": True},
            {"charging_periods": []}, {"charging_periods": [{"tariff_id": None}]},
            {"charging_periods": [{"tariff_id": "one"}, {"tariff_id": "two"}]},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                self.db.execute("DELETE FROM parsed_cdrs")
                digest = self.add(changes)
                with self.assertRaises(sqlite3.OperationalError):
                    self.rows()
                self.assertIn("Invalid or unsupported CDR", self.last_error)
                self.assertIn(digest, self.last_error)

    def test_excess_precision_is_rejected(self):
        self.add(body=BODY.replace('"excl_vat":5.63', '"excl_vat":5.6300001'))
        with self.assertRaises(sqlite3.OperationalError):
            self.rows()
        self.assertIn("Invalid or unsupported CDR", self.last_error)

    def test_hash_integrity_and_release_role_are_required(self):
        for kwargs in ({"corrupt_hash": True}, {"role": "unknown"}):
            with self.subTest(kwargs=kwargs):
                self.db.execute("DELETE FROM parsed_cdrs")
                self.add(**kwargs)
                with self.assertRaises(sqlite3.OperationalError):
                    self.rows()


class BundleWiringTests(unittest.TestCase):
    def test_sql_paths_parameters_and_dependencies(self):
        text = (ROOT / "resources/jobs.yml").read_text(encoding="utf-8")
        tasks = {}
        for block in re.split(r"(?m)^        - task_key: ", text)[1:]:
            key = block.splitlines()[0]
            self.assertNotIn(key, tasks)
            path = re.search(r"(?m)^              path: (.+)$", block).group(1)
            source = (ROOT / "resources" / path).read_text(encoding="utf-8")
            params = set(re.findall(r"(?m)^              (\w+): ", block.split("            parameters:\n")[1]))
            self.assertEqual(params, set(re.findall(r"IDENTIFIER\(:(\w+)\)", source)), key)
            tasks[key] = re.findall(r"(?m)^            - task_key: (.+)$", block)

        def visit(key, trail=()):
            self.assertIn(key, tasks)
            self.assertNotIn(key, trail)
            for dep in tasks[key]:
                visit(dep, trail + (key,))

        for key in tasks:
            visit(key)
        self.assertEqual(tasks["create_actual_ledger"], ["create_ocpi_cdrs_raw"])
        self.assertEqual(set(re.findall(r"IDENTIFIER\(:(\w+)\)", SQL)), {"table_name", "raw_cdrs_table_name"})


if __name__ == "__main__":
    unittest.main()
