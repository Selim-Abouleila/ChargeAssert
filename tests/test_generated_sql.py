"""Execute generated billing SQL through SQLite adapters, not Spark/Delta.

Covers financial rules, oracle coverage and isolation. JSON parsing, DECIMAL
analysis, DDL and Delta MERGE must still be verified in Databricks.
"""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unittest
from unittest.mock import patch

import test_actual_ledger as actual_fixture
import test_amount_scenarios as amount_fixture
import test_assertion_result as assertion_fixture
from notebooks import mock_billing, session_generator

ROOT = Path(__file__).resolve().parents[1]
NAMES = ('03_session_lifecycle', '05_tariff_history', '06_expected_ledger',
         '08_actual_ledger', '09_assertion_result', '10_release_verdict')
SQL = {name[:2]: (ROOT / 'sql/generated' / (name + '.sql')).read_text(encoding='utf-8') for name in NAMES}
RUN = 'generated-billing-sql-test'


def source(sql):
    return sql.split('USING (\n', 1)[1].split('\n) AS source', 1)[0]


def adapt(sql):
    return (amount_fixture.adapt_query(sql)
            .replace('IDENTIFIER(:input_session_table_name)', 'input_sessions')
            .replace('IDENTIFIER(:expected_ledger_table_name)', 'expected_ledger')
            .replace('IDENTIFIER(:actual_ledger_table_name)', 'actual_ledger')
            .replace('current_timestamp()', 'CURRENT_TIMESTAMP')
            # SQLite uses binary multiplication; emulate Spark's exact DECIMAL product.
            .replace('expected_energy_kwh * price_per_kwh', 'decimal_product(expected_energy_kwh, price_per_kwh)'))


class GeneratedSqlTests(unittest.TestCase):
    def setUp(self):
        self.fixture = amount_fixture.AmountScenarioTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.db = self.fixture.db
        self.db.create_function('decimal_product', 2, lambda left, right:
                                format(Decimal(str(left)) * Decimal(str(right)), 'f'))
        self.db.execute('CREATE TABLE input_sessions (run_id TEXT, session_id TEXT, tariff_id TEXT)')
        tariff_id = 'generated-energy-v1'
        tariff = {'id': tariff_id, 'currency': 'EUR', 'elements': [
            {'price_components': [{'type': 'ENERGY', 'price': 0.45, 'step_size': 1}]}]}
        digest = hashlib.sha256(json.dumps(tariff).encode()).hexdigest()
        self.fixture.insert_rows('tariff_history', [{
            'run_id': RUN, 'tariff_id': tariff_id, 'valid_from': '2026-01-01T00:00:00Z',
            'valid_to': None, 'currency': 'EUR',
            'price_components': json.dumps(tariff['elements'][0]['price_components']),
            'source_payload_hash': digest,
        }])
        self.fixture.insert_rows('run_manifest', [{
            'run_id': RUN, 'scenario_id': 'generated-sql-test', 'seed': 42,
            'baseline_sha': 'baseline-test', 'candidate_sha': 'candidate-test',
            'tariff_hash': digest, 'created_at': '2026-09-01T00:00:00Z',
        }])
        groups = {}
        for line in session_generator.generate_batch('sql-test', 2, 42).splitlines():
            envelope = json.loads(line)
            if 'event_id' not in envelope:
                continue
            payload = envelope['payload']
            event = json.loads(payload)
            session = event['transactionInfo']['transactionId']
            groups.setdefault(session, []).append(event)
            self.fixture.insert_rows('raw_events', [{
                'run_id': RUN, 'event_id': envelope['event_id'],
                'charging_station_id': envelope['charging_station_id'],
                'transaction_id': session, 'event_type': event['eventType'],
                'sequence_number': event['seqNo'], 'event_time': event['timestamp'],
                'ingest_time': event['timestamp'], 'payload': payload,
                'payload_hash': hashlib.sha256(payload.encode()).hexdigest(),
            }])
        self.sessions = sorted(groups)
        self.db.executemany('INSERT INTO input_sessions VALUES (?,?,?)',
                            [(RUN, session, tariff_id) for session in self.sessions])
        self.fixture.insert_rows('session_lifecycle', self.rows(adapt(source(SQL['03']))))
        self.check_expected()
        self.fixture.insert_rows('expected_ledger', self.rows(adapt(source(SQL['06']))))
        normalization = actual_fixture.ActualLedgerTests()
        normalization.setUp()
        try:
            for session, events in groups.items():
                parsed = [(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')),
                           event['meterValue'][0]['sampledValue'][0]['value'])
                          for event in sorted(events, key=lambda event: event['seqNo'])]
                for role in ('baseline', 'candidate'):
                    # Mock reports calculate from events independently of SQL's oracle.
                    body = json.loads(mock_billing._cdr(parsed, tariff, Decimal('0.45'), False)['payload'])
                    body['id'] = 'cdr-' + session.removeprefix('txn-')
                    body['session_id'] = session
                    body['charging_periods'][0]['tariff_id'] = tariff_id
                    normalization.add(run=RUN, role=role, body=json.dumps(body))
            with patch.object(actual_fixture, 'SQL', SQL['08']):
                self.fixture.insert_rows('actual_ledger', normalization.rows())
        finally:
            normalization.tearDown()
        self.refresh_assertions()

    def rows(self, query, run=RUN):
        return [dict(row) for row in self.db.execute(query, {'run_id': run})]

    def check_expected(self):
        for statement in amount_fixture.statements(SQL['06']):
            if statement.startswith('MERGE INTO'):
                break
            if 'assert_true(' in statement:
                self.rows(adapt(statement))

    def refresh_assertions(self):
        self.db.execute('DELETE FROM assertion_result WHERE run_id = ?', (RUN,))
        with patch.object(assertion_fixture, 'SQL', SQL['09']):
            self.fixture.insert_rows('assertion_result', self.rows(assertion_fixture.source_query()))

    def verdict(self, run=RUN):
        rows = self.rows(adapt(source(SQL['10'])), run=run)
        self.assertEqual(len(rows), 1)
        return rows[0]

    def test_default_two_sessions_have_twenty_eight_passing_checks(self):
        verdict = self.verdict()
        self.assertEqual((verdict['run_id'], verdict['verdict'], verdict['required_assertions'],
                          verdict['passed_assertions'], verdict['failed_assertions']),
                         (RUN, 'PASS', 28, 28, 0))
        self.assertEqual(len(self.rows('SELECT * FROM expected_ledger WHERE run_id = :run_id')), 2)

    def test_wrong_candidate_amount_fails_only_its_amount_check(self):
        self.db.execute("UPDATE actual_ledger SET actual_amount = actual_amount + 0.87 "
                        "WHERE run_id = ? AND release_role = 'candidate' AND session_id = ?",
                        (RUN, self.sessions[0]))
        self.refresh_assertions()
        verdict = self.verdict()
        self.assertEqual((verdict['baseline_verdict'], verdict['candidate_verdict'],
                          verdict['passed_assertions'], verdict['failed_assertions']),
                         ('PASS', 'FAIL', 27, 1))
        self.assertEqual(json.loads(verdict['first_problem'])['assertion_id'], 'amount_match')

    def test_missing_candidate_cdr_fails_and_blocks_value_rules(self):
        self.db.execute("DELETE FROM actual_ledger WHERE run_id = ? AND release_role = 'candidate' "
                        'AND session_id = ?', (RUN, self.sessions[0]))
        self.refresh_assertions()
        verdict = self.verdict()
        self.assertEqual((verdict['required_assertions'], verdict['passed_assertions'],
                          verdict['failed_assertions'], verdict['blocked_assertions']), (28, 22, 1, 5))

    def check_rejected(self, statements):
        for statement in statements:
            with self.subTest(statement=statement):
                self.db.execute('SAVEPOINT invalid_input')
                self.db.execute(statement, {'run_id': RUN})
                with self.assertRaises(sqlite3.OperationalError):
                    self.check_expected()
                self.db.execute('ROLLBACK TO invalid_input')
                self.db.execute('RELEASE invalid_input')

    def test_empty_duplicate_or_missing_registered_session_is_rejected(self):
        self.check_rejected((
            'DELETE FROM input_sessions WHERE run_id = :run_id',
            'INSERT INTO input_sessions SELECT * FROM input_sessions WHERE run_id = :run_id LIMIT 1',
            "UPDATE input_sessions SET session_id = 'absent' WHERE run_id = :run_id AND session_id = "
            '(SELECT MIN(session_id) FROM input_sessions WHERE run_id = :run_id)',
        ))

    def test_absent_or_ambiguous_effective_tariff_is_rejected(self):
        self.check_rejected((
            'DELETE FROM tariff_history WHERE run_id = :run_id',
            'INSERT INTO tariff_history SELECT * FROM tariff_history WHERE run_id = :run_id',
        ))

    def test_selected_run_isolated_from_existing_fixtures(self):
        verdict = self.verdict(run='amount-bad-v1')
        self.assertEqual((verdict['run_id'], verdict['required_assertions'], verdict['failed_assertions']),
                         ('amount-bad-v1', 14, 1))
        events = self.rows(adapt(source(SQL['03'])), run='smoke-run-v1')
        self.assertEqual([(row['run_id'], row['session_id']) for row in events],
                         [('smoke-run-v1', 'txn-smoke-v1')])

    def test_fixture_pipeline_preserves_checks_and_excludes_generated_rows(self):
        self.assertEqual({row['run_id'] for row in self.fixture.fixture.rows()}, amount_fixture.ALL_RUNS)
        sessions = self.rows(amount_fixture.source_query(
            (ROOT / 'sql/03_create_session_lifecycle.sql').read_text(encoding='utf-8')))
        self.assertEqual({row['run_id'] for row in sessions}, amount_fixture.ALL_RUNS)

    def test_all_source_reads_and_gold_deletes_are_scoped(self):
        for filename, sql in SQL.items():
            with self.subTest(sql=filename):
                parameters = re.findall(r'IDENTIFIER\(:(?!table_name\b)([a-z_]+)\)', sql)
                self.assertTrue(parameters)
                for parameter in parameters:
                    self.assertIn(f'IDENTIFIER(:{parameter}) WHERE run_id = :run_id', sql)
                self.assertNotIn('smoke-run-v1', sql)
        for filename in ('09', '10'):
            self.assertIn('WHEN NOT MATCHED BY SOURCE AND target.run_id = :run_id THEN DELETE', SQL[filename])


if __name__ == '__main__':
    unittest.main()
