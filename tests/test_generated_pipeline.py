"""Check generated-job receipt boundaries and immutable storage without Spark."""

from datetime import datetime
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from notebooks import generated_pipeline as pipeline


class Result:
    def __init__(self, rows=()):
        self.rows = list(rows)
    def collect(self):
        return self.rows


class ReceiptSpark:
    def __init__(self, incoming=()):
        self.incoming = incoming
        self.receipts = {}
        self.calls = []
    def sql(self, statement, args=None):
        args = args or {}
        self.calls.append((statement, args))
        if statement.startswith('INSERT INTO'):
            self.receipts[args['execution_id']] = dict(args, status='RUNNING', started_at=datetime(2026, 9, 30))
        elif statement.startswith('UPDATE'):
            receipt = self.receipts[args['execution_id']]
            if receipt['status'] == 'RUNNING':
                receipt.update(status='FAILED' if "status='FAILED'" in statement else 'SUCCEEDED',
                               reason=args['reason'], run_id=args['run_id'])
        elif statement.startswith('SELECT *') and 'execution_id' in args:
            row = self.receipts.get(args['execution_id'])
            return Result([row] if row else [])
        elif statement.startswith('SELECT raw_record'):
            return Result(self.incoming)
        return Result()


class GeneratedPipelineTests(unittest.TestCase):
    def arguments(self, **changes):
        return dict(dict(catalog_name='workspace', batch_id='batch', candidate_mode='healthy',
                         job_id='7', job_run_id='101', repair_count='0'), **changes)

    def test_sql_splitter_keeps_semicolons_and_comment_like_text_inside_strings(self):
        source = "-- note;\nSELECT 'a;b--c', 'it''s;ok'; SELECT `we;ird`;"
        self.assertEqual(list(pipeline.statements(source)), ["SELECT 'a;b--c', 'it''s;ok'", 'SELECT `we;ird`'])

    def test_actual_sql_files_have_all_required_parameters_and_use_generated_targets(self):
        names = pipeline.table_names('workspace')
        spark = ReceiptSpark()
        pipeline._transform(spark, names, 'test-run')
        self.assertGreater(len(spark.calls), 10)
        for statement, args in spark.calls:
            if 'table_name' in args:
                self.assertIn('.generated_', args['table_name'])
            self.assertNotIn('smoke-run-v1', statement)
        self.assertEqual(len(pipeline.pipeline_hash()), 64)

    def test_immutable_rows_accept_identical_business_content_but_reject_conflicts_and_duplicates(self):
        original = dict(run_id='r', event_id='e', payload='original', ingest_time=datetime(2026, 1, 1))
        repeated = dict(original, ingest_time=datetime(2026, 2, 1))
        self.assertTrue(pipeline.check_immutable([original], [repeated], ('run_id','event_id'), ('ingest_time',)))
        self.assertFalse(pipeline.check_immutable([], [original], ('run_id','event_id')))
        for previous in ([dict(original, payload='changed')], [original, original], [dict(original, event_id='unexpected')]):
            with self.assertRaises(ValueError):
                pipeline.check_immutable(previous, [original], ('run_id','event_id'))

    def test_catalog_names_cannot_inject_sql_or_choose_another_schema(self):
        for catalog in ('', 'workspace.foo', 'workspace`', 'x;DROP TABLE t', '../x'):
            with self.assertRaises(ValueError):
                pipeline.table_names(catalog)

    def test_invalid_batch_gets_failed_receipt_before_any_billing_writes(self):
        spark = ReceiptSpark()
        args = self.arguments()
        args['batch_id'] = '../bad'
        with self.assertRaises(ValueError):
            pipeline.evaluate(spark, **args)
        self.assertEqual(spark.receipts['7:101:0']['status'], 'FAILED')
        self.assertFalse(any('MERGE INTO' in sql for sql, _ in spark.calls))

    def test_transform_failure_is_recorded_and_does_not_publish_a_snapshot(self):
        spark = ReceiptSpark()
        prepared = dict(run_id='new-run', session_ids=['s'], tables={name:[{}] for name in pipeline.KEYS if name!='execution_verdict'})
        with patch('notebooks.generated_batch.prepare_batch', return_value=prepared), \
             patch.object(pipeline, '_ensure_tables'), patch.object(pipeline, 'write_immutable') as write, \
             patch.object(pipeline, '_transform', side_effect=RuntimeError('tariff check failed')):
            with self.assertRaisesRegex(RuntimeError, 'tariff check failed'):
                pipeline.evaluate(spark, **self.arguments())
        self.assertEqual(spark.receipts['7:101:0']['status'], 'FAILED')
        self.assertIn('tariff check failed', spark.receipts['7:101:0']['reason'])
        self.assertEqual(write.call_count, 5)
        self.assertTrue(all('.generated_execution_verdict' not in call.args[1] for call in write.call_args_list))

    def test_same_execution_cannot_be_repaired_or_promoted_after_failure(self):
        spark = ReceiptSpark()
        spark.receipts['7:101:0'] = dict(status='FAILED', reason='original failure')
        with self.assertRaisesRegex(ValueError, 'already has a receipt'):
            pipeline.evaluate(spark, **self.arguments())
        self.assertEqual(spark.receipts['7:101:0']['reason'], 'original failure')

    def test_financial_fail_can_finish_successfully_with_its_own_snapshot(self):
        spark = ReceiptSpark()
        prepared = dict(run_id='new-run', session_ids=['s'], input_hash='a'*64,
                        tables={name:[{}] for name in pipeline.KEYS if name!='execution_verdict'})
        capture = dict(session_count=1, assertion_count=14, baseline_verdict='PASS',
                       candidate_verdict='FAIL', verdict='FAIL', verdict_row={'run_id':'new-run'}, assertions=[])
        with patch('notebooks.generated_batch.prepare_batch', return_value=prepared), \
             patch('notebooks.generated_capture.capture_result', return_value=capture), \
             patch.object(pipeline, '_ensure_tables'), patch.object(pipeline, 'write_immutable') as write, \
             patch.object(pipeline, '_transform'), patch('builtins.print'):
            result = pipeline.evaluate(spark, **self.arguments())
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertEqual(spark.receipts['7:101:0']['status'], 'SUCCEEDED')
        saved = write.call_args_list[-1].args[2][0]
        self.assertEqual(saved['execution_id'], '7:101:0')
        self.assertEqual(saved['verdict'], 'FAIL')


class GeneratedConsumerTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.db.executescript("""
          CREATE TABLE executions (execution_id TEXT,status TEXT,run_id TEXT,pipeline_hash TEXT,reason TEXT);
          CREATE TABLE saved_snapshots (execution_id TEXT,run_id TEXT,pipeline_hash TEXT,session_count INTEGER,
            assertion_count INTEGER,baseline_verdict TEXT,candidate_verdict TEXT,verdict TEXT);
        """)
        sql = (Path(__file__).resolve().parents[1] / 'sql/15_check_generated_billing.sql').read_text(encoding='utf-8')
        self.query = next(pipeline.statements(sql))
        self.query = self.query.replace('workspace.chargeassert_dev_bronze.generated_job_execution', 'executions')
        self.query = self.query.replace('workspace.chargeassert_dev_gold.generated_execution_verdict', 'saved_snapshots')
        self.db.execute("INSERT INTO executions VALUES ('old','SUCCEEDED','run','hash','done')")
        self.db.execute("INSERT INTO saved_snapshots VALUES ('old','run','hash',2,28,'PASS','PASS','PASS')")

    def result(self, execution_id):
        return dict(self.db.execute(self.query, {'execution_id':execution_id}).fetchone())

    def test_success_reads_the_exact_snapshot(self):
        self.assertEqual(self.result('old')['verdict'], 'PASS')

    def test_failed_missing_or_running_attempt_cannot_reuse_old_pass(self):
        for status in ('FAILED', 'RUNNING'):
            self.db.execute('INSERT INTO executions VALUES (?,?,?,?,?)', (status,status,'run','hash','unfinished'))
            self.assertEqual(self.result(status)['verdict'], 'BLOCKED')
        self.assertEqual(self.result('absent')['verdict'], 'BLOCKED')

    def test_duplicate_missing_mismatched_or_incomplete_evidence_is_blocked(self):
        mutations = (
            "INSERT INTO executions SELECT * FROM executions",
            "INSERT INTO saved_snapshots SELECT * FROM saved_snapshots",
            "DELETE FROM saved_snapshots",
            "UPDATE saved_snapshots SET pipeline_hash = 'wrong'",
            "UPDATE saved_snapshots SET run_id = 'wrong'",
            "UPDATE saved_snapshots SET assertion_count = 14",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.db.execute('SAVEPOINT invalid')
                self.db.execute(mutation)
                self.assertEqual(self.result('old')['verdict'], 'BLOCKED')
                self.db.execute('ROLLBACK TO invalid')
                self.db.execute('RELEASE invalid')

    def test_completed_financial_failure_stays_fail(self):
        self.db.execute("UPDATE saved_snapshots SET candidate_verdict='FAIL',verdict='FAIL'")
        self.assertEqual(self.result('old')['verdict'], 'FAIL')


if __name__ == '__main__':
    unittest.main()
