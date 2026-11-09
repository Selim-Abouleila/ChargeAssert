"""Check job ownership, exact evidence selection, safe inputs and SQL pagination."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.service import AppError, SimulationService

CONFIG = {"PIPELINE_JOB_ID": "100", "EVALUATOR_JOB_ID": "200", "SQL_WAREHOUSE_ID": "warehouse",
          "CATALOG_NAME": "workspace", "DASHBOARD_ID": "dashboard"}
TOKEN = "8f9d00c7-a1bc-45ba-ae65-fc66c9db14bf"


def run(run_id, job_id, state="TERMINATED", result="SUCCESS", tasks=None):
    return {"run_id": run_id, "job_id": job_id,
            "state": {"life_cycle_state": state, "result_state": result},
            "tasks": tasks or [], "start_time": 1792130580000,
            "job_parameters": [{"name": "candidate_mode", "value": "amount_error"},
                               {"name": "batch_id", "value": "sessions-123"}]}


def task(key="evaluate_generated_batch", state="TERMINATED", result="SUCCESS"):
    return {"task_key": key, "run_id": 999, "run_job_task": {"job_id": 200},
            "state": {"life_cycle_state": state, "result_state": result}}


def statement(columns, rows, *, next_index=None, total=None):
    return {"statement_id": "statement-1", "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": name, "type_name": kind}
                                                  for name, kind in columns]},
                         "total_row_count": len(rows) if total is None else total,
                         "truncated": False},
            "result": {"data_array": rows, "next_chunk_index": next_index}}


class SimulationServiceTests(unittest.TestCase):
    def setUp(self):
        self.client = SimpleNamespace(config=SimpleNamespace(host="https://example.cloud.databricks.com"),
                                      jobs=Mock(), statement_execution=Mock())
        self.parent = run(123, 100, tasks=[task()])
        self.child = run(456, 200)
        self.client.jobs.get_run.side_effect = self.get_run
        self.client.jobs.get_run_output.return_value = {"run_job_output": {"run_id": 456}}
        self.service = SimulationService(self.client, CONFIG)
        sources = patch("app.service.SQL_DIRECTORY", Path(__file__).resolve().parents[1] / "sql/dashboard")
        sources.start()
        self.addCleanup(sources.stop)
        # The service only imports the SDK for live SQL parameters; local safety tests need no SDK.
        self.parameter = patch("app.service._sql_parameter", side_effect=lambda execution_id:
                               {"name": "execution_id", "value": execution_id, "type": "STRING"})
        self.parameter.start()
        self.addCleanup(self.parameter.stop)

    def get_run(self, run_id, **kwargs):
        return deepcopy(self.parent if run_id == 123 else self.child)

    def test_launch_uses_validated_parameters_and_idempotency_without_batch_override(self):
        self.client.jobs.run_now.return_value = SimpleNamespace(response=SimpleNamespace(run_id=123))
        result = self.service.start_run({"idempotency_token": TOKEN})
        self.assertEqual(result, {"run_id": "123"})
        self.client.jobs.run_now.assert_called_once_with(
            job_id=100, idempotency_token=TOKEN,
            job_parameters={"candidate_mode": "amount_error", "session_count": "100", "seed": "42"})

    def test_int64_launch_and_history_ids_are_returned_as_exact_strings(self):
        large_id = 2**63 - 1
        self.client.jobs.run_now.return_value = SimpleNamespace(response=SimpleNamespace(run_id=large_id))
        self.assertEqual(self.service.start_run({"idempotency_token": TOKEN})["run_id"], str(large_id))
        self.client.jobs.list_runs.return_value = iter([run(large_id, 100)])
        self.assertEqual(self.service.list_runs()[0]["run_id"], str(large_id))

    def test_int64_parent_and_child_ids_survive_status_round_trip(self):
        parent_id, child_id = 2**63 - 1, 2**63 - 3
        self.client.jobs.get_run.side_effect = [run(parent_id, 100, tasks=[task()]), run(child_id, 200)]
        self.client.jobs.get_run_output.return_value = {"run_job_output": {"run_id": child_id}}
        status = self.service.get_run(str(parent_id))
        self.assertEqual(status["run_id"], str(parent_id))
        self.assertEqual(status["execution_id"], f"200:{child_id}:0")
        self.assertEqual(self.client.jobs.get_run.call_args_list[0].kwargs["run_id"], parent_id)
        self.assertEqual(self.client.jobs.get_run.call_args_list[1].kwargs["run_id"], child_id)

    def test_launch_supports_healthy_count_and_seed_limits(self):
        self.client.jobs.run_now.return_value = SimpleNamespace(response=SimpleNamespace(run_id=123))
        self.service.start_run({"idempotency_token": TOKEN, "session_count": 1000,
                                "seed": 4294967295, "candidate_mode": "healthy"})
        self.assertEqual(self.client.jobs.run_now.call_args.kwargs["job_parameters"],
                         {"candidate_mode": "healthy", "session_count": "1000", "seed": "4294967295"})

    def test_invalid_inputs_never_launch(self):
        bad_payloads = [{"session_count": value} for value in (0, 1001, True, 1.5, "1;DROP", None)]
        bad_payloads += [{"seed": value} for value in (-1, 4294967296, False, "NaN")]
        bad_payloads += [{"candidate_mode": "other"}, {"batch_id": "injected"}, {"job_id": 555},
                         {"idempotency_token": "unsafe"}]
        for payload in bad_payloads:
            with self.subTest(payload=payload), self.assertRaises(AppError):
                self.service.start_run({"idempotency_token": TOKEN, **payload})
        self.client.jobs.run_now.assert_not_called()

    def test_config_uses_workspace_url_and_defaults(self):
        config = self.service.get_config()
        self.assertEqual(config["defaults"]["session_count"], 100)
        self.assertFalse(config["preview"])
        self.assertEqual(config["dashboard_url"], "https://example.cloud.databricks.com/dashboardsv3/dashboard/published")

    def test_missing_configuration_is_actionable(self):
        with self.assertRaises(AppError) as raised:
            SimulationService(config={})
        self.assertEqual(raised.exception.status, 503)
        self.assertIn("Deploy", str(raised.exception))

    def test_history_only_reads_twenty_configured_job_runs(self):
        consumed = []
        def runs():
            for index in range(25):
                consumed.append(index)
                yield run(index + 1, 100)
        self.client.jobs.list_runs.return_value = runs()
        results = self.service.list_runs()
        self.assertEqual(len(results), 20)
        self.assertEqual(len(consumed), 20)
        self.client.jobs.list_runs.assert_called_once_with(job_id=100, limit=20, expand_tasks=False)
        self.assertTrue(results[0]["started_at"].endswith("Z"))

    def test_history_omits_foreign_jobs_even_if_api_returns_one(self):
        self.client.jobs.list_runs.return_value = iter([run(1, 100), run(2, 400)])
        self.assertEqual([r["run_id"] for r in self.service.list_runs()], ["1"])

    def test_true_child_id_is_resolved_from_task_output(self):
        status = self.service.get_run(123)
        self.assertEqual(status["execution_id"], "200:456:0")
        self.client.jobs.get_run_output.assert_called_once_with(run_id=999)
        self.assertTrue(status["terminal"])
        self.client.statement_execution.execute_statement.assert_not_called()

    def test_foreign_parent_is_rejected_before_any_child_or_sql_lookup(self):
        self.parent["job_id"] = 400
        with self.assertRaises(AppError) as raised:
            self.service.get_results(123)
        self.assertEqual(raised.exception.status, 404)
        self.client.jobs.get_run_output.assert_not_called()
        self.client.statement_execution.execute_statement.assert_not_called()

    def test_wrong_evaluator_job_is_rejected(self):
        self.child["job_id"] = 300
        with self.assertRaises(AppError):
            self.service.get_results(123)
        self.client.statement_execution.execute_statement.assert_not_called()

    def test_wrong_task_job_is_rejected_before_output_read(self):
        self.parent["tasks"][0]["run_job_task"]["job_id"] = 300
        with self.assertRaises(AppError):
            self.service.get_results(123)
        self.client.jobs.get_run_output.assert_not_called()

    def test_upstream_failure_has_no_old_billing_results(self):
        self.parent["state"]["result_state"] = "FAILED"
        self.parent["tasks"] = [task(state="SKIPPED", result="UPSTREAM_FAILED")]
        results = self.service.get_results(123)
        self.assertIsNone(results["summary"])
        self.assertEqual(results["bills"], [])
        self.assertIn("did not finish successfully", results["message"])
        self.client.jobs.get_run_output.assert_not_called()
        self.client.statement_execution.execute_statement.assert_not_called()

    def test_active_evaluation_does_not_query_sql(self):
        self.parent["state"] = {"life_cycle_state": "RUNNING"}
        self.parent["tasks"] = [task(state="RUNNING", result=None)]
        results = self.service.get_results(123)
        self.assertIsNone(results["execution_id"])
        self.client.statement_execution.execute_statement.assert_not_called()

    def test_missing_child_id_never_falls_back_to_parent_or_latest(self):
        self.client.jobs.get_run_output.return_value = {}
        results = self.service.get_results(123)
        self.assertIsNone(results["execution_id"])
        self.assertIn("could not be identified", results["message"])
        self.client.statement_execution.execute_statement.assert_not_called()

    def test_repaired_parent_or_child_is_blocked(self):
        for target in (self.parent, self.child):
            with self.subTest(target=target["job_id"]):
                target["repair_history"] = [{"type": "ORIGINAL"}, {"type": "REPAIR", "id": 1}]
                self.assertIsNone(self.service.get_results(123)["summary"])
                target.pop("repair_history")
        self.client.statement_execution.execute_statement.assert_not_called()

    def test_original_history_is_not_a_repair(self):
        self.parent["repair_history"] = [{"type": "ORIGINAL", "id": None}]
        self.child["repair_history"] = [SimpleNamespace(type="ORIGINAL", id=None)]
        self.assertEqual(self.service.get_run(123)["execution_id"], "200:456:0")

    def test_evaluator_task_on_later_page_is_found(self):
        first = run(123, 100)
        first["next_page_token"] = "page-two"
        page = {"tasks": [task()]}
        self.client.jobs.get_run.side_effect = [first, page, self.child]
        self.assertEqual(self.service.get_run(123)["execution_id"], "200:456:0")
        self.assertEqual(self.client.jobs.get_run.call_args_list[1].kwargs["page_token"], "page-two")

    def test_repeated_pagination_token_is_rejected(self):
        self.parent["next_page_token"] = "repeat"
        with self.assertRaises(AppError):
            self.service.get_run(123)
        self.assertEqual(self.client.jobs.get_run.call_count, 2)

    def test_sql_uses_exact_named_identity_and_preserves_decimal_money(self):
        summary = statement([("execution_id", "STRING"), ("usable", "BOOLEAN"), ("session_count", "LONG"),
                             ("verdict", "STRING")], [["200:456:0", "true", "1", "FAIL"]])
        bills = statement([("execution_id", "STRING"), ("actual_amount", "DECIMAL")],
                          [["200:456:0", "5.630000"], ["200:456:0", "6.500000"]])
        self.client.statement_execution.execute_statement.side_effect = [summary, bills]
        result = self.service.get_results(123)
        self.assertIs(result["summary"]["usable"], True)
        self.assertEqual(result["summary"]["session_count"], 1)
        self.assertEqual(result["bills"][1]["actual_amount"], "6.500000")
        for call in self.client.statement_execution.execute_statement.call_args_list:
            self.assertEqual(call.kwargs["parameters"], [{"name": "execution_id", "value": "200:456:0", "type": "STRING"}])
            self.assertEqual(call.kwargs["catalog"], "workspace")
            self.assertIn(":execution_id", call.kwargs["statement"])

    def test_failed_receipt_keeps_blocked_summary_without_reading_bills(self):
        self.parent["state"]["result_state"] = "FAILED"
        self.child["state"]["result_state"] = "FAILED"
        response = statement([("execution_id", "STRING"), ("usable", "BOOLEAN"), ("verdict", "STRING")],
                             [["200:456:0", "false", "BLOCKED"]])
        self.client.statement_execution.execute_statement.return_value = response
        result = self.service.get_results(123)
        self.assertEqual(result["summary"]["verdict"], "BLOCKED")
        self.assertEqual(result["bills"], [])
        self.client.statement_execution.execute_statement.assert_called_once()

    def test_mismatched_or_duplicate_summary_is_rejected(self):
        for rows in ([["different"]], [["200:456:0"], ["200:456:0"]]):
            with self.subTest(rows=rows):
                self.client.statement_execution.execute_statement.return_value = statement([("execution_id", "STRING")], rows)
                with self.assertRaises(AppError):
                    self.service.get_results(123)

    def test_missing_bill_rows_are_not_presented_as_complete(self):
        self.client.statement_execution.execute_statement.side_effect = [
            statement([("execution_id", "STRING"), ("usable", "BOOLEAN"), ("session_count", "LONG")],
                      [["200:456:0", "true", "2"]]),
            statement([("execution_id", "STRING")], [["200:456:0"]])]
        with self.assertRaises(AppError):
            self.service.get_results(123)

    def test_statement_poll_and_all_result_chunks_are_read(self):
        api = self.client.statement_execution
        api.execute_statement.return_value = {"statement_id": "s", "status": {"state": "RUNNING"}}
        api.get_statement.return_value = statement([("number", "LONG")], [["1"]], next_index=1, total=2)
        api.get_statement_result_chunk_n.return_value = {"data_array": [["2"]]}
        with patch("app.service.time.sleep"):
            rows = self.service._query("session_bills", "200:456:0")
        self.assertEqual(rows, [{"number": 1}, {"number": 2}])
        api.get_statement_result_chunk_n.assert_called_once_with(statement_id="s", chunk_index=1)

    def test_truncated_manifest_is_rejected(self):
        response = statement([("number", "LONG")], [["1"]])
        response["manifest"]["truncated"] = True
        self.client.statement_execution.execute_statement.return_value = response
        with self.assertRaises(AppError):
            self.service._query("session_bills", "200:456:0")

    def test_incomplete_chunks_are_rejected(self):
        self.client.statement_execution.execute_statement.return_value = statement([("number", "LONG")], [["1"]], total=2)
        with self.assertRaises(AppError):
            self.service._query("session_bills", "200:456:0")

    def test_repeated_chunk_index_is_rejected(self):
        api = self.client.statement_execution
        api.execute_statement.return_value = statement([("number", "LONG")], [["1"]], next_index=1, total=3)
        api.get_statement_result_chunk_n.return_value = {"data_array": [["2"]], "next_chunk_index": 1}
        with self.assertRaises(AppError):
            self.service._query("session_bills", "200:456:0")

    def test_slow_query_is_cancelled_and_returns_retry_message(self):
        api = self.client.statement_execution
        api.execute_statement.return_value = {"statement_id": "s", "status": {"state": "RUNNING"}}
        with patch("app.service.time.monotonic", side_effect=[0, 31]), self.assertRaises(AppError) as raised:
            self.service._query("session_bills", "200:456:0")
        api.cancel_execution.assert_called_once_with(statement_id="s")
        self.assertEqual(raised.exception.status, 503)

    def test_api_exception_details_are_not_leaked(self):
        self.client.jobs.get_run.side_effect = RuntimeError("token=super-secret raw SQL and paths")
        with self.assertRaises(AppError) as raised:
            self.service.get_run(123)
        self.assertNotIn("super-secret", str(raised.exception))
        self.assertNotIn("raw SQL", str(raised.exception))

    def test_current_run_status_fallback_is_supported(self):
        self.parent.pop("state")
        self.parent["status"] = {"state": "TERMINATED", "termination_details": {"code": "SUCCESS"}}
        result = self.service.get_run(123)
        self.assertTrue(result["terminal"])
        self.assertEqual(result["result"], "SUCCESS")

    def test_wrong_run_identity_is_rejected(self):
        self.parent["run_id"] = 777
        with self.assertRaises(AppError):
            self.service.get_results(123)


if __name__ == "__main__":
    unittest.main()
