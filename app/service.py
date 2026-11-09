"""Launch the configured simulation job and read evidence for its exact child run."""
from datetime import datetime, timezone
from itertools import islice
import os
from pathlib import Path
import re
import time
from urllib.parse import quote, urlsplit
from uuid import UUID


class AppError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


DEFAULTS = {"session_count": 100, "seed": 42, "candidate_mode": "amount_error"}
STEPS = (("generate_sessions", "Generate sessions"),
         ("ingest_ocpp_files", "Load charging events"),
         ("evaluate_generated_batch", "Check the bills"))
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
SQL_DIRECTORY = Path(__file__).resolve().parent / "sql"


def _field(value, name, default=None):
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


def _value(value):
    return getattr(value, "value", value)


def _integer(value, name, minimum, maximum):
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
        raise AppError(f"{name} must be a whole number between {minimum} and {maximum}.")
    result = int(value)
    if not minimum <= result <= maximum:
        raise AppError(f"{name} must be between {minimum} and {maximum}.")
    return result


def _state(run):
    legacy = _field(run, "state")
    current = _field(run, "status")
    state = _value(_field(legacy, "life_cycle_state")) or _value(_field(current, "state")) or "PENDING"
    result = _value(_field(legacy, "result_state"))
    if not result:
        result = _value(_field(_field(current, "termination_details"), "code"))
    return state, result


def _repaired(run):
    # Databricks returns history.id only for repairs; ORIGINAL entries omit it.
    return any(_value(_field(item, "type")) == "REPAIR" or _field(item, "id") is not None
               for item in (_field(run, "repair_history") or []))


def _parameters(run):
    result = {}
    for parameter in _field(run, "job_parameters") or []:
        name = _field(parameter, "name")
        if name in (*DEFAULTS, "batch_id"):
            result[name] = _field(parameter, "value")
    return result


def _timestamp(milliseconds):
    if not milliseconds:
        return None
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _sql_parameter(execution_id):
    from databricks.sdk.service.sql import StatementParameterListItem
    return StatementParameterListItem(name="execution_id", value=execution_id, type="STRING")


class SimulationService:
    """All job IDs and SQL sources are server-owned; request data supplies values only."""

    def __init__(self, client=None, config=None):
        self._client = client
        settings = os.environ if config is None else config
        try:
            self.pipeline_job_id = _integer(settings.get("PIPELINE_JOB_ID"), "Pipeline job ID", 1, 2**63 - 1)
            self.evaluator_job_id = _integer(settings.get("EVALUATOR_JOB_ID"), "Evaluator job ID", 1, 2**63 - 1)
            self.warehouse_id = settings["SQL_WAREHOUSE_ID"]
            self.catalog = settings["CATALOG_NAME"]
            self.dashboard_id = settings["DASHBOARD_ID"]
            if not all(isinstance(v, str) and v.strip() for v in
                       (self.warehouse_id, self.catalog, self.dashboard_id)):
                raise ValueError("Empty configuration")
        except (KeyError, ValueError, AppError) as exc:
            raise AppError("The app is not configured yet. Deploy its bundle resources and start the app.", 503) from exc

    @property
    def client(self):
        if self._client is None:
            try:
                from databricks.sdk import WorkspaceClient
                from databricks.sdk.config import Config
                self._client = WorkspaceClient(config=Config(http_timeout_seconds=15, retry_timeout_seconds=5))
            except Exception as exc:
                raise AppError("The app could not connect to Databricks. Check its resource permissions.", 503) from exc
        return self._client

    def _call(self, operation, **kwargs):
        try:
            return operation(**kwargs)
        except AppError:
            raise
        except Exception as exc:
            # API exceptions can contain SQL, workspace paths or tokens. Keep them out of the UI.
            raise AppError("Databricks could not complete this request. Check the job and app resource permissions, then retry.", 503) from exc

    def _host(self):
        host = str(_field(self.client.config, "host", "")).rstrip("/")
        parsed = urlsplit(host)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise AppError("The Databricks workspace URL is not configured correctly.", 503)
        return host

    def get_config(self):
        return {"defaults": dict(DEFAULTS), "limits": {"session_count": 1000, "seed": 4294967295},
                "dashboard_url": f"{self._host()}/dashboardsv3/{quote(self.dashboard_id, safe='')}/published",
                "preview": False}

    def _run_view(self, run):
        state, result = _state(run)
        run_id = _field(run, "run_id")
        return {"run_id": str(run_id), "started_at": _timestamp(_field(run, "start_time")),
                "state": state, "result": result,
                "run_page_url": f"{self._host()}/jobs/{self.pipeline_job_id}/runs/{run_id}",
                "parameters": _parameters(run)}

    def list_runs(self):
        try:
            runs = self.client.jobs.list_runs(job_id=self.pipeline_job_id, limit=20, expand_tasks=False)
            return [self._run_view(run) for run in islice(runs, 20)
                    if _field(run, "job_id") == self.pipeline_job_id]
        except AppError:
            raise
        except Exception as exc:
            raise AppError("Recent runs could not be loaded. Check the app's job permissions and retry.", 503) from exc

    def start_run(self, payload):
        if not isinstance(payload, dict):
            raise AppError("Choose the simulation settings before starting a run.")
        if set(payload) - {*DEFAULTS, "idempotency_token"}:
            raise AppError("Only billing mode, session count and seed can be changed.")
        mode = payload.get("candidate_mode", DEFAULTS["candidate_mode"])
        if mode not in ("healthy", "amount_error"):
            raise AppError("Choose correct bills or deliberate overcharges.")
        count = _integer(payload.get("session_count", 100), "Session count", 1, 1000)
        seed = _integer(payload.get("seed", 42), "Seed", 0, 4294967295)
        try:
            token = str(UUID(str(payload.get("idempotency_token", ""))))
        except (ValueError, AttributeError) as exc:
            raise AppError("The run request needs a valid request ID. Refresh the page and try again.") from exc
        response = self._call(self.client.jobs.run_now, job_id=self.pipeline_job_id,
                              job_parameters={"candidate_mode": mode, "session_count": str(count), "seed": str(seed)},
                              idempotency_token=token)
        # run_now returns a Wait; read its response without waiting for job completion.
        run_id = _field(_field(response, "response", response), "run_id")
        if not isinstance(run_id, int) or run_id <= 0:
            raise AppError("Databricks did not return a run ID. Retry the same request.", 503)
        # Jobs IDs are int64. Keep them opaque strings in JSON for JavaScript clients.
        return {"run_id": str(run_id)}

    def _read_run(self, run_id):
        run = self._call(self.client.jobs.get_run, run_id=run_id, include_history=True)
        tasks = list(_field(run, "tasks") or [])
        history = list(_field(run, "repair_history") or [])
        token = _field(run, "next_page_token")
        seen = set()
        while token:
            if token in seen or len(seen) >= 10:
                raise AppError("The run details are incomplete. Open the Lakeflow run for details.", 503)
            seen.add(token)
            page = self._call(self.client.jobs.get_run, run_id=run_id, include_history=True, page_token=token)
            tasks.extend(_field(page, "tasks") or [])
            history.extend(_field(page, "repair_history") or [])
            token = _field(page, "next_page_token")
        # SDK objects stay untouched; this small view also accepts plain dictionaries in tests.
        fields = ("run_id", "job_id", "start_time", "state", "status", "job_parameters")
        result = {name: _field(run, name) for name in fields}
        result.update(tasks=tasks, repair_history=history)
        if result["run_id"] != run_id:
            raise AppError("Databricks returned a different run. No results were selected.", 409)
        return result

    def _parent(self, run_id):
        run_id = _integer(run_id, "Run ID", 1, 2**63 - 1)
        parent = self._read_run(run_id)
        if parent["job_id"] != self.pipeline_job_id:
            raise AppError("This run does not belong to the configured simulation job.", 404)
        return parent

    def _child(self, parent):
        if _repaired(parent):
            return None, "This run was repaired. Start a fresh simulation to get an unambiguous billing result."
        tasks = [task for task in parent["tasks"] if _field(task, "task_key") == "evaluate_generated_batch"]
        if len(tasks) > 1:
            raise AppError("The evaluator task is ambiguous. No billing result was selected.", 409)
        if not tasks or _state(tasks[0])[0] not in TERMINAL:
            return None, "Billing results will be available after the evaluation finishes."
        task = tasks[0]
        state, task_result = _state(task)
        if state == "SKIPPED" or task_result in ("UPSTREAM_FAILED", "UPSTREAM_CANCELED", "EXCLUDED", "SKIPPED"):
            return None, "The evaluation did not run. Check the generation and ingestion steps in Lakeflow."
        if _field(_field(task, "run_job_task"), "job_id") != self.evaluator_job_id:
            raise AppError("The evaluator task points to a different job. No billing result was selected.", 409)
        task_run_id = _field(task, "run_id")
        if not task_run_id:
            return None, "The evaluator task has no run ID yet."
        output = self._call(self.client.jobs.get_run_output, run_id=task_run_id)
        child_id = _field(_field(output, "run_job_output"), "run_id")
        if not child_id:
            return None, "The evaluator child run could not be identified. Open the Lakeflow run for details."
        child = self._read_run(child_id)
        if child["job_id"] != self.evaluator_job_id:
            raise AppError("The evaluator run belongs to a different job. No billing result was selected.", 409)
        if _repaired(child):
            return None, "The evaluation was repaired. Start a fresh simulation to read its billing result here."
        if _state(child)[0] not in TERMINAL:
            return None, "The evaluation is still running."
        return child, ""

    def _status(self, parent, child, message):
        result = self._run_view(parent)
        result["terminal"] = result["state"] in TERMINAL
        tasks = {_field(task, "task_key"): task for task in parent["tasks"]}
        result["steps"] = [{"key": key, "label": label, "state": _state(tasks.get(key))[0],
                            "result": _state(tasks.get(key))[1]} for key, label in STEPS]
        result["execution_id"] = self._execution_id(child) if child else None
        if result["terminal"] and result["result"] not in ("SUCCESS", "SUCCEEDED"):
            message = "The pipeline did not finish successfully. Check its Lakeflow run. " + message
        result["message"] = message.strip()
        return result

    @staticmethod
    def _execution_id(child):
        return f"{child['job_id']}:{child['run_id']}:0"

    def get_run(self, run_id):
        parent = self._parent(run_id)
        child, message = self._child(parent)
        return self._status(parent, child, message)

    def get_results(self, run_id):
        parent = self._parent(run_id)
        child, message = self._child(parent)
        if not child:
            status = self._status(parent, child, message)
            return {"execution_id": None, "summary": None, "bills": [], "message": status["message"]}
        execution_id = self._execution_id(child)
        summaries = self._query("execution_summary", execution_id)
        if len(summaries) != 1 or summaries[0].get("execution_id") != execution_id:
            raise AppError("The saved summary does not match this evaluation. No billing result was shown.", 409)
        summary = summaries[0]
        bills = self._query("session_bills", execution_id) if summary.get("usable") is True else []
        if any(row.get("execution_id") != execution_id for row in bills):
            raise AppError("Saved bill details do not match this evaluation. No billing result was shown.", 409)
        if summary.get("usable") is True and len(bills) != 2 * summary.get("session_count", 0):
            raise AppError("Saved bill details are incomplete. No billing result was shown.", 409)
        return {"execution_id": execution_id, "summary": summary, "bills": bills,
                "message": summary.get("reason") or ""}

    def _query(self, name, execution_id):
        statement = (SQL_DIRECTORY / f"{name}.sql").read_text(encoding="utf-8")
        api = self.client.statement_execution
        deadline = time.monotonic() + 30
        response = self._call(api.execute_statement, statement=statement, warehouse_id=self.warehouse_id,
                              catalog=self.catalog, parameters=[_sql_parameter(execution_id)],
                              wait_timeout="10s", row_limit=2001)
        statement_id = _field(response, "statement_id")
        state = _value(_field(_field(response, "status"), "state"))
        while state in ("PENDING", "RUNNING") and time.monotonic() < deadline:
            if not statement_id:
                break
            time.sleep(1)
            response = self._call(api.get_statement, statement_id=statement_id)
            state = _value(_field(_field(response, "status"), "state"))
        if state != "SUCCEEDED":
            if statement_id and state in ("PENDING", "RUNNING"):
                try:
                    api.cancel_execution(statement_id=statement_id)
                except Exception:
                    pass
            raise AppError("Saved results could not be read yet. Check the SQL warehouse and table access, then retry.", 503)
        manifest = _field(response, "manifest")
        columns = _field(_field(manifest, "schema"), "columns") or []
        expected_count = _field(manifest, "total_row_count")
        if (_field(manifest, "truncated") or not columns or not isinstance(expected_count, int)
                or not 0 <= expected_count <= 2000):
            raise AppError("The SQL result is incomplete or too large. No billing result was shown.", 503)
        rows = []
        chunk = _field(response, "result")
        seen = set()
        while chunk is not None:
            for values in _field(chunk, "data_array") or []:
                if len(values) != len(columns):
                    raise AppError("The SQL result has an unexpected shape.", 503)
                rows.append({_field(column, "name"): self._cell(value, _value(_field(column, "type_name")))
                             for column, value in zip(columns, values)})
            next_index = _field(chunk, "next_chunk_index")
            if next_index is None:
                break
            if (next_index in seen or len(seen) >= 100 or len(rows) > 2000
                    or not statement_id or time.monotonic() >= deadline):
                raise AppError("The SQL result could not be read completely.", 503)
            seen.add(next_index)
            chunk = self._call(api.get_statement_result_chunk_n, statement_id=statement_id, chunk_index=next_index)
        if len(rows) != expected_count:
            raise AppError("The SQL result is missing rows. No billing result was shown.", 503)
        return rows

    @staticmethod
    def _cell(value, type_name):
        if value is None:
            return None
        try:
            if type_name in ("BYTE", "SHORT", "INT", "LONG", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
                return int(value)
            if type_name == "BOOLEAN":
                if str(value).lower() not in ("true", "false"):
                    raise ValueError("Invalid boolean")
                return str(value).lower() == "true"
        except (ValueError, TypeError) as exc:
            raise AppError("The SQL result contains an invalid value.", 503) from exc
        # Decimal money remains a string, avoiding a float rounding change in the API.
        return value
