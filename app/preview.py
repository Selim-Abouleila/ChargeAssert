"""Explicit local preview: sample data only, no Databricks calls."""
from datetime import datetime, timezone
from decimal import Decimal
import threading
import time
from uuid import UUID


class AppError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


class SimulationService:
    def __init__(self):
        self._lock = threading.Lock()
        self._requests = {}
        self._runs = {
            900100: {"run_id": 900100, "created": time.time() - 120,
                     "parameters": {"session_count": 100, "seed": 42, "candidate_mode": "amount_error", "batch_id": "preview-900100"}}
        }

    def get_config(self):
        return {"defaults": {"session_count": 100, "seed": 42, "candidate_mode": "amount_error"},
                "limits": {"session_count": 1000, "seed": 4294967295}, "dashboard_url": "", "preview": True}

    def start_run(self, payload):
        try:
            token = str(UUID(payload["idempotency_token"]))
            count, seed = payload.get("session_count", 100), payload.get("seed", 42)
            mode = payload.get("candidate_mode", "amount_error")
            if type(count) is not int or not 1 <= count <= 1000:
                raise ValueError()
            if type(seed) is not int or not 0 <= seed <= 4294967295:
                raise ValueError()
            if mode not in ("amount_error", "healthy"):
                raise ValueError()
        except (KeyError, ValueError, TypeError, AttributeError):
            raise AppError("Choose a valid mode, 1–1,000 sessions and a valid seed.") from None
        with self._lock:
            if token in self._requests:
                return {"run_id": self._requests[token]}
            run_id = max(self._runs) + 1
            self._runs[run_id] = {"run_id": run_id, "created": time.time(), "parameters": {
                "session_count": count, "seed": seed, "candidate_mode": mode, "batch_id": f"preview-{run_id}"}}
            self._requests[token] = run_id
            return {"run_id": run_id}

    def list_runs(self):
        with self._lock:
            ids = sorted(self._runs, reverse=True)[:20]
        return [self.get_run(run_id) for run_id in ids]

    def get_run(self, run_id):
        with self._lock:
            raw = self._runs.get(run_id)
        if raw is None:
            raise AppError("This sample run does not exist.", 404)
        elapsed = time.time() - raw["created"]
        terminal = elapsed >= 6
        steps = []
        for index, (key, label) in enumerate((("generate_sessions", "Generate sessions"),
                                             ("ingest_ocpp_files", "Load events"),
                                             ("evaluate_generated_batch", "Check bills"))):
            done = elapsed >= (index + 1) * 2
            active = elapsed >= index * 2
            steps.append({"key": key, "label": label, "state": "TERMINATED" if done else "RUNNING" if active else "PENDING", "result": "SUCCESS" if done else None})
        return {"run_id": run_id, "started_at": datetime.fromtimestamp(raw["created"], timezone.utc).isoformat().replace("+00:00", "Z"),
                "state": "TERMINATED" if terminal else "RUNNING", "result": "SUCCESS" if terminal else None,
                "terminal": terminal, "run_page_url": "", "parameters": dict(raw["parameters"]), "steps": steps,
                "execution_id": f"preview:{run_id}:0" if terminal else None, "message": "Sample simulation is running; no Databricks job was started." if not terminal else ""}

    def get_results(self, run_id):
        run = self.get_run(run_id)
        if not run["terminal"]:
            return {"execution_id": None, "summary": None, "bills": [], "message": "The sample simulation is still running."}
        count, mode = run["parameters"]["session_count"], run["parameters"]["candidate_mode"]
        bad = mode == "amount_error"
        bills = []
        for index in range(count):
            expected = Decimal(400 + ((index * 137 + run["parameters"]["seed"]) % 2200)) / 100
            for role in ("baseline", "candidate"):
                failure = bad and role == "candidate"
                difference = Decimal("0.87") if failure else Decimal("0.00")
                bills.append({"execution_id": run["execution_id"], "session_id": f"sample-{run_id}-session-{index + 1:04d}",
                              "release_role": role, "currency": "EUR", "expected_amount": f"{expected:.2f}",
                              "actual_amount": f"{expected + difference:.2f}", "difference": f"{difference:.2f}",
                              "amount_status": "FAIL" if failure else "PASS", "verdict": "FAIL" if failure else "PASS",
                              "passed_checks": 6 if failure else 7, "failed_checks": int(failure), "blocked_checks": 0})
        return {"execution_id": run["execution_id"], "summary": {"usable": True, "session_count": count,
                "assertion_count": 14 * count, "passed_checks": (13 if bad else 14) * count, "failed_checks": count if bad else 0,
                "blocked_checks": 0, "baseline_verdict": "PASS", "candidate_verdict": "FAIL" if bad else "PASS",
                "verdict": "FAIL" if bad else "PASS", "evaluation_status": "SUCCEEDED"}, "bills": bills,
                "message": "Preview only: these are sample results, not results from your Databricks workspace."}
