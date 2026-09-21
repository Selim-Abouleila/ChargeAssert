"""Fail-closed execution receipts for the five deterministic MVP fixture runs.

The existing Gold tables remain current snapshots. Only an immutable snapshot
joined to a SUCCEEDED job_execution row is a result of this job invocation.
This module is a regular workspace file; track_execution.py is its notebook.
"""

from datetime import datetime, timezone
from decimal import Decimal
import json
import re


EXPECTED_RUN_IDS = (
    "smoke-run-v1", "amount-bad-v1", "amount-fixed-v1",
    "mock-amount-bad-v1", "mock-amount-fixed-v1",
)
PIPELINE_TASK_KEYS = (
    "create_run_manifest", "create_ocpp_transaction_events_raw",
    "create_session_lifecycle", "create_tariffs_raw", "create_tariff_history",
    "create_expected_ledger", "create_ocpi_cdrs_raw", "seed_amount_scenarios",
    "generate_mock_billing", "create_actual_ledger", "create_assertion_result",
    "create_release_verdict",
)
CAPTURE_TASK_KEYS = ("begin_execution",) + PIPELINE_TASK_KEYS
FINISH_TASK_KEYS = CAPTURE_TASK_KEYS + ("capture_execution",)
# Short aliases used by the bundle wiring checks.
CAPTURE_TASKS = CAPTURE_TASK_KEYS
FINISH_TASKS = FINISH_TASK_KEYS
ASSERTION_IDS = (
    "oracle_available", "final_cdr_count", "energy_match", "duration_match",
    "currency_match", "tariff_match", "amount_match",
)
COUNT_FIELDS = (
    "required_assertions", "passed_assertions", "failed_assertions",
    "blocked_assertions", "missing_assertions", "duplicate_assertion_keys",
    "unexpected_assertions", "invalid_assertions",
)
COPIED_VERDICT_FIELDS = (
    "baseline_verdict", "candidate_verdict", "verdict",
) + COUNT_FIELDS + ("reason", "first_problem")


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _integer(value, name, minimum):
    _require(type(value) in (int, str) and re.fullmatch(r"[0-9]+", str(value)) is not None,
             f"{name} must be a resolved decimal integer.")
    number = int(value)
    _require(number >= minimum, f"{name} must be at least {minimum}.")
    return number


def execution_identity(job_id, job_run_id, repair_count):
    """Scope receipts to the job, its parent run, and the repair attempt."""
    return ":".join(str(_integer(value, name, minimum)) for value, name, minimum in (
        (job_id, "job_id", 1), (job_run_id, "job_run_id", 1),
        (repair_count, "repair_count", 0),
    ))


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _require(isinstance(value, datetime), "Expected a timestamp.")
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def canonical_json(value):
    def encode(item):
        if isinstance(item, datetime):
            return utc(item).isoformat().replace("+00:00", "Z")
        if isinstance(item, Decimal):
            _require(item.is_finite(), "Snapshot decimal must be finite.")
            return str(item)
        raise TypeError(f"Unsupported snapshot value: {type(item).__name__}")
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":"), default=encode)


def _registration(registration, repair_count):
    _require(isinstance(registration, dict), "The execution was not registered.")
    identity = execution_identity(registration["job_id"], registration["job_run_id"],
                                  registration["repair_count"])
    _require(registration["execution_id"] == identity, "Execution registration identity differs.")
    _require(_integer(repair_count, "repair_count", 0) == registration["repair_count"],
             "Execution repair identity differs.")
    _require(repair_count == 0, "Repair runs are unsupported; start a new complete job run.")
    _require(json.loads(registration["expected_run_ids_json"]) == list(EXPECTED_RUN_IDS),
             "Execution inventory differs from the five required runs.")
    utc(registration["started_at"])
    return identity


def _task_success(task_states, required):
    _require(isinstance(task_states, dict), "Task results must be a JSON object.")
    failures = [f"{key}={task_states.get(key, 'missing')}"
                for key in required if task_states.get(key) != "success"]
    _require(not failures, "Required tasks did not all succeed: " + ", ".join(failures))


def _assertions(rows, run_id):
    expected = {(role, "txn-smoke-v1", rule)
                for role in ("baseline", "candidate") for rule in ASSERTION_IDS}
    keys = [(row["release_role"], row["session_id"], row["assertion_id"]) for row in rows]
    _require(len(keys) == len(expected) and set(keys) == expected,
             f"Assertion snapshot coverage is incomplete or duplicated for {run_id}.")
    _require(all(row["run_id"] == run_id and row["status"] in ("PASS", "FAIL", "BLOCKED")
                 for row in rows), f"Invalid assertion snapshot values for {run_id}.")
    return sorted(rows, key=lambda row: (row["release_role"], row["session_id"], row["assertion_id"]))


def _snapshot_rows(registration, snapshots):
    """Validate complete, internally consistent evidence without current Gold reads."""
    identity = registration["execution_id"]
    run_ids = [row["run_id"] for row in snapshots]
    _require(len(run_ids) == len(EXPECTED_RUN_IDS) and set(run_ids) == set(EXPECTED_RUN_IDS),
             "Execution snapshots are missing, duplicated, or contain unexpected runs.")
    for row in snapshots:
        _require(row["execution_id"] == identity, "Snapshot belongs to another execution.")
        verdict = json.loads(row["verdict_snapshot"])
        assertions = json.loads(row["assertions_snapshot"])
        _require(verdict["run_id"] == row["run_id"], "Snapshot verdict run differs.")
        _require(utc(verdict["evaluated_at"]) >= utc(registration["started_at"]),
                 f"Stale Gold verdict for {row['run_id']}.")
        _require(all(row[field] == verdict[field] for field in COPIED_VERDICT_FIELDS),
                 "Snapshot columns differ from their preserved verdict.")
        _require(all(type(row[field]) is int and row[field] >= 0 for field in COUNT_FIELDS),
                 "Invalid snapshot assertion counts.")
        _require(all(row[field] in ("PASS", "FAIL")
                     for field in ("baseline_verdict", "candidate_verdict", "verdict")),
                 "Invalid financial verdict.")
        _assertions(assertions, row["run_id"])
        _require(row["required_assertions"] == 14
                 and all(row[field] == 0 for field in (
                     "missing_assertions", "duplicate_assertion_keys",
                     "unexpected_assertions", "invalid_assertions")),
                 "Snapshot required assertion coverage differs from the fixture inventory.")
        for status, field in (("PASS", "passed_assertions"), ("FAIL", "failed_assertions"),
                              ("BLOCKED", "blocked_assertions")):
            _require(row[field] == sum(item["status"] == status for item in assertions),
                     "Snapshot assertion counts differ from preserved assertions.")
        for role in ("baseline", "candidate"):
            _require(row[f"{role}_verdict"] != "PASS"
                     or all(item["status"] == "PASS" for item in assertions
                            if item["release_role"] == role),
                     "Snapshot financial verdict differs from preserved assertions.")
        # A conservative FAIL can also be caused by manifest validation in Gold.
        _require(row["verdict"] != "PASS"
                 or row["baseline_verdict"] == row["candidate_verdict"] == "PASS",
                 "Overall snapshot verdict differs from release verdicts.")


def validate_capture(registration, verdicts, assertions, task_states):
    """Freeze the current five verdicts only after this invocation ran all tasks."""
    identity = _registration(registration, 0)
    _require(registration["status"] == "RUNNING", "Only a RUNNING execution can capture Gold.")
    _task_success(task_states, CAPTURE_TASK_KEYS)
    ids = [row["run_id"] for row in verdicts]
    _require(len(ids) == len(EXPECTED_RUN_IDS) and set(ids) == set(EXPECTED_RUN_IDS),
             "Expected exactly one current Gold verdict for every required run.")
    _require(all(row["run_id"] in EXPECTED_RUN_IDS for row in assertions),
             "Unexpected run in assertion snapshot.")
    snapshots = []
    for verdict in sorted(verdicts, key=lambda row: row["run_id"]):
        run_id = verdict["run_id"]
        rows = _assertions([row for row in assertions if row["run_id"] == run_id], run_id)
        snapshots.append(dict(
            {field: verdict[field] for field in COPIED_VERDICT_FIELDS},
            execution_id=identity, run_id=run_id,
            verdict_snapshot=canonical_json(verdict), assertions_snapshot=canonical_json(rows),
        ))
    _snapshot_rows(registration, snapshots)
    return snapshots


def verify_snapshots(existing, expected, *, require_complete=False):
    """Allow identical retry writes; never overwrite or accept conflicting evidence."""
    by_key = {(row["execution_id"], row["run_id"]): row for row in expected}
    _require(len(expected) == len(EXPECTED_RUN_IDS) and len(by_key) == len(expected),
             "Expected snapshots must contain five unique execution/run keys.")
    seen = set()
    for row in existing:
        key = row["execution_id"], row["run_id"]
        _require(key not in seen and by_key.get(key) == row,
                 f"Existing execution evidence differs at {key}; snapshots are immutable.")
        seen.add(key)
    _require(not require_complete or seen == by_key.keys(), "Execution snapshot write is incomplete.")


def evaluate_finish(registration, snapshots, task_states, repair_count):
    """Return operational status; an intentional financial FAIL still completed."""
    try:
        _registration(registration, repair_count)
        _require(registration["status"] in ("RUNNING", "SUCCEEDED"),
                 "A failed execution cannot be promoted to SUCCEEDED.")
        _task_success(task_states, FINISH_TASK_KEYS)
        _snapshot_rows(registration, snapshots)
        return "SUCCEEDED", "All required tasks completed and all five execution snapshots were captured."
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as error:
        return "FAILED", str(error)


def _ensure_tables(spark, execution_table, snapshot_table):
    spark.sql("""CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
      execution_id STRING NOT NULL, job_id STRING NOT NULL, job_run_id STRING NOT NULL,
      repair_count INT NOT NULL, status STRING NOT NULL, expected_run_ids_json STRING NOT NULL,
      started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, task_states_json STRING, reason STRING NOT NULL
    ) USING DELTA COMMENT 'Job execution receipt; only SUCCEEDED permits consuming its frozen Gold evidence.'""",
              args={"table_name": execution_table})
    spark.sql("""CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
      execution_id STRING NOT NULL, run_id STRING NOT NULL,
      baseline_verdict STRING NOT NULL, candidate_verdict STRING NOT NULL, verdict STRING NOT NULL,
      required_assertions BIGINT NOT NULL, passed_assertions BIGINT NOT NULL,
      failed_assertions BIGINT NOT NULL, blocked_assertions BIGINT NOT NULL,
      missing_assertions BIGINT NOT NULL, duplicate_assertion_keys BIGINT NOT NULL,
      unexpected_assertions BIGINT NOT NULL, invalid_assertions BIGINT NOT NULL,
      reason STRING NOT NULL, first_problem STRING, verdict_snapshot STRING NOT NULL,
      assertions_snapshot STRING NOT NULL
    ) USING DELTA COMMENT 'Immutable per-execution verdict and assertion evidence; join a SUCCEEDED job_execution.'""",
              args={"table_name": snapshot_table})


def track(spark, *, mode, job_id, job_run_id, repair_count, job_execution_table_name,
          execution_verdict_table_name, release_verdict_table_name, assertion_result_table_name,
          task_states_json):
    """Run exactly one lifecycle phase using Spark; no credentials or Jobs API."""
    from pyspark.sql import functions as F

    _require(mode in ("begin", "capture", "finish"), "Unknown execution tracking mode.")
    identity = execution_identity(job_id, job_run_id, repair_count)
    job_id, job_run_id, repair_text = identity.split(":")
    repair_count = int(repair_text)
    spark.sql("SET TIME ZONE 'UTC'")
    _ensure_tables(spark, job_execution_table_name, execution_verdict_table_name)

    def registration_rows():
        return [row.asDict(recursive=True) for row in spark.table(job_execution_table_name)
                .where(F.col("execution_id") == identity).limit(2).collect()]

    def snapshot_rows():
        return [row.asDict(recursive=True) for row in spark.table(execution_verdict_table_name)
                .where(F.col("execution_id") == identity).limit(len(EXPECTED_RUN_IDS) + 1).collect()]

    def insert_registration(status, reason):
        spark.sql("""MERGE INTO IDENTIFIER(:table_name) AS target
          USING (SELECT :execution_id AS execution_id, :job_id AS job_id,
            :job_run_id AS job_run_id, CAST(:repair_count AS INT) AS repair_count,
            :status AS status, :inventory AS expected_run_ids_json,
            current_timestamp() AS started_at,
            CASE WHEN :status = 'FAILED' THEN current_timestamp() ELSE CAST(NULL AS TIMESTAMP) END AS finished_at,
            CAST(NULL AS STRING) AS task_states_json, :reason AS reason) AS source
          ON target.execution_id = source.execution_id WHEN NOT MATCHED THEN INSERT *""",
          args={"table_name": job_execution_table_name, "execution_id": identity,
                "job_id": job_id, "job_run_id": job_run_id, "repair_count": repair_count,
                "status": status, "inventory": canonical_json(list(EXPECTED_RUN_IDS)), "reason": reason})

    def update_status(status, reason, states):
        # A FAILED receipt is terminal even if a later invocation tries to repair
        # it with passing task parameters. Never update receipt identity/inventory.
        spark.sql("""UPDATE IDENTIFIER(:table_name) SET
          status = CASE WHEN status = 'FAILED' THEN 'FAILED' ELSE :status END,
          finished_at = COALESCE(finished_at, current_timestamp()),
          task_states_json = :states,
          reason = CASE WHEN status = 'FAILED' THEN reason ELSE :reason END
          WHERE execution_id = :execution_id""",
          args={"table_name": job_execution_table_name, "execution_id": identity,
                "status": status, "reason": reason, "states": canonical_json(states)})

    if mode == "begin":
        status = "FAILED" if repair_count else "RUNNING"
        reason = ("Repair runs are unsupported; start a new complete job run." if repair_count else
                  "Execution registered; waiting for all required tasks.")
        insert_registration(status, reason)
        stored = registration_rows()
        _require(len(stored) == 1, "Execution registration is duplicated.")
        _registration(stored[0], repair_count)
        _require(stored[0]["status"] == "RUNNING", "Execution is already terminal; start a new job run.")
        print(f"Registered execution {identity} with five required scenario runs.")
        return

    # A malformed dynamic reference must leave a FAILED receipt in the finalizer,
    # not raise before that receipt is written.
    try:
        states = json.loads(task_states_json)
        _require(isinstance(states, dict), "Task results must be a JSON object.")
        state_error = None
    except (ValueError, TypeError) as error:
        states, state_error = {}, f"Invalid task result parameters: {error}"
    stored = registration_rows()
    registration = stored[0] if len(stored) == 1 else None
    if mode == "capture":
        _require(state_error is None, state_error)
        verdicts = [row.asDict(recursive=True) for row in spark.table(release_verdict_table_name)
                    .where(F.col("run_id").isin(EXPECTED_RUN_IDS)).limit(len(EXPECTED_RUN_IDS) + 1).collect()]
        assertions = [row.asDict(recursive=True) for row in spark.table(assertion_result_table_name)
                      .where(F.col("run_id").isin(EXPECTED_RUN_IDS)).limit(len(EXPECTED_RUN_IDS) * 14 + 1).collect()]
        expected = validate_capture(registration, verdicts, assertions, states)
        verify_snapshots(snapshot_rows(), expected)
        spark.createDataFrame(expected, schema=spark.table(execution_verdict_table_name).schema) \
            .createOrReplaceTempView("chargeassert_execution_snapshots")
        spark.sql("""MERGE INTO IDENTIFIER(:table_name) AS target
          USING chargeassert_execution_snapshots AS source
          ON target.execution_id = source.execution_id AND target.run_id = source.run_id
          WHEN NOT MATCHED THEN INSERT *""", args={"table_name": execution_verdict_table_name})
        verify_snapshots(snapshot_rows(), expected, require_complete=True)
        print(f"Captured five immutable verdicts and 70 assertions for execution {identity}.")
        return

    status, reason = evaluate_finish(registration, snapshot_rows(), states, repair_count)
    if state_error:
        status, reason = "FAILED", state_error
    if len(stored) > 1:
        status, reason = "FAILED", "Execution registration is duplicated."
    if not stored:
        insert_registration("FAILED", "The begin task did not register this execution.")
    update_status(status, reason, states)
    final = registration_rows()
    _require(len(final) == 1 and final[0]["status"] == status,
             "Execution receipt could not be finalized consistently.")
    if status != "SUCCEEDED":
        raise RuntimeError(f"Execution {identity} FAILED: {reason}")
    print(f"Execution {identity} SUCCEEDED. Financial PASS/FAIL values are in execution_verdict.")
