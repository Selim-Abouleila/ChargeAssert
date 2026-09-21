-- Bind the exact execution_id returned for the requested Databricks job attempt.
-- Never substitute the most recent successful execution for a failed/missing one.
-- This query always returns one row, including when registration never happened.
WITH requested AS (
  SELECT CAST(:execution_id AS STRING) AS execution_id, CAST(:run_id AS STRING) AS run_id
), executions AS (
  SELECT execution_id, COUNT(*) AS execution_rows,
    MAX(status) AS execution_status, MAX(reason) AS execution_reason
  FROM IDENTIFIER(:job_execution_table_name)
  WHERE execution_id = :execution_id
  GROUP BY execution_id
), snapshots AS (
  SELECT execution_id, run_id, COUNT(*) AS snapshot_rows,
    MAX(baseline_verdict) AS baseline_verdict,
    MAX(candidate_verdict) AS candidate_verdict,
    MAX(verdict) AS verdict, MAX(reason) AS reason,
    MAX(required_assertions) AS required_assertions,
    MAX(passed_assertions) AS passed_assertions,
    MAX(failed_assertions) AS failed_assertions,
    MAX(blocked_assertions) AS blocked_assertions,
    MAX(missing_assertions) AS missing_assertions,
    MAX(duplicate_assertion_keys) AS duplicate_assertion_keys,
    MAX(unexpected_assertions) AS unexpected_assertions,
    MAX(invalid_assertions) AS invalid_assertions
  FROM IDENTIFIER(:execution_verdict_table_name)
  WHERE execution_id = :execution_id AND run_id = :run_id
  GROUP BY execution_id, run_id
), checked AS (
  SELECT r.execution_id, r.run_id,
    COALESCE(e.execution_rows, 0) AS execution_rows,
    COALESCE(s.snapshot_rows, 0) AS snapshot_rows,
    e.execution_status, e.execution_reason,
    s.baseline_verdict, s.candidate_verdict, s.verdict, s.reason,
    s.passed_assertions, s.failed_assertions,
    CASE WHEN e.execution_rows = 1 AND e.execution_status = 'SUCCEEDED'
      AND s.snapshot_rows = 1
      AND s.baseline_verdict IN ('PASS', 'FAIL')
      AND s.candidate_verdict IN ('PASS', 'FAIL')
      AND s.verdict IN ('PASS', 'FAIL')
      AND (s.verdict <> 'PASS' OR (
        s.baseline_verdict = 'PASS' AND s.candidate_verdict = 'PASS'
        AND s.required_assertions > 0 AND s.passed_assertions = s.required_assertions
        AND s.failed_assertions = 0 AND s.blocked_assertions = 0
        AND s.missing_assertions = 0 AND s.duplicate_assertion_keys = 0
        AND s.unexpected_assertions = 0 AND s.invalid_assertions = 0
      ))
    THEN 1 ELSE 0 END AS usable
  FROM requested AS r
  LEFT JOIN executions AS e ON e.execution_id = r.execution_id
  LEFT JOIN snapshots AS s ON s.execution_id = r.execution_id AND s.run_id = r.run_id
)
SELECT execution_id, run_id,
  CASE WHEN execution_rows = 0 THEN 'MISSING'
    WHEN execution_rows <> 1 THEN 'INVALID' ELSE execution_status END AS execution_status,
  CASE WHEN usable = 1 THEN baseline_verdict END AS baseline_verdict,
  CASE WHEN usable = 1 THEN candidate_verdict END AS candidate_verdict,
  CASE WHEN usable = 1 THEN verdict ELSE 'BLOCKED' END AS verdict,
  CASE WHEN usable = 1 THEN passed_assertions END AS passed_assertions,
  CASE WHEN usable = 1 THEN failed_assertions END AS failed_assertions,
  CASE WHEN execution_rows <> 1 THEN 'No unique registration for the requested execution.'
    WHEN execution_status IS NULL OR execution_status <> 'SUCCEEDED'
      THEN concat('Execution is incomplete or failed: ', COALESCE(execution_reason, 'No completion recorded.'))
    WHEN snapshot_rows <> 1 THEN 'No unique verdict snapshot for this execution and scenario.'
    WHEN usable = 0 THEN 'Invalid financial verdict snapshot.'
    ELSE reason END AS reason
FROM checked;
