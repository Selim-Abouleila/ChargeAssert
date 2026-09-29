-- Set :execution_id to the exact ID printed by evaluate_generated_batch.
-- A missing, failed or incomplete attempt returns BLOCKED; no older PASS is used.
WITH requested AS (SELECT :execution_id AS execution_id),
receipts AS (
  SELECT execution_id, COUNT(*) AS receipt_rows, MAX(status) AS status,
    MAX(run_id) AS run_id, MAX(pipeline_hash) AS pipeline_hash, MAX(reason) AS reason
  FROM workspace.chargeassert_dev_bronze.generated_job_execution
  WHERE execution_id = :execution_id
  GROUP BY execution_id
), snapshots AS (
  SELECT execution_id, COUNT(*) AS snapshot_rows, MAX(run_id) AS run_id,
    MAX(pipeline_hash) AS pipeline_hash, MAX(session_count) AS session_count,
    MAX(assertion_count) AS assertion_count, MAX(baseline_verdict) AS baseline_verdict,
    MAX(candidate_verdict) AS candidate_verdict, MAX(verdict) AS verdict
  FROM workspace.chargeassert_dev_gold.generated_execution_verdict
  WHERE execution_id = :execution_id
  GROUP BY execution_id
), checked AS (
  SELECT q.execution_id, e.status, e.reason, v.run_id, v.session_count, v.assertion_count,
    v.baseline_verdict, v.candidate_verdict, v.verdict,
    COALESCE(e.receipt_rows = 1 AND e.status = 'SUCCEEDED' AND v.snapshot_rows = 1
      AND e.run_id = v.run_id AND e.pipeline_hash = v.pipeline_hash
      AND v.session_count > 0 AND v.assertion_count = 14 * v.session_count, FALSE) AS usable
  FROM requested q LEFT JOIN receipts e ON e.execution_id = q.execution_id
  LEFT JOIN snapshots v ON v.execution_id = q.execution_id
)
SELECT execution_id, COALESCE(status, 'MISSING') AS job_status, run_id,
  CASE WHEN usable THEN baseline_verdict ELSE 'BLOCKED' END AS baseline_verdict,
  CASE WHEN usable THEN candidate_verdict ELSE 'BLOCKED' END AS candidate_verdict,
  CASE WHEN usable THEN verdict ELSE 'BLOCKED' END AS verdict,
  session_count, assertion_count, reason
FROM checked;

-- Session results come from this attempt's saved assertions, not mutable Gold.
-- Run after the first query confirms a usable completed attempt.
WITH evidence AS (
  SELECT v.assertions_snapshot
  FROM workspace.chargeassert_dev_gold.generated_execution_verdict v
  JOIN workspace.chargeassert_dev_bronze.generated_job_execution e
    ON e.execution_id = v.execution_id AND e.run_id = v.run_id
      AND e.pipeline_hash = v.pipeline_hash
  WHERE e.execution_id = :execution_id AND e.status = 'SUCCEEDED'
    AND (SELECT COUNT(*) FROM workspace.chargeassert_dev_bronze.generated_job_execution
         WHERE execution_id = :execution_id) = 1
    AND (SELECT COUNT(*) FROM workspace.chargeassert_dev_gold.generated_execution_verdict
         WHERE execution_id = :execution_id) = 1
), assertions AS (
  SELECT explode(from_json(assertions_snapshot,
    'ARRAY<STRUCT<session_id:STRING,release_role:STRING,assertion_id:STRING,status:STRING>>')) AS a
  FROM evidence
)
SELECT a.session_id, a.release_role,
  CASE WHEN COUNT(*) <> 7 OR COUNT(DISTINCT a.assertion_id) <> 7 THEN 'BLOCKED'
       WHEN count_if(a.status = 'PASS') = 7 THEN 'PASS' ELSE 'FAIL' END AS verdict,
  count_if(a.status = 'PASS') AS passed_checks,
  count_if(a.status = 'FAIL') AS failed_checks,
  count_if(a.status = 'BLOCKED') AS blocked_checks
FROM assertions
GROUP BY a.session_id, a.release_role
ORDER BY a.session_id, a.release_role;
