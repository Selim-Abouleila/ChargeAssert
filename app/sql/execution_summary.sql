-- Blank or 'latest' :execution_id selects the latest evaluator attempt, including failures.
-- Results belong to that evaluator only; they do not report parent job status.
WITH requested AS (
  SELECT COALESCE(NULLIF(NULLIF(TRIM(:execution_id), ''), 'latest'),
    (SELECT execution_id
     FROM chargeassert_dev_bronze.generated_job_execution
     ORDER BY started_at DESC, execution_id DESC LIMIT 1)) AS execution_id
), receipts AS (
  SELECT e.execution_id, COUNT(*) AS receipt_rows,
    MAX(e.batch_id) AS batch_id, MAX(e.candidate_mode) AS candidate_mode,
    MAX(e.started_at) AS started_at, MAX(e.finished_at) AS finished_at,
    MAX(e.status) AS evaluation_status, MAX(e.reason) AS evaluation_reason,
    MAX(e.run_id) AS run_id, MAX(e.pipeline_hash) AS pipeline_hash
  FROM chargeassert_dev_bronze.generated_job_execution e
  JOIN requested q ON q.execution_id = e.execution_id
  GROUP BY e.execution_id
), snapshots AS (
  SELECT v.execution_id, COUNT(*) AS snapshot_rows,
    MAX(v.run_id) AS run_id, MAX(v.pipeline_hash) AS pipeline_hash,
    MAX(v.session_count) AS session_count, MAX(v.assertion_count) AS assertion_count,
    MAX(v.baseline_verdict) AS baseline_verdict, MAX(v.candidate_verdict) AS candidate_verdict,
    MAX(v.verdict) AS verdict, MAX(v.assertions_snapshot) AS assertions_snapshot,
    MAX(v.captured_at) AS captured_at
  FROM chargeassert_dev_gold.generated_execution_verdict v
  JOIN requested q ON q.execution_id = v.execution_id
  GROUP BY v.execution_id
), context AS (
  SELECT q.execution_id, e.batch_id, e.candidate_mode, e.started_at, e.finished_at,
    COALESCE(e.evaluation_status, 'MISSING') AS evaluation_status, e.evaluation_reason,
    e.receipt_rows, e.run_id, v.snapshot_rows, v.session_count, v.assertion_count,
    v.baseline_verdict, v.candidate_verdict, v.verdict, v.captured_at, v.assertions_snapshot,
    COALESCE(e.receipt_rows = 1 AND e.evaluation_status = 'SUCCEEDED'
      AND v.snapshot_rows = 1 AND e.run_id = v.run_id AND e.pipeline_hash = v.pipeline_hash
      AND v.session_count > 0 AND v.assertion_count = 14 * v.session_count, FALSE) AS receipt_usable
  FROM requested q
  LEFT JOIN receipts e ON e.execution_id = q.execution_id
  LEFT JOIN snapshots v ON v.execution_id = q.execution_id
), parsed_assertions AS (
  SELECT c.execution_id, explode(from_json(
    CASE WHEN c.receipt_usable THEN c.assertions_snapshot ELSE '[]' END,
    'ARRAY<STRUCT<run_id:STRING,session_id:STRING,release_role:STRING,assertion_id:STRING,status:STRING,expected_value:STRING,actual_value:STRING,difference:STRING>>')) AS a
  FROM context c
), assertions AS (
  SELECT execution_id, a.run_id AS run_id, a.session_id AS session_id,
    a.release_role AS release_role, a.assertion_id AS assertion_id, a.status AS status,
    a.expected_value AS expected_value, a.actual_value AS actual_value, a.difference AS difference
  FROM parsed_assertions
), session_results AS (
  SELECT a.execution_id, a.session_id, a.release_role,
    SUM(CASE WHEN a.status = 'PASS' THEN 1 ELSE 0 END) AS passed_checks,
    SUM(CASE WHEN a.status = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks,
    SUM(CASE WHEN a.status = 'BLOCKED' THEN 1 ELSE 0 END) AS blocked_checks,
    COUNT(*) AS assertion_rows,
    CASE WHEN COUNT(*) = 7 AND COUNT(DISTINCT a.assertion_id) = 7
      AND MIN(CASE WHEN a.run_id = c.run_id AND COALESCE(a.session_id, '') <> ''
        AND a.release_role IN ('baseline', 'candidate')
        AND a.assertion_id IN ('oracle_available', 'final_cdr_count', 'energy_match',
          'duration_match', 'amount_match', 'currency_match', 'tariff_match')
        AND a.status IN ('PASS', 'FAIL', 'BLOCKED') THEN 1 ELSE 0 END) = 1
      THEN 1 ELSE 0 END AS coverage_valid,
    MAX(CASE WHEN a.assertion_id = 'currency_match' THEN a.expected_value END) AS currency,
    MAX(CASE WHEN a.assertion_id = 'amount_match' THEN a.expected_value END) AS expected_amount,
    MAX(CASE WHEN a.assertion_id = 'amount_match' THEN a.actual_value END) AS actual_amount,
    MAX(CASE WHEN a.assertion_id = 'amount_match' THEN a.difference END) AS difference,
    MAX(CASE WHEN a.assertion_id = 'amount_match' THEN a.status END) AS amount_status
  FROM assertions a JOIN context c ON c.execution_id = a.execution_id
  GROUP BY a.execution_id, a.session_id, a.release_role
), totals AS (
  SELECT execution_id, COUNT(*) AS session_release_rows,
    COUNT(DISTINCT session_id) AS observed_sessions, MIN(coverage_valid) AS coverage_valid,
    SUM(assertion_rows) AS observed_assertions, SUM(passed_checks) AS passed_checks,
    SUM(failed_checks) AS failed_checks, SUM(blocked_checks) AS blocked_checks,
    CASE WHEN MIN(CASE WHEN release_role = 'baseline' AND passed_checks <> 7 THEN 0 ELSE 1 END) = 1
      THEN 'PASS' ELSE 'FAIL' END AS baseline_verdict,
    CASE WHEN MIN(CASE WHEN release_role = 'candidate' AND passed_checks <> 7 THEN 0 ELSE 1 END) = 1
      THEN 'PASS' ELSE 'FAIL' END AS candidate_verdict,
    CASE WHEN MIN(passed_checks) = 7 THEN 'PASS' ELSE 'FAIL' END AS verdict
  FROM session_results GROUP BY execution_id
), checked AS (
  SELECT c.*, t.passed_checks, t.failed_checks, t.blocked_checks,
    COALESCE(c.receipt_usable AND t.coverage_valid = 1
      AND t.session_release_rows = 2 * c.session_count AND t.observed_sessions = c.session_count
      AND t.observed_assertions = c.assertion_count
      AND t.baseline_verdict = c.baseline_verdict AND t.candidate_verdict = c.candidate_verdict
      AND t.verdict = c.verdict, FALSE) AS usable
  FROM context c LEFT JOIN totals t ON t.execution_id = c.execution_id
)
SELECT execution_id, batch_id, candidate_mode, started_at, finished_at, evaluation_status,
  CASE WHEN receipt_rows IS NULL THEN 'No evaluator receipt exists for this selection.'
       WHEN receipt_rows <> 1 THEN 'Duplicate evaluator receipts: results cannot be trusted.'
       WHEN evaluation_status <> 'SUCCEEDED' THEN evaluation_reason
       WHEN NOT usable THEN 'Saved evidence is missing, duplicated or inconsistent; billing results are blocked.'
       ELSE evaluation_reason END AS reason,
  usable, captured_at,
  CASE WHEN usable THEN session_count END AS session_count,
  CASE WHEN usable THEN assertion_count END AS assertion_count,
  CASE WHEN usable THEN passed_checks END AS passed_checks,
  CASE WHEN usable THEN failed_checks END AS failed_checks,
  CASE WHEN usable THEN blocked_checks END AS blocked_checks,
  CASE WHEN usable THEN baseline_verdict ELSE 'BLOCKED' END AS baseline_verdict,
  CASE WHEN usable THEN candidate_verdict ELSE 'BLOCKED' END AS candidate_verdict,
  CASE WHEN usable THEN verdict ELSE 'BLOCKED' END AS verdict
FROM checked;
