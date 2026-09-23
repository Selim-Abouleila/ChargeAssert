CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL,
  scenario_id STRING,
  seed BIGINT,
  baseline_sha STRING,
  candidate_sha STRING,
  tariff_hash STRING,
  required_assertions BIGINT NOT NULL,
  passed_assertions BIGINT NOT NULL,
  failed_assertions BIGINT NOT NULL,
  blocked_assertions BIGINT NOT NULL,
  missing_assertions BIGINT NOT NULL,
  duplicate_assertion_keys BIGINT NOT NULL,
  unexpected_assertions BIGINT NOT NULL,
  invalid_assertions BIGINT NOT NULL,
  baseline_verdict STRING NOT NULL,
  candidate_verdict STRING NOT NULL,
  verdict STRING NOT NULL COMMENT 'PASS only with complete passing coverage for both releases and a unique manifest.',
  reason STRING NOT NULL,
  first_problem STRING COMMENT 'Deterministic JSON problem reference; not a chronological first divergence.',
  evidence STRING NOT NULL COMMENT 'JSON with manifest count and separate baseline/candidate coverage summaries.',
  evaluated_at TIMESTAMP NOT NULL COMMENT 'Evaluation time, not proof of upstream execution freshness.'
)
USING DELTA
COMMENT 'Current run-level financial test decision; consume only after a successful current full job.'
TBLPROPERTIES (
  'chargeassert.layer' = 'gold',
  'chargeassert.environment' = 'dev'
);

-- Evaluate all retained runs, including empty manifests and orphan data. Coverage
-- comes from Silver, not the assertions that happened to be produced. A missing
-- release, session or rule must never turn into a smaller all-PASS result set.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  WITH runs AS (
    SELECT run_id FROM IDENTIFIER(:run_manifest_table_name)
    UNION SELECT run_id FROM IDENTIFIER(:expected_ledger_table_name)
    UNION SELECT run_id FROM IDENTIFIER(:session_lifecycle_table_name)
    UNION SELECT run_id FROM IDENTIFIER(:actual_ledger_table_name)
    UNION SELECT run_id FROM IDENTIFIER(:assertion_result_table_name)
  ), manifests AS (
    SELECT run_id, COUNT(*) AS manifest_rows,
      -- Never select arbitrary provenance from duplicate/conflicting manifests.
      CASE WHEN COUNT(*) = 1 THEN MAX(scenario_id) END AS scenario_id,
      CASE WHEN COUNT(*) = 1 THEN MAX(seed) END AS seed,
      CASE WHEN COUNT(*) = 1 THEN MAX(baseline_sha) END AS baseline_sha,
      CASE WHEN COUNT(*) = 1 THEN MAX(candidate_sha) END AS candidate_sha,
      CASE WHEN COUNT(*) = 1 THEN MAX(tariff_hash) END AS tariff_hash
    FROM IDENTIFIER(:run_manifest_table_name)
    GROUP BY run_id
  ), release_roles AS (
    SELECT 'baseline' AS release_role UNION ALL SELECT 'candidate'
  ), required_rules AS (
    -- Version 1 must stay in sync with the rule registry in assertion_result.
    SELECT 'oracle_available' AS assertion_id
    UNION ALL SELECT 'final_cdr_count'
    UNION ALL SELECT 'energy_match'
    UNION ALL SELECT 'duration_match'
    UNION ALL SELECT 'currency_match'
    UNION ALL SELECT 'tariff_match'
    UNION ALL SELECT 'amount_match'
  ), expected_sessions AS (
    SELECT run_id, session_id FROM IDENTIFIER(:expected_ledger_table_name)
    UNION
    SELECT run_id, session_id FROM IDENTIFIER(:session_lifecycle_table_name)
    WHERE status = 'Completed'
  ), session_releases AS (
    SELECT s.run_id, r.release_role, s.session_id
    FROM expected_sessions AS s CROSS JOIN release_roles AS r
    UNION
    SELECT run_id, release_role, session_id
    FROM IDENTIFIER(:actual_ledger_table_name)
    WHERE release_role IN ('baseline', 'candidate') AND cdr_type = 'FINAL'
  ), required_checks AS (
    SELECT s.run_id, s.release_role, s.session_id, r.assertion_id
    FROM session_releases AS s CROSS JOIN required_rules AS r
  ), assertion_keys AS (
    SELECT run_id, release_role, session_id, assertion_id,
      COUNT(*) AS assertion_rows,
      SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed_assertions,
      SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_assertions,
      SUM(CASE WHEN status = 'BLOCKED' THEN 1 ELSE 0 END) AS blocked_assertions,
      SUM(CASE WHEN status IS NULL OR status NOT IN ('PASS', 'FAIL', 'BLOCKED')
        THEN 1 ELSE 0 END) AS invalid_assertions
    FROM IDENTIFIER(:assertion_result_table_name)
    GROUP BY run_id, release_role, session_id, assertion_id
  ), checked_keys AS (
    SELECT a.*,
      CASE WHEN c.run_id IS NULL THEN a.assertion_rows ELSE 0 END AS unexpected_assertions
    FROM assertion_keys AS a
    LEFT JOIN required_checks AS c
      ON c.run_id = a.run_id AND c.release_role = a.release_role
      AND c.session_id = a.session_id AND c.assertion_id = a.assertion_id
  ), coverage_by_release AS (
    SELECT c.run_id, c.release_role, COUNT(*) AS required_assertions,
      SUM(CASE WHEN a.run_id IS NULL THEN 1 ELSE 0 END) AS missing_assertions
    FROM required_checks AS c
    LEFT JOIN assertion_keys AS a
      ON a.run_id = c.run_id AND a.release_role = c.release_role
      AND a.session_id = c.session_id AND a.assertion_id = c.assertion_id
    GROUP BY c.run_id, c.release_role
  ), observed_by_release AS (
    SELECT run_id, release_role,
      SUM(passed_assertions) AS passed_assertions,
      SUM(failed_assertions) AS failed_assertions,
      SUM(blocked_assertions) AS blocked_assertions,
      SUM(CASE WHEN assertion_rows > 1 THEN 1 ELSE 0 END) AS duplicate_assertion_keys,
      SUM(unexpected_assertions) AS unexpected_assertions,
      SUM(invalid_assertions) AS invalid_assertions
    FROM checked_keys
    GROUP BY run_id, release_role
  ), release_counts AS (
    SELECT r.run_id, roles.release_role,
      COALESCE(m.manifest_rows, 0) AS manifest_rows,
      COALESCE(c.required_assertions, 0) AS required_assertions,
      COALESCE(o.passed_assertions, 0) AS passed_assertions,
      COALESCE(o.failed_assertions, 0) AS failed_assertions,
      COALESCE(o.blocked_assertions, 0) AS blocked_assertions,
      COALESCE(c.missing_assertions, 0) AS missing_assertions,
      COALESCE(o.duplicate_assertion_keys, 0) AS duplicate_assertion_keys,
      COALESCE(o.unexpected_assertions, 0) AS unexpected_assertions,
      COALESCE(o.invalid_assertions, 0) AS invalid_assertions
    FROM runs AS r CROSS JOIN release_roles AS roles
    LEFT JOIN manifests AS m ON m.run_id = r.run_id
    LEFT JOIN coverage_by_release AS c
      ON c.run_id = r.run_id AND c.release_role = roles.release_role
    LEFT JOIN observed_by_release AS o
      ON o.run_id = r.run_id AND o.release_role = roles.release_role
  ), release_decisions AS (
    SELECT *,
      CASE WHEN manifest_rows = 1 AND required_assertions > 0
        AND passed_assertions = required_assertions
        AND failed_assertions = 0 AND blocked_assertions = 0
        AND missing_assertions = 0 AND duplicate_assertion_keys = 0
        AND unexpected_assertions = 0 AND invalid_assertions = 0
      THEN 'PASS' ELSE 'FAIL' END AS release_verdict
    FROM release_counts
  ), observed_by_run AS (
    -- Include unknown release roles in run totals; never hide them by pivoting
    -- only the two supported roles. They are unexpected assertions and fail.
    SELECT run_id,
      SUM(passed_assertions) AS passed_assertions,
      SUM(failed_assertions) AS failed_assertions,
      SUM(blocked_assertions) AS blocked_assertions,
      SUM(duplicate_assertion_keys) AS duplicate_assertion_keys,
      SUM(unexpected_assertions) AS unexpected_assertions,
      SUM(invalid_assertions) AS invalid_assertions
    FROM observed_by_release
    GROUP BY run_id
  ), problems AS (
    SELECT r.run_id, 1 AS priority, 'MANIFEST_COUNT' AS problem_type,
      CAST(NULL AS STRING) AS release_role, CAST(NULL AS STRING) AS session_id,
      CAST(NULL AS STRING) AS assertion_id,
      'Require exactly one run manifest; inspect run_manifest.' AS message
    FROM runs AS r LEFT JOIN manifests AS m ON m.run_id = r.run_id
    WHERE COALESCE(m.manifest_rows, 0) <> 1
    UNION ALL
    SELECT run_id, 2, 'NO_REQUIRED_CHECKS', release_role, NULL, NULL,
      'No required checks for this release; an empty run cannot pass.'
    FROM release_counts WHERE required_assertions = 0
    UNION ALL
    SELECT c.run_id, 3, 'MISSING_ASSERTION', c.release_role, c.session_id, c.assertion_id,
      'Required assertion is absent; inspect the session and assertion job.'
    FROM required_checks AS c
    LEFT JOIN assertion_keys AS a
      ON a.run_id = c.run_id AND a.release_role = c.release_role
      AND a.session_id = c.session_id AND a.assertion_id = c.assertion_id
    WHERE a.run_id IS NULL
    UNION ALL
    SELECT run_id, 4, 'DUPLICATE_ASSERTION', release_role, session_id, assertion_id,
      'Multiple rows have the same assertion key; no arbitrary result is accepted.'
    FROM checked_keys WHERE assertion_rows > 1
    UNION ALL
    SELECT run_id, 5, 'UNEXPECTED_ASSERTION', release_role, session_id, assertion_id,
      'Assertion key is outside the required Silver session/release/rule set.'
    FROM checked_keys WHERE unexpected_assertions > 0
    UNION ALL
    SELECT run_id, 6, 'INVALID_STATUS', release_role, session_id, assertion_id,
      'Assertion status must be PASS, FAIL or BLOCKED.'
    FROM checked_keys WHERE invalid_assertions > 0
    UNION ALL
    SELECT run_id, 7, 'FAILED_ASSERTION', release_role, session_id, assertion_id,
      'Assertion failed; inspect its expected/actual values, message and evidence.'
    FROM checked_keys WHERE failed_assertions > 0
    UNION ALL
    SELECT run_id, 8, 'BLOCKED_ASSERTION', release_role, session_id, assertion_id,
      'Assertion could not be evaluated; inspect its prerequisite and evidence.'
    FROM checked_keys WHERE blocked_assertions > 0
  ), ranked_problems AS (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY run_id ORDER BY priority, release_role, session_id, assertion_id
    ) AS problem_rank
    FROM problems
  )
  SELECT
    r.run_id, m.scenario_id, m.seed, m.baseline_sha, m.candidate_sha, m.tariff_hash,
    b.required_assertions + c.required_assertions AS required_assertions,
    COALESCE(o.passed_assertions, 0) AS passed_assertions,
    COALESCE(o.failed_assertions, 0) AS failed_assertions,
    COALESCE(o.blocked_assertions, 0) AS blocked_assertions,
    b.missing_assertions + c.missing_assertions AS missing_assertions,
    COALESCE(o.duplicate_assertion_keys, 0) AS duplicate_assertion_keys,
    COALESCE(o.unexpected_assertions, 0) AS unexpected_assertions,
    COALESCE(o.invalid_assertions, 0) AS invalid_assertions,
    b.release_verdict AS baseline_verdict,
    c.release_verdict AS candidate_verdict,
    CASE WHEN b.release_verdict = 'PASS' AND c.release_verdict = 'PASS'
      AND COALESCE(o.unexpected_assertions, 0) = 0
      AND COALESCE(o.invalid_assertions, 0) = 0
      AND COALESCE(o.duplicate_assertion_keys, 0) = 0
      THEN 'PASS' ELSE 'FAIL' END AS verdict,
    CASE WHEN p.run_id IS NOT NULL THEN concat(p.problem_type, ': ', p.message)
      ELSE 'All required assertions passed exactly once for baseline and candidate.' END AS reason,
    CASE WHEN p.run_id IS NOT NULL THEN to_json(named_struct(
      'problem_type', p.problem_type, 'run_id', p.run_id,
      'release_role', p.release_role, 'session_id', p.session_id,
      'assertion_id', p.assertion_id, 'message', p.message
    )) END AS first_problem,
    to_json(named_struct(
      'rule_version', 'v1', 'manifest_rows', COALESCE(m.manifest_rows, 0),
      'baseline', named_struct(
        'verdict', b.release_verdict, 'required_assertions', b.required_assertions,
        'passed_assertions', b.passed_assertions, 'failed_assertions', b.failed_assertions,
        'blocked_assertions', b.blocked_assertions, 'missing_assertions', b.missing_assertions,
        'duplicate_assertion_keys', b.duplicate_assertion_keys,
        'unexpected_assertions', b.unexpected_assertions, 'invalid_assertions', b.invalid_assertions
      ),
      'candidate', named_struct(
        'verdict', c.release_verdict, 'required_assertions', c.required_assertions,
        'passed_assertions', c.passed_assertions, 'failed_assertions', c.failed_assertions,
        'blocked_assertions', c.blocked_assertions, 'missing_assertions', c.missing_assertions,
        'duplicate_assertion_keys', c.duplicate_assertion_keys,
        'unexpected_assertions', c.unexpected_assertions, 'invalid_assertions', c.invalid_assertions
      )
    )) AS evidence,
    current_timestamp() AS evaluated_at
  FROM runs AS r
  LEFT JOIN manifests AS m ON m.run_id = r.run_id
  JOIN release_decisions AS b ON b.run_id = r.run_id AND b.release_role = 'baseline'
  JOIN release_decisions AS c ON c.run_id = r.run_id AND c.release_role = 'candidate'
  LEFT JOIN observed_by_run AS o ON o.run_id = r.run_id
  LEFT JOIN ranked_problems AS p ON p.run_id = r.run_id AND p.problem_rank = 1
) AS source
ON target.run_id = source.run_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
-- Current derived snapshot over all retained runs, matching assertion_result.
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Financial FAIL verdicts are data, not SQL errors. Only this known healthy
-- fixture is required to pass. A future GitHub consumer must read the verdict
-- AND verify successful execution of the current full job for the requested run.
SELECT assert_true(
  COUNT(*) = 1 AND count_if(
    verdict = 'PASS' AND baseline_verdict = 'PASS' AND candidate_verdict = 'PASS'
      AND required_assertions = 14 AND passed_assertions = 14
      AND failed_assertions = 0 AND blocked_assertions = 0 AND missing_assertions = 0
      AND duplicate_assertion_keys = 0 AND unexpected_assertions = 0
      AND invalid_assertions = 0 AND first_problem IS NULL
  ) = 1,
  'Expected one smoke release verdict: PASS with fourteen passing assertions and no coverage defects.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';

-- Both canned and executable mock pairs must fail for their faulty candidate
-- and pass for their correction. The job succeeds when it detects each defect.
SELECT assert_true(
  COUNT(*) = 4 AND COUNT(DISTINCT run_id) = 4
    AND count_if(
      baseline_verdict = 'PASS' AND required_assertions = 14
        AND blocked_assertions = 0 AND missing_assertions = 0
        AND duplicate_assertion_keys = 0 AND unexpected_assertions = 0
        AND invalid_assertions = 0
    ) = 4
    AND count_if(
      run_id IN ('amount-bad-v1', 'mock-amount-bad-v1')
        AND candidate_verdict = 'FAIL' AND verdict = 'FAIL'
        AND passed_assertions = 13 AND failed_assertions = 1
        AND get_json_object(first_problem, '$.release_role') = 'candidate'
        AND get_json_object(first_problem, '$.session_id') = 'txn-smoke-v1'
        AND get_json_object(first_problem, '$.assertion_id') = 'amount_match'
    ) = 2
    AND count_if(
      run_id IN ('amount-fixed-v1', 'mock-amount-fixed-v1')
        AND candidate_verdict = 'PASS' AND verdict = 'PASS'
        AND passed_assertions = 14 AND failed_assertions = 0 AND first_problem IS NULL
    ) = 2,
  'Canned and executable mock amount pairs must each show bad candidate FAIL (13 PASS, 1 amount FAIL) and fixed candidate PASS (14 PASS), with all baselines PASS.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id IN ('amount-bad-v1', 'amount-fixed-v1',
  'mock-amount-bad-v1', 'mock-amount-fixed-v1');

SELECT assert_true(
  COUNT(*) = 4 AND COUNT(DISTINCT run_id) = 4
    AND count_if(CAST(expected_value AS DECIMAL(18,6)) = CAST(5.63 AS DECIMAL(18,6))) = 4
    AND count_if(
      run_id IN ('amount-bad-v1', 'mock-amount-bad-v1') AND status = 'FAIL'
        AND CAST(actual_value AS DECIMAL(18,6)) = CAST(6.50 AS DECIMAL(18,6))
        AND difference = CAST(0.87 AS DECIMAL(38,6))
    ) = 2
    AND count_if(
      run_id IN ('amount-fixed-v1', 'mock-amount-fixed-v1') AND status = 'PASS'
        AND CAST(actual_value AS DECIMAL(18,6)) = CAST(5.63 AS DECIMAL(18,6))
        AND difference = CAST(0 AS DECIMAL(38,6))
    ) = 2,
  'All amount fixtures must retain the EUR 5.63 independent expectation; bad reports EUR 6.50 (+0.87), fixed reports EUR 5.63.'
)
FROM IDENTIFIER(:assertion_result_table_name)
WHERE run_id IN ('amount-bad-v1', 'amount-fixed-v1',
  'mock-amount-bad-v1', 'mock-amount-fixed-v1')
  AND release_role = 'candidate' AND assertion_id = 'amount_match';

-- A completed session remains billable even when a release produces no CDR.
-- The missing record must fail cardinality and block all five value comparisons.
SELECT assert_true(
  COUNT(*) = 2 AND COUNT(DISTINCT run_id) = 2
    AND count_if(
      baseline_verdict = 'PASS' AND required_assertions = 14
        AND missing_assertions = 0 AND duplicate_assertion_keys = 0
        AND unexpected_assertions = 0 AND invalid_assertions = 0
    ) = 2
    AND count_if(
      run_id = 'mock-missing-cdr-bad-v1'
        AND candidate_verdict = 'FAIL' AND verdict = 'FAIL'
        AND passed_assertions = 8 AND failed_assertions = 1 AND blocked_assertions = 5
        AND get_json_object(first_problem, '$.release_role') = 'candidate'
        AND get_json_object(first_problem, '$.session_id') = 'txn-smoke-v1'
        AND get_json_object(first_problem, '$.assertion_id') = 'final_cdr_count'
    ) = 1
    AND count_if(
      run_id = 'mock-missing-cdr-fixed-v1'
        AND candidate_verdict = 'PASS' AND verdict = 'PASS'
        AND passed_assertions = 14 AND failed_assertions = 0 AND blocked_assertions = 0
        AND first_problem IS NULL
    ) = 1,
  'Missing-CDR mock pair must show bad candidate FAIL (8 PASS, 1 missing-CDR FAIL, 5 BLOCKED) and fixed candidate PASS (14 PASS), with both baselines PASS.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id IN ('mock-missing-cdr-bad-v1', 'mock-missing-cdr-fixed-v1');

SELECT assert_true(
  COUNT(*) = 2 AND COUNT(DISTINCT run_id) = 2
    AND count_if(
      session_id = 'txn-smoke-v1'
        AND CAST(expected_value AS DECIMAL(18,6)) = CAST(1 AS DECIMAL(18,6))
    ) = 2
    AND count_if(
      run_id = 'mock-missing-cdr-bad-v1' AND status = 'FAIL'
        AND CAST(actual_value AS DECIMAL(18,6)) = CAST(0 AS DECIMAL(18,6))
        AND difference = CAST(-1 AS DECIMAL(38,6))
    ) = 1
    AND count_if(
      run_id = 'mock-missing-cdr-fixed-v1' AND status = 'PASS'
        AND CAST(actual_value AS DECIMAL(18,6)) = CAST(1 AS DECIMAL(18,6))
        AND difference = CAST(0 AS DECIMAL(38,6))
    ) = 1,
  'Missing-CDR candidate must report zero final CDRs against one expected; its correction must report exactly one.'
)
FROM IDENTIFIER(:assertion_result_table_name)
WHERE run_id IN ('mock-missing-cdr-bad-v1', 'mock-missing-cdr-fixed-v1')
  AND release_role = 'candidate' AND assertion_id = 'final_cdr_count';

SELECT assert_true(
  COUNT(*) = 5 AND COUNT(DISTINCT assertion_id) = 5
    AND count_if(
      session_id = 'txn-smoke-v1' AND status = 'BLOCKED'
        AND expected_value IS NOT NULL AND actual_value IS NULL AND difference IS NULL
    ) = 5,
  'Missing candidate CDR must block all five value checks without fabricating an actual value or monetary difference.'
)
FROM IDENTIFIER(:assertion_result_table_name)
WHERE run_id = 'mock-missing-cdr-bad-v1' AND release_role = 'candidate'
  AND assertion_id IN ('energy_match', 'duration_match', 'currency_match', 'tariff_match', 'amount_match');

SELECT run_id, baseline_verdict, candidate_verdict, verdict,
  required_assertions, passed_assertions, failed_assertions,
  blocked_assertions, missing_assertions, reason
FROM IDENTIFIER(:table_name)
WHERE run_id IN ('smoke-run-v1', 'amount-bad-v1', 'amount-fixed-v1',
  'mock-amount-bad-v1', 'mock-amount-fixed-v1',
  'mock-missing-cdr-bad-v1', 'mock-missing-cdr-fixed-v1')
ORDER BY run_id;
