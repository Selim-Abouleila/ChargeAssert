-- Opt-in operational failure test. Stop before writing Gold so an older PASS
-- remains present and the execution-specific consumer can prove it blocks it.
SELECT assert_true(
  :fail_before_gold = 'false',
  'Deliberate pre-Gold failure (or invalid fail_before_gold value); use false for a normal full run.'
);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL,
  release_role STRING NOT NULL,
  session_id STRING NOT NULL,
  assertion_id STRING NOT NULL,
  expected_value STRING,
  actual_value STRING,
  difference DECIMAL(38,6)
    COMMENT 'Actual minus expected for comparable numeric values; NULL when blocked or nonnumeric.',
  status STRING NOT NULL
    COMMENT 'PASS, FAIL or BLOCKED. BLOCKED is never a successful assertion.',
  severity STRING NOT NULL,
  message STRING NOT NULL,
  evidence STRING NOT NULL
    COMMENT 'JSON containing oracle provenance, session times and reported CDR identities/payload hashes.'
)
USING DELTA
COMMENT 'Independent financial assertions for each session and release; current results, not a release verdict.'
TBLPROPERTIES (
  'chargeassert.layer' = 'gold',
  'chargeassert.environment' = 'dev'
);

-- Reconcile the complete current Silver inputs. In particular, expected sessions
-- drive BOTH releases even when one release returns no CDR. Actual-only sessions
-- and completed sessions without an oracle must not disappear through an inner join.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  WITH release_roles AS (
    SELECT 'baseline' AS release_role
    UNION ALL SELECT 'candidate'
  ), expected_by_session AS (
    SELECT
      run_id, session_id, COUNT(*) AS expected_rows,
      MAX(expected_energy_kwh) AS expected_energy_kwh,
      MAX(expected_amount) AS expected_amount,
      MAX(currency) AS expected_currency,
      MAX(tariff_id) AS expected_tariff_id,
      MAX(tariff_valid_from) AS tariff_valid_from,
      MAX(tariff_payload_hash) AS tariff_payload_hash
    FROM IDENTIFIER(:expected_ledger_table_name)
    GROUP BY run_id, session_id
  ), lifecycle_by_session AS (
    SELECT
      run_id, session_id, COUNT(*) AS lifecycle_rows,
      SUM(CASE WHEN status = 'Completed' AND started_at IS NOT NULL
        AND ended_at >= started_at THEN 1 ELSE 0 END) AS valid_completed_rows,
      MAX(started_at) AS started_at,
      MAX(ended_at) AS ended_at
    FROM IDENTIFIER(:session_lifecycle_table_name)
    GROUP BY run_id, session_id
  ), actual_by_session AS (
    SELECT
      run_id, release_role, session_id, COUNT(*) AS cdr_count,
      -- Scalar values are used ONLY when cdr_count = 1. Multiple financial
      -- records must fail cardinality, never be summed or arbitrarily selected.
      MAX(actual_energy_kwh) AS actual_energy_kwh,
      MAX(actual_duration_hours) AS actual_duration_hours,
      MAX(actual_amount) AS actual_amount,
      MAX(currency) AS actual_currency,
      MAX(tariff_id) AS actual_tariff_id,
      sort_array(collect_list(named_struct(
        'country_code', country_code, 'party_id', party_id, 'cdr_id', cdr_id,
        'energy_kwh', actual_energy_kwh, 'duration_hours', actual_duration_hours,
        'amount', actual_amount, 'currency', currency, 'tariff_id', tariff_id,
        'source_payload_hashes', sort_array(source_payload_hashes)
      ))) AS actual_records
    FROM IDENTIFIER(:actual_ledger_table_name)
    WHERE release_role IN ('baseline', 'candidate') AND cdr_type = 'FINAL'
    GROUP BY run_id, release_role, session_id
  ), session_releases AS (
    SELECT e.run_id, e.session_id, r.release_role
    FROM expected_by_session AS e CROSS JOIN release_roles AS r
    UNION
    SELECT s.run_id, s.session_id, r.release_role
    FROM IDENTIFIER(:session_lifecycle_table_name) AS s
    CROSS JOIN release_roles AS r
    WHERE s.status = 'Completed'
    UNION
    SELECT run_id, session_id, release_role FROM actual_by_session
  ), comparison_inputs AS (
    SELECT
      k.run_id, k.release_role, k.session_id,
      COALESCE(e.expected_rows = 1 AND s.lifecycle_rows = 1
        AND s.valid_completed_rows = 1, FALSE) AS oracle_available,
      COALESCE(a.cdr_count, 0) AS cdr_count,
      e.expected_energy_kwh, e.expected_amount, e.expected_currency,
      lower(e.expected_tariff_id) AS expected_tariff_id,
      CASE WHEN s.lifecycle_rows = 1 AND s.valid_completed_rows = 1 THEN
        CAST(round(
          CAST(timestampdiff(MICROSECOND, s.started_at, s.ended_at) AS DECIMAL(20,0))
            / CAST(3600000000 AS DECIMAL(10,0)), 6
        ) AS DECIMAL(18,6))
      END AS expected_duration_hours,
      a.actual_energy_kwh, a.actual_duration_hours, a.actual_amount,
      a.actual_currency, a.actual_tariff_id,
      to_json(named_struct(
        'rule_version', 'v1',
        'expected_rows', COALESCE(e.expected_rows, 0),
        'lifecycle_rows', COALESCE(s.lifecycle_rows, 0),
        'valid_completed_rows', COALESCE(s.valid_completed_rows, 0),
        'cdr_count', COALESCE(a.cdr_count, 0),
        'expected_tariff', CASE WHEN e.expected_rows = 1 THEN named_struct(
          'tariff_id', e.expected_tariff_id, 'valid_from', e.tariff_valid_from,
          'payload_hash', e.tariff_payload_hash, 'currency', e.expected_currency
        ) END,
        'session', CASE WHEN s.lifecycle_rows = 1 THEN named_struct(
          'started_at', s.started_at, 'ended_at', s.ended_at
        ) END,
        'actual_cdrs', COALESCE(a.actual_records, array())
      )) AS evidence
    FROM session_releases AS k
    LEFT JOIN expected_by_session AS e
      ON e.run_id = k.run_id AND e.session_id = k.session_id
    LEFT JOIN lifecycle_by_session AS s
      ON s.run_id = k.run_id AND s.session_id = k.session_id
    LEFT JOIN actual_by_session AS a
      ON a.run_id = k.run_id AND a.session_id = k.session_id
      AND a.release_role = k.release_role
  ), rules AS (
    SELECT 'oracle_available' AS assertion_id,
      'Require one expected ledger row and one valid completed session.' AS description
    UNION ALL SELECT 'final_cdr_count', 'Require exactly one final CDR per completed billable session.'
    UNION ALL SELECT 'energy_match', 'Reported kWh must equal independent expected kWh.'
    UNION ALL SELECT 'duration_match', 'Reported hours must equal elapsed session hours rounded HALF_UP to six decimals.'
    UNION ALL SELECT 'currency_match', 'Reported currency must equal the expected currency.'
    UNION ALL SELECT 'tariff_match', 'Reported tariff ID must equal the selected tariff ID (case-insensitive).'
    UNION ALL SELECT 'amount_match', 'Reported exclusive-VAT amount must equal the rounded expected amount, without rounding the report.'
  ), rule_inputs AS (
    SELECT c.*, r.assertion_id, r.description,
      CASE
        WHEN r.assertion_id = 'oracle_available' THEN TRUE
        WHEN NOT c.oracle_available THEN FALSE
        WHEN r.assertion_id = 'final_cdr_count' THEN TRUE
        WHEN c.cdr_count <> 1 THEN FALSE
        WHEN r.assertion_id = 'amount_match'
          AND c.actual_currency <> c.expected_currency THEN FALSE
        ELSE TRUE
      END AS can_evaluate,
      CASE WHEN c.oracle_available THEN CAST(CASE r.assertion_id
        WHEN 'final_cdr_count' THEN 1
        WHEN 'energy_match' THEN c.expected_energy_kwh
        WHEN 'duration_match' THEN c.expected_duration_hours
        WHEN 'amount_match' THEN c.expected_amount
      END AS DECIMAL(26,6)) END AS expected_numeric,
      CAST(CASE
        WHEN r.assertion_id = 'final_cdr_count' THEN c.cdr_count
        WHEN c.cdr_count = 1 THEN CASE r.assertion_id
          WHEN 'energy_match' THEN c.actual_energy_kwh
          WHEN 'duration_match' THEN c.actual_duration_hours
          WHEN 'amount_match' THEN c.actual_amount
        END
      END AS DECIMAL(26,6)) AS actual_numeric
    FROM comparison_inputs AS c CROSS JOIN rules AS r
  ), evaluated_rules AS (
    SELECT *,
      CASE assertion_id
        WHEN 'oracle_available' THEN oracle_available
        WHEN 'currency_match' THEN actual_currency = expected_currency
        WHEN 'tariff_match' THEN actual_tariff_id = expected_tariff_id
        ELSE actual_numeric = expected_numeric
      END AS values_match,
      CASE
        WHEN assertion_id = 'oracle_available' THEN 'AVAILABLE'
        WHEN NOT oracle_available THEN NULL
        WHEN assertion_id = 'currency_match' THEN expected_currency
        WHEN assertion_id = 'tariff_match' THEN expected_tariff_id
        ELSE CAST(expected_numeric AS STRING)
      END AS expected_value,
      CASE
        WHEN assertion_id = 'oracle_available' THEN
          CASE WHEN oracle_available THEN 'AVAILABLE' ELSE 'UNAVAILABLE' END
        WHEN assertion_id = 'final_cdr_count' THEN CAST(actual_numeric AS STRING)
        WHEN cdr_count <> 1 THEN NULL
        WHEN assertion_id = 'currency_match' THEN actual_currency
        WHEN assertion_id = 'tariff_match' THEN actual_tariff_id
        ELSE CAST(actual_numeric AS STRING)
      END AS actual_value
    FROM rule_inputs
  )
  SELECT
    run_id, release_role, session_id, assertion_id, expected_value, actual_value,
    CASE WHEN can_evaluate THEN
      CAST(actual_numeric - expected_numeric AS DECIMAL(38,6))
    END AS difference,
    CASE WHEN NOT can_evaluate THEN 'BLOCKED'
      WHEN COALESCE(values_match, FALSE) THEN 'PASS' ELSE 'FAIL' END AS status,
    'ERROR' AS severity,
    CASE
      WHEN assertion_id <> 'oracle_available' AND NOT oracle_available
        THEN 'No unique, valid independent expectation; inspect oracle_available.'
      WHEN assertion_id NOT IN ('oracle_available', 'final_cdr_count') AND cdr_count <> 1
        THEN 'Require exactly one final CDR before comparing values; inspect final_cdr_count.'
      WHEN assertion_id = 'amount_match' AND actual_currency <> expected_currency
        THEN 'Amounts use different currencies; inspect currency_match. No monetary difference calculated.'
      WHEN assertion_id = 'final_cdr_count' AND cdr_count = 0
        THEN 'MISSING_CDR: completed billable session has no final CDR.'
      WHEN assertion_id = 'final_cdr_count' AND cdr_count > 1
        THEN 'DUPLICATE_CDR: multiple final CDRs for one billable session; inspect every record in evidence.'
      ELSE description
    END AS message,
    evidence
  FROM evaluated_rules
) AS source
ON target.run_id = source.run_id
AND target.release_role = source.release_role
AND target.session_id = source.session_id
AND target.assertion_id = source.assertion_id
WHEN MATCHED THEN UPDATE SET
  target.expected_value = source.expected_value,
  target.actual_value = source.actual_value,
  target.difference = source.difference,
  target.status = source.status,
  target.severity = source.severity,
  target.message = source.message,
  target.evidence = source.evidence
WHEN NOT MATCHED THEN INSERT (
  run_id, release_role, session_id, assertion_id, expected_value, actual_value,
  difference, status, severity, message, evidence
) VALUES (
  source.run_id, source.release_role, source.session_id, source.assertion_id,
  source.expected_value, source.actual_value, source.difference, source.status,
  source.severity, source.message, source.evidence
)
-- This table is the current derived snapshot over ALL inputs above. Remove stale
-- results if a source session is removed; never delete Bronze or Silver evidence.
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Only the fixed healthy smoke run must pass here. A financial FAIL/BLOCKED in
-- another run is a stored result, not a SQL exception or a release verdict.
SELECT assert_true(
  COUNT(*) = 14
    AND count_if(release_role = 'baseline') = 7
    AND count_if(release_role = 'candidate') = 7
    AND count_if(session_id = 'txn-smoke-v1' AND status = 'PASS') = 14,
  'Expected fourteen healthy smoke assertions: seven PASS results for each release.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';

SELECT * FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1'
ORDER BY release_role, session_id, assertion_id;
