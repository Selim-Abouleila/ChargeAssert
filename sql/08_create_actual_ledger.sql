CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL,
  release_role STRING NOT NULL,
  country_code STRING NOT NULL,
  party_id STRING NOT NULL,
  cdr_id STRING NOT NULL
    COMMENT 'Case-normalized CDR identity within its owner, release and run.',
  session_id STRING NOT NULL
    COMMENT 'Reported session ID; different CDR IDs for this session remain separate.',
  cdr_type STRING NOT NULL
    COMMENT 'Currently FINAL; credit CDR normalization is not supported yet.',
  started_at TIMESTAMP NOT NULL,
  ended_at TIMESTAMP NOT NULL,
  currency STRING NOT NULL,
  actual_energy_kwh DECIMAL(18,6) NOT NULL
    COMMENT 'Reported total_energy, without recalculation from meter evidence.',
  actual_duration_hours DECIMAL(18,6) NOT NULL
    COMMENT 'Reported total_time in hours, without recalculation from timestamps.',
  actual_amount DECIMAL(18,6) NOT NULL
    COMMENT 'Reported total_cost.excl_vat; retain fractional cents without rounding.',
  tariff_id STRING NOT NULL
    COMMENT 'Reported tariff ID from the single supported charging period.',
  source_payload_hashes ARRAY<STRING> NOT NULL
    COMMENT 'Sorted distinct Bronze hashes of equivalent normalized billing records.'
)
USING DELTA
COMMENT 'Normalized reported CDRs, independent of the expected ledger; preserves distinct financial records.'
TBLPROPERTIES (
  'chargeassert.layer' = 'silver',
  'chargeassert.environment' = 'dev'
);

-- Parse the original CDR, not Bronze convenience projections or oracle values.
-- Invalid evidence raises an error with its run/release/hash; Bronze is retained.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  WITH parsed_cdrs AS (
    SELECT
      run_id,
      release_role,
      payload,
      payload_hash,
      from_json(
        payload,
        'STRUCT<country_code: STRING, party_id: STRING, id: STRING, session_id: STRING, start_date_time: TIMESTAMP, end_date_time: TIMESTAMP, currency: STRING, total_energy: DECIMAL(18,6), total_time: DECIMAL(18,6), total_cost: STRUCT<excl_vat: DECIMAL(18,6)>, credit: BOOLEAN, charging_periods: ARRAY<STRUCT<tariff_id: STRING>>>',
        map('mode', 'FAILFAST')
      ) AS cdr
    FROM IDENTIFIER(:raw_cdrs_table_name)
  ), validated_cdrs AS (
    SELECT
      run_id,
      release_role,
      upper(cdr.country_code) AS country_code,
      upper(cdr.party_id) AS party_id,
      CASE WHEN COALESCE(
        length(trim(run_id)) > 0
          AND release_role IN ('baseline', 'candidate')
          AND payload_hash = sha2(payload, 256)
          AND cdr.country_code RLIKE '^[A-Za-z]{2}$'
          AND cdr.party_id RLIKE '^[A-Za-z0-9]{3}$'
          AND length(trim(cdr.id)) BETWEEN 1 AND 36
          AND cdr.id = trim(cdr.id)
          AND length(trim(cdr.session_id)) BETWEEN 1 AND 36
          AND cdr.session_id = trim(cdr.session_id)
          AND cdr.start_date_time IS NOT NULL
          AND cdr.end_date_time >= cdr.start_date_time
          AND cdr.currency RLIKE '^[A-Z]{3}$'
          AND cdr.total_energy >= 0
          AND cdr.total_time >= 0
          AND cdr.total_cost.excl_vat >= 0
          -- Reject unsupported precision instead of silently rounding evidence.
          AND get_json_object(payload, '$.total_energy') RLIKE '^[0-9]+([.][0-9]{1,6})?$'
          AND get_json_object(payload, '$.total_time') RLIKE '^[0-9]+([.][0-9]{1,6})?$'
          AND get_json_object(payload, '$.total_cost.excl_vat') RLIKE '^[0-9]+([.][0-9]{1,6})?$'
          AND (cdr.credit IS NULL OR cdr.credit = FALSE)
          AND size(cdr.charging_periods) = 1
          AND length(trim(try_element_at(cdr.charging_periods, 1).tariff_id)) BETWEEN 1 AND 36
          AND try_element_at(cdr.charging_periods, 1).tariff_id = trim(try_element_at(cdr.charging_periods, 1).tariff_id),
        FALSE
      ) THEN lower(cdr.id)
      ELSE raise_error(concat(
        'Invalid or unsupported CDR at run/release/hash ', run_id, '/', release_role, '/', payload_hash,
        ': require valid identity/hash/times, nonnegative plain decimals with at most six fractional digits, a final CDR and one reported tariff. Inspect ocpi_cdrs_raw.'
      )) END AS cdr_id,
      cdr.session_id AS session_id,
      'FINAL' AS cdr_type,
      cdr.start_date_time AS started_at,
      cdr.end_date_time AS ended_at,
      cdr.currency AS currency,
      cdr.total_energy AS actual_energy_kwh,
      cdr.total_time AS actual_duration_hours,
      cdr.total_cost.excl_vat AS actual_amount,
      lower(try_element_at(cdr.charging_periods, 1).tariff_id) AS tariff_id,
      payload_hash
    FROM parsed_cdrs
  ), normalized_cdrs AS (
    -- Collapse equivalent billing fields, retaining every source hash. The CDR
    -- identity is part of this grouping: two IDs for one session stay two rows.
    SELECT
      run_id, release_role, country_code, party_id, cdr_id, session_id,
      cdr_type, started_at, ended_at, currency, actual_energy_kwh,
      actual_duration_hours, actual_amount, tariff_id,
      sort_array(collect_set(payload_hash)) AS source_payload_hashes
    FROM validated_cdrs
    GROUP BY
      run_id, release_role, country_code, party_id, cdr_id, session_id,
      cdr_type, started_at, ended_at, currency, actual_energy_kwh,
      actual_duration_hours, actual_amount, tariff_id
  ), counted_cdrs AS (
    SELECT
      *,
      COUNT(*) OVER (
        PARTITION BY run_id, release_role, country_code, party_id, cdr_id
      ) AS variant_count
    FROM normalized_cdrs
  )
  SELECT
    run_id,
    release_role,
    country_code,
    party_id,
    CASE WHEN variant_count = 1 THEN cdr_id
      ELSE raise_error(concat(
        'Conflicting normalized CDR values for ', run_id, '/', release_role, '/', country_code, '/', party_id, '/', cdr_id,
        '. Inspect all payload versions in ocpi_cdrs_raw.'
      )) END AS cdr_id,
    session_id,
    cdr_type,
    started_at,
    ended_at,
    currency,
    actual_energy_kwh,
    actual_duration_hours,
    actual_amount,
    tariff_id,
    source_payload_hashes
  FROM counted_cdrs
) AS source
ON target.run_id = source.run_id
AND target.release_role = source.release_role
AND target.country_code = source.country_code
AND target.party_id = source.party_id
AND target.cdr_id = source.cdr_id
WHEN MATCHED THEN UPDATE SET
  target.session_id = source.session_id,
  target.cdr_type = source.cdr_type,
  target.started_at = source.started_at,
  target.ended_at = source.ended_at,
  target.currency = source.currency,
  target.actual_energy_kwh = source.actual_energy_kwh,
  target.actual_duration_hours = source.actual_duration_hours,
  target.actual_amount = source.actual_amount,
  target.tariff_id = source.tariff_id,
  target.source_payload_hashes = source.source_payload_hashes
WHEN NOT MATCHED THEN INSERT (
  run_id, release_role, country_code, party_id, cdr_id, session_id,
  cdr_type, started_at, ended_at, currency, actual_energy_kwh,
  actual_duration_hours, actual_amount, tariff_id, source_payload_hashes
) VALUES (
  source.run_id, source.release_role, source.country_code, source.party_id,
  source.cdr_id, source.session_id, source.cdr_type, source.started_at,
  source.ended_at, source.currency, source.actual_energy_kwh,
  source.actual_duration_hours, source.actual_amount, source.tariff_id,
  source.source_payload_hashes
);

-- Fixture check only. Financial differences in other runs remain reported
-- values for the future Gold assertions; this task never looks up expectations.
SELECT assert_true(
  COUNT(*) = 2
    AND count_if(release_role = 'baseline') = 1
    AND count_if(release_role = 'candidate') = 1
    AND count_if(
      country_code = 'FR'
        AND party_id = 'CAS'
        AND cdr_id = 'cdr-smoke-v1'
        AND session_id = 'txn-smoke-v1'
        AND cdr_type = 'FINAL'
        AND started_at = CAST('2026-08-22T10:00:00Z' AS TIMESTAMP)
        AND ended_at = CAST('2026-08-22T11:00:00Z' AS TIMESTAMP)
        AND currency = 'EUR'
        AND actual_energy_kwh = CAST(12.5 AS DECIMAL(18,6))
        AND actual_duration_hours = CAST(1 AS DECIMAL(18,6))
        AND actual_amount = CAST(5.63 AS DECIMAL(18,6))
        AND tariff_id = 'tariff-smoke-v1'
        AND size(source_payload_hashes) >= 1
    ) = 2,
  'Expected two actual smoke ledger rows, one per release: 12.5 kWh, one hour and EUR 5.63, with source hashes.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';

SELECT *
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1'
ORDER BY release_role;
