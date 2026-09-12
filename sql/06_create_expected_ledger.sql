CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL
    COMMENT 'ChargeAssert test run for this independently calculated charge.',
  session_id STRING NOT NULL
    COMMENT 'Logical session priced from validated meter evidence.',
  tariff_id STRING NOT NULL
    COMMENT 'Tariff selected for this session.',
  tariff_valid_from TIMESTAMP NOT NULL
    COMMENT 'Inclusive start of the selected tariff period.',
  tariff_payload_hash STRING NOT NULL
    COMMENT 'Original tariff payload hash retained from Silver tariff history.',
  currency STRING NOT NULL
    COMMENT 'Currency of the expected charge; currently EUR.',
  expected_energy_kwh DECIMAL(18,6) NOT NULL
    COMMENT 'Difference between final and initial Wh meter readings, divided by 1000.',
  price_per_kwh DECIMAL(18,6) NOT NULL
    COMMENT 'Selected flat ENERGY price per kWh.',
  expected_amount_unrounded DECIMAL(37,12) NOT NULL
    COMMENT 'Exact product of the decimal energy and price before currency rounding.',
  expected_amount DECIMAL(18,2) NOT NULL
    COMMENT 'Expected EUR amount, rounded once at session total using HALF_UP to two decimals.'
)
USING DELTA
COMMENT 'Independent expected session charges; first implementation covers the smoke fixture.'
TBLPROPERTIES (
  'chargeassert.layer' = 'silver',
  'chargeassert.environment' = 'dev'
);

-- This first oracle deliberately prices only the verified smoke session.
-- A scenario-defined session-to-tariff mapping is needed before generalizing.
SELECT assert_true(
  COUNT(*) = 1
    AND count_if(
      status = 'Completed'
        AND started_at IS NOT NULL
        AND ended_at >= started_at
        AND meter_start_wh >= 0
        AND meter_end_wh >= meter_start_wh
    ) = 1,
  'Expected exactly one completed smoke session with valid timestamps and nondecreasing, nonnegative Wh meter readings.'
)
FROM IDENTIFIER(:session_lifecycle_table_name)
WHERE run_id = 'smoke-run-v1'
  AND session_id = 'txn-smoke-v1';

-- Require exactly one effective tariff. Do not let an absent or ambiguous join
-- silently omit a billable session or create multiple expected charges.
-- Until tariff-boundary pricing exists, the whole session must fit the period.
SELECT assert_true(
  COUNT(*) = 1
    AND count_if(
      t.currency = 'EUR'
        AND size(t.price_components) = 1
        AND try_element_at(t.price_components, 1).type = 'ENERGY'
        AND try_element_at(t.price_components, 1).price >= 0
        AND try_element_at(t.price_components, 1).step_size = 1
        AND t.source_payload_hash IS NOT NULL
        AND (t.valid_to IS NULL OR s.ended_at <= t.valid_to)
    ) = 1,
  'Expected exactly one flat EUR ENERGY tariff for the smoke session, with step_size=1 and a validity period covering the full session.'
)
FROM IDENTIFIER(:session_lifecycle_table_name) AS s
JOIN IDENTIFIER(:tariff_history_table_name) AS t
  ON t.run_id = s.run_id
  AND t.tariff_id = 'tariff-smoke-v1'
  AND s.started_at >= t.valid_from
  AND (t.valid_to IS NULL OR s.started_at < t.valid_to)
WHERE s.run_id = 'smoke-run-v1'
  AND s.session_id = 'txn-smoke-v1';

-- Read only session evidence and tariffs, independently of release outputs.
-- DECIMAL(18,6) * DECIMAL(18,6) yields DECIMAL(37,12); retain that product
-- and apply HALF_UP rounding once to obtain the two-decimal EUR amount.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  WITH session_price AS (
    SELECT
      s.run_id,
      s.session_id,
      t.tariff_id,
      t.valid_from AS tariff_valid_from,
      t.source_payload_hash AS tariff_payload_hash,
      t.currency,
      CAST(
        (s.meter_end_wh - s.meter_start_wh) / 1000.0
        AS DECIMAL(18,6)
      ) AS expected_energy_kwh,
      try_element_at(t.price_components, 1).price AS price_per_kwh
    FROM IDENTIFIER(:session_lifecycle_table_name) AS s
    JOIN IDENTIFIER(:tariff_history_table_name) AS t
      ON t.run_id = s.run_id
      AND t.tariff_id = 'tariff-smoke-v1'
      AND s.started_at >= t.valid_from
      AND (t.valid_to IS NULL OR s.started_at < t.valid_to)
    WHERE s.run_id = 'smoke-run-v1'
      AND s.session_id = 'txn-smoke-v1'
  ), calculated_charge AS (
    SELECT
      *,
      expected_energy_kwh * price_per_kwh AS expected_amount_unrounded
    FROM session_price
  )
  SELECT
    *,
    CAST(round(expected_amount_unrounded, 2) AS DECIMAL(18,2)) AS expected_amount
  FROM calculated_charge
) AS source
ON target.run_id = source.run_id
AND target.session_id = source.session_id
WHEN MATCHED THEN UPDATE SET
  target.tariff_id = source.tariff_id,
  target.tariff_valid_from = source.tariff_valid_from,
  target.tariff_payload_hash = source.tariff_payload_hash,
  target.currency = source.currency,
  target.expected_energy_kwh = source.expected_energy_kwh,
  target.price_per_kwh = source.price_per_kwh,
  target.expected_amount_unrounded = source.expected_amount_unrounded,
  target.expected_amount = source.expected_amount
WHEN NOT MATCHED THEN INSERT (
  run_id,
  session_id,
  tariff_id,
  tariff_valid_from,
  tariff_payload_hash,
  currency,
  expected_energy_kwh,
  price_per_kwh,
  expected_amount_unrounded,
  expected_amount
) VALUES (
  source.run_id,
  source.session_id,
  source.tariff_id,
  source.tariff_valid_from,
  source.tariff_payload_hash,
  source.currency,
  source.expected_energy_kwh,
  source.price_per_kwh,
  source.expected_amount_unrounded,
  source.expected_amount
);

-- Fixed expectations make the half-cent rounding decision observable.
SELECT assert_true(
  COUNT(*) = 1
    AND count_if(
      ledger.session_id = 'txn-smoke-v1'
        AND ledger.tariff_id = 'tariff-smoke-v1'
        AND ledger.tariff_valid_from = CAST('2026-01-01T00:00:00Z' AS TIMESTAMP)
        AND ledger.tariff_payload_hash = tariff.source_payload_hash
        AND ledger.currency = 'EUR'
        AND ledger.expected_energy_kwh = CAST(12.5 AS DECIMAL(18,6))
        AND ledger.price_per_kwh = CAST(0.45 AS DECIMAL(18,6))
        AND ledger.expected_amount_unrounded = CAST(5.625 AS DECIMAL(37,12))
        AND ledger.expected_amount = CAST(5.63 AS DECIMAL(18,2))
    ) = 1,
  'Expected one smoke ledger row: 12.5 kWh at EUR 0.45/kWh, EUR 5.625 unrounded and EUR 5.63 rounded HALF_UP, with matching tariff provenance.'
)
FROM IDENTIFIER(:table_name) AS ledger
LEFT JOIN IDENTIFIER(:tariff_history_table_name) AS tariff
  ON tariff.run_id = ledger.run_id
  AND tariff.tariff_id = ledger.tariff_id
  AND tariff.valid_from = ledger.tariff_valid_from
WHERE ledger.run_id = 'smoke-run-v1';

SELECT *
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';
