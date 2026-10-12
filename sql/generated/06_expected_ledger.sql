-- Isolated generated pipeline: every source read and derived deletion is scoped to :run_id.
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
COMMENT 'Independent expected charges for the explicitly selected generated input sessions.'
TBLPROPERTIES (
  'chargeassert.layer' = 'silver',
  'chargeassert.environment' = 'dev'
);

-- The registry is input evidence from validated OCPP generation, independent of
-- release outputs. Empty or duplicate session mappings must fail before pricing.
SELECT assert_true(
  COUNT(*) > 0 AND COUNT(*) = COUNT(DISTINCT session_id)
    AND count_if(COALESCE(
      length(trim(run_id)) > 0 AND length(trim(session_id)) > 0
        AND length(trim(tariff_id)) > 0, FALSE
    )) = COUNT(*),
  'Require a nonempty, unique generated session registry with explicit input tariff IDs.'
)
FROM (SELECT * FROM IDENTIFIER(:input_session_table_name) WHERE run_id = :run_id);
WITH scenario_sessions AS (
  SELECT run_id, session_id, tariff_id
    FROM (SELECT * FROM IDENTIFIER(:input_session_table_name) WHERE run_id = :run_id)
), checked_sessions AS (
  SELECT m.run_id, m.session_id, COUNT(s.run_id) AS session_rows,
    count_if(
      s.status = 'Completed' AND s.started_at IS NOT NULL
        AND s.ended_at >= s.started_at
        AND s.meter_start_wh >= 0 AND s.meter_end_wh >= s.meter_start_wh
    ) AS valid_rows
  FROM scenario_sessions AS m
  LEFT JOIN (SELECT * FROM IDENTIFIER(:session_lifecycle_table_name) WHERE run_id = :run_id) AS s
    ON s.run_id = m.run_id AND s.session_id = m.session_id
  GROUP BY m.run_id, m.session_id
)
SELECT assert_true(
  COUNT(*) > 0 AND count_if(session_rows = 1 AND valid_rows = 1) = COUNT(*),
  'Each registered generated session must have exactly one completed session with valid timestamps and nondecreasing, nonnegative Wh readings.'
)
FROM checked_sessions;

-- Require exactly one effective tariff. Do not let an absent or ambiguous join
-- silently omit a billable session or create multiple expected charges.
-- Until tariff-boundary pricing exists, the whole session must fit the period.
WITH scenario_sessions AS (
  SELECT run_id, session_id, tariff_id
    FROM (SELECT * FROM IDENTIFIER(:input_session_table_name) WHERE run_id = :run_id)
), checked_tariffs AS (
  SELECT m.run_id, m.session_id, COUNT(t.run_id) AS tariff_rows,
    count_if(
      t.currency = 'EUR'
        AND size(t.price_components) = 1
        AND try_element_at(t.price_components, 1).type = 'ENERGY'
        AND try_element_at(t.price_components, 1).price >= 0
        AND try_element_at(t.price_components, 1).step_size = 1
        AND t.source_payload_hash IS NOT NULL
        AND (t.valid_to IS NULL OR s.ended_at <= t.valid_to)
    ) AS valid_rows
  FROM scenario_sessions AS m
  LEFT JOIN (SELECT * FROM IDENTIFIER(:session_lifecycle_table_name) WHERE run_id = :run_id) AS s
    ON s.run_id = m.run_id AND s.session_id = m.session_id
  LEFT JOIN (SELECT * FROM IDENTIFIER(:tariff_history_table_name) WHERE run_id = :run_id) AS t
    ON t.run_id = m.run_id AND t.tariff_id = m.tariff_id
    AND s.started_at >= t.valid_from
    AND (t.valid_to IS NULL OR s.started_at < t.valid_to)
  GROUP BY m.run_id, m.session_id
)
SELECT assert_true(
  COUNT(*) > 0 AND count_if(tariff_rows = 1 AND valid_rows = 1) = COUNT(*),
  'Each registered generated session must have exactly one flat EUR ENERGY tariff, step_size=1, covering the full session.'
)
FROM checked_tariffs;

-- Read only session evidence and tariffs, independently of release outputs.
-- DECIMAL(18,6) * DECIMAL(18,6) yields DECIMAL(37,12); retain that product
-- and apply HALF_UP rounding once to obtain the two-decimal EUR amount.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  WITH scenario_sessions AS (
    SELECT run_id, session_id, tariff_id
    FROM (SELECT * FROM IDENTIFIER(:input_session_table_name) WHERE run_id = :run_id)
  ), session_price AS (
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
    FROM scenario_sessions AS m
    JOIN (SELECT * FROM IDENTIFIER(:session_lifecycle_table_name) WHERE run_id = :run_id) AS s
      ON s.run_id = m.run_id AND s.session_id = m.session_id
    JOIN (SELECT * FROM IDENTIFIER(:tariff_history_table_name) WHERE run_id = :run_id) AS t
      ON t.run_id = s.run_id
      AND t.tariff_id = m.tariff_id
      AND s.started_at >= t.valid_from
      AND (t.valid_to IS NULL OR s.started_at < t.valid_to)
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
