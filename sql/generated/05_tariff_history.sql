-- Isolated generated pipeline: every source read and derived deletion is scoped to :run_id.
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL
    COMMENT 'ChargeAssert test run that owns this tariff snapshot.',
  tariff_id STRING NOT NULL
    COMMENT 'Tariff identifier within the test run.',
  valid_from TIMESTAMP NOT NULL
    COMMENT 'Inclusive start of the effective tariff period.',
  valid_to TIMESTAMP
    COMMENT 'Exclusive end of the effective period; NULL means open-ended.',
  currency STRING NOT NULL
    COMMENT 'Currency of the normalized price components; currently EUR.',
  price_components ARRAY<STRUCT<type: STRING, price: DECIMAL(18,6), step_size: INT>> NOT NULL
    COMMENT 'Typed tariff components; currently one flat ENERGY price in EUR/kWh.',
  source_payload_hash STRING NOT NULL
    COMMENT 'SHA-256 of the exact Bronze payload used to produce this period.'
)
USING DELTA
COMMENT 'Validated effective tariff periods, isolated by ChargeAssert test run.'
TBLPROPERTIES (
  'chargeassert.layer' = 'silver',
  'chargeassert.environment' = 'dev'
);

-- Start with the existing flat EUR energy contract. Fail explicitly when a
-- tariff needs pricing rules that the first expected ledger will not support.
-- FAILFAST rejects malformed JSON and incompatible field types. COALESCE
-- ensures missing required fields fail validation instead of becoming UNKNOWN.
WITH parsed_tariffs AS (
  SELECT
    *,
    from_json(
      payload,
      'STRUCT<id: STRING, currency: STRING, elements: ARRAY<STRUCT<price_components: ARRAY<STRUCT<type: STRING, price: DECIMAL(18,6), step_size: INT>>>>>',
      map('mode', 'FAILFAST')
    ) AS tariff
  FROM (SELECT * FROM IDENTIFIER(:raw_tariffs_table_name) WHERE run_id = :run_id)
)
SELECT assert_true(
  COUNT(*) = count_if(COALESCE(
    length(trim(run_id)) > 0
      AND length(trim(tariff_id)) > 0
      AND payload_hash = sha2(payload, 256)
      AND tariff.id = tariff_id
      AND tariff.currency = currency
      AND currency = 'EUR'
      AND valid_from IS NOT NULL
      AND (valid_to IS NULL OR valid_to > valid_from)
      AND size(tariff.elements) = 1
      AND size(try_element_at(tariff.elements, 1).price_components) = 1
      AND get_json_object(payload, '$.elements[0].price_components[0].type') = 'ENERGY'
      AND try_cast(get_json_object(payload, '$.elements[0].price_components[0].price') AS DECIMAL(18,6)) >= 0
      AND get_json_object(payload, '$.elements[0].price_components[0].step_size') = '1'
      AND get_json_object(payload, '$.elements[0].price_components[0].vat') IS NULL
      AND get_json_object(payload, '$.elements[0].restrictions') IS NULL
      AND get_json_object(payload, '$.min_price') IS NULL
      AND get_json_object(payload, '$.max_price') IS NULL
      AND get_json_object(payload, '$.start_date_time') IS NULL
      AND get_json_object(payload, '$.end_date_time') IS NULL,
    FALSE
  )),
  'Invalid tariff evidence or unsupported pricing: require matching hashes/IDs/currency, valid periods, and one unrestricted EUR ENERGY component with nonnegative price, step_size=1 and no VAT or price bounds. Use validity columns for dates.'
)
FROM parsed_tariffs;

-- Exact duplicate evidence is harmless. Conflicting rows at the same key and
-- overlapping periods must fail; adjacent [valid_from, valid_to) periods are OK.
WITH distinct_tariffs AS (
  SELECT DISTINCT
    run_id, tariff_id, valid_from, valid_to, currency, payload, payload_hash
  FROM (SELECT * FROM IDENTIFIER(:raw_tariffs_table_name) WHERE run_id = :run_id)
), ordered_periods AS (
  SELECT
    *,
    LEAD(valid_from) OVER (
      PARTITION BY run_id, tariff_id
      ORDER BY valid_from
    ) AS next_valid_from
  FROM distinct_tariffs
)
SELECT assert_true(
  COUNT(*) = 0,
  'Conflicting or overlapping tariff periods for the same run_id and tariff_id.'
)
FROM ordered_periods
WHERE next_valid_from IS NOT NULL
  AND (valid_to IS NULL OR valid_to > next_valid_from);

-- Rebuild normalized values from Bronze; reruns keep one row per effective
-- period and cannot mix tariffs from different runs that reuse the same ID.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  SELECT DISTINCT
    run_id,
    tariff_id,
    valid_from,
    valid_to,
    currency,
    try_element_at(from_json(
      payload,
      'STRUCT<id: STRING, currency: STRING, elements: ARRAY<STRUCT<price_components: ARRAY<STRUCT<type: STRING, price: DECIMAL(18,6), step_size: INT>>>>>',
      map('mode', 'FAILFAST')
    ).elements, 1).price_components AS price_components,
    payload_hash AS source_payload_hash
  FROM (SELECT * FROM IDENTIFIER(:raw_tariffs_table_name) WHERE run_id = :run_id)
) AS source
ON target.run_id = source.run_id
AND target.tariff_id = source.tariff_id
AND target.valid_from = source.valid_from
WHEN MATCHED THEN UPDATE SET
  target.valid_to = source.valid_to,
  target.currency = source.currency,
  target.price_components = source.price_components,
  target.source_payload_hash = source.source_payload_hash
WHEN NOT MATCHED THEN INSERT (
  run_id,
  tariff_id,
  valid_from,
  valid_to,
  currency,
  price_components,
  source_payload_hash
) VALUES (
  source.run_id,
  source.tariff_id,
  source.valid_from,
  source.valid_to,
  source.currency,
  source.price_components,
  source.source_payload_hash
);
