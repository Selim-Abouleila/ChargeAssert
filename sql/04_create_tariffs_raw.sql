CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL
    COMMENT 'ChargeAssert test run that this tariff applies to.',
  tariff_id STRING NOT NULL
    COMMENT 'Unique identifier for the tariff version.',
  valid_from TIMESTAMP NOT NULL
    COMMENT 'Start of validity for this tariff.',
  valid_to TIMESTAMP
    COMMENT 'End of validity for this tariff (optional).',
  currency STRING NOT NULL
    COMMENT 'ISO 4217 currency code (e.g., EUR).',
  payload STRING NOT NULL
    COMMENT 'Original OCPI Tariff payload preserved as JSON text.',
  payload_hash STRING NOT NULL
    COMMENT 'SHA-256 hash of the exact payload text for integrity checks.'
)
USING DELTA
COMMENT 'Raw and immutable OCPI tariff evidence for ChargeAssert test runs.'
TBLPROPERTIES (
  'chargeassert.layer' = 'bronze',
  'chargeassert.environment' = 'dev'
);

-- Seed one deterministic smoke tariff (0.45 EUR/kWh flat rate)
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  SELECT
    run_id,
    tariff_id,
    valid_from,
    valid_to,
    currency,
    payload,
    sha2(payload, 256) AS payload_hash
  FROM VALUES
    (
      'smoke-run-v1',
      'tariff-smoke-v1',
      CAST('2026-01-01T00:00:00Z' AS TIMESTAMP),
      CAST('2026-12-31T23:59:59Z' AS TIMESTAMP),
      'EUR',
      '{"id":"tariff-smoke-v1","currency":"EUR","elements":[{"price_components":[{"type":"ENERGY","price":0.4500,"step_size":1}]}]}'
    )
  AS tariffs (
    run_id,
    tariff_id,
    valid_from,
    valid_to,
    currency,
    payload
  )
) AS source
ON target.run_id = source.run_id
AND target.tariff_id = source.tariff_id
WHEN NOT MATCHED THEN INSERT (
  run_id,
  tariff_id,
  valid_from,
  valid_to,
  currency,
  payload,
  payload_hash
) VALUES (
  source.run_id,
  source.tariff_id,
  source.valid_from,
  source.valid_to,
  source.currency,
  source.payload,
  source.payload_hash
);

-- Fail the job unless the smoke tariff exists and is intact
SELECT assert_true(
  COUNT(*) = 1
    AND count_if(tariff_id = 'tariff-smoke-v1') = 1
    AND count_if(currency = 'EUR') = 1
    AND count_if(payload_hash = sha2(payload, 256)) = 1,
  'Golden tariff must contain exactly one valid row with correct hash.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';

-- Expose the tariff in the task output
SELECT *
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';
