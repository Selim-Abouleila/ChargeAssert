-- Keep raw release outputs independent of the expected ledger. Only the run
-- manifest must exist before these fixed, synthetic CDR responses are loaded.
SELECT assert_true(
  COUNT(*) = 1,
  'Expected exactly one smoke-run-v1 row in run_manifest before loading CDR evidence.'
)
FROM IDENTIFIER(:run_manifest_table_name)
WHERE run_id = 'smoke-run-v1';

CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL
    COMMENT 'ChargeAssert run that captured this release output.',
  release_role STRING NOT NULL
    COMMENT 'Controlled system that produced the output: baseline or candidate.',
  country_code STRING
    COMMENT 'CDR owner country_code extracted from the payload.',
  party_id STRING
    COMMENT 'CDR owner party_id; the owner and cdr_id identify the financial record.',
  cdr_id STRING
    COMMENT 'CDR id extracted from the payload; different IDs must remain separate.',
  session_id STRING
    COMMENT 'Reported logical session ID, mapped to the OCPP transaction ID in the smoke fixture.',
  cdr_type STRING
    COMMENT 'ChargeAssert classification from the credit flag: FINAL or CREDIT; invalid flags yield NULL.',
  currency STRING
    COMMENT 'Reported currency; retained without applying billing validation.',
  total_cost DECIMAL(18,6)
    COMMENT 'Convenience projection of total_cost.excl_vat, without cent rounding; exact source remains in payload.',
  payload STRING NOT NULL
    COMMENT 'Original OCPI-shaped CDR body preserved as exact JSON text.',
  payload_hash STRING NOT NULL
    COMMENT 'SHA-256 of the exact payload text; also distinguishes conflicting versions of one CDR.',
  ingest_time TIMESTAMP NOT NULL
    COMMENT 'Time this exact payload was first captured for the run and release.'
)
USING DELTA
COMMENT 'Raw baseline and candidate CDR evidence; initial loader contains fixed synthetic smoke responses.'
TBLPROPERTIES (
  'chargeassert.layer' = 'bronze',
  'chargeassert.environment' = 'dev',
  'chargeassert.ocpi_version' = '2.2.1'
);

-- Both healthy smoke releases return the same CDR ID and body. Their evidence
-- must still occupy separate rows. These literals are canned responses, not
-- output from an implemented mock adapter or values copied from the oracle.
-- This is an OCPI-shaped financial subset, not a full certification payload.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  WITH fixture_responses AS (
    SELECT
      'smoke-run-v1' AS run_id,
      roles.release_role,
      CAST('2026-08-22T11:00:02Z' AS TIMESTAMP) AS ingest_time,
      '{"country_code":"FR","party_id":"CAS","id":"cdr-smoke-v1","session_id":"txn-smoke-v1","start_date_time":"2026-08-22T10:00:00Z","end_date_time":"2026-08-22T11:00:00Z","currency":"EUR","tariffs":[{"id":"tariff-smoke-v1","currency":"EUR","elements":[{"price_components":[{"type":"ENERGY","price":0.4500,"step_size":1}]}]}],"charging_periods":[{"start_date_time":"2026-08-22T10:00:00Z","dimensions":[{"type":"ENERGY","volume":12.5},{"type":"TIME","volume":1.0}],"tariff_id":"tariff-smoke-v1"}],"total_cost":{"excl_vat":5.63},"total_energy":12.5,"total_time":1.0,"credit":false,"last_updated":"2026-08-22T11:00:01Z"}' AS payload
    FROM VALUES ('baseline'), ('candidate') AS roles(release_role)
  )
  SELECT
    run_id,
    release_role,
    get_json_object(payload, '$.country_code') AS country_code,
    get_json_object(payload, '$.party_id') AS party_id,
    get_json_object(payload, '$.id') AS cdr_id,
    get_json_object(payload, '$.session_id') AS session_id,
    CASE
      WHEN get_json_object(payload, '$.credit') = 'true' THEN 'CREDIT'
      WHEN get_json_object(payload, '$.credit') = 'false'
        OR get_json_object(payload, '$.credit') IS NULL THEN 'FINAL'
      ELSE NULL
    END AS cdr_type,
    get_json_object(payload, '$.currency') AS currency,
    try_cast(get_json_object(payload, '$.total_cost.excl_vat') AS DECIMAL(18,6)) AS total_cost,
    payload,
    sha2(payload, 256) AS payload_hash,
    ingest_time
  FROM fixture_responses
) AS source
-- An exact retry is one raw payload per run and release. A different CDR ID or
-- changed body produces a different hash and is preserved for Silver to assess.
-- Never deduplicate by session_id, or overwrite an earlier financial record.
ON target.run_id = source.run_id
AND target.release_role = source.release_role
AND target.payload_hash = source.payload_hash
WHEN NOT MATCHED THEN INSERT (
  run_id,
  release_role,
  country_code,
  party_id,
  cdr_id,
  session_id,
  cdr_type,
  currency,
  total_cost,
  payload,
  payload_hash,
  ingest_time
) VALUES (
  source.run_id,
  source.release_role,
  source.country_code,
  source.party_id,
  source.cdr_id,
  source.session_id,
  source.cdr_type,
  source.currency,
  source.total_cost,
  source.payload,
  source.payload_hash,
  source.ingest_time
);

-- This checks only the fixed healthy fixture. Business validation of arbitrary
-- release outputs belongs in Silver/Gold; Bronze must retain faulty evidence.
SELECT assert_true(
  COUNT(*) = 2
    AND count_if(release_role = 'baseline') = 1
    AND count_if(release_role = 'candidate') = 1
    AND COUNT(DISTINCT payload_hash) = 1
    AND count_if(
      country_code = 'FR'
        AND party_id = 'CAS'
        AND cdr_id = 'cdr-smoke-v1'
        AND session_id = 'txn-smoke-v1'
        AND cdr_type = 'FINAL'
        AND currency = 'EUR'
        AND total_cost = CAST(5.63 AS DECIMAL(18,6))
        AND get_json_object(payload, '$.id') = cdr_id
        AND get_json_object(payload, '$.session_id') = session_id
        AND try_cast(get_json_object(payload, '$.total_cost.excl_vat') AS DECIMAL(18,6)) = total_cost
        AND try_cast(get_json_object(payload, '$.total_energy') AS DECIMAL(18,6)) = CAST(12.5 AS DECIMAL(18,6))
        AND try_cast(get_json_object(payload, '$.total_time') AS DECIMAL(18,6)) = CAST(1 AS DECIMAL(18,6))
        AND get_json_object(payload, '$.charging_periods[0].tariff_id') = 'tariff-smoke-v1'
        AND payload_hash = sha2(payload, 256)
    ) = 2,
  'Expected two intact smoke CDRs: one per release, each for txn-smoke-v1 with 12.5 kWh, one hour and EUR 5.63 excl_vat. Matching bodies must remain isolated by release.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';

SELECT
  run_id,
  release_role,
  country_code,
  party_id,
  cdr_id,
  session_id,
  cdr_type,
  currency,
  total_cost,
  get_json_object(payload, '$.total_energy') AS reported_energy_kwh,
  get_json_object(payload, '$.total_time') AS reported_duration_hours,
  payload_hash
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1'
ORDER BY release_role;
