-- Refuse to create orphan smoke events if their parent run is unavailable.
SELECT assert_true(
  COUNT(*) = 1,
  'Expected exactly one smoke-run-v1 row in run_manifest.'
)
FROM IDENTIFIER(:run_manifest_table_name)
WHERE run_id = 'smoke-run-v1';

-- Keep useful routing metadata in columns while preserving the original
-- OCPP 2.0.1 TransactionEvent request payload as exact JSON text.
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL
    COMMENT 'ChargeAssert test run that received or generated this event.',
  event_id STRING NOT NULL
    COMMENT 'OCPP CALL unique ID used as the stable raw-event identifier.',
  charging_station_id STRING NOT NULL
    COMMENT 'Charging station connection that emitted the event.',
  transaction_id STRING NOT NULL
    COMMENT 'OCPP transactionInfo.transactionId shared by the session events.',
  event_type STRING NOT NULL
    COMMENT 'OCPP TransactionEvent type: Started, Updated, or Ended.',
  sequence_number BIGINT NOT NULL
    COMMENT 'OCPP seqNo used to order events within one transaction.',
  event_time TIMESTAMP NOT NULL
    COMMENT 'Timestamp declared by the charging station in the payload.',
  ingest_time TIMESTAMP NOT NULL
    COMMENT 'Timestamp when ChargeAssert received the event.',
  payload STRING NOT NULL
    COMMENT 'Original TransactionEvent request payload preserved as JSON text.',
  payload_hash STRING NOT NULL
    COMMENT 'SHA-256 hash of the exact payload text for integrity checks.'
)
USING DELTA
COMMENT 'Raw OCPP 2.0.1 TransactionEvent evidence for ChargeAssert test runs.'
TBLPROPERTIES (
  'chargeassert.layer' = 'bronze',
  'chargeassert.environment' = 'dev',
  'chargeassert.ocpp_version' = '2.0.1'
);

-- Seed one deterministic golden session. The insert-only MERGE makes retries safe.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  SELECT
    run_id,
    event_id,
    charging_station_id,
    transaction_id,
    event_type,
    sequence_number,
    event_time,
    ingest_time,
    payload,
    sha2(payload, 256) AS payload_hash
  FROM VALUES
    (
      'smoke-run-v1',
      'msg-smoke-started-v1',
      'cs-smoke-001',
      'txn-smoke-v1',
      'Started',
      CAST(0 AS BIGINT),
      CAST('2026-08-22T10:00:00Z' AS TIMESTAMP),
      CAST('2026-08-22T10:00:01Z' AS TIMESTAMP),
      '{"eventType":"Started","timestamp":"2026-08-22T10:00:00Z","triggerReason":"Authorized","seqNo":0,"transactionInfo":{"transactionId":"txn-smoke-v1","chargingState":"Charging"},"meterValue":[{"timestamp":"2026-08-22T10:00:00Z","sampledValue":[{"value":100000,"context":"Transaction.Begin","measurand":"Energy.Active.Import.Register","location":"Outlet","unitOfMeasure":{"unit":"Wh","multiplier":0}}]}],"evse":{"id":1,"connectorId":1},"idToken":{"idToken":"TEST-RFID-001","type":"ISO14443"}}'
    ),
    (
      'smoke-run-v1',
      'msg-smoke-updated-v1',
      'cs-smoke-001',
      'txn-smoke-v1',
      'Updated',
      CAST(1 AS BIGINT),
      CAST('2026-08-22T10:30:00Z' AS TIMESTAMP),
      CAST('2026-08-22T10:30:01Z' AS TIMESTAMP),
      '{"eventType":"Updated","timestamp":"2026-08-22T10:30:00Z","triggerReason":"MeterValuePeriodic","seqNo":1,"transactionInfo":{"transactionId":"txn-smoke-v1","chargingState":"Charging","timeSpentCharging":1800},"meterValue":[{"timestamp":"2026-08-22T10:30:00Z","sampledValue":[{"value":106000,"context":"Sample.Periodic","measurand":"Energy.Active.Import.Register","location":"Outlet","unitOfMeasure":{"unit":"Wh","multiplier":0}}]}],"evse":{"id":1,"connectorId":1}}'
    ),
    (
      'smoke-run-v1',
      'msg-smoke-ended-v1',
      'cs-smoke-001',
      'txn-smoke-v1',
      'Ended',
      CAST(2 AS BIGINT),
      CAST('2026-08-22T11:00:00Z' AS TIMESTAMP),
      CAST('2026-08-22T11:00:01Z' AS TIMESTAMP),
      '{"eventType":"Ended","timestamp":"2026-08-22T11:00:00Z","triggerReason":"StopAuthorized","seqNo":2,"transactionInfo":{"transactionId":"txn-smoke-v1","chargingState":"Idle","timeSpentCharging":3600,"stoppedReason":"Local"},"meterValue":[{"timestamp":"2026-08-22T11:00:00Z","sampledValue":[{"value":112500,"context":"Transaction.End","measurand":"Energy.Active.Import.Register","location":"Outlet","unitOfMeasure":{"unit":"Wh","multiplier":0}}]}],"evse":{"id":1,"connectorId":1}}'
    )
  AS events (
    run_id,
    event_id,
    charging_station_id,
    transaction_id,
    event_type,
    sequence_number,
    event_time,
    ingest_time,
    payload
  )
) AS source
ON target.run_id = source.run_id
AND target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT (
  run_id,
  event_id,
  charging_station_id,
  transaction_id,
  event_type,
  sequence_number,
  event_time,
  ingest_time,
  payload,
  payload_hash
) VALUES (
  source.run_id,
  source.event_id,
  source.charging_station_id,
  source.transaction_id,
  source.event_type,
  source.sequence_number,
  source.event_time,
  source.ingest_time,
  source.payload,
  source.payload_hash
);

-- Fail the job unless the complete, ordered golden session and its hashes exist.
SELECT assert_true(
  COUNT(*) = 3
    AND COUNT(DISTINCT event_id) = 3
    AND count_if(event_type = 'Started' AND sequence_number = 0) = 1
    AND count_if(event_type = 'Updated' AND sequence_number = 1) = 1
    AND count_if(event_type = 'Ended' AND sequence_number = 2) = 1
    AND count_if(payload_hash = sha2(payload, 256)) = 3,
  'Golden OCPP session must contain exactly three ordered events with valid hashes.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';

-- The task output exposes the three events in lifecycle order.
SELECT
  run_id,
  transaction_id,
  event_type,
  sequence_number,
  event_time,
  get_json_object(payload, '$.meterValue[0].sampledValue[0].value') AS meter_value_wh,
  payload_hash
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1'
ORDER BY sequence_number;
