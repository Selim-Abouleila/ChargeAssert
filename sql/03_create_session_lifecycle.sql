CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL
    COMMENT 'ChargeAssert test run for this session.',
  session_id STRING NOT NULL
    COMMENT 'Logical charging session ID (mapped from OCPP transaction_id).',
  started_at TIMESTAMP NOT NULL
    COMMENT 'Timestamp when the charging session started.',
  ended_at TIMESTAMP
    COMMENT 'Timestamp when the charging session ended, if completed.',
  meter_start_wh BIGINT NOT NULL
    COMMENT 'Meter value at the start of the session in Wh.',
  meter_end_wh BIGINT
    COMMENT 'Meter value at the end of the session in Wh, if completed.',
  status STRING NOT NULL
    COMMENT 'Session status: Completed or In Progress.'
)
USING DELTA
COMMENT 'Validated and normalized logical charging sessions.'
TBLPROPERTIES (
  'chargeassert.layer' = 'silver',
  'chargeassert.environment' = 'dev'
);

-- Safely aggregate events by transaction_id into a single logical session
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  SELECT
    run_id,
    transaction_id AS session_id,
    MIN(CASE WHEN event_type = 'Started' THEN event_time END) AS started_at,
    MAX(CASE WHEN event_type = 'Ended' THEN event_time END) AS ended_at,
    CAST(MAX(CASE WHEN event_type = 'Started' THEN get_json_object(payload, '$.meterValue[0].sampledValue[0].value') END) AS BIGINT) AS meter_start_wh,
    CAST(MAX(CASE WHEN event_type = 'Ended' THEN get_json_object(payload, '$.meterValue[0].sampledValue[0].value') END) AS BIGINT) AS meter_end_wh,
    CASE 
      WHEN count_if(event_type = 'Ended') > 0 THEN 'Completed'
      ELSE 'In Progress' 
    END AS status
  FROM IDENTIFIER(:raw_events_table_name)
  GROUP BY run_id, transaction_id
) AS source
ON target.run_id = source.run_id
AND target.session_id = source.session_id
WHEN MATCHED THEN UPDATE SET
  target.started_at = source.started_at,
  target.ended_at = source.ended_at,
  target.meter_start_wh = source.meter_start_wh,
  target.meter_end_wh = source.meter_end_wh,
  target.status = source.status
WHEN NOT MATCHED THEN INSERT (
  run_id,
  session_id,
  started_at,
  ended_at,
  meter_start_wh,
  meter_end_wh,
  status
) VALUES (
  source.run_id,
  source.session_id,
  source.started_at,
  source.ended_at,
  source.meter_start_wh,
  source.meter_end_wh,
  source.status
);

-- Fail the job unless the session is correctly aggregated
SELECT assert_true(
  COUNT(*) = 1
    AND count_if(session_id = 'txn-smoke-v1') = 1
    AND count_if(status = 'Completed') = 1
    AND count_if(meter_start_wh = 100000) = 1
    AND count_if(meter_end_wh = 112500) = 1,
  'Silver session_lifecycle must correctly aggregate the smoke test session.'
)
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';

-- Expose the aggregated row in the task output
SELECT *
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';
