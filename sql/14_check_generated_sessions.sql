-- Run after generate_sessions and ingest_ocpp_files both succeed, sequentially.
-- Set :batch_id to the generator job's batch_id, e.g. sessions-001.
-- The default batch should have 6 events, 2 sessions, 6 distinct events,
-- 1 source file and 0 invalid hashes. Identical reruns keep those counts.
WITH batch AS (
  SELECT raw_record,
    get_json_object(raw_record, '$.payload') AS payload,
    record_hash, source_file_name
  FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
  WHERE get_json_object(raw_record, '$.run_id') = concat('generated-v1-', :batch_id)
)
SELECT COUNT(*) AS landed_events,
  COUNT(DISTINCT get_json_object(payload, '$.transactionInfo.transactionId')) AS sessions,
  COUNT(DISTINCT get_json_object(raw_record, '$.event_id')) AS distinct_events,
  COUNT(DISTINCT source_file_name) AS source_files,
  COALESCE(SUM(CASE WHEN record_hash IS NULL OR record_hash <> sha2(raw_record, 256) THEN 1 ELSE 0 END), 0) AS invalid_hashes
FROM batch;

-- Inspect generated inputs. No expected or reported bill is created yet.
WITH events AS (
  SELECT get_json_object(raw_record, '$.payload') AS payload,
    get_json_object(raw_record, '$.generator.seed') AS seed,
    get_json_object(raw_record, '$.generator.tariff.price_per_kwh') AS price_per_kwh
  FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
  WHERE get_json_object(raw_record, '$.run_id') = concat('generated-v1-', :batch_id)
)
SELECT get_json_object(payload, '$.transactionInfo.transactionId') AS session_id,
  get_json_object(payload, '$.eventType') AS event_type,
  CAST(get_json_object(payload, '$.seqNo') AS INT) AS sequence_number,
  get_json_object(payload, '$.timestamp') AS event_time,
  CAST(get_json_object(payload, '$.meterValue[0].sampledValue[0].value') AS BIGINT) AS meter_wh,
  seed, price_per_kwh
FROM events
ORDER BY session_id, sequence_number;
