-- Read-only checks for the separate incremental file-ingestion demonstration.
-- Default dev catalog shown; change the table qualifier if catalog_name differs.
-- Run after each ingest_ocpp_files job has finished successfully.
-- First-time sequence: batch_001 => 3 rows; repeat => 3;
-- publish batch_002 => 6 rows; repeat => 6. Existing demo data is not reset.

SELECT
  COUNT(*) AS landed_rows,
  COUNT(DISTINCT source_file_path) AS source_files,
  COUNT(DISTINCT record_hash) AS distinct_record_hashes
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing;

-- Expect one file with three rows after batch_001, then two files with three each.
-- Repeated ingestion must leave row counts and first/last ingestion times unchanged.
SELECT
  source_file_name,
  source_file_path,
  COUNT(*) AS landed_rows,
  MIN(source_file_modification_time) AS source_file_modification_time,
  MIN(ingested_at) AS first_ingested_at,
  MAX(ingested_at) AS last_ingested_at
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
GROUP BY source_file_name, source_file_path
ORDER BY source_file_name, source_file_path;

-- Both counts should be zero. The hash covers the preserved UTF-8 text line,
-- excluding its newline delimiter, not a parsed or reserialized JSON object.
SELECT
  COALESCE(SUM(CASE
    WHEN raw_record IS NULL OR record_hash IS NULL
      OR record_hash <> sha2(raw_record, 256) THEN 1 ELSE 0
  END), 0) AS invalid_record_hashes,
  COALESCE(SUM(CASE
    WHEN source_file_path IS NULL OR source_file_path = ''
      OR source_file_name IS NULL OR source_file_name = ''
      OR source_file_modification_time IS NULL OR ingested_at IS NULL
    THEN 1 ELSE 0
  END), 0) AS missing_source_metadata
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing;

-- Expect no rows for these fixtures. This is a diagnostic duplicate check,
-- not a universal key: identical text can legitimately occur twice in one file.
-- The pipeline does not deduplicate repeated business events across files.
SELECT source_file_path, record_hash, COUNT(*) AS occurrences
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
GROUP BY source_file_path, record_hash
HAVING COUNT(*) > 1;

-- Inspect the original envelope and source evidence. These convenience JSON
-- projections do not imply that ingestion validates or quarantines JSON.
SELECT
  source_file_name,
  get_json_object(raw_record, '$.schema_version') AS schema_version,
  get_json_object(raw_record, '$.run_id') AS run_id,
  get_json_object(raw_record, '$.event_id') AS event_id,
  get_json_object(raw_record, '$.charging_station_id') AS charging_station_id,
  raw_record,
  record_hash,
  source_file_path,
  source_file_modification_time,
  ingested_at
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
ORDER BY source_file_name, event_id;
