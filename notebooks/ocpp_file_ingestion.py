"""Incremental raw OCPP JSON-lines landing with Auto Loader.

Each input line is retained as UTF-8 text (without its line delimiter). Parsing,
business-event deduplication, and the Silver/Gold adapter are separate steps.
The fixed source, checkpoint, and sink form one ingestion identity; neither
source files nor checkpoint state are deleted or rewritten by this module.
"""

import re


LANDING_DDL = """CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  raw_record STRING NOT NULL COMMENT 'Original UTF-8 JSON line, excluding its line delimiter; not yet validated.',
  record_hash STRING NOT NULL COMMENT 'SHA-256 of raw_record, not a unique business-event key.',
  source_file_path STRING NOT NULL,
  source_file_name STRING NOT NULL,
  source_file_modification_time TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Append-only file landing evidence; parsing and event-level deduplication happen downstream.'
TBLPROPERTIES ('chargeassert.layer' = 'bronze', 'chargeassert.source_contract' = 'ocpp-jsonl-v1')"""


def validate_config(input_path, checkpoint_path, table_name):
    """Pin this first stream to its own Volume, stable state, and landing table."""
    if not isinstance(input_path, str):
        raise ValueError("input_path must be a Unity Catalog Volume incoming path.")
    match = re.fullmatch(
        r"/Volumes/([A-Za-z_][A-Za-z0-9_-]*)/chargeassert_dev_bronze/ocpp_ingestion/incoming",
        input_path,
    )
    if match is None:
        raise ValueError("input_path must be /Volumes/<catalog>/chargeassert_dev_bronze/ocpp_ingestion/incoming.")
    catalog = match.group(1)
    volume_root = input_path.rsplit("/", 1)[0]
    if checkpoint_path != f"{volume_root}/checkpoints/ocpp_v1":
        raise ValueError("Use the persistent ocpp_v1 checkpoint outside the incoming directory.")
    if table_name != f"{catalog}.chargeassert_dev_bronze.ocpp_events_landing":
        raise ValueError("The input, checkpoint, and landing table must belong to the same ingestion identity.")


def ingest(spark, *, input_path, checkpoint_path, table_name):
    """Process available immutable .jsonl files, append to Delta, then stop."""
    validate_config(input_path, checkpoint_path, table_name)
    from pyspark.sql import functions as F

    spark.sql("SET TIME ZONE 'UTC'")
    spark.sql(LANDING_DDL, args={"table_name": table_name})

    # Text has a fixed schema. Keeping the line intact also retains malformed
    # JSON for future validation instead of silently dropping or rewriting it.
    source = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.allowOverwrites", "false")
        .option("cloudFiles.partitionColumns", "")
        .option("pathGlobFilter", "*.jsonl")
        .schema("value STRING")
        .load(input_path)
    )
    landed = source.select(
        F.col("value").alias("raw_record"),
        F.sha2(F.col("value"), 256).alias("record_hash"),
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_modification_time").alias("source_file_modification_time"),
        F.current_timestamp().alias("ingested_at"),
    )
    query = (
        landed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )
    # Propagate stream errors so the Databricks task cannot report success early.
    query.awaitTermination()
    print(f"Ingestion completed into {table_name}. Checkpoint retained at {checkpoint_path}.")
