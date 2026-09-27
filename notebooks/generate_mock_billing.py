# Databricks notebook source
"""Run the small mock once, preserve Bronze evidence, then let Silver/Gold run.

The adjacent mock modules are regular workspace files imported by this
notebook. No package installation, HTTP service, polling or scheduling is used.
"""

from datetime import datetime, timezone


TABLE_PARAMETERS = {
    "run_manifest": "run_manifest_table_name",
    "ocpp_transaction_events_raw": "raw_events_table_name",
    "tariffs_raw": "raw_tariffs_table_name",
    "ocpi_cdrs_raw": "raw_cdrs_table_name",
}
KEYS = {
    "run_manifest": ("run_id",),
    "ocpp_transaction_events_raw": ("run_id", "event_id"),
    "tariffs_raw": ("run_id", "tariff_id"),
    "ocpi_cdrs_raw": ("run_id", "release_role", "payload_hash"),
}


def comparable(row):
    # Spark returns UTC TIMESTAMP values as naive Python datetimes. The mock
    # may supply aware UTC values; compare the same instants, including micros.
    return {
        key: (value.replace(tzinfo=timezone.utc) if value.tzinfo is None
              else value.astimezone(timezone.utc))
        if isinstance(value, datetime) else value
        for key, value in row.items()
    }


def verify_stored_rows(table, existing, expected, *, require_complete=False):
    """Permit missing rows for retry recovery; never permit changed evidence."""
    keys = KEYS[table]
    expected_by_key = {tuple(row[key] for key in keys): comparable(row) for row in expected}
    if not expected or len(expected_by_key) != len(expected):
        raise ValueError(f"Mock generation produced empty or duplicate rows for {table}.")
    seen = set()
    for row in existing:
        key = tuple(row[field] for field in keys)
        if key in seen or expected_by_key.get(key) != comparable(row):
            raise ValueError(
                f"Stored mock evidence differs in {table} at {key}. "
                "Keep the original evidence and use new versioned run IDs when changing the mock or inputs."
            )
        seen.add(key)
    if require_complete and seen != expected_by_key.keys():
        raise ValueError(f"Mock evidence is incomplete in {table} after loading.")


def run(spark, dbutils):
    from pyspark.sql import functions as F
    from mock_missing_cdr import generate_all_mock_runs

    spark.sql("SET TIME ZONE 'UTC'")
    names = {}
    for table, parameter in TABLE_PARAMETERS.items():
        dbutils.widgets.text(parameter, "")
        names[table] = dbutils.widgets.get(parameter)
        if not names[table]:
            raise ValueError(f"Missing required job parameter: {parameter}")

    def smoke_rows(table, maximum):
        # Bounded collection is deliberate: this is a three-event fixture replay,
        # not a driver-side implementation for processing production volumes.
        rows = spark.table(names[table]).where(F.col("run_id") == "smoke-run-v1").limit(maximum + 1).collect()
        if len(rows) != maximum:
            raise ValueError(f"Expected {maximum} smoke input rows in {table}; found {len(rows)}.")
        return [row.asDict() for row in rows]

    generated = generate_all_mock_runs(
        smoke_rows("ocpp_transaction_events_raw", 3),
        smoke_rows("tariffs_raw", 1)[0],
        smoke_rows("run_manifest", 1)[0],
    )
    run_ids = [row["run_id"] for row in generated["run_manifest"]]

    def stored_rows(table):
        return [row.asDict() for row in spark.table(names[table])
                .where(F.col("run_id").isin(run_ids))
                .limit(len(generated[table]) + 1).collect()]

    # Check every destination before the first write. A retry can complete a
    # partial prior write; conflicting rows must never be overwritten/deleted.
    for table, rows in generated.items():
        verify_stored_rows(table, stored_rows(table), rows)

    for table, rows in generated.items():
        view = f"chargeassert_mock_source_{table}"
        schema = spark.table(names[table]).schema
        spark.createDataFrame(rows, schema=schema).createOrReplaceTempView(view)
        join = " AND ".join(f"target.{key} = source.{key}" for key in KEYS[table])
        spark.sql(
            f"MERGE INTO IDENTIFIER(:table_name) AS target USING {view} AS source "
            f"ON {join} WHEN NOT MATCHED THEN INSERT *",
            args={"table_name": names[table]},
        )

    for table, rows in generated.items():
        verify_stored_rows(table, stored_rows(table), rows, require_complete=True)
    print(f"Mock replay complete: {', '.join(run_ids)}. Silver/Gold will evaluate the CDRs next.")


if __name__ == "__main__":
    run(spark, dbutils)
