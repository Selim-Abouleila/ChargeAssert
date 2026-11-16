# Databricks notebook source
"""Grant the app read-only access to saved generated results, without changing data."""

import re
from uuid import UUID


def grant_result_access(spark, catalog_name, app_principal):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", catalog_name or ""):
        raise ValueError("Use a simple Unity Catalog catalog name.")
    try:
        principal = str(UUID(app_principal))
    except (ValueError, TypeError, AttributeError) as error:
        raise ValueError("The app service principal must be a UUID.") from error
    tables = (
        ("chargeassert_dev_bronze", "generated_job_execution"),
        ("chargeassert_dev_gold", "generated_execution_verdict"),
    )
    qualified = [f"`{catalog_name}`.`{schema}`.`{table}`" for schema, table in tables]
    missing = [name for name in qualified if not spark.catalog.tableExists(name)]
    if missing:
        raise ValueError(
            "Saved-result tables are missing. Run run_generated_pipeline once, "
            "then run setup_simulation_app again. Missing: " + ", ".join(missing)
        )
    # Check all tables before granting anything; repeated GRANTs are safe.
    spark.sql(f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{principal}`").collect()
    for (schema, _), table in zip(tables, qualified):
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema}` TO `{principal}`").collect()
        spark.sql(f"GRANT SELECT ON TABLE {table} TO `{principal}`").collect()
    print("App access ready: it can read saved evaluation receipts and Gold snapshots.")


def run(spark, dbutils):
    parameters = {}
    for name in ("catalog_name", "app_principal"):
        dbutils.widgets.text(name, "")
        parameters[name] = dbutils.widgets.get(name)
    grant_result_access(spark, **parameters)


if __name__ == "__main__":
    run(spark, dbutils)
