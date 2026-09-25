# Databricks notebook source
"""Run the on-demand Auto Loader job independently of the billing fixtures."""


def run(spark, dbutils):
    from ocpp_file_ingestion import ingest

    parameters = {}
    for name in ("input_path", "checkpoint_path", "table_name"):
        dbutils.widgets.text(name, "")
        parameters[name] = dbutils.widgets.get(name)
        if not parameters[name]:
            raise ValueError(f"Missing required ingestion parameter: {name}")
    ingest(spark, **parameters)


if __name__ == "__main__":
    run(spark, dbutils)
