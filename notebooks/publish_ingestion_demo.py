# Databricks notebook source
"""Publish one immutable synthetic batch for the incremental-ingestion demo."""


def run(dbutils):
    from ocpp_ingestion_demo import publish_batch

    parameters = {}
    for name in ("input_path", "batch"):
        dbutils.widgets.text(name, "")
        parameters[name] = dbutils.widgets.get(name)
        if not parameters[name]:
            raise ValueError(f"Missing required publisher parameter: {name}")
    result = publish_batch(dbutils.fs, **parameters)
    print(
        f"Synthetic batch {result['status']}: {result['path']} "
        f"({result['records']} records, SHA-256 {result['file_sha256']})."
    )


if __name__ == "__main__":
    run(dbutils)
