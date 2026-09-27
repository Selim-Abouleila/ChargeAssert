# Databricks notebook source
"""Register, capture, or finalize one ChargeAssert job invocation."""


def run(spark, dbutils):
    from execution_tracking import track

    names = (
        "mode", "job_id", "job_run_id", "repair_count", "job_execution_table_name",
        "execution_verdict_table_name", "release_verdict_table_name",
        "assertion_result_table_name", "task_states_json",
    )
    parameters = {}
    for name in names:
        dbutils.widgets.text(name, "")
        parameters[name] = dbutils.widgets.get(name)
        if not parameters[name]:
            raise ValueError(f"Missing required execution tracking parameter: {name}")
    track(spark, **parameters)


if __name__ == "__main__":
    run(spark, dbutils)
