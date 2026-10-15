# Databricks notebook source
"""Validate and evaluate a generated batch, then save its exact Gold snapshot."""


def run(spark, dbutils):
    from generated_pipeline import evaluate
    parameters = {}
    for name in ('catalog_name', 'batch_id', 'candidate_mode', 'job_id', 'job_run_id', 'repair_count'):
        dbutils.widgets.text(name, '')
        value = dbutils.widgets.get(name)
        if not value:
            raise ValueError(f'Missing generated billing parameter: {name}')
        parameters[name] = value
    result = evaluate(spark, **parameters)
    # Refresh the saved attempt, including when its billing verdict is FAIL.
    dbutils.jobs.taskValues.set(key="execution_id", value=result["execution_id"])


if __name__ == '__main__':
    run(spark, dbutils)
