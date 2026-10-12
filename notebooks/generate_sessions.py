# Databricks notebook source
"""Generate one bounded batch of charging sessions, publish it, then stop."""


def run(dbutils):
    import json
    import re
    from session_generator import publish_batch

    parameters = {}
    for name in ("input_path", "batch_id", "session_count", "seed"):
        dbutils.widgets.text(name, "")
        value = dbutils.widgets.get(name)
        if not value:
            raise ValueError(f"Missing required generator parameter: {name}")
        if name in ("session_count", "seed"):
            if not re.fullmatch(r"[0-9]{1,10}", value):
                raise ValueError(f"{name} must contain 1-10 digits.")
            value = int(value)
        parameters[name] = value
    result = publish_batch(dbutils.fs, **parameters)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    run(dbutils)
