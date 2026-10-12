"""Evaluate a validated generated batch through isolated Bronze, Silver and Gold.

The fixed seven-scenario pipeline and its historical evidence remain separate.
Every attempt has a receipt; only a complete snapshot with a SUCCEEDED receipt
is a usable result. No source file, landing record or checkpoint is changed.
"""

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SQL_FILES = (
    "03_session_lifecycle.sql", "05_tariff_history.sql", "06_expected_ledger.sql",
    "08_actual_ledger.sql", "09_assertion_result.sql", "10_release_verdict.sql",
)
RAW_FILES = {
    "run_manifest": "01_create_run_manifest.sql",
    "ocpp_transaction_events_raw": "02_create_ocpp_transaction_events_raw.sql",
    "tariffs_raw": "04_create_tariffs_raw.sql",
    "ocpi_cdrs_raw": "07_create_ocpi_cdrs_raw.sql",
}
TABLE_LAYERS = {
    **{name: "bronze" for name in (*RAW_FILES, "input_session", "job_execution")},
    **{name: "silver" for name in ("session_lifecycle", "tariff_history", "expected_ledger", "actual_ledger")},
    **{name: "gold" for name in ("assertion_result", "release_verdict", "execution_verdict")},
}
KEYS = {
    "run_manifest": ("run_id",), "input_session": ("run_id", "session_id"),
    "ocpp_transaction_events_raw": ("run_id", "event_id"),
    "tariffs_raw": ("run_id", "tariff_id"),
    "ocpi_cdrs_raw": ("run_id", "release_role", "payload_hash"),
    "execution_verdict": ("execution_id",),
}
MAX_LANDED_ROWS = 10000

EXECUTION_DDL = """CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  execution_id STRING NOT NULL, batch_id STRING NOT NULL, candidate_mode STRING NOT NULL,
  pipeline_hash STRING NOT NULL, status STRING NOT NULL, reason STRING NOT NULL,
  run_id STRING, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP
) USING DELTA COMMENT 'Generated-batch attempt receipt. Financial FAIL can accompany operational SUCCEEDED.'"""
SNAPSHOT_DDL = """CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  execution_id STRING NOT NULL, run_id STRING NOT NULL, input_hash STRING NOT NULL,
  pipeline_hash STRING NOT NULL, session_count BIGINT NOT NULL, assertion_count BIGINT NOT NULL,
  baseline_verdict STRING NOT NULL, candidate_verdict STRING NOT NULL, verdict STRING NOT NULL,
  verdict_snapshot STRING NOT NULL, assertions_snapshot STRING NOT NULL, captured_at TIMESTAMP NOT NULL
) USING DELTA COMMENT 'Immutable generated-batch Gold evidence. Join to a SUCCEEDED execution receipt.'"""
INPUT_DDL = """CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL, session_id STRING NOT NULL, tariff_id STRING NOT NULL
) USING DELTA COMMENT 'Complete intended session inventory validated before any generated billing writes.'"""


def statements(text):
    """Split these SQL files while preserving quoted strings and identifiers."""
    text = re.sub(r"'(?:''|[^'])*'|`(?:``|[^`])*`|--[^\n]*", lambda m:
                  "" if m[0].startswith("--") else m[0], text)
    start = 0
    for match in re.finditer(r"'(?:''|[^'])*'|`(?:``|[^`])*`|;", text):
        if match[0] == ";":
            if text[start:match.start()].strip():
                yield text[start:match.start()].strip()
            start = match.end()
    if text[start:].strip():
        yield text[start:].strip()


def _sql(spark, statement, parameters):
    required = set(re.findall(r":([a-z_][a-z0-9_]*)", statement))
    missing = required - parameters.keys()
    if missing:
        raise ValueError(f"Missing SQL parameters: {sorted(missing)}")
    return spark.sql(statement, args={name: parameters[name] for name in required})


def table_names(catalog):
    if not isinstance(catalog, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", catalog):
        raise ValueError("Invalid catalog_name.")
    return {name: f"`{catalog}`.chargeassert_dev_{layer}.generated_{name}"
            for name, layer in TABLE_LAYERS.items()}


def pipeline_hash():
    # Bind immutable run identities to implementation and transformation bytes.
    paths = [ROOT / "notebooks" / name for name in (
        "generated_pipeline.py", "generated_batch.py", "generated_capture.py", "execution_tracking.py")]
    paths += [ROOT / "sql" / name for name in RAW_FILES.values()]
    paths += [ROOT / "sql/generated" / name for name in SQL_FILES]
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(str(path.relative_to(ROOT)).replace("\\", "/").encode())
        digest.update(b"\0" + path.read_bytes().replace(b"\r\n", b"\n") + b"\0")
    return digest.hexdigest()


def canonical_json(value):
    def encode(item):
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, datetime):
            value = item.replace(tzinfo=timezone.utc) if item.tzinfo is None else item.astimezone(timezone.utc)
            return value.isoformat().replace("+00:00", "Z")
        raise TypeError(type(item).__name__)
    return json.dumps(value, default=encode, sort_keys=True, allow_nan=False, separators=(",", ":"))


def _rows(frame):
    return [row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
            for row in frame.collect()]


def check_immutable(existing, expected, keys, ignored=()):
    """Reject changed evidence, unexpected rows and duplicate logical keys."""
    def indexed(rows):
        result = {}
        for row in rows:
            key = tuple(row[name] for name in keys)
            if key in result:
                raise ValueError(f"Duplicate immutable key: {key}")
            result[key] = {name: value for name, value in row.items() if name not in ignored}
        return result
    old, wanted = indexed(existing), indexed(expected)
    for key, row in old.items():
        if key not in wanted or row != wanted[key]:
            raise ValueError(f"Existing evidence conflicts with generated run at {key}; nothing will be overwritten.")
    return len(old) == len(wanted)


def write_immutable(spark, table, rows, keys, identity_column, identity):
    if not rows:
        raise ValueError("Refusing empty immutable evidence.")
    schema = spark.table(table).schema
    fields = [field.name for field in schema.fields]
    rows = [{field: row[field] for field in fields} for row in rows]
    query = "SELECT * FROM IDENTIFIER(:table_name) WHERE IDENTIFIER(:identity_column) = :identity LIMIT :row_limit"
    args = dict(table_name=table, identity_column=identity_column, identity=identity, row_limit=len(rows) + 1)
    existing = _rows(_sql(spark, query, args))
    ignored = ("ingest_time", "captured_at")
    if check_immutable(existing, rows, keys, ignored):
        return
    spark.createDataFrame(rows, schema=schema).createOrReplaceTempView("chargeassert_generated_source")
    try:
        on = " AND ".join(f"target.`{key}` = source.`{key}`" for key in keys)
        _sql(spark, "MERGE INTO IDENTIFIER(:table_name) AS target USING chargeassert_generated_source AS source ON "
             + on + " WHEN NOT MATCHED THEN INSERT *", {"table_name": table})
    finally:
        spark.catalog.dropTempView("chargeassert_generated_source")
    final = _rows(_sql(spark, query, args))
    if not check_immutable(final, rows, keys, ignored):
        raise RuntimeError("Generated evidence was not saved completely.")


def _ensure_tables(spark, names):
    for logical, filename in RAW_FILES.items():
        ddl = next(s for s in statements((ROOT / "sql" / filename).read_text(encoding="utf-8"))
                   if s.startswith("CREATE TABLE"))
        _sql(spark, ddl, {"table_name": names[logical]})
    _sql(spark, INPUT_DDL, {"table_name": names["input_session"]})


def _transform(spark, names, run_id):
    common = {
        "run_id": run_id, "fail_before_gold": "false",
        "run_manifest_table_name": names["run_manifest"],
        "input_session_table_name": names["input_session"],
        "raw_events_table_name": names["ocpp_transaction_events_raw"],
        "raw_tariffs_table_name": names["tariffs_raw"],
        "raw_cdrs_table_name": names["ocpi_cdrs_raw"],
        **{name + "_table_name": names[name] for name in (
            "session_lifecycle", "tariff_history", "expected_ledger", "actual_ledger", "assertion_result")},
    }
    outputs = ("session_lifecycle", "tariff_history", "expected_ledger", "actual_ledger", "assertion_result", "release_verdict")
    for filename, output in zip(SQL_FILES, outputs):
        parameters = dict(common, table_name=names[output])
        for statement in statements((ROOT / "sql/generated" / filename).read_text(encoding="utf-8")):
            _sql(spark, statement, parameters).collect()


def _execution_rows(spark, table, execution_id):
    return _rows(_sql(spark, "SELECT * FROM IDENTIFIER(:table_name) WHERE execution_id = :execution_id LIMIT 2",
                      dict(table_name=table, execution_id=execution_id)))


def evaluate(spark, *, catalog_name, batch_id, candidate_mode, job_id, job_run_id, repair_count):
    try:
        from .generated_batch import prepare_batch
        from .generated_capture import capture_result
        from .execution_tracking import execution_identity
    except ImportError:
        from generated_batch import prepare_batch
        from generated_capture import capture_result
        from execution_tracking import execution_identity
    names = table_names(catalog_name)
    execution_id = execution_identity(job_id, job_run_id, repair_count)
    code_hash = pipeline_hash()
    spark.sql("SET TIME ZONE 'UTC'")
    _sql(spark, EXECUTION_DDL, {"table_name": names["job_execution"]})
    _sql(spark, SNAPSHOT_DDL, {"table_name": names["execution_verdict"]})
    if _execution_rows(spark, names["job_execution"], execution_id):
        raise ValueError("This execution already has a receipt. Start a new complete job run; do not repair or reuse it.")
    parameters = dict(table_name=names["job_execution"], execution_id=execution_id,
                      batch_id=batch_id, candidate_mode=candidate_mode, pipeline_hash=code_hash)
    _sql(spark, """INSERT INTO IDENTIFIER(:table_name)
      (execution_id,batch_id,candidate_mode,pipeline_hash,status,reason,started_at)
      VALUES (:execution_id,:batch_id,:candidate_mode,:pipeline_hash,'RUNNING','Validating generated inputs.',current_timestamp())""", parameters)
    run_id = None
    try:
        registered = _execution_rows(spark, names["job_execution"], execution_id)
        if len(registered) != 1 or registered[0]["status"] != "RUNNING":
            raise RuntimeError("Could not register a unique generated execution receipt.")
        started_at = registered[0]["started_at"]
        if str(repair_count) != "0":
            raise ValueError("Repair attempts are unsupported; start a new complete job run.")
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", batch_id or ""):
            raise ValueError("Invalid batch_id; use 1-32 lowercase letters/digits/hyphens/underscores starting with a letter.")
        if candidate_mode not in ("healthy", "amount_error"):
            raise ValueError("candidate_mode must be healthy or amount_error.")
        landing = f"`{catalog_name}`.chargeassert_dev_bronze.ocpp_events_landing"
        incoming = _rows(_sql(spark, """SELECT raw_record,record_hash,source_file_path,source_file_name,
          source_file_modification_time,ingested_at FROM IDENTIFIER(:landing_table_name)
          WHERE source_file_name = :filename OR get_json_object(raw_record, '$.run_id') = :source_run_id
            OR get_json_object(raw_record, '$.generator.batch_id') = :batch_id
          LIMIT :row_limit""", dict(landing_table_name=landing, filename=f"generated-{batch_id}.jsonl",
                                   source_run_id=f"generated-v1-{batch_id}", batch_id=batch_id,
                                   row_limit=MAX_LANDED_ROWS + 1)))
        if len(incoming) > MAX_LANDED_ROWS:
            raise ValueError("Batch has too many landed records for this bounded evaluator.")
        prepared = prepare_batch(incoming, batch_id, candidate_mode, code_hash)
        run_id = prepared["run_id"]
        _ensure_tables(spark, names)
        for logical in ("run_manifest", "input_session", "ocpp_transaction_events_raw", "tariffs_raw", "ocpi_cdrs_raw"):
            write_immutable(spark, names[logical], prepared["tables"][logical], KEYS[logical], "run_id", run_id)
        _transform(spark, names, run_id)
        verdicts = _rows(_sql(spark, "SELECT * FROM IDENTIFIER(:table_name) WHERE run_id=:run_id AND evaluated_at >= :started_at LIMIT 2",
                             dict(table_name=names["release_verdict"], run_id=run_id, started_at=started_at)))
        assertions = _rows(_sql(spark, "SELECT * FROM IDENTIFIER(:table_name) WHERE run_id=:run_id LIMIT :row_limit",
                              dict(table_name=names["assertion_result"], run_id=run_id,
                                   row_limit=14 * len(prepared["session_ids"]) + 1)))
        captured = capture_result(prepared, verdicts, assertions)
        snapshot = dict(execution_id=execution_id, run_id=run_id, input_hash=prepared["input_hash"],
                        pipeline_hash=code_hash, session_count=captured["session_count"],
                        assertion_count=captured["assertion_count"],
                        baseline_verdict=captured["baseline_verdict"], candidate_verdict=captured["candidate_verdict"],
                        verdict=captured["verdict"], verdict_snapshot=canonical_json(captured["verdict_row"]),
                        assertions_snapshot=canonical_json(captured["assertions"]),
                        captured_at=datetime.now(timezone.utc).replace(tzinfo=None))
        write_immutable(spark, names["execution_verdict"], [snapshot], KEYS["execution_verdict"], "execution_id", execution_id)
        reason = f"Captured {captured['assertion_count']} assertions for all {captured['session_count']} sessions and both releases."
        _sql(spark, """UPDATE IDENTIFIER(:table_name) SET status='SUCCEEDED',reason=:reason,
          run_id=:run_id,finished_at=current_timestamp() WHERE execution_id=:execution_id AND status='RUNNING'""",
             dict(parameters, run_id=run_id, reason=reason))
        final = _execution_rows(spark, names["job_execution"], execution_id)
        if len(final) != 1 or final[0]["status"] != "SUCCEEDED":
            raise RuntimeError("Could not finalize the generated execution receipt.")
        result = {key: snapshot[key] for key in ("execution_id", "run_id", "session_count", "assertion_count",
                                                  "baseline_verdict", "candidate_verdict", "verdict")}
        print(canonical_json(dict(result, status="SUCCEEDED")))
        return result
    except Exception as error:
        # Preserve the first failed attempt and its diagnostic. Never use older Gold.
        _sql(spark, """UPDATE IDENTIFIER(:table_name) SET status='FAILED',reason=:reason,
          run_id=:run_id,finished_at=current_timestamp() WHERE execution_id=:execution_id AND status='RUNNING'""",
             dict(parameters, run_id=run_id, reason=f"{type(error).__name__}: {error}"[:8000]))
        raise
