"""Validate one generated batch's Gold evidence before the caller saves it.

This function checks session coverage, provenance and financial status. The
runtime remains responsible for proving that this attempt finished processing
and for recording operational success or failure. A valid financial FAIL is
still useful evidence and does not become a financial PASS during capture.
"""

from copy import deepcopy

try:
    from .execution_tracking import ASSERTION_IDS, COUNT_FIELDS
except ImportError:  # Databricks imports sibling workspace files directly.
    from execution_tracking import ASSERTION_IDS, COUNT_FIELDS


ROLES = ("baseline", "candidate")
PROVENANCE_FIELDS = ("run_id", "scenario_id", "seed", "baseline_sha", "candidate_sha", "tariff_hash")
COVERAGE_FIELDS = ("missing_assertions", "duplicate_assertion_keys", "unexpected_assertions", "invalid_assertions")


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def capture_result(prepared, verdicts, assertions):
    """Return complete, internally consistent evidence for this exact batch.

    ``prepared`` comes from generated_batch.prepare_batch. Verdicts and
    assertions are dictionaries read after the current generated run's SQL
    transformations. Inputs are left unchanged; returned evidence is copied.
    """
    _require(isinstance(prepared, dict), "Prepared batch must be a dictionary.")
    run_id = prepared.get("run_id")
    _require(isinstance(run_id, str) and bool(run_id), "Prepared batch run_id is missing.")
    sessions = prepared.get("session_ids")
    _require(isinstance(sessions, (list, tuple)) and len(sessions) > 0,
             "Prepared session inventory must not be empty.")
    _require(all(isinstance(session, str) and bool(session) for session in sessions),
             "Prepared session IDs must be nonempty strings.")
    _require(len(set(sessions)) == len(sessions), "Prepared session inventory contains duplicates.")
    metadata = prepared.get("metadata")
    _require(isinstance(metadata, dict), "Prepared batch metadata is missing.")
    _require(type(metadata.get("session_count")) is int
             and metadata["session_count"] == len(sessions),
             "Prepared session_count differs from the declared session inventory.")
    for field, value in (("input_hash", prepared.get("input_hash")),
                         ("pipeline_hash", metadata.get("pipeline_hash")),
                         ("candidate_mode", metadata.get("candidate_mode"))):
        _require(isinstance(value, str) and bool(value), f"Prepared {field} is missing.")
    tables = prepared.get("tables")
    _require(isinstance(tables, dict), "Prepared batch tables are missing.")
    manifests = tables.get("run_manifest")
    _require(isinstance(manifests, (list, tuple)) and len(manifests) == 1
             and isinstance(manifests[0], dict), "Prepared batch must have exactly one manifest.")
    manifest = manifests[0]
    _require(manifest.get("run_id") == run_id, "Prepared manifest belongs to another run.")
    for field in PROVENANCE_FIELDS:
        value = manifest.get(field)
        valid = type(value) is int and value >= 0 if field == "seed" else isinstance(value, str) and bool(value)
        _require(valid, f"Prepared manifest provenance is missing or invalid: {field}.")

    _require(isinstance(verdicts, (list, tuple)) and len(verdicts) == 1
             and isinstance(verdicts[0], dict), "Expected exactly one Gold verdict for the generated run.")
    verdict = verdicts[0]
    for field in PROVENANCE_FIELDS:
        _require(type(verdict.get(field)) is type(manifest[field])
                 and verdict.get(field) == manifest[field],
                 f"Gold verdict provenance differs from the prepared manifest: {field}.")

    _require(isinstance(assertions, (list, tuple)), "Gold assertions must be a list of rows.")
    for row in assertions:
        _require(isinstance(row, dict), "Each Gold assertion must be a dictionary.")
        _require(row.get("run_id") == run_id, "Gold assertion belongs to another run.")
        _require(all(isinstance(row.get(field), str) and bool(row[field])
                     for field in ("release_role", "session_id", "assertion_id")),
                 "Gold assertion identity is missing or invalid.")
        _require(row.get("status") in ("PASS", "FAIL", "BLOCKED"),
                 "Gold assertion has an invalid status.")
    expected_keys = {(role, session, rule) for role in ROLES
                     for session in sessions for rule in ASSERTION_IDS}
    keys = [(row["release_role"], row["session_id"], row["assertion_id"]) for row in assertions]
    _require(len(keys) == len(expected_keys) and set(keys) == expected_keys,
             "Gold assertion coverage is missing, duplicated or outside the prepared session inventory.")

    for field in COUNT_FIELDS:
        _require(type(verdict.get(field)) is int and verdict[field] >= 0,
                 f"Gold assertion count is missing or invalid: {field}.")
    _require(verdict["required_assertions"] == len(expected_keys),
             "Gold required assertion count differs from the prepared session inventory.")
    _require(all(verdict[field] == 0 for field in COVERAGE_FIELDS),
             "Gold verdict reports incomplete or invalid assertion coverage.")
    for status, field in (("PASS", "passed_assertions"), ("FAIL", "failed_assertions"),
                          ("BLOCKED", "blocked_assertions")):
        _require(verdict[field] == sum(row["status"] == status for row in assertions),
                 f"Gold {field} differs from the captured assertion rows.")

    outcomes = {}
    for role in ROLES:
        field = f"{role}_verdict"
        outcomes[field] = "PASS" if all(row["status"] == "PASS" for row in assertions
                                         if row["release_role"] == role) else "FAIL"
        _require(verdict.get(field) == outcomes[field],
                 f"Gold {field} differs from the captured assertion statuses.")
    outcomes["verdict"] = "PASS" if all(value == "PASS" for value in outcomes.values()) else "FAIL"
    _require(verdict.get("verdict") == outcomes["verdict"],
             "Gold overall verdict differs from the release verdicts.")
    ordered = sorted(assertions, key=lambda row: (row["release_role"], row["session_id"], row["assertion_id"]))
    return {
        "verdict_row": deepcopy(verdict),
        "assertions": deepcopy(ordered),
        "session_count": len(sessions),
        "assertion_count": len(expected_keys),
        **outcomes,
    }
