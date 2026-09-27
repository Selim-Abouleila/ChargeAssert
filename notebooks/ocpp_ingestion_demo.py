"""Publish two small synthetic OCPP NDJSON fixtures without replacing evidence.

This is a bounded demonstration producer, not the ingestion implementation.
The first file wraps the existing smoke payloads unchanged. The second contains
a different complete session. Only the selected batch is ever published.
"""

import hashlib
from pathlib import Path
import re


BATCHES = ("batch_001", "batch_002")
FIXTURE_DIRECTORY = Path(__file__).resolve().parents[1] / "data" / "ingestion_demo"
MAX_FIXTURE_BYTES = 65536
_INPUT_PATH = re.compile(
    r"/Volumes/[A-Za-z0-9_-]+/chargeassert_dev_bronze/ocpp_ingestion/incoming/?"
)


def fixture_content(batch):
    """Read a committed batch with stable LF delimiters on Windows and Linux."""
    if batch not in BATCHES:
        raise ValueError(f"Unsupported demonstration batch: {batch!r}. Choose {BATCHES}.")
    path = FIXTURE_DIRECTORY / f"{batch}.jsonl"
    if path.stat().st_size > MAX_FIXTURE_BYTES:
        raise ValueError("Demonstration fixture exceeds the bounded publisher limit.")
    # Universal newline translation affects the NDJSON separators only: escaped
    # newlines and characters within each JSON payload string remain unchanged.
    content = path.read_text(encoding="utf-8")
    if not content or len(content.encode("utf-8")) > MAX_FIXTURE_BYTES:
        raise ValueError("Demonstration fixture is empty or exceeds the size limit.")
    return content


def _existing_file(fs, directory, filename):
    # Do not treat access/transport errors as a missing file. The caller has
    # created the incoming directory; any ls failure must stop publication.
    matches = [item for item in fs.ls(directory) if item.name.rstrip("/") == filename]
    if len(matches) > 1:
        raise ValueError(f"Ambiguous existing demonstration file: {filename}")
    return matches[0] if matches else None


def _verify_file(fs, path, existing, expected_bytes):
    if existing is None or existing.name.endswith("/") or existing.size != len(expected_bytes):
        raise ValueError(f"Published demonstration file differs; refusing to overwrite: {path}")
    # Check size as well as content: head alone could hide an appended suffix.
    actual_bytes = fs.head(path, len(expected_bytes) + 1).encode("utf-8")
    if actual_bytes != expected_bytes:
        raise ValueError(f"Published demonstration file differs; refusing to overwrite: {path}")


def publish_batch(fs, input_path, batch):
    """Publish once; identical repeats return unchanged, conflicts fail closed.

    ``fs`` is dbutils.fs in Databricks and a minimal filesystem fake in tests.
    No volume/table is created here, and no published file is deleted or changed.
    """
    if not isinstance(input_path, str) or not _INPUT_PATH.fullmatch(input_path):
        raise ValueError(
            "input_path must be /Volumes/<catalog>/chargeassert_dev_bronze/"
            "ocpp_ingestion/incoming with no traversal or alternate directory."
        )
    content = fixture_content(batch)
    expected_bytes = content.encode("utf-8")
    directory = input_path.rstrip("/")
    filename = f"{batch}.jsonl"
    path = f"{directory}/{filename}"
    if fs.mkdirs(directory) is not True:
        raise RuntimeError(f"Could not create incoming directory: {directory}")
    existing = _existing_file(fs, directory, filename)
    status = "unchanged"
    if existing is None:
        # A concurrent publisher can cause this to fail, but cannot authorize an
        # overwrite. A subsequent invocation verifies the already-published file.
        if fs.put(path, content, overwrite=False) is not True:
            raise RuntimeError(f"Demonstration file write did not succeed: {path}")
        status = "published"
        existing = _existing_file(fs, directory, filename)
    _verify_file(fs, path, existing, expected_bytes)
    return {
        "status": status,
        "path": path,
        "records": len(content.splitlines()),
        "file_sha256": hashlib.sha256(expected_bytes).hexdigest(),
    }
