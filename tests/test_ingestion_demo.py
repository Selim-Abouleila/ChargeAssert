"""Verify immutable demo publication without Spark or cloud access."""

from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from notebooks import ocpp_ingestion_demo as demo
from notebooks import publish_ingestion_demo as wrapper


ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = "/Volumes/workspace/chargeassert_dev_bronze/ocpp_ingestion/incoming"
FILE_HASHES = {
    "batch_001": "c455eb30486fbf795cfc2aee270e4e9863421d9bf8c977e4d9e7d0d286f68f93",
    "batch_002": "daff215a46a07144db29975c1db29223dfddec66b2c4fc28cdf6db4a67d3afca",
}


class FakeFileSystem:
    def __init__(self):
        self.files = {}
        self.puts = []
        self.calls = []
        self.errors = {}
        self.mkdir_result = True
        self.put_result = True

    def _called(self, operation):
        self.calls.append(operation)
        if operation in self.errors:
            raise self.errors[operation]

    def mkdirs(self, path):
        self._called("mkdirs")
        return self.mkdir_result

    def ls(self, path):
        self._called("ls")
        return [SimpleNamespace(name=key.rsplit("/", 1)[-1], size=len(value))
                for key, value in self.files.items() if key.startswith(path + "/")]

    def head(self, path, max_bytes):
        self._called("head")
        return self.files[path][:max_bytes].decode("utf-8")

    def put(self, path, content, overwrite=False):
        self._called("put")
        self.puts.append((path, content, overwrite))
        if path in self.files and not overwrite:
            raise FileExistsError(path)
        if self.put_result:
            self.files[path] = content.encode("utf-8")
        return self.put_result


class IngestionDemoTests(unittest.TestCase):
    def test_fixtures_are_stable_complete_distinct_sessions(self):
        run_ids, transaction_ids, event_ids = set(), set(), set()
        for batch in demo.BATCHES:
            content = demo.fixture_content(batch)
            self.assertTrue(content.endswith("\n"))
            self.assertNotIn("\r", content)
            self.assertEqual(hashlib.sha256(content.encode()).hexdigest(), FILE_HASHES[batch])
            rows = [json.loads(line) for line in content.splitlines()]
            self.assertEqual(len(rows), 3)
            bodies = [json.loads(row["payload"]) for row in rows]
            self.assertEqual([body["seqNo"] for body in bodies], [0, 1, 2])
            self.assertEqual([body["eventType"] for body in bodies], ["Started", "Updated", "Ended"])
            self.assertEqual(len({row["run_id"] for row in rows}), 1)
            self.assertEqual(len({row["charging_station_id"] for row in rows}), 1)
            self.assertEqual(len({body["transactionInfo"]["transactionId"] for body in bodies}), 1)
            start = datetime.fromisoformat(bodies[0]["timestamp"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(bodies[2]["timestamp"].replace("Z", "+00:00"))
            self.assertEqual((end - start).total_seconds(), 3600)
            meter_values = [body["meterValue"][0]["sampledValue"][0]["value"] for body in bodies]
            self.assertEqual(meter_values, [100000, 106000, 112500])
            for row, body in zip(rows, bodies):
                self.assertEqual(set(row), {
                    "schema_version", "run_id", "event_id", "charging_station_id", "payload",
                })
                self.assertEqual(row["schema_version"], 1)
                self.assertIs(type(row["schema_version"]), int)
                self.assertIn(body["eventType"].lower(), row["event_id"])
                self.assertEqual(body["timestamp"], body["meterValue"][0]["timestamp"])
                self.assertNotIn(row["event_id"], event_ids)
                event_ids.add(row["event_id"])
            run_ids.add(rows[0]["run_id"])
            transaction_ids.add(bodies[0]["transactionInfo"]["transactionId"])
        self.assertEqual(len(run_ids), 2)
        self.assertEqual(len(transaction_ids), 2)
        self.assertEqual(len(event_ids), 6)

    def test_first_batch_preserves_exact_smoke_payload_strings(self):
        source_bodies = re.findall(
            r"'(\{\"eventType\":[^'\r\n]+})'",
            (ROOT / "sql/02_create_ocpp_transaction_events_raw.sql").read_text(),
        )
        published_bodies = [json.loads(line)["payload"]
                            for line in demo.fixture_content("batch_001").splitlines()]
        self.assertEqual(len(source_bodies), 3)
        self.assertEqual(published_bodies, source_bodies)

    def test_first_publish_then_repeat_does_not_rewrite_file(self):
        fs = FakeFileSystem()
        first = demo.publish_batch(fs, INPUT_PATH, "batch_001")
        before = dict(fs.files)
        repeated = demo.publish_batch(fs, INPUT_PATH + "/", "batch_001")
        self.assertEqual(first["status"], "published")
        self.assertEqual(repeated, dict(first, status="unchanged"))
        self.assertEqual(first["records"], 3)
        self.assertEqual(first["file_sha256"], FILE_HASHES["batch_001"])
        self.assertEqual(fs.files, before)
        self.assertEqual(len(fs.puts), 1)
        self.assertIs(fs.puts[0][2], False)
        self.assertEqual(list(fs.files), [INPUT_PATH + "/batch_001.jsonl"])

    def test_second_publish_only_adds_selected_batch(self):
        fs = FakeFileSystem()
        demo.publish_batch(fs, INPUT_PATH, "batch_001")
        first_bytes = fs.files[INPUT_PATH + "/batch_001.jsonl"]
        demo.publish_batch(fs, INPUT_PATH, "batch_002")
        self.assertEqual(fs.files[INPUT_PATH + "/batch_001.jsonl"], first_bytes)
        self.assertEqual(len(fs.files), 2)
        self.assertEqual(sum(len(content.splitlines()) for content in fs.files.values()), 6)

    def test_conflicting_content_and_appended_suffix_are_never_overwritten(self):
        original = demo.fixture_content("batch_001").encode("utf-8")
        for conflicting in (original.replace(b"smoke-run-v1", b"wrong-run-v1"),
                            original + b"\n", b"", original[:-1]):
            with self.subTest(size=len(conflicting)):
                fs = FakeFileSystem()
                fs.files[INPUT_PATH + "/batch_001.jsonl"] = conflicting
                with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
                    demo.publish_batch(fs, INPUT_PATH, "batch_001")
                self.assertEqual(fs.puts, [])
                self.assertEqual(fs.files[INPUT_PATH + "/batch_001.jsonl"], conflicting)

    def test_invalid_batch_is_rejected_before_filesystem_access(self):
        for batch in ("", "batch_003", "../batch_001", "batch_001.jsonl", None):
            with self.subTest(batch=batch):
                fs = FakeFileSystem()
                with self.assertRaisesRegex(ValueError, "Unsupported demonstration batch"):
                    demo.publish_batch(fs, INPUT_PATH, batch)
                self.assertEqual(fs.calls, [])

    def test_invalid_input_path_is_rejected_before_filesystem_access(self):
        for path in (None, "", INPUT_PATH + "/../incoming", INPUT_PATH + "/batch_001.jsonl",
                     INPUT_PATH.replace("/incoming", "/checkpoints/ocpp_v1"),
                     INPUT_PATH.replace("/workspace/", "/../"),
                     INPUT_PATH.replace("/workspace/", "//workspace/"),
                     "dbfs:" + INPUT_PATH, INPUT_PATH.replace("/", "\\")):
            with self.subTest(path=path):
                fs = FakeFileSystem()
                with self.assertRaisesRegex(ValueError, "input_path must be"):
                    demo.publish_batch(fs, path, "batch_001")
                self.assertEqual(fs.calls, [])

    def test_permission_and_transport_failures_are_not_treated_as_absence(self):
        for operation in ("mkdirs", "ls", "head", "put"):
            with self.subTest(operation=operation):
                fs = FakeFileSystem()
                if operation == "head":
                    fs.files[INPUT_PATH + "/batch_001.jsonl"] = demo.fixture_content("batch_001").encode()
                fs.errors[operation] = PermissionError(f"Denied {operation}")
                with self.assertRaisesRegex(PermissionError, f"Denied {operation}"):
                    demo.publish_batch(fs, INPUT_PATH, "batch_001")
                self.assertEqual(fs.puts, [])

    def test_false_filesystem_result_fails_publication(self):
        fs = FakeFileSystem()
        fs.mkdir_result = False
        with self.assertRaisesRegex(RuntimeError, "Could not create incoming"):
            demo.publish_batch(fs, INPUT_PATH, "batch_001")
        self.assertEqual(fs.puts, [])
        fs = FakeFileSystem()
        fs.put_result = False
        with self.assertRaisesRegex(RuntimeError, "write did not succeed"):
            demo.publish_batch(fs, INPUT_PATH, "batch_001")
        self.assertEqual(fs.files, {})

    def test_post_write_verification_detects_a_truncated_file(self):
        fs = FakeFileSystem()
        original_put = fs.put

        def truncated_put(path, content, overwrite=False):
            return original_put(path, content[:-1], overwrite)

        fs.put = truncated_put
        with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
            demo.publish_batch(fs, INPUT_PATH, "batch_001")
        self.assertEqual(len(fs.puts), 1)

    def test_concurrent_file_creation_does_not_enable_overwrite(self):
        fs = FakeFileSystem()
        original_put = fs.put

        def concurrent_put(path, content, overwrite=False):
            fs.files[path] = b"different publisher's evidence"
            return original_put(path, content, overwrite)

        fs.put = concurrent_put
        with self.assertRaises(FileExistsError):
            demo.publish_batch(fs, INPUT_PATH, "batch_001")
        self.assertEqual(fs.files[INPUT_PATH + "/batch_001.jsonl"], b"different publisher's evidence")

    def test_wrapper_reads_job_parameters_and_publishes_selected_batch(self):
        parameters = {"input_path": INPUT_PATH, "batch": "batch_002"}
        widgets = SimpleNamespace(text=lambda name, default: None, get=parameters.__getitem__)
        dbutils = SimpleNamespace(fs=FakeFileSystem(), widgets=widgets)
        with patch.dict("sys.modules", {"ocpp_ingestion_demo": demo}), patch("builtins.print"):
            wrapper.run(dbutils)
        self.assertEqual(list(dbutils.fs.files), [INPUT_PATH + "/batch_002.jsonl"])


if __name__ == "__main__":
    unittest.main()
