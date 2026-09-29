"""Check repeatable sessions and safe publication without Spark or cloud access."""

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import random
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from notebooks import generate_sessions as wrapper
from notebooks import session_generator as generator


INPUT_PATH = "/Volumes/workspace/chargeassert_dev_bronze/ocpp_ingestion/incoming"


class FakeFileSystem:
    def __init__(self):
        self.files = {}
        self.calls = []
        self.puts = []
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
        return [SimpleNamespace(name=key.rsplit("/", 1)[-1], size=len(content))
                for key, content in self.files.items() if key.startswith(path + "/")]

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


def rows_in(content):
    return [json.loads(line) for line in content.splitlines()]


def session_groups(content):
    sessions = defaultdict(list)
    for row in rows_in(content):
        body = json.loads(row["payload"])
        sessions[body["transactionInfo"]["transactionId"]].append((row, body))
    return sessions


class SessionGeneratorTests(unittest.TestCase):
    def test_two_sessions_have_complete_distinct_ocpp_events_and_provenance(self):
        content = generator.generate_batch("demo_one", 2, 42)
        self.assertTrue(content.endswith("\n"))
        self.assertNotIn("\r", content)
        rows = rows_in(content)
        self.assertEqual(len(rows), 6)
        self.assertEqual(len({row["event_id"] for row in rows}), 6)
        self.assertEqual({row["run_id"] for row in rows}, {"generated-v1-demo_one"})
        sessions = session_groups(content)
        self.assertEqual(len(sessions), 2)
        for pairs in sessions.values():
            bodies = [body for row, body in pairs]
            self.assertEqual([body["eventType"] for body in bodies], ["Started", "Updated", "Ended"])
            self.assertEqual([body["seqNo"] for body in bodies], [0, 1, 2])
            self.assertEqual(len({row["charging_station_id"] for row, body in pairs}), 1)
            times = [datetime.fromisoformat(body["timestamp"].replace("Z", "+00:00"))
                     for body in bodies]
            self.assertTrue(all(stamp.tzinfo == timezone.utc for stamp in times))
            self.assertLess(times[0], times[1])
            self.assertLess(times[1], times[2])
            readings = []
            for row, body in pairs:
                self.assertEqual(row["schema_version"], 1)
                self.assertIs(type(row["schema_version"]), int)
                self.assertEqual(row["generator"], {
                    "version": "sessions-v1",
                    "batch_id": "demo_one",
                    "seed": 42,
                    "session_count": 2,
                    "tariff": {
                        "id": "generated-energy-v1", "currency": "EUR", "price_per_kwh": "0.45",
                    },
                })
                self.assertEqual(body["timestamp"], body["meterValue"][0]["timestamp"])
                sampled = body["meterValue"][0]["sampledValue"][0]
                self.assertIs(type(sampled["value"]), int)
                self.assertEqual(sampled["unitOfMeasure"], {"unit": "Wh", "multiplier": 0})
                self.assertEqual(sampled["measurand"], "Energy.Active.Import.Register")
                readings.append(sampled["value"])
            self.assertGreaterEqual(readings[0], 0)
            self.assertLess(readings[0], readings[1])
            self.assertLess(readings[1], readings[2])

    def test_generation_is_repeatable_and_does_not_use_global_random_state(self):
        initial_state = random.getstate()
        try:
            random.seed(10)
            before = random.getstate()
            first = generator.generate_batch("repeat", 8, 42)
            self.assertEqual(random.getstate(), before)
            random.seed(99)
            repeated = generator.generate_batch("repeat", 8, 42)
            self.assertEqual(first.encode("utf-8"), repeated.encode("utf-8"))
            self.assertNotEqual(first, generator.generate_batch("repeat", 8, 43))
            sessions = session_groups(first)
            energies = []
            for pairs in sessions.values():
                values = [body["meterValue"][0]["sampledValue"][0]["value"] for row, body in pairs]
                energies.append(values[-1] - values[0])
            self.assertGreater(len(set(energies)), 1)
        finally:
            random.setstate(initial_state)

    def test_different_batches_have_disjoint_session_and_event_ids(self):
        first = generator.generate_batch("batch_one", 3, 42)
        second = generator.generate_batch("batch_two", 3, 42)
        self.assertTrue(set(session_groups(first)).isdisjoint(session_groups(second)))
        self.assertTrue({row["event_id"] for row in rows_in(first)}.isdisjoint(
            {row["event_id"] for row in rows_in(second)}))

    def test_allowed_parameter_boundaries(self):
        for batch, count, seed in (("a", 1, 0), ("a" + "0" * 31, 1000, 2**32 - 1)):
            with self.subTest(count=count, seed=seed):
                content = generator.generate_batch(batch, count, seed)
                rows = rows_in(content)
                self.assertEqual(len(rows), count * 3)
                self.assertEqual(len(session_groups(content)), count)
                self.assertEqual(len({row["event_id"] for row in rows}), count * 3)

    def test_invalid_parameters_are_rejected_before_filesystem_access(self):
        invalid_arguments = [
            (batch, 2, 42) for batch in
            (None, "", "1batch", "UPPER", "../escape", "batch/file", "a" * 33, "batch\n")
        ] + [
            ("batch", count, 42) for count in (None, False, True, 0, -1, 1001, 2.0, "2")
        ] + [
            ("batch", 2, seed) for seed in (None, False, True, -1, 2**32, 42.0, "42")
        ]
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments):
                fs = FakeFileSystem()
                with self.assertRaises(ValueError):
                    generator.publish_batch(fs, INPUT_PATH, *arguments)
                self.assertEqual(fs.calls, [])

    def test_invalid_input_paths_are_rejected_before_filesystem_access(self):
        for path in (None, "", INPUT_PATH + "/../incoming", INPUT_PATH + "/file.jsonl",
                     INPUT_PATH.replace("/incoming", "/checkpoints/ocpp_v1"),
                     INPUT_PATH.replace("/workspace/", "/../"),
                     INPUT_PATH.replace("/workspace/", "//workspace/"),
                     "dbfs:" + INPUT_PATH, INPUT_PATH.replace("/", "\\")):
            with self.subTest(path=path):
                fs = FakeFileSystem()
                with self.assertRaises(ValueError):
                    generator.publish_batch(fs, path, "batch", 2, 42)
                self.assertEqual(fs.calls, [])

    def test_publish_repeat_then_new_batch_gives_six_six_twelve_source_records(self):
        fs = FakeFileSystem()
        first = generator.publish_batch(fs, INPUT_PATH, "batch_one", 2, 42)
        self.assertEqual(first["status"], "published")
        self.assertEqual(first["path"], INPUT_PATH + "/generated-batch_one.jsonl")
        self.assertEqual(first["run_id"], "generated-v1-batch_one")
        self.assertEqual(first["sessions"], 2)
        self.assertEqual(first["records"], 6)
        self.assertEqual(first["file_sha256"], hashlib.sha256(fs.files[first["path"]]).hexdigest())
        self.assertEqual(sum(len(content.splitlines()) for content in fs.files.values()), 6)
        original_files = dict(fs.files)
        repeated = generator.publish_batch(fs, INPUT_PATH, "batch_one", 2, 42)
        self.assertEqual(repeated, dict(first, status="unchanged"))
        self.assertEqual(fs.files, original_files)
        self.assertEqual(len(fs.puts), 1)
        second = generator.publish_batch(fs, INPUT_PATH, "batch_two", 2, 99)
        self.assertEqual(second["status"], "published")
        self.assertEqual(fs.files[first["path"]], original_files[first["path"]])
        self.assertEqual(sum(len(content.splitlines()) for content in fs.files.values()), 12)
        self.assertTrue(all(overwrite is False for path, content, overwrite in fs.puts))

    def test_reusing_batch_id_with_different_settings_fails_without_overwrite(self):
        for count, seed in ((3, 42), (2, 43)):
            with self.subTest(count=count, seed=seed):
                fs = FakeFileSystem()
                generator.publish_batch(fs, INPUT_PATH, "batch", 2, 42)
                original_files = dict(fs.files)
                with self.assertRaises(ValueError):
                    generator.publish_batch(fs, INPUT_PATH, "batch", count, seed)
                self.assertEqual(fs.files, original_files)
                self.assertEqual(len(fs.puts), 1)

    def test_conflicting_bytes_truncation_and_suffix_are_never_overwritten(self):
        original = generator.generate_batch("batch", 2, 42).encode("utf-8")
        for conflicting in (original.replace(b"EUR", b"USD"), b"", original[:-1], original + b"\n"):
            with self.subTest(size=len(conflicting)):
                fs = FakeFileSystem()
                path = INPUT_PATH + "/generated-batch.jsonl"
                fs.files[path] = conflicting
                with self.assertRaises(ValueError):
                    generator.publish_batch(fs, INPUT_PATH, "batch", 2, 42)
                self.assertEqual(fs.files[path], conflicting)
                self.assertEqual(fs.puts, [])

    def test_permission_and_transport_failures_propagate(self):
        for operation in ("mkdirs", "ls", "head", "put"):
            with self.subTest(operation=operation):
                fs = FakeFileSystem()
                if operation == "head":
                    fs.files[INPUT_PATH + "/generated-batch.jsonl"] = generator.generate_batch(
                        "batch", 2, 42).encode("utf-8")
                fs.errors[operation] = PermissionError(f"Denied {operation}")
                with self.assertRaisesRegex(PermissionError, f"Denied {operation}"):
                    generator.publish_batch(fs, INPUT_PATH, "batch", 2, 42)
                self.assertEqual(fs.puts, [])

    def test_false_filesystem_results_fail_publication(self):
        fs = FakeFileSystem()
        fs.mkdir_result = False
        with self.assertRaises(RuntimeError):
            generator.publish_batch(fs, INPUT_PATH, "batch", 2, 42)
        self.assertEqual(fs.puts, [])
        fs = FakeFileSystem()
        fs.put_result = False
        with self.assertRaises(RuntimeError):
            generator.publish_batch(fs, INPUT_PATH, "batch", 2, 42)
        self.assertEqual(fs.files, {})

    def test_post_write_verification_detects_a_truncated_file(self):
        fs = FakeFileSystem()
        original_put = fs.put

        def truncated_put(path, content, overwrite=False):
            return original_put(path, content[:-1], overwrite)

        fs.put = truncated_put
        with self.assertRaises(ValueError):
            generator.publish_batch(fs, INPUT_PATH, "batch", 2, 42)
        self.assertEqual(len(fs.puts), 1)

    def test_concurrent_creation_cannot_enable_overwriting_evidence(self):
        fs = FakeFileSystem()
        original_put = fs.put

        def concurrent_put(path, content, overwrite=False):
            fs.files[path] = b"another publisher's evidence"
            return original_put(path, content, overwrite)

        fs.put = concurrent_put
        with self.assertRaises(FileExistsError):
            generator.publish_batch(fs, INPUT_PATH, "batch", 2, 42)
        self.assertEqual(fs.files[INPUT_PATH + "/generated-batch.jsonl"], b"another publisher's evidence")
        self.assertIs(fs.puts[0][2], False)


    def test_wrapper_reads_job_parameters_and_prints_publication_receipt(self):
        parameters = {"input_path": INPUT_PATH, "batch_id": "from_job", "session_count": "2", "seed": "42"}
        widgets = SimpleNamespace(text=lambda name, default: None, get=parameters.__getitem__)
        dbutils = SimpleNamespace(fs=FakeFileSystem(), widgets=widgets)
        with patch.dict("sys.modules", {"session_generator": generator}), patch("builtins.print") as output:
            wrapper.run(dbutils)
        self.assertEqual(list(dbutils.fs.files), [INPUT_PATH + "/generated-from_job.jsonl"])
        receipt = json.loads(output.call_args.args[0])
        self.assertEqual(receipt["status"], "published")
        self.assertEqual(receipt["sessions"], 2)
        self.assertEqual(receipt["records"], 6)

    def test_wrapper_rejects_missing_or_invalid_widgets_before_filesystem_access(self):
        valid = {"input_path": INPUT_PATH, "batch_id": "from_job", "session_count": "2", "seed": "42"}
        invalid_parameters = [(name, "") for name in valid]
        invalid_parameters += [(name, value) for name in ("session_count", "seed")
                               for value in ("2.5", " 2", "+2", "-1", "true", "12345678901")]
        invalid_parameters += [("session_count", "1001"), ("seed", "4294967296")]
        for name, value in invalid_parameters:
            with self.subTest(name=name, value=value):
                parameters = dict(valid, **{name: value})
                widgets = SimpleNamespace(text=lambda name, default: None, get=parameters.__getitem__)
                dbutils = SimpleNamespace(fs=FakeFileSystem(), widgets=widgets)
                with patch.dict("sys.modules", {"session_generator": generator}), self.assertRaises(ValueError):
                    wrapper.run(dbutils)
                self.assertEqual(dbutils.fs.calls, [])

if __name__ == "__main__":
    unittest.main()
