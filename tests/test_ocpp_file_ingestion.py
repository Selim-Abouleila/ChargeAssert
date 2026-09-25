"""Test the actual ingestion function at a narrow Spark API boundary.

The adapter checks raw-record/metadata projection and stream configuration.
It is not Auto Loader: the 3 -> 3 -> 6 -> 6 checkpoint demonstration must run
in Databricks, as must Delta constraints and streaming failure recovery.
"""

from datetime import datetime
import hashlib
from pathlib import Path
import re
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from notebooks import ocpp_file_ingestion as ingestion


ROOT = Path(__file__).resolve().parents[1]
INPUT = "/Volumes/workspace/chargeassert_dev_bronze/ocpp_ingestion/incoming"
CHECKPOINT = "/Volumes/workspace/chargeassert_dev_bronze/ocpp_ingestion/checkpoints/ocpp_v1"
TABLE = "workspace.chargeassert_dev_bronze.ocpp_events_landing"


class Expression:
    def __init__(self, evaluate):
        self.evaluate = evaluate

    def alias(self, name):
        return name, self.evaluate


def field(name):
    def evaluate(row):
        value = row
        for key in name.split("."):
            value = value[key]
        return value
    return Expression(evaluate)


class FileIngestionTests(unittest.TestCase):
    def setUp(self):
        self.timestamp = datetime(2026, 9, 25, 12)
        self.rows = [
            {"value": '  {"event_id":"same-business-event", "payload":"é"}  ',
             "_metadata": {"file_path": INPUT + "/batch_001.jsonl", "file_name": "batch_001.jsonl",
                           "file_modification_time": datetime(2026, 9, 25, 10)}},
            {"value": '{broken JSON',
             "_metadata": {"file_path": INPUT + "/batch_002.jsonl", "file_name": "batch_002.jsonl",
                           "file_modification_time": datetime(2026, 9, 25, 11)}},
        ]
        self.projected = []
        self.reader = Mock()
        for method in ("format", "option", "schema"):
            getattr(self.reader, method).return_value = self.reader
        self.writer = Mock()
        for method in ("format", "outputMode", "option", "trigger"):
            getattr(self.writer, method).return_value = self.writer
        self.query = self.writer.toTable.return_value

        def select(*columns):
            self.projected[:] = [
                {name: evaluate(row) for name, evaluate in columns} for row in self.rows
            ]
            return SimpleNamespace(writeStream=self.writer)

        self.reader.load.return_value = SimpleNamespace(select=select)
        self.spark = SimpleNamespace(readStream=self.reader, sql=Mock())
        functions = ModuleType("pyspark.sql.functions")
        functions.col = field

        def sha2(expression, bits):
            self.assertEqual(bits, 256)
            return Expression(lambda row: hashlib.sha256(expression.evaluate(row).encode("utf-8")).hexdigest())

        functions.sha2 = sha2
        functions.current_timestamp = lambda: Expression(lambda row: self.timestamp)
        modules = {name: ModuleType(name) for name in ("pyspark", "pyspark.sql")}
        modules["pyspark.sql.functions"] = functions
        modules["pyspark.sql"].functions = functions
        modules["pyspark"].sql = modules["pyspark.sql"]
        self.module_patch = patch.dict(sys.modules, modules)
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)

    def run_ingestion(self, **overrides):
        with patch("builtins.print") as output:
            ingestion.ingest(self.spark, **{
                "input_path": INPUT, "checkpoint_path": CHECKPOINT, "table_name": TABLE, **overrides,
            })
        return output

    def test_preserves_raw_lines_and_file_metadata_without_parsing_or_dropping(self):
        self.run_ingestion()
        self.assertEqual(len(self.projected), 2)
        for source, landed in zip(self.rows, self.projected):
            self.assertEqual(landed, {
                "raw_record": source["value"],
                "record_hash": hashlib.sha256(source["value"].encode("utf-8")).hexdigest(),
                "source_file_path": source["_metadata"]["file_path"],
                "source_file_name": source["_metadata"]["file_name"],
                "source_file_modification_time": source["_metadata"]["file_modification_time"],
                "ingested_at": self.timestamp,
            })

    def test_identical_records_in_different_files_are_preserved_for_downstream_deduplication(self):
        self.rows[1]["value"] = self.rows[0]["value"]
        self.run_ingestion()
        self.assertEqual(len(self.projected), 2)
        self.assertEqual(self.projected[0]["record_hash"], self.projected[1]["record_hash"])
        self.assertNotEqual(self.projected[0]["source_file_path"], self.projected[1]["source_file_path"])

    def test_reads_existing_and_new_immutable_jsonl_files_with_fixed_text_schema(self):
        self.run_ingestion()
        self.reader.format.assert_called_once_with("cloudFiles")
        self.reader.schema.assert_called_once_with("value STRING")
        self.reader.load.assert_called_once_with(INPUT)
        options = {call.args[0]: call.args[1] for call in self.reader.option.call_args_list}
        self.assertEqual(options["cloudFiles.format"], "text")
        self.assertEqual(options["cloudFiles.includeExistingFiles"], "true")
        self.assertEqual(options["cloudFiles.allowOverwrites"], "false")
        self.assertEqual(options["cloudFiles.partitionColumns"], "")
        self.assertEqual(options["pathGlobFilter"], "*.jsonl")
        self.assertNotIn("cloudFiles.cleanSource", options)

    def test_uses_same_checkpoint_and_append_sink_on_each_invocation_and_waits(self):
        self.run_ingestion()
        self.run_ingestion()
        self.assertEqual(self.writer.option.call_count, 2)
        for call in self.writer.option.call_args_list:
            self.assertEqual(call.args, ("checkpointLocation", CHECKPOINT))
        for call in self.writer.outputMode.call_args_list:
            self.assertEqual(call.args, ("append",))
        self.assertEqual(self.writer.outputMode.call_count, 2)
        for call in self.writer.trigger.call_args_list:
            self.assertEqual(call.kwargs, {"availableNow": True})
        self.assertEqual(self.writer.trigger.call_count, 2)
        for call in self.writer.toTable.call_args_list:
            self.assertEqual(call.args, (TABLE,))
        self.assertEqual(self.query.awaitTermination.call_count, 2)
        self.assertEqual(self.spark.sql.call_args.kwargs, {"args": {"table_name": TABLE}})

    def test_stream_failure_propagates_without_printing_completion(self):
        self.query.awaitTermination.side_effect = RuntimeError("stream failed")
        with patch("builtins.print") as output, self.assertRaisesRegex(RuntimeError, "stream failed"):
            ingestion.ingest(self.spark, input_path=INPUT, checkpoint_path=CHECKPOINT, table_name=TABLE)
        output.assert_not_called()

    def test_rejects_changed_sink_checkpoint_and_unsafe_source_before_any_spark_calls(self):
        cases = (
            {"input_path": INPUT + "/../incoming"},
            {"input_path": INPUT.rsplit("/", 1)[0]},
            {"input_path": INPUT.replace("workspace", "workspace;DROP")},
            {"input_path": None},
            {"checkpoint_path": INPUT + "/checkpoint"},
            {"checkpoint_path": CHECKPOINT + "_new"},
            {"table_name": TABLE.replace("ocpp_events_landing", "ocpp_transaction_events_raw")},
            {"table_name": TABLE.replace("workspace", "other_catalog")},
        )
        for parameters in cases:
            with self.subTest(parameters=parameters), self.assertRaises(ValueError):
                self.run_ingestion(**parameters)
        self.spark.sql.assert_not_called()
        self.reader.load.assert_not_called()

    def test_catalog_override_keeps_all_three_locations_consistent(self):
        ingestion.validate_config(INPUT.replace("workspace", "my-catalog"),
                                  CHECKPOINT.replace("workspace", "my-catalog"),
                                  TABLE.replace("workspace", "my-catalog"))


class IngestionBundleTests(unittest.TestCase):
    def test_separate_jobs_resolve_to_the_runtime_contract_and_keep_checkpoint_storage(self):
        resource = (ROOT / "resources/ingestion.yml").read_text(encoding="utf-8")
        self.assertIn("      volume_type: MANAGED", resource)
        self.assertIn("        prevent_destroy: true", resource)
        self.assertEqual(resource.count("      max_concurrent_runs: 1"), 2)
        self.assertNotRegex(resource, r"(?m)^      (?:schedule|trigger|continuous):")
        self.assertNotRegex(resource, r"(?m)^          (?:new_cluster|existing_cluster_id|job_cluster_key):")
        references = {
            "${var.catalog_name}": "workspace", "${resources.schemas.bronze.name}": "chargeassert_dev_bronze",
            "${resources.volumes.ocpp_ingestion.name}": "ocpp_ingestion",
        }
        resolved = resource
        for key, value in references.items():
            resolved = resolved.replace(key, value)
        ingest_block = resolved.split("    ingest_ocpp_files:\n")[1]
        parameters = dict(re.findall(r"(?m)^              (\w+): (.+)$", ingest_block))
        ingestion.validate_config(**parameters)
        self.assertIn('batch: "{{job.parameters.batch}}"', resource)
        for path in re.findall(r"(?m)^            notebook_path: (.+)$", resource):
            notebook = ROOT / "resources" / path
            self.assertTrue(notebook.read_text(encoding="utf-8").startswith("# Databricks notebook source"))
        self.assertIn("data/ingestion_demo/*.jsonl", (ROOT / "databricks.yml").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
