"""Protect the native dashboard's dataset, filter and layout contracts."""

import json
from pathlib import Path
import subprocess
import sys
import unittest

from scripts import build_dashboard as builder


ROOT = Path(__file__).resolve().parents[1]
SUMMARY_COLUMNS = {
    "execution_id", "batch_id", "candidate_mode", "started_at", "finished_at",
    "evaluation_status", "reason", "usable", "captured_at", "session_count",
    "assertion_count", "passed_checks", "failed_checks", "blocked_checks",
    "baseline_verdict", "candidate_verdict", "verdict",
}
BILL_COLUMNS = {
    "execution_id", "session_id", "release_role", "currency", "expected_amount",
    "actual_amount", "difference", "amount_status", "verdict", "passed_checks",
    "failed_checks", "blocked_checks",
}


class DashboardAssetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.asset = json.loads(builder.OUTPUT.read_text(encoding="utf-8"))
        cls.datasets = {row["name"]: row for row in cls.asset["datasets"]}
        cls.pages = {page["name"]: page for page in cls.asset["pages"]}
        cls.widgets = {
            item["widget"]["name"]: item["widget"]
            for page in cls.asset["pages"] for item in page["layout"]
        }

    def test_committed_asset_rebuilds_exactly_and_check_does_not_write(self):
        original = builder.OUTPUT.read_bytes()
        self.assertEqual(original.decode("utf-8"), builder.serialize())
        result = subprocess.run(
            [sys.executable, "-B", str(ROOT / "scripts/build_dashboard.py"), "--check"],
            capture_output=True, text=True, cwd=ROOT, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(builder.OUTPUT.read_bytes(), original)

    def test_queries_are_embedded_verbatim_with_portable_catalog_resolution(self):
        self.assertEqual(set(self.datasets), set(builder.DATASETS))
        for name, dataset in self.datasets.items():
            source = (ROOT / "sql/dashboard" / (name + ".sql")).read_text(encoding="utf-8")
            self.assertEqual("".join(dataset["queryLines"]), source)
            self.assertNotIn("workspace.", source)
            self.assertNotIn("${", source, "Bundle placeholders are not expanded in embedded SQL")

    def test_global_selector_updates_both_datasets_and_offers_latest(self):
        page = self.pages["filters"]
        self.assertEqual(page["pageType"], "PAGE_TYPE_GLOBAL_FILTERS")
        widget = page["layout"][0]["widget"]
        self.assertEqual(widget["name"], "execution_id")
        self.assertEqual(widget["spec"]["widgetType"], "filter-single-select")
        self.assertEqual(widget["spec"]["selection"]["defaultSelection"], builder.selection("latest"))
        queries = {item["name"]: item["query"] for item in widget["queries"]}
        targets = set()
        for binding in widget["spec"]["encodings"]["fields"]:
            query = queries[binding["queryName"]]
            self.assertFalse(query["disaggregated"])
            if "parameterName" in binding:
                self.assertEqual(binding["parameterName"], "execution_id")
                self.assertEqual(query["parameters"], [{"name": "execution_id", "keyword": "execution_id"}])
                targets.add(query["datasetName"])
            else:
                self.assertEqual(query["datasetName"], "execution_choices")
                self.assertEqual(binding["fieldName"], "execution_id")
        self.assertEqual(targets, {"execution_summary", "session_bills"})
        for name in targets:
            self.assertEqual(self.datasets[name]["parameters"], [{
                "keyword": "execution_id", "displayName": "Evaluation", "dataType": "STRING",
                "defaultSelection": builder.selection("latest"),
            }])
        self.assertNotIn("parameters", self.datasets["execution_choices"])

    def test_widget_fields_match_dataset_and_query_columns(self):
        contracts = {
            "execution_summary": SUMMARY_COLUMNS, "session_bills": BILL_COLUMNS,
            "execution_choices": {"execution_id"},
        }
        for widget in self.widgets.values():
            for query in widget.get("queries", []):
                body = query["query"]
                self.assertIn(body["datasetName"], contracts)
                for field in body.get("fields", []):
                    self.assertIn(field["name"], contracts[body["datasetName"]])
                    self.assertEqual(field["expression"], "`" + field["name"] + "`")
            spec = widget.get("spec")
            if not spec or spec["widgetType"].startswith("filter-"):
                continue
            query_fields = {field["name"] for field in widget["queries"][0]["query"]["fields"]}
            encoded = (spec["encodings"]["columns"] if spec["widgetType"] == "table"
                       else [spec["encodings"]["value"]])
            self.assertEqual({column["fieldName"] for column in encoded}, query_fields)
            self.assertNotIn("parameters", widget["queries"][0]["query"])

    def test_detail_filters_do_not_change_overview_totals(self):
        overview = self.pages["overview"]["layout"]
        self.assertFalse(any(item["widget"].get("spec", {}).get("widgetType", "").startswith("filter-")
                             for item in overview))
        filters = [item["widget"] for item in self.pages["bills"]["layout"]
                   if item["widget"].get("spec", {}).get("widgetType") == "filter-multi-select"]
        self.assertEqual({widget["name"] for widget in filters}, {"release_role", "billing_verdict"})
        for widget in filters:
            self.assertEqual(widget["queries"][0]["query"]["datasetName"], "session_bills")
        fields = {field["name"] for field in self.widgets["session_bills"]["queries"][0]["query"]["fields"]}
        self.assertTrue({"execution_id", "expected_amount", "actual_amount", "difference", "currency", "verdict"} <= fields)

    def test_status_and_billing_remain_separate_and_missing_results_are_explained(self):
        status = self.widgets["evaluation_state"]["queries"][0]["query"]["fields"]
        self.assertEqual({field["name"] for field in status}, {"evaluation_status", "assertion_count"})
        decision = self.widgets["billing_decision"]["queries"][0]["query"]["fields"]
        self.assertEqual({field["name"] for field in decision},
                         {"baseline_verdict", "candidate_verdict", "verdict"})
        guidance = "".join(self.widgets["scope"]["multilineTextboxSpec"]["lines"])
        self.assertIn("latest evaluation attempt", guidance)
        self.assertIn("fail before evaluation starts", guidance)
        self.assertIn("Lakeflow job run", guidance)
        self.assertIn("older results are never substituted", "".join(
            self.widgets["missing_bills"]["multilineTextboxSpec"]["lines"]))

    def test_valid_native_widget_versions_and_non_overlapping_grid(self):
        names = []
        for page in self.asset["pages"]:
            self.assertEqual(page["layoutVersion"], "GRID_V1")
            occupied = set()
            for item in page["layout"]:
                widget, box = item["widget"], item["position"]
                names.append(widget["name"])
                self.assertRegex(widget["name"], r"^[a-zA-Z0-9_-]{1,60}$")
                self.assertGreaterEqual(box["x"], 0)
                self.assertGreaterEqual(box["y"], 0)
                self.assertGreater(box["height"], 0)
                self.assertGreater(box["width"], 0)
                self.assertLessEqual(box["x"] + box["width"], 12)
                cells = {(x, y) for x in range(box["x"], box["x"] + box["width"])
                         for y in range(box["y"], box["y"] + box["height"])}
                self.assertFalse(occupied & cells, widget["name"] + " overlaps another widget")
                occupied |= cells
                if "spec" in widget:
                    self.assertEqual(widget["spec"]["version"], 2)
                    self.assertIn(widget["spec"]["widgetType"], {
                        "counter", "table", "filter-single-select", "filter-multi-select",
                    })
                    self.assertTrue(widget["spec"]["frame"]["showTitle"])
                    self.assertNotIn("multilineTextboxSpec", widget)
                else:
                    self.assertIn("multilineTextboxSpec", widget)
        self.assertEqual(len(names), len(set(names)))


if __name__ == "__main__":
    unittest.main()
