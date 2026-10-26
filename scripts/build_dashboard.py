"""Build the committed native AI/BI dashboard; only Python's stdlib is needed.

Run with --check to detect stale embedded SQL or layout without changing files.
Widget/filter shapes follow Databricks' published dashboard examples:
https://github.com/databricks/databricks-agent-skills/tree/main/plugins/databricks/claude/skills/databricks-aibi-dashboards/references
This checks the source asset, not live Databricks SQL execution or rendering.
"""

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "dashboards" / "generated_billing.lvdash.json"
LOGO_URL = (
    "https://raw.githubusercontent.com/Selim-Abouleila/ChargeAssert/dev/"
    "docs/images/chargeassert-logo.png"
)
DATASETS = {
    "execution_choices": "Available evaluations",
    "execution_summary": "Selected evaluation",
    "session_bills": "Saved session bills",
}


def selection(value):
    return {"values": {"dataType": "STRING", "values": [{"value": value}]}}


def frame(title, description=None):
    result = {"showTitle": True, "title": title}
    if description:
        result.update(showDescription=True, description=description)
    return result


def place(widget, x, y, width, height):
    return {"widget": widget, "position": {
        "x": x, "y": y, "width": width, "height": height,
    }}


def text_widget(name, title, body):
    return {"name": name, "multilineTextboxSpec": {"lines": [f"## {title}\n\n", body]}}


def query(dataset, fields, *, aggregate=False):
    return {"name": "main_query", "query": {
        "datasetName": dataset,
        "fields": [{"name": field, "expression": f"`{field}`"} for field in fields],
        "disaggregated": not aggregate,
    }}


def counter(name, field, title):
    return {"name": name, "queries": [query("execution_summary", [field])], "spec": {
        "version": 2, "widgetType": "counter", "frame": frame(title),
        "encodings": {"value": {"fieldName": field, "displayName": title,
            "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 0}}}},
    }}


def table(name, dataset, title, columns, description=None):
    encoded = []
    for field, label in columns:
        column = {"fieldName": field, "displayName": label}
        if field in ("failed_checks", "blocked_checks"):
            # Numeric conditional styling follows the published native table example.
            column["style"] = {"type": "basic", "rules": [{
                "condition": {"operand": {"type": "data-value", "value": "0"}, "operator": ">"},
                "backgroundColor": {"hex": "#FFE3DB" if field == "failed_checks" else "#FFF0C2"},
            }]}
        if field in ("expected_amount", "actual_amount", "difference"):
            column["format"] = {
                "type": "number", "decimalPlaces": {"type": "max", "places": 6},
            }
        encoded.append(column)
    return {"name": name, "queries": [query(dataset, [c[0] for c in columns])], "spec": {
        "version": 2, "widgetType": "table", "frame": frame(title, description),
        "encodings": {"columns": encoded},
    }}


def field_filter(name, field, title):
    return {"name": name, "queries": [query("session_bills", [field], aggregate=True)], "spec": {
        "version": 2, "widgetType": "filter-multi-select", "frame": frame(title),
        "encodings": {"fields": [{
            "fieldName": field, "queryName": "main_query", "displayName": title,
        }]},
    }}


def execution_filter():
    choices = query("execution_choices", ["execution_id"], aggregate=True)
    choices["name"] = "available_executions"
    queries = [choices]
    fields = [{"fieldName": "execution_id", "queryName": "available_executions"}]
    for dataset in ("execution_summary", "session_bills"):
        name = dataset + "_parameter"
        queries.append({"name": name, "query": {
            "datasetName": dataset, "disaggregated": False,
            "parameters": [{"name": "execution_id", "keyword": "execution_id"}],
        }})
        fields.append({"parameterName": "execution_id", "queryName": name})
    return {"name": "execution_id", "queries": queries, "spec": {
        "version": 2, "widgetType": "filter-single-select",
        "frame": frame("Evaluation", "Choose latest or a saved execution ID."),
        "selection": {"defaultSelection": selection("latest")},
        "encodings": {"fields": fields},
    }}


def build_dashboard():
    datasets = []
    for name, title in DATASETS.items():
        sql = (ROOT / "sql" / "dashboard" / (name + ".sql")).read_text(encoding="utf-8")
        dataset = {"name": name, "displayName": title, "queryLines": sql.splitlines(keepends=True)}
        if name != "execution_choices":
            dataset["parameters"] = [{
                "keyword": "execution_id", "displayName": "Evaluation", "dataType": "STRING",
                "defaultSelection": selection("latest"),
            }]
        datasets.append(dataset)

    overview = [
        # Public image URLs in Markdown text widgets are supported by Databricks:
        # https://docs.databricks.com/aws/en/dashboards/manage/visualizations/text-widgets
        place({"name": "brand_logo", "multilineTextboxSpec": {
            "lines": [f"![ChargeAssert]({LOGO_URL})"],
        }}, 0, 0, 3, 2),
        place(text_widget("overview_header", "ChargeAssert | Billing checks",
            "Catch incorrect charging bills before a billing release reaches drivers. "
            "Compare the baseline with the candidate using the same simulated sessions."), 3, 0, 9, 2),
        place(text_widget("scope", "Read the result in context",
            "**latest** means the latest evaluation attempt, including failed attempts. "
            "It may not be the latest pipeline run: a run can fail before evaluation starts. "
            "Check the Lakeflow job run for generation and ingestion status. "
            "A completed evaluation can correctly report billing **FAIL**."), 0, 2, 12, 3),
    ]
    for index, (field, label) in enumerate((
        ("session_count", "Charging sessions"), ("passed_checks", "Checks passed"),
        ("failed_checks", "Checks failed"), ("blocked_checks", "Checks blocked"),
    )):
        overview.append(place(counter("card_" + field, field, label), index * 3, 5, 3, 3))
    overview.extend([
        place(table("billing_decision", "execution_summary", "Billing decision", [
            ("baseline_verdict", "Baseline"), ("candidate_verdict", "Candidate"), ("verdict", "Overall"),
        ], "PASS: all checks passed. FAIL: at least one check did not pass. BLOCKED: evidence is incomplete."),
            0, 8, 7, 3),
        place(table("evaluation_state", "execution_summary", "Evaluation receipt", [
            ("evaluation_status", "Evaluation status"), ("assertion_count", "Checks saved"),
        ], "This is the evaluator's status, not the whole pipeline's status."), 7, 8, 5, 3),
        place(table("evaluation_identity", "execution_summary", "Selected evaluation", [
            ("execution_id", "Execution ID"), ("batch_id", "Batch"), ("candidate_mode", "Candidate mode"),
            ("started_at", "Started"), ("finished_at", "Finished"), ("captured_at", "Evidence saved"),
        ]), 0, 11, 12, 3),
        place(table("evaluation_reason", "execution_summary", "What happened", [("reason", "Reason")],
            "Blank counts and BLOCKED verdicts mean there is no trusted billing result for this selection."),
            0, 14, 12, 3),
    ])
    bills = [
        place(text_widget("bills_header", "Which bills need attention?",
            "Each row is one charging session for the baseline or candidate. "
            "Amounts come from the selected evaluation's saved evidence. "
            "A positive difference means the bill is too high. "
            "Choose **FAIL** below to focus on problems."), 0, 0, 12, 2),
        place(field_filter("release_role", "release_role", "Baseline or candidate"), 0, 2, 6, 2),
        place(field_filter("billing_verdict", "verdict", "Session result"), 6, 2, 6, 2),
        place(table("session_bills", "session_bills", "Expected charges and actual bills", [
            ("session_id", "Charging session"), ("release_role", "Release"),
            ("currency", "Currency"), ("expected_amount", "Expected charge"),
            ("actual_amount", "Actual bill"), ("difference", "Over / under charge"),
            ("amount_status", "Amount check"), ("verdict", "Session result"),
            ("passed_checks", "Passed"), ("failed_checks", "Failed"), ("blocked_checks", "Blocked"),
            ("execution_id", "Evaluation ID"),
        ], "Failed checks appear first. Filters on this page do not change the overview totals."),
            0, 4, 12, 10),
        place(text_widget("missing_bills", "No rows?",
            "Check the overview's evaluation status and reason. Failed or incomplete evaluations "
            "show no bills; older results are never substituted. Clear the page filters to see all bills."),
            0, 14, 12, 2),
    ]
    pages = [
        {"name": "overview", "displayName": "Overview", "pageType": "PAGE_TYPE_CANVAS",
         "layoutVersion": "GRID_V1", "layout": overview},
        {"name": "bills", "displayName": "Session bills", "pageType": "PAGE_TYPE_CANVAS",
         "layoutVersion": "GRID_V1", "layout": bills},
        {"name": "filters", "displayName": "Choose an evaluation", "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
         "layoutVersion": "GRID_V1", "layout": [place(execution_filter(), 0, 0, 6, 2)]},
    ]
    return {"datasets": datasets, "pages": pages, "uiSettings": {"theme": {
        "canvasBackgroundColor": {"light": "#F4F7F8", "dark": "#17232B"},
        "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#202F39"},
        "widgetBorderColor": {"light": "#FFFFFF", "dark": "#202F39"},
        "fontColor": {"light": "#172D3B", "dark": "#ECF2F5"},
        "selectionColor": {"light": "#0072B2", "dark": "#71C6F5"},
        "visualizationColors": ["#009E73", "#D55E00", "#E69F00", "#0072B2", "#CC79A7"],
        "widgetHeaderAlignment": "LEFT",
    }}}


def serialize():
    return json.dumps(build_dashboard(), ensure_ascii=False, indent=2) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if the committed dashboard is stale.")
    args = parser.parse_args()
    content = serialize()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != content:
            parser.exit(1, "Dashboard is stale. Run: python -B scripts/build_dashboard.py\n")
        print("Dashboard matches its SQL and layout sources.")
    else:
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(content, encoding="utf-8", newline="\n")
        print(f"Built {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
