"""Package the dashboard SQL with the app so both read the same saved evidence."""

import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUERIES = ("execution_summary.sql", "session_bills.sql")


def build(check=False):
    stale = []
    for name in QUERIES:
        source = ROOT / "sql" / "dashboard" / name
        target = ROOT / "app" / "sql" / name
        if check:
            # Git may apply CRLF on checkout; compare text rather than newline bytes.
            if not target.exists() or target.read_text(encoding="utf-8") != source.read_text(encoding="utf-8"):
                stale.append(name)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
    if stale:
        raise SystemExit("App SQL is stale; run python scripts/build_app_assets.py: " + ", ".join(stale))
    print("App SQL matches the dashboard queries." if check else "Packaged dashboard SQL for the app.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    build(parser.parse_args().check)
