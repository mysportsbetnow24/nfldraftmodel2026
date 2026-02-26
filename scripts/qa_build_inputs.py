#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.prebuild_validation import format_prebuild_report_md, run_prebuild_checks


DEFAULT_SEED = ROOT / "data" / "processed" / "prospect_seed_2026.csv"
DEFAULT_COMBINE = ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv"
DEFAULT_RETURNING = ROOT / "data" / "sources" / "manual" / "returning_to_school_2026.csv"
DEFAULT_OUT_JSON = ROOT / "data" / "outputs" / "prebuild_qa_report.json"
DEFAULT_OUT_MD = ROOT / "data" / "outputs" / "prebuild_qa_report.md"


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate draft build inputs and fail on hard QA errors.")
    parser.add_argument("--seed", type=str, default=str(DEFAULT_SEED))
    parser.add_argument("--combine", type=str, default=str(DEFAULT_COMBINE))
    parser.add_argument("--returning", type=str, default=str(DEFAULT_RETURNING))
    parser.add_argument("--out-json", type=str, default=str(DEFAULT_OUT_JSON))
    parser.add_argument("--out-md", type=str, default=str(DEFAULT_OUT_MD))
    args = parser.parse_args()

    report = run_prebuild_checks(
        seed_path=Path(args.seed),
        combine_path=Path(args.combine),
        returning_path=Path(args.returning),
    )

    out_json = Path(args.out_json)
    out_md = Path(args.out_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2))
    out_md.write_text(format_prebuild_report_md(report))

    print(f"Prebuild QA status: {report.get('status')}")
    print(f"Errors: {report.get('counts', {}).get('errors', 0)}")
    print(f"Warnings: {report.get('counts', {}).get('warnings', 0)}")
    print(f"JSON report: {out_json}")
    print(f"Markdown report: {out_md}")

    if report.get("status") == "fail":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
