#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.cfbd_loader import fetch_dataset


CFBD_DIR = ROOT / "data" / "sources" / "cfbd"
REPORT_PATH = ROOT / "data" / "outputs" / "cfbd_historical_comp_inputs_pull_report_2026-03-05.md"


DEFAULT_DATASETS = ["player_season_stats", "player_ppa", "player_usage"]


def _parse_datasets(raw: str) -> list[str]:
    items = [s.strip() for s in str(raw or "").split(",") if s.strip()]
    return items or list(DEFAULT_DATASETS)


def _out_path(dataset: str, year: int) -> Path:
    return CFBD_DIR / f"{dataset}_{year}.json"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Pull CFBD historical datasets used by production comp generation "
            "(player season stats, player PPA, player usage)."
        )
    )
    p.add_argument("--start-year", type=int, default=2016)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument("--datasets", type=str, default=",".join(DEFAULT_DATASETS))
    p.add_argument("--max-calls", type=int, default=1000)
    p.add_argument(
        "--execute",
        action="store_true",
        help="Perform live API calls (without this, outputs dry-run metadata only).",
    )
    p.add_argument("--report", type=Path, default=REPORT_PATH)
    return p


def main() -> None:
    args = build_parser().parse_args()
    datasets = _parse_datasets(args.datasets)
    years = list(range(int(args.start_year), int(args.end_year) + 1))
    CFBD_DIR.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [
        "# CFBD Historical Comp Inputs Pull Report",
        "",
        f"- generated_utc: {datetime.now(timezone.utc).isoformat()}",
        f"- execute: {bool(args.execute)}",
        f"- year_range: {args.start_year}-{args.end_year}",
        f"- datasets: {', '.join(datasets)}",
        f"- max_calls: {args.max_calls}",
        "",
        "## Pull Results",
        "",
        "| Dataset | Year | Status | Rows | Calls Used | Calls Remaining | Output |",
        "|---|---:|---|---:|---:|---:|---|",
    ]

    success = 0
    failed = 0
    for dataset in datasets:
        for year in years:
            try:
                result = fetch_dataset(
                    dataset=dataset,
                    year=year,
                    execute=bool(args.execute),
                    max_calls_per_month=int(args.max_calls),
                )
                rows = len(result.get("data", []) or []) if not result.get("dry_run", False) else 0
                status = "dry-run" if result.get("dry_run", False) else "ok"
                out_path = _out_path(dataset, year)
                if not result.get("dry_run", False):
                    out_path.write_text(json.dumps(result, indent=2))
                calls_used = result.get("calls_used", "")
                calls_remaining = result.get("calls_remaining", "")
                lines.append(
                    f"| {dataset} | {year} | {status} | {rows} | {calls_used} | {calls_remaining} | `{out_path}` |"
                )
                success += 1
            except Exception as exc:
                failed += 1
                lines.append(f"| {dataset} | {year} | ERROR | 0 | - | - | `{type(exc).__name__}: {exc}` |")

    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- jobs_succeeded: {success}",
            f"- jobs_failed: {failed}",
            "",
        ]
    )
    args.report.write_text("\n".join(lines))
    print(f"Report: {args.report}")
    print(f"Succeeded: {success} | Failed: {failed}")


if __name__ == "__main__":
    main()

