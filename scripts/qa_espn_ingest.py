#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.espn_loader import write_espn_qa_report


DEFAULT_BOARD = ROOT / "data" / "outputs" / "big_board_2026.csv"
DEFAULT_OUT_JSON = ROOT / "data" / "outputs" / "espn_ingest_qa_report.json"
DEFAULT_OUT_MD = ROOT / "data" / "outputs" / "espn_ingest_qa_report.md"


def _load_board_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_md(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    checks = report.get("qa_checks", {})
    joins = checks.get("join_coverage", {})
    rows = report.get("row_counts", {})

    lines = [
        "# ESPN Ingest QA Report",
        "",
        f"- Status: `{report.get('status','unknown')}`",
        f"- Dataset directory: `{report.get('dataset_base_dir','')}`",
        "",
        "## Row counts",
        "",
        f"- prospects: `{rows.get('prospects',0)}`",
        f"- profiles: `{rows.get('profiles',0)}`",
        f"- college_qbr: `{rows.get('college_qbr',0)}`",
        f"- college_stats: `{rows.get('college_stats',0)}`",
        f"- ids: `{rows.get('ids',0)}`",
        "",
        "## QA checks",
        "",
        f"- duplicate (draft_year, player_id): `{checks.get('player_id_duplicates_by_year',0)}`",
        f"- non-empty profile text rows: `{checks.get('profile_text_nonempty_rows',0)}`",
        f"- board rows checked: `{joins.get('board_rows',0)}`",
        f"- name+position join rate: `{joins.get('name_pos_join_rate',0.0)}`",
        f"- name-only join rate: `{joins.get('name_only_join_rate',0.0)}`",
        "",
        "## Useful fields",
        "",
    ]

    useful = report.get("useful_fields", {})
    for table, fields in useful.items():
        lines.append(f"- {table}: {', '.join(fields)}")

    lines.extend(["", "## Rejected field categories", ""])
    for item in report.get("rejected_field_categories", []):
        lines.append(f"- {item}")

    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description="Run ESPN ingest QA checks and join coverage report")
    p.add_argument("--target-year", type=int, default=2026)
    p.add_argument("--board", type=str, default=str(DEFAULT_BOARD))
    p.add_argument("--out-json", type=str, default=str(DEFAULT_OUT_JSON))
    p.add_argument("--out-md", type=str, default=str(DEFAULT_OUT_MD))
    args = p.parse_args()

    board_rows = _load_board_rows(Path(args.board))
    report = write_espn_qa_report(Path(args.out_json), board_rows=board_rows, target_year=args.target_year)
    _write_md(Path(args.out_md), report)

    print(f"QA JSON: {args.out_json}")
    print(f"QA MD: {args.out_md}")
    print(f"Status: {report.get('status','unknown')}")
    print(f"Join rate (name+position): {report.get('qa_checks',{}).get('join_coverage',{}).get('name_pos_join_rate',0.0)}")


if __name__ == "__main__":
    main()
