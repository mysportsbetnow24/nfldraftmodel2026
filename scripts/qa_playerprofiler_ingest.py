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

from src.ingest.playerprofiler_loader import load_playerprofiler_signals
from src.ingest.rankings_loader import canonical_player_name, normalize_pos


DEFAULT_BOARD = ROOT / "data" / "outputs" / "big_board_2026.csv"
DEFAULT_OUT_JSON = ROOT / "data" / "outputs" / "playerprofiler_ingest_qa_report.json"
DEFAULT_OUT_MD = ROOT / "data" / "outputs" / "playerprofiler_ingest_qa_report.md"


def _load_board(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_md(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# PlayerProfiler Ingest QA Report",
        "",
        f"- status: `{report.get('status','unknown')}`",
        f"- source path: `{report.get('source_path','')}`",
        f"- source rows: `{report.get('source_rows',0)}`",
        f"- board rows: `{report.get('board_rows',0)}`",
        f"- name+position join rate: `{report.get('join_rate_name_pos',0.0)}`",
        f"- name-only join rate: `{report.get('join_rate_name',0.0)}`",
        f"- skill-position rows in source: `{report.get('skill_source_rows',0)}`",
        f"- source rows with both breakout+dominator: `{report.get('source_rows_full_coverage',0)}`",
    ]
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description="QA PlayerProfiler manual ingest joins and coverage")
    p.add_argument("--board", type=str, default=str(DEFAULT_BOARD))
    p.add_argument("--out-json", type=str, default=str(DEFAULT_OUT_JSON))
    p.add_argument("--out-md", type=str, default=str(DEFAULT_OUT_MD))
    args = p.parse_args()

    board_rows = _load_board(Path(args.board))
    pp = load_playerprofiler_signals()
    by_name_pos = pp.get("by_name_pos", {})
    by_name = pp.get("by_name", {})

    hit_np = 0
    hit_n = 0
    for row in board_rows:
        name_key = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position") or row.get("pos_raw") or "")
        if (name_key, pos) in by_name_pos:
            hit_np += 1
        if name_key in by_name:
            hit_n += 1

    skill_rows = 0
    full_cov = 0
    for payload in by_name_pos.values():
        if payload.get("position") in {"WR", "RB", "TE"}:
            skill_rows += 1
        if str(payload.get("pp_breakout_age", "")).strip() and str(payload.get("pp_college_dominator", "")).strip():
            full_cov += 1

    report = {
        "status": pp.get("meta", {}).get("status", "unknown"),
        "source_path": pp.get("meta", {}).get("path", ""),
        "source_rows": pp.get("meta", {}).get("rows", 0),
        "board_rows": len(board_rows),
        "join_rate_name_pos": round(hit_np / len(board_rows), 4) if board_rows else 0.0,
        "join_rate_name": round(hit_n / len(board_rows), 4) if board_rows else 0.0,
        "skill_source_rows": skill_rows,
        "source_rows_full_coverage": full_cov,
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w") as f:
        json.dump(report, f, indent=2)

    _write_md(Path(args.out_md), report)

    print(f"QA JSON: {out_json}")
    print(f"QA MD: {args.out_md}")
    print(f"Status: {report['status']}")
    print(f"Join rate (name+position): {report['join_rate_name_pos']}")


if __name__ == "__main__":
    main()
