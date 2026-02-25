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

from src.ingest.espn_loader import build_historical_training_rows, leakage_safe_year_splits


OUT_DIR = ROOT / "data" / "processed"
REPORT_MD = ROOT / "data" / "outputs" / "espn_leakage_safe_splits.md"


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _filter_by_years(rows: list[dict], years: list[int]) -> list[dict]:
    yset = set(years)
    return [r for r in rows if int(r["draft_year"]) in yset]


def _write_report(rows: list[dict], split: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# ESPN Leakage-Safe Training Splits",
        "",
        "## Policy",
        "",
        "- Time-based split by draft year only.",
        "- Train uses earlier years than validation/test.",
        "- No post-draft outcomes are used as features.",
        "- Targets are kept as labels only: drafted_flag, draft_round, overall_pick, draft_team.",
        "",
        "## Split years",
        "",
        f"- train_years: {split.get('train_years', [])}",
        f"- valid_years: {split.get('valid_years', [])}",
        f"- test_years: {split.get('test_years', [])}",
        "",
        "## Counts",
        "",
        f"- total rows: {len(rows)}",
        f"- train rows: {len(_filter_by_years(rows, split.get('train_years', [])))}",
        f"- valid rows: {len(_filter_by_years(rows, split.get('valid_years', [])))}",
        f"- test rows: {len(_filter_by_years(rows, split.get('test_years', [])))}",
    ]
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description="Build leakage-safe ESPN historical train/valid/test splits")
    p.add_argument("--min-year", type=int, default=2016)
    p.add_argument("--max-year", type=int, default=2025)
    p.add_argument("--valid-years", type=int, default=1)
    p.add_argument("--test-years", type=int, default=1)
    args = p.parse_args()

    rows = build_historical_training_rows(min_year=args.min_year, max_year=args.max_year)
    split = leakage_safe_year_splits(rows, valid_years=args.valid_years, test_years=args.test_years)

    all_path = OUT_DIR / "espn_historical_features_2016_2025.csv"
    train_path = OUT_DIR / "espn_train_split.csv"
    valid_path = OUT_DIR / "espn_valid_split.csv"
    test_path = OUT_DIR / "espn_test_split.csv"
    meta_path = OUT_DIR / "espn_split_meta.json"

    _write_csv(all_path, rows)
    _write_csv(train_path, _filter_by_years(rows, split.get("train_years", [])))
    _write_csv(valid_path, _filter_by_years(rows, split.get("valid_years", [])))
    _write_csv(test_path, _filter_by_years(rows, split.get("test_years", [])))

    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with meta_path.open("w") as f:
        json.dump(split, f, indent=2)

    _write_report(rows, split, REPORT_MD)

    print(f"All rows: {len(rows)} -> {all_path}")
    print(f"Train years: {split.get('train_years', [])} -> {train_path}")
    print(f"Valid years: {split.get('valid_years', [])} -> {valid_path}")
    print(f"Test years: {split.get('test_years', [])} -> {test_path}")
    print(f"Meta: {meta_path}")
    print(f"Report: {REPORT_MD}")


if __name__ == "__main__":
    main()
