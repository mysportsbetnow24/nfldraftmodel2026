#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.eligibility_loader import load_returning_to_school
from src.ingest.eligibility_loader import load_already_in_nfl_exclusions
from src.ingest.rankings_loader import canonical_player_name, normalize_pos


DEFAULT_SEED = ROOT / "data" / "processed" / "prospect_seed_2026.csv"
DEFAULT_REPORT = ROOT / "data" / "outputs" / "seed_cleanup_report_2026.json"


def _rank_seed(row: dict) -> int:
    try:
        return int(row.get("rank_seed", 999999))
    except (TypeError, ValueError):
        return 999999


def _seed_row_id(row: dict) -> int:
    try:
        return int(row.get("seed_row_id", 999999))
    except (TypeError, ValueError):
        return 999999


def main() -> None:
    p = argparse.ArgumentParser(description="Auto-dedupe seed by best rank and remove returning players.")
    p.add_argument("--seed", type=str, default=str(DEFAULT_SEED))
    p.add_argument("--out", type=str, default=None, help="Output path (default in-place overwrite).")
    p.add_argument("--report", type=str, default=str(DEFAULT_REPORT))
    args = p.parse_args()

    seed_path = Path(args.seed)
    out_path = Path(args.out) if args.out else seed_path
    report_path = Path(args.report)

    if not seed_path.exists():
        raise SystemExit(f"Seed file not found: {seed_path}")

    with seed_path.open() as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    returning_names = load_returning_to_school()
    already_drafted_names = load_already_in_nfl_exclusions()
    before_rows = len(rows)

    removed_returning = []
    removed_already_drafted = []
    kept_rows = []
    for row in rows:
        key = canonical_player_name(row.get("player_name", ""))
        if key in returning_names:
            removed_returning.append(
                {
                    "player_name": row.get("player_name", ""),
                    "school": row.get("school", ""),
                    "class_year": row.get("class_year", ""),
                    "rank_seed": row.get("rank_seed", ""),
                    "seed_row_id": row.get("seed_row_id", ""),
                }
            )
            continue
        if key in already_drafted_names:
            removed_already_drafted.append(
                {
                    "player_name": row.get("player_name", ""),
                    "school": row.get("school", ""),
                    "class_year": row.get("class_year", ""),
                    "rank_seed": row.get("rank_seed", ""),
                    "seed_row_id": row.get("seed_row_id", ""),
                }
            )
            continue
        kept_rows.append(row)

    best: dict[tuple[str, str], dict] = {}
    duplicate_drops = []
    for row in kept_rows:
        name = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("pos_raw", ""))
        key = (name, pos)
        cur = best.get(key)
        if cur is None:
            best[key] = row
            continue

        row_key = (_rank_seed(row), _seed_row_id(row))
        cur_key = (_rank_seed(cur), _seed_row_id(cur))
        if row_key < cur_key:
            duplicate_drops.append(
                {
                    "player_name": cur.get("player_name", ""),
                    "pos_raw": cur.get("pos_raw", ""),
                    "dropped_rank_seed": cur.get("rank_seed", ""),
                    "dropped_seed_row_id": cur.get("seed_row_id", ""),
                    "kept_rank_seed": row.get("rank_seed", ""),
                    "kept_seed_row_id": row.get("seed_row_id", ""),
                }
            )
            best[key] = row
        else:
            duplicate_drops.append(
                {
                    "player_name": row.get("player_name", ""),
                    "pos_raw": row.get("pos_raw", ""),
                    "dropped_rank_seed": row.get("rank_seed", ""),
                    "dropped_seed_row_id": row.get("seed_row_id", ""),
                    "kept_rank_seed": cur.get("rank_seed", ""),
                    "kept_seed_row_id": cur.get("seed_row_id", ""),
                }
            )

    deduped_rows = list(best.values())
    deduped_rows.sort(key=lambda r: (_rank_seed(r), _seed_row_id(r)))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped_rows)

    report = {
        "cleaned_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "seed_path": str(seed_path),
        "output_path": str(out_path),
        "counts": {
            "before_rows": before_rows,
            "after_rows": len(deduped_rows),
            "removed_returning_rows": len(removed_returning),
            "removed_already_drafted_rows": len(removed_already_drafted),
            "removed_duplicate_rows": len(duplicate_drops),
        },
        "removed_returning_players": removed_returning,
        "removed_already_drafted_players": removed_already_drafted,
        "removed_duplicate_rows": duplicate_drops[:200],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))

    print(f"Seed rows before: {before_rows}")
    print(f"Removed returning rows: {len(removed_returning)}")
    print(f"Removed already-drafted rows: {len(removed_already_drafted)}")
    print(f"Removed duplicate rows: {len(duplicate_drops)}")
    print(f"Seed rows after: {len(deduped_rows)}")
    print(f"Wrote cleaned seed: {out_path}")
    print(f"Wrote cleanup report: {report_path}")


if __name__ == "__main__":
    main()
