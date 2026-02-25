#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.mockdraftable_loader import load_mockdraftable_baselines
from src.ingest.rankings_loader import canonical_player_name, normalize_pos
from src.modeling.mockdraftable_features import compute_mockdraftable_composite

DEFAULT_COMBINE = ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv"
DEFAULT_OUT = ROOT / "data" / "processed" / "mockdraftable_features_2026.csv"


_COMBINE_TO_METRIC = {
    "height_in": "height",
    "weight_lb": "weight",
    "arm_in": "arm",
    "hand_in": "hand",
    "ten_split": "ten_split",
    "forty": "forty",
    "vertical": "vertical",
    "broad": "broad",
    "shuttle": "shuttle",
    "three_cone": "three_cone",
    "bench": "bench",
}


def _load_combine(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Build MockDraftable-based percentile features from combine results")
    p.add_argument("--combine", type=str, default=str(DEFAULT_COMBINE))
    p.add_argument("--out", type=str, default=str(DEFAULT_OUT))
    args = p.parse_args()

    baselines = load_mockdraftable_baselines()
    rows = _load_combine(Path(args.combine))

    out_rows = []
    for row in rows:
        player_name = (row.get("player_name") or "").strip()
        pos = normalize_pos((row.get("position") or "").strip())
        if not player_name or not pos:
            continue

        meas = {}
        for combine_col, metric_key in _COMBINE_TO_METRIC.items():
            meas[metric_key] = row.get(combine_col, "")

        features = compute_mockdraftable_composite(pos, meas, baselines)
        out = {
            "player_name": player_name,
            "player_key": canonical_player_name(player_name),
            "position": pos,
            **features,
        }
        out_rows.append(out)

    _write_csv(Path(args.out), out_rows)
    print(f"Rows written: {len(out_rows)}")
    print(f"Output: {args.out}")


if __name__ == "__main__":
    main()
