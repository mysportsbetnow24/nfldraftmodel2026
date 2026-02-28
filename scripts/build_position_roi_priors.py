#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import normalize_pos


IN_PATH = ROOT / "data" / "processed" / "historical_labels_leagify_2015_2023.csv"
OUT_PATH = ROOT / "data" / "processed" / "position_roi_priors_leagify_2016_2023.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "position_roi_priors_report_2026-02-28.md"


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _to_int(value) -> int | None:
    f = _to_float(value)
    if f is None:
        return None
    return int(round(f))


def _pick_band(draft_round: int) -> str:
    if draft_round <= 1:
        return "R1"
    if draft_round <= 2:
        return "R2"
    if draft_round <= 3:
        return "R3"
    if draft_round <= 4:
        return "R4"
    return "R5+"


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
    parser = argparse.ArgumentParser(description="Build positional ROI priors from historical Leagify surplus labels.")
    parser.add_argument("--input", type=Path, default=IN_PATH)
    parser.add_argument("--output", type=Path, default=OUT_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--min-year", type=int, default=2016)
    parser.add_argument("--max-year", type=int, default=2023)
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Missing input: {args.input}")

    with args.input.open() as f:
        rows = list(csv.DictReader(f))

    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        year = _to_int(row.get("draft_year"))
        if year is None or year < args.min_year or year > args.max_year:
            continue
        rnd = _to_int(row.get("draft_round"))
        if rnd is None:
            continue
        pos = normalize_pos(row.get("position", ""))
        if not pos:
            continue
        key = (pos, _pick_band(rnd))
        grouped[key].append(row)

    # Weighted means by (position, band)
    agg_rows: list[dict] = []
    by_band_mean_surplus: dict[str, list[float]] = defaultdict(list)
    for (pos, band), grp in grouped.items():
        weighted_surplus_num = 0.0
        weighted_success_num = 0.0
        weighted_den = 0.0
        for row in grp:
            weight = float(_to_float(row.get("censor_weight")) or 1.0)
            surplus = float(_to_float(row.get("surplus_value")) or 0.0)
            success = float(_to_float(row.get("success_label_3yr")) or 0.0)
            weighted_surplus_num += weight * surplus
            weighted_success_num += weight * success
            weighted_den += weight
        if weighted_den <= 0:
            continue
        weighted_mean_surplus = weighted_surplus_num / weighted_den
        weighted_success_rate = weighted_success_num / weighted_den
        by_band_mean_surplus[band].append(weighted_mean_surplus)
        agg_rows.append(
            {
                "position": pos,
                "pick_band": band,
                "sample_n": len(grp),
                "weighted_n": round(weighted_den, 2),
                "weighted_mean_surplus": round(weighted_mean_surplus, 4),
                "weighted_success_rate": round(weighted_success_rate, 4),
            }
        )

    # Band z-score + capped grade adjustment.
    for row in agg_rows:
        band = row["pick_band"]
        vals = by_band_mean_surplus.get(band, [])
        mean = sum(vals) / len(vals) if vals else 0.0
        if vals:
            var = sum((v - mean) ** 2 for v in vals) / len(vals)
            std = math.sqrt(var)
        else:
            std = 0.0
        surplus = float(row["weighted_mean_surplus"])
        if std <= 1e-9:
            z = 0.0
        else:
            z = (surplus - mean) / std

        # Hard cap keeps ROI prior small so it refines, not rewrites.
        adjustment = max(-0.60, min(0.60, 0.25 * z))
        row["surplus_z"] = round(z, 4)
        row["roi_grade_adjustment"] = round(adjustment, 4)

    agg_rows.sort(key=lambda r: (r["pick_band"], r["position"]))
    _write_csv(args.output, agg_rows)

    lines = [
        "# Position ROI Priors Report",
        "",
        f"- Input: `{args.input}`",
        f"- Output: `{args.output}`",
        f"- Year window: **{args.min_year}-{args.max_year}**",
        f"- Rows written: **{len(agg_rows)}**",
        "",
        "## Adjustment Rule",
        "",
        "- `roi_grade_adjustment = clamp(0.25 * surplus_z, -0.60, +0.60)`",
        "- Used as a small prior only; cannot override core grading.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Rows: {len(agg_rows)}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
