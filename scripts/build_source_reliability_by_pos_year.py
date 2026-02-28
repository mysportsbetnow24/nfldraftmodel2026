#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import normalize_pos  # noqa: E402


BASE_SOURCE_PATH = ROOT / "data" / "sources" / "manual" / "source_reliability_weights_2026.csv"
HISTORICAL_PATH = ROOT / "data" / "sources" / "manual" / "historical_draft_outcomes_2016_2025.csv"
OUT_PATH = ROOT / "data" / "sources" / "manual" / "source_reliability_by_pos_year_2016_2025.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "source_reliability_by_pos_year_report_2026-02-28.md"


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _read_csv(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def _weighted_success(rows: list[dict]) -> tuple[float, float]:
    num = 0.0
    den = 0.0
    for row in rows:
        y = _to_float(row.get("success_label"))
        if y is None:
            continue
        w = _to_float(row.get("sample_weight"))
        if w is None:
            w = 1.0
        num += float(y) * float(w)
        den += float(w)
    return num, den


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build source reliability table by position and draft year from historical outcomes."
    )
    p.add_argument("--base", type=Path, default=BASE_SOURCE_PATH)
    p.add_argument("--historical", type=Path, default=HISTORICAL_PATH)
    p.add_argument("--out", type=Path, default=OUT_PATH)
    p.add_argument("--report", type=Path, default=REPORT_PATH)
    p.add_argument("--min-year", type=int, default=2016)
    p.add_argument("--max-year", type=int, default=2025)
    args = p.parse_args()

    if not args.base.exists():
        raise FileNotFoundError(f"Missing base source reliability file: {args.base}")
    if not args.historical.exists():
        raise FileNotFoundError(f"Missing historical outcomes file: {args.historical}")

    base_rows = _read_csv(args.base)
    hist_rows = _read_csv(args.historical)
    hist_rows = [
        r
        for r in hist_rows
        if args.min_year <= int(_to_float(r.get("draft_year")) or 0) <= args.max_year
    ]

    overall_num, overall_den = _weighted_success(hist_rows)
    overall_sr = (overall_num / overall_den) if overall_den > 0 else 0.5

    grouped: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for row in hist_rows:
        yr = int(_to_float(row.get("draft_year")) or 0)
        pos = normalize_pos(str(row.get("position", "")).strip())
        if not pos:
            continue
        grouped[(yr, pos)].append(row)

    # Build historical position-year quality modifiers.
    pos_year_quality: dict[tuple[int, str], dict] = {}
    for (yr, pos), rows in grouped.items():
        num, den = _weighted_success(rows)
        if den <= 0:
            continue
        sr = num / den
        n = len(rows)
        # Shrink noisy cells toward the global historical rate.
        shrink = _clamp(float(n) / float(n + 40), 0.10, 0.85)
        sr_shrunk = ((1.0 - shrink) * overall_sr) + (shrink * sr)
        # Convert to a bounded multiplier for source hit-rate adjustment.
        quality_mult = _clamp(sr_shrunk / max(0.01, overall_sr), 0.82, 1.18)
        pos_year_quality[(yr, pos)] = {
            "hist_success_rate_raw": round(sr, 4),
            "hist_success_rate_shrunk": round(sr_shrunk, 4),
            "hist_quality_multiplier": round(quality_mult, 4),
            "hist_row_count": n,
            "hist_weighted_den": round(den, 2),
            "hist_shrink": round(shrink, 4),
            "imputed": 0,
        }

    # Fill missing years in the requested window by nearest available year per position.
    all_positions = sorted({pos for (_yr, pos) in pos_year_quality.keys()})
    for pos in all_positions:
        available = sorted([yr for (yr, p) in pos_year_quality.keys() if p == pos])
        if not available:
            continue
        for yr in range(args.min_year, args.max_year + 1):
            key = (yr, pos)
            if key in pos_year_quality:
                continue
            nearest_year = min(available, key=lambda y: abs(y - yr))
            src = dict(pos_year_quality[(nearest_year, pos)])
            src["hist_row_count"] = max(10, int(src.get("hist_row_count", 0)))
            src["imputed"] = 1
            src["imputed_from_year"] = nearest_year
            pos_year_quality[key] = src

    out_rows: list[dict] = []
    for base in base_rows:
        source = str(base.get("source", "")).strip()
        if not source:
            continue
        base_hit = _to_float(base.get("hit_rate"))
        base_stability = _to_float(base.get("stability"))
        base_sample = _to_float(base.get("sample_size"))
        if base_hit is None or base_stability is None:
            continue
        base_hit = _clamp(float(base_hit), 0.35, 0.85)
        base_stability = _clamp(float(base_stability), 0.30, 0.95)
        base_sample = max(1, int(base_sample or 300))

        for (yr, pos), quality in pos_year_quality.items():
            q = float(quality["hist_quality_multiplier"])
            n = int(quality["hist_row_count"])
            adj_hit = _clamp(base_hit * q, 0.35, 0.85)
            # Stability gets a small lift when cell sample is decent.
            cell_support = _clamp(float(n) / 75.0, 0.0, 1.0)
            adj_stability = _clamp(base_stability * (0.93 + (0.11 * cell_support)), 0.30, 0.95)
            adj_sample = max(20, min(base_sample, n))

            out_rows.append(
                {
                    "source": source,
                    "position": pos,
                    "draft_year": yr,
                    "hit_rate": round(adj_hit, 4),
                    "stability": round(adj_stability, 4),
                    "sample_size": int(adj_sample),
                    "hist_success_rate_raw": quality["hist_success_rate_raw"],
                    "hist_success_rate_shrunk": quality["hist_success_rate_shrunk"],
                    "hist_quality_multiplier": quality["hist_quality_multiplier"],
                    "hist_row_count": quality["hist_row_count"],
                    "imputed": int(quality.get("imputed", 0)),
                    "imputed_from_year": quality.get("imputed_from_year", ""),
                    "method": "base_source_weight_x_historical_pos_year_success",
                }
            )

    out_rows.sort(key=lambda r: (r["source"], int(r["draft_year"]), r["position"]))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source",
                "position",
                "draft_year",
                "hit_rate",
                "stability",
                "sample_size",
                "hist_success_rate_raw",
                "hist_success_rate_shrunk",
                "hist_quality_multiplier",
                "hist_row_count",
                "imputed",
                "imputed_from_year",
                "method",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    pos_set = sorted({r["position"] for r in out_rows})
    report_lines = [
        "# Source Reliability By Position/Year Report",
        "",
        f"- Base source table: `{args.base}`",
        f"- Historical outcomes: `{args.historical}`",
        f"- Output table: `{args.out}`",
        f"- Year window: **{args.min_year}-{args.max_year}**",
        f"- Base sources: **{len(base_rows)}**",
        f"- Position-year cells: **{len(pos_year_quality)}**",
        f"- Rows written: **{len(out_rows)}**",
        f"- Overall weighted success rate: **{overall_sr:.4f}**",
        "",
        "## Positions Covered",
        "",
        ", ".join(pos_set) if pos_set else "_none_",
        "",
        "## Notes",
        "",
        "- This table is used as a refinement layer over base source reliability.",
        "- Runtime resolver applies recency decay and shrinkage back to global source defaults.",
        "- If this file is missing, the model falls back to base source-only weights.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(report_lines))

    print(f"Wrote: {args.out}")
    print(f"Rows: {len(out_rows)}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
