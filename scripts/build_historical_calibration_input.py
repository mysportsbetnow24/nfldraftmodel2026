#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


LABELS_PATH = ROOT / "data" / "processed" / "historical_labels_leagify_2015_2023.csv"
SNAPSHOTS_PATH = ROOT / "data" / "sources" / "manual" / "historical_model_grade_snapshots_2016_2025.csv"
OUT_PATH = ROOT / "data" / "sources" / "manual" / "historical_draft_outcomes_2016_2025.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "historical_calibration_input_build_report_2026-02-28.md"


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


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _read_rows(path: Path) -> list[dict]:
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


def _snapshot_map(path: Path) -> dict[tuple[int, str, str], float]:
    if not path.exists():
        return {}

    out: dict[tuple[int, str, str], float] = {}
    for row in _read_rows(path):
        year = _to_int(row.get("draft_year") or row.get("year"))
        name = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position", ""))
        model_grade = _to_float(row.get("model_grade"))
        if year is None or not name or not pos or model_grade is None:
            continue
        out[(year, name, pos)] = float(model_grade)
    return out


def _range(rows: list[dict], key: str) -> tuple[float, float]:
    vals = []
    for row in rows:
        v = _to_float(row.get(key))
        if v is not None:
            vals.append(float(v))
    if not vals:
        return 0.0, 1.0
    return min(vals), max(vals)


def _blend_draft_value(row: dict) -> float | None:
    otc = _to_float(row.get("otc_value"))
    johnson = _to_float(row.get("johnson_value"))
    hill = _to_float(row.get("hill_value"))
    pff = _to_float(row.get("pff_value"))

    parts: list[tuple[float, float]] = []
    if otc is not None:
        parts.append((0.50, otc))
    if johnson is not None:
        parts.append((0.25, johnson))
    if hill is not None:
        parts.append((0.15, hill))
    if pff is not None:
        parts.append((0.10, pff))
    if not parts:
        return None
    num = sum(w * v for w, v in parts)
    den = sum(w for w, _ in parts)
    return num / den if den > 0 else None


def _proxy_model_grade(
    row: dict,
    pred_min: float,
    pred_max: float,
    value_log_min: float,
    value_log_max: float,
) -> float:
    pick = int(_to_float(row.get("overall_pick")) or 262)
    pick_grade = _clamp(95.0 - (0.115 * float(pick - 1)), 55.0, 95.0)

    pred_av = _to_float(row.get("predicted_av"))
    pred_grade = None
    if pred_av is not None and pred_max > pred_min:
        pred_pct = (float(pred_av) - pred_min) / (pred_max - pred_min)
        pred_grade = 58.0 + (36.0 * _clamp(pred_pct, 0.0, 1.0))

    blended_value = _blend_draft_value(row)
    value_grade = None
    if blended_value is not None and value_log_max > value_log_min:
        value_log = math.log1p(max(0.0, float(blended_value)))
        value_pct = (value_log - value_log_min) / (value_log_max - value_log_min)
        value_grade = 55.0 + (40.0 * _clamp(value_pct, 0.0, 1.0))

    parts = [(0.55, pick_grade)]
    if pred_grade is not None:
        parts.append((0.30, pred_grade))
    if value_grade is not None:
        parts.append((0.15, value_grade))

    num = sum(w * v for w, v in parts)
    den = sum(w for w, _ in parts)
    return round(_clamp(num / den, 55.0, 95.0), 2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build historical calibration input from Leagify labels.")
    parser.add_argument("--labels", type=Path, default=LABELS_PATH)
    parser.add_argument("--snapshots", type=Path, default=SNAPSHOTS_PATH)
    parser.add_argument("--output", type=Path, default=OUT_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--min-year", type=int, default=2016)
    parser.add_argument("--max-year", type=int, default=2025)
    args = parser.parse_args()

    if not args.labels.exists():
        raise FileNotFoundError(f"Missing labels input: {args.labels}")

    labels = _read_rows(args.labels)
    labels = [
        r
        for r in labels
        if (args.min_year <= int(_to_float(r.get("draft_year")) or 0) <= args.max_year)
    ]
    labels.sort(key=lambda r: (int(_to_float(r.get("draft_year")) or 0), int(_to_float(r.get("overall_pick")) or 0)))

    pred_min, pred_max = _range(labels, "predicted_av")
    value_logs = []
    for row in labels:
        v = _blend_draft_value(row)
        if v is not None and v > 0:
            value_logs.append(math.log1p(v))
    if value_logs:
        value_log_min, value_log_max = min(value_logs), max(value_logs)
    else:
        value_log_min, value_log_max = 0.0, 1.0

    snapshots = _snapshot_map(args.snapshots)
    used_snapshot = 0
    used_proxy = 0

    out_rows: list[dict] = []
    for row in labels:
        draft_year = int(_to_float(row.get("draft_year")) or 0)
        pick = int(_to_float(row.get("overall_pick")) or 0)
        rnd = int(_to_float(row.get("draft_round")) or 0)
        player_name = canonical_player_name(row.get("player_name", ""))
        position = normalize_pos(row.get("position", ""))

        key = (draft_year, player_name, position)
        snapshot_grade = snapshots.get(key)
        if snapshot_grade is not None:
            model_grade = round(_clamp(float(snapshot_grade), 55.0, 95.0), 2)
            used_snapshot += 1
        else:
            model_grade = _proxy_model_grade(
                row=row,
                pred_min=pred_min,
                pred_max=pred_max,
                value_log_min=value_log_min,
                value_log_max=value_log_max,
            )
            used_proxy += 1

        wav = float(_to_float(row.get("wav")) or 0.0)
        starter_seasons = int(_to_float(row.get("starter_seasons_proxy")) or 0)
        second_contract = int(_to_float(row.get("second_contract_proxy")) or 0)
        success_label = int(_to_float(row.get("success_label_3yr")) or 0)
        sample_weight = float(_to_float(row.get("censor_weight")) or 1.0)

        out_rows.append(
            {
                "draft_year": draft_year,
                "overall_pick": pick,
                "draft_round": rnd,
                "position": position,
                "model_grade": model_grade,
                "ras": 7.0,
                "pff_grade": 70.0,
                "career_value": round(wav, 2),
                "starter_seasons": starter_seasons,
                "second_contract": second_contract,
                "success_label": success_label,
                "sample_weight": round(max(0.01, sample_weight), 2),
                "data_source": "leagify_2015_2023",
            }
        )

    _write_csv(args.output, out_rows)

    report_lines = [
        "# Historical Calibration Input Build Report",
        "",
        f"- Labels input: `{args.labels}`",
        f"- Optional snapshot input: `{args.snapshots}`",
        f"- Output: `{args.output}`",
        f"- Year window: **{args.min_year}-{args.max_year}**",
        f"- Rows written: **{len(out_rows)}**",
        f"- Rows using snapshot model_grade: **{used_snapshot}**",
        f"- Rows using proxy model_grade: **{used_proxy}**",
        "",
        "## Notes",
        "",
        "- `model_grade` uses snapshot values when provided; otherwise uses a proxy from pick, predicted AV, and blended draft value.",
        "- `ras` and `pff_grade` are neutral placeholders for historical calibration context.",
        "- `career_value`, `starter_seasons`, `second_contract`, and `success_label` come from Leagify outcome labels.",
    ]

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(report_lines))

    print(f"Wrote: {args.output}")
    print(f"Rows: {len(out_rows)}")
    print(f"Snapshot grades used: {used_snapshot}")
    print(f"Proxy grades used: {used_proxy}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
