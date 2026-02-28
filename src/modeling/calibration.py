from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HISTORICAL_PATH = ROOT / "data" / "sources" / "manual" / "historical_draft_outcomes_2016_2025.csv"
DEFAULT_CALIBRATION_PATH = ROOT / "data" / "processed" / "historical_calibration_2016_2025.json"


@dataclass
class CalibrationConfig:
    intercept: float
    slope: float
    position_additive: Dict[str, float]
    sample_size: int
    data_source: str



def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))



def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))



def load_historical_rows(
    path: Path = DEFAULT_HISTORICAL_PATH,
    min_year: int = 2016,
    max_year: int = 2025,
) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(
            f"Historical outcomes file not found: {path}. "
            "Provide real outcomes data for 2016-2025; synthetic fallback is disabled."
        )

    rows: List[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                draft_year = int(row["draft_year"])
                if draft_year < min_year or draft_year > max_year:
                    continue
                data_source = str(row.get("data_source", "manual")).strip() or "manual"
                if "synthetic" in data_source.lower():
                    continue
                rows.append(
                    {
                        "draft_year": draft_year,
                        "overall_pick": int(row["overall_pick"]),
                        "draft_round": int(row["draft_round"]),
                        "position": str(row["position"]).strip().upper(),
                        "model_grade": float(row["model_grade"]),
                        "ras": float(row.get("ras", 7.0) or 7.0),
                        "pff_grade": float(row.get("pff_grade", 70.0) or 70.0),
                        "career_value": float(row.get("career_value", 0.0) or 0.0),
                        "starter_seasons": int(float(row.get("starter_seasons", 0) or 0)),
                        "second_contract": int(float(row.get("second_contract", 0) or 0)),
                        "success_label": int(float(row["success_label"])),
                        "sample_weight": max(0.01, float(row.get("sample_weight", 1.0) or 1.0)),
                        "data_source": data_source,
                    }
                )
            except Exception:
                continue
    if not rows:
        raise RuntimeError(
            "No valid real historical rows found in outcomes file. "
            "Check schema/years and ensure data_source is not synthetic."
        )
    return rows



def fit_logistic_grade(rows: List[dict], iterations: int = 1500, lr: float = 0.0008) -> Tuple[float, float]:
    intercept = -9.0
    slope = 0.11

    for _ in range(iterations):
        grad_b = 0.0
        grad_w = 0.0
        for r in rows:
            x = float(r["model_grade"])
            y = float(r["success_label"])
            w = float(r.get("sample_weight", 1.0) or 1.0)
            pred = _sigmoid(intercept + slope * x)
            err = pred - y
            grad_b += err * w
            grad_w += err * x * w

        n = max(1.0, sum(float(r.get("sample_weight", 1.0) or 1.0) for r in rows))
        intercept -= lr * (grad_b / n)
        slope -= lr * (grad_w / n)

    return intercept, slope



def position_additives(rows: List[dict], intercept: float, slope: float) -> Dict[str, float]:
    by_pos = defaultdict(list)
    for r in rows:
        by_pos[r["position"]].append(r)

    out: Dict[str, float] = {}
    for pos, group in by_pos.items():
        if len(group) < 30:
            out[pos] = 0.0
            continue

        den = sum(float(g.get("sample_weight", 1.0) or 1.0) for g in group)
        if den <= 0:
            out[pos] = 0.0
            continue
        obs = sum(float(g["success_label"]) * float(g.get("sample_weight", 1.0) or 1.0) for g in group) / den
        exp = (
            sum(
                _sigmoid(intercept + slope * float(g["model_grade"])) * float(g.get("sample_weight", 1.0) or 1.0)
                for g in group
            )
            / den
        )
        out[pos] = round(_clamp(obs - exp, -0.12, 0.12), 4)

    return out



def calibration_bins(rows: List[dict], bins: int = 12) -> List[dict]:
    ordered = sorted(rows, key=lambda r: r["model_grade"], reverse=True)
    n = len(ordered)
    if n == 0:
        return []

    chunk = max(1, n // bins)
    out = []
    for i in range(0, n, chunk):
        grp = ordered[i : i + chunk]
        if not grp:
            continue
        out.append(
            {
                "bin": len(out) + 1,
                "grade_min": round(min(g["model_grade"] for g in grp), 2),
                "grade_max": round(max(g["model_grade"] for g in grp), 2),
                "sample_size": len(grp),
                "hit_rate": round(
                    sum(g["success_label"] * float(g.get("sample_weight", 1.0) or 1.0) for g in grp)
                    / max(1.0, sum(float(g.get("sample_weight", 1.0) or 1.0) for g in grp)),
                    4,
                ),
                "avg_career_value": round(
                    sum(float(g["career_value"]) * float(g.get("sample_weight", 1.0) or 1.0) for g in grp)
                    / max(1.0, sum(float(g.get("sample_weight", 1.0) or 1.0) for g in grp)),
                    2,
                ),
            }
        )
    return out



def build_config(rows: List[dict]) -> CalibrationConfig:
    b0, b1 = fit_logistic_grade(rows)
    pos_adj = position_additives(rows, b0, b1)
    source = rows[0].get("data_source", "manual") if rows else "manual"
    return CalibrationConfig(
        intercept=round(b0, 6),
        slope=round(b1, 6),
        position_additive=pos_adj,
        sample_size=len(rows),
        data_source=source,
    )


def year_based_backtest(rows: List[dict], min_train_rows: int = 250) -> List[dict]:
    years = sorted({int(r["draft_year"]) for r in rows})
    report_rows: List[dict] = []
    for holdout_year in years:
        train = [r for r in rows if int(r["draft_year"]) < holdout_year]
        test = [r for r in rows if int(r["draft_year"]) == holdout_year]
        if len(train) < min_train_rows or not test:
            continue

        b0, b1 = fit_logistic_grade(train)
        pos_adj = position_additives(train, b0, b1)

        probs: List[float] = []
        labels: List[float] = []
        weights: List[float] = []
        for r in test:
            base = _sigmoid(b0 + b1 * float(r["model_grade"]))
            p = _clamp(base + float(pos_adj.get(r["position"], 0.0)), 0.02, 0.98)
            probs.append(p)
            labels.append(float(r["success_label"]))
            weights.append(float(r.get("sample_weight", 1.0) or 1.0))

        wden = max(1.0, sum(weights))
        brier = sum(((p - y) ** 2) * w for p, y, w in zip(probs, labels, weights)) / wden
        accuracy = (
            sum(((1 if p >= 0.5 else 0) == int(y)) * w for p, y, w in zip(probs, labels, weights)) / wden
        )
        avg_prob = sum(p * w for p, w in zip(probs, weights)) / wden
        obs_rate = sum(y * w for y, w in zip(labels, weights)) / wden

        report_rows.append(
            {
                "holdout_year": holdout_year,
                "train_rows": len(train),
                "test_rows": len(test),
                "brier_score": round(brier, 4),
                "accuracy": round(accuracy, 4),
                "avg_predicted_success": round(avg_prob, 4),
                "observed_success_rate": round(obs_rate, 4),
            }
        )
    return report_rows



def save_calibration_outputs(rows: List[dict], config: CalibrationConfig, output_path: Path = DEFAULT_CALIBRATION_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bins = calibration_bins(rows)

    payload = {
        "intercept": config.intercept,
        "slope": config.slope,
        "position_additive": config.position_additive,
        "sample_size": config.sample_size,
        "data_source": config.data_source,
        "grade_bins": bins,
    }
    with output_path.open("w") as f:
        json.dump(payload, f, indent=2)

    # Additional CSV exports for inspection
    bins_path = output_path.with_name("historical_calibration_bins_2016_2025.csv")
    if bins:
        with bins_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(bins[0].keys()))
            writer.writeheader()
            writer.writerows(bins)



def load_calibration_config(path: Path = DEFAULT_CALIBRATION_PATH) -> CalibrationConfig | None:
    if not path.exists():
        return None
    with path.open() as f:
        payload = json.load(f)

    return CalibrationConfig(
        intercept=float(payload.get("intercept", -9.0)),
        slope=float(payload.get("slope", 0.11)),
        position_additive={k: float(v) for k, v in payload.get("position_additive", {}).items()},
        sample_size=int(payload.get("sample_size", 0)),
        data_source=payload.get("data_source", "manual"),
    )



def calibrated_success_probability(
    grade: float,
    position: str,
    config: CalibrationConfig | None,
    ras_estimate: float | None = None,
    pff_grade: float | None = None,
) -> float:
    if config is None:
        base = _sigmoid(-9.0 + 0.11 * grade)
        return round(base, 4)

    base = _sigmoid(config.intercept + config.slope * grade)
    pos_delta = config.position_additive.get(position, 0.0)

    ras_delta = 0.0
    if ras_estimate is not None:
        ras_delta = _clamp((ras_estimate - 7.0) * 0.015, -0.04, 0.05)

    pff_delta = 0.0
    if pff_grade is not None:
        pff_delta = _clamp((pff_grade - 75.0) * 0.003, -0.05, 0.06)

    prob = _clamp(base + pos_delta + ras_delta + pff_delta, 0.02, 0.98)
    return round(prob, 4)
