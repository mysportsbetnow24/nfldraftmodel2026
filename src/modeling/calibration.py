from __future__ import annotations

import csv
import json
import math
import random
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



def generate_seed_historical(path: Path, seed: int = 20260225) -> int:
    """Generates synthetic historical outcomes for offline calibration bootstrapping."""
    random.seed(seed)
    path.parent.mkdir(parents=True, exist_ok=True)

    positions = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]
    pos_bias = {
        "QB": 0.08,
        "OT": 0.06,
        "EDGE": 0.05,
        "CB": 0.04,
        "WR": 0.03,
        "DT": 0.02,
        "S": 0.00,
        "LB": -0.01,
        "TE": -0.01,
        "IOL": -0.02,
        "RB": -0.03,
    }

    rows: List[dict] = []
    overall = 1
    for year in range(2016, 2026):
        for rnd in range(1, 8):
            picks_in_round = 32
            for _ in range(picks_in_round):
                pos = random.choice(positions)
                model_grade = _clamp(random.gauss(88 - (rnd - 1) * 3.4, 3.5), 62, 97)
                ras = _clamp(random.gauss(7.4 - (rnd - 1) * 0.25, 1.1), 2.0, 10.0)
                pff_grade = _clamp(random.gauss(80 - (rnd - 1) * 1.8, 7.0), 55, 95)

                base = 0.58 - (rnd - 1) * 0.06 + pos_bias[pos]
                grade_term = (model_grade - 78) * 0.015
                ras_term = (ras - 7.0) * 0.03
                pff_term = (pff_grade - 75) * 0.007
                prob = _clamp(base + grade_term + ras_term + pff_term, 0.04, 0.96)

                success = 1 if random.random() < prob else 0
                second_contract = 1 if random.random() < _clamp(prob - 0.12, 0.01, 0.9) else 0
                starter_seasons = 0
                if success:
                    starter_seasons = int(_clamp(random.gauss(2.8 + prob * 1.8, 1.2), 1, 9))
                career_value = round(max(0.0, random.gauss((prob * 26) + (second_contract * 8), 6.5)), 2)

                rows.append(
                    {
                        "draft_year": year,
                        "overall_pick": overall,
                        "draft_round": rnd,
                        "position": pos,
                        "model_grade": round(model_grade, 2),
                        "ras": round(ras, 2),
                        "pff_grade": round(pff_grade, 2),
                        "career_value": career_value,
                        "starter_seasons": starter_seasons,
                        "second_contract": second_contract,
                        "success_label": success,
                        "data_source": "synthetic_seed",
                    }
                )
                overall += 1
        overall = 1

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)



def load_historical_rows(path: Path = DEFAULT_HISTORICAL_PATH, generate_seed_if_missing: bool = True) -> List[dict]:
    if not path.exists():
        if not generate_seed_if_missing:
            raise FileNotFoundError(f"Historical outcomes file not found: {path}")
        generate_seed_historical(path)

    rows: List[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rows.append(
                    {
                        "draft_year": int(row["draft_year"]),
                        "overall_pick": int(row["overall_pick"]),
                        "draft_round": int(row["draft_round"]),
                        "position": row["position"],
                        "model_grade": float(row["model_grade"]),
                        "ras": float(row.get("ras", 7.0) or 7.0),
                        "pff_grade": float(row.get("pff_grade", 70.0) or 70.0),
                        "career_value": float(row.get("career_value", 0.0) or 0.0),
                        "starter_seasons": int(float(row.get("starter_seasons", 0) or 0)),
                        "second_contract": int(float(row.get("second_contract", 0) or 0)),
                        "success_label": int(float(row["success_label"])),
                        "data_source": row.get("data_source", "manual"),
                    }
                )
            except Exception:
                continue
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
            pred = _sigmoid(intercept + slope * x)
            err = pred - y
            grad_b += err
            grad_w += err * x

        n = max(1, len(rows))
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

        obs = sum(float(g["success_label"]) for g in group) / len(group)
        exp = sum(_sigmoid(intercept + slope * float(g["model_grade"])) for g in group) / len(group)
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
                "hit_rate": round(sum(g["success_label"] for g in grp) / len(grp), 4),
                "avg_career_value": round(sum(g["career_value"] for g in grp) / len(grp), 2),
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
