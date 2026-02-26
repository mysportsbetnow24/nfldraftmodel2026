from __future__ import annotations

import math
from typing import Dict, Mapping, Optional

from src.modeling.film_traits import score_film_traits
from src.schemas import round_from_grade


POSITION_WEIGHTS: Dict[str, Dict[str, float]] = {
    "QB": {"trait": 0.40, "production": 0.25, "athletic": 0.15, "size": 0.10, "context": 0.10},
    "RB": {"trait": 0.33, "production": 0.30, "athletic": 0.20, "size": 0.07, "context": 0.10},
    "WR": {"trait": 0.35, "production": 0.30, "athletic": 0.20, "size": 0.05, "context": 0.10},
    "TE": {"trait": 0.35, "production": 0.25, "athletic": 0.20, "size": 0.10, "context": 0.10},
    "OT": {"trait": 0.40, "production": 0.20, "athletic": 0.15, "size": 0.15, "context": 0.10},
    "IOL": {"trait": 0.40, "production": 0.20, "athletic": 0.12, "size": 0.18, "context": 0.10},
    "EDGE": {"trait": 0.36, "production": 0.28, "athletic": 0.20, "size": 0.08, "context": 0.08},
    "DT": {"trait": 0.36, "production": 0.26, "athletic": 0.15, "size": 0.15, "context": 0.08},
    "LB": {"trait": 0.35, "production": 0.25, "athletic": 0.20, "size": 0.10, "context": 0.10},
    "CB": {"trait": 0.38, "production": 0.25, "athletic": 0.22, "size": 0.05, "context": 0.10},
    "S": {"trait": 0.35, "production": 0.25, "athletic": 0.20, "size": 0.08, "context": 0.12},
}

ATHLETIC_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "QB": {"min_height_in": 74, "min_weight_lb": 215},
    "RB": {"min_height_in": 69, "min_weight_lb": 200},
    "WR": {"min_height_in": 70, "min_weight_lb": 185},
    "TE": {"min_height_in": 76, "min_weight_lb": 245},
    "OT": {"min_height_in": 77, "min_weight_lb": 310},
    "IOL": {"min_height_in": 74, "min_weight_lb": 300},
    "EDGE": {"min_height_in": 75, "min_weight_lb": 250},
    "DT": {"min_height_in": 74, "min_weight_lb": 290},
    "LB": {"min_height_in": 73, "min_weight_lb": 225},
    "CB": {"min_height_in": 70, "min_weight_lb": 185},
    "S": {"min_height_in": 71, "min_weight_lb": 200},
}


CORE_STAT_BY_POS = {
    "QB": "Decision Velocity Index",
    "RB": "Contact Balance Density",
    "WR": "Separation Threat Index",
    "TE": "Mismatch Conversion Rate",
    "OT": "Pass-Set Stability Score",
    "IOL": "Pocket Integrity Rate",
    "EDGE": "Disruption Conversion Score",
    "DT": "Interior Shock Index",
    "LB": "Trigger Range Index",
    "CB": "Coverage Disruption Rate",
    "S": "Conflict Resolver Index",
}


SCHEME_FIT_BY_POS = {
    "QB": "spread_timing",
    "RB": "wide_zone",
    "WR": "spread_vertical",
    "TE": "multiple_attach_detach",
    "OT": "zone_power_hybrid",
    "IOL": "inside_zone_gap",
    "EDGE": "multiple_front_pass_rush",
    "DT": "one_gap_attack",
    "LB": "match_zone_blitz",
    "CB": "press_match",
    "S": "split_safety_multiplicity",
}


ROLE_BY_POS = {
    "QB": "Franchise or high-end distributor",
    "RB": "Primary committee back with passing-down utility",
    "WR": "Alignment-flexible target earner",
    "TE": "In-line + move mismatch",
    "OT": "Starting tackle with pass-pro floor",
    "IOL": "Starter with interior pocket-control value",
    "EDGE": "Three-down pressure creator",
    "DT": "Early-down anchor with interior rush upside",
    "LB": "Run-and-hit communicator",
    "CB": "Outside starter with matchup flexibility",
    "S": "Coverage-adjustment back-end starter",
}


def _size_score(position: str, height_in: int, weight_lb: int) -> float:
    """
    Position-based frame scoring.
    - Penalize below minimum thresholds aggressively.
    - Allow a reasonable above-threshold band with only mild penalty.
    """
    thresholds = ATHLETIC_THRESHOLDS.get(position, {})
    min_h = int(thresholds.get("min_height_in", 72))
    min_w = int(thresholds.get("min_weight_lb", 200))

    # Position-specific tolerance above minimum before mild penalties.
    h_upper_soft = min_h + 4
    w_upper_soft = min_w + 35

    h_below = max(0, min_h - height_in)
    h_above = max(0, height_in - h_upper_soft)
    w_below = max(0, min_w - weight_lb)
    w_above = max(0, weight_lb - w_upper_soft)

    h_pen = (1.5 * h_below) + (0.35 * h_above)
    w_pen = (0.12 * w_below) + (0.02 * w_above)
    score = 90.0 - h_pen - w_pen
    return max(62.0, min(95.0, score))


def _athletic_proxy(position: str, rank_seed: int, height_in: int, weight_lb: int) -> float:
    base = 90.0 - (rank_seed * 0.09)
    thresholds = ATHLETIC_THRESHOLDS.get(position, {})
    frame_h = int(thresholds.get("min_height_in", 72))
    frame_w = int(thresholds.get("min_weight_lb", 220))
    frame_bonus = (height_in - frame_h) * 0.28 + (weight_lb - frame_w) * 0.008
    pos_mod = {
        "QB": 1.5,
        "RB": 0.8,
        "WR": 1.0,
        "TE": 0.9,
        "OT": 0.7,
        "IOL": 0.4,
        "EDGE": 1.2,
        "DT": 0.8,
        "LB": 1.0,
        "CB": 1.1,
        "S": 1.0,
    }.get(position, 0.8)
    score = base + frame_bonus * pos_mod
    return max(60.0, min(95.0, score))


def _trait_proxy_score(position: str, rank_seed: int) -> float:
    pos_bonus = {
        "QB": 1.8,
        "OT": 1.2,
        "EDGE": 1.0,
        "CB": 0.9,
        "WR": 0.8,
    }.get(position, 0.5)
    score = 95.0 - (rank_seed * 0.085) + pos_bonus
    return max(60.0, min(97.0, score))


def _film_trait_blend_weight(coverage: float) -> float:
    """Blend weight for film-charted trait score; higher coverage gets more authority."""
    if coverage >= 0.90:
        return 0.55
    if coverage >= 0.75:
        return 0.45
    if coverage >= 0.50:
        return 0.35
    return 0.25


def _production_score(class_year: str, rank_seed: int) -> float:
    exp_bonus = 1.2 if class_year.endswith("SR") or class_year == "SR" else 0.5
    if class_year in {"SO", "RSO"}:
        exp_bonus = 0.2
    score = 92.0 - (rank_seed * 0.08) + exp_bonus
    return max(58.0, min(95.0, score))


def _context_score(rank_seed: int) -> float:
    return max(62.0, min(93.0, 90.0 - rank_seed * 0.07))


def _risk_penalty(class_year: str, rank_seed: int) -> float:
    early_entry = 1.8 if class_year in {"SO", "RSO", "RFR", "FR"} else 0.9
    uncertainty = 1.4 if rank_seed > 180 else 0.6
    return early_entry + uncertainty


def grade_player(
    position: str,
    rank_seed: int,
    class_year: str,
    height_in: int,
    weight_lb: int,
    film_subtraits: Optional[Mapping[str, float]] = None,
) -> dict:
    weights = POSITION_WEIGHTS[position]

    trait_proxy = _trait_proxy_score(position, rank_seed)
    film_eval = score_film_traits(position, film_subtraits or {})

    if film_eval["film_trait_score"] is not None:
        film_weight = _film_trait_blend_weight(film_eval["film_trait_coverage"])
        trait = (1.0 - film_weight) * trait_proxy + film_weight * float(film_eval["film_trait_score"])
    else:
        film_weight = 0.0
        trait = trait_proxy

    prod = _production_score(class_year, rank_seed)
    ath = _athletic_proxy(position, rank_seed, height_in, weight_lb)
    size = _size_score(position, height_in, weight_lb)
    context = _context_score(rank_seed)
    risk = _risk_penalty(class_year, rank_seed)

    final_grade = (
        weights["trait"] * trait
        + weights["production"] * prod
        + weights["athletic"] * ath
        + weights["size"] * size
        + weights["context"] * context
        - risk
    )

    psi = 0.45 * trait + 0.30 * prod + 0.15 * ath + 0.10 * size
    floor = final_grade - max(1.5, risk)
    ceiling = final_grade + (2.2 if class_year in {"SO", "RSO", "JR"} else 1.5)

    core_stat_name = CORE_STAT_BY_POS.get(position, "Prospect Signature Index")
    core_stat_value = round(50.0 + (psi - 70.0) * 1.4 + (100 - rank_seed) * 0.08, 2)

    return {
        "trait_score": round(trait, 2),
        "trait_proxy_score": round(trait_proxy, 2),
        "film_trait_score": round(film_eval["film_trait_score"], 2) if film_eval["film_trait_score"] is not None else "",
        "film_trait_coverage": round(film_eval["film_trait_coverage"], 3),
        "film_trait_blend_weight": round(film_weight, 3),
        "film_trait_missing_count": film_eval["film_trait_missing_count"],
        "production_score": round(prod, 2),
        "athletic_score": round(ath, 2),
        "size_score": round(size, 2),
        "context_score": round(context, 2),
        "risk_penalty": round(risk, 2),
        "final_grade": round(final_grade, 2),
        "floor_grade": round(floor, 2),
        "ceiling_grade": round(ceiling, 2),
        "round_value": round_from_grade(final_grade),
        "psi": round(psi, 2),
        "core_stat_name": core_stat_name,
        "core_stat_value": core_stat_value,
        "best_role": ROLE_BY_POS.get(position, "Depth and developmental value"),
        "best_scheme_fit": SCHEME_FIT_BY_POS.get(position, "multiple"),
    }


def scouting_note(position: str, final_grade: float, rank_seed: int) -> str:
    tier = "instant starter" if final_grade >= 88 else "early contributor" if final_grade >= 82 else "developmental contributor"
    lens = {
        "QB": "wins with timing and structure while retaining off-script creation",
        "RB": "creates hidden yards and stays on schedule",
        "WR": "creates separation and finishes at the catch point",
        "TE": "projects as a mismatch in multiple alignments",
        "OT": "has playable pass-pro mechanics with growth runway",
        "IOL": "stabilizes interior pocket depth and run fits",
        "EDGE": "produces pressure through get-off and counters",
        "DT": "controls interior gaps and collapses pocket",
        "LB": "processes quickly and closes efficiently",
        "CB": "matches routes with disciplined leverage",
        "S": "eliminates explosives with range and communication",
    }.get(position, "has translatable NFL tools")
    return f"{tier} profile; {lens}. Seed rank {rank_seed} indicates current market confidence with developmental upside still available."


def quick_z(value: float, mean: float, std: float) -> float:
    if std == 0:
        return 0.0
    return (value - mean) / std


def softplus(x: float) -> float:
    return math.log1p(math.exp(x))
