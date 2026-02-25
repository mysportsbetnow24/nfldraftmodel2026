from __future__ import annotations

from typing import Dict, Mapping


# Position-specific sub-trait rubrics. Each trait is scored on a 0-100 scale by scouts.
POSITION_FILM_TRAIT_WEIGHTS: Dict[str, Dict[str, float]] = {
    "QB": {
        "processing": 0.22,
        "accuracy": 0.20,
        "arm_talent": 0.14,
        "creation": 0.14,
        "pocket_presence": 0.15,
        "situational_command": 0.15,
    },
    "RB": {
        "vision": 0.22,
        "burst": 0.16,
        "contact_balance": 0.20,
        "lateral_agility": 0.12,
        "pass_pro": 0.15,
        "receiving": 0.15,
    },
    "WR": {
        "release": 0.18,
        "route_running": 0.21,
        "separation": 0.22,
        "ball_skills": 0.16,
        "yac": 0.13,
        "play_strength": 0.10,
    },
    "TE": {
        "release": 0.12,
        "route_running": 0.17,
        "ball_skills": 0.17,
        "yac": 0.14,
        "inline_blocking": 0.22,
        "pass_pro": 0.18,
    },
    "OT": {
        "pass_set": 0.20,
        "anchor": 0.18,
        "hand_usage": 0.18,
        "recovery": 0.14,
        "run_blocking": 0.16,
        "processing": 0.14,
    },
    "IOL": {
        "leverage": 0.18,
        "anchor": 0.18,
        "hand_usage": 0.16,
        "processing": 0.18,
        "lateral_agility": 0.12,
        "run_blocking": 0.18,
    },
    "EDGE": {
        "get_off": 0.17,
        "bend": 0.19,
        "hand_usage": 0.16,
        "rush_plan": 0.15,
        "counter_moves": 0.13,
        "run_defense": 0.20,
    },
    "DT": {
        "get_off": 0.15,
        "power": 0.20,
        "hand_usage": 0.16,
        "leverage": 0.18,
        "pass_rush": 0.15,
        "run_defense": 0.16,
    },
    "LB": {
        "processing": 0.21,
        "trigger": 0.16,
        "range": 0.18,
        "block_deconstruction": 0.14,
        "coverage": 0.16,
        "tackling": 0.15,
    },
    "CB": {
        "press": 0.15,
        "footwork": 0.16,
        "recovery_speed": 0.17,
        "processing": 0.16,
        "ball_skills": 0.19,
        "tackling": 0.17,
    },
    "S": {
        "processing": 0.20,
        "range": 0.18,
        "man_coverage": 0.14,
        "tackling": 0.16,
        "angles": 0.16,
        "communication": 0.16,
    },
}


ALL_FILM_TRAITS = sorted({trait for weights in POSITION_FILM_TRAIT_WEIGHTS.values() for trait in weights})


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def score_film_traits(position: str, subtraits: Mapping[str, float]) -> dict:
    """
    Calculate weighted film trait score from per-position sub-traits.
    Returns score + coverage metadata to support blend confidence.
    """
    weights = POSITION_FILM_TRAIT_WEIGHTS.get(position, {})
    if not weights:
        return {
            "film_trait_score": None,
            "film_trait_raw": None,
            "film_trait_coverage": 0.0,
            "film_trait_missing_count": 0,
        }

    weighted_sum = 0.0
    covered_weight = 0.0
    missing_count = 0

    for trait, weight in weights.items():
        raw = subtraits.get(trait)
        if raw is None:
            missing_count += 1
            continue
        try:
            score = _clamp_score(float(raw))
        except (TypeError, ValueError):
            missing_count += 1
            continue

        weighted_sum += score * weight
        covered_weight += weight

    if covered_weight == 0:
        return {
            "film_trait_score": None,
            "film_trait_raw": None,
            "film_trait_coverage": 0.0,
            "film_trait_missing_count": len(weights),
        }

    raw_score = weighted_sum / covered_weight

    # Penalize heavy missingness to prevent sparse charts from over-influencing grades.
    coverage = covered_weight
    missing_penalty = max(0.0, (0.65 - coverage) * 8.0)
    final_score = _clamp_score(raw_score - missing_penalty)

    return {
        "film_trait_score": round(final_score, 2),
        "film_trait_raw": round(raw_score, 2),
        "film_trait_coverage": round(coverage, 3),
        "film_trait_missing_count": missing_count,
    }
