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
    "QB": "Timing-spread distributor framework",
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
    "QB": "Starting QB profile (role lane pending)",
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


def _safe_trait_value(subtraits: Mapping[str, float], key: str) -> Optional[float]:
    raw = subtraits.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _safe_prod_value(prod: Mapping[str, object], *keys: str) -> Optional[float]:
    for key in keys:
        raw = prod.get(key)
        if raw is None:
            continue
        txt = str(raw).strip()
        if not txt:
            continue
        try:
            return float(txt)
        except (TypeError, ValueError):
            continue
    return None


def _avg_defined(values: list[Optional[float]]) -> Optional[float]:
    usable = [v for v in values if v is not None]
    if not usable:
        return None
    return sum(usable) / len(usable)


def _infer_lb_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
) -> tuple[str, str]:
    """
    Role/scheme inference for off-ball linebackers.
    Keeps labels scouting-native while avoiding one-size-fits-all defaults.
    """
    processing = _safe_trait_value(film_subtraits, "processing")
    trigger = _safe_trait_value(film_subtraits, "trigger")
    range_score = _safe_trait_value(film_subtraits, "range")
    decon = _safe_trait_value(film_subtraits, "block_deconstruction")
    coverage = _safe_trait_value(film_subtraits, "coverage")
    tackling = _safe_trait_value(film_subtraits, "tackling")

    coverage_profile = _avg_defined([coverage, range_score, trigger])
    box_profile = _avg_defined([processing, decon, tackling])
    pressure_profile = _avg_defined([trigger, decon, processing])

    xl_frame = weight_lb >= 240
    big_frame = weight_lb >= 235
    light_frame = weight_lb <= 227
    long_frame = height_in >= 75
    explosive_athlete = athletic_score >= 88.0
    mobile_athlete = athletic_score >= 85.0
    limited_athlete = athletic_score < 81.0

    # Film-first role classification when sub-trait coverage exists.
    if coverage_profile is not None and coverage_profile >= 82.0 and (light_frame or (mobile_athlete and long_frame)):
        return ("STAR overhang coverage backer", "Big-nickel overhang match")
    if coverage_profile is not None and coverage_profile >= 78.0 and (light_frame or mobile_athlete):
        return ("Coverage WILL backer", "Two-high match zone")
    if pressure_profile is not None and pressure_profile >= 79.0 and mobile_athlete and (big_frame or long_frame):
        return ("Pressure SAM backer", "Sim-pressure multiple front")
    if box_profile is not None and box_profile >= 77.0 and xl_frame:
        return ("MIKE thumper (stack-and-shed)", "Single-high run-fit zone match")
    if box_profile is not None and box_profile >= 75.0 and big_frame:
        return ("Stack-and-shed MIKE backer", "Base over-under run fit")
    if trigger is not None and trigger >= 79.0 and mobile_athlete and not big_frame:
        return ("Run-and-chase WILL backer", "Pursuit-heavy split-safety fit")

    # Fallback classification for sparse film traits.
    if light_frame and explosive_athlete:
        if long_frame:
            return ("STAR overhang coverage backer", "Big-nickel overhang match")
        return ("Coverage WILL backer", "Two-high match zone")
    if light_frame and mobile_athlete:
        return ("Coverage WILL backer", "Two-high match zone")
    if light_frame:
        return ("Run-and-chase WILL backer", "Split-safety pursuit fit")
    if big_frame and mobile_athlete:
        return ("Pressure SAM backer", "Sim-pressure multiple front")
    if xl_frame or (big_frame and limited_athlete):
        return ("MIKE thumper (stack-and-shed)", "Single-high run-fit zone match")
    if big_frame:
        return ("Stack-and-shed MIKE backer", "Base over-under run fit")
    if mobile_athlete:
        return ("Run-and-chase WILL backer", "Pursuit-heavy split-safety fit")
    return ("Balanced MIKE/WILL communicator", "Match-zone multiple")


def _infer_rb_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    """
    Role/scheme inference for running backs.
    Keeps labels scouting-native while distinguishing archetypes beyond committee defaults.
    """
    vision = _safe_trait_value(film_subtraits, "vision")
    burst = _safe_trait_value(film_subtraits, "burst")
    contact_balance = _safe_trait_value(film_subtraits, "contact_balance")
    lateral = _safe_trait_value(film_subtraits, "lateral_agility")
    pass_pro = _safe_trait_value(film_subtraits, "pass_pro")
    receiving = _safe_trait_value(film_subtraits, "receiving")

    power_profile = _avg_defined([contact_balance, vision, pass_pro])
    space_profile = _avg_defined([burst, lateral, receiving])
    three_down_profile = _avg_defined([vision, pass_pro, receiving, burst])

    heavy_back = weight_lb >= 222
    compact_back = weight_lb <= 202
    dense_back = weight_lb >= 216
    explosive = athletic_score >= 88.0
    dynamic = athletic_score >= 85.0
    limited = athletic_score < 81.0

    prod = production_context or {}
    explosive_rate = _safe_prod_value(prod, "cfb_rb_explosive_rate", "explosive_run_rate")
    mtf_per_touch = _safe_prod_value(
        prod,
        "cfb_rb_missed_tackles_forced_per_touch",
        "missed_tackles_forced_per_touch",
    )
    yac_per_att = _safe_prod_value(
        prod,
        "cfb_rb_yards_after_contact_per_attempt",
        "cfb_rb_yac_per_att",
        "rb_yards_after_contact_per_attempt",
        "rb_yac_per_att",
    )
    rb_target_share = _safe_prod_value(prod, "cfb_rb_target_share", "rb_target_share", "target_share")
    rb_receiving_eff = _safe_prod_value(
        prod,
        "cfb_rb_receiving_efficiency",
        "rb_receiving_efficiency",
        "cfb_rb_yards_per_reception",
        "rb_yards_per_reception",
    )

    explosive_prod = explosive_rate is not None and explosive_rate >= 0.155
    tackle_break_prod = mtf_per_touch is not None and mtf_per_touch >= 0.24
    yac_prod = yac_per_att is not None and yac_per_att >= 3.15
    receiving_volume_prod = rb_target_share is not None and rb_target_share >= 0.10
    receiving_eff_prod = rb_receiving_eff is not None and rb_receiving_eff >= 8.6
    weak_explosive_prod = explosive_rate is not None and explosive_rate < 0.11
    weak_tackle_break_prod = mtf_per_touch is not None and mtf_per_touch < 0.18

    rb_prod_flags = sum(
        int(flag)
        for flag in [explosive_prod, tackle_break_prod, yac_prod, receiving_volume_prod, receiving_eff_prod]
    )

    # Production-supported overrides (requires multiple positive signals).
    if rb_prod_flags >= 3 and dynamic and weight_lb >= 205:
        if dense_back and (explosive_prod or yac_prod):
            return ("Do-it-all juggernaut", "Gap-zone hybrid feature run game")
        return ("Primary every-down creator", "Wide-zone/play-action feature back")
    if rb_prod_flags >= 2 and compact_back and dynamic and (receiving_volume_prod or receiving_eff_prod):
        return ("Shifty scat back", "Spread-space angle/screen package")
    if rb_prod_flags >= 2 and heavy_back and (tackle_break_prod or yac_prod):
        return ("Classic two-down bruiser", "Gap/power downhill run menu")

    # Film-led mapping when charted traits exist.
    if three_down_profile is not None and three_down_profile >= 80.0 and dynamic and weight_lb >= 205:
        if dense_back and explosive:
            return ("Do-it-all juggernaut", "Gap-zone hybrid feature run game")
        return ("Primary every-down creator", "Wide-zone/play-action feature back")
    if space_profile is not None and space_profile >= 80.0 and compact_back and dynamic:
        return ("Shifty scat back", "Spread-space angle/screen package")
    if space_profile is not None and space_profile >= 78.0 and dynamic:
        return ("Explosive one-cut slasher", "Wide-zone one-cut system")
    if power_profile is not None and power_profile >= 78.0 and heavy_back:
        return ("Classic two-down bruiser", "Gap/power downhill run menu")
    if receiving is not None and pass_pro is not None and receiving >= 78.0 and pass_pro >= 74.0:
        return ("Passing-down mismatch back", "Empty/gun pass-game backfield usage")
    if power_profile is not None and power_profile >= 74.0 and dense_back:
        return ("Early-down grinder", "Inside-zone/gap rotation")

    # Sparse-trait fallback.
    if weak_explosive_prod and weak_tackle_break_prod and not dynamic:
        return ("Primary committee back with passing-down utility", "Wide-zone committee")
    if explosive and dense_back:
        return ("Do-it-all juggernaut", "Gap-zone hybrid feature run game")
    if explosive and weight_lb >= 204:
        return ("Primary every-down creator", "Wide-zone/play-action feature back")
    if compact_back and dynamic:
        return ("Shifty scat back", "Spread-space angle/screen package")
    if heavy_back and (dynamic or not limited):
        return ("Classic two-down bruiser", "Gap/power downhill run menu")
    if heavy_back:
        return ("Early-down grinder", "Inside-zone/gap rotation")
    if dynamic:
        return ("Explosive one-cut slasher", "Wide-zone one-cut system")
    return ("Primary committee back with passing-down utility", "Wide-zone committee")


def _infer_qb_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
) -> tuple[str, str]:
    """
    Role/scheme inference for quarterbacks.
    Keeps public labels scouting-native and avoids a single default archetype.
    """
    processing = _safe_trait_value(film_subtraits, "processing")
    accuracy = _safe_trait_value(film_subtraits, "accuracy")
    arm_talent = _safe_trait_value(film_subtraits, "arm_talent")
    creation = _safe_trait_value(film_subtraits, "creation")
    pocket = _safe_trait_value(film_subtraits, "pocket_presence")
    situational = _safe_trait_value(film_subtraits, "situational_command")

    distributor_profile = _avg_defined([processing, accuracy, pocket, situational])
    creator_profile = _avg_defined([creation, arm_talent, situational])
    pure_arm_profile = _avg_defined([arm_talent, creation])
    structure_profile = _avg_defined([processing, pocket, accuracy])

    plus_athlete = athletic_score >= 88.0
    good_athlete = athletic_score >= 84.0
    limited_athlete = athletic_score < 80.0
    prototype_frame = height_in >= 75 and weight_lb >= 220

    if distributor_profile is not None and distributor_profile >= 81.0 and good_athlete:
        if creation is not None and creation >= 78.0:
            return ("Franchise dual-threat creator", "Spread/RPO vertical-play-action")
        return ("Franchise timing distributor", "West Coast timing spread")
    if creator_profile is not None and creator_profile >= 80.0 and plus_athlete:
        return ("Dual-threat shot-play creator", "Spread/RPO vertical-play-action")
    if pure_arm_profile is not None and pure_arm_profile >= 80.0 and (arm_talent or 0.0) >= 82.0:
        return ("Vertical drive-starter", "Play-action deep-shot structure")
    if structure_profile is not None and structure_profile >= 76.0 and not limited_athlete:
        return ("Rhythm-and-timing starter", "Quick-game progression spread")
    if structure_profile is not None and structure_profile >= 74.0 and limited_athlete:
        return ("Pocket distributor", "Play-action under-center progression")
    if creator_profile is not None and creator_profile >= 74.0 and good_athlete:
        return ("Developmental creator with upside", "Spread movement-passer package")

    if plus_athlete and prototype_frame:
        return ("Developmental dual-threat upside QB", "Spread/RPO vertical-play-action")
    if good_athlete:
        return ("Developmental creator with upside", "Spread movement-passer package")
    if limited_athlete:
        return ("Developmental pocket backup profile", "Quick-game progression spread")
    return ("Starter-caliber distributor profile", "Timing-spread distributor framework")


def _infer_role_and_scheme(
    *,
    position: str,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    if position == "QB":
        return _infer_qb_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
        )
    if position == "RB":
        return _infer_rb_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context,
        )
    if position == "LB":
        return _infer_lb_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
        )
    return (
        ROLE_BY_POS.get(position, "Depth and developmental value"),
        SCHEME_FIT_BY_POS.get(position, "multiple"),
    )


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
    production_context: Optional[Mapping[str, object]] = None,
) -> dict:
    weights = POSITION_WEIGHTS[position]
    film_inputs = film_subtraits or {}

    trait_proxy = _trait_proxy_score(position, rank_seed)
    film_eval = score_film_traits(position, film_inputs)

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
    best_role, best_scheme_fit = _infer_role_and_scheme(
        position=position,
        height_in=height_in,
        weight_lb=weight_lb,
        athletic_score=ath,
        film_subtraits=film_inputs,
        production_context=production_context or {},
    )

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
        "best_role": best_role,
        "best_scheme_fit": best_scheme_fit,
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
