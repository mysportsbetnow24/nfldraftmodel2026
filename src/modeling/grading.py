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


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _score_linear(value: Optional[float], lo: float, hi: float) -> Optional[float]:
    if value is None or hi <= lo:
        return None
    scaled = (float(value) - lo) / (hi - lo)
    return round(_clamp(60.0 + (40.0 * scaled), 60.0, 100.0), 2)


def _score_inverse(value: Optional[float], hi_bad: float, lo_good: float) -> Optional[float]:
    if value is None or hi_bad <= lo_good:
        return None
    scaled = (hi_bad - float(value)) / (hi_bad - lo_good)
    return round(_clamp(60.0 + (40.0 * scaled), 60.0, 100.0), 2)


def _infer_lb_archetype(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> str:
    processing = _safe_trait_value(film_subtraits, "processing")
    trigger = _safe_trait_value(film_subtraits, "trigger")
    range_score = _safe_trait_value(film_subtraits, "range")
    decon = _safe_trait_value(film_subtraits, "block_deconstruction")
    coverage = _safe_trait_value(film_subtraits, "coverage")
    tackling = _safe_trait_value(film_subtraits, "tackling")

    coverage_profile = _avg_defined([coverage, range_score, trigger])
    box_profile = _avg_defined([processing, decon, tackling])
    pressure_profile = _avg_defined([trigger, decon, processing])

    prod = production_context or {}
    lb_sacks = _safe_prod_value(prod, "cfb_lb_sacks", "lb_sacks")
    lb_hurries = _safe_prod_value(prod, "cfb_lb_qb_hurries", "lb_qb_hurries")
    lb_rush_signal = _safe_prod_value(prod, "cfb_lb_rush_impact_signal")
    sg_pressures = _safe_prod_value(prod, "sg_def_total_pressures")
    sg_tfl = _safe_prod_value(prod, "sg_def_tackles_for_loss")
    sg_cov_grade = _safe_prod_value(prod, "sg_def_coverage_grade", "sg_cov_grade")
    sg_slot_snaps = _safe_prod_value(prod, "sg_slot_cov_snaps")
    sg_slot_spt = _safe_prod_value(prod, "sg_slot_cov_snaps_per_target")

    light_frame = weight_lb <= 228
    big_frame = weight_lb >= 236
    long_frame = height_in >= 75
    mobile_athlete = athletic_score >= 85.0

    pressure_usage = (
        (lb_sacks is not None and lb_sacks >= 4.0)
        or (lb_hurries is not None and lb_hurries >= 9.0)
        or (lb_rush_signal is not None and lb_rush_signal >= 74.0)
        or (sg_pressures is not None and sg_pressures >= 18.0)
    )
    strong_coverage = (
        (coverage_profile is not None and coverage_profile >= 79.0)
        or (sg_cov_grade is not None and sg_cov_grade >= 78.0)
        or (sg_slot_spt is not None and sg_slot_spt >= 5.0)
    )
    overhang_usage = (sg_slot_snaps is not None and sg_slot_snaps >= 35.0) or (sg_slot_spt is not None and sg_slot_spt >= 5.0)
    strong_box = (
        (box_profile is not None and box_profile >= 76.0)
        or (sg_tfl is not None and sg_tfl >= 6.0)
    )

    if pressure_usage and ((pressure_profile is not None and pressure_profile >= 76.0) or mobile_athlete) and not strong_coverage:
        return "hybrid_edge_lb"
    if strong_coverage and (overhang_usage or light_frame or mobile_athlete or long_frame):
        return "coverage_overhang_lb"
    if strong_box or big_frame:
        return "off_ball_lb"
    if pressure_usage and strong_coverage:
        return "coverage_overhang_lb"
    return "off_ball_lb"


def _infer_lb_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str, str]:
    # Returns: best_role, best_scheme_fit, lb_archetype
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
    prod = production_context or {}
    lb_tackles = _safe_prod_value(prod, "cfb_lb_tackles", "lb_tackles")
    lb_tfl = _safe_prod_value(prod, "cfb_lb_tfl", "lb_tfl")
    lb_sacks = _safe_prod_value(prod, "cfb_lb_sacks", "lb_sacks")
    lb_hurries = _safe_prod_value(prod, "cfb_lb_qb_hurries", "lb_qb_hurries")
    lb_tackle_signal = _safe_prod_value(prod, "cfb_lb_tackle_signal")
    lb_rush_signal = _safe_prod_value(prod, "cfb_lb_rush_impact_signal")

    xl_frame = weight_lb >= 240
    big_frame = weight_lb >= 235
    light_frame = weight_lb <= 227
    long_frame = height_in >= 75
    explosive_athlete = athletic_score >= 88.0
    mobile_athlete = athletic_score >= 85.0
    limited_athlete = athletic_score < 81.0
    high_volume_tackler = (lb_tackles is not None and lb_tackles >= 78) or (lb_tackle_signal is not None and lb_tackle_signal >= 74)
    impact_run_game = (lb_tfl is not None and lb_tfl >= 8) or (lb_tackles is not None and lb_tackles >= 95)
    pressure_usage = (
        (lb_sacks is not None and lb_sacks >= 4.0)
        or (lb_hurries is not None and lb_hurries >= 9.0)
        or (lb_rush_signal is not None and lb_rush_signal >= 76.0)
    )
    archetype = _infer_lb_archetype(
        height_in=height_in,
        weight_lb=weight_lb,
        athletic_score=athletic_score,
        film_subtraits=film_subtraits,
        production_context=production_context or {},
    )

    if archetype == "coverage_overhang_lb":
        if pressure_usage and (coverage_profile is not None and coverage_profile >= 78.0):
            return ("Coverage overhang WILL/SAM hybrid", "Big nickel overhang match", "coverage_overhang_lb")
        if high_volume_tackler and (coverage_profile is not None and coverage_profile >= 76.0):
            return ("Coverage WILL backer", "Two-high match zone", "coverage_overhang_lb")
        return ("Space-match overhang backer", "Big nickel overhang match", "coverage_overhang_lb")

    if archetype == "hybrid_edge_lb":
        if pressure_usage and mobile_athlete and (big_frame or long_frame):
            return ("Stand-up pressure SAM/EDGE hybrid", "Sim-pressure multiple front", "hybrid_edge_lb")
        return ("Pressure SAM backer", "Sim-pressure multiple front", "hybrid_edge_lb")

    # Production-supported role nudges.
    if pressure_usage and mobile_athlete and (big_frame or long_frame):
        return ("Pressure SAM backer", "Sim-pressure multiple front", "off_ball_lb")
    if high_volume_tackler and impact_run_game and (big_frame or xl_frame):
        return ("Stack-and-shed MIKE backer", "Base over-under run fit", "off_ball_lb")
    if high_volume_tackler and (light_frame or mobile_athlete):
        return ("Run-and-chase WILL backer", "Pursuit-heavy split-safety fit", "off_ball_lb")

    # Film-first role classification when sub-trait coverage exists.
    if coverage_profile is not None and coverage_profile >= 82.0 and (light_frame or (mobile_athlete and long_frame)):
        return ("STAR overhang coverage backer", "Big-nickel overhang match", "coverage_overhang_lb")
    if coverage_profile is not None and coverage_profile >= 78.0 and (light_frame or mobile_athlete):
        return ("Coverage WILL backer", "Two-high match zone", "coverage_overhang_lb")
    if pressure_profile is not None and pressure_profile >= 79.0 and mobile_athlete and (big_frame or long_frame):
        return ("Pressure SAM backer", "Sim-pressure multiple front", "hybrid_edge_lb")
    if box_profile is not None and box_profile >= 77.0 and xl_frame:
        return ("MIKE thumper (stack-and-shed)", "Single-high run-fit zone match", "off_ball_lb")
    if box_profile is not None and box_profile >= 75.0 and big_frame:
        return ("Stack-and-shed MIKE backer", "Base over-under run fit", "off_ball_lb")
    if trigger is not None and trigger >= 79.0 and mobile_athlete and not big_frame:
        return ("Run-and-chase WILL backer", "Pursuit-heavy split-safety fit", "off_ball_lb")

    # Fallback classification for sparse film traits.
    if light_frame and explosive_athlete:
        if long_frame:
            return ("STAR overhang coverage backer", "Big-nickel overhang match", "coverage_overhang_lb")
        return ("Coverage WILL backer", "Two-high match zone", "coverage_overhang_lb")
    if light_frame and mobile_athlete:
        return ("Coverage WILL backer", "Two-high match zone", "coverage_overhang_lb")
    if light_frame:
        return ("Run-and-chase WILL backer", "Split-safety pursuit fit", "off_ball_lb")
    if big_frame and mobile_athlete:
        return ("Pressure SAM backer", "Sim-pressure multiple front", "hybrid_edge_lb")
    if xl_frame or (big_frame and limited_athlete):
        return ("MIKE thumper (stack-and-shed)", "Single-high run-fit zone match", "off_ball_lb")
    if big_frame:
        return ("Stack-and-shed MIKE backer", "Base over-under run fit", "off_ball_lb")
    if mobile_athlete:
        return ("Run-and-chase WILL backer", "Pursuit-heavy split-safety fit", "off_ball_lb")
    return ("Balanced MIKE/WILL communicator", "Match-zone multiple", "off_ball_lb")


def _lb_trait_context_score(
    *,
    archetype: str,
    athletic_score: float,
    production_context: Mapping[str, object] | None = None,
) -> Optional[float]:
    prod = production_context or {}
    run_grade = _safe_prod_value(prod, "sg_def_run_grade")
    coverage_grade = _safe_prod_value(prod, "sg_def_coverage_grade", "sg_cov_grade")
    tackle_grade = _safe_prod_value(prod, "sg_def_tackle_grade")
    pressures = _safe_prod_value(prod, "sg_def_total_pressures")
    tfl = _safe_prod_value(prod, "sg_def_tackles_for_loss")
    rush_signal = _safe_prod_value(prod, "cfb_lb_rush_impact_signal")
    slot_spt = _safe_prod_value(prod, "sg_slot_cov_snaps_per_target")
    cov_yps = _safe_prod_value(prod, "sg_cov_yards_per_snap")
    cov_qbr = _safe_prod_value(prod, "sg_cov_qb_rating_against")

    run_component = _avg_defined(
        [
            _score_linear(run_grade, 58.0, 90.0),
            _score_linear(tackle_grade, 60.0, 92.0),
            _score_linear(tfl, 2.0, 10.0),
        ]
    )
    coverage_component = _avg_defined(
        [
            _score_linear(coverage_grade, 52.0, 90.0),
            _score_linear(slot_spt, 2.5, 7.5),
            _score_inverse(cov_yps, 2.0, 0.45),
            _score_inverse(cov_qbr, 130.0, 55.0),
        ]
    )
    pressure_component = _avg_defined(
        [
            _score_linear(pressures, 4.0, 28.0),
            _score_linear(tfl, 2.0, 12.0),
            _score_linear(rush_signal, 16.0, 42.0),
        ]
    )
    athletic_component = _clamp(float(athletic_score), 60.0, 95.0)

    if archetype == "coverage_overhang_lb":
        weights = [
            (0.42, coverage_component),
            (0.20, run_component),
            (0.14, pressure_component),
            (0.24, athletic_component),
        ]
    elif archetype == "hybrid_edge_lb":
        weights = [
            (0.40, pressure_component),
            (0.15, coverage_component),
            (0.20, run_component),
            (0.25, athletic_component),
        ]
    else:
        weights = [
            (0.42, run_component),
            (0.22, coverage_component),
            (0.14, pressure_component),
            (0.22, athletic_component),
        ]

    usable = [(w, v) for w, v in weights if v is not None]
    if not usable:
        return None
    weight_total = sum(w for w, _ in usable)
    return round(sum(w * float(v) for w, v in usable) / weight_total, 2)


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


def _infer_wr_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    release = _safe_trait_value(film_subtraits, "release")
    route_running = _safe_trait_value(film_subtraits, "route_running")
    separation = _safe_trait_value(film_subtraits, "separation")
    ball_skills = _safe_trait_value(film_subtraits, "ball_skills")
    yac = _safe_trait_value(film_subtraits, "yac")
    play_strength = _safe_trait_value(film_subtraits, "play_strength")

    separator_profile = _avg_defined([release, route_running, separation])
    volume_profile = _avg_defined([route_running, separation, ball_skills])
    vertical_profile = _avg_defined([separation, ball_skills, yac])

    prod = production_context or {}
    route_grade = _safe_prod_value(prod, "sg_wrte_route_grade")
    yprr = _safe_prod_value(prod, "sg_wrte_yprr")
    targets_per_route = _safe_prod_value(prod, "sg_wrte_targets_per_route")
    man_yprr = _safe_prod_value(prod, "sg_wrte_man_yprr")
    zone_yprr = _safe_prod_value(prod, "sg_wrte_zone_yprr")
    contested = _safe_prod_value(prod, "sg_wrte_contested_catch_rate")
    drop_rate = _safe_prod_value(prod, "sg_wrte_drop_rate")

    big_frame = height_in >= 74 and weight_lb >= 205
    compact_frame = height_in <= 71 and weight_lb <= 192
    explosive = athletic_score >= 88.0
    dynamic = athletic_score >= 84.0

    elite_route = (route_grade is not None and route_grade >= 79.0) or (separator_profile is not None and separator_profile >= 79.0)
    high_volume = ((targets_per_route is not None and targets_per_route >= 0.24) and (yprr is not None and yprr >= 2.1)) or (
        volume_profile is not None and volume_profile >= 78.0
    )
    vertical_winner = (
        (man_yprr is not None and man_yprr >= 2.0)
        or (contested is not None and contested >= 0.55)
        or (vertical_profile is not None and vertical_profile >= 77.0 and explosive)
    )
    reliable_hands = (drop_rate is not None and drop_rate <= 0.07) or (ball_skills is not None and ball_skills >= 77.0)

    if elite_route and high_volume and reliable_hands:
        return ("Volume-driving X/Z separator", "Spread isolation / full-route-tree attack")
    if vertical_winner and big_frame:
        return ("Boundary vertical stressor", "Play-action shot / isolation winner")
    if elite_route and compact_frame and dynamic:
        return ("Movement Z separator", "Spread motion / leverage-creation menu")
    if high_volume:
        return ("Alignment-flexible target earner", "Spread spacing / progression target funnel")
    if vertical_winner:
        return ("Field-stretching Z receiver", "Vertical spread / play-action launcher")
    if compact_frame and dynamic:
        return ("YAC-tilted slot/inside-out target", "Motion slot / quick-game stress")
    return ("Alignment-flexible target earner", "Spread vertical / spacing menu")


def _infer_te_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    route_running = _safe_trait_value(film_subtraits, "route_running")
    ball_skills = _safe_trait_value(film_subtraits, "ball_skills")
    yac = _safe_trait_value(film_subtraits, "yac")
    inline_blocking = _safe_trait_value(film_subtraits, "inline_blocking")
    pass_pro = _safe_trait_value(film_subtraits, "pass_pro")

    detached_profile = _avg_defined([route_running, ball_skills, yac])
    attached_profile = _avg_defined([inline_blocking, pass_pro, ball_skills])

    prod = production_context or {}
    route_grade = _safe_prod_value(prod, "sg_wrte_route_grade")
    yprr = _safe_prod_value(prod, "sg_wrte_yprr")
    targets_per_route = _safe_prod_value(prod, "sg_wrte_targets_per_route")
    contested = _safe_prod_value(prod, "sg_wrte_contested_catch_rate")
    pass_block = _safe_prod_value(prod, "sg_ol_pass_block_grade")
    run_block = _safe_prod_value(prod, "sg_ol_run_block_grade")

    dynamic = athletic_score >= 84.0
    attached_blocker = (
        (attached_profile is not None and attached_profile >= 75.0)
        or (pass_block is not None and pass_block >= 70.0)
        or (run_block is not None and run_block >= 68.0)
    )
    route_earner = (
        (route_grade is not None and route_grade >= 74.0 and yprr is not None and yprr >= 1.75)
        or (detached_profile is not None and detached_profile >= 76.0)
    )
    mismatch_ball = (contested is not None and contested >= 0.52) or (ball_skills is not None and ball_skills >= 76.0)

    if route_earner and attached_blocker and mismatch_ball:
        return ("In-line + move mismatch", "Multiple attach/detach stress package")
    if route_earner and dynamic:
        return ("Detached mismatch F tight end", "Spread detach / seam-stretch menu")
    if attached_blocker and mismatch_ball:
        return ("Y-tight end with chain-moving value", "Play-action attach / boot complement")
    if attached_blocker:
        return ("Inline TE2 with protection utility", "Balanced attach / split-zone package")
    return ("Move tight end mismatch piece", "Detach / slot seam package")


def _infer_ot_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    pass_set = _safe_trait_value(film_subtraits, "pass_set")
    anchor = _safe_trait_value(film_subtraits, "anchor")
    hand_usage = _safe_trait_value(film_subtraits, "hand_usage")
    recovery = _safe_trait_value(film_subtraits, "recovery")
    run_blocking = _safe_trait_value(film_subtraits, "run_blocking")
    processing = _safe_trait_value(film_subtraits, "processing")

    pass_pro_profile = _avg_defined([pass_set, anchor, hand_usage, recovery])
    run_profile = _avg_defined([run_blocking, processing, anchor])

    prod = production_context or {}
    pass_grade = _safe_prod_value(prod, "sg_ol_pass_block_grade")
    run_grade = _safe_prod_value(prod, "sg_ol_run_block_grade")
    pbe = _safe_prod_value(prod, "sg_ol_pbe")
    pressure_rate = _safe_prod_value(prod, "sg_ol_pressure_allowed_rate")
    versatility = _safe_prod_value(prod, "sg_ol_versatility_count")

    elite_pass = (
        (pass_pro_profile is not None and pass_pro_profile >= 78.0)
        or (pass_grade is not None and pass_grade >= 80.0 and pbe is not None and pbe >= 98.0)
    )
    strong_run = (run_profile is not None and run_profile >= 75.0) or (run_grade is not None and run_grade >= 76.0)
    efficient_pass = (pressure_rate is not None and pressure_rate <= 0.03) or (pbe is not None and pbe >= 98.7)
    mover = athletic_score >= 84.0

    if elite_pass and mover and efficient_pass:
        return ("Blindside pass-pro translator", "Wide-zone / vertical-set tackle playbook")
    if elite_pass and strong_run:
        return ("Balanced starting tackle", "Zone-power hybrid tackle menu")
    if strong_run and mover:
        return ("Movement tackle with run-game lift", "Wide-zone / pin-pull tackle fit")
    if strong_run:
        return ("Power-side right tackle profile", "Gap / duo front-side tackle fit")
    if versatility is not None and versatility >= 3:
        return ("Swing tackle with lineup resilience", "Zone-power hybrid depth package")
    return ("Starting tackle with pass-pro floor", "Zone-power hybrid tackle menu")


def _infer_iol_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    leverage = _safe_trait_value(film_subtraits, "leverage")
    anchor = _safe_trait_value(film_subtraits, "anchor")
    hand_usage = _safe_trait_value(film_subtraits, "hand_usage")
    processing = _safe_trait_value(film_subtraits, "processing")
    lateral = _safe_trait_value(film_subtraits, "lateral_agility")
    run_blocking = _safe_trait_value(film_subtraits, "run_blocking")

    pass_pro_profile = _avg_defined([anchor, hand_usage, processing])
    movement_profile = _avg_defined([lateral, processing, run_blocking])
    power_profile = _avg_defined([leverage, anchor, run_blocking])

    prod = production_context or {}
    pass_grade = _safe_prod_value(prod, "sg_ol_pass_block_grade")
    run_grade = _safe_prod_value(prod, "sg_ol_run_block_grade")
    pbe = _safe_prod_value(prod, "sg_ol_pbe")
    pressure_rate = _safe_prod_value(prod, "sg_ol_pressure_allowed_rate")
    versatility = _safe_prod_value(prod, "sg_ol_versatility_count")

    elite_pass = (
        (pass_pro_profile is not None and pass_pro_profile >= 77.0)
        or (pass_grade is not None and pass_grade >= 80.0 and pbe is not None and pbe >= 99.0)
    )
    low_pressure = pressure_rate is not None and pressure_rate <= 0.02
    movement = (movement_profile is not None and movement_profile >= 75.0) or athletic_score >= 80.0
    power = (power_profile is not None and power_profile >= 75.0) or (run_grade is not None and run_grade >= 77.0)

    if elite_pass and low_pressure and movement:
        return ("Pocket-control interior starter", "Inside-zone / dropback protection core")
    if power and movement:
        return ("Zone-to-gap interior mover", "Inside-zone / duo combo-climb fit")
    if power:
        return ("Power-drive guard profile", "Gap / duo interior displacement menu")
    if versatility is not None and versatility >= 4:
        return ("Multi-spot interior stabilizer", "Inside-zone / hybrid interior depth")
    return ("Starter with interior pocket-control value", "Inside-zone / gap hybrid")


def _infer_edge_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    get_off = _safe_trait_value(film_subtraits, "get_off")
    bend = _safe_trait_value(film_subtraits, "bend")
    hand_usage = _safe_trait_value(film_subtraits, "hand_usage")
    rush_plan = _safe_trait_value(film_subtraits, "rush_plan")
    counter_moves = _safe_trait_value(film_subtraits, "counter_moves")
    run_defense = _safe_trait_value(film_subtraits, "run_defense")

    speed_rush_profile = _avg_defined([get_off, bend, counter_moves])
    power_profile = _avg_defined([hand_usage, rush_plan, run_defense])

    prod = production_context or {}
    rush_grade = _safe_prod_value(prod, "sg_dl_pass_rush_grade")
    true_win = _safe_prod_value(prod, "sg_dl_true_pass_set_win_rate")
    true_prp = _safe_prod_value(prod, "sg_dl_true_pass_set_prp")
    run_grade = _safe_prod_value(prod, "sg_front_run_def_grade")
    stop_pct = _safe_prod_value(prod, "sg_front_stop_percent")
    pressures = _safe_prod_value(prod, "sg_dl_total_pressures", "sg_def_total_pressures")

    explosive = athletic_score >= 87.0
    clean_pocket_rusher = (
        (speed_rush_profile is not None and speed_rush_profile >= 77.0)
        or (true_win is not None and true_win >= 17.0 and true_prp is not None and true_prp >= 9.0)
        or (rush_grade is not None and rush_grade >= 81.0 and true_win is not None and true_win >= 15.0)
    )
    three_down = (
        (run_grade is not None and run_grade >= 74.0 and stop_pct is not None and stop_pct >= 7.0)
        or (power_profile is not None and power_profile >= 75.0)
        or (run_grade is not None and run_grade >= 78.0)
    )
    high_volume_pressure = pressures is not None and pressures >= 26.0
    rush_driver = (
        (rush_grade is not None and rush_grade >= 78.0)
        or (true_prp is not None and true_prp >= 10.5)
        or (pressures is not None and pressures >= 24.0)
    )

    if clean_pocket_rusher and explosive and high_volume_pressure:
        return ("Wide-alignment speed pressure creator", "Multiple-front upfield rush plan")
    if clean_pocket_rusher and (three_down or rush_driver):
        return ("Three-down pressure creator", "Multiple-front every-down edge role")
    if three_down and (run_grade is not None and run_grade >= 77.0):
        return ("Power edge with base-down value", "Odd-even front edge-setting role")
    return ("Rotational rush specialist", "Sub-package designated rush lane")


def _infer_dt_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    get_off = _safe_trait_value(film_subtraits, "get_off")
    power = _safe_trait_value(film_subtraits, "power")
    hand_usage = _safe_trait_value(film_subtraits, "hand_usage")
    leverage = _safe_trait_value(film_subtraits, "leverage")
    pass_rush = _safe_trait_value(film_subtraits, "pass_rush")
    run_defense = _safe_trait_value(film_subtraits, "run_defense")

    penetration_profile = _avg_defined([get_off, pass_rush, hand_usage])
    anchor_profile = _avg_defined([power, leverage, run_defense])

    prod = production_context or {}
    rush_grade = _safe_prod_value(prod, "sg_dl_pass_rush_grade")
    true_win = _safe_prod_value(prod, "sg_dl_true_pass_set_win_rate")
    true_prp = _safe_prod_value(prod, "sg_dl_true_pass_set_prp")
    run_grade = _safe_prod_value(prod, "sg_front_run_def_grade")
    stop_pct = _safe_prod_value(prod, "sg_front_stop_percent")

    explosive = athletic_score >= 82.0
    strong_anchor = (
        (anchor_profile is not None and anchor_profile >= 76.0)
        or (run_grade is not None and run_grade >= 80.0 and stop_pct is not None and stop_pct >= 8.5)
    )
    interior_rush = (
        (penetration_profile is not None and penetration_profile >= 74.0)
        or (rush_grade is not None and rush_grade >= 70.0 and true_win is not None and true_win >= 10.0)
        or (rush_grade is not None and rush_grade >= 74.0 and explosive)
        or (true_win is not None and true_win >= 14.0 and explosive)
        or (true_prp is not None and true_prp >= 7.0 and rush_grade is not None and rush_grade >= 68.0)
    )

    if interior_rush and explosive and not strong_anchor:
        return ("One-gap interior disruptor", "Upfield one-gap attack front")
    if strong_anchor and interior_rush:
        return ("Three-tech disruptor with anchor", "Attack front with base-down sturdiness")
    if strong_anchor:
        return ("Double-team absorbing anchor", "Odd-front shade / early-down interior fit")
    return ("Early-down anchor with interior rush upside", "One-gap attack interior rotation")


def _infer_cb_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    press = _safe_trait_value(film_subtraits, "press")
    footwork = _safe_trait_value(film_subtraits, "footwork")
    recovery_speed = _safe_trait_value(film_subtraits, "recovery_speed")
    processing = _safe_trait_value(film_subtraits, "processing")
    ball_skills = _safe_trait_value(film_subtraits, "ball_skills")
    tackling = _safe_trait_value(film_subtraits, "tackling")

    press_profile = _avg_defined([press, recovery_speed, ball_skills])
    off_profile = _avg_defined([processing, footwork, ball_skills])

    prod = production_context or {}
    cov_grade = _safe_prod_value(prod, "sg_cov_grade")
    man_grade = _safe_prod_value(prod, "sg_cov_man_grade")
    zone_grade = _safe_prod_value(prod, "sg_cov_zone_grade")
    fir = _safe_prod_value(prod, "sg_cov_forced_incompletion_rate")
    slot_snaps = _safe_prod_value(prod, "sg_slot_cov_snaps")
    qbr = _safe_prod_value(prod, "sg_cov_qb_rating_against")
    run_grade = _safe_prod_value(prod, "sg_def_run_grade")
    snaps_per_target = _safe_prod_value(prod, "sg_cov_snaps_per_target")

    long_frame = height_in >= 72
    explosive = athletic_score >= 87.0
    strong_press = (press_profile is not None and press_profile >= 76.0) or (man_grade is not None and man_grade >= 78.0)
    strong_zone = (off_profile is not None and off_profile >= 75.0) or (zone_grade is not None and zone_grade >= 76.0)
    slot_usage = slot_snaps is not None and slot_snaps >= 120.0
    ball_disruption = (fir is not None and fir >= 0.18) or (qbr is not None and qbr <= 70.0)
    elite_coverage = (
        (cov_grade is not None and cov_grade >= 83.0)
        and (
            (snaps_per_target is not None and snaps_per_target >= 8.0)
            or (qbr is not None and qbr <= 60.0)
            or (fir is not None and fir >= 0.22)
        )
    )
    strong_outside_coverage = (
        cov_grade is not None
        and cov_grade >= 80.0
        and man_grade is not None
        and man_grade >= 76.0
        and (
            (snaps_per_target is not None and snaps_per_target >= 8.5)
            or (qbr is not None and qbr <= 55.0)
            or (fir is not None and fir >= 0.20)
        )
    )
    true_slot_corner = slot_snaps is not None and slot_snaps >= 180.0
    zone_tilt = (
        zone_grade is not None
        and man_grade is not None
        and zone_grade >= 77.0
        and zone_grade >= man_grade + 4.0
    )

    if true_slot_corner and strong_zone and (ball_disruption or (cov_grade is not None and cov_grade >= 80.0)):
        return ("Nickel matchup corner", "Match-zone / big-nickel coverage family")
    if elite_coverage and strong_press and strong_zone:
        return ("Outside matchup corner", "Press-match / quarters travel-capable fit")
    if strong_outside_coverage and not slot_usage:
        return ("Outside matchup corner", "Press-match / quarters travel-capable fit")
    if strong_press and long_frame and explosive:
        return ("Press-man outside corner", "Press-match outside corner framework")
    if strong_zone and ball_disruption and (zone_tilt or not strong_press):
        return ("Off-zone ballhawk corner", "Pattern-match zone / off-alignment corner fit")
    if true_slot_corner and run_grade is not None and run_grade >= 80.0 and (cov_grade is None or cov_grade < 80.0):
        return ("Slot-support corner", "Nickel fit with run-support stress")
    return ("Outside starter with matchup flexibility", "Press-match / quarters hybrid")


def _infer_s_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str]:
    processing = _safe_trait_value(film_subtraits, "processing")
    range_score = _safe_trait_value(film_subtraits, "range")
    man_coverage = _safe_trait_value(film_subtraits, "man_coverage")
    tackling = _safe_trait_value(film_subtraits, "tackling")
    angles = _safe_trait_value(film_subtraits, "angles")
    communication = _safe_trait_value(film_subtraits, "communication")

    deep_profile = _avg_defined([processing, range_score, angles])
    box_profile = _avg_defined([tackling, processing, communication])
    coverage_profile = _avg_defined([man_coverage, range_score, processing])

    prod = production_context or {}
    cov_grade = _safe_prod_value(prod, "sg_cov_grade")
    man_grade = _safe_prod_value(prod, "sg_cov_man_grade")
    zone_grade = _safe_prod_value(prod, "sg_cov_zone_grade")
    run_grade = _safe_prod_value(prod, "sg_def_run_grade")
    pressures = _safe_prod_value(prod, "sg_def_total_pressures")
    slot_snaps = _safe_prod_value(prod, "sg_slot_cov_snaps")
    tfl = _safe_prod_value(prod, "sg_def_tackles_for_loss")
    snaps_per_target = _safe_prod_value(prod, "sg_cov_snaps_per_target")
    qbr = _safe_prod_value(prod, "sg_cov_qb_rating_against")

    explosive = athletic_score >= 85.0
    slot_usage = slot_snaps is not None and slot_snaps >= 90.0
    deep_safety = (
        (deep_profile is not None and deep_profile >= 76.0)
        or (zone_grade is not None and zone_grade >= 79.0)
        or (
            cov_grade is not None
            and cov_grade >= 88.0
            and snaps_per_target is not None
            and snaps_per_target >= 10.0
        )
    )
    matchup_safety = (coverage_profile is not None and coverage_profile >= 75.0) or (man_grade is not None and man_grade >= 76.0)
    box_safety = (box_profile is not None and box_profile >= 75.0) or (run_grade is not None and run_grade >= 76.0)
    pressure_role = (pressures is not None and pressures >= 4.0) or (tfl is not None and tfl >= 4.0)
    complete_coverage = (
        cov_grade is not None
        and cov_grade >= 86.0
        and man_grade is not None
        and man_grade >= 78.0
        and zone_grade is not None
        and zone_grade >= 78.0
    )
    slot_matchup_star = (
        slot_usage
        and cov_grade is not None
        and cov_grade >= 88.0
        and man_grade is not None
        and man_grade >= 88.0
        and run_grade is not None
        and run_grade >= 80.0
    )
    matchup_safety_star = (
        matchup_safety
        and cov_grade is not None
        and cov_grade >= 86.0
        and man_grade is not None
        and man_grade >= 80.0
        and (
            slot_usage
            or (qbr is not None and qbr <= 62.0)
            or (snaps_per_target is not None and snaps_per_target >= 9.0)
        )
    )
    deep_eraser = (
        deep_safety
        and cov_grade is not None
        and cov_grade >= 89.0
        and zone_grade is not None
        and zone_grade >= 87.0
        and snaps_per_target is not None
        and snaps_per_target >= 12.0
    )
    coverage_eraser = (
        complete_coverage
        and cov_grade is not None
        and cov_grade >= 89.0
        and (
            (snaps_per_target is not None and snaps_per_target >= 10.0)
            or (qbr is not None and qbr <= 60.0)
        )
    )

    if slot_matchup_star or (slot_usage and matchup_safety and box_safety and not (deep_safety and complete_coverage)):
        return ("Matchup safety", "Big-nickel / split-safety coverage family")
    if matchup_safety_star and not pressure_role:
        return ("Matchup safety", "TE / slot matchup coverage family")
    if coverage_eraser:
        return ("Coverage eraser safety", "Split-safety eraser / disguise-heavy family")
    if deep_eraser or (deep_safety and explosive):
        return ("Range-first split safety", "Quarters / middle-field-open range fit")
    if box_safety and pressure_role:
        return ("Robber/box safety with pressure value", "Sim-pressure / robber rotation family")
    if matchup_safety or (qbr is not None and qbr <= 65.0):
        return ("Coverage-adjustment back-end starter", "Split-safety multiplicity")
    return ("Coverage-adjustment back-end starter", "Split-safety multiplicity")


def _infer_qb_role_and_scheme(
    *,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
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
    prod = production_context or {}
    qb_epa = _safe_prod_value(prod, "cfb_qb_epa_per_play", "qb_epa_per_play", "qb_epa_per_pl")
    qb_pressure_signal = _safe_prod_value(prod, "cfb_qb_pressure_signal")
    qb_pass_td = _safe_prod_value(prod, "cfb_qb_pass_td", "qb_pass_td")
    qb_int = _safe_prod_value(prod, "cfb_qb_pass_int", "qb_pass_int")
    qb_int_rate = _safe_prod_value(prod, "cfb_qb_int_rate", "qb_int_rate")

    elite_athlete = athletic_score >= 74.0
    plus_athlete = athletic_score >= 70.0
    good_athlete = athletic_score >= 64.0
    limited_athlete = athletic_score < 58.0
    prototype_frame = height_in >= 75 and weight_lb >= 220
    secure_ball = (qb_int_rate is not None and qb_int_rate <= 0.018) or (qb_int is not None and qb_int <= 6.0)
    turnover_prone = (qb_int_rate is not None and qb_int_rate >= 0.027) or (qb_int is not None and qb_int >= 10.0)
    efficient_passer = qb_epa is not None and qb_epa >= 0.34
    solid_passer = qb_epa is not None and qb_epa >= 0.26
    elite_under_pressure = qb_pressure_signal is not None and qb_pressure_signal >= 84.0
    stable_under_pressure = qb_pressure_signal is not None and qb_pressure_signal >= 72.0
    shaky_under_pressure = qb_pressure_signal is not None and qb_pressure_signal <= 50.0

    if (
        distributor_profile is not None
        and distributor_profile >= 84.0
        and secure_ball
        and (efficient_passer or elite_under_pressure)
    ):
        if creation is not None and creation >= 79.0 and good_athlete:
            return ("Franchise dual-threat creator", "Spread/RPO movement-launch attack")
        return ("Franchise field general", "West Coast spread / full-field progression")
    if (
        creator_profile is not None
        and creator_profile >= 81.0
        and plus_athlete
        and not turnover_prone
        and stable_under_pressure
    ):
        return ("Dual-threat pressure creator", "Spread/RPO movement-launch attack")
    if (
        pure_arm_profile is not None
        and pure_arm_profile >= 80.0
        and (arm_talent or 0.0) >= 82.0
        and (efficient_passer or solid_passer)
        and not turnover_prone
    ):
        return ("Vertical pocket aggressor", "Play-action vertical shot offense")
    if (
        structure_profile is not None
        and structure_profile >= 80.0
        and secure_ball
        and stable_under_pressure
    ):
        return ("High-end structure distributor", "West Coast spread / quick-game progression")
    if (
        structure_profile is not None
        and structure_profile >= 76.0
        and secure_ball
        and (stable_under_pressure or solid_passer)
    ):
        return ("Timing-and-rhythm distributor", "Quick-game spread / progression menu")
    if (
        creator_profile is not None
        and creator_profile >= 76.0
        and good_athlete
        and not turnover_prone
    ):
        return ("Movement-passer starter", "Boot/play-action spread")
    if (
        structure_profile is not None
        and structure_profile >= 74.0
        and prototype_frame
        and secure_ball
    ):
        return ("Play-action drive starter", "Under-center play-action progression")
    if turnover_prone and shaky_under_pressure:
        return ("High-variance developmental passer", "Half-field progression + shot-play menu")
    if efficient_passer and secure_ball and stable_under_pressure:
        if plus_athlete:
            return ("Dynamic spread starter", "Spread/RPO movement-launch attack")
        return ("High-end structure distributor", "West Coast spread / full-field progression")
    if solid_passer and secure_ball and not turnover_prone:
        if good_athlete:
            return ("Movement-passer starter", "Boot/play-action spread")
        if prototype_frame:
            return ("Play-action drive starter", "Under-center play-action progression")
        return ("Timing-and-rhythm distributor", "Quick-game spread / progression menu")
    if plus_athlete and not turnover_prone:
        return ("Developmental live-arm creator", "Movement-passer package")
    if elite_athlete and prototype_frame and (creation or 0.0) >= 74.0:
        return ("Developmental dual-threat upside QB", "Spread/RPO vertical-play-action")
    if creator_profile is not None and creator_profile >= 72.0 and good_athlete:
        return ("Developmental live-arm creator", "Movement-passer package")
    if limited_athlete and secure_ball:
        return ("Low-variance reserve distributor", "Quick-game backup structure")
    return ("Starter-caliber distributor profile", "Timing-spread distributor framework")


def _infer_role_and_scheme(
    *,
    position: str,
    height_in: int,
    weight_lb: int,
    athletic_score: float,
    film_subtraits: Mapping[str, float],
    production_context: Mapping[str, object] | None = None,
) -> tuple[str, str, str]:
    if position == "QB":
        role, scheme = _infer_qb_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "RB":
        role, scheme = _infer_rb_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context,
        )
        return role, scheme, ""
    if position == "WR":
        role, scheme = _infer_wr_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "TE":
        role, scheme = _infer_te_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "OT":
        role, scheme = _infer_ot_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "IOL":
        role, scheme = _infer_iol_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "EDGE":
        role, scheme = _infer_edge_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "DT":
        role, scheme = _infer_dt_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "LB":
        return _infer_lb_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
    if position == "CB":
        role, scheme = _infer_cb_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    if position == "S":
        role, scheme = _infer_s_role_and_scheme(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=athletic_score,
            film_subtraits=film_subtraits,
            production_context=production_context or {},
        )
        return role, scheme, ""
    return (
        ROLE_BY_POS.get(position, "Depth and developmental value"),
        SCHEME_FIT_BY_POS.get(position, "multiple"),
        "",
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
    lb_trait_context_score: float | None = None

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

    if position == "LB":
        lb_archetype_seed = _infer_lb_archetype(
            height_in=height_in,
            weight_lb=weight_lb,
            athletic_score=ath,
            film_subtraits=film_inputs,
            production_context=production_context or {},
        )
        lb_trait_context_score = _lb_trait_context_score(
            archetype=lb_archetype_seed,
            athletic_score=ath,
            production_context=production_context or {},
        )
        if lb_trait_context_score is not None:
            if film_eval["film_trait_score"] is not None:
                trait = (0.85 * trait) + (0.15 * lb_trait_context_score)
            else:
                trait = (0.45 * trait) + (0.55 * lb_trait_context_score)

    best_role, best_scheme_fit, lb_archetype = _infer_role_and_scheme(
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
        "lb_trait_context_score": round(lb_trait_context_score, 2) if lb_trait_context_score is not None else "",
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
        "lb_archetype": lb_archetype,
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
