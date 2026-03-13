#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.combine_loader import load_combine_results
from src.ingest.athletic_profile_loader import evaluate_athletic_profile, load_historical_athletic_context
from src.ingest.cfb_production_loader import load_cfb_production_signals
from src.ingest.draft_age_loader import load_draft_age_signals
from src.ingest.early_declare_loader import load_early_declare_signals
from src.ingest.consensus_board_loader import load_consensus_board_signals
from src.ingest.eligibility_loader import load_returning_to_school
from src.ingest.eligibility_loader import load_declared_underclassmen, is_senior_class, load_already_in_nfl_exclusions
from src.ingest.historical_combine_loader import build_combine_merge_key, find_historical_combine_comps, load_historical_combine_profiles
from src.ingest.espn_loader import load_espn_player_signals
from src.ingest.analyst_language_loader import load_analyst_linguistic_signals
from src.ingest.kiper_loader import load_kiper_structured_signals
from src.ingest.tdn_ringer_loader import load_tdn_ringer_signals
from src.ingest.film_traits_loader import load_film_trait_rows
from src.ingest.prebuild_validation import format_prebuild_report_md, run_prebuild_checks
from src.ingest.playerprofiler_loader import load_playerprofiler_signals
from src.ingest.mockdraftable_loader import load_mockdraftable_baselines
from src.ingest.ras_benchmarks_loader import load_ras_benchmarks
from src.ingest.roi_prior_loader import load_position_roi_priors, pick_band_from_rank
from src.ingest.production_percentile_comps_loader import (
    POSITION_BASELINES as PROD_POSITION_BASELINES,
    REVERSE_DIRECTION_METRICS as PROD_REVERSE_METRICS,
    find_production_percentile_comps,
    load_production_percentile_pack,
)
from src.ingest.rankings_loader import (
    analyst_aggregate_score,
    canonical_player_name,
    load_analyst_rows,
    load_external_big_board,
    load_external_big_board_rows,
    normalize_pos,
)
from src.modeling.comp_model import assign_comp
from src.modeling.calibration import load_calibration_config, calibrated_success_probability
from src.modeling.grading import grade_player, scouting_note
from src.modeling.mockdraftable_features import compute_mockdraftable_composite
from src.modeling.ras import (
    estimate_ras,
    historical_ras_comparison,
    ras_from_combine_profile,
    ras_percentile,
    ras_tier,
)
from src.modeling.team_fit import best_team_fit, reset_team_fit_state
from src.schemas import parse_height_to_inches, round_from_grade

PROCESSED = ROOT / "data" / "processed"
OUTPUTS = ROOT / "data" / "outputs"
SOURCE_RELIABILITY_PATH = ROOT / "data" / "sources" / "manual" / "source_reliability_weights_2026.csv"
SOURCE_RELIABILITY_POS_YEAR_PATH = (
    ROOT / "data" / "sources" / "manual" / "source_reliability_by_pos_year_2016_2025.csv"
)
SCOUTING_GLOSSARY_PATH = ROOT / "data" / "sources" / "manual" / "scouting_glossary_2026.csv"
SCOUTING_LANGUAGE_INPUTS_PATH = ROOT / "data" / "sources" / "manual" / "scouting_language_inputs_2026.csv"
NFL_OFFICIAL_PROSPECT_PATH = ROOT / "data" / "sources" / "manual" / "nfl_combine_invites_2026.csv"
CURRENT_DRAFT_YEAR = int(os.getenv("DRAFT_YEAR", "2026"))

POSITION_DEFAULT_FRAME = {
    "QB": (76, 220),
    "RB": (71, 210),
    "WR": (73, 200),
    "TE": (77, 250),
    "OT": (78, 315),
    "IOL": (75, 310),
    "EDGE": (76, 260),
    "DT": (75, 305),
    "LB": (74, 235),
    "CB": (71, 190),
    "S": (72, 205),
}

ALLOWED_POSITIONS = set(POSITION_DEFAULT_FRAME.keys())
ENABLE_SOURCE_UNIVERSE_EXPANSION = True
ENFORCE_2026_EVIDENCE_UNIVERSE = False
ENFORCE_NFL_OFFICIAL_UNIVERSE = str(
    os.getenv("ENFORCE_NFL_OFFICIAL_UNIVERSE", "1")
).strip().lower() in {"1", "true", "yes", "y"}
ENABLE_FILM_WEIGHTING = False
PRODUCTION_SIGNAL_NEUTRAL = float(os.getenv("PRODUCTION_SIGNAL_NEUTRAL", "70.0"))
PRODUCTION_SIGNAL_MULTIPLIER = float(os.getenv("PRODUCTION_SIGNAL_MULTIPLIER", "0.82"))
PRODUCTION_SIGNAL_MAX_DELTA = float(os.getenv("PRODUCTION_SIGNAL_MAX_DELTA", "7.0"))
PRODUCTION_SIGNAL_QB_MULTIPLIER = float(os.getenv("PRODUCTION_SIGNAL_QB_MULTIPLIER", "0.74"))
PRODUCTION_SIGNAL_QB_MAX_DELTA = float(os.getenv("PRODUCTION_SIGNAL_QB_MAX_DELTA", "6.0"))
PRODUCTION_SIGNAL_FRONT7_MULTIPLIER = float(
    os.getenv("PRODUCTION_SIGNAL_FRONT7_MULTIPLIER", "0.64")
)
PRODUCTION_SIGNAL_FRONT7_MAX_UP_DELTA = float(
    os.getenv("PRODUCTION_SIGNAL_FRONT7_MAX_UP_DELTA", "3.4")
)
PRODUCTION_SIGNAL_FRONT7_MAX_DOWN_DELTA = float(
    os.getenv("PRODUCTION_SIGNAL_FRONT7_MAX_DOWN_DELTA", "9.0")
)
FRONT7_INFLATION_BRAKE_MAX = float(os.getenv("FRONT7_INFLATION_BRAKE_MAX", "2.10"))
NICKEL_CB_INFLATION_BRAKE_MAX = float(os.getenv("NICKEL_CB_INFLATION_BRAKE_MAX", "1.55"))
TRAIT_PROXY_ANCHOR_MAX_WEIGHT = float(os.getenv("TRAIT_PROXY_ANCHOR_MAX_WEIGHT", "0.70"))
TRAIT_PROXY_ANCHOR_MID_WEIGHT = float(os.getenv("TRAIT_PROXY_ANCHOR_MID_WEIGHT", "0.34"))
TRAIT_PROXY_ANCHOR_MIN_WEIGHT = float(os.getenv("TRAIT_PROXY_ANCHOR_MIN_WEIGHT", "0.18"))
TRAIT_PROXY_ANCHOR_LOW_COVERAGE = float(os.getenv("TRAIT_PROXY_ANCHOR_LOW_COVERAGE", "90.0"))
TRAIT_PROXY_ANCHOR_MID_COVERAGE = float(os.getenv("TRAIT_PROXY_ANCHOR_MID_COVERAGE", "180.0"))
AUTO_WEEKLY_STABILITY_CHECK = str(
    os.getenv("AUTO_WEEKLY_STABILITY_CHECK", "1")
).strip().lower() in {"1", "true", "yes", "y"}
AUTO_DELTA_AUDIT_AFTER_BUILD = str(
    os.getenv("AUTO_DELTA_AUDIT_AFTER_BUILD", "1")
).strip().lower() in {"1", "true", "yes", "y"}
GRADE_CALIBRATED_BLEND = float(os.getenv("GRADE_CALIBRATED_BLEND", "0.78"))
GRADE_PRIOR_BLEND = float(os.getenv("GRADE_PRIOR_BLEND", "0.22"))
RANK_UNCERTAINTY_DRAG_WEIGHT = float(os.getenv("RANK_UNCERTAINTY_DRAG_WEIGHT", "0.022"))
RANK_PRIOR_OVERHANG_DRAG_WEIGHT = float(os.getenv("RANK_PRIOR_OVERHANG_DRAG_WEIGHT", "0.11"))
RANK_PRIOR_OVERHANG_SCALE = float(os.getenv("RANK_PRIOR_OVERHANG_SCALE", "5.0"))
RANK_CONSENSUS_REALIGN_MAX = float(os.getenv("RANK_CONSENSUS_REALIGN_MAX", "1.55"))
RANK_CONSENSUS_REALIGN_DEADBAND = float(os.getenv("RANK_CONSENSUS_REALIGN_DEADBAND", "20.0"))
RANK_CONSENSUS_REALIGN_SCALE = float(os.getenv("RANK_CONSENSUS_REALIGN_SCALE", "120.0"))
RANK_DRAG_CAP_BLUECHIP = float(os.getenv("RANK_DRAG_CAP_BLUECHIP", "0.30"))
RANK_DRAG_CAP_HIGHGRADE = float(os.getenv("RANK_DRAG_CAP_HIGHGRADE", "0.58"))
BLUECHIP_RANK_PROTECTION_ENABLED = str(
    os.getenv("BLUECHIP_RANK_PROTECTION_ENABLED", "1")
).strip().lower() in {"1", "true", "yes", "y"}
BLUECHIP_RANK_PROTECTION_MAX = float(os.getenv("BLUECHIP_RANK_PROTECTION_MAX", "1.15"))
TOP50_EVIDENCE_APPLY_TOP_N = int(os.getenv("TOP50_EVIDENCE_APPLY_TOP_N", "50"))
TOP50_EVIDENCE_MIN_SIGNALS = int(os.getenv("TOP50_EVIDENCE_MIN_SIGNALS", "2"))
TOP50_EVIDENCE_BRAKE_BASE = float(os.getenv("TOP50_EVIDENCE_BRAKE_BASE", "1.05"))
TOP50_EVIDENCE_BRAKE_PER_MISSING = float(os.getenv("TOP50_EVIDENCE_BRAKE_PER_MISSING", "0.75"))
TOP50_EVIDENCE_BRAKE_MAX = float(os.getenv("TOP50_EVIDENCE_BRAKE_MAX", "2.4"))
TOP50_EVIDENCE_REBALANCE_PASSES = int(os.getenv("TOP50_EVIDENCE_REBALANCE_PASSES", "5"))
EXTREME_RANK_DELTA_MIN_RISE = int(os.getenv("EXTREME_RANK_DELTA_MIN_RISE", "55"))
EXTREME_RANK_DELTA_MAX_ALLOWED = int(os.getenv("EXTREME_RANK_DELTA_MAX_ALLOWED", "6"))
EXTREME_RANK_DELTA_FAIL_ON_TRIGGER = str(
    os.getenv("EXTREME_RANK_DELTA_FAIL_ON_TRIGGER", "1")
).strip().lower() in {"1", "true", "yes", "y"}
UPSIDE_EVIDENCE_GUARDRAIL_ACTIVE_GRADE = float(
    os.getenv("UPSIDE_EVIDENCE_GUARDRAIL_ACTIVE_GRADE", "82.0")
)
UPSIDE_EVIDENCE_GUARDRAIL_PENALTY_PER_MISSING = float(
    os.getenv("UPSIDE_EVIDENCE_GUARDRAIL_PENALTY_PER_MISSING", "0.28")
)
UPSIDE_EVIDENCE_GUARDRAIL_MAX_PENALTY = float(
    os.getenv("UPSIDE_EVIDENCE_GUARDRAIL_MAX_PENALTY", "1.1")
)
ALLOW_SINGLE_YEAR_PRODUCTION_KNN = str(
    os.getenv("ALLOW_SINGLE_YEAR_PRODUCTION_KNN", "0")
).strip().lower() in {"1", "true", "yes", "y"}
ENABLE_DRAFT_AGE_SCORING = str(os.getenv("ENABLE_DRAFT_AGE_SCORING", "0")).strip().lower() in {"1", "true", "yes", "y"}
ENABLE_EARLY_DECLARE_SCORING = str(os.getenv("ENABLE_EARLY_DECLARE_SCORING", "0")).strip().lower() in {"1", "true", "yes", "y"}
CFB_PROXY_FALLBACK_HEAVY_MAX_COVERAGE = int(os.getenv("CFB_PROXY_FALLBACK_HEAVY_MAX_COVERAGE", "2"))
CFB_PROXY_FALLBACK_HEAVY_MIN_FALLBACKS = int(os.getenv("CFB_PROXY_FALLBACK_HEAVY_MIN_FALLBACKS", "1"))
CFB_PROXY_FALLBACK_HEAVY_MAX_RELIABILITY = float(os.getenv("CFB_PROXY_FALLBACK_HEAVY_MAX_RELIABILITY", "0.45"))
CFB_PROXY_FALLBACK_FAIL_ON_HEAVY = str(
    os.getenv("CFB_PROXY_FALLBACK_FAIL_ON_HEAVY", "0")
).strip().lower() in {"1", "true", "yes", "y"}
FAIL_ON_POSTBUILD_INELIGIBLE = str(
    os.getenv("FAIL_ON_POSTBUILD_INELIGIBLE", "1")
).strip().lower() in {"1", "true", "yes", "y"}
ATHLETIC_MISSING_RISK_WEIGHT = float(os.getenv("ATHLETIC_MISSING_RISK_WEIGHT", "0.12"))
ATHLETIC_VARIANCE_RISK_WEIGHT = float(os.getenv("ATHLETIC_VARIANCE_RISK_WEIGHT", "0.08"))
TESTING_MISSING_SIGNAL_WEIGHT_QB = float(os.getenv("TESTING_MISSING_SIGNAL_WEIGHT_QB", "0.35"))
TESTING_MISSING_SIGNAL_WEIGHT_NON_QB = float(os.getenv("TESTING_MISSING_SIGNAL_WEIGHT_NON_QB", "0.10"))
TESTING_MISSING_SIGNAL_WEIGHT_PENDING_QB = float(os.getenv("TESTING_MISSING_SIGNAL_WEIGHT_PENDING_QB", "0.08"))
TESTING_MISSING_SIGNAL_WEIGHT_PENDING_NON_QB = float(os.getenv("TESTING_MISSING_SIGNAL_WEIGHT_PENDING_NON_QB", "0.03"))
TESTING_MISSING_SIGNAL_WEIGHT_DNP_QB = float(os.getenv("TESTING_MISSING_SIGNAL_WEIGHT_DNP_QB", "0.20"))
TESTING_MISSING_SIGNAL_WEIGHT_DNP_NON_QB = float(os.getenv("TESTING_MISSING_SIGNAL_WEIGHT_DNP_NON_QB", "0.08"))
EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT = float(
    os.getenv("EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT", "0.35")
)
EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT_PENDING = float(
    os.getenv("EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT_PENDING", "0.18")
)
EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT_DNP = float(
    os.getenv("EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT_DNP", "0.45")
)
ATHLETIC_MISSING_RISK_FACTOR_PENDING = float(os.getenv("ATHLETIC_MISSING_RISK_FACTOR_PENDING", "0.25"))
ATHLETIC_MISSING_RISK_FACTOR_DNP = float(os.getenv("ATHLETIC_MISSING_RISK_FACTOR_DNP", "0.70"))

POSITION_VALUE_ADJUSTMENT = {
    "QB": 0.35,
    "OT": 0.45,
    "EDGE": 0.25,
    "CB": 0.20,
    "WR": 0.15,
    "DT": 0.10,
    "S": 0.10,
    "LB": 0.05,
    "TE": -0.15,
    "IOL": 0.05,
    "RB": -0.80,
}

ROUND_LABEL_ORDER = [
    "Round 1",
    "Round 1-2",
    "Round 2-3",
    "Round 3-4",
    "Round 4-5",
    "Round 5-6",
    "Round 6-7",
    "UDFA",
]
ROUND_LABEL_INDEX = {label: idx for idx, label in enumerate(ROUND_LABEL_ORDER)}

# Class-relative rank bands used only for round-value labeling (not scoring).
ROUND_RANK_BANDS = [
    (8, "Round 1"),
    (24, "Round 1-2"),
    (48, "Round 2-3"),
    (80, "Round 3-4"),
    (120, "Round 4-5"),
    (170, "Round 5-6"),
    (230, "Round 6-7"),
]

DEFAULT_SOURCE_RELIABILITY = {
    "seed_rank": {"hit_rate": 0.58, "stability": 0.76, "sample_size": 3000},
    "external_rank": {"hit_rate": 0.61, "stability": 0.82, "sample_size": 1200},
    "analyst_rank": {"hit_rate": 0.57, "stability": 0.70, "sample_size": 1500},
    "consensus_rank": {"hit_rate": 0.64, "stability": 0.86, "sample_size": 1800},
    "kiper_rank": {"hit_rate": 0.62, "stability": 0.78, "sample_size": 900},
    "tdn_rank": {"hit_rate": 0.56, "stability": 0.66, "sample_size": 600},
    "ringer_rank": {"hit_rate": 0.56, "stability": 0.66, "sample_size": 600},
    "bleacher_rank": {"hit_rate": 0.57, "stability": 0.68, "sample_size": 700},
    "atoz_rank": {"hit_rate": 0.54, "stability": 0.62, "sample_size": 500},
    "si_rank": {"hit_rate": 0.53, "stability": 0.61, "sample_size": 400},
    "cbs_rank": {"hit_rate": 0.58, "stability": 0.74, "sample_size": 800},
    "cbs_wilson_rank": {"hit_rate": 0.59, "stability": 0.75, "sample_size": 800},
    "tdn_grade_label": {"hit_rate": 0.55, "stability": 0.64, "sample_size": 450},
}


def _load_source_reliability_weights(
    path: Path = SOURCE_RELIABILITY_PATH,
    pos_year_path: Path = SOURCE_RELIABILITY_POS_YEAR_PATH,
) -> dict:
    """
    Optional manual override for source reliability.
    CSV schema:
      source,hit_rate,stability,sample_size
    Values are clamped into safe ranges to keep weights stable.
    """
    global_map = {k: dict(v) for k, v in DEFAULT_SOURCE_RELIABILITY.items()}
    if not path.exists():
        out = {
            "global": global_map,
            "by_source_pos_year": {},
            "by_source_year": {},
            "meta": {
                "global_keys": len(global_map),
                "pos_year_rows": 0,
                "has_pos_year_table": 0,
            },
        }
        return out

    with path.open() as f:
        for row in csv.DictReader(f):
            source = str(row.get("source", "")).strip()
            if not source:
                continue
            hit_rate = _as_float(row.get("hit_rate"))
            stability = _as_float(row.get("stability"))
            sample_size = _as_float(row.get("sample_size"))
            if hit_rate is None and stability is None and sample_size is None:
                continue
            current = global_map.get(source, {"hit_rate": 0.56, "stability": 0.66, "sample_size": 300})
            if hit_rate is not None:
                current["hit_rate"] = _clamp(float(hit_rate), 0.35, 0.85)
            if stability is not None:
                current["stability"] = _clamp(float(stability), 0.30, 0.95)
            if sample_size is not None:
                current["sample_size"] = max(1, int(sample_size))
            global_map[source] = current

    by_source_pos_year: dict[tuple[str, str], list[dict]] = {}
    by_source_year: dict[str, list[dict]] = {}
    pos_year_rows = 0
    if pos_year_path.exists():
        with pos_year_path.open() as f:
            for row in csv.DictReader(f):
                source = str(row.get("source", "")).strip()
                position = normalize_pos(str(row.get("position", "")).strip())
                year = _as_float(row.get("draft_year"))
                hit_rate = _as_float(row.get("hit_rate"))
                stability = _as_float(row.get("stability"))
                sample_size = _as_float(row.get("sample_size"))
                if not source or year is None or hit_rate is None:
                    continue
                payload = {
                    "source": source,
                    "position": position,
                    "draft_year": int(year),
                    "hit_rate": _clamp(float(hit_rate), 0.35, 0.85),
                    "stability": _clamp(float(stability), 0.30, 0.95) if stability is not None else 0.66,
                    "sample_size": max(1, int(sample_size)) if sample_size is not None else 120,
                }
                by_source_year.setdefault(source, []).append(payload)
                if position:
                    by_source_pos_year.setdefault((source, position), []).append(payload)
                pos_year_rows += 1

    return {
        "global": global_map,
        "by_source_pos_year": by_source_pos_year,
        "by_source_year": by_source_year,
        "meta": {
            "global_keys": len(global_map),
            "pos_year_rows": pos_year_rows,
            "has_pos_year_table": 1 if pos_year_rows > 0 else 0,
        },
    }


def _resolve_source_reliability(
    *,
    reliability_pack: dict,
    source_key: str,
    position: str,
    draft_year: int,
) -> dict:
    global_map = reliability_pack.get("global", {}) if isinstance(reliability_pack, dict) else {}
    by_source_pos_year = (
        reliability_pack.get("by_source_pos_year", {}) if isinstance(reliability_pack, dict) else {}
    )
    by_source_year = reliability_pack.get("by_source_year", {}) if isinstance(reliability_pack, dict) else {}

    base = dict(global_map.get(source_key, {"hit_rate": 0.56, "stability": 0.66, "sample_size": 300}))
    base.setdefault("hit_rate", 0.56)
    base.setdefault("stability", 0.66)
    base.setdefault("sample_size", 300)
    base["_layer"] = "global"
    base["_selected_year"] = ""
    base["_year_distance"] = ""

    pos = normalize_pos(position)
    candidates = list(by_source_pos_year.get((source_key, pos), []))
    layer = "source_pos_year"
    if not candidates:
        candidates = list(by_source_year.get(source_key, []))
        layer = "source_year"
    if not candidates:
        return base

    def _cand_sort_key(c: dict) -> tuple[int, int]:
        y = int(c.get("draft_year", draft_year))
        n = int(c.get("sample_size", 0))
        return (abs(y - draft_year), -n)

    selected = sorted(candidates, key=_cand_sort_key)[0]
    sel_year = int(selected.get("draft_year", draft_year))
    year_distance = abs(sel_year - draft_year)

    # Recency decay + sample-size shrinkage protects against overfitting historical noise.
    recency_weight = _clamp(1.0 - (0.03 * year_distance), 0.70, 1.0)
    sample_size = int(selected.get("sample_size", 120))
    shrink = _clamp(sample_size / float(sample_size + 220), 0.15, 0.75) * recency_weight

    hit_rate = ((1.0 - shrink) * float(base["hit_rate"])) + (shrink * float(selected.get("hit_rate", base["hit_rate"])))
    stability = ((1.0 - shrink) * float(base["stability"])) + (
        shrink * float(selected.get("stability", base["stability"]))
    )

    resolved = {
        "hit_rate": _clamp(hit_rate, 0.35, 0.85),
        "stability": _clamp(stability, 0.30, 0.95),
        "sample_size": max(1, int(sample_size)),
        "_layer": layer,
        "_selected_year": sel_year,
        "_year_distance": year_distance,
        "_shrink": round(shrink, 4),
        "_recency_weight": round(recency_weight, 4),
    }
    return resolved


def _source_reliability_multiplier(reliability: dict | None) -> float:
    if not reliability:
        return 1.0

    hit_rate = _clamp(float(reliability.get("hit_rate", 0.56)), 0.35, 0.85)
    stability = _clamp(float(reliability.get("stability", 0.66)), 0.30, 0.95)
    sample_size = max(1, int(float(reliability.get("sample_size", 300))))

    # Keep multipliers narrow so source weighting is refined, not rewritten.
    hit_mult = 0.88 + ((hit_rate - 0.50) * 1.00)  # ~0.73..1.23 before clamp
    stability_mult = 0.90 + ((stability - 0.60) * 0.55)
    if sample_size >= 1200:
        sample_mult = 1.03
    elif sample_size >= 600:
        sample_mult = 1.00
    elif sample_size >= 300:
        sample_mult = 0.97
    else:
        sample_mult = 0.94

    return round(_clamp(hit_mult * stability_mult * sample_mult, 0.82, 1.20), 3)


def _append_prior_part(
    *,
    parts: list[tuple[float, float]],
    diagnostics: dict,
    source_key: str,
    base_weight: float,
    signal_value: float,
    reliability_pack: dict,
    position: str,
    draft_year: int,
) -> None:
    reliability = _resolve_source_reliability(
        reliability_pack=reliability_pack,
        source_key=source_key,
        position=position,
        draft_year=draft_year,
    )
    multiplier = _source_reliability_multiplier(reliability)
    effective_weight = base_weight * multiplier
    parts.append((effective_weight, signal_value))
    diagnostics[source_key] = {
        "base_weight": round(base_weight, 4),
        "multiplier": round(multiplier, 4),
        "effective_weight": round(effective_weight, 4),
        "layer": reliability.get("_layer", "global"),
        "selected_year": reliability.get("_selected_year", ""),
        "year_distance": reliability.get("_year_distance", ""),
        "shrink": reliability.get("_shrink", ""),
    }


def _confidence_uncertainty_profile(
    *,
    final_grade: float,
    evidence_missing_count: int,
    risk_penalty: float,
    consensus_source_count: int,
    consensus_rank_std: float | None,
    consensus_confidence_factor: float,
    has_calibrated_prob: bool,
    testing_missing_weight: float = 0.0,
    testing_missing_status: str = "reported",
) -> dict:
    evidence_score = _clamp(((4 - evidence_missing_count) / 4.0) * 44.0, 6.0, 44.0)
    source_coverage_score = _clamp((consensus_source_count / 5.0) * 24.0, 2.0, 24.0)
    split_score = 10.0
    if consensus_rank_std is not None:
        split_score = _clamp(10.0 - (consensus_rank_std * 0.32), 0.0, 10.0)
    consensus_score = (source_coverage_score + split_score) * _clamp(consensus_confidence_factor, 0.45, 1.05)
    risk_score = _clamp(20.0 - (risk_penalty * 3.2), 0.0, 20.0)
    calibration_score = 8.0 if has_calibrated_prob else 4.0

    confidence = evidence_score + consensus_score + risk_score + calibration_score
    status = str(testing_missing_status or "").strip().lower()
    testing_confidence_penalty = 0.0
    if testing_missing_weight > 0:
        if status == "pending":
            testing_confidence_penalty = _clamp(0.8 + (testing_missing_weight * 6.0), 0.6, 2.2)
        elif status == "dnp":
            testing_confidence_penalty = _clamp(1.4 + (testing_missing_weight * 7.5), 1.2, 3.6)
        else:
            testing_confidence_penalty = _clamp(1.2 + (testing_missing_weight * 8.0), 1.0, 3.2)
    confidence -= testing_confidence_penalty
    if final_grade >= 82.0 and evidence_missing_count >= 2:
        confidence -= 9.0
    confidence = round(_clamp(confidence, 1.0, 99.0), 2)
    uncertainty = round(100.0 - confidence, 2)

    if final_grade >= 82.0 and evidence_missing_count >= 2:
        variance_flag = "high_variance_thin_evidence"
    elif uncertainty >= 58.0:
        variance_flag = "high_variance"
    elif uncertainty >= 40.0:
        variance_flag = "medium_variance"
    else:
        variance_flag = "low_variance"

    return {
        "confidence_score": confidence,
        "uncertainty_score": uncertainty,
        "variance_flag": variance_flag,
    }


def _round_from_rank_band(consensus_rank: int) -> str:
    for max_rank, label in ROUND_RANK_BANDS:
        if consensus_rank <= max_rank:
            return label
    return "UDFA"


def _blend_round_projection(grade_label: str, rank_label: str, consensus_rank: int) -> str:
    """
    Keep grade as baseline, then allow a capped rank-based uplift.
    This keeps overlap realistic: some Round 2-3 profiles can still go Round 1.
    """
    grade_idx = ROUND_LABEL_INDEX.get(grade_label, len(ROUND_LABEL_ORDER) - 1)
    rank_idx = ROUND_LABEL_INDEX.get(rank_label, len(ROUND_LABEL_ORDER) - 1)

    if rank_idx >= grade_idx:
        return ROUND_LABEL_ORDER[grade_idx]

    if consensus_rank <= 8:
        max_uplift = 2
    elif consensus_rank <= 40:
        max_uplift = 1
    else:
        max_uplift = 0

    uplifted_idx = max(rank_idx, grade_idx - max_uplift)
    return ROUND_LABEL_ORDER[uplifted_idx]



def _height_str(inches: int) -> str:
    feet = inches // 12
    rem = inches % 12
    return f"{feet}'{rem}\""



def read_seed(path: Path) -> list[dict]:
    with path.open() as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["rank_seed"] = int(row["rank_seed"])
        row["weight_lb"] = int(row["weight_lb"])
        row["seed_row_id"] = int(row["seed_row_id"])
    return rows



def dedupe_seed_rows(rows: list[dict]) -> list[dict]:
    """Remove duplicate prospects by canonical name + normalized position (keep best seed rank)."""
    best = {}
    for row in rows:
        pos = normalize_pos(row["pos_raw"])
        if pos not in ALLOWED_POSITIONS:
            continue
        key = (canonical_player_name(row["player_name"]), pos)
        current = best.get(key)
        if current is None or row["rank_seed"] < current["rank_seed"]:
            best[key] = row

    deduped = list(best.values())
    deduped.sort(key=lambda r: r["rank_seed"])
    return deduped



def augment_seed_with_external_and_analyst(
    seed_rows: list[dict],
    external_rows: list[dict],
    analyst_rows: list[dict],
    returning_names: set[str] | None = None,
    already_drafted_names: set[str] | None = None,
    allowed_names: set[str] | None = None,
) -> tuple[list[dict], int, int, int]:
    merged = list(seed_rows)
    returning_names = returning_names or set()
    already_drafted_names = already_drafted_names or set()
    allowed_names = allowed_names or set()
    existing = {(canonical_player_name(r["player_name"]), normalize_pos(r["pos_raw"])) for r in merged}
    next_id = max((r["seed_row_id"] for r in merged), default=0) + 1

    added_external = 0
    added_analyst = 0
    skipped_ineligible = 0

    for ext in external_rows:
        pos = normalize_pos(ext.get("external_pos", ""))
        if pos not in ALLOWED_POSITIONS:
            continue

        player_name = (ext.get("player_name") or "").strip()
        key = (canonical_player_name(player_name), pos)
        if key[0] in returning_names:
            skipped_ineligible += 1
            continue
        if key[0] in already_drafted_names:
            skipped_ineligible += 1
            continue
        if allowed_names and key[0] not in allowed_names:
            skipped_ineligible += 1
            continue
        if not player_name or key in existing:
            continue

        ext_rank = int(ext.get("external_rank", 300) or 300)
        pseudo_seed_rank = min(300, max(12, int(round(ext_rank * 0.92 + 12))))
        h, w = POSITION_DEFAULT_FRAME[pos]

        merged.append(
            {
                "rank_seed": pseudo_seed_rank,
                "player_name": player_name,
                "school": ext.get("external_school") or "Unknown",
                "pos_raw": pos,
                "height": _height_str(h),
                "weight_lb": int(w),
                "class_year": "SR",
                "source_primary": "External_Board_Import",
                "seed_row_id": next_id,
            }
        )
        next_id += 1
        existing.add(key)
        added_external += 1

    # Add analyst-only players not present in seed or external board.
    for row in sorted(analyst_rows, key=lambda r: int(r["source_rank"])):
        pos = normalize_pos(row.get("position", ""))
        if pos not in ALLOWED_POSITIONS:
            continue

        player_name = (row.get("player_name") or "").strip()
        key = (canonical_player_name(player_name), pos)
        if key[0] in returning_names:
            skipped_ineligible += 1
            continue
        if key[0] in already_drafted_names:
            skipped_ineligible += 1
            continue
        if allowed_names and key[0] not in allowed_names:
            skipped_ineligible += 1
            continue
        if not player_name or key in existing:
            continue

        source_rank = int(row.get("source_rank", 200) or 200)
        pseudo_seed_rank = min(300, max(24, int(round(source_rank * 1.25 + 30))))
        h, w = POSITION_DEFAULT_FRAME[pos]

        merged.append(
            {
                "rank_seed": pseudo_seed_rank,
                "player_name": player_name,
                "school": row.get("school") or "Unknown",
                "pos_raw": pos,
                "height": _height_str(h),
                "weight_lb": int(w),
                "class_year": "SR",
                "source_primary": f"Analyst_Import_{row.get('source','Unknown')}",
                "seed_row_id": next_id,
            }
        )
        next_id += 1
        existing.add(key)
        added_analyst += 1

    return merged, added_external, added_analyst, skipped_ineligible



def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    seen = set(fieldnames)
    for row in rows[1:]:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)



def write_top_board_md(path: Path, rows: list[dict], limit: int = 100) -> None:
    lines = [
        "# 2026 Big Board (Top 100)",
        "",
        "| Rank | Player | Pos | School | Grade | Round | Best Team Fit | PFF | RAS |",
        "|---:|---|---|---|---:|---|---|---:|---:|",
    ]
    for row in rows[:limit]:
        pff = row.get("pff_grade") or ""
        ras = row.get("ras_estimate") or ""
        lines.append(
            f"| {row['consensus_rank']} | {row['player_name']} | {row['position']} | {row['school']} | {row['final_grade']} | {row['round_value']} | {row['best_team_fit']} | {pff} | {ras} |"
        )
    path.write_text("\n".join(lines))


def _run_locked_stability_checks(board_path: Path, watchlist_path: Path) -> None:
    if not AUTO_WEEKLY_STABILITY_CHECK:
        return

    try:
        from scripts.run_weekly_stability_check import run_check

        report_txt, latest_txt = run_check(
            board_path=board_path,
            watchlist_path=watchlist_path,
            snapshot_dir=OUTPUTS / "stability_snapshots",
        )
        print(f"Weekly stability report: {report_txt}")
        print(f"Weekly stability latest: {latest_txt}")
    except Exception as exc:
        print(f"Weekly stability check skipped due to error: {exc}")
        return

    if not AUTO_DELTA_AUDIT_AFTER_BUILD:
        return

    try:
        from scripts.run_delta_audit import run_audit

        snapshots = sorted((OUTPUTS / "stability_snapshots").glob("big_board_2026_snapshot_*.csv"))
        if len(snapshots) < 2:
            print("Delta audit skipped: need at least 2 stability snapshots.")
            return
        previous = snapshots[-2]
        txt_out, csv_out = run_audit(board_path, previous, top_n=25)
        print(f"Delta audit report: {txt_out}")
        print(f"Delta audit rows: {csv_out}")
    except Exception as exc:
        print(f"Delta audit skipped due to error: {exc}")

def _build_rank_driver_summary(
    *,
    model_score: float,
    formula: dict,
    prior_signal: float,
    language_adjustment_applied: float,
    guardrail_penalty: float,
    drift_penalty: float,
    soft_ceiling_penalty: float,
    cap_penalty: float,
    consensus_tail_penalty: float,
    front7_inflation_penalty: float,
    cb_nickel_inflation_penalty: float,
    bluechip_floor_lift: float,
    top50_evidence_brake_penalty: float,
    bluechip_rank_protection_adjustment: float,
) -> str:
    risk_penalty = float(formula.get("formula_risk_penalty", 0.0) or 0.0)
    prod_component = float(
        formula.get("formula_production_component", PRODUCTION_SIGNAL_NEUTRAL) or PRODUCTION_SIGNAL_NEUTRAL
    )
    ath_component = float(formula.get("formula_athletic_component", 70.0) or 70.0)
    trait_component = float(formula.get("formula_trait_component", 70.0) or 70.0)
    prior_grade = float(formula.get("formula_prior_grade", 70.0) or 70.0)
    calibrated_grade = float(formula.get("formula_calibrated_grade", 70.0) or 70.0)
    components = [
        ("trait", (trait_component - 70.0) * 0.38),
        ("production", (prod_component - 70.0) * 0.24),
        ("athletic", (ath_component - 70.0) * 0.18),
        ("prior", (prior_grade - calibrated_grade)),
        ("language", float(language_adjustment_applied)),
        ("risk", -risk_penalty),
        ("bluechip_floor", float(bluechip_floor_lift)),
        ("bluechip_rank_protect", float(bluechip_rank_protection_adjustment)),
        ("top50_evidence_brake", -float(top50_evidence_brake_penalty)),
        (
            "guardrail",
            -(
                float(guardrail_penalty)
                + float(drift_penalty)
                + float(soft_ceiling_penalty)
                + float(cap_penalty)
                + float(consensus_tail_penalty)
                + float(front7_inflation_penalty)
                + float(cb_nickel_inflation_penalty)
            ),
        ),
    ]
    top = sorted(components, key=lambda x: abs(float(x[1])), reverse=True)[:4]
    return (
        f"score={round(float(model_score),2)};"
        f" prior_signal={round(float(prior_signal),2)};"
        " "
        + " | ".join(f"{k}:{v:+.2f}" for k, v in top)
    ).strip()


def _run_extreme_rank_delta_gate(
    *,
    current_rows: list[dict],
    snapshot_dir: Path,
    min_rise: int = EXTREME_RANK_DELTA_MIN_RISE,
) -> tuple[list[dict], Path | None]:
    board_snaps = sorted(snapshot_dir.glob("big_board_2026_snapshot_*.csv"))
    if not board_snaps:
        return [], None
    prev_path = board_snaps[-1]
    with prev_path.open() as f:
        prev_rows = list(csv.DictReader(f))
    prev_idx = {
        canonical_player_name(r.get("player_name", "")): r
        for r in prev_rows
        if str(r.get("player_name", "")).strip()
    }

    flagged: list[dict] = []
    for row in current_rows:
        name = str(row.get("player_name", "")).strip()
        if not name:
            continue
        key = canonical_player_name(name)
        prev = prev_idx.get(key)
        if not prev:
            continue
        curr_rank = int(_as_float(row.get("consensus_rank")) or 9999)
        prev_rank = int(_as_float(prev.get("consensus_rank")) or 9999)
        if curr_rank >= 9999 or prev_rank >= 9999:
            continue
        rise = prev_rank - curr_rank
        if rise < int(min_rise):
            continue

        curr_conf = float(_as_float(row.get("confidence_score")) or 0.0)
        prev_conf = float(_as_float(prev.get("confidence_score")) or 0.0)
        curr_missing = float(
            _as_float(row.get("formula_evidence_missing_count_weighted"))
            or _as_float(row.get("formula_evidence_missing_count"))
            or 0.0
        )
        prev_missing = float(
            _as_float(prev.get("formula_evidence_missing_count_weighted"))
            or _as_float(prev.get("formula_evidence_missing_count"))
            or 0.0
        )
        curr_cov = int(_as_float(row.get("cfb_prod_coverage_count")) or 0)
        prev_cov = int(_as_float(prev.get("cfb_prod_coverage_count")) or 0)
        curr_testing = str(row.get("combine_testing_status") or "").strip().lower()
        prev_testing = str(prev.get("combine_testing_status") or "").strip().lower()

        evidence_improved = (
            (curr_conf - prev_conf >= 5.0)
            or (prev_missing - curr_missing >= 0.75)
            or (curr_cov - prev_cov >= 2)
            or (prev_testing in {"pending", "unknown", "dnp"} and curr_testing == "reported")
        )
        if evidence_improved:
            continue

        flagged.append(
            {
                "player_name": name,
                "position": row.get("position", ""),
                "prev_rank": prev_rank,
                "curr_rank": curr_rank,
                "rank_rise": rise,
                "prev_confidence": round(prev_conf, 2),
                "curr_confidence": round(curr_conf, 2),
                "prev_evidence_missing": round(prev_missing, 2),
                "curr_evidence_missing": round(curr_missing, 2),
                "prev_cfb_cov": prev_cov,
                "curr_cfb_cov": curr_cov,
                "prev_testing_status": prev_testing,
                "curr_testing_status": curr_testing,
                "rank_driver_summary": row.get("rank_driver_summary", ""),
            }
        )

    flagged.sort(key=lambda r: int(r.get("rank_rise", 0)), reverse=True)
    return flagged, prev_path


def _scale_waa(pff_waa: float | None) -> float:
    if pff_waa is None:
        return 55.0
    bounded = max(-0.5, min(2.0, pff_waa))
    return 50.0 + bounded * 22.0



def _athletic_source_confidence(source: str) -> float:
    source_key = str(source or "").strip().lower()
    if source_key == "combine_official":
        return 1.0
    if source_key == "combine_derived_partial":
        return 0.82
    if source_key == "estimated_profile_proxy":
        return 0.58
    if source_key == "pending_combine":
        return 0.35
    return 0.5


def _official_ras_fields(position: str, combine: dict, *, fallback_ras: dict) -> tuple[dict, dict]:
    official = combine.get("ras_official")
    if official is not None:
        score = round(max(0.0, min(10.0, float(official))), 2)
        tier = ras_tier(score)
        ras = {
            "ras_estimate": score,
            "ras_tier": tier,
            "ras_percentile": ras_percentile(score),
            "ras_source": "combine_official",
        }
        return ras, historical_ras_comparison(position, tier)

    derived = ras_from_combine_profile(position, combine, fallback_ras)
    if str(derived.get("ras_source") or "").strip() == "combine_derived_partial":
        return derived, historical_ras_comparison(position, str(derived.get("ras_tier") or "average"))

    if derived.get("ras_estimate") not in {"", None}:
        return derived, historical_ras_comparison(position, str(derived.get("ras_tier") or "average"))

    if combine.get("combine_testing_event_count"):
        return (
            {
                "ras_estimate": fallback_ras.get("ras_estimate", ""),
                "ras_tier": fallback_ras.get("ras_tier", ""),
                "ras_percentile": fallback_ras.get("ras_percentile", ""),
                "ras_source": fallback_ras.get("ras_source", "estimated_profile_proxy"),
            },
            historical_ras_comparison(position, str(fallback_ras.get("ras_tier") or "average")),
        )

    return (
        {
            "ras_estimate": "",
            "ras_tier": "",
            "ras_percentile": "",
            "ras_source": "pending_combine",
        },
        {
            "ras_historical_comp_1": "",
            "ras_historical_comp_2": "",
            "ras_comparison_note": "Pending official combine RAS",
        },
    )


def _build_analyst_pos_votes(analyst_rows: list[dict]) -> dict[str, dict[str, int]]:
    votes: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in analyst_rows:
        name = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position", ""))
        if not name or not pos:
            continue
        votes[name][pos] += 1
    return votes


def _build_allowed_universe_names(
    external_rows: list[dict],
    analyst_rows: list[dict],
    declared_underclassmen: set[str],
    nfl_official_names: set[str] | None = None,
) -> set[str]:
    nfl_official_names = nfl_official_names or set()
    if ENFORCE_NFL_OFFICIAL_UNIVERSE and nfl_official_names:
        return set(nfl_official_names) | set(declared_underclassmen)

    allowed: set[str] = set(declared_underclassmen)
    for row in external_rows:
        name = canonical_player_name(row.get("player_name", ""))
        if name:
            allowed.add(name)
    for row in analyst_rows:
        name = canonical_player_name(row.get("player_name", ""))
        if name:
            allowed.add(name)
    return allowed


def _load_nfl_official_universe_names(path: Path = NFL_OFFICIAL_PROSPECT_PATH) -> set[str]:
    """
    Load the authoritative 2026 prospect universe from NFL.com source rows
    (currently combine invite pull file). This is used as a hard allowlist
    when ENFORCE_NFL_OFFICIAL_UNIVERSE is enabled.
    """
    if not path.exists():
        return set()

    names: set[str] = set()
    with path.open() as f:
        for row in csv.DictReader(f):
            name = str(row.get("player_name", "")).strip()
            if not name:
                continue
            lower = name.lower()
            # Drop parser artifact rows from article body extraction.
            if any(
                token in lower
                for token in [
                    "the nfl released the list of players invited",
                    "here are the invitees",
                    "sorted by position",
                ]
            ):
                continue
            if len(name.split()) < 2:
                continue
            key = canonical_player_name(name)
            if key:
                names.add(key)
    return names


def _position_evidence_score(
    name_key: str,
    pos: str,
    row: dict,
    ext: dict,
    analyst_votes: dict[str, dict[str, int]],
    espn_by_name_pos: dict,
    pp_by_name_pos: dict,
    lang_by_name_pos: dict,
) -> int:
    score = 0
    if (name_key, pos) in lang_by_name_pos:
        score += 5
    if (name_key, pos) in espn_by_name_pos:
        score += 4
    if (name_key, pos) in pp_by_name_pos:
        score += 2
    if normalize_pos(ext.get("external_pos", "")) == pos:
        score += 3

    votes = analyst_votes.get(name_key, {})
    if votes:
        score += 2 * int(votes.get(pos, 0))

    src = str(row.get("source_primary", "")).lower()
    if "analyst_import_" in src:
        score += 1
    if "external_board_import" in src:
        score += 1

    return score


def _as_float(value) -> float | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _enforce_production_knn_history_qa(pack: dict) -> None:
    """
    Prevent accidental use of same-season fallback comps as historical comps.
    """
    meta = pack.get("meta", {}) if isinstance(pack, dict) else {}
    if str(meta.get("status", "")) != "ok":
        return

    years_count = int(_as_float(meta.get("years_count")) or 0)
    years_min = meta.get("years_min", "")
    years_max = meta.get("years_max", "")
    single_year = years_count <= 1 or (
        str(years_min).strip() != "" and str(years_min) == str(years_max)
    )
    if not single_year:
        return

    msg = (
        "Production percentile KNN QA failed: comps source is single-year only "
        f"(years={years_min}-{years_max}, years_count={years_count}). "
        "Historical comps require multi-year data. "
        "Set ALLOW_SINGLE_YEAR_PRODUCTION_KNN=1 to override intentionally."
    )
    if ALLOW_SINGLE_YEAR_PRODUCTION_KNN:
        print(f"WARNING: {msg}")
        return
    print(msg)
    raise SystemExit(2)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _language_feature_block(
    *,
    lang: dict,
    lang_risk_hits: int,
    risk_flags: list[int],
    extra_risk_hits: list[int],
) -> dict:
    """
    Derive simple, explainable language features with bounded influence.
    """
    word_count = int(_as_float(lang.get("lang_text_coverage")) or 0)
    source_count = int(_as_float(lang.get("lang_source_count")) or 0)
    traits = [
        _as_float(lang.get("lang_trait_processing")) or 50.0,
        _as_float(lang.get("lang_trait_technique")) or 50.0,
        _as_float(lang.get("lang_trait_explosiveness")) or 50.0,
        _as_float(lang.get("lang_trait_physicality")) or 50.0,
        _as_float(lang.get("lang_trait_competitiveness")) or 50.0,
        _as_float(lang.get("lang_trait_versatility")) or 50.0,
    ]
    positive_trait_rate = sum(1 for t in traits if float(t) >= 58.0) / len(traits)

    risk_flag_values = [int(v) for v in risk_flags if v is not None]
    developmental_flag_rate = (
        sum(risk_flag_values) / len(risk_flag_values) if risk_flag_values else 0.0
    )
    total_risk_hits = max(0, int(lang_risk_hits) + sum(max(0, int(x)) for x in extra_risk_hits))
    concern_rate = (total_risk_hits / max(40.0, float(word_count))) * 100.0 if word_count > 0 else 0.0

    # Bounded language delta: refine, never rewrite rankings.
    base_delta = (
        ((positive_trait_rate * 100.0) - 50.0) * 0.020
        - (developmental_flag_rate * 0.75)
        - (_clamp(concern_rate, 0.0, 25.0) * 0.030)
    )
    source_conf = _clamp(float(source_count) / 3.0, 0.0, 1.0)
    coverage_conf = _clamp(float(word_count) / 180.0, 0.0, 1.0)
    confidence = max(0.0, source_conf * coverage_conf)
    applied_delta = _clamp(base_delta * confidence, -0.9, 0.9)

    return {
        "lang_report_word_count": word_count,
        "lang_positive_trait_rate": round(positive_trait_rate, 4),
        "lang_developmental_flag_rate": round(developmental_flag_rate, 4),
        "lang_concern_rate": round(concern_rate, 4),
        "language_adjustment_raw": round(base_delta, 4),
        "language_adjustment_confidence": round(confidence, 4),
        "language_adjustment_applied": round(applied_delta, 4),
        "language_adjustment_cap": 0.9,
    }


def _guardrail_production_component(position: str, raw_component: float) -> tuple[float, float]:
    neutral = PRODUCTION_SIGNAL_NEUTRAL
    if position == "QB":
        multiplier = PRODUCTION_SIGNAL_QB_MULTIPLIER
        max_up_delta = PRODUCTION_SIGNAL_QB_MAX_DELTA
        max_down_delta = PRODUCTION_SIGNAL_QB_MAX_DELTA
    elif position in {"EDGE", "DT", "LB"}:
        multiplier = PRODUCTION_SIGNAL_FRONT7_MULTIPLIER
        max_up_delta = PRODUCTION_SIGNAL_FRONT7_MAX_UP_DELTA
        max_down_delta = PRODUCTION_SIGNAL_FRONT7_MAX_DOWN_DELTA
    else:
        multiplier = PRODUCTION_SIGNAL_MULTIPLIER
        max_up_delta = PRODUCTION_SIGNAL_MAX_DELTA
        max_down_delta = PRODUCTION_SIGNAL_MAX_DELTA

    delta = (float(raw_component) - neutral) * multiplier
    guarded_delta = _clamp(delta, -abs(max_down_delta), abs(max_up_delta))
    guarded_component = neutral + guarded_delta
    return guarded_component, guarded_delta


def _assert_no_team_metrics_in_player_feed(player_name: str, cfb_row: dict) -> None:
    # Team-level CFBD context is intentionally separate from player grading.
    # If a team_* field appears in player production rows, stop the build.
    if not cfb_row:
        return
    team_keys = [k for k in cfb_row.keys() if str(k).startswith("team_")]
    if team_keys:
        cols = ", ".join(sorted(team_keys))
        raise ValueError(
            f"Player production row for '{player_name}' contains team metrics ({cols}). "
            "Keep team stats in team-context datasets only."
        )


_SG_POSITION_FIELD_FAMILIES = {
    "QB": {
        "sg_qb_pass_grade",
        "sg_qb_btt_rate",
        "sg_qb_twp_rate",
        "sg_qb_pressure_to_sack_rate",
        "sg_qb_pressure_grade",
        "sg_qb_blitz_grade",
        "sg_qb_no_screen_grade",
        "sg_qb_quick_qb_rating",
    },
    "RB": {
        "sg_rb_run_grade",
        "sg_rb_elusive_rating",
        "sg_rb_yco_attempt",
        "sg_rb_explosive_rate",
        "sg_rb_breakaway_percent",
        "sg_rb_targets_per_route",
        "sg_rb_yprr",
    },
    "WR": {
        "sg_wrte_route_grade",
        "sg_wrte_yprr",
        "sg_wrte_targets_per_route",
        "sg_wrte_man_yprr",
        "sg_wrte_zone_yprr",
        "sg_wrte_contested_catch_rate",
        "sg_wrte_drop_rate",
    },
    "TE": {
        "sg_wrte_route_grade",
        "sg_wrte_yprr",
        "sg_wrte_targets_per_route",
        "sg_wrte_man_yprr",
        "sg_wrte_zone_yprr",
        "sg_wrte_contested_catch_rate",
        "sg_wrte_drop_rate",
    },
    "EDGE": {
        "sg_dl_pass_rush_grade",
        "sg_dl_pass_rush_win_rate",
        "sg_dl_prp",
        "sg_dl_true_pass_set_win_rate",
        "sg_dl_true_pass_set_prp",
        "sg_dl_total_pressures",
        "sg_front_run_def_grade",
        "sg_front_stop_percent",
        "sg_def_total_pressures",
        "sg_def_tackles_for_loss",
        "sg_def_tackles",
    },
    "DT": {
        "sg_dl_pass_rush_grade",
        "sg_dl_pass_rush_win_rate",
        "sg_dl_prp",
        "sg_dl_true_pass_set_win_rate",
        "sg_dl_true_pass_set_prp",
        "sg_dl_total_pressures",
        "sg_front_run_def_grade",
        "sg_front_stop_percent",
        "sg_def_total_pressures",
        "sg_def_tackles_for_loss",
        "sg_def_tackles",
    },
    "LB": {
        "sg_def_coverage_grade",
        "sg_def_run_grade",
        "sg_def_tackle_grade",
        "sg_def_missed_tackle_rate",
        "sg_def_total_pressures",
        "sg_def_tackles_for_loss",
        "sg_def_tackles",
        "sg_def_pass_break_ups",
        "sg_def_interceptions",
        "sg_cov_grade",
        "sg_cov_forced_incompletion_rate",
        "sg_cov_snaps_per_target",
        "sg_cov_yards_per_snap",
        "sg_cov_qb_rating_against",
        "sg_cov_man_grade",
        "sg_cov_zone_grade",
        "sg_slot_cov_snaps",
        "sg_slot_cov_snaps_per_target",
        "sg_slot_cov_qb_rating_against",
        "sg_slot_cov_yards_per_snap",
    },
    "CB": {
        "sg_cov_grade",
        "sg_cov_forced_incompletion_rate",
        "sg_cov_snaps_per_target",
        "sg_cov_yards_per_snap",
        "sg_cov_qb_rating_against",
        "sg_cov_man_grade",
        "sg_cov_zone_grade",
        "sg_slot_cov_snaps",
        "sg_slot_cov_snaps_per_target",
        "sg_slot_cov_qb_rating_against",
        "sg_slot_cov_yards_per_snap",
        "sg_def_pass_break_ups",
        "sg_def_interceptions",
        "sg_def_tackles",
        "sg_def_tackles_for_loss",
        "sg_def_total_pressures",
    },
    "S": {
        "sg_cov_grade",
        "sg_cov_forced_incompletion_rate",
        "sg_cov_snaps_per_target",
        "sg_cov_yards_per_snap",
        "sg_cov_qb_rating_against",
        "sg_cov_man_grade",
        "sg_cov_zone_grade",
        "sg_slot_cov_snaps",
        "sg_slot_cov_snaps_per_target",
        "sg_slot_cov_qb_rating_against",
        "sg_slot_cov_yards_per_snap",
        "sg_def_pass_break_ups",
        "sg_def_interceptions",
        "sg_def_tackles",
        "sg_def_tackles_for_loss",
        "sg_def_total_pressures",
    },
    "OT": {
        "sg_ol_pass_block_grade",
        "sg_ol_run_block_grade",
        "sg_ol_pbe",
        "sg_ol_pressure_allowed_rate",
        "sg_ol_versatility_count",
    },
    "IOL": {
        "sg_ol_pass_block_grade",
        "sg_ol_run_block_grade",
        "sg_ol_pbe",
        "sg_ol_pressure_allowed_rate",
        "sg_ol_versatility_count",
    },
}


def _sanitize_position_scoped_cfb_payload(position: str, cfb_row: dict) -> dict:
    if not cfb_row:
        return {}
    cleaned = dict(cfb_row)
    allowed = _SG_POSITION_FIELD_FAMILIES.get(position, set())
    sg_raw_fields = {
        key
        for key in cleaned
        if key.startswith("sg_")
        and key
        not in {
            "sg_advanced_signal",
            "sg_advanced_available_count",
            "sg_advanced_source",
            "sg_source_season",
            "sg_cov_source_season",
        }
    }
    for field in sg_raw_fields:
        if field not in allowed:
            cleaned[field] = ""
    has_allowed_values = any(str(cleaned.get(field, "")).strip() for field in allowed)
    if not has_allowed_values:
        cleaned["sg_advanced_signal"] = ""
        cleaned["sg_advanced_available_count"] = 0
        cleaned["sg_advanced_source"] = ""
        cleaned["sg_source_season"] = ""
        cleaned["sg_cov_source_season"] = ""
    return cleaned


def _consensus_anchor_adjustment(
    *,
    rank_seed: int,
    consensus_mean_rank,
    consensus_source_count,
    external_rank: int | None,
) -> float:
    """
    Adjustment applied to prior_signal to reduce seed-vs-market drift.
    Negative adjustment when market is materially lower on a player than seed rank.
    """
    mean_rank = _as_float(consensus_mean_rank)
    if mean_rank is None:
        return 0.0

    src_n_val = _as_float(consensus_source_count)
    src_n = int(src_n_val) if src_n_val is not None else 0
    if src_n <= 1:
        confidence = 0.35
    elif src_n == 2:
        confidence = 0.70
    else:
        confidence = 1.0

    delta = mean_rank - float(rank_seed)  # positive => market lower than seed
    adjustment = 0.0

    # Penalize optimistic seed outliers.
    if delta >= 35.0:
        adjustment -= min(8.0, 0.8 + ((delta - 35.0) / 18.0))
    # Small reward when market is clearly higher than seed.
    elif delta <= -35.0:
        adjustment += min(2.0, 0.3 + ((abs(delta) - 35.0) / 30.0))

    if external_rank is not None:
        ext_delta = float(external_rank) - float(rank_seed)
        if ext_delta >= 50.0:
            adjustment -= min(2.0, 0.5 + ((ext_delta - 50.0) / 45.0))

    return round(adjustment * confidence, 2)


def _weighted_mean(parts: list[tuple[float, float]]) -> float | None:
    if not parts:
        return None
    num = 0.0
    den = 0.0
    for weight, value in parts:
        num += weight * value
        den += weight
    if den <= 0:
        return None
    return num / den


def _class_context_score(class_year: str) -> float:
    cy = (class_year or "").upper()
    if cy in {"JR", "RSJ"}:
        return 83.0
    if cy in {"SR", "RSR"}:
        return 80.0
    if cy in {"SO", "RSO"}:
        return 77.0
    if cy in {"FR", "RFR"}:
        return 74.0
    return 78.0


def _compute_formula_score(
    *,
    position: str,
    class_year: str,
    grades: dict,
    pff_grade: float | None,
    espn_prod_signal: float,
    pp_skill_signal: float,
    pp_player_available: bool,
    cfb_prod_signal: float,
    cfb_player_available: bool,
    cfb_prod_coverage_count: int,
    cfb_prod_reliability: float,
    cfb_prod_quality_label: str,
    prior_signal: float,
    lang: dict,
    ras: dict,
    md_features: dict,
    athletic_profile: dict,
    external_rank: int | None,
    analyst_score: float,
    espn_volatility_flag: bool,
    pp_risk_flag: bool,
    kiper_volatility_penalty: float,
    tdn_text_trait_signal: float,
    tdn_risk_penalty: float,
    br_text_trait_signal: float,
    br_risk_penalty: float,
    atoz_text_trait_signal: float,
    atoz_risk_penalty: float,
    si_text_trait_signal: float,
    si_risk_penalty: float,
    cbs_text_trait_signal: float,
    cbs_risk_penalty: float,
    years_played: float | None,
    draft_age: float | None,
    early_declare: bool,
    combine_testing_status: str,
    combine_testing_event_count: int,
    combine_invited: bool,
) -> dict:
    film_trait = _as_float(grades.get("film_trait_score"))
    film_enabled = ENABLE_FILM_WEIGHTING
    espn_trait_vals = [
        _as_float(lang.get("lang_trait_processing")),
        _as_float(lang.get("lang_trait_technique")),
        _as_float(lang.get("lang_trait_explosiveness")),
        _as_float(lang.get("lang_trait_physicality")),
        _as_float(lang.get("lang_trait_competitiveness")),
        _as_float(lang.get("lang_trait_versatility")),
    ]
    if film_enabled:
        espn_trait_vals.append(_as_float(grades.get("film_trait_score")))
    espn_trait_vals = [v for v in espn_trait_vals if v is not None]
    language_trait = _as_float(lang.get("lang_trait_composite"))

    trait_proxy_component = float(_as_float(grades.get("trait_score")) or 70.0)
    lang_text_coverage = _as_float(lang.get("lang_text_coverage")) or 0.0
    trait_parts: list[tuple[float, float]] = []
    if film_enabled and film_trait is not None:
        trait_parts.append((0.52, film_trait))
    if language_trait is not None:
        trait_parts.append((0.27, language_trait))
    if espn_trait_vals:
        trait_parts.append((0.15, sum(espn_trait_vals) / len(espn_trait_vals)))
    if tdn_text_trait_signal > 0:
        trait_parts.append((0.05, tdn_text_trait_signal))
    if br_text_trait_signal > 0:
        trait_parts.append((0.04, br_text_trait_signal))
    if atoz_text_trait_signal > 0:
        trait_parts.append((0.04, atoz_text_trait_signal))
    if si_text_trait_signal > 0:
        trait_parts.append((0.02, si_text_trait_signal))
    if cbs_text_trait_signal > 0:
        trait_parts.append((0.05, cbs_text_trait_signal))
    trait_component_raw = _weighted_mean(trait_parts) or (58.0 if position == "QB" else 60.0)
    # Trait stability anchor: when language coverage is thin/noisy, keep an anchor to
    # the core trait profile so blue-chip prospects don't collapse on parser noise.
    if lang_text_coverage <= TRAIT_PROXY_ANCHOR_LOW_COVERAGE:
        trait_anchor_weight = TRAIT_PROXY_ANCHOR_MID_WEIGHT
    elif lang_text_coverage <= TRAIT_PROXY_ANCHOR_MID_COVERAGE:
        trait_anchor_weight = (TRAIT_PROXY_ANCHOR_MIN_WEIGHT + TRAIT_PROXY_ANCHOR_MID_WEIGHT) / 2.0
    else:
        trait_anchor_weight = TRAIT_PROXY_ANCHOR_MIN_WEIGHT
    if external_rank is not None and int(external_rank) <= 30 and analyst_score >= 70.0:
        trait_anchor_weight += 0.34
    elif external_rank is not None and int(external_rank) <= 60 and analyst_score >= 62.0:
        trait_anchor_weight += 0.20
    elif analyst_score >= 72.0 and prior_signal >= 82.0:
        trait_anchor_weight += 0.14
    trait_anchor_weight = _clamp(
        trait_anchor_weight,
        TRAIT_PROXY_ANCHOR_MIN_WEIGHT,
        TRAIT_PROXY_ANCHOR_MAX_WEIGHT,
    )
    trait_component = (
        (trait_anchor_weight * trait_proxy_component)
        + ((1.0 - trait_anchor_weight) * trait_component_raw)
    )

    prod_parts: list[tuple[float, float]] = []
    pff_weight = 0.42
    if position in {"EDGE", "DT", "LB"}:
        pff_weight = 0.30
    elif position in {"CB", "S"}:
        pff_weight = 0.34
    if pff_grade is not None:
        prod_parts.append((pff_weight, pff_grade))
    prod_parts.append((0.24, espn_prod_signal))
    if pp_player_available:
        prod_parts.append((0.12, pp_skill_signal))
    if cfb_player_available:
        cov_boost = min(1.0, max(0.5, float(cfb_prod_coverage_count) / 3.0))
        reliability = max(0.0, min(1.0, float(cfb_prod_reliability)))
        cfb_weight_base = 0.22
        if position in {"EDGE", "DT", "LB"}:
            cfb_weight_base = 0.07
        elif position in {"CB", "S"}:
            cfb_weight_base = 0.12
        if str(cfb_prod_quality_label or "").strip().lower() == "proxy":
            cfb_weight_base *= 0.80
        prod_parts.append((cfb_weight_base * cov_boost * reliability, cfb_prod_signal))
    production_component_raw = _weighted_mean(prod_parts) or 62.0
    production_component, production_guardrail_delta = _guardrail_production_component(
        position, production_component_raw
    )
    # Front-seven production is noisy year-to-year; compress its influence.
    if position in {"EDGE", "DT", "LB"}:
        production_component = PRODUCTION_SIGNAL_NEUTRAL + (
            (production_component - PRODUCTION_SIGNAL_NEUTRAL) * 0.70
        )
        if (
            cfb_player_available
            and str(cfb_prod_quality_label or "").strip().lower() == "proxy"
            and int(cfb_prod_coverage_count) <= 1
        ):
            production_component = min(production_component, 70.0)
        production_guardrail_delta = production_component - PRODUCTION_SIGNAL_NEUTRAL

    official_ras = _as_float(ras.get("ras_estimate"))
    ras_source = str(ras.get("ras_source") or "").strip().lower()
    has_official_ras = ras_source == "combine_official"
    has_partial_ras = ras_source == "combine_derived_partial"
    md_speed_pct = _as_float(md_features.get("md_speed_pct"))
    md_explosion_pct = _as_float(md_features.get("md_explosion_pct"))
    md_agility_pct = _as_float(md_features.get("md_agility_pct"))

    # Legacy athletic fallback from MockDraftable composites (kept as fallback only).
    legacy_athletic_parts: list[tuple[float, float]] = []
    if md_speed_pct is not None:
        legacy_athletic_parts.append((0.45, md_speed_pct))
    if md_explosion_pct is not None:
        legacy_athletic_parts.append((0.35, md_explosion_pct))
    if md_agility_pct is not None:
        legacy_athletic_parts.append((0.20, md_agility_pct))
    legacy_athletic_component = _weighted_mean(legacy_athletic_parts)
    if legacy_athletic_component is None:
        legacy_athletic_component = 68.0 if position == "QB" else 70.0

    # New position+era adjusted athletic profile.
    athletic_profile_score = _as_float(athletic_profile.get("athletic_profile_score"))
    athletic_profile_cov_count = int(_as_float(athletic_profile.get("athletic_metric_coverage_count")) or 0)
    athletic_profile_cov_rate = float(_as_float(athletic_profile.get("athletic_metric_coverage_rate")) or 0.0)
    athletic_profile_missing_penalty = float(_as_float(athletic_profile.get("athletic_missing_penalty")) or 0.0)
    athletic_profile_variance_penalty = float(_as_float(athletic_profile.get("athletic_variance_penalty")) or 0.0)

    athletic_evidence_count = max(athletic_profile_cov_count, len(legacy_athletic_parts))
    athletic_cap_applied = ""
    athletic_source = "neutral_default"
    if has_official_ras and official_ras is not None and athletic_profile_score is not None:
        profile_w = _clamp(0.20 + (0.35 * athletic_profile_cov_rate), 0.20, 0.55)
        athletic_component = ((1.0 - profile_w) * (official_ras * 10.0)) + (profile_w * athletic_profile_score)
        athletic_source = "official_combine_plus_position_era_profile"
    elif has_official_ras and official_ras is not None:
        athletic_component = official_ras * 10.0
        athletic_source = "official_combine_only"
    elif has_partial_ras and official_ras is not None and athletic_profile_score is not None:
        profile_w = _clamp(0.25 + (0.45 * athletic_profile_cov_rate), 0.25, 0.70)
        athletic_component = ((1.0 - profile_w) * (official_ras * 10.0)) + (profile_w * athletic_profile_score)
        athletic_source = "pro_day_or_partial_plus_position_era_profile"
    elif has_partial_ras and official_ras is not None:
        athletic_component = official_ras * 10.0
        athletic_source = "pro_day_or_partial_only"
    elif athletic_profile_score is not None:
        profile_w = _clamp(0.20 + (0.55 * athletic_profile_cov_rate), 0.20, 0.75)
        athletic_component = (profile_w * athletic_profile_score) + ((1.0 - profile_w) * legacy_athletic_component)
        athletic_source = "position_era_profile_plus_md"
    else:
        athletic_component = legacy_athletic_component
        athletic_source = "md_fallback"

    strong_athletic_profile = athletic_profile_score is not None and athletic_profile_score >= 68.0
    elite_athletic_profile = athletic_profile_score is not None and athletic_profile_score >= 74.0
    if position in {"EDGE", "DT", "LB"}:
        # Keep front-seven athletic upside from sparse testing in check without over-penalizing
        # a class where many top prospects are skipping full official testing.
        cap_ceiling = None
        if not has_official_ras and athletic_evidence_count <= 1:
            if has_partial_ras and not strong_athletic_profile:
                cap_ceiling = 89.0
            elif not has_partial_ras and elite_athletic_profile:
                cap_ceiling = None
            elif not has_partial_ras and strong_athletic_profile:
                cap_ceiling = 90.0
            else:
                cap_ceiling = 87.5
        if cap_ceiling is not None:
            capped = min(athletic_component, cap_ceiling)
            if capped < athletic_component:
                athletic_component = capped
                athletic_cap_applied = f"front7_sparse_testing_cap_{cap_ceiling:.1f}"

    size_component = float(grades.get("size_score", 75.0) or 75.0)
    context_component = _class_context_score(class_year)
    years_played_context_adjustment = 0.0
    years_played_risk_adjustment = 0.0
    years_played_bucket = ""
    if years_played is not None:
        yp = float(years_played)
        if yp >= 4.0:
            years_played_bucket = "4plus"
            years_played_context_adjustment = 0.65
            years_played_risk_adjustment = -0.10
        elif yp >= 3.0:
            years_played_bucket = "3"
            years_played_context_adjustment = 0.20
        elif yp > 0:
            years_played_bucket = "2orless"
            years_played_context_adjustment = -0.35
            years_played_risk_adjustment = 0.18
    context_component += years_played_context_adjustment

    draft_age_context_adjustment = 0.0
    draft_age_risk_adjustment = 0.0
    draft_age_bucket = ""
    if draft_age is not None:
        da = float(draft_age)
        if da < 21.0:
            draft_age_bucket = "very_young"
            draft_age_context_adjustment = 0.12
            draft_age_risk_adjustment = 0.05
        elif da <= 22.6:
            draft_age_bucket = "prime"
            draft_age_context_adjustment = 0.0
        elif da <= 23.5:
            draft_age_bucket = "older"
            draft_age_context_adjustment = -0.08
            draft_age_risk_adjustment = 0.04
        else:
            draft_age_bucket = "oldest"
            draft_age_context_adjustment = -0.18
            draft_age_risk_adjustment = 0.10
    if ENABLE_DRAFT_AGE_SCORING:
        context_component += draft_age_context_adjustment

    early_declare_context_adjustment = 0.0
    early_declare_risk_adjustment = 0.0
    if early_declare:
        early_declare_context_adjustment = 0.10
        early_declare_risk_adjustment = -0.06
    if ENABLE_EARLY_DECLARE_SCORING:
        context_component += early_declare_context_adjustment
    if film_enabled and _as_float(grades.get("film_trait_coverage")) and float(grades.get("film_trait_coverage", 0.0) or 0.0) >= 0.75:
        context_component += 1.2
    if _as_float(lang.get("lang_text_coverage")) and float(lang.get("lang_text_coverage", 0.0) or 0.0) >= 120:
        context_component += 0.8

    has_testing_signal = (
        official_ras is not None
        or int(combine_testing_event_count or 0) > 0
    )

    film_coverage = _as_float(grades.get("film_trait_coverage")) or 0.0
    lang_text_coverage = _as_float(lang.get("lang_text_coverage")) or 0.0
    has_film_signal = film_enabled and film_trait is not None and film_coverage >= 0.45
    has_language_signal = language_trait is not None and lang_text_coverage >= 40.0
    has_market_signal = external_rank is not None
    testing_status_norm = str(combine_testing_status or "").strip().lower()
    if testing_status_norm not in {"reported", "pending", "dnp", "unknown"}:
        testing_status_norm = "unknown"
    if has_testing_signal and testing_status_norm in {"pending", "unknown"}:
        testing_status_norm = "reported"
    if (not has_testing_signal) and combine_invited and testing_status_norm == "unknown":
        testing_status_norm = "pending"

    missing_non_testing_count = sum(
        0 if present else 1 for present in (has_film_signal, has_language_signal, has_market_signal)
    )
    testing_missing_count = 0 if has_testing_signal else 1
    if testing_missing_count == 0:
        testing_missing_weight = 0.0
        evidence_testing_missing_weight = 0.0
        athletic_missing_risk_factor = 1.0
    elif testing_status_norm == "pending":
        testing_missing_weight = (
            TESTING_MISSING_SIGNAL_WEIGHT_PENDING_QB
            if position == "QB"
            else TESTING_MISSING_SIGNAL_WEIGHT_PENDING_NON_QB
        )
        evidence_testing_missing_weight = EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT_PENDING
        athletic_missing_risk_factor = ATHLETIC_MISSING_RISK_FACTOR_PENDING
    elif testing_status_norm == "dnp":
        testing_missing_weight = (
            TESTING_MISSING_SIGNAL_WEIGHT_DNP_QB
            if position == "QB"
            else TESTING_MISSING_SIGNAL_WEIGHT_DNP_NON_QB
        )
        evidence_testing_missing_weight = EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT_DNP
        athletic_missing_risk_factor = ATHLETIC_MISSING_RISK_FACTOR_DNP
    else:
        testing_missing_weight = TESTING_MISSING_SIGNAL_WEIGHT_QB if position == "QB" else TESTING_MISSING_SIGNAL_WEIGHT_NON_QB
        evidence_testing_missing_weight = EVIDENCE_GUARDRAIL_TESTING_MISSING_WEIGHT
        athletic_missing_risk_factor = 1.0
    if has_partial_ras:
        athletic_missing_risk_factor *= 0.35
    elif strong_athletic_profile and not has_official_ras:
        athletic_missing_risk_factor *= 0.55
    missing_signal_count = missing_non_testing_count + (testing_missing_weight * testing_missing_count)
    missing_signal_count_raw = missing_non_testing_count + testing_missing_count

    risk_penalty = 0.0
    if espn_volatility_flag:
        risk_penalty += 1.7
    if pp_risk_flag:
        risk_penalty += 1.0
    risk_penalty += max(0.0, float(kiper_volatility_penalty or 0.0))
    risk_penalty += max(0.0, float(tdn_risk_penalty or 0.0))
    risk_penalty += max(0.0, float(br_risk_penalty or 0.0))
    risk_penalty += max(0.0, float(atoz_risk_penalty or 0.0))
    risk_penalty += max(0.0, float(si_risk_penalty or 0.0))
    risk_penalty += max(0.0, float(cbs_risk_penalty or 0.0))
    risk_penalty += (ATHLETIC_MISSING_RISK_WEIGHT * athletic_profile_missing_penalty * athletic_missing_risk_factor)
    risk_penalty += (ATHLETIC_VARIANCE_RISK_WEIGHT * athletic_profile_variance_penalty * athletic_missing_risk_factor)
    risk_penalty += years_played_risk_adjustment
    if ENABLE_DRAFT_AGE_SCORING:
        risk_penalty += draft_age_risk_adjustment
    if ENABLE_EARLY_DECLARE_SCORING:
        risk_penalty += early_declare_risk_adjustment
    # Data-sufficiency penalty: sparse profiles should not sit near the top on neutral defaults.
    if position == "QB":
        risk_penalty += 0.55 * missing_signal_count
        if missing_non_testing_count >= 3:
            risk_penalty += 0.8
        if not (has_film_signal or has_language_signal or has_testing_signal or has_market_signal):
            risk_penalty += 1.2
    else:
        risk_penalty += 0.22 * missing_signal_count
        if missing_non_testing_count >= 3:
            risk_penalty += 0.35
        # Thin proxy production for non-QB profiles gets a small risk tax.
        if (
            cfb_player_available
            and str(cfb_prod_quality_label or "").strip().lower() == "proxy"
            and int(cfb_prod_coverage_count) <= 1
        ):
            risk_penalty += 0.45 if position in {"EDGE", "DT", "LB"} else 0.25
    if int(lang.get("lang_risk_flag", 0) or 0) == 1:
        risk_penalty += 0.6
    if position == "QB" and analyst_score < 40:
        risk_penalty += 0.4
    if missing_non_testing_count >= 3:
        context_component -= 1.0 if position == "QB" else 0.5

    trait_w, prod_w, athletic_w, size_w, context_w = 0.38, 0.24, 0.18, 0.10, 0.10
    if position in {"EDGE", "DT", "LB"}:
        # De-emphasize production for front-seven to avoid pass-rush stat spikes driving ranks.
        trait_w, prod_w, athletic_w, size_w, context_w = 0.42, 0.18, 0.18, 0.10, 0.12

    raw_formula_score = (
        (trait_w * trait_component)
        + (prod_w * production_component)
        + (athletic_w * athletic_component)
        + (size_w * size_component)
        + (context_w * context_component)
        - risk_penalty
    )
    # Calibrate to scouting-grade scale so round-value mapping is realistic.
    calibrated_grade = (1.22 * raw_formula_score) - 2.0
    calibrated_grade = max(55.0, min(95.0, calibrated_grade))
    prior_grade = max(55.0, min(95.0, 60.0 + 0.33 * prior_signal))
    calibrated_blend = _clamp(float(GRADE_CALIBRATED_BLEND), 0.60, 0.90)
    prior_blend = _clamp(float(GRADE_PRIOR_BLEND), 0.10, 0.40)
    blend_total = calibrated_blend + prior_blend
    if blend_total <= 0:
        calibrated_blend, prior_blend = 0.78, 0.22
    else:
        calibrated_blend = calibrated_blend / blend_total
        prior_blend = prior_blend / blend_total
    final_grade = (calibrated_blend * calibrated_grade) + (prior_blend * prior_grade)
    final_grade += float(POSITION_VALUE_ADJUSTMENT.get(position, 0.0))
    final_grade = max(55.0, min(95.0, final_grade))

    evidence_missing_count_raw = missing_non_testing_count + testing_missing_count
    evidence_missing_count_adjusted = missing_non_testing_count + (
        evidence_testing_missing_weight if testing_missing_count else 0.0
    )
    evidence_missing_count = int(round(evidence_missing_count_adjusted))
    evidence_guardrail_penalty = 0.0
    if final_grade >= UPSIDE_EVIDENCE_GUARDRAIL_ACTIVE_GRADE:
        if official_ras is None and athletic_evidence_count <= 1 and evidence_missing_count_adjusted >= 2.0:
            evidence_guardrail_penalty = min(
                UPSIDE_EVIDENCE_GUARDRAIL_MAX_PENALTY,
                evidence_missing_count_adjusted * UPSIDE_EVIDENCE_GUARDRAIL_PENALTY_PER_MISSING,
            )
            final_grade = max(55.0, min(95.0, final_grade - evidence_guardrail_penalty))

    floor = max(52.0, final_grade - (1.8 + risk_penalty))
    ceiling = min(97.0, final_grade + (2.0 if class_year.upper() in {"SO", "RSO", "JR", "RSJ"} else 1.3))

    if position == "QB":
        production_max_up_delta = PRODUCTION_SIGNAL_QB_MAX_DELTA
        production_max_down_delta = PRODUCTION_SIGNAL_QB_MAX_DELTA
    elif position in {"EDGE", "DT", "LB"}:
        production_max_up_delta = PRODUCTION_SIGNAL_FRONT7_MAX_UP_DELTA
        production_max_down_delta = PRODUCTION_SIGNAL_FRONT7_MAX_DOWN_DELTA
    else:
        production_max_up_delta = PRODUCTION_SIGNAL_MAX_DELTA
        production_max_down_delta = PRODUCTION_SIGNAL_MAX_DELTA

    return {
        "formula_trait_component": round(trait_component, 2),
        "formula_trait_component_raw": round(trait_component_raw, 2),
        "formula_trait_proxy_component": round(trait_proxy_component, 2),
        "formula_trait_anchor_weight": round(trait_anchor_weight, 3),
        "formula_production_component_raw": round(production_component_raw, 2),
        "formula_production_component": round(production_component, 2),
        "formula_production_guardrail_delta": round(production_guardrail_delta, 2),
        "formula_athletic_component": round(athletic_component, 2),
        "formula_athletic_evidence_count": athletic_evidence_count,
        "formula_athletic_source": athletic_source,
        "formula_athletic_source_confidence": round(_athletic_source_confidence(ras_source or athletic_source), 2),
        "formula_athletic_cap_applied": athletic_cap_applied,
        "athletic_profile_score": athletic_profile.get("athletic_profile_score", ""),
        "athletic_speed_score": athletic_profile.get("athletic_speed_score", ""),
        "athletic_explosion_score": athletic_profile.get("athletic_explosion_score", ""),
        "athletic_agility_score": athletic_profile.get("athletic_agility_score", ""),
        "athletic_size_adj_score": athletic_profile.get("athletic_size_adj_score", ""),
        "athletic_metric_coverage_count": athletic_profile.get("athletic_metric_coverage_count", ""),
        "athletic_metric_expected_count": athletic_profile.get("athletic_metric_expected_count", ""),
        "athletic_metric_missing_count": athletic_profile.get("athletic_metric_missing_count", ""),
        "athletic_metric_coverage_rate": athletic_profile.get("athletic_metric_coverage_rate", ""),
        "athletic_missing_penalty": athletic_profile.get("athletic_missing_penalty", ""),
        "athletic_variance_penalty": athletic_profile.get("athletic_variance_penalty", ""),
        "athletic_hit_bin": athletic_profile.get("athletic_hit_bin", ""),
        "athletic_hit_bin_sample_n": athletic_profile.get("athletic_hit_bin_sample_n", ""),
        "athletic_hit_rate_round12_bin": athletic_profile.get("athletic_hit_rate_round12_bin", ""),
        "athletic_hit_rate_top100_bin": athletic_profile.get("athletic_hit_rate_top100_bin", ""),
        "athletic_comp_confidence": athletic_profile.get("athletic_comp_confidence", ""),
        "athletic_nn_comp_1": athletic_profile.get("athletic_nn_comp_1", ""),
        "athletic_nn_comp_1_year": athletic_profile.get("athletic_nn_comp_1_year", ""),
        "athletic_nn_comp_1_picktotal": athletic_profile.get("athletic_nn_comp_1_picktotal", ""),
        "athletic_nn_comp_1_similarity": athletic_profile.get("athletic_nn_comp_1_similarity", ""),
        "athletic_nn_comp_2": athletic_profile.get("athletic_nn_comp_2", ""),
        "athletic_nn_comp_2_year": athletic_profile.get("athletic_nn_comp_2_year", ""),
        "athletic_nn_comp_2_picktotal": athletic_profile.get("athletic_nn_comp_2_picktotal", ""),
        "athletic_nn_comp_2_similarity": athletic_profile.get("athletic_nn_comp_2_similarity", ""),
        "athletic_nn_comp_3": athletic_profile.get("athletic_nn_comp_3", ""),
        "athletic_nn_comp_3_year": athletic_profile.get("athletic_nn_comp_3_year", ""),
        "athletic_nn_comp_3_picktotal": athletic_profile.get("athletic_nn_comp_3_picktotal", ""),
        "athletic_nn_comp_3_similarity": athletic_profile.get("athletic_nn_comp_3_similarity", ""),
        "athletic_pct_forty": athletic_profile.get("athletic_pct_forty", ""),
        "athletic_pct_ten_split": athletic_profile.get("athletic_pct_ten_split", ""),
        "athletic_pct_vertical": athletic_profile.get("athletic_pct_vertical", ""),
        "athletic_pct_broad": athletic_profile.get("athletic_pct_broad", ""),
        "athletic_pct_bench": athletic_profile.get("athletic_pct_bench", ""),
        "athletic_pct_shuttle": athletic_profile.get("athletic_pct_shuttle", ""),
        "athletic_pct_three_cone": athletic_profile.get("athletic_pct_three_cone", ""),
        "athletic_pct_height_in": athletic_profile.get("athletic_pct_height_in", ""),
        "athletic_pct_weight_lb": athletic_profile.get("athletic_pct_weight_lb", ""),
        "athletic_pct_arm_in": athletic_profile.get("athletic_pct_arm_in", ""),
        "athletic_pct_hand_in": athletic_profile.get("athletic_pct_hand_in", ""),
        "athletic_z_forty": athletic_profile.get("athletic_z_forty", ""),
        "athletic_z_ten_split": athletic_profile.get("athletic_z_ten_split", ""),
        "athletic_z_vertical": athletic_profile.get("athletic_z_vertical", ""),
        "athletic_z_broad": athletic_profile.get("athletic_z_broad", ""),
        "athletic_z_bench": athletic_profile.get("athletic_z_bench", ""),
        "athletic_z_shuttle": athletic_profile.get("athletic_z_shuttle", ""),
        "athletic_z_three_cone": athletic_profile.get("athletic_z_three_cone", ""),
        "athletic_z_height_in": athletic_profile.get("athletic_z_height_in", ""),
        "athletic_z_weight_lb": athletic_profile.get("athletic_z_weight_lb", ""),
        "athletic_z_arm_in": athletic_profile.get("athletic_z_arm_in", ""),
        "athletic_z_hand_in": athletic_profile.get("athletic_z_hand_in", ""),
        "formula_size_component": round(size_component, 2),
        "formula_context_component": round(context_component, 2),
        "formula_years_played": round(float(years_played), 2) if years_played is not None else "",
        "formula_years_played_bucket": years_played_bucket,
        "formula_years_played_context_adjustment": round(years_played_context_adjustment, 2),
        "formula_years_played_risk_adjustment": round(years_played_risk_adjustment, 2),
        "formula_draft_age": round(float(draft_age), 3) if draft_age is not None else "",
        "formula_draft_age_bucket": draft_age_bucket,
        "formula_draft_age_context_adjustment": round(draft_age_context_adjustment, 2),
        "formula_draft_age_risk_adjustment": round(draft_age_risk_adjustment, 2),
        "formula_early_declare": int(bool(early_declare)),
        "formula_early_declare_context_adjustment": round(early_declare_context_adjustment, 2),
        "formula_early_declare_risk_adjustment": round(early_declare_risk_adjustment, 2),
        "formula_testing_missing_status": testing_status_norm,
        "formula_testing_missing_weight": round(testing_missing_weight, 3),
        "formula_athletic_missing_risk_factor": round(athletic_missing_risk_factor, 3),
        "formula_risk_penalty": round(risk_penalty, 2),
        "formula_missing_signal_count_raw": missing_signal_count_raw,
        "formula_missing_signal_count_weighted": round(missing_signal_count, 2),
        "formula_evidence_missing_count": evidence_missing_count,
        "formula_evidence_missing_count_raw": evidence_missing_count_raw,
        "formula_evidence_missing_count_weighted": round(evidence_missing_count_adjusted, 2),
        "formula_evidence_guardrail_penalty": round(evidence_guardrail_penalty, 2),
        "formula_raw_score": round(raw_formula_score, 2),
        "formula_calibrated_grade": round(calibrated_grade, 2),
        "formula_prior_signal": round(prior_signal, 2),
        "formula_prior_grade": round(prior_grade, 2),
        "formula_score": round(final_grade, 2),
        "formula_floor": round(floor, 2),
        "formula_ceiling": round(ceiling, 2),
        "formula_round_value": round_from_grade(final_grade),
        "weight_production_multiplier": round(
            PRODUCTION_SIGNAL_QB_MULTIPLIER
            if position == "QB"
            else PRODUCTION_SIGNAL_FRONT7_MULTIPLIER
            if position in {"EDGE", "DT", "LB"}
            else PRODUCTION_SIGNAL_MULTIPLIER,
            3,
        ),
        "weight_production_max_delta": round(production_max_up_delta, 2),
        "weight_production_max_up_delta": round(production_max_up_delta, 2),
        "weight_production_max_down_delta": round(production_max_down_delta, 2),
    }


def _consensus_guardrail_penalty(
    *,
    position: str,
    external_rank: int | None,
    analyst_score: float,
    pff_grade: float | None,
    language_trait: float | None,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
) -> float:
    """Softly suppress low-consensus outliers unless supported by strong evidence."""
    penalty = 0.0
    if position == "QB":
        if external_rank is None:
            penalty += 1.4
        elif external_rank > 150:
            penalty += 2.4
        elif external_rank > 100:
            penalty += 1.8
        elif external_rank > 75:
            penalty += 1.0
        elif external_rank > 50:
            penalty += 0.5
    else:
        if external_rank is not None:
            if external_rank > 250:
                penalty += 3.0
            elif external_rank > 200:
                penalty += 2.3
            elif external_rank > 150:
                penalty += 1.6
            elif external_rank > 100:
                penalty += 1.0

    if analyst_score < 35:
        penalty += 1.0 if position == "QB" else 0.8
    elif analyst_score < 45:
        penalty += 0.6 if position == "QB" else 0.4

    # Let premium production/traits offset some consensus drag.
    if pff_grade is not None and pff_grade >= 85.0:
        penalty -= 0.5 if position == "QB" else 0.8
    elif pff_grade is not None and pff_grade >= 80.0:
        penalty -= 0.25 if position == "QB" else 0.4

    if language_trait is not None and language_trait >= 60.0:
        penalty -= 0.3 if position == "QB" else 0.4

    if consensus_mean_rank is not None:
        consensus_conf = 1.0 if consensus_source_count >= 3 else 0.65 if consensus_source_count == 2 else 0.35
        if position == "QB":
            if consensus_mean_rank > 120:
                penalty += 1.4 * consensus_conf
            elif consensus_mean_rank > 95:
                penalty += 0.9 * consensus_conf
            elif consensus_mean_rank > 75:
                penalty += 0.4 * consensus_conf
        else:
            if consensus_mean_rank > 210:
                penalty += 1.8 * consensus_conf
            elif consensus_mean_rank > 170:
                penalty += 1.3 * consensus_conf
            elif consensus_mean_rank > 140:
                penalty += 0.9 * consensus_conf
            elif consensus_mean_rank > 110:
                penalty += 0.5 * consensus_conf

    max_cap = 5.5 if position == "QB" else 4.5
    return max(0.0, min(max_cap, penalty))


def _consensus_outlier_cap(
    *,
    position: str,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
) -> float | None:
    """
    Cross-position cap for persistent low-consensus outliers.
    Only applied when at least two consensus sources are present.
    """
    if consensus_mean_rank is None or consensus_source_count < 2:
        return None

    if position == "QB":
        if consensus_mean_rank > 140:
            return 76.0
        if consensus_mean_rank > 115:
            return 77.5
        if consensus_mean_rank > 90:
            return 79.0
        return None

    if consensus_mean_rank > 220:
        return 74.5
    if consensus_mean_rank > 185:
        return 76.0
    if consensus_mean_rank > 155:
        return 77.0
    if consensus_mean_rank > 130:
        return 78.0
    if consensus_mean_rank > 105:
        return 79.5
    return None


def _front7_pass_rush_inflation_penalty(
    *,
    position: str,
    is_diamond_exception: bool,
    production_component: float,
    production_guardrail_delta: float,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    consensus_rank_std: float | None,
    cfb_prod_available: bool,
    cfb_prod_quality_label: str,
    cfb_prod_reliability: float,
    cfb_prod_coverage_count: int,
    evidence_missing_count: int,
    roi_pick_band: str,
    roi_adjustment_applied: float,
    roi_sample_n: int,
    roi_surplus_z: float | None,
    calibrated_success_prob: str | float | None,
) -> tuple[float, str]:
    """
    Production inflation brake for EDGE/DT/LB.
    Uses ROI and calibration outputs so pass-rush spikes refine, not rewrite, ranks.
    """
    if position not in {"EDGE", "DT", "LB"} or is_diamond_exception:
        return 0.0, ""

    rank = float(consensus_mean_rank) if consensus_mean_rank is not None else None
    prod_up = max(0.0, float(production_component) - PRODUCTION_SIGNAL_NEUTRAL)
    prod_delta = max(0.0, float(production_guardrail_delta))
    if prod_up < 1.10 and prod_delta < 0.70:
        return 0.0, ""
    confidence = _consensus_confidence_factor(
        consensus_source_count=consensus_source_count,
        consensus_rank_std=consensus_rank_std,
    )

    penalty = 0.0
    reasons: list[str] = []

    base = max(0.0, (prod_up - 0.95) * 0.15) + max(0.0, (prod_delta - 0.45) * 0.12)
    penalty += base
    if base > 0:
        reasons.append(f"prod_up={prod_up:.2f}")

    if rank is not None:
        rank_mult = 0.0
        if rank > 100:
            rank_mult = 0.95
        elif rank > 70:
            rank_mult = 0.72
        elif rank > 45:
            rank_mult = 0.50
        elif rank > 25:
            rank_mult = 0.28
        if rank_mult > 0:
            r_pen = min(0.65, rank_mult * max(0.0, prod_up - 0.8) * 0.34)
            penalty += r_pen
            if r_pen > 0:
                reasons.append(f"rank={rank:.1f}")

    if consensus_source_count <= 2:
        penalty += 0.18 * confidence
        reasons.append("low_consensus_sources")

    roi_band = str(roi_pick_band or "").strip().upper()
    roi_adj = float(roi_adjustment_applied or 0.0)
    if roi_adj > 0 and roi_band in {"R3", "R4", "R5+"}:
        roi_pen = min(0.45, roi_adj * 2.6)
        penalty += roi_pen
        if roi_pen > 0:
            reasons.append(f"roi_{roi_band}_plus")
        if roi_sample_n < 40:
            penalty += 0.14
            reasons.append("roi_small_sample")
        if roi_surplus_z is not None and float(roi_surplus_z) >= 1.2:
            penalty += 0.10
            reasons.append("roi_high_z")

    quality = str(cfb_prod_quality_label or "").strip().lower()
    if quality == "proxy" and float(cfb_prod_reliability or 0.0) <= 0.35 and prod_up >= 1.4:
        penalty += 0.28
        reasons.append("proxy_prod")
    elif (not cfb_prod_available) and prod_up >= 2.0:
        penalty += 0.16
        reasons.append("no_cfb_prod")

    # Extra sparse-profile brake: pass-rush spikes with thin evidence should not dominate top ranks.
    if quality in {"proxy", "mixed"} and rank is not None and rank >= 55.0 and prod_up >= 1.2:
        sparse_pen = 0.0
        if float(cfb_prod_reliability or 0.0) <= 0.45:
            sparse_pen += 0.16
        if int(cfb_prod_coverage_count) <= 2:
            sparse_pen += 0.14
        if int(evidence_missing_count) >= 2:
            sparse_pen += min(0.22, 0.08 * (int(evidence_missing_count) - 1))
        if sparse_pen > 0:
            penalty += sparse_pen * confidence
            reasons.append("sparse_profile_brake")

    prob = _as_float(calibrated_success_prob)
    if prob is not None and rank is not None:
        expected_floor = 0.60
        if rank <= 20:
            expected_floor = 0.84
        elif rank <= 35:
            expected_floor = 0.80
        elif rank <= 50:
            expected_floor = 0.76
        elif rank <= 75:
            expected_floor = 0.71
        elif rank <= 100:
            expected_floor = 0.66
        if prob < expected_floor:
            cal_pen = min(0.85, (expected_floor - prob) * 4.5)
            penalty += cal_pen
            if cal_pen > 0:
                reasons.append(f"cal_prob<{expected_floor:.2f}")
    elif rank is not None and rank > 40:
        penalty += 0.10
        reasons.append("no_cal_prob")

    penalty = round(_clamp(penalty, 0.0, FRONT7_INFLATION_BRAKE_MAX), 4)
    return penalty, ";".join(reasons)


def _cb_nickel_inflation_penalty(
    *,
    position: str,
    is_diamond_exception: bool,
    height_in: int | None,
    weight_lb: int | None,
    production_component: float,
    production_guardrail_delta: float,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    consensus_rank_std: float | None,
    cfb_prod_quality_label: str,
    cfb_prod_reliability: float,
    cfb_prod_coverage_count: int,
    cfb_prod_proxy_fallback_features: int,
    external_rank: int | None,
    pff_grade: float | None,
) -> tuple[float, str]:
    """
    Targeted inflation brake for smaller/nickel-style CB profiles when production signals
    materially outrun consensus support. This is a soft penalty, not a hard cap.
    """
    if position != "CB" or is_diamond_exception:
        return 0.0, ""
    if consensus_mean_rank is None or consensus_source_count < 2:
        return 0.0, ""

    rank = float(consensus_mean_rank)
    if rank <= 35.0:
        return 0.0, ""
    prod_up = max(0.0, float(production_component) - (PRODUCTION_SIGNAL_NEUTRAL + 0.6))
    prod_delta = max(0.0, float(production_guardrail_delta))
    if prod_up < 0.9 and prod_delta < 0.7:
        return 0.0, ""

    is_nickel_frame = False
    if height_in is not None and weight_lb is not None:
        is_nickel_frame = int(height_in) <= 71 and int(weight_lb) <= 192

    # Only apply when consensus is meaningfully lower than model momentum,
    # or when profile looks like a smaller nickel with outsized production lift.
    if rank < 45.0 and not (is_nickel_frame and (prod_up >= 2.3 or prod_delta >= 1.9)):
        return 0.0, ""

    confidence = _consensus_confidence_factor(
        consensus_source_count=consensus_source_count,
        consensus_rank_std=consensus_rank_std,
    )
    penalty = 0.0
    reasons: list[str] = []

    if rank > 95.0:
        penalty += 0.62
    elif rank > 80.0:
        penalty += 0.48
    elif rank > 65.0:
        penalty += 0.34
    elif rank > 50.0:
        penalty += 0.22

    penalty += max(0.0, prod_up - 0.85) * 0.19
    penalty += max(0.0, prod_delta - 0.45) * 0.16
    if is_nickel_frame:
        penalty += 0.14
        reasons.append("nickel_frame")

    quality = str(cfb_prod_quality_label or "").strip().lower()
    if quality == "proxy":
        penalty += 0.24
        reasons.append("proxy_prod")
    elif quality == "mixed" and float(cfb_prod_reliability or 0.0) <= 0.55:
        penalty += 0.10
        reasons.append("mixed_low_rel")

    if int(cfb_prod_coverage_count) <= 2:
        penalty += 0.16
        reasons.append("thin_cfb_cov")
    if int(cfb_prod_proxy_fallback_features) >= 1:
        penalty += 0.10
        reasons.append("proxy_fallback")

    # Keep true independently-supported outliers alive.
    if external_rank is not None and int(external_rank) <= 55:
        penalty -= 0.22
        reasons.append("ext_support")
    if pff_grade is not None and float(pff_grade) >= 84.0:
        penalty -= 0.18
        reasons.append("pff_support")

    penalty = _clamp(penalty * confidence, 0.0, NICKEL_CB_INFLATION_BRAKE_MAX)
    if penalty <= 0:
        return 0.0, ""
    reasons.append(f"rank={rank:.1f}")
    reasons.append(f"prod_up={prod_up:.2f}")
    return round(penalty, 4), ";".join(reasons)


def _diamond_exception_profile(
    *,
    position: str,
    rank_seed: int,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    trait_score: float,
    production_score: float,
    athletic_score: float,
    risk_penalty: float,
    language_coverage: float,
    pff_grade: float | None,
    external_rank: int | None,
    official_ras: float | None,
    impact_target_ras: float | None,
) -> tuple[bool, str]:
    if consensus_mean_rank is None or consensus_source_count < 2:
        return False, ""
    if consensus_mean_rank <= 100:
        return False, ""

    dislocation = consensus_mean_rank - float(rank_seed)
    if dislocation < 45:
        return False, ""

    prod_ok = production_score >= 85.0
    trait_ok = trait_score >= 82.0
    ras_impact_ok = (
        official_ras is not None
        and impact_target_ras is not None
        and official_ras >= impact_target_ras
    )
    ath_ok = athletic_score >= 83.0 or ras_impact_ok
    risk_ok = risk_penalty <= 2.2

    coverage_points = 0
    if external_rank is not None:
        coverage_points += 1
    if pff_grade is not None:
        coverage_points += 1
    if official_ras is not None:
        coverage_points += 1
    if language_coverage >= 40.0:
        coverage_points += 1
    coverage_ok = coverage_points >= 2

    is_exception = prod_ok and trait_ok and ath_ok and risk_ok and coverage_ok
    if not is_exception:
        return False, ""

    reasons = [
        f"dislocation={dislocation:.1f}",
        "prod>=85",
        "trait>=82",
        "athletic>=83_or_ras_impact",
        "risk<=2.2",
        f"coverage_points={coverage_points}",
    ]
    return True, ";".join(reasons)


def _contrarian_watch_score(
    *,
    rank_seed: int,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    trait_score: float,
    production_score: float,
    athletic_score: float,
    size_score: float,
    context_score: float,
    risk_penalty: float,
) -> float:
    base = (
        0.32 * trait_score
        + 0.30 * production_score
        + 0.22 * athletic_score
        + 0.08 * size_score
        + 0.08 * context_score
        - (risk_penalty * 4.0)
    )
    if consensus_mean_rank is not None:
        dislocation = max(0.0, consensus_mean_rank - float(rank_seed))
        dislocation_bonus = min(8.0, dislocation / 20.0)
    else:
        dislocation_bonus = 0.0
    coverage_bonus = min(3.0, float(consensus_source_count))
    score = base + dislocation_bonus + (0.5 * coverage_bonus)
    return round(max(0.0, min(100.0, score)), 2)


def _seed_consensus_drift_penalty(
    *,
    position: str,
    rank_seed: int,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
) -> float:
    """
    Additional suppression when seed rank is far better than market consensus.
    """
    if consensus_mean_rank is None or consensus_source_count < 2:
        return 0.0

    delta = float(consensus_mean_rank) - float(rank_seed)
    if delta <= 45.0:
        return 0.0

    if position == "QB":
        penalty = min(4.0, 0.9 + ((delta - 45.0) / 24.0))
    else:
        penalty = min(3.4, 0.7 + ((delta - 45.0) / 30.0))
    return round(max(0.0, penalty), 2)


def _consensus_confidence_factor(
    *,
    consensus_source_count: int,
    consensus_rank_std: float | None,
) -> float:
    """
    Confidence multiplier for consensus-based suppressors.
    Lower confidence (fewer sources / split boards) => lower suppression.
    """
    if consensus_source_count >= 4:
        source_factor = 1.0
    elif consensus_source_count == 3:
        source_factor = 0.9
    elif consensus_source_count == 2:
        source_factor = 0.78
    else:
        source_factor = 0.6

    if consensus_rank_std is None:
        split_factor = 0.85
    elif consensus_rank_std <= 8.0:
        split_factor = 1.0
    elif consensus_rank_std <= 14.0:
        split_factor = 0.9
    elif consensus_rank_std <= 22.0:
        split_factor = 0.78
    else:
        split_factor = 0.62

    return round(_clamp(source_factor * split_factor, 0.45, 1.05), 3)


def _rank_sort_consensus_realign_adjustment(
    *,
    position: str,
    rank_seed: int,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    consensus_rank_std: float | None,
) -> float:
    """
    Soft ranking-only nudge that reduces extreme model-vs-consensus dislocation.
    This does not change final_grade/model_score; it only affects rank ordering.
    """
    if consensus_mean_rank is None or consensus_source_count < 2:
        return 0.0

    dislocation = float(consensus_mean_rank) - float(rank_seed)
    deadband = max(0.0, float(RANK_CONSENSUS_REALIGN_DEADBAND))
    if abs(dislocation) <= deadband:
        return 0.0

    confidence = _consensus_confidence_factor(
        consensus_source_count=consensus_source_count,
        consensus_rank_std=consensus_rank_std,
    )
    position_multiplier = {
        "QB": 0.85,
        "OT": 0.95,
        "EDGE": 0.95,
        "CB": 0.95,
        "WR": 1.0,
        "DT": 1.0,
        "LB": 1.0,
        "S": 1.05,
        "IOL": 1.0,
        "RB": 1.15,
        "TE": 1.10,
    }.get(position, 1.0)

    scale = max(1.0, float(RANK_CONSENSUS_REALIGN_SCALE))
    max_adjustment = max(0.0, float(RANK_CONSENSUS_REALIGN_MAX))
    magnitude = min(max_adjustment, ((abs(dislocation) - deadband) / scale) * max_adjustment)
    adjustment = magnitude * confidence * position_multiplier

    # Positive dislocation means model seed is much better than consensus.
    # Apply a drag. Negative dislocation gets a small upward nudge.
    if dislocation > 0:
        adjustment *= -1.0
    return round(adjustment, 4)


def _has_espn_trait_evidence(espn_row: dict) -> bool:
    trait_keys = (
        "espn_trait_processing",
        "espn_trait_separation",
        "espn_trait_play_strength",
        "espn_trait_motor",
        "espn_trait_instincts",
    )
    for key in trait_keys:
        val = _as_float(espn_row.get(key))
        if val is not None:
            return True
    return bool(str(espn_row.get("espn_text_coverage", "")).strip())


def _top50_independent_evidence_signals(
    *,
    consensus_source_count: int,
    external_rank: int | None,
    pff_grade: float | None,
    cfb_prod_available: bool,
    espn_row: dict,
) -> tuple[int, str]:
    """
    Returns independent-signal count/labels used for top-50 rank-only evidence guardrails.
    """
    labels: list[str] = []
    if int(consensus_source_count) >= 2:
        labels.append("consensus_multi_source")
    if external_rank is not None:
        labels.append("external_rank")
    if pff_grade is not None or bool(cfb_prod_available):
        labels.append("production")
    if _has_espn_trait_evidence(espn_row):
        labels.append("espn_traits")
    return len(labels), ";".join(labels)


def _top50_evidence_rank_brake_penalty(signal_count: int) -> float:
    missing = max(0, int(TOP50_EVIDENCE_MIN_SIGNALS) - int(signal_count))
    if missing <= 0:
        return 0.0
    # Soft ranking-only brake. One missing independent signal gets a mild drag;
    # zero evidence gets a stronger drag while still allowing elite profiles through.
    penalty = float(TOP50_EVIDENCE_BRAKE_BASE) + max(0, missing - 1) * float(TOP50_EVIDENCE_BRAKE_PER_MISSING)
    return round(_clamp(penalty, 0.0, float(TOP50_EVIDENCE_BRAKE_MAX)), 4)


def _apply_top50_evidence_rank_brake(final_rows: list[dict]) -> None:
    if not final_rows:
        return
    if TOP50_EVIDENCE_APPLY_TOP_N <= 0 or TOP50_EVIDENCE_MIN_SIGNALS <= 0:
        return

    top_n = max(1, int(TOP50_EVIDENCE_APPLY_TOP_N))
    for row in final_rows:
        base = float(
            _as_float(row.get("rank_sort_score_base"))
            or _as_float(row.get("rank_sort_score"))
            or _as_float(row.get("consensus_score"))
            or 0.0
        )
        row["rank_sort_score_base"] = round(base, 4)
        row["rank_sort_score"] = round(base, 4)
        row["top50_evidence_brake_penalty"] = 0.0
        row["top50_evidence_brake_applied"] = 0
        row["top50_evidence_brake_reason"] = ""

    penalized_keys_prev: set[str] = set()
    for _ in range(max(1, int(TOP50_EVIDENCE_REBALANCE_PASSES))):
        final_rows.sort(
            key=lambda x: (
                float(x.get("rank_sort_score", x.get("consensus_score", 0.0)) or 0.0),
                float(x.get("consensus_score", 0.0) or 0.0),
            ),
            reverse=True,
        )
        penalized: dict[str, float] = {}
        for row in final_rows[:top_n]:
            signal_count = int(_as_float(row.get("top50_evidence_signal_count")) or 0)
            penalty = _top50_evidence_rank_brake_penalty(signal_count)
            if penalty <= 0:
                continue
            penalized[canonical_player_name(row.get("player_name", ""))] = penalty

        penalized_keys = set(penalized.keys())
        if penalized_keys == penalized_keys_prev:
            break
        penalized_keys_prev = penalized_keys

        for row in final_rows:
            base = float(
                _as_float(row.get("rank_sort_score_base"))
                or _as_float(row.get("rank_sort_score"))
                or _as_float(row.get("consensus_score"))
                or 0.0
            )
            row["rank_sort_score"] = round(base, 4)
            row["top50_evidence_brake_penalty"] = 0.0
            row["top50_evidence_brake_applied"] = 0
            row["top50_evidence_brake_reason"] = ""

        for row in final_rows:
            key = canonical_player_name(row.get("player_name", ""))
            penalty = penalized.get(key)
            if penalty is None:
                continue
            base = float(
                _as_float(row.get("rank_sort_score_base"))
                or _as_float(row.get("rank_sort_score"))
                or _as_float(row.get("consensus_score"))
                or 0.0
            )
            row["top50_evidence_brake_penalty"] = round(penalty, 4)
            row["top50_evidence_brake_applied"] = 1
            row["top50_evidence_brake_reason"] = (
                f"top{top_n}_evidence_lt_{TOP50_EVIDENCE_MIN_SIGNALS}"
            )
            row["rank_sort_score"] = round(base - float(penalty), 4)
            summary = str(row.get("rank_driver_summary", "")).strip()
            if summary:
                row["rank_driver_summary"] = (
                    f"{summary} | top50_evidence_brake:{-float(penalty):+.2f}"
                )


def _bluechip_rank_protection_adjustment(
    *,
    consensus_score: float,
    external_rank: int | None,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    uncertainty_score: float,
    rank_sort_total_drag: float,
    evidence_signal_count: int,
) -> float:
    """
    Rank-only soft lift for high-grade, market-supported profiles that can be
    over-demoted by thin/noisy rank drag effects.
    """
    if not BLUECHIP_RANK_PROTECTION_ENABLED:
        return 0.0
    if consensus_score < 84.0:
        return 0.0
    if evidence_signal_count < 2:
        return 0.0

    strong_market = False
    if external_rank is not None and int(external_rank) <= 45:
        strong_market = True
    if (
        consensus_mean_rank is not None
        and consensus_source_count >= 3
        and float(consensus_mean_rank) <= 45.0
    ):
        strong_market = True
    if not strong_market:
        return 0.0

    if rank_sort_total_drag < 0.55 and uncertainty_score < 60.0:
        return 0.0

    if consensus_score >= 88.0:
        base = 0.65
    elif consensus_score >= 86.0:
        base = 0.5
    else:
        base = 0.35

    if external_rank is not None and int(external_rank) <= 20:
        base += 0.2
    elif (
        consensus_mean_rank is not None
        and consensus_source_count >= 4
        and float(consensus_mean_rank) <= 20.0
    ):
        base += 0.2

    pressure = max(0.0, rank_sort_total_drag - 0.55)
    adj = base + (0.35 * pressure)
    return round(_clamp(adj, 0.0, BLUECHIP_RANK_PROTECTION_MAX), 4)


def _write_rank_vs_consensus_outputs(final_rows: list[dict]) -> None:
    rows = []
    for row in final_rows:
        mean_rank = _as_float(row.get("consensus_board_mean_rank"))
        if mean_rank is None:
            continue
        model_rank = int(_as_float(row.get("consensus_rank")) or 9999)
        if model_rank >= 9999:
            continue
        delta = float(mean_rank) - float(model_rank)
        rows.append(
            {
                "consensus_rank": model_rank,
                "player_name": row.get("player_name", ""),
                "position": row.get("position", ""),
                "school": row.get("school", ""),
                "final_grade": row.get("final_grade", ""),
                "consensus_board_mean_rank": round(float(mean_rank), 2),
                "consensus_board_source_count": row.get("consensus_board_source_count", ""),
                "consensus_board_rank_std": row.get("consensus_board_rank_std", ""),
                "rank_delta_consensus_minus_model": round(delta, 2),
                "rank_delta_abs": round(abs(delta), 2),
                "model_higher_than_consensus": 1 if delta > 0 else 0,
                "model_lower_than_consensus": 1 if delta < 0 else 0,
                "confidence_score": row.get("confidence_score", ""),
                "uncertainty_score": row.get("uncertainty_score", ""),
                "top50_evidence_signal_count": row.get("top50_evidence_signal_count", ""),
                "top50_evidence_brake_penalty": row.get("top50_evidence_brake_penalty", ""),
                "bluechip_rank_protection_adjustment": row.get("bluechip_rank_protection_adjustment", ""),
                "rank_driver_summary": row.get("rank_driver_summary", ""),
            }
        )

    rows.sort(
        key=lambda r: (
            float(_as_float(r.get("rank_delta_abs")) or 0.0),
            -int(_as_float(r.get("consensus_rank")) or 9999),
        ),
        reverse=True,
    )
    write_csv(OUTPUTS / "big_board_2026_rank_vs_consensus.csv", rows)

    top100 = [
        r
        for r in rows
        if int(_as_float(r.get("consensus_rank")) or 9999) <= 100
    ]
    top100.sort(
        key=lambda r: float(_as_float(r.get("rank_delta_abs")) or 0.0),
        reverse=True,
    )
    write_csv(OUTPUTS / "top100_disagreement_audit_2026.csv", top100)

    lines = [
        "Top-100 Disagreement Audit (Model Rank vs Consensus Mean Rank)",
        "",
        f"Rows: {len(top100)}",
        "",
        "ModelRank | Player | Pos | Grade | ConsensusMean | Delta(consensus-model) | AbsDelta | Sources | Driver",
    ]
    for row in top100[:120]:
        lines.append(
            f"{row['consensus_rank']} | {row['player_name']} | {row['position']} | {row['final_grade']} | "
            f"{row['consensus_board_mean_rank']} | {row['rank_delta_consensus_minus_model']} | "
            f"{row['rank_delta_abs']} | {row['consensus_board_source_count']} | {row['rank_driver_summary']}"
        )
    (OUTPUTS / "top100_disagreement_audit_2026.txt").write_text("\n".join(lines))


def _run_postbuild_eligibility_qa(
    *,
    final_rows: list[dict],
    returning_names: set[str],
    declared_underclassmen: set[str],
    already_drafted_names: set[str],
) -> None:
    failures: list[dict] = []
    for row in final_rows:
        name = str(row.get("player_name", "")).strip()
        name_key = canonical_player_name(name)
        class_year = str(row.get("class_year", "")).strip().upper()
        if name_key in returning_names:
            failures.append(
                {
                    "player_name": name,
                    "position": row.get("position", ""),
                    "school": row.get("school", ""),
                    "class_year": class_year,
                    "reason": "returning_to_school",
                }
            )
        if name_key in already_drafted_names:
            failures.append(
                {
                    "player_name": name,
                    "position": row.get("position", ""),
                    "school": row.get("school", ""),
                    "class_year": class_year,
                    "reason": "already_in_nfl",
                }
            )
        if not (is_senior_class(class_year) or name_key in declared_underclassmen):
            failures.append(
                {
                    "player_name": name,
                    "position": row.get("position", ""),
                    "school": row.get("school", ""),
                    "class_year": class_year,
                    "reason": "non_senior_not_declared",
                }
            )

    failures.sort(key=lambda r: (r.get("reason", ""), r.get("player_name", "")))
    qa_csv_path = OUTPUTS / "postbuild_ineligible_players_qa_2026.csv"
    if failures:
        write_csv(qa_csv_path, failures)
    else:
        qa_csv_path.parent.mkdir(parents=True, exist_ok=True)
        with qa_csv_path.open("w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["player_name", "position", "school", "class_year", "reason"],
            )
            writer.writeheader()
    qa_lines = [
        "Postbuild Ineligible Player QA (2026)",
        "",
        f"Rows: {len(failures)}",
        "",
        "Reason | Player | Pos | School | ClassYear",
    ]
    for row in failures[:250]:
        qa_lines.append(
            f"{row.get('reason','')} | {row.get('player_name','')} | {row.get('position','')} | "
            f"{row.get('school','')} | {row.get('class_year','')}"
        )
    (OUTPUTS / "postbuild_ineligible_players_qa_2026.txt").write_text("\n".join(qa_lines))

    if FAIL_ON_POSTBUILD_INELIGIBLE and failures:
        print(
            "Postbuild ineligible-player QA failed: "
            f"{len(failures)} rows. "
            f"See {qa_csv_path}"
        )
        raise SystemExit(2)


def _midband_consensus_brake_penalty(
    *,
    position: str,
    rank_seed: int,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    consensus_rank_std: float | None,
    external_rank: int | None,
    pff_grade: float | None,
    language_trait: float | None,
) -> float:
    """
    Mild brake for model-vs-consensus drift in the mid band.
    Purpose: reduce extreme top-50 leaps without killing legitimate outliers.
    """
    if consensus_mean_rank is None or consensus_source_count < 2:
        return 0.0
    if consensus_mean_rank < 40.0 or consensus_mean_rank > 90.0:
        return 0.0

    confidence = _consensus_confidence_factor(
        consensus_source_count=consensus_source_count,
        consensus_rank_std=consensus_rank_std,
    )

    # Base pressure from consensus mid-band rank.
    base = 0.25 + ((float(consensus_mean_rank) - 40.0) / 50.0) * 1.35
    base = _clamp(base, 0.0, 1.8)

    # Mild extra pressure when seed rank materially outruns consensus.
    delta = float(consensus_mean_rank) - float(rank_seed)
    if delta > 35.0:
        base += min(0.35, (delta - 35.0) / 40.0)

    # Credits for strong independent support so real outliers can still climb.
    if external_rank is not None:
        if external_rank <= 40:
            base -= 0.35
        elif external_rank <= 60:
            base -= 0.15
    if pff_grade is not None:
        if pff_grade >= 90.0:
            base -= 0.25
        elif pff_grade >= 85.0:
            base -= 0.15
    if language_trait is not None and language_trait >= 60.0:
        base -= 0.15

    pos_mult = {
        "QB": 1.25,
        "CB": 0.85,
        "EDGE": 0.9,
    }.get(position, 1.0)
    return round(_clamp(base * confidence * pos_mult, 0.0, 1.8), 2)


def _position_band_soft_ceiling_target(
    *,
    position: str,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    consensus_rank_std: float | None,
) -> float | None:
    """
    Soft (non-hard) consensus ceiling target for selected positions.
    Applied as a mild penalty above the target, not a hard clamp.
    """
    if position not in {"CB", "EDGE", "S"}:
        return None
    if consensus_mean_rank is None or consensus_source_count < 2:
        return None
    if consensus_mean_rank < 40.0 or consensus_mean_rank > 90.0:
        return None

    # Base targets by position and consensus band.
    if position == "CB":
        if consensus_mean_rank > 80.0:
            target = 82.9
        elif consensus_mean_rank > 65.0:
            target = 83.6
        elif consensus_mean_rank > 50.0:
            target = 84.2
        else:
            target = 84.6
    elif position == "EDGE":
        if consensus_mean_rank > 80.0:
            target = 83.1
        elif consensus_mean_rank > 65.0:
            target = 83.8
        elif consensus_mean_rank > 50.0:
            target = 84.5
        else:
            target = 84.9
    else:  # S
        if consensus_mean_rank > 80.0:
            target = 82.8
        elif consensus_mean_rank > 65.0:
            target = 83.5
        elif consensus_mean_rank > 50.0:
            target = 84.1
        else:
            target = 84.5

    # Split consensus => allow more freedom (higher target).
    confidence = _consensus_confidence_factor(
        consensus_source_count=consensus_source_count,
        consensus_rank_std=consensus_rank_std,
    )
    target += (1.0 - confidence) * 0.8
    return round(target, 2)


def _soft_ceiling_penalty(model_score: float, soft_target: float | None) -> float:
    if soft_target is None or model_score <= soft_target:
        return 0.0
    # Soft penalty scales with exceedance; not a hard clamp.
    over = model_score - soft_target
    return round(min(1.8, 0.55 * over), 2)


def _consensus_tail_soft_penalty(
    *,
    position: str,
    model_score: float,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    consensus_rank_std: float | None,
    external_rank: int | None,
    pff_grade: float | None,
    language_trait: float | None,
    is_diamond_exception: bool,
) -> tuple[float, float | None]:
    """
    Cross-position soft brake for consensus-tail profiles (mean rank ~90-150).
    This is intentionally mild and support-aware so it refines, not rewrites, ranks.
    """
    if is_diamond_exception:
        return 0.0, None
    if consensus_mean_rank is None or consensus_source_count < 2:
        return 0.0, None

    mean_rank = float(consensus_mean_rank)
    if mean_rank < 90.0 or mean_rank > 150.0:
        return 0.0, None

    confidence = _consensus_confidence_factor(
        consensus_source_count=consensus_source_count,
        consensus_rank_std=consensus_rank_std,
    )

    # Base target by consensus tail band.
    if mean_rank > 135.0:
        target = 78.8
    elif mean_rank > 120.0:
        target = 79.4
    elif mean_rank > 105.0:
        target = 80.0
    else:
        target = 80.6

    # Premium positions can carry slightly higher model scores in this band.
    if position == "QB":
        target += 0.45
    elif position in {"OT", "EDGE", "CB"}:
        target += 0.20

    # Independent support lifts target and avoids false suppression.
    if external_rank is not None:
        if external_rank <= 50:
            target += 0.80
        elif external_rank <= 75:
            target += 0.45
        elif external_rank <= 100:
            target += 0.15
    if pff_grade is not None:
        if pff_grade >= 88.0:
            target += 0.55
        elif pff_grade >= 84.0:
            target += 0.25
    if language_trait is not None and language_trait >= 60.0:
        target += 0.20

    # Split boards => loosen braking.
    target += (1.0 - confidence) * 0.50

    if model_score <= target:
        return 0.0, round(target, 2)

    over = model_score - target
    penalty = min(1.6, (0.55 * over * confidence) + (0.10 if mean_rank > 120.0 else 0.0))
    return round(max(0.0, penalty), 2), round(target, 2)


def _consensus_bluechip_floor(
    *,
    external_rank: int | None,
    consensus_mean_rank: float | None,
    consensus_source_count: int,
    consensus_rank_std: float | None,
    analyst_score: float,
    pff_grade: float | None,
) -> float | None:
    """
    Protect consensus-validated blue-chip profiles from collapsing due sparse/noisy
    trait parsing or thin production artifacts.
    """
    anchor_rank = float(external_rank) if external_rank is not None else consensus_mean_rank
    if anchor_rank is None:
        return None
    if consensus_source_count < 4 and external_rank is None:
        return None
    if analyst_score < 68.0:
        return None

    floor = None
    if anchor_rank <= 5:
        floor = 83.2
    elif anchor_rank <= 10:
        floor = 82.6
    elif anchor_rank <= 16:
        floor = 81.9
    elif anchor_rank <= 24:
        floor = 81.2
    elif anchor_rank <= 32:
        floor = 80.6
    if floor is None:
        return None

    if pff_grade is not None and float(pff_grade) >= 86.0:
        floor += 0.20
    if consensus_rank_std is not None and float(consensus_rank_std) > 18.0:
        floor -= 0.20
    if consensus_source_count <= 4:
        floor -= 0.10
    return round(_clamp(floor, 80.4, 84.8), 2)


def _consensus_hard_cap(
    *,
    position: str,
    rank_seed: int,
    external_rank: int | None,
    analyst_score: float,
    pff_grade: float | None,
    language_trait: float | None,
) -> float | None:
    """
    Enforce a ceiling for low-consensus players unless there is elite independent support.
    """
    if position == "QB":
        # QB-specific protection: if consensus and testing/film signals are weak, keep ceiling conservative.
        if external_rank is None:
            if rank_seed > 45:
                return 76.0
            if rank_seed > 25:
                return 77.5
            if rank_seed > 10:
                return 79.0
            return None
        if external_rank > 150:
            return 77.0
        if external_rank > 100:
            return 78.5
        if external_rank > 75:
            return 80.0
        if external_rank > 50:
            return 81.5
        return None

    if external_rank is None:
        return None

    # Preserve true top-seed prospects; cap logic is for fringe/outlier suppression.
    if rank_seed <= 10:
        return None

    # Allow true outliers to break through if multiple support signals are strong.
    elite_support = False
    if pff_grade is not None and pff_grade >= 92.0 and analyst_score >= 65.0:
        elite_support = True
    if language_trait is not None and language_trait >= 64.0 and analyst_score >= 62.0:
        elite_support = True
    if elite_support:
        return None

    # Top-seed prospects still get a softer cap band.
    if rank_seed <= 25:
        if external_rank > 250:
            return 80.5
        if external_rank > 200:
            return 82.0
        if external_rank > 150:
            return 83.5
        if external_rank > 100:
            return 85.0
        return None

    if external_rank > 250:
        return 75.0
    if external_rank > 200:
        return 76.5
    if external_rank > 150:
        return 78.0
    if external_rank > 100:
        return 80.0
    return None


def _compact_text(value: str, max_chars: int = 180) -> str:
    txt = " ".join(str(value or "").replace("\n", " ").split())
    if not txt:
        return ""
    if len(txt) <= max_chars:
        return txt
    return txt[: max_chars - 3].rstrip() + "..."


_SCOUTING_GLOSSARY_CACHE: dict[str, dict] | None = None
_SCOUTING_LANGUAGE_CACHE: dict[tuple[str, str, str], dict] | None = None


def _scouting_key(name: str, position: str = "", school: str = "") -> tuple[str, str, str]:
    return (
        canonical_player_name(name or ""),
        normalize_pos(position or ""),
        canonical_player_name(school or ""),
    )


def _load_scouting_glossary() -> dict[str, dict]:
    global _SCOUTING_GLOSSARY_CACHE
    if _SCOUTING_GLOSSARY_CACHE is not None:
        return _SCOUTING_GLOSSARY_CACHE
    terms_by_id: dict[str, dict] = {}
    if SCOUTING_GLOSSARY_PATH.exists():
        with SCOUTING_GLOSSARY_PATH.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                term_id = str(row.get("term_id", "")).strip()
                if not term_id:
                    continue
                terms_by_id[term_id] = {k: str(v or "").strip() for k, v in row.items()}
    _SCOUTING_GLOSSARY_CACHE = terms_by_id
    return terms_by_id


def _load_scouting_language_inputs() -> dict[tuple[str, str, str], dict]:
    global _SCOUTING_LANGUAGE_CACHE
    if _SCOUTING_LANGUAGE_CACHE is not None:
        return _SCOUTING_LANGUAGE_CACHE
    rows: dict[tuple[str, str, str], dict] = {}
    if SCOUTING_LANGUAGE_INPUTS_PATH.exists():
        with SCOUTING_LANGUAGE_INPUTS_PATH.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                name = str(row.get("player_name", "")).strip()
                pos = str(row.get("position", "")).strip()
                school = str(row.get("school", "")).strip()
                if not name or not pos:
                    continue
                clean_row = {k: str(v or "").strip() for k, v in row.items()}
                rows[_scouting_key(name, pos, school)] = clean_row
                rows.setdefault(_scouting_key(name, pos, ""), clean_row)
    _SCOUTING_LANGUAGE_CACHE = rows
    return rows


def _get_scouting_language_row(name: str, position: str, school: str) -> dict:
    rows = _load_scouting_language_inputs()
    return rows.get(_scouting_key(name, position, school), rows.get(_scouting_key(name, position, ""), {}))


def _phrase_from_term(term: dict) -> str:
    sample = _compact_text(term.get("sample_phrase", ""), 180)
    if sample:
        return sample.rstrip(".") + "."
    plain = _compact_text(term.get("plain_english", ""), 180)
    return plain.rstrip(".") + "." if plain else ""


def _lookup_glossary_terms(term_ids: list[str], *, section: str) -> list[dict]:
    glossary = _load_scouting_glossary()
    out: list[dict] = []
    seen: set[str] = set()
    for term_id in term_ids:
        row = glossary.get(term_id)
        if not row:
            continue
        if row.get("audience") != "public_safe":
            continue
        if row.get("section") != section:
            continue
        if term_id in seen:
            continue
        seen.add(term_id)
        out.append(row)
    return out


def _default_glossary_tags(
    *,
    pos: str,
    qb_epa: float | None,
    qb_press: float | None,
    wr_yprr: float | None,
    wr_share: float | None,
    rb_explosive: float | None,
    rb_mtf: float | None,
    edge_pr: float | None,
    edge_sacks_pr: float | None,
    db_plays_ball: float | None,
    db_yards_cov: float | None,
    shuttle_pct: float | None,
    cone_pct: float | None,
    forty_pct: float | None,
    ten_pct: float | None,
    arm_pct: float | None,
    weight_pct: float | None,
) -> list[str]:
    tags: list[str] = []
    if pos == "QB":
        if qb_epa is not None and qb_epa >= 0.20:
            tags.extend(["qb_anticipation_distributor", "qb_mental_processing"])
        else:
            tags.append("qb_structure_creation_hybrid")
        if ((forty_pct or 0) >= 65) or ((ten_pct or 0) >= 65):
            tags.append("qb_play_speed")
        if qb_press is not None and qb_press < 0.0:
            tags.append("qb_pressure_sensitive")
    elif pos == "RB":
        if rb_explosive is not None and rb_explosive >= 0.14:
            tags.append("rb_explosive_runner")
        if rb_mtf is not None and rb_mtf >= 0.24:
            tags.append("rb_contact_creator")
        if rb_mtf is not None and rb_mtf >= 0.18:
            tags.append("rb_functional_athleticism")
        tags.append("rb_passing_game_utility")
    elif pos == "WR":
        if wr_share is not None and wr_share >= 0.22:
            tags.append("wr_volume_target_earner")
        if wr_yprr is not None and wr_yprr >= 2.0:
            tags.append("wr_route_craft_separator")
        if forty_pct is not None and forty_pct >= 65:
            tags.append("wr_vertical_stressor")
        if ((shuttle_pct or 0) >= 65) or ((cone_pct or 0) >= 65):
            tags.append("wr_twitchy_mover")
        tags.append("wr_press_answers")
    elif pos == "TE":
        tags.append("te_inline_move_mismatch")
        if arm_pct is not None and arm_pct < 20:
            tags.append("te_anchor_point")
    elif pos == "OT":
        tags.extend(["ol_anchor", "ol_inside_out_recovery"])
        tags.append("ol_anchor_consistency")
    elif pos == "IOL":
        tags.extend(["ol_anchor", "ol_inside_out_recovery"])
        tags.append("ol_anchor_consistency")
    elif pos == "EDGE":
        if edge_pr is not None and edge_pr >= 0.14:
            tags.append("edge_true_pass_set_winner")
        if edge_sacks_pr is not None and edge_sacks_pr >= 0.02:
            tags.append("edge_clean_pocket_finisher")
        if ((cone_pct or 0) >= 45) or ((shuttle_pct or 0) >= 45):
            tags.append("edge_bend")
        if weight_pct is not None and weight_pct >= 35:
            tags.append("edge_set_the_edge")
        tags.append("edge_rush_plan_depth")
    elif pos == "DT":
        tags.append("dt_one_gap_disruptor")
        if weight_pct is not None and weight_pct >= 30:
            tags.append("dl_point_of_attack")
        tags.append("dt_stack_and_shed")
    elif pos == "LB":
        tags.append("lb_run_fit_anchor")
        if ((shuttle_pct or 0) >= 50) or ((forty_pct or 0) >= 50):
            tags.extend(["lb_coverage_range", "lb_play_speed"])
        if edge_pr is not None and edge_pr >= 0.10:
            tags.append("lb_pressure_utility")
        if weight_pct is not None and weight_pct >= 25:
            tags.append("lb_stack_and_shed")
    elif pos in {"CB", "S"}:
        if db_plays_ball is not None and db_plays_ball >= 0.22:
            tags.append("db_ball_disruptor")
        if db_yards_cov is not None and db_yards_cov <= 1.05:
            tags.append("db_target_deterrent")
        if ((shuttle_pct or 0) >= 55) or ((cone_pct or 0) >= 55):
            tags.extend(["db_click_and_close", "db_reactive_athleticism"])
        tags.append("db_scheme_translator")
        tags.append("db_angle_discipline")
    deduped: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if tag in seen:
            continue
        seen.add(tag)
        deduped.append(tag)
    return deduped


def _cfb_prod_snapshot_label(position: str, cfb: dict) -> str:
    def _fmt_int(value) -> str:
        val = _as_float(value)
        if val is None:
            return ""
        return f"{int(round(val)):,}"

    pos = normalize_pos(position)
    if pos == "QB":
        yds = _fmt_int(cfb.get("cfb_qb_pass_yds"))
        td = _fmt_int(cfb.get("cfb_qb_pass_td"))
        itc = _fmt_int(cfb.get("cfb_qb_pass_int"))
        comp = _fmt_int(cfb.get("cfb_qb_pass_comp"))
        att = _fmt_int(cfb.get("cfb_qb_pass_att"))
        rush_yds = _fmt_int(cfb.get("cfb_qb_rush_yds"))
        rush_td = _fmt_int(cfb.get("cfb_qb_rush_td"))
        parts = []
        if yds:
            parts.append(f"passing yards {yds}")
        if td or itc:
            if td and itc:
                parts.append(f"pass TD-INT {td}-{itc}")
            elif td:
                parts.append(f"pass TD {td}")
            else:
                parts.append(f"INT {itc}")
        if comp and att:
            parts.append(f"completions/attempts {comp}/{att}")
        if rush_yds or rush_td:
            rush = []
            if rush_yds:
                rush.append(f"{rush_yds} rush yds")
            if rush_td:
                rush.append(f"{rush_td} rush TD")
            parts.append(", ".join(rush))
        if parts:
            return "; ".join(parts)
        # Fallback to existing context if counting stats are unavailable.
        fallback = []
        if str(cfb.get("cfb_qb_epa_per_play", "")).strip():
            fallback.append(f"QB EPA/play {cfb.get('cfb_qb_epa_per_play')}")
        if str(cfb.get("cfb_qb_pressure_signal", "")).strip():
            fallback.append(f"QB pressure signal {cfb.get('cfb_qb_pressure_signal')}")
        if str(cfb.get("cfb_opp_def_toughness_index", "")).strip():
            fallback.append(f"opp-def index {cfb.get('cfb_opp_def_toughness_index')}")
        return "; ".join(fallback)
    if pos in {"WR", "TE"}:
        rec = _fmt_int(cfb.get("cfb_wrte_rec"))
        yds = _fmt_int(cfb.get("cfb_wrte_rec_yds"))
        td = _fmt_int(cfb.get("cfb_wrte_rec_td"))
        parts = []
        if rec:
            parts.append(f"receptions {rec}")
        if yds:
            parts.append(f"receiving yards {yds}")
        if td:
            parts.append(f"receiving TD {td}")
        if parts:
            return "; ".join(parts)
        # Fallback to existing context if counting stats are unavailable.
        parts = []
        if str(cfb.get("cfb_wrte_yprr", "")).strip():
            parts.append(f"YPRR {cfb.get('cfb_wrte_yprr')}")
        if str(cfb.get("cfb_wrte_targets_per_route", "")).strip():
            parts.append(f"targets/route {cfb.get('cfb_wrte_targets_per_route')}")
        if str(cfb.get("cfb_wrte_target_share", "")).strip():
            parts.append(f"target share {cfb.get('cfb_wrte_target_share')}")
        if str(cfb.get("cfb_opp_def_toughness_index", "")).strip():
            parts.append(f"opp-def index {cfb.get('cfb_opp_def_toughness_index')}")
        return "; ".join(parts)
    if pos == "RB":
        att = _fmt_int(cfb.get("cfb_rb_rush_att"))
        yds = _fmt_int(cfb.get("cfb_rb_rush_yds"))
        td = _fmt_int(cfb.get("cfb_rb_rush_td"))
        rec = _fmt_int(cfb.get("cfb_rb_rec"))
        rec_yds = _fmt_int(cfb.get("cfb_rb_rec_yds"))
        rec_td = _fmt_int(cfb.get("cfb_rb_rec_td"))
        parts = []
        if att:
            parts.append(f"rush attempts {att}")
        if yds:
            parts.append(f"rush yards {yds}")
        if td:
            parts.append(f"rush TD {td}")
        if rec or rec_yds or rec_td:
            rec_parts = []
            if rec:
                rec_parts.append(f"{rec} rec")
            if rec_yds:
                rec_parts.append(f"{rec_yds} rec yds")
            if rec_td:
                rec_parts.append(f"{rec_td} rec TD")
            parts.append(", ".join(rec_parts))
        if parts:
            return "; ".join(parts)
        # Fallback to existing context if counting stats are unavailable.
        parts = []
        if str(cfb.get("cfb_rb_explosive_rate", "")).strip():
            parts.append(f"explosive run rate {cfb.get('cfb_rb_explosive_rate')}")
        if str(cfb.get("cfb_rb_missed_tackles_forced_per_touch", "")).strip():
            parts.append(f"MTF/touch {cfb.get('cfb_rb_missed_tackles_forced_per_touch')}")
        if str(cfb.get("cfb_opp_def_toughness_index", "")).strip():
            parts.append(f"opp-def index {cfb.get('cfb_opp_def_toughness_index')}")
        return "; ".join(parts)
    if pos == "EDGE":
        sacks = _fmt_int(cfb.get("cfb_edge_sacks"))
        hurries = _fmt_int(cfb.get("cfb_edge_qb_hurries"))
        tfl = _fmt_int(cfb.get("cfb_edge_tfl"))
        tackles = _fmt_int(cfb.get("cfb_edge_tackles"))
        parts = []
        if sacks:
            parts.append(f"sacks {sacks}")
        if hurries:
            parts.append(f"QB hurries {hurries}")
        if tfl:
            parts.append(f"TFL {tfl}")
        if tackles:
            parts.append(f"tackles {tackles}")
        if parts:
            return "; ".join(parts)
        # Fallback to existing context if counting stats are unavailable.
        parts = []
        if str(cfb.get("cfb_edge_pressure_rate", "")).strip():
            parts.append(f"pressure/pr-snap {cfb.get('cfb_edge_pressure_rate')}")
        if str(cfb.get("cfb_edge_sacks_per_pr_snap", "")).strip():
            parts.append(f"sacks/pr-snap {cfb.get('cfb_edge_sacks_per_pr_snap')}")
        return "; ".join(parts)
    if pos == "DT":
        sacks = _fmt_int(cfb.get("cfb_edge_sacks"))
        hurries = _fmt_int(cfb.get("cfb_edge_qb_hurries"))
        tfl = _fmt_int(cfb.get("cfb_edge_tfl"))
        tackles = _fmt_int(cfb.get("cfb_edge_tackles"))
        parts = []
        if tackles:
            parts.append(f"tackles {tackles}")
        if tfl:
            parts.append(f"TFL {tfl}")
        if sacks:
            parts.append(f"sacks {sacks}")
        if hurries:
            parts.append(f"QB hurries {hurries}")
        if parts:
            return "; ".join(parts)
        if str(cfb.get("cfb_opp_def_toughness_index", "")).strip():
            return f"opp-def index {cfb.get('cfb_opp_def_toughness_index')}"
        return ""
    if pos == "LB":
        tackles = _fmt_int(cfb.get("cfb_db_tackles")) or _fmt_int(cfb.get("cfb_edge_tackles"))
        tfl = _fmt_int(cfb.get("cfb_db_tfl")) or _fmt_int(cfb.get("cfb_edge_tfl"))
        sacks = _fmt_int(cfb.get("cfb_edge_sacks"))
        itc = _fmt_int(cfb.get("cfb_db_int"))
        pbu = _fmt_int(cfb.get("cfb_db_pbu"))
        parts = []
        if tackles:
            parts.append(f"tackles {tackles}")
        if tfl:
            parts.append(f"TFL {tfl}")
        if sacks:
            parts.append(f"sacks {sacks}")
        if itc:
            parts.append(f"INT {itc}")
        if pbu:
            parts.append(f"PBUs {pbu}")
        if parts:
            return "; ".join(parts)
        if str(cfb.get("cfb_opp_def_toughness_index", "")).strip():
            return f"opp-def index {cfb.get('cfb_opp_def_toughness_index')}"
        return ""
    if pos in {"CB", "S"}:
        itc = _fmt_int(cfb.get("cfb_db_int"))
        pbu = _fmt_int(cfb.get("cfb_db_pbu"))
        tackles = _fmt_int(cfb.get("cfb_db_tackles"))
        tfl = _fmt_int(cfb.get("cfb_db_tfl"))
        parts = []
        if itc:
            parts.append(f"INT {itc}")
        if pbu:
            parts.append(f"PBUs {pbu}")
        if tackles:
            parts.append(f"tackles {tackles}")
        if tfl:
            parts.append(f"TFL {tfl}")
        if parts:
            return "; ".join(parts)
        # Fallback to existing context if counting stats are unavailable.
        parts = []
        if str(cfb.get("cfb_db_coverage_plays_per_target", "")).strip():
            parts.append(f"coverage plays/target {cfb.get('cfb_db_coverage_plays_per_target')}")
        if str(cfb.get("cfb_db_yards_allowed_per_coverage_snap", "")).strip():
            parts.append(f"yards allowed/cov snap {cfb.get('cfb_db_yards_allowed_per_coverage_snap')}")
        return "; ".join(parts)
    if pos in {"OT", "IOL"}:
        years = _fmt_int(cfb.get("cfb_years_played"))
        quality = str(cfb.get("cfb_prod_quality_label", "")).strip().lower()
        parts = []
        if years:
            parts.append(f"seasons played {years}")
        if quality in {"real", "hybrid", "proxy"}:
            parts.append(f"production profile {quality}")
        if str(cfb.get("cfb_opp_def_toughness_index", "")).strip():
            parts.append(f"opp-def index {cfb.get('cfb_opp_def_toughness_index')}")
        return "; ".join(parts)
    return ""


def _cfb_proxy_audit_label(position: str, cfb: dict) -> str:
    pos = normalize_pos(position)
    if pos in {"WR", "TE"}:
        val = str(cfb.get("cfb_wrte_targets_per_route", "")).strip()
        src = str(cfb.get("cfb_wrte_targets_per_route_source", "")).strip()
        wt = str(cfb.get("cfb_wrte_targets_per_route_weight", "")).strip()
        if val:
            return f"targets/route={val} source={src or 'unknown'} wt={wt or 'n/a'}"
    if pos == "EDGE":
        pr = str(cfb.get("cfb_edge_pressure_rate", "")).strip()
        sk = str(cfb.get("cfb_edge_sacks_per_pr_snap", "")).strip()
        src = str(cfb.get("cfb_edge_sacks_per_pr_snap_source", "")).strip()
        pwt = str(cfb.get("cfb_edge_pressure_weight", "")).strip()
        swt = str(cfb.get("cfb_edge_sack_weight", "")).strip()
        parts = []
        if pr:
            parts.append(f"pressure/pr={pr}")
        if sk:
            parts.append(f"sacks/pr={sk}")
        if src:
            parts.append(f"sack_src={src}")
        if pwt:
            parts.append(f"p_wt={pwt}")
        if swt:
            parts.append(f"s_wt={swt}")
        return " ".join(parts)
    if pos in {"CB", "S"}:
        yacs = str(cfb.get("cfb_db_yards_allowed_per_coverage_snap", "")).strip()
        src = str(cfb.get("cfb_db_yards_allowed_per_cov_snap_source", "")).strip()
        cwt = str(cfb.get("cfb_db_cov_weight", "")).strip()
        ywt = str(cfb.get("cfb_db_yacs_weight", "")).strip()
        if yacs:
            return f"yards_allowed/cov={yacs} source={src or 'unknown'} cov_wt={cwt or 'n/a'} yacs_wt={ywt or 'n/a'}"
    return ""


def _cfb_proxy_fallback_heavy_flag(cfb: dict) -> tuple[int, str]:
    quality = str(cfb.get("cfb_prod_quality_label", "")).strip().lower()
    coverage = int(_as_float(cfb.get("cfb_prod_coverage_count")) or 0)
    fallback_count = int(_as_float(cfb.get("cfb_prod_proxy_fallback_features")) or 0)
    reliability = float(_as_float(cfb.get("cfb_prod_reliability")) or 0.0)

    reasons = []
    if quality == "proxy":
        reasons.append("proxy")
    if coverage <= CFB_PROXY_FALLBACK_HEAVY_MAX_COVERAGE:
        reasons.append(f"low_cov:{coverage}")
    if fallback_count >= CFB_PROXY_FALLBACK_HEAVY_MIN_FALLBACKS:
        reasons.append(f"fallbacks:{fallback_count}")
    if reliability <= CFB_PROXY_FALLBACK_HEAVY_MAX_RELIABILITY:
        reasons.append(f"low_rel:{round(reliability,2)}")

    heavy = (
        quality == "proxy"
        and coverage <= CFB_PROXY_FALLBACK_HEAVY_MAX_COVERAGE
        and fallback_count >= CFB_PROXY_FALLBACK_HEAVY_MIN_FALLBACKS
        and reliability <= CFB_PROXY_FALLBACK_HEAVY_MAX_RELIABILITY
    )
    return (1 if heavy else 0), ("|".join(reasons) if heavy else "")


def _build_scouting_sections(
    *,
    name: str,
    position: str,
    school: str,
    final_grade: float,
    round_value: str,
    best_role: str,
    best_scheme_fit: str,
    best_team_fit: str,
    scouting_notes: str,
    kiper_rank: str,
    kiper_prev_rank: str,
    kiper_rank_delta: str,
    kiper_strength_tags: str,
    kiper_concern_tags: str,
    kiper_statline_2025: str,
    tdn_strengths: str,
    tdn_concerns: str,
    br_strengths: str,
    br_concerns: str,
    atoz_strengths: str,
    atoz_concerns: str,
    si_strengths: str,
    si_concerns: str,
    pff_grade: float | None,
    espn_qbr,
    espn_epa_per_play,
    cfb_prod_signal,
    cfb_prod_label,
    cfb_prod_quality,
    cfb_prod_reliability,
    consensus_rank: int,
    historical_combine_comp_1: str,
    historical_combine_comp_1_year,
    historical_combine_comp_1_similarity,
    combine_height_in,
    combine_weight_lb,
    combine_arm_in,
    athletic_pct_forty,
    athletic_pct_ten_split,
    athletic_pct_vertical,
    athletic_pct_broad,
    athletic_pct_shuttle,
    athletic_pct_three_cone,
    athletic_pct_weight_lb,
    athletic_pct_arm_in,
    cfb_qb_epa_per_play,
    cfb_qb_pressure_signal,
    cfb_qb_pass_int,
    cfb_wrte_yprr,
    cfb_wrte_target_share,
    cfb_rb_explosive_rate,
    cfb_rb_missed_tackles_forced_per_touch,
    cfb_edge_pressure_rate,
    cfb_edge_sacks_per_pr_snap,
    cfb_edge_qb_hurries,
    cfb_db_coverage_plays_per_target,
    cfb_db_yards_allowed_per_coverage_snap,
    cfb_db_int,
    cfb_db_pbu,
) -> dict:
    pos = normalize_pos(position)
    clean_role = " ".join(str(best_role or "").replace("_", " ").split()) or "Role TBD"
    clean_scheme = " ".join(str(best_scheme_fit or "").replace("_", " ").split()) or "Scheme TBD"
    clean_team = " ".join(str(best_team_fit or "").replace("_", " ").split()) or "Team TBD"
    language_row = _get_scouting_language_row(name, pos, school)

    def _phrase_list(*raw_values: str, max_items: int = 8) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in raw_values:
            txt = str(raw or "").replace("_", " ").replace("\n", " ").strip()
            if not txt:
                continue
            parts = re.split(r"[|;/]|,\s*(?=[A-Za-z])", txt)
            if len(parts) <= 1:
                parts = [txt]
            for part in parts:
                phrase = " ".join(part.split()).strip(" .,-")
                if not phrase:
                    continue
                lower = phrase.lower()
                if lower in {"n/a", "na", "none", "pending", "unknown"}:
                    continue
                if len(phrase) > 100:
                    phrase = phrase[:97].rstrip() + "..."
                    lower = phrase.lower()
                if lower in seen:
                    continue
                seen.add(lower)
                out.append(phrase)
                if len(out) >= max_items:
                    return out
        return out

    def _with_article(text: str) -> str:
        phrase = " ".join(str(text or "").split()).strip()
        if not phrase:
            return ""
        article = "an" if phrase[:1].lower() in {"a", "e", "i", "o", "u"} else "a"
        return f"{article} {phrase}"

    def _dedupe_points(points: list[str], limit: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for point in points:
            clean = " ".join(str(point or "").split()).strip()
            if not clean:
                continue
            key = clean.lower().rstrip(".")
            if key in seen:
                continue
            seen.add(key)
            out.append(clean)
            if len(out) >= limit:
                break
        return out

    strengths = _phrase_list(kiper_strength_tags, tdn_strengths, br_strengths, atoz_strengths, si_strengths, max_items=8)
    concerns = _phrase_list(kiper_concern_tags, tdn_concerns, br_concerns, atoz_concerns, si_concerns, max_items=8)

    def _f(value):
        val = _as_float(value)
        return float(val) if val is not None else None

    # Core percentile / production signals reused in both "How He Wins" and "Primary Concerns".
    arm_pct = _f(athletic_pct_arm_in)
    weight_pct = _f(athletic_pct_weight_lb)
    vert_pct = _f(athletic_pct_vertical)
    broad_pct = _f(athletic_pct_broad)
    shuttle_pct = _f(athletic_pct_shuttle)
    cone_pct = _f(athletic_pct_three_cone)
    forty_pct = _f(athletic_pct_forty)
    ten_pct = _f(athletic_pct_ten_split)

    qb_epa = _f(cfb_qb_epa_per_play)
    qb_press = _f(cfb_qb_pressure_signal)
    qb_int = _f(cfb_qb_pass_int)
    wr_yprr = _f(cfb_wrte_yprr)
    wr_share = _f(cfb_wrte_target_share)
    rb_explosive = _f(cfb_rb_explosive_rate)
    rb_mtf = _f(cfb_rb_missed_tackles_forced_per_touch)
    edge_pr = _f(cfb_edge_pressure_rate)
    edge_sacks_pr = _f(cfb_edge_sacks_per_pr_snap)
    edge_hurries = _f(cfb_edge_qb_hurries)
    db_plays_ball = _f(cfb_db_coverage_plays_per_target)
    db_yards_cov = _f(cfb_db_yards_allowed_per_coverage_snap)
    db_int = _f(cfb_db_int)
    db_pbu = _f(cfb_db_pbu)
    glossary_tags = [
        tag.strip()
        for tag in str(language_row.get("glossary_tags", "")).split("|")
        if tag.strip()
    ]
    glossary_tags.extend(
        _default_glossary_tags(
            pos=pos,
            qb_epa=qb_epa,
            qb_press=qb_press,
            wr_yprr=wr_yprr,
            wr_share=wr_share,
            rb_explosive=rb_explosive,
            rb_mtf=rb_mtf,
            edge_pr=edge_pr,
            edge_sacks_pr=edge_sacks_pr,
            db_plays_ball=db_plays_ball,
            db_yards_cov=db_yards_cov,
            shuttle_pct=shuttle_pct,
            cone_pct=cone_pct,
            forty_pct=forty_pct,
            ten_pct=ten_pct,
            arm_pct=arm_pct,
            weight_pct=weight_pct,
        )
    )
    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in glossary_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)
    glossary_tags = deduped_tags
    wins_glossary_terms = _lookup_glossary_terms(glossary_tags, section="How He Wins")
    concern_glossary_terms = _lookup_glossary_terms(glossary_tags, section="Primary Concerns")
    player_how_he_wins_notes = _compact_text(language_row.get("how_he_wins_notes", ""), 210)
    player_primary_concerns_notes = _compact_text(language_row.get("primary_concerns_notes", ""), 210)
    player_role_projection_notes = _compact_text(language_row.get("role_projection_notes", ""), 220)

    def _qb_style_profile() -> str:
        if qb_epa is None and qb_press is None and qb_int is None:
            return "QB style lens: evaluate the full play sequence (drop, eye discipline, trigger timing, and finish) rather than isolated throws."
        if qb_epa is not None and qb_epa >= 0.22 and (qb_int is None or qb_int <= 8):
            return "QB style lens: high-efficiency anticipation distributor; wins by seeing windows early and triggering on time from structure."
        if (forty_pct is not None and forty_pct >= 75) or (ten_pct is not None and ten_pct >= 75):
            return "QB style lens: movement-enabled creator; adds value when structure breaks and can steal hidden yards outside the pocket."
        if qb_press is not None and qb_press < 0.0:
            return "QB style lens: pocket outcomes are pressure-sensitive; projection leans toward controlled game-manager usage until response stabilizes."
        return "QB style lens: pocket-first rhythm passer with intermediate accuracy value; ceiling depends on downfield placement consistency."

    def _receiver_style_profile() -> str:
        # Snap-to-catch separation style inspired by scout process:
        # release -> stem pacing -> break efficiency -> catch-point finish.
        quickness_flag = (shuttle_pct is not None and shuttle_pct >= 65) or (cone_pct is not None and cone_pct >= 65)
        speed_flag = forty_pct is not None and forty_pct >= 65
        target_flag = wr_share is not None and wr_share >= 0.24
        efficiency_flag = wr_yprr is not None and wr_yprr >= 2.4
        hands_flag = arm_pct is not None and arm_pct >= 50

        style_parts: list[str] = []
        if quickness_flag:
            style_parts.append("quickness at breaks")
        if speed_flag:
            style_parts.append("vertical speed stress")
        if target_flag or efficiency_flag:
            style_parts.append("route-volume earning")
        if hands_flag:
            style_parts.append("catch-radius finish")

        if style_parts:
            return "Receiver style lens: separation is driven by " + ", ".join(style_parts[:3]) + "."
        return (
            "Receiver style lens: evaluate from snap to catch — release plan, break mechanics, edge attack, "
            "and contact-point finish — to separate true NFL translatability."
        )

    def _strengths_phrase() -> str:
        cleaned = [s.rstrip(".") for s in strengths[:2] if str(s or "").strip()]
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return f"{cleaned[0]} and {cleaned[1]}"

    def _summary_detail_sentence() -> str:
        strengths_text = _strengths_phrase()
        if pos == "QB":
            if qb_epa is not None and qb_epa >= 0.20:
                core = "The passing profile is driven by on-time structure play, with enough anticipation to stay ahead of coverage rotations"
            elif qb_press is not None and qb_press >= 0.0:
                core = "The projection is cleaner in a managed pocket structure where timing, field mapping, and controlled off-platform answers stay married"
            else:
                core = "The projection still leans on structure and sequencing more than chaos creation, so pocket discipline remains the central swing trait"
            if strengths_text:
                return f"{core}; the best tape flashes come from {strengths_text.lower()}."
            return core + "."
        if pos == "RB":
            if rb_mtf is not None and rb_mtf >= 0.24 and rb_explosive is not None and rb_explosive >= 0.14:
                core = "The best translation comes when his run style can pair contact creation with enough chunk-play stress to keep boxes honest"
            elif rb_mtf is not None and rb_mtf >= 0.24:
                core = "The projection is strongest as a physical chain-mover who creates hidden yards through contact balance and track discipline"
            elif rb_explosive is not None and rb_explosive >= 0.14:
                core = "The profile wins most cleanly when the run game can stress edges and let his burst show up before contact muddies the lane"
            else:
                core = "The projection is cleaner as a schedule-on-time runner than a true space-creator, so efficiency has to carry more of the value"
            if strengths_text:
                return f"{core}; the most bankable snaps come from {strengths_text.lower()}."
            return core + "."
        if pos in {"WR", "TE"}:
            if wr_share is not None and wr_share >= 0.24 and wr_yprr is not None and wr_yprr >= 2.4:
                core = "The profile already looks like a real target earner, with route volume and efficiency supporting a primary-read role in the right menu"
            elif wr_yprr is not None and wr_yprr >= 2.2:
                core = "The receiving translation is strongest when route craft and spacing discipline can create clean separation without forcing constant contested finishes"
            else:
                core = "The role is easier to buy inside a controlled usage lane where releases, leverage work, and finish technique create enough dependable targets"
            if strengths_text:
                return f"{core}; the strongest film moments come from {strengths_text.lower()}."
            return core + "."
        if pos == "OT":
            if arm_pct is not None and arm_pct >= 50 and shuttle_pct is not None and shuttle_pct >= 50:
                core = "The tackle projection is built on cleaner pass-set geometry, recovery movement, and enough length to survive wider NFL rush tracks"
            else:
                core = "The projection works best when his set points, timing, and recovery mechanics stay inside a stable pass-protection environment"
            if strengths_text:
                return f"{core}; the best reps show up through {strengths_text.lower()}."
            return core + "."
        if pos == "IOL":
            if weight_pct is not None and weight_pct >= 45:
                core = "The interior profile is built on pocket firmness and play-strength, with value tied to keeping interior rushers from collapsing launch depth"
            else:
                core = "The projection is strongest in a communication-heavy interior role where leverage, angles, and recovery technique do more of the work than raw mass"
            if strengths_text:
                return f"{core}; the cleaner snaps come through {strengths_text.lower()}."
            return core + "."
        if pos == "EDGE":
            if edge_pr is not None and edge_pr >= 0.16:
                core = "The translation is easiest to buy when first-step stress can force protection issues and let the rush plan work through counters instead of one-move wins"
            else:
                core = "The profile still needs the rush plan to become more down-to-down reliable, so role value currently comes more from controlled deployment than takeover volume"
            if strengths_text:
                return f"{core}; the best tape flashes come via {strengths_text.lower()}."
            return core + "."
        if pos == "DT":
            if edge_pr is not None and edge_pr >= 0.11:
                core = "The interior translation works when he can pair block control with real pocket push, keeping him useful on both early downs and passing situations"
            else:
                core = "The profile is cleaner as an interior tone-setter than a pure disruption bet, so the run-game floor is still carrying more of the projection"
            if strengths_text:
                return f"{core}; his better reps are driven by {strengths_text.lower()}."
            return core + "."
        if pos == "LB":
            if shuttle_pct is not None and shuttle_pct >= 55:
                core = "The linebacker projection is strongest when read/trigger speed and range let him play fast from depth without overrunning the fit"
            else:
                core = "The role is cleaner when the read path is defined and the profile can play downhill without living in difficult space-match situations"
            if strengths_text:
                return f"{core}; the tape is most convincing when {strengths_text.lower()}."
            return core + "."
        if pos == "CB":
            if db_plays_ball is not None and db_plays_ball >= 0.24:
                core = "The corner profile is easiest to trust when leverage discipline turns into real catch-point disruption instead of passive phase coverage"
            else:
                core = "The projection depends on leverage consistency and route recognition holding up often enough to avoid living purely on recovery athleticism"
            if strengths_text:
                return f"{core}; the best snaps come from {strengths_text.lower()}."
            return core + "."
        if pos == "S":
            if db_plays_ball is not None and db_plays_ball >= 0.22:
                core = "The safety projection gains value when range and route anticipation turn into true overlap plays instead of just tidy alignment flexibility"
            else:
                core = "The profile is cleaner in a structure that can let his eyes and angles stay in phase, because the value currently comes more from control than splash"
            if strengths_text:
                return f"{core}; the better tape sequences show up through {strengths_text.lower()}."
            return core + "."
        if strengths_text:
            return f"The projection is strongest when the role stays inside the player’s cleanest lane, with the best tape coming from {strengths_text.lower()}."
        return "The projection is strongest when the role stays inside the player’s cleanest lane and asks for repeatable execution rather than broad expansion."

    wins_logic = {
        "QB": "Film translation comes from timing/processing: ID leverage pre-snap, hold structure from the pocket, and create only when structure breaks.",
        "RB": "Film translation comes from vision plus contact balance: press tracks, force linebacker displacement, then accelerate through daylight.",
        "WR": "Film translation comes from route craft: release with a plan, manipulate leverage in stems, and separate at breakpoints with late hands.",
        "TE": "Film translation comes from dual-phase utility: route detail vs coverage plus in-line toughness that keeps personnel groupings flexible.",
        "OT": "Film translation comes from pass-pro consistency: set points, half-man leverage, and anchor recovery against speed-to-power.",
        "IOL": "Film translation comes from interior control: hand placement, leverage, and communication to pass off stunts without leakage.",
        "EDGE": "Film translation comes from rush sequencing: first-step threat, hand counters, and rush-to-contain discipline by game state.",
        "DT": "Film translation comes from disruption and block control: strike timing, gap integrity, and pocket compression on long downs.",
        "LB": "Film translation comes from read/trigger speed: sort run-pass keys early, fit cleanly, and carry routes with controlled angles.",
        "CB": "Film translation comes from coverage mechanics: maintain leverage, transition cleanly, and finish at the catch point without panic.",
        "S": "Film translation comes from range plus communication: rotate on time, overlap windows, and close space with controlled tackling angles.",
    }

    concern_logic = {
        "QB": "QB projection risk is driven by anticipation timing, pressure response, and turnover discipline.",
        "RB": "RB projection risk is driven by burst creation, tackle-breaking translation, and passing-down value.",
        "WR": "WR projection risk is driven by release efficiency, separation detail, and route precision under press.",
        "TE": "TE projection risk is driven by separation quality against man and functional in-line consistency.",
        "OT": "OT projection risk is driven by length/anchor thresholds and recovery movement versus speed-to-power.",
        "IOL": "IOL projection risk is driven by anchor strength and movement mechanics versus interior games.",
        "EDGE": "EDGE projection risk is driven by first-step burst, bend/cornering, and rush-plan finish rate.",
        "DT": "DT projection risk is driven by block anchor and pass-rush conversion quality from interior alignments.",
        "LB": "LB projection risk is driven by trigger/space movement and block deconstruction consistency.",
        "CB": "CB projection risk is driven by speed/leverage recovery and ball production at the catch point.",
        "S": "S projection risk is driven by range/transition speed and ball-finish consistency in split-field work.",
    }
    concern_depth = {
        "QB": [
            "Pocket disruption response must stay on schedule; drifting off platform creates NFL turnover windows.",
            "Third-down decision speed versus disguised coverage is the key separator between starter and backup outcomes.",
        ],
        "RB": [
            "Early-down production must be paired with dependable pass-protection reps to hold three-down value.",
            "Contact balance has to translate against tighter NFL pursuit angles, not just open-lane college looks.",
        ],
        "WR": [
            "Release plan versus press corners has to hold up when defenders force timing disruption at the line.",
            "Route pacing and stem detail must stay efficient versus complex leverage rotations and trap coverages.",
        ],
        "TE": [
            "Blocking strain has to stay consistent so personnel usage is not telegraphed by down/distance.",
            "Separation profile versus man coverage must improve enough to avoid becoming only a schematic target.",
        ],
        "OT": [
            "Hand timing and recovery mechanics must stay clean when rushers chain counters on long downs.",
            "Anchor consistency versus NFL power fronts is the primary pass-game floor variable.",
        ],
        "IOL": [
            "Interior communication versus games/stunts must be consistent to protect pocket depth for the QB.",
            "Leverage and hand placement must hold late in reps against stronger NFL interior rushers.",
        ],
        "EDGE": [
            "Rush plan depth has to expand beyond first move so pressure translates into consistent finish rate.",
            "Run-game edge setting must stay disciplined to avoid being reduced to a package-only role.",
        ],
        "DT": [
            "Pad-level consistency is required to convert flashes into down-to-down run-defense value.",
            "Pass-rush impact must hold beyond first contact when NFL guards reset and anchor late.",
        ],
        "LB": [
            "Coverage spacing discipline versus route combinations must be clean to avoid explosive seams.",
            "Processing speed has to stay stable when offenses stress eye candy and tempo.",
        ],
        "CB": [
            "Eye discipline through route breaks must tighten to limit transition losses against pro route pacing.",
            "Tackle finish and block deconstruction must hold so offenses cannot isolate him in run support.",
        ],
        "S": [
            "Communication and rotation timing must stay precise to avoid coverage busts in split-field structures.",
            "Open-field tackling angles must remain controlled against NFL speed and spacing stress.",
        ],
    }

    role_projection_logic = {
        "QB": "Early value comes from rhythm passing on early downs, controlled aggression on explosives, and situational command on third down or in two-minute.",
        "RB": "Early value comes from box-count recognition, efficient run tracks, and enough receiving utility to stay on the field in sub packages.",
        "WR": "Early value comes from leverage-based separation, coverage-ID chemistry with the quarterback, and finish strength through contact.",
        "TE": "Early value comes from personnel flexibility, where the same player can stress seams and still survive attached to the formation.",
        "OT": "Early value comes from stable pass sets versus wide alignments and enough run-game displacement without losing balance.",
        "IOL": "Early value comes from interior pocket integrity, line-call chemistry, and efficient climb timing in zone and gap runs.",
        "EDGE": "Early value comes from a true four-down profile that combines rush productivity with edge-setting discipline versus the run.",
        "DT": "Early value comes from early-down run control plus enough third-down pocket push to keep fronts multiple.",
        "LB": "Early value comes from fit integrity, pursuit range, and route-distribution awareness in coverage.",
        "CB": "Early value comes from leverage consistency across man and zone, plus reliable tackle finish when the offense tests him in run support.",
        "S": "Early value comes from rotation versatility, overlap range, and communication that prevents explosive busts.",
    }

    def _sentence(text: str, max_chars: int = 220) -> str:
        clean = _compact_text(text, max_chars)
        if not clean:
            return ""
        return clean.rstrip(".") + "."

    def _useful_scouting_note(text: str) -> str:
        clean = _compact_text(text, 180)
        if not clean:
            return ""
        lower = clean.lower()
        generic_patterns = [
            "instant starter profile",
            "early contributor profile",
            "developmental contributor profile",
            "starter-caliber profile",
            "starter-caliber distributor profile",
        ]
        if any(pattern in lower for pattern in generic_patterns):
            parts = [part.strip(" ;.") for part in clean.split(";")]
            meaningful = [part for part in parts if part and "profile" not in part.lower()]
            clean = "; ".join(meaningful).strip()
        return clean

    def _wins_mechanism_sentence() -> str:
        if pos == "QB":
            if qb_epa is not None and qb_epa >= 0.20:
                return "Wins when he can stay on platform, throw windows open before coverage fully settles, and keep the operation on schedule from structure."
            if (forty_pct is not None and forty_pct >= 75) or (ten_pct is not None and ten_pct >= 75):
                return "Wins by stressing structure with movement ability, then resetting his base quickly enough to keep throws alive after the pocket shifts."
            return "Wins when the drop, eyes, and trigger stay married, because the profile is built more on sequencing and pocket order than pure off-script creation."
        if pos == "RB":
            if rb_explosive is not None and rb_explosive >= 0.14 and rb_mtf is not None and rb_mtf >= 0.24:
                return "Wins by pressing the track with patience, accelerating through narrow entry points, and carrying contact without losing north-south momentum."
            if rb_mtf is not None and rb_mtf >= 0.24:
                return "Wins through contact creation and pad-level control, which lets him steal hidden yards when the first crease closes quickly."
            return "Wins when the read path is clear enough for him to trust the track, get downhill on time, and keep the run on schedule."
        if pos in {"WR", "TE"}:
            if wr_share is not None and wr_share >= 0.24 and wr_yprr is not None and wr_yprr >= 2.4:
                return "Wins by earning targets through pacing, leverage work, and dependable timing into the quarterback's window rather than living on contested variance."
            if (shuttle_pct is not None and shuttle_pct >= 65) or (cone_pct is not None and cone_pct >= 65):
                return "Wins by pacing stems and forcing defenders to turn early, which creates late separation at the breakpoint without needing pure track speed."
            return "Wins when releases stay on time and the route can re-stack leverage before the catch phase, giving him a cleaner target picture through traffic."
        if pos == "OT":
            if arm_pct is not None and arm_pct >= 50 and shuttle_pct is not None and shuttle_pct >= 50:
                return "Wins with set-point control and recovery movement that keep rushers on his edges instead of letting them cross his face into the chest."
            return "Wins when the feet and hands stay synchronized early in the rep, because the profile depends on geometry and recovery more than overpowering contact."
        if pos == "IOL":
            if weight_pct is not None and weight_pct >= 45:
                return "Wins by building a firm pocket from the inside out, anchoring first contact, and passing off movement without giving away launch depth."
            return "Wins when leverage, angles, and hand timing let him stay square through interior games instead of trying to absorb reps with raw mass alone."
        if pos == "EDGE":
            if edge_pr is not None and edge_pr >= 0.16:
                return "Wins by threatening the upfield shoulder early, then cashing that stress into counters once tackles overset to protect the edge."
            return "Wins when the first step forces protection to honor speed, because the rush becomes much cleaner once he can work from tackle panic instead of neutral sets."
        if pos == "DT":
            if edge_pr is not None and edge_pr >= 0.11:
                return "Wins by controlling first contact, compressing the pocket through the guard's frame, and staying disruptive without losing gap integrity."
            return "Wins when strike timing and pad level let him own the block first, because the profile still leans on tone-setting control more than pure interior juice."
        if pos == "LB":
            if shuttle_pct is not None and shuttle_pct >= 55:
                return "Wins by seeing the picture early, fitting through traffic on time, and closing space before the run lane or route window fully opens."
            return "Wins when the read path stays clean enough for him to trigger downhill on time, because the value comes from fit speed and controlled range."
        if pos == "CB":
            if db_plays_ball is not None and db_plays_ball >= 0.24:
                return "Wins by staying patient in phase, preserving leverage through the stem, and attacking the catch point once the receiver declares."
            return "Wins when footwork and eyes stay quiet enough to keep him on top of routes, because the profile depends on controlled transitions more than panic recovery."
        if pos == "S":
            if db_plays_ball is not None and db_plays_ball >= 0.22:
                return "Wins by reading route distribution early, overlapping windows on time, and finishing the rep with real ball disruption instead of passive overlap."
            return "Wins when his eyes, angles, and communication stay in phase, which lets range show up before the route picture gets stretched."
        return "Wins when technique, processing, and deployment stay inside the cleanest lane of the role."

    def _wins_translation_sentence() -> str:
        if pos == "QB":
            return "The cleanest translation is into a structure-first passing game that still leaves room for controlled creation once pressure moves the launch point."
        if pos == "RB":
            return "That style carries best in an offense that values efficient early-down creation and trusts him to hold his own on passing downs."
        if pos in {"WR", "TE"}:
            return "That profile translates best when route detail and leverage manipulation are allowed to create the target rather than forcing him into constant off-schedule catches."
        if pos in {"OT", "IOL"}:
            return "That gives him a cleaner NFL path in a front that values repeatable pass-pro structure and communication over constant recovery chaos."
        if pos in {"EDGE", "DT"}:
            return "That carries best when the front can let him attack with a plan instead of asking every pressure snap to win the same way."
        if pos == "LB":
            return "That translates best in a role that asks him to sort the picture early, fit on time, and use range as a result of clean processing."
        if pos in {"CB", "S"}:
            return "That profile works best when the coverage structure lets his eyes and leverage stay connected to the route picture."
        return "That profile works best when the NFL role keeps his execution inside the current strength lane."

    def _concern_mechanism_sentence() -> str:
        if pos == "QB":
            if qb_press is not None and qb_press < 0.0:
                return "When interior pressure compresses the platform, placement can flatten and force him into late reactions instead of on-time throws."
            if qb_int is not None and qb_int >= 10:
                return "When the picture changes after the top of the drop, turnover-worthy decisions can show up because the answer arrives late."
            return "When the pocket picture muddies, the projection still depends on staying disciplined enough to keep the feet and eyes tied together."
        if pos == "RB":
            if rb_explosive is not None and rb_explosive < 0.12:
                return "When the first lane is not clean, he does not always have the sudden burst to erase a late read and still create chunk yardage."
            if rb_mtf is not None and rb_mtf < 0.20:
                return "When contact arrives early, the run can die on schedule because the profile is not consistently creating extra yards on its own."
            return "When the first picture clouds, he can get too eager to bounce the run instead of trusting the track into efficient north-south space."
        if pos in {"WR", "TE"}:
            if wr_share is not None and wr_share < 0.20:
                return "When defenders disrupt timing early in the rep, target volume can dry up because the route is not consistently re-winning leverage."
            if cone_pct is not None and cone_pct < 35:
                return "When he has to snap off route breaks under tighter spacing, the transition can take too long and squeeze the throw window."
            return "When corners or safeties land first contact into the route, the timing can get compressed before he fully re-stacks leverage."
        if pos == "OT":
            if arm_pct is not None and arm_pct < 20:
                return "When rushers get into his frame first, recovery windows shrink because the length margin is lighter than ideal for NFL edge stress."
            return "When counters arrive after the initial set point, the anchor and recovery have to stay cleaner or the rep spills back into the pocket."
        if pos == "IOL":
            if weight_pct is not None and weight_pct < 25:
                return "When heavier interior rushers land first contact, the pocket can soften because the anchor is working without ideal mass behind it."
            return "When games force him to redirect late, the recovery mechanics still have to prove they can hold up against NFL interior movement."
        if pos == "EDGE":
            if edge_pr is not None and edge_pr < 0.14:
                return "When the first move stalls, the rush can flatten because the second answer is not yet forcing consistent tackle panic."
            if (cone_pct is not None and cone_pct < 35) or (shuttle_pct is not None and shuttle_pct < 35):
                return "When he has to corner tightly through the top of the rush, the bend can run out and turn pressure into a near-miss."
            return "When tackles stay square on the first threat, the rush still needs more sequencing so reps do not end without a clean counter."
        if pos == "DT":
            return "When he loses pad level through first contact, the rep can stall because the profile still needs cleaner conversion from control into disruption."
        if pos == "LB":
            return "When eye candy widens the first step, the fit can get late and force him to recover with speed instead of controlling the angle early."
        if pos == "CB":
            return "When he has to open and redirect from off leverage, the transition can get too linear against sharper in-breakers."
        if pos == "S":
            return "When the route picture changes late, the angle can get stressed because the profile still depends on clean eyes and timely rotation."
        return "When the role expands beyond the current strength lane, the technique has to stay stable enough to survive faster NFL processing speed."

    def _concern_consequence_sentence() -> str:
        if pos == "QB":
            return "That is the swing trait separating a stable starter path from a pressure-sensitive projection."
        if pos == "RB":
            return "That is what determines whether the profile holds three-down value or settles into a narrower early-down lane."
        if pos in {"WR", "TE"}:
            return "That is the difference between dependable target earning and a role that has to be manufactured more aggressively."
        if pos in {"OT", "IOL"}:
            return "That is the floor variable that decides whether he can hold structure against NFL power and movement."
        if pos in {"EDGE", "DT"}:
            return "That is the difference between real four-down disruption and a role that needs more controlled deployment."
        if pos == "LB":
            return "That is the key to keeping the projection in phase against NFL spacing stress instead of living on recovery athleticism."
        if pos in {"CB", "S"}:
            return "That is the trait that decides whether the coverage value is proactive or merely reactive at the next level."
        return "That is the swing factor in the NFL transition."

    def _summary_driver_clause() -> str:
        if pos == "QB":
            return "platform discipline and on-time passing from structure"
        if pos == "RB":
            return "track discipline, contact balance, and enough passing-down value to stay on the field"
        if pos in {"WR", "TE"}:
            return "route pacing, leverage manipulation, and finish timing through the catch phase"
        if pos in {"OT", "IOL"}:
            return "pass-pro structure, leverage control, and recovery timing"
        if pos in {"EDGE", "DT"}:
            return "first-contact stress, rush sequencing, and front-aligned deployment"
        if pos == "LB":
            return "read/trigger speed, block navigation, and range that shows up after the key declares"
        if pos in {"CB", "S"}:
            return "leverage discipline, transition control, and ball-finish timing"
        return "repeatable technique and role clarity"

    def _summary_concern_clause() -> str:
        if pos == "QB":
            return "pressure response and late-down platform stability"
        if pos == "RB":
            return "how much of the passing-down value survives against NFL size and disguise"
        if pos in {"WR", "TE"}:
            return "whether releases and route timing still hold once defenders disrupt the rep early"
        if pos in {"OT", "IOL"}:
            return "whether the anchor and recovery mechanics stay firm against NFL power and movement"
        if pos in {"EDGE", "DT"}:
            return "how reliably the rush gets to a second answer once the first move is stalled"
        if pos == "LB":
            return "processing discipline once offenses start stressing the eyes and fit rules"
        if pos in {"CB", "S"}:
            return "whether transitions and eyes stay connected once route distribution gets sharper"
        return "how the profile holds once the role expands against NFL speed"

    if kiper_rank and kiper_rank_delta:
        try:
            delta = int(float(str(kiper_rank_delta)))
            if delta > 0:
                move_text = f"Kiper movement: up {delta} spots (prev {kiper_prev_rank or 'N/A'} -> now {kiper_rank})."
            elif delta < 0:
                move_text = f"Kiper movement: down {abs(delta)} spots (prev {kiper_prev_rank or 'N/A'} -> now {kiper_rank})."
            else:
                move_text = f"Kiper movement: stable at {kiper_rank}."
        except ValueError:
            move_text = f"Kiper board reference: rank {kiper_rank}."
    elif kiper_rank:
        move_text = f"Kiper board reference: rank {kiper_rank}."
    else:
        move_text = "Kiper board movement unavailable for this player."

    stat_parts: list[str] = []
    if str(kiper_statline_2025 or "").strip():
        stat_parts.append(str(kiper_statline_2025).strip().rstrip("."))
    if str(cfb_prod_label or "").strip():
        stat_parts.append(str(cfb_prod_label).strip().rstrip("."))
    if pos == "QB":
        if str(espn_qbr or "").strip():
            stat_parts.append(f"ESPN QBR {espn_qbr}")
        if str(espn_epa_per_play or "").strip():
            stat_parts.append(f"ESPN EPA/play {espn_epa_per_play}")
    stat_parts = [p for p in stat_parts if p]
    # Keep stable ordering and remove exact duplicates.
    stat_parts = list(dict.fromkeys(stat_parts))
    if stat_parts:
        stat_line = "Stat context: " + _compact_text("; ".join(stat_parts), 260)
        production_snapshot = stat_line
    else:
        production_snapshot = ""

    report_parts = [
        (
            f"{name} ({position}, {school}) projects as a {round_value} talent with his cleanest NFL path coming as "
            f"{_with_article(clean_role.lower())} in {clean_scheme}. The translation case is driven by {_summary_driver_clause()}, "
            f"while the main swing factor is {_summary_concern_clause()}."
        ),
        _compact_text(_summary_detail_sentence(), 235),
        f"The current model grade sits at {final_grade:.2f}, with the board currently slotting him at No. {consensus_rank}; the early NFL path points toward {_with_article(clean_role.lower())} whose value grows if the current strengths hold against better size, speed, and processing.",
    ]
    if player_role_projection_notes:
        report_parts.append(_sentence(player_role_projection_notes, 220))
    else:
        report_parts.append(
            _compact_text(
                role_projection_logic.get(
                    pos,
                    "NFL translation is strongest when deployment and technique stay inside the current role lane.",
                ),
                220,
            )
        )
    report = " ".join(report_parts)

    wins_points: list[str] = []
    if player_how_he_wins_notes:
        wins_points.append(_sentence(player_how_he_wins_notes, 230))
    elif wins_glossary_terms:
        for term in wins_glossary_terms[:2]:
            phrase = _phrase_from_term(term)
            if phrase:
                wins_points.append(_sentence(phrase, 220))
    wins_points.append(_wins_mechanism_sentence())
    wins_points.append(_wins_translation_sentence())
    if pos == "QB":
        wins_points.append(_sentence(_qb_style_profile(), 210))
    elif pos in {"WR", "TE"}:
        wins_points.append(_sentence(_receiver_style_profile(), 210))
    wins = "\n".join(f"- {point}" for point in _dedupe_points(wins_points, 5))

    concern_points: list[str] = []
    if player_primary_concerns_notes:
        concern_points.append(_sentence(player_primary_concerns_notes, 230))
    elif concern_glossary_terms:
        for term in concern_glossary_terms[:2]:
            phrase = _phrase_from_term(term)
            if phrase:
                concern_points.append(_sentence(phrase, 220))
    concern_points.append(_concern_mechanism_sentence())
    concern_points.append(_concern_consequence_sentence())

    if pos == "QB":
        if qb_epa is not None and qb_epa < 0.08:
            concern_points.append(f"Drive efficiency still sits below top-tier starter range (EPA/play {qb_epa:.2f}), so timing and anticipation have to keep climbing.")
        if qb_press is not None and qb_press < 0.0:
            concern_points.append("Pressure response trends negative right now, which raises the risk of pocket drift and late-trigger throws against NFL pressure looks.")
        if qb_int is not None and qb_int >= 10:
            concern_points.append(f"Turnover management still needs cleanup ({int(round(qb_int))} INT) before the high-leverage starter path is fully stable.")
    elif pos == "RB":
        if rb_explosive is not None and rb_explosive < 0.12:
            concern_points.append(f"Explosive-run creation ({rb_explosive*100:.1f}% rate) is still light, which narrows the home-run element of the profile.")
        if rb_mtf is not None and rb_mtf < 0.20:
            concern_points.append(f"Contact-creation efficiency (MTF/touch {rb_mtf:.2f}) is modest, so more yards have to come from blocked space than self-creation.")
        if ten_pct is not None and ten_pct < 40:
            concern_points.append("Short-area burst is below the ideal three-down threshold, which can show up when second-level angles close quickly.")
    elif pos in {"WR", "TE"}:
        if wr_yprr is not None and wr_yprr < 2.0:
            concern_points.append(f"Route-level efficiency (YPRR {wr_yprr:.2f}) is still light for top projection tiers, so the route menu has to keep expanding.")
        if wr_share is not None and wr_share < 0.20:
            concern_points.append(f"Target-earning rate ({wr_share*100:.1f}% share) suggests some volume-translation risk once NFL coverage disrupts timing.")
        if cone_pct is not None and cone_pct < 35:
            concern_points.append("Change-of-direction indicators are below the ideal separator threshold, which can tighten route precision margins against man coverage.")
        if pos == "WR" and arm_pct is not None and arm_pct < 20:
            concern_points.append("Short-arm profile compresses the catch-radius margin at the boundary and through contact, so late placement has less room for error.")
    elif pos == "OT":
        if arm_pct is not None and arm_pct < 20:
            concern_points.append("Short-arm profile projects to tighter recovery windows against long-edge rushers, especially once first contact is lost.")
        if shuttle_pct is not None and shuttle_pct < 35:
            concern_points.append("Lateral recovery movement is below the ideal tackle threshold, which can stress pass-pro consistency once counters start chaining together.")
        if weight_pct is not None and weight_pct < 25:
            concern_points.append("Play-mass profile is light for NFL tackle anchor demands, so speed-to-power conversion will keep testing the pocket depth.")
    elif pos == "IOL":
        if weight_pct is not None and weight_pct < 25:
            concern_points.append("Interior anchor mass is light versus NFL power fronts, creating real pocket-depth stress if first contact is not won cleanly.")
        if shuttle_pct is not None and shuttle_pct < 35:
            concern_points.append("Short-area movement profile may limit late recovery against interior stunts and games once the picture changes post-snap.")
    elif pos == "EDGE":
        if edge_pr is not None and edge_pr < 0.14:
            concern_points.append(f"Pressure rate ({edge_pr*100:.1f}%) is still below the premium EDGE translation zone, so the rush needs more down-to-down disruption.")
        if edge_sacks_pr is not None and edge_sacks_pr < 0.020:
            concern_points.append(f"Finish rate (sacks/pass-rush snap {edge_sacks_pr:.3f}) is light for a high-ceiling rusher projection, so pressure has to convert more often.")
        if (vert_pct is not None and vert_pct < 40) or (broad_pct is not None and broad_pct < 40):
            concern_points.append("Burst/explosion profile is below the ideal EDGE threshold, which can limit how much immediate stress the first step creates.")
        if (cone_pct is not None and cone_pct < 35) or (shuttle_pct is not None and shuttle_pct < 35):
            concern_points.append("Bend/cornering indicators are modest, which can cap true speed-to-dip conversion once NFL tackles keep him high through the arc.")
        if edge_hurries is not None and edge_hurries < 20:
            concern_points.append(f"Total hurry volume ({edge_hurries:.1f}) leaves less evidence of sustained rush disruption across full-game sample sizes.")
    elif pos == "DT":
        if edge_pr is not None and edge_pr < 0.10:
            concern_points.append(f"Interior pressure rate ({edge_pr*100:.1f}%) suggests the pass-rush ceiling still needs role tailoring to show up consistently.")
        if weight_pct is not None and weight_pct < 30:
            concern_points.append("Interior mass profile is light for consistent NFL anchor control on early downs, which can stress the run-game floor.")
        if vert_pct is not None and vert_pct < 35:
            concern_points.append("Lower-body explosion is below the preferred interior disruption threshold, so pocket push has to come more from leverage than raw twitch.")
    elif pos == "LB":
        if shuttle_pct is not None and shuttle_pct < 35:
            concern_points.append("Space-change profile is below the ideal linebacker threshold, which can stress mismatch coverage range against faster route distribution.")
        if weight_pct is not None and weight_pct < 20:
            concern_points.append("Play-strength profile can limit block deconstruction consistency once NFL second-level bodies get into him cleanly.")
    elif pos == "CB":
        if forty_pct is not None and forty_pct < 35:
            concern_points.append("Long-speed percentile is below the ideal outside-CB threshold, increasing vertical stress when receivers force him to open early.")
        if arm_pct is not None and arm_pct < 20:
            concern_points.append("Short-arm profile can limit catch-point disruption margin versus NFL size and length, especially once the receiver owns the window.")
        if db_plays_ball is not None and db_plays_ball < 0.22:
            concern_points.append(f"Ball-production rate (plays on ball/target {db_plays_ball:.2f}) is below premium outside-CB bands, so passive phase reps need to become finishes.")
        if db_yards_cov is not None and db_yards_cov > 1.30:
            concern_points.append(f"Coverage efficiency ({db_yards_cov:.2f} yards/cov snap) needs tighter leverage-to-finish translation once route tempo improves.")
        if (db_int is not None and db_int < 1) and (db_pbu is not None and db_pbu < 5):
            concern_points.append("Limited pure ball-finish production keeps the takeaway ceiling less certain until more catch-point disruption shows up on tape.")
    elif pos == "S":
        if forty_pct is not None and forty_pct < 35:
            concern_points.append("Range-speed profile is below the ideal safety threshold for deep-half overlap demands, so angle discipline has to stay clean.")
        if db_plays_ball is not None and db_plays_ball < 0.20:
            concern_points.append(f"Plays-on-ball rate ({db_plays_ball:.2f} per target) is light for a top-end safety projection, so overlap has to turn into more finishes.")
        if db_yards_cov is not None and db_yards_cov > 1.30:
            concern_points.append(f"Coverage efficiency ({db_yards_cov:.2f} yards/cov snap) suggests transition and angle cleanup once the route picture stretches vertically.")
    if concerns and len(concern_points) < 4:
        for concern in concerns[:2]:
            concern_points.append(f"Film note to verify: {concern.rstrip('.')}.")
    if pos == "QB":
        if not str(espn_qbr or "").strip():
            concern_points.append("Missing ESPN QBR context leaves more uncertainty around down-to-down efficiency under pressure.")
        if not str(espn_epa_per_play or "").strip():
            concern_points.append("Missing EPA/play context limits separation of explosive production from sustainable drive efficiency.")
    if len(concern_points) < 2:
        for fallback_concern in concern_depth.get(pos, []):
            if len(concern_points) >= 3:
                break
            if fallback_concern not in concern_points:
                concern_points.append(fallback_concern)
    if not concern_points:
        concern_points.append(
            "No major red flag stands out in the current profile, but the NFL transition still depends on holding this role against better athletes and processing speed."
        )
    primary_concerns = "\n".join(f"- {point}" for point in _dedupe_points(concern_points, 4))

    hist_comp_text = ""
    if str(historical_combine_comp_1 or "").strip():
        if str(historical_combine_comp_1).strip().lower() == str(name).strip().lower():
            historical_combine_comp_1 = ""
    if str(historical_combine_comp_1 or "").strip():
        sim_txt = str(historical_combine_comp_1_similarity or "").strip()
        year_txt = str(historical_combine_comp_1_year or "").strip()
        hist_comp_text = f" Historical combine comp: {historical_combine_comp_1}"
        if year_txt:
            hist_comp_text += f" ({year_txt})"
        if sim_txt:
            hist_comp_text += f", athletic-profile match {sim_txt}%"
        hist_comp_text += "."

    projection_parts = [
        f"Best early team fit: {clean_team}.",
        f"Expected early deployment: {clean_role} in {clean_scheme}.",
        role_projection_logic.get(pos, "For film-heavy evaluation: this role asks for translatable execution against faster processing environments."),
    ]
    if hist_comp_text:
        projection_parts.append(hist_comp_text.strip())
    projection = " ".join(projection_parts)

    return {
        "scouting_report_summary": report,
        "scouting_why_he_wins": wins,
        "scouting_primary_concerns": primary_concerns,
        "scouting_production_snapshot": production_snapshot,
        "scouting_board_movement": move_text,
        "scouting_role_projection": projection,
        "scouting_historical_athletic_comp": hist_comp_text.strip(),
    }


def main() -> None:
    reset_team_fit_state()
    prebuild_report = run_prebuild_checks(
        seed_path=PROCESSED / "prospect_seed_2026.csv",
        combine_path=ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv",
        returning_path=ROOT / "data" / "sources" / "manual" / "returning_to_school_2026.csv",
        allowed_positions=ALLOWED_POSITIONS,
    )
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    (OUTPUTS / "prebuild_qa_report.json").write_text(json.dumps(prebuild_report, indent=2))
    (OUTPUTS / "prebuild_qa_report.md").write_text(format_prebuild_report_md(prebuild_report))
    if prebuild_report.get("status") == "fail":
        print("Prebuild QA failed. Resolve input errors before building board.")
        print(f"Report: {OUTPUTS / 'prebuild_qa_report.md'}")
        raise SystemExit(2)

    returning_names = load_returning_to_school()
    declared_underclassmen = load_declared_underclassmen()
    already_drafted_names = load_already_in_nfl_exclusions()
    nfl_official_universe_names = _load_nfl_official_universe_names()
    analyst_rows = load_analyst_rows()
    analyst_pos_votes = _build_analyst_pos_votes(analyst_rows)
    external_board_rows = load_external_big_board_rows()
    allowed_universe_names = _build_allowed_universe_names(
        external_rows=external_board_rows,
        analyst_rows=analyst_rows,
        declared_underclassmen=declared_underclassmen,
        nfl_official_names=nfl_official_universe_names,
    )

    seed_all = read_seed(PROCESSED / "prospect_seed_2026.csv")
    removed_returning = []
    removed_already_drafted = []
    removed_ineligible_class = []
    removed_outside_universe = []
    raw_seed = []
    for row in seed_all:
        name_key = canonical_player_name(row["player_name"])
        if name_key in returning_names:
            removed_returning.append(row["player_name"])
            continue
        if name_key in already_drafted_names:
            removed_already_drafted.append(row["player_name"])
            continue
        if not (is_senior_class(row["class_year"]) or name_key in declared_underclassmen):
            removed_ineligible_class.append(row["player_name"])
            continue
        if ENFORCE_NFL_OFFICIAL_UNIVERSE and name_key not in nfl_official_universe_names:
            removed_outside_universe.append(row["player_name"])
            continue
        if ENFORCE_2026_EVIDENCE_UNIVERSE and name_key not in allowed_universe_names:
            removed_outside_universe.append(row["player_name"])
            continue
        raw_seed.append(row)

    if ENFORCE_NFL_OFFICIAL_UNIVERSE:
        excl_path = OUTPUTS / "nfl_official_universe_exclusions_2026.txt"
        unique_removed = sorted({str(x).strip() for x in removed_outside_universe if str(x).strip()})
        excl_lines = [
            "NFL.com Official Universe Exclusions (2026)",
            "",
            f"official_universe_size: {len(nfl_official_universe_names)}",
            f"excluded_count: {len(unique_removed)}",
            "",
        ]
        excl_lines.extend(unique_removed)
        excl_path.write_text("\n".join(excl_lines))

    if ENABLE_SOURCE_UNIVERSE_EXPANSION:
        expanded_seed, added_external, added_analyst, skipped_ineligible = augment_seed_with_external_and_analyst(
            seed_rows=raw_seed,
            external_rows=external_board_rows,
            analyst_rows=analyst_rows,
            returning_names=returning_names,
            already_drafted_names=already_drafted_names,
            allowed_names=allowed_universe_names if (ENFORCE_NFL_OFFICIAL_UNIVERSE or ENFORCE_2026_EVIDENCE_UNIVERSE) else None,
        )
    else:
        expanded_seed = list(raw_seed)
        added_external = 0
        added_analyst = 0
        skipped_ineligible = 0

    seed = dedupe_seed_rows(expanded_seed)
    if ENFORCE_NFL_OFFICIAL_UNIVERSE and nfl_official_universe_names:
        seed = [
            row for row in seed
            if canonical_player_name(row.get("player_name", "")) in nfl_official_universe_names
        ]

    analyst_scores = analyst_aggregate_score(analyst_rows)
    external_board = load_external_big_board()
    combine_results = load_combine_results()
    film_rows = load_film_trait_rows()
    espn_pack = load_espn_player_signals(target_year=2026)
    pp_pack = load_playerprofiler_signals()
    lang_pack = load_analyst_linguistic_signals()
    kiper_pack = load_kiper_structured_signals()
    tdn_ringer_pack = load_tdn_ringer_signals()
    consensus_pack = load_consensus_board_signals()
    cfb_prod_pack = load_cfb_production_signals(target_season=2025)
    draft_age_pack = load_draft_age_signals()
    early_declare_pack = load_early_declare_signals()
    production_knn_pack = load_production_percentile_pack()
    _enforce_production_knn_history_qa(production_knn_pack)
    roi_prior_pack = load_position_roi_priors()
    historical_combine_pack = load_historical_combine_profiles()
    historical_athletic_pack = load_historical_athletic_context()
    mockdraftable_baselines = load_mockdraftable_baselines()
    ras_benchmarks = load_ras_benchmarks()
    espn_by_name_pos = espn_pack.get("by_name_pos", {})
    espn_by_name = espn_pack.get("by_name", {})
    pp_by_name_pos = pp_pack.get("by_name_pos", {})
    pp_by_name = pp_pack.get("by_name", {})
    lang_by_name_pos = lang_pack.get("by_name_pos", {})
    lang_by_name = lang_pack.get("by_name", {})
    kiper_by_name_pos = kiper_pack.get("by_name_pos", {})
    kiper_by_name = kiper_pack.get("by_name", {})
    tdn_ringer_by_name_pos = tdn_ringer_pack.get("by_name_pos", {})
    tdn_ringer_by_name = tdn_ringer_pack.get("by_name", {})
    consensus_by_name = consensus_pack
    cfb_prod_by_name_pos = cfb_prod_pack.get("by_name_pos", {})
    cfb_prod_by_name = cfb_prod_pack.get("by_name", {})
    draft_age_by_name_pos = draft_age_pack.get("by_name_pos", {})
    draft_age_by_name = draft_age_pack.get("by_name", {})
    early_declare_by_name_pos = early_declare_pack.get("by_name_pos", {})
    early_declare_by_name = early_declare_pack.get("by_name", {})
    calibration_cfg = load_calibration_config()
    source_reliability = _load_source_reliability_weights()
    has_espn_signals = bool(espn_by_name_pos)
    has_pp_signals = bool(pp_by_name_pos)

    film_map = {}
    for frow in film_rows:
        film_pos = normalize_pos(frow["position"])
        key = (canonical_player_name(frow["player_name"]), film_pos)
        existing = film_map.get(key)
        if existing is None or frow.get("coverage_count", 0) > existing.get("coverage_count", 0):
            film_map[key] = frow

    enriched = []
    for row in seed:
        pos = normalize_pos(row["pos_raw"])
        key = canonical_player_name(row["player_name"])

        ext = external_board.get(key, {})
        combine = combine_results.get(key, {})
        film = film_map.get((key, pos), {}) if ENABLE_FILM_WEIGHTING else {}
        espn = espn_by_name_pos.get((key, pos), espn_by_name.get(key, {}))
        pp = pp_by_name_pos.get((key, pos), pp_by_name.get(key, {}))
        lang = lang_by_name_pos.get((key, pos), lang_by_name.get(key, {}))
        kiper = kiper_by_name_pos.get((key, pos), kiper_by_name.get(key, {}))
        tdn_ringer = tdn_ringer_by_name_pos.get((key, pos), tdn_ringer_by_name.get(key, {}))
        consensus = consensus_by_name.get(key, {})
        cfb = cfb_prod_by_name_pos.get((key, pos), cfb_prod_by_name.get(key, {}))
        cfb = _sanitize_position_scoped_cfb_payload(pos, cfb)
        draft_age_row = draft_age_by_name_pos.get((key, pos), draft_age_by_name.get(key, {}))
        early_declare_row = early_declare_by_name_pos.get((key, pos), early_declare_by_name.get(key, {}))
        _assert_no_team_metrics_in_player_feed(row["player_name"], cfb)

        seed_height_in = parse_height_to_inches(row["height"]) or POSITION_DEFAULT_FRAME.get(pos, (74, 220))[0]
        seed_weight_lb = int(row["weight_lb"])
        effective_height_in = int(combine["height_in"]) if combine.get("height_in") is not None else seed_height_in
        effective_weight_lb = int(combine["weight_lb"]) if combine.get("weight_lb") is not None else seed_weight_lb

        grades = grade_player(
            position=pos,
            rank_seed=row["rank_seed"],
            class_year=row["class_year"],
            height_in=effective_height_in,
            weight_lb=effective_weight_lb,
            film_subtraits=film.get("traits", {}),
            production_context=cfb,
        )

        seed_signal = float(301 - row["rank_seed"])
        analyst_score = float(analyst_scores.get(key, 35.0))
        external_rank = ext.get("external_rank")
        external_rank_signal = float(max(1, 301 - external_rank)) if external_rank else 35.0

        pff_grade = ext.get("pff_grade")
        pff_grade_signal = float(pff_grade) if pff_grade is not None else 70.0
        pff_waa = ext.get("pff_waa")
        waa_signal = _scale_waa(pff_waa)

        espn_rank_signal = float(espn.get("espn_rank_signal", 35.0) or 35.0)
        espn_pos_signal = float(espn.get("espn_pos_signal", 35.0) or 35.0)
        espn_grade_signal = float(espn.get("espn_grade_signal", 70.0) or 70.0)
        espn_prod_signal = float(espn.get("espn_prod_signal", 55.0) or 55.0)
        espn_volatility_flag = bool(espn.get("espn_volatility_flag", False))

        pp_skill_signal = float(pp.get("pp_skill_signal", 55.0) or 55.0)
        pp_breakout_signal = float(pp.get("pp_breakout_signal", 55.0) or 55.0)
        pp_dominator_signal = float(pp.get("pp_dominator_signal", 55.0) or 55.0)
        pp_risk_flag = bool(pp.get("pp_risk_flag", 0))
        pp_early_declare_flag = int(_as_float(pp.get("pp_early_declare")) or 0)
        pp_player_available = bool(str(pp.get("pp_data_coverage", "")).strip())
        early_declare_source_flag = int(_as_float(early_declare_row.get("early_declare")) or 0)
        early_declare_flag = int(pp_early_declare_flag == 1 or early_declare_source_flag == 1)
        combine_invited_flag = int(_as_float(early_declare_row.get("combine_invited")) or 0)
        cfb_prod_signal = float(_as_float(cfb.get("cfb_prod_signal")) or 55.0)
        cfb_player_available = bool(int(_as_float(cfb.get("cfb_prod_available")) or 0))
        cfb_prod_coverage_count = int(_as_float(cfb.get("cfb_prod_coverage_count")) or 0)
        cfb_prod_reliability = float(_as_float(cfb.get("cfb_prod_reliability")) or 0.0)
        kiper_rank_signal = float(kiper.get("kiper_rank_signal", 0.0) or 0.0)
        kiper_rank = kiper.get("kiper_rank", "")
        kiper_prev_rank = kiper.get("kiper_prev_rank", "")
        kiper_rank_delta = kiper.get("kiper_rank_delta", "")
        kiper_volatility_flag = int(kiper.get("kiper_volatility_flag", 0) or 0)
        kiper_volatility_penalty = float(kiper.get("kiper_volatility_penalty", 0.0) or 0.0)
        tdn_rank_signal = float(tdn_ringer.get("tdn_rank_signal", 0.0) or 0.0)
        ringer_rank_signal = float(tdn_ringer.get("ringer_rank_signal", 0.0) or 0.0)
        br_rank_signal = float(tdn_ringer.get("br_rank_signal", 0.0) or 0.0)
        atoz_rank_signal = float(tdn_ringer.get("atoz_rank_signal", 0.0) or 0.0)
        si_rank_signal = float(tdn_ringer.get("si_rank_signal", 0.0) or 0.0)
        tdn_grade_label_signal = float(tdn_ringer.get("tdn_grade_label_signal", 0.0) or 0.0)
        tdn_text_trait_signal = float(tdn_ringer.get("tdn_text_trait_signal", 0.0) or 0.0)
        tdn_risk_penalty = float(tdn_ringer.get("tdn_risk_penalty", 0.0) or 0.0)
        tdn_risk_flag = int(_as_float(tdn_ringer.get("tdn_risk_flag")) or 0)
        tdn_risk_hits = int(_as_float(tdn_ringer.get("tdn_risk_hits")) or 0)
        br_text_trait_signal = float(tdn_ringer.get("br_text_trait_signal", 0.0) or 0.0)
        br_risk_penalty = float(tdn_ringer.get("br_risk_penalty", 0.0) or 0.0)
        br_risk_flag = int(_as_float(tdn_ringer.get("br_risk_flag")) or 0)
        br_risk_hits = int(_as_float(tdn_ringer.get("br_risk_hits")) or 0)
        atoz_text_trait_signal = float(tdn_ringer.get("atoz_text_trait_signal", 0.0) or 0.0)
        atoz_risk_penalty = float(tdn_ringer.get("atoz_risk_penalty", 0.0) or 0.0)
        atoz_risk_flag = int(_as_float(tdn_ringer.get("atoz_risk_flag")) or 0)
        atoz_risk_hits = int(_as_float(tdn_ringer.get("atoz_risk_hits")) or 0)
        si_text_trait_signal = float(tdn_ringer.get("si_text_trait_signal", 0.0) or 0.0)
        si_risk_penalty = float(tdn_ringer.get("si_risk_penalty", 0.0) or 0.0)
        si_risk_flag = int(_as_float(tdn_ringer.get("si_risk_flag")) or 0)
        si_risk_hits = int(_as_float(tdn_ringer.get("si_risk_hits")) or 0)
        cbs_rank_signal = float(tdn_ringer.get("cbs_rank_signal", 0.0) or 0.0)
        cbs_wilson_rank_signal = float(tdn_ringer.get("cbs_wilson_rank_signal", 0.0) or 0.0)
        cbs_text_trait_signal = float(tdn_ringer.get("cbs_text_trait_signal", 0.0) or 0.0)
        cbs_risk_penalty = float(tdn_ringer.get("cbs_risk_penalty", 0.0) or 0.0)
        cbs_risk_flag = int(_as_float(tdn_ringer.get("cbs_risk_flag")) or 0)
        cbs_risk_hits = int(_as_float(tdn_ringer.get("cbs_risk_hits")) or 0)
        consensus_signal = float(consensus.get("consensus_signal", 0.0) or 0.0)
        consensus_mean_rank = consensus.get("consensus_mean_rank", "")
        consensus_rank_std = consensus.get("consensus_rank_std", "")
        consensus_source_count = consensus.get("consensus_source_count", "")
        consensus_sources = consensus.get("consensus_sources", "")
        consensus_mean_rank_val = _as_float(consensus_mean_rank)
        consensus_rank_std_val = _as_float(consensus_rank_std)
        consensus_source_count_val = int(_as_float(consensus_source_count) or 0)
        top50_evidence_signal_count, top50_evidence_signal_labels = _top50_independent_evidence_signals(
            consensus_source_count=consensus_source_count_val,
            external_rank=external_rank,
            pff_grade=pff_grade,
            cfb_prod_available=cfb_player_available,
            espn_row=espn,
        )
        top50_evidence_missing_signals = max(
            0,
            int(TOP50_EVIDENCE_MIN_SIGNALS) - int(top50_evidence_signal_count),
        )

        # Market score is kept for diagnostics only.
        if has_espn_signals and has_pp_signals and pp_player_available:
            market_signal_score = (
                0.30 * seed_signal
                + 0.14 * analyst_score
                + 0.14 * external_rank_signal
                + 0.08 * pff_grade_signal
                + 0.04 * waa_signal
                + 0.10 * espn_rank_signal
                + 0.03 * espn_pos_signal
                + 0.04 * espn_grade_signal
                + 0.03 * espn_prod_signal
                + 0.08 * pp_skill_signal
                + 0.01 * pp_breakout_signal
                + 0.01 * pp_dominator_signal
            )
        elif has_espn_signals:
            market_signal_score = (
                0.33 * seed_signal
                + 0.15 * analyst_score
                + 0.15 * external_rank_signal
                + 0.09 * pff_grade_signal
                + 0.04 * waa_signal
                + 0.12 * espn_rank_signal
                + 0.04 * espn_pos_signal
                + 0.05 * espn_grade_signal
                + 0.03 * espn_prod_signal
            )
        elif has_pp_signals and pp_player_available:
            market_signal_score = (
                0.40 * seed_signal
                + 0.19 * analyst_score
                + 0.19 * external_rank_signal
                + 0.10 * pff_grade_signal
                + 0.05 * waa_signal
                + 0.06 * pp_skill_signal
                + 0.01 * pp_breakout_signal
            )
        else:
            market_signal_score = (
                0.45 * seed_signal
                + 0.20 * analyst_score
                + 0.20 * external_rank_signal
                + 0.10 * pff_grade_signal
                + 0.05 * waa_signal
            )

        fit_team, fit_score = best_team_fit(
            pos,
            role_hint=str(grades.get("best_role", "") or ""),
            scheme_hint=str(grades.get("best_scheme_fit", "") or ""),
            athletic_score=float(_as_float(grades.get("athletic_score")) or 0.0),
            prospect_rank_seed=int(row.get("rank_seed") or 9999),
        )
        comp = assign_comp(pos, row["rank_seed"])
        fallback_ras = estimate_ras(
            pos,
            int(effective_height_in or POSITION_DEFAULT_FRAME.get(pos, (72, 210))[0]),
            int(effective_weight_lb or POSITION_DEFAULT_FRAME.get(pos, (72, 210))[1]),
            float(_as_float(grades.get("athletic_score")) or 70.0),
            int(row["rank_seed"]),
        )
        ras, ras_comps = _official_ras_fields(pos, combine, fallback_ras=fallback_ras)
        ras_score_val = _as_float(ras.get("ras_estimate"))
        ras_bench = ras_benchmarks.get(pos, {})
        starter_target = _as_float(ras_bench.get("starter_target_ras"))
        impact_target = _as_float(ras_bench.get("impact_target_ras"))
        elite_target = _as_float(ras_bench.get("elite_target_ras"))
        meets_starter = (
            "yes" if (ras_score_val is not None and starter_target is not None and ras_score_val >= starter_target) else ""
        )
        meets_impact = (
            "yes" if (ras_score_val is not None and impact_target is not None and ras_score_val >= impact_target) else ""
        )
        meets_elite = (
            "yes" if (ras_score_val is not None and elite_target is not None and ras_score_val >= elite_target) else ""
        )

        md_meas = {
            "height": effective_height_in,
            "weight": effective_weight_lb,
            "arm": combine.get("arm_in", ""),
            "hand": combine.get("hand_in", ""),
            "ten_split": combine.get("ten_split", ""),
            "forty": combine.get("forty", ""),
            "vertical": combine.get("vertical", ""),
            "broad": combine.get("broad", ""),
            "shuttle": combine.get("shuttle", ""),
            "three_cone": combine.get("three_cone", ""),
            "bench": combine.get("bench", ""),
        }
        md_features = compute_mockdraftable_composite(pos, md_meas, mockdraftable_baselines)
        historical_combine_metrics = {
            "height_in": float(effective_height_in) if effective_height_in is not None else None,
            "weight_lb": float(effective_weight_lb) if effective_weight_lb is not None else None,
            "arm_in": _as_float(combine.get("arm_in")),
            "hand_in": _as_float(combine.get("hand_in")),
            "forty": _as_float(combine.get("forty")),
            "ten_split": _as_float(combine.get("ten_split")),
            "vertical": _as_float(combine.get("vertical")),
            "broad": _as_float(combine.get("broad")),
            "three_cone": _as_float(combine.get("three_cone")),
            "shuttle": _as_float(combine.get("shuttle")),
            "bench": _as_float(combine.get("bench")),
            "wingspan_in": _as_float(combine.get("wingspan_in")),
        }
        hist_comp_result = find_historical_combine_comps(
            position=pos,
            current_metrics=historical_combine_metrics,
            pack=historical_combine_pack,
            player_name=row["player_name"],
            max_year_exclusive=CURRENT_DRAFT_YEAR,
            k=3,
            min_overlap_metrics=3,
        )
        hist_comps = hist_comp_result.get("comps", [])
        hist_comp_1 = hist_comps[0] if len(hist_comps) >= 1 else {}
        hist_comp_2 = hist_comps[1] if len(hist_comps) >= 2 else {}
        hist_comp_3 = hist_comps[2] if len(hist_comps) >= 3 else {}
        prod_knn_result = find_production_percentile_comps(
            player_name=row["player_name"],
            position=pos,
            pack=production_knn_pack,
            target_season=2025,
            k=3,
            min_overlap=3,
            allow_same_season_fallback=False,
        )
        prod_knn_comps = prod_knn_result.get("comps", [])
        prod_knn_1 = prod_knn_comps[0] if len(prod_knn_comps) >= 1 else {}
        prod_knn_2 = prod_knn_comps[1] if len(prod_knn_comps) >= 2 else {}
        prod_knn_3 = prod_knn_comps[2] if len(prod_knn_comps) >= 3 else {}
        # Prefer data-driven percentile-vector comp for explainer text; keep static comp as fallback.
        if str(prod_knn_1.get("player_name", "")).strip():
            comp["historical_comp"] = str(prod_knn_1.get("player_name", ""))
            comp["comp_style"] = "production percentile vector nearest-neighbor"
            sim = _as_float(prod_knn_1.get("similarity"))
            if sim is not None:
                if sim >= 86.0:
                    comp["comp_confidence"] = "A"
                elif sim >= 78.0:
                    comp["comp_confidence"] = "B"
                else:
                    comp["comp_confidence"] = "C"
        athletic_profile = evaluate_athletic_profile(
            position=pos,
            current_metrics=historical_combine_metrics,
            pack=historical_athletic_pack,
        )
        language_coverage_val = _as_float(lang.get("lang_text_coverage")) or 0.0
        language_features = _language_feature_block(
            lang=lang,
            lang_risk_hits=int(_as_float(lang.get("lang_risk_hits")) or 0),
            risk_flags=[
                int(_as_float(lang.get("lang_risk_flag")) or 0),
                int(espn_volatility_flag),
                int(pp_risk_flag),
                tdn_risk_flag,
                br_risk_flag,
                atoz_risk_flag,
                si_risk_flag,
                cbs_risk_flag,
            ],
            extra_risk_hits=[
                tdn_risk_hits,
                br_risk_hits,
                atoz_risk_hits,
                si_risk_hits,
                cbs_risk_hits,
            ],
        )

        is_diamond_exception, diamond_exception_reasons = _diamond_exception_profile(
            position=pos,
            rank_seed=row["rank_seed"],
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            trait_score=float(grades.get("trait_score", 0.0) or 0.0),
            production_score=float(grades.get("production_score", 0.0) or 0.0),
            athletic_score=float(grades.get("athletic_score", 0.0) or 0.0),
            risk_penalty=float(grades.get("risk_penalty", 0.0) or 0.0),
            language_coverage=language_coverage_val,
            pff_grade=pff_grade,
            external_rank=external_rank,
            official_ras=ras_score_val,
            impact_target_ras=impact_target,
        )
        contrarian_score = _contrarian_watch_score(
            rank_seed=row["rank_seed"],
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            trait_score=float(grades.get("trait_score", 0.0) or 0.0),
            production_score=float(grades.get("production_score", 0.0) or 0.0),
            athletic_score=float(grades.get("athletic_score", 0.0) or 0.0),
            size_score=float(grades.get("size_score", 0.0) or 0.0),
            context_score=float(grades.get("context_score", 0.0) or 0.0),
            risk_penalty=float(grades.get("risk_penalty", 0.0) or 0.0),
        )

        # Source-anchor blend.
        # New source weights in prior blend (inside weighted mean, not raw additive):
        # TDN rank 0.08, Ringer rank 0.08, Bleacher rank 0.09, AtoZ rank 0.08,
        # SI/FCS rank 0.03, CBS rank 0.10, TDN grade label 0.04.
        prior_parts: list[tuple[float, float]] = []
        prior_diag: dict[str, dict] = {}
        _append_prior_part(
            parts=prior_parts,
            diagnostics=prior_diag,
            source_key="seed_rank",
            base_weight=0.20,
            signal_value=max(1.0, min(100.0, seed_signal / 3.0)),
            reliability_pack=source_reliability,
            position=pos,
            draft_year=CURRENT_DRAFT_YEAR,
        )
        if external_rank is not None:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="external_rank",
                base_weight=0.24,
                signal_value=max(1.0, min(100.0, external_rank_signal / 3.0)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if analyst_score > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="analyst_rank",
                base_weight=0.14,
                signal_value=max(1.0, min(100.0, analyst_score)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        # Consensus board signal should only anchor priors when at least two
        # independent sources agree. Single-source rows are kept for diagnostics,
        # but not trusted enough to steer the board/mocks.
        if consensus_signal > 0 and consensus_source_count_val >= 2:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="consensus_rank",
                base_weight=0.24,
                signal_value=max(1.0, min(100.0, consensus_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if kiper_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="kiper_rank",
                base_weight=0.08,
                signal_value=max(1.0, min(100.0, kiper_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if tdn_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="tdn_rank",
                base_weight=0.08,
                signal_value=max(1.0, min(100.0, tdn_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if ringer_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="ringer_rank",
                base_weight=0.08,
                signal_value=max(1.0, min(100.0, ringer_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if br_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="bleacher_rank",
                base_weight=0.09,
                signal_value=max(1.0, min(100.0, br_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if atoz_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="atoz_rank",
                base_weight=0.08,
                signal_value=max(1.0, min(100.0, atoz_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if si_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="si_rank",
                base_weight=0.03,
                signal_value=max(1.0, min(100.0, si_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if cbs_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="cbs_rank",
                base_weight=0.10,
                signal_value=max(1.0, min(100.0, cbs_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if cbs_wilson_rank_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="cbs_wilson_rank",
                base_weight=0.06,
                signal_value=max(1.0, min(100.0, cbs_wilson_rank_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        if tdn_grade_label_signal > 0:
            _append_prior_part(
                parts=prior_parts,
                diagnostics=prior_diag,
                source_key="tdn_grade_label",
                base_weight=0.04,
                signal_value=max(1.0, min(100.0, tdn_grade_label_signal)),
                reliability_pack=source_reliability,
                position=pos,
                draft_year=CURRENT_DRAFT_YEAR,
            )
        prior_signal = _weighted_mean(prior_parts) or max(1.0, min(100.0, seed_signal / 3.0))

        prior_anchor_adjustment = _consensus_anchor_adjustment(
            rank_seed=row["rank_seed"],
            consensus_mean_rank=consensus_mean_rank,
            consensus_source_count=consensus_source_count,
            external_rank=external_rank,
        )
        prior_signal = max(1.0, min(100.0, prior_signal + prior_anchor_adjustment))

        formula = _compute_formula_score(
            position=pos,
            class_year=row["class_year"],
            grades=grades,
            pff_grade=pff_grade,
            espn_prod_signal=espn_prod_signal,
            pp_skill_signal=pp_skill_signal,
            pp_player_available=pp_player_available,
            cfb_prod_signal=cfb_prod_signal,
            cfb_player_available=cfb_player_available,
            cfb_prod_coverage_count=cfb_prod_coverage_count,
            cfb_prod_reliability=cfb_prod_reliability,
            cfb_prod_quality_label=str(cfb.get("cfb_prod_quality_label", "") or ""),
            prior_signal=prior_signal,
            lang=lang,
            ras=ras,
            md_features=md_features,
            athletic_profile=athletic_profile,
            external_rank=external_rank,
            analyst_score=analyst_score,
            espn_volatility_flag=espn_volatility_flag,
            pp_risk_flag=pp_risk_flag,
            kiper_volatility_penalty=kiper_volatility_penalty,
            tdn_text_trait_signal=tdn_text_trait_signal,
            tdn_risk_penalty=tdn_risk_penalty,
            br_text_trait_signal=br_text_trait_signal,
            br_risk_penalty=br_risk_penalty,
            atoz_text_trait_signal=atoz_text_trait_signal,
            atoz_risk_penalty=atoz_risk_penalty,
            si_text_trait_signal=si_text_trait_signal,
            si_risk_penalty=si_risk_penalty,
            cbs_text_trait_signal=cbs_text_trait_signal,
            cbs_risk_penalty=cbs_risk_penalty,
            years_played=_as_float(cfb.get("cfb_years_played")),
            draft_age=_as_float(draft_age_row.get("draft_age")),
            early_declare=bool(early_declare_flag),
            combine_testing_status=str(combine.get("combine_testing_status", "") or ""),
            combine_testing_event_count=int(_as_float(combine.get("combine_testing_event_count")) or 0),
            combine_invited=bool(combine_invited_flag),
        )
        language_trait = _as_float(lang.get("lang_trait_composite"))
        guardrail_penalty = _consensus_guardrail_penalty(
            position=pos,
            external_rank=external_rank,
            analyst_score=analyst_score,
            pff_grade=pff_grade,
            language_trait=language_trait,
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
        )
        drift_penalty = _seed_consensus_drift_penalty(
            position=pos,
            rank_seed=row["rank_seed"],
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
        )
        midband_brake_penalty = _midband_consensus_brake_penalty(
            position=pos,
            rank_seed=row["rank_seed"],
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            consensus_rank_std=consensus_rank_std_val,
            external_rank=external_rank,
            pff_grade=pff_grade,
            language_trait=language_trait,
        )
        consensus_confidence_factor = _consensus_confidence_factor(
            consensus_source_count=consensus_source_count_val,
            consensus_rank_std=consensus_rank_std_val,
        )
        # "Diamond in the rough" exception: keep contrarian upside, but only with strong support.
        if is_diamond_exception:
            guardrail_penalty *= 0.45
            drift_penalty *= 0.35
            midband_brake_penalty *= 0.35

        # Broad outlier suppression: players consensus boards push far down should not stay top-75
        # unless they cleared the hard exception profile above.
        top75_gate_penalty = 0.0
        if (
            not is_diamond_exception
            and consensus_mean_rank_val is not None
            and consensus_source_count_val >= 2
            and consensus_mean_rank_val > 150.0
        ):
            top75_gate_penalty = min(4.0, 1.2 + ((consensus_mean_rank_val - 150.0) / 38.0))

        model_score = max(
            55.0,
            min(
                95.0,
                float(formula["formula_score"])
                - guardrail_penalty
                - drift_penalty
                - midband_brake_penalty
                - top75_gate_penalty,
            ),
        )
        soft_ceiling_target = _position_band_soft_ceiling_target(
            position=pos,
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            consensus_rank_std=consensus_rank_std_val,
        )
        soft_ceiling_penalty = _soft_ceiling_penalty(model_score, soft_ceiling_target)
        if is_diamond_exception:
            soft_ceiling_penalty = round(soft_ceiling_penalty * 0.5, 2)
        model_score = max(55.0, min(95.0, model_score - soft_ceiling_penalty))
        language_adjustment_applied = float(language_features.get("language_adjustment_applied", 0.0) or 0.0)
        model_score = max(55.0, min(95.0, model_score + language_adjustment_applied))

        roi_rank_reference = int(round(consensus_mean_rank_val)) if consensus_mean_rank_val is not None else int(row["rank_seed"])
        roi_pick_band = pick_band_from_rank(max(1, min(300, roi_rank_reference)))
        roi_row = roi_prior_pack.get((pos, roi_pick_band), {})
        roi_base_adjustment = _clamp(float(_as_float(roi_row.get("roi_grade_adjustment")) or 0.0), -0.60, 0.60)
        roi_conf_mult = 1.0 if consensus_source_count_val >= 2 else 0.75
        roi_sample_n = int(_as_float(roi_row.get("sample_n")) or 0)
        if roi_sample_n < 20:
            roi_conf_mult *= 0.75
        roi_adjustment_applied = round(roi_base_adjustment * roi_conf_mult, 4)
        model_score = max(55.0, min(95.0, model_score + roi_adjustment_applied))

        consensus_tail_penalty, consensus_tail_target = _consensus_tail_soft_penalty(
            position=pos,
            model_score=model_score,
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            consensus_rank_std=consensus_rank_std_val,
            external_rank=external_rank,
            pff_grade=pff_grade,
            language_trait=language_trait,
            is_diamond_exception=is_diamond_exception,
        )
        if consensus_tail_penalty > 0:
            model_score = max(55.0, min(95.0, model_score - consensus_tail_penalty))

        hard_cap = _consensus_hard_cap(
            position=pos,
            rank_seed=row["rank_seed"],
            external_rank=external_rank,
            analyst_score=analyst_score,
            pff_grade=pff_grade,
            language_trait=language_trait,
        )
        outlier_cap = _consensus_outlier_cap(
            position=pos,
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
        )
        if is_diamond_exception:
            # Exception players can break through more naturally.
            if hard_cap is not None:
                hard_cap = min(95.0, float(hard_cap) + 2.0)
            if outlier_cap is not None:
                outlier_cap = min(95.0, float(outlier_cap) + 2.0)
        elif (
            consensus_mean_rank_val is not None
            and consensus_source_count_val >= 2
            and consensus_mean_rank_val > 150.0
        ):
            top75_gate_cap = 77.0 if pos == "QB" else 78.0
            if hard_cap is None:
                hard_cap = top75_gate_cap
            else:
                hard_cap = min(float(hard_cap), top75_gate_cap)
        if hard_cap is None:
            hard_cap = outlier_cap
        elif outlier_cap is not None:
            hard_cap = min(float(hard_cap), float(outlier_cap))
        cap_penalty = 0.0
        if hard_cap is not None and model_score > hard_cap:
            cap_penalty = model_score - hard_cap
            model_score = hard_cap

        calibration_pos_delta = 0.0
        calibration_grade_adjustment = 0.0
        calibrated_success_prob = ""
        if calibration_cfg is not None and calibration_cfg.sample_size > 0:
            calibration_pos_delta = float(calibration_cfg.position_additive.get(pos, 0.0))
            calibration_grade_adjustment = calibration_pos_delta * 8.0
            model_score = max(55.0, min(95.0, model_score + calibration_grade_adjustment))
            calibrated_success_prob = calibrated_success_probability(
                grade=model_score,
                position=pos,
                config=calibration_cfg,
                ras_estimate=_as_float(ras.get("ras_estimate")),
                pff_grade=pff_grade,
            )

        front7_inflation_penalty = 0.0
        front7_inflation_reason = ""
        front7_success_prob_before_brake = calibrated_success_prob
        cb_nickel_inflation_penalty = 0.0
        cb_nickel_inflation_reason = ""
        if pos in {"EDGE", "DT", "LB"}:
            front7_inflation_penalty, front7_inflation_reason = _front7_pass_rush_inflation_penalty(
                position=pos,
                is_diamond_exception=is_diamond_exception,
                production_component=float(formula.get("formula_production_component", PRODUCTION_SIGNAL_NEUTRAL) or PRODUCTION_SIGNAL_NEUTRAL),
                production_guardrail_delta=float(formula.get("formula_production_guardrail_delta", 0.0) or 0.0),
                consensus_mean_rank=consensus_mean_rank_val,
                consensus_source_count=consensus_source_count_val,
                consensus_rank_std=consensus_rank_std_val,
                cfb_prod_available=cfb_player_available,
                cfb_prod_quality_label=str(cfb.get("cfb_prod_quality_label", "") or ""),
                cfb_prod_reliability=float(_as_float(cfb.get("cfb_prod_reliability")) or 0.0),
                cfb_prod_coverage_count=cfb_prod_coverage_count,
                evidence_missing_count=int(formula.get("formula_evidence_missing_count", 0) or 0),
                roi_pick_band=roi_pick_band,
                roi_adjustment_applied=roi_adjustment_applied,
                roi_sample_n=roi_sample_n,
                roi_surplus_z=_as_float(roi_row.get("surplus_z")),
                calibrated_success_prob=calibrated_success_prob,
            )
            if front7_inflation_penalty > 0:
                model_score = max(55.0, min(95.0, model_score - front7_inflation_penalty))
                if calibration_cfg is not None and calibration_cfg.sample_size > 0:
                    calibrated_success_prob = calibrated_success_probability(
                        grade=model_score,
                        position=pos,
                        config=calibration_cfg,
                        ras_estimate=_as_float(ras.get("ras_estimate")),
                        pff_grade=pff_grade,
                    )
        elif pos == "CB":
            cb_nickel_inflation_penalty, cb_nickel_inflation_reason = _cb_nickel_inflation_penalty(
                position=pos,
                is_diamond_exception=is_diamond_exception,
                height_in=effective_height_in,
                weight_lb=effective_weight_lb,
                production_component=float(
                    formula.get("formula_production_component", PRODUCTION_SIGNAL_NEUTRAL) or PRODUCTION_SIGNAL_NEUTRAL
                ),
                production_guardrail_delta=float(formula.get("formula_production_guardrail_delta", 0.0) or 0.0),
                consensus_mean_rank=consensus_mean_rank_val,
                consensus_source_count=consensus_source_count_val,
                consensus_rank_std=consensus_rank_std_val,
                cfb_prod_quality_label=str(cfb.get("cfb_prod_quality_label", "") or ""),
                cfb_prod_reliability=float(_as_float(cfb.get("cfb_prod_reliability")) or 0.0),
                cfb_prod_coverage_count=cfb_prod_coverage_count,
                cfb_prod_proxy_fallback_features=int(
                    _as_float(cfb.get("cfb_prod_proxy_fallback_features")) or 0
                ),
                external_rank=external_rank,
                pff_grade=pff_grade,
            )
            if cb_nickel_inflation_penalty > 0:
                model_score = max(55.0, min(95.0, model_score - cb_nickel_inflation_penalty))
                if calibration_cfg is not None and calibration_cfg.sample_size > 0:
                    calibrated_success_prob = calibrated_success_probability(
                        grade=model_score,
                        position=pos,
                        config=calibration_cfg,
                        ras_estimate=_as_float(ras.get("ras_estimate")),
                        pff_grade=pff_grade,
                    )

        bluechip_floor = _consensus_bluechip_floor(
            external_rank=external_rank,
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            consensus_rank_std=consensus_rank_std_val,
            analyst_score=analyst_score,
            pff_grade=pff_grade,
        )
        bluechip_floor_lift = 0.0
        if bluechip_floor is not None and model_score < bluechip_floor:
            bluechip_floor_lift = bluechip_floor - model_score
            model_score = bluechip_floor

        confidence_profile = _confidence_uncertainty_profile(
            final_grade=model_score,
            evidence_missing_count=int(formula.get("formula_evidence_missing_count", 0) or 0),
            risk_penalty=float(formula.get("formula_risk_penalty", 0.0) or 0.0),
            consensus_source_count=consensus_source_count_val,
            consensus_rank_std=consensus_rank_std_val,
            consensus_confidence_factor=consensus_confidence_factor,
            has_calibrated_prob=bool(calibrated_success_prob),
            testing_missing_weight=float(formula.get("formula_testing_missing_weight", 0.0) or 0.0),
            testing_missing_status=str(formula.get("formula_testing_missing_status", "") or ""),
        )

        scout_note = scouting_note(pos, model_score, row["rank_seed"])
        scouting_sections = _build_scouting_sections(
            name=row["player_name"],
            position=pos,
            school=row["school"],
            final_grade=model_score,
            round_value=round_from_grade(model_score),
            best_role=grades.get("best_role", ""),
            best_scheme_fit=grades.get("best_scheme_fit", ""),
            best_team_fit=fit_team,
            scouting_notes=scout_note,
            kiper_rank=str(kiper_rank),
            kiper_prev_rank=str(kiper_prev_rank),
            kiper_rank_delta=str(kiper_rank_delta),
            kiper_strength_tags=str(kiper.get("kiper_strength_tags", "")),
            kiper_concern_tags=str(kiper.get("kiper_concern_tags", "")),
            kiper_statline_2025=str(kiper.get("kiper_statline_2025", "")),
            tdn_strengths=str(tdn_ringer.get("tdn_strengths", "")),
            tdn_concerns=str(tdn_ringer.get("tdn_concerns", "")),
            br_strengths=str(tdn_ringer.get("br_strengths", "")),
            br_concerns=str(tdn_ringer.get("br_concerns", "")),
            atoz_strengths=str(tdn_ringer.get("atoz_strengths", "")),
            atoz_concerns=str(tdn_ringer.get("atoz_concerns", "")),
            si_strengths=str(tdn_ringer.get("si_strengths", "")),
            si_concerns=str(tdn_ringer.get("si_concerns", "")),
            pff_grade=pff_grade,
            espn_qbr=espn.get("espn_qbr", ""),
            espn_epa_per_play=espn.get("espn_epa_per_play", ""),
            cfb_prod_signal=round(cfb_prod_signal, 2) if cfb_player_available else "",
            cfb_prod_label=_cfb_prod_snapshot_label(pos, cfb),
            cfb_prod_quality=cfb.get("cfb_prod_quality_label", ""),
            cfb_prod_reliability=cfb.get("cfb_prod_reliability", ""),
            consensus_rank=row["rank_seed"],
            historical_combine_comp_1=str(hist_comp_1.get("player_name", "")),
            historical_combine_comp_1_year=hist_comp_1.get("year", ""),
            historical_combine_comp_1_similarity=hist_comp_1.get("similarity", ""),
            combine_height_in=combine.get("combine_height_in", ""),
            combine_weight_lb=combine.get("combine_weight_lb", ""),
            combine_arm_in=combine.get("combine_arm_in", ""),
            athletic_pct_forty=athletic_profile.get("athletic_pct_forty", ""),
            athletic_pct_ten_split=athletic_profile.get("athletic_pct_ten_split", ""),
            athletic_pct_vertical=athletic_profile.get("athletic_pct_vertical", ""),
            athletic_pct_broad=athletic_profile.get("athletic_pct_broad", ""),
            athletic_pct_shuttle=athletic_profile.get("athletic_pct_shuttle", ""),
            athletic_pct_three_cone=athletic_profile.get("athletic_pct_three_cone", ""),
            athletic_pct_weight_lb=athletic_profile.get("athletic_pct_weight_lb", ""),
            athletic_pct_arm_in=athletic_profile.get("athletic_pct_arm_in", ""),
            cfb_qb_epa_per_play=cfb.get("cfb_qb_epa_per_play", ""),
            cfb_qb_pressure_signal=cfb.get("cfb_qb_pressure_signal", ""),
            cfb_qb_pass_int=cfb.get("cfb_qb_pass_int", ""),
            cfb_wrte_yprr=cfb.get("cfb_wrte_yprr", ""),
            cfb_wrte_target_share=cfb.get("cfb_wrte_target_share", ""),
            cfb_rb_explosive_rate=cfb.get("cfb_rb_explosive_rate", ""),
            cfb_rb_missed_tackles_forced_per_touch=cfb.get("cfb_rb_missed_tackles_forced_per_touch", ""),
            cfb_edge_pressure_rate=cfb.get("cfb_edge_pressure_rate", ""),
            cfb_edge_sacks_per_pr_snap=cfb.get("cfb_edge_sacks_per_pr_snap", ""),
            cfb_edge_qb_hurries=cfb.get("cfb_edge_qb_hurries", ""),
            cfb_db_coverage_plays_per_target=cfb.get("cfb_db_coverage_plays_per_target", ""),
            cfb_db_yards_allowed_per_coverage_snap=cfb.get("cfb_db_yards_allowed_per_coverage_snap", ""),
            cfb_db_int=cfb.get("cfb_db_int", ""),
            cfb_db_pbu=cfb.get("cfb_db_pbu", ""),
        )
        cfb_proxy_audit = _cfb_proxy_audit_label(pos, cfb)
        cfb_proxy_heavy_flag, cfb_proxy_heavy_reason = _cfb_proxy_fallback_heavy_flag(cfb)

        report = {
            **row,
            "player_uid": f"{row['seed_row_id']}-{row['player_name'].lower().replace(' ', '-')}",
            "position": pos,
            "position_evidence_score": _position_evidence_score(
                name_key=key,
                pos=pos,
                row=row,
                ext=ext,
                analyst_votes=analyst_pos_votes,
                espn_by_name_pos=espn_by_name_pos,
                pp_by_name_pos=pp_by_name_pos,
                lang_by_name_pos=lang_by_name_pos,
            ),
            "height_in": effective_height_in,
            "weight_lb_effective": effective_weight_lb,
            "seed_signal": round(seed_signal, 2),
            "analyst_signal": round(analyst_score, 2),
            "external_rank": external_rank if external_rank is not None else "",
            "external_rank_signal": round(external_rank_signal, 2),
            "pff_grade": round(pff_grade, 2) if pff_grade is not None else "",
            "pff_waa": round(pff_waa, 3) if pff_waa is not None else "",
            "pff_grade_locked": True,
            "market_signal_score": round(market_signal_score, 2),
            "consensus_score": round(model_score, 2),
            "consensus_board_mean_rank": consensus_mean_rank,
            "consensus_board_rank_std": consensus_rank_std,
            "consensus_board_source_count": consensus_source_count,
            "consensus_board_sources": consensus_sources,
            "consensus_board_signal": round(consensus_signal, 2) if consensus_signal > 0 else "",
            "prior_anchor_adjustment": round(prior_anchor_adjustment, 2),
            "prior_weight_total_effective": round(sum(weight for weight, _ in prior_parts), 4),
            "kiper_rank": kiper_rank,
            "kiper_prev_rank": kiper_prev_rank,
            "kiper_rank_delta": kiper_rank_delta,
            "kiper_rank_signal": round(kiper_rank_signal, 2) if kiper_rank_signal > 0 else "",
            "kiper_strength_tags": kiper.get("kiper_strength_tags", ""),
            "kiper_concern_tags": kiper.get("kiper_concern_tags", ""),
            "kiper_statline_2025": kiper.get("kiper_statline_2025", ""),
            "kiper_statline_2025_games": kiper.get("kiper_statline_2025_games", ""),
            "kiper_statline_2025_yards": kiper.get("kiper_statline_2025_yards", ""),
            "kiper_statline_2025_tds": kiper.get("kiper_statline_2025_tds", ""),
            "kiper_statline_2025_efficiency": kiper.get("kiper_statline_2025_efficiency", ""),
            "kiper_games_norm": kiper.get("kiper_games_norm", ""),
            "kiper_yards_norm": kiper.get("kiper_yards_norm", ""),
            "kiper_tds_norm": kiper.get("kiper_tds_norm", ""),
            "kiper_efficiency_norm": kiper.get("kiper_efficiency_norm", ""),
            "kiper_statline_2025_norm": kiper.get("kiper_statline_2025_norm", ""),
            "kiper_volatility_flag": kiper_volatility_flag,
            "kiper_volatility_penalty": round(kiper_volatility_penalty, 2),
            "kiper_source_url": kiper.get("kiper_source_url", ""),
            "tdn_rank": tdn_ringer.get("tdn_rank", ""),
            "tdn_rank_signal": round(tdn_rank_signal, 2) if tdn_rank_signal > 0 else "",
            "tdn_grade_label": tdn_ringer.get("tdn_grade_label", ""),
            "tdn_grade_round": tdn_ringer.get("tdn_grade_round", ""),
            "tdn_grade_label_signal": round(tdn_grade_label_signal, 2) if tdn_grade_label_signal > 0 else "",
            "tdn_text_trait_signal": round(tdn_text_trait_signal, 2) if tdn_text_trait_signal > 0 else "",
            "tdn_text_coverage": tdn_ringer.get("tdn_text_coverage", ""),
            "tdn_risk_hits": tdn_ringer.get("tdn_risk_hits", ""),
            "tdn_risk_flag": tdn_ringer.get("tdn_risk_flag", ""),
            "tdn_risk_penalty": round(tdn_risk_penalty, 2) if tdn_risk_penalty > 0 else "",
            "tdn_strengths": tdn_ringer.get("tdn_strengths", ""),
            "tdn_concerns": tdn_ringer.get("tdn_concerns", ""),
            "tdn_summary": tdn_ringer.get("tdn_summary", ""),
            "ringer_rank": tdn_ringer.get("ringer_rank", ""),
            "ringer_rank_signal": round(ringer_rank_signal, 2) if ringer_rank_signal > 0 else "",
            "br_rank": tdn_ringer.get("br_rank", ""),
            "br_rank_signal": round(br_rank_signal, 2) if br_rank_signal > 0 else "",
            "br_text_trait_signal": round(br_text_trait_signal, 2) if br_text_trait_signal > 0 else "",
            "br_text_coverage": tdn_ringer.get("br_text_coverage", ""),
            "br_risk_hits": tdn_ringer.get("br_risk_hits", ""),
            "br_risk_flag": tdn_ringer.get("br_risk_flag", ""),
            "br_risk_penalty": round(br_risk_penalty, 2) if br_risk_penalty > 0 else "",
            "br_strengths": tdn_ringer.get("br_strengths", ""),
            "br_concerns": tdn_ringer.get("br_concerns", ""),
            "br_summary": tdn_ringer.get("br_summary", ""),
            "atoz_rank": tdn_ringer.get("atoz_rank", ""),
            "atoz_rank_signal": round(atoz_rank_signal, 2) if atoz_rank_signal > 0 else "",
            "atoz_text_trait_signal": round(atoz_text_trait_signal, 2) if atoz_text_trait_signal > 0 else "",
            "atoz_text_coverage": tdn_ringer.get("atoz_text_coverage", ""),
            "atoz_risk_hits": tdn_ringer.get("atoz_risk_hits", ""),
            "atoz_risk_flag": tdn_ringer.get("atoz_risk_flag", ""),
            "atoz_risk_penalty": round(atoz_risk_penalty, 2) if atoz_risk_penalty > 0 else "",
            "atoz_strengths": tdn_ringer.get("atoz_strengths", ""),
            "atoz_concerns": tdn_ringer.get("atoz_concerns", ""),
            "atoz_summary": tdn_ringer.get("atoz_summary", ""),
            "si_rank": tdn_ringer.get("si_rank", ""),
            "si_rank_signal": round(si_rank_signal, 2) if si_rank_signal > 0 else "",
            "si_text_trait_signal": round(si_text_trait_signal, 2) if si_text_trait_signal > 0 else "",
            "si_text_coverage": tdn_ringer.get("si_text_coverage", ""),
            "si_risk_hits": tdn_ringer.get("si_risk_hits", ""),
            "si_risk_flag": tdn_ringer.get("si_risk_flag", ""),
            "si_risk_penalty": round(si_risk_penalty, 2) if si_risk_penalty > 0 else "",
            "si_strengths": tdn_ringer.get("si_strengths", ""),
            "si_concerns": tdn_ringer.get("si_concerns", ""),
            "si_summary": tdn_ringer.get("si_summary", ""),
            "cbs_rank": tdn_ringer.get("cbs_rank", ""),
            "cbs_rank_signal": round(cbs_rank_signal, 2) if cbs_rank_signal > 0 else "",
            "cbs_wilson_rank": tdn_ringer.get("cbs_wilson_rank", ""),
            "cbs_wilson_rank_signal": round(cbs_wilson_rank_signal, 2) if cbs_wilson_rank_signal > 0 else "",
            "cbs_text_trait_signal": round(cbs_text_trait_signal, 2) if cbs_text_trait_signal > 0 else "",
            "cbs_text_coverage": tdn_ringer.get("cbs_text_coverage", ""),
            "cbs_risk_hits": tdn_ringer.get("cbs_risk_hits", ""),
            "cbs_risk_flag": tdn_ringer.get("cbs_risk_flag", ""),
            "cbs_risk_penalty": round(cbs_risk_penalty, 2) if cbs_risk_penalty > 0 else "",
            "cbs_summary": tdn_ringer.get("cbs_summary", ""),
            "espn_source_year": espn.get("espn_source_year", ""),
            "espn_ovr_rank": espn.get("espn_ovr_rank", ""),
            "espn_pos_rank": espn.get("espn_pos_rank", ""),
            "espn_grade": espn.get("espn_grade", ""),
            "espn_grade_z": espn.get("espn_grade_z", ""),
            "espn_rank_signal": round(espn_rank_signal, 2),
            "espn_pos_signal": round(espn_pos_signal, 2),
            "espn_grade_signal": round(espn_grade_signal, 2),
            "espn_prod_signal": round(espn_prod_signal, 2),
            "espn_qbr": espn.get("espn_qbr", ""),
            "espn_epa_per_play": espn.get("espn_epa_per_play", ""),
            "espn_trait_processing": espn.get("espn_trait_processing", ""),
            "espn_trait_separation": espn.get("espn_trait_separation", ""),
            "espn_trait_play_strength": espn.get("espn_trait_play_strength", ""),
            "espn_trait_motor": espn.get("espn_trait_motor", ""),
            "espn_trait_instincts": espn.get("espn_trait_instincts", ""),
            "espn_text_coverage": espn.get("espn_text_coverage", ""),
            "espn_volatility_flag": int(espn_volatility_flag),
            "espn_volatility_hits": espn.get("espn_volatility_hits", ""),
            "pp_source": pp.get("pp_source", ""),
            "pp_last_updated": pp.get("pp_last_updated", ""),
            "pp_breakout_age": pp.get("pp_breakout_age", ""),
            "pp_college_dominator": pp.get("pp_college_dominator", ""),
            "pp_breakout_signal": round(pp_breakout_signal, 2) if pp_player_available else "",
            "pp_dominator_signal": round(pp_dominator_signal, 2) if pp_player_available else "",
            "pp_skill_signal": round(pp_skill_signal, 2) if pp_player_available else "",
            "pp_data_coverage": pp.get("pp_data_coverage", "") if pp_player_available else "",
            "pp_early_declare": pp.get("pp_early_declare", "") if pp_player_available else "",
            "early_declare": early_declare_flag,
            "early_declare_flag": early_declare_flag,
            "early_declare_source_flag": early_declare_source_flag,
            "early_declare_source_count": early_declare_row.get("early_declare_evidence_count", 0),
            "early_declare_sources": early_declare_row.get("early_declare_sources", ""),
            "early_declare_source_urls": early_declare_row.get("early_declare_source_urls", ""),
            "combine_invited": combine_invited_flag,
            "combine_invite_sources": early_declare_row.get("combine_invite_sources", ""),
            "combine_invite_source_urls": early_declare_row.get("combine_invite_source_urls", ""),
            "pp_risk_flag": int(pp_risk_flag) if pp_player_available else "",
            "pp_profile_tier": pp.get("pp_profile_tier", ""),
            "pp_notes": pp.get("pp_notes", ""),
            "cfb_prod_signal": round(cfb_prod_signal, 2) if cfb_player_available else "",
            "sg_advanced_signal": cfb.get("sg_advanced_signal", ""),
            "sg_advanced_available_count": cfb.get("sg_advanced_available_count", 0),
            "sg_advanced_source": cfb.get("sg_advanced_source", ""),
            "cfb_proxy_audit_summary": cfb_proxy_audit,
            "cfb_proxy_fallback_heavy_flag": cfb_proxy_heavy_flag,
            "cfb_proxy_fallback_heavy_reason": cfb_proxy_heavy_reason,
            "cfb_prod_signal_raw": cfb.get("cfb_prod_signal_raw", ""),
            "cfb_prod_signal_contextual_raw": cfb.get("cfb_prod_signal_contextual_raw", ""),
            "cfb_prod_percentile_signal": cfb.get("cfb_prod_percentile_signal", ""),
            "cfb_prod_percentile_population_n": cfb.get("cfb_prod_percentile_population_n", ""),
            "cfb_prod_usage_rate": cfb.get("cfb_prod_usage_rate", ""),
            "cfb_prod_usage_multiplier": cfb.get("cfb_prod_usage_multiplier", ""),
            "cfb_prod_context_conference": cfb.get("cfb_prod_context_conference", ""),
            "cfb_opp_def_ppa_allowed_avg": cfb.get("cfb_opp_def_ppa_allowed_avg", ""),
            "cfb_opp_def_success_rate_allowed_avg": cfb.get("cfb_opp_def_success_rate_allowed_avg", ""),
            "cfb_opp_def_toughness_index": cfb.get("cfb_opp_def_toughness_index", ""),
            "cfb_opp_def_adjustment_multiplier": cfb.get("cfb_opp_def_adjustment_multiplier", ""),
            "cfb_opp_def_adjustment_delta": cfb.get("cfb_opp_def_adjustment_delta", ""),
            "cfb_opp_def_context_applied": cfb.get("cfb_opp_def_context_applied", 0),
            "cfb_opp_def_context_source": cfb.get("cfb_opp_def_context_source", ""),
            "cfb_prod_available": 1 if cfb_player_available else 0,
            "cfb_prod_coverage_count": cfb_prod_coverage_count,
            "cfb_prod_quality_label": cfb.get("cfb_prod_quality_label", ""),
            "cfb_prod_reliability": cfb.get("cfb_prod_reliability", ""),
            "cfb_prod_real_features": cfb.get("cfb_prod_real_features", ""),
            "cfb_prod_proxy_features": cfb.get("cfb_prod_proxy_features", ""),
            "cfb_prod_proxy_fallback_features": cfb.get("cfb_prod_proxy_fallback_features", ""),
            "cfb_years_played": cfb.get("cfb_years_played", ""),
            "cfb_years_played_seasons": cfb.get("cfb_years_played_seasons", ""),
            "cfb_years_played_source": cfb.get("cfb_years_played_source", ""),
            "birth_date": draft_age_row.get("birth_date", ""),
            "draft_age": draft_age_row.get("draft_age", ""),
            "draft_age_source": draft_age_row.get("draft_age_source", ""),
            "draft_age_source_url": draft_age_row.get("draft_age_source_url", ""),
            "draft_age_ref_date": draft_age_row.get("draft_age_ref_date", ""),
            "draft_age_available": draft_age_row.get("draft_age_available", 0),
            "age": draft_age_row.get("draft_age", ""),
            "cfb_nonpos_metrics_ignored_count": cfb.get("cfb_nonpos_metrics_ignored_count", ""),
            "cfb_nonpos_metrics_ignored_fields": cfb.get("cfb_nonpos_metrics_ignored_fields", ""),
            "cfb_prod_provenance": cfb.get("cfb_prod_provenance", ""),
            "cfbfastr_p0_signal_raw": cfb.get("cfbfastr_p0_signal_raw", ""),
            "cfbfastr_p0_available": cfb.get("cfbfastr_p0_available", 0),
            "cfbfastr_p0_mode": cfb.get("cfbfastr_p0_mode", ""),
            "cfbfastr_p0_applied_delta": cfb.get("cfbfastr_p0_applied_delta", ""),
            "cfbfastr_p0_max_delta": cfb.get("cfbfastr_p0_max_delta", ""),
            "cfbfastr_p0_coverage_count": cfb.get("cfbfastr_p0_coverage_count", 0),
            "cfb_qb_eff_signal": cfb.get("cfb_qb_eff_signal", ""),
            "cfb_qb_pressure_signal": cfb.get("cfb_qb_pressure_signal", ""),
            "cfb_wrte_yprr_signal": cfb.get("cfb_wrte_yprr_signal", ""),
            "cfb_wrte_target_share_signal": cfb.get("cfb_wrte_target_share_signal", ""),
            "cfb_wrte_targets_per_route_signal": cfb.get("cfb_wrte_targets_per_route_signal", ""),
            "cfb_rb_explosive_signal": cfb.get("cfb_rb_explosive_signal", ""),
            "cfb_rb_mtf_signal": cfb.get("cfb_rb_mtf_signal", ""),
            "cfb_rb_yac_per_att_signal": cfb.get("cfb_rb_yac_per_att_signal", ""),
            "cfb_rb_target_share_signal": cfb.get("cfb_rb_target_share_signal", ""),
            "cfb_rb_receiving_eff_signal": cfb.get("cfb_rb_receiving_eff_signal", ""),
            "cfb_edge_pressure_signal": cfb.get("cfb_edge_pressure_signal", ""),
            "cfb_edge_sacks_per_pr_snap_signal": cfb.get("cfb_edge_sacks_per_pr_snap_signal", ""),
            "cfb_lb_signal": cfb.get("cfb_lb_signal", ""),
            "cfb_lb_tackle_signal": cfb.get("cfb_lb_tackle_signal", ""),
            "cfb_lb_tfl_signal": cfb.get("cfb_lb_tfl_signal", ""),
            "cfb_lb_rush_impact_signal": cfb.get("cfb_lb_rush_impact_signal", ""),
            "cfb_ol_proxy_signal": cfb.get("cfb_ol_proxy_signal", ""),
            "cfb_db_cov_plays_per_target_signal": cfb.get("cfb_db_cov_plays_per_target_signal", ""),
            "cfb_db_yards_allowed_per_cov_snap_signal": cfb.get("cfb_db_yards_allowed_per_cov_snap_signal", ""),
            "cfb_qb_epa_per_play": cfb.get("cfb_qb_epa_per_play", ""),
            "cfb_qb_pass_att": cfb.get("cfb_qb_pass_att", ""),
            "cfb_qb_pass_comp": cfb.get("cfb_qb_pass_comp", ""),
            "cfb_qb_pass_yds": cfb.get("cfb_qb_pass_yds", ""),
            "cfb_qb_pass_td": cfb.get("cfb_qb_pass_td", ""),
            "cfb_qb_pass_int": cfb.get("cfb_qb_pass_int", ""),
            "cfb_qb_int_rate": cfb.get("cfb_qb_int_rate", ""),
            "cfb_qb_rush_yds": cfb.get("cfb_qb_rush_yds", ""),
            "cfb_qb_rush_td": cfb.get("cfb_qb_rush_td", ""),
            "cfb_wrte_yprr": cfb.get("cfb_wrte_yprr", ""),
            "cfb_wrte_target_share": cfb.get("cfb_wrte_target_share", ""),
            "cfb_wrte_targets_per_route": cfb.get("cfb_wrte_targets_per_route", ""),
            "cfb_wrte_targets_per_route_source": cfb.get("cfb_wrte_targets_per_route_source", ""),
            "cfb_wrte_targets_per_route_weight": cfb.get("cfb_wrte_targets_per_route_weight", ""),
            "cfb_wrte_rec": cfb.get("cfb_wrte_rec", ""),
            "cfb_wrte_rec_yds": cfb.get("cfb_wrte_rec_yds", ""),
            "cfb_wrte_rec_td": cfb.get("cfb_wrte_rec_td", ""),
            "cfb_rb_explosive_rate": cfb.get("cfb_rb_explosive_rate", ""),
            "cfb_rb_missed_tackles_forced_per_touch": cfb.get("cfb_rb_missed_tackles_forced_per_touch", ""),
            "cfb_rb_yards_after_contact_per_attempt": cfb.get("cfb_rb_yards_after_contact_per_attempt", ""),
            "cfb_rb_target_share": cfb.get("cfb_rb_target_share", ""),
            "cfb_rb_receiving_efficiency": cfb.get("cfb_rb_receiving_efficiency", ""),
            "cfb_rb_target_share_source": cfb.get("cfb_rb_target_share_source", ""),
            "cfb_rb_rush_att": cfb.get("cfb_rb_rush_att", ""),
            "cfb_rb_rush_yds": cfb.get("cfb_rb_rush_yds", ""),
            "cfb_rb_rush_td": cfb.get("cfb_rb_rush_td", ""),
            "cfb_rb_rec": cfb.get("cfb_rb_rec", ""),
            "cfb_rb_rec_yds": cfb.get("cfb_rb_rec_yds", ""),
            "cfb_rb_rec_td": cfb.get("cfb_rb_rec_td", ""),
            "cfb_lb_tackles": cfb.get("cfb_lb_tackles", ""),
            "cfb_lb_tfl": cfb.get("cfb_lb_tfl", ""),
            "cfb_lb_sacks": cfb.get("cfb_lb_sacks", ""),
            "cfb_lb_qb_hurries": cfb.get("cfb_lb_qb_hurries", ""),
            "cfb_lb_usage_rate": cfb.get("cfb_lb_usage_rate", ""),
            "cfb_lb_def_snaps": cfb.get("cfb_lb_def_snaps", ""),
            "cfb_lb_rate_source": cfb.get("cfb_lb_rate_source", ""),
            "cfb_ol_years_played": cfb.get("cfb_ol_years_played", ""),
            "cfb_ol_starts": cfb.get("cfb_ol_starts", ""),
            "cfb_ol_usage_rate": cfb.get("cfb_ol_usage_rate", ""),
            "cfb_ol_proxy_quality_label": cfb.get("cfb_ol_proxy_quality_label", ""),
            "cfb_edge_pressure_rate": cfb.get("cfb_edge_pressure_rate", ""),
            "cfb_edge_sacks_per_pr_snap": cfb.get("cfb_edge_sacks_per_pr_snap", ""),
            "cfb_edge_sacks_per_pr_snap_source": cfb.get("cfb_edge_sacks_per_pr_snap_source", ""),
            "cfb_edge_pressure_weight": cfb.get("cfb_edge_pressure_weight", ""),
            "cfb_edge_sack_weight": cfb.get("cfb_edge_sack_weight", ""),
            "cfb_edge_sacks": cfb.get("cfb_edge_sacks", ""),
            "cfb_edge_qb_hurries": cfb.get("cfb_edge_qb_hurries", ""),
            "cfb_edge_tfl": cfb.get("cfb_edge_tfl", ""),
            "cfb_edge_tackles": cfb.get("cfb_edge_tackles", ""),
            "cfb_db_coverage_plays_per_target": cfb.get("cfb_db_coverage_plays_per_target", ""),
            "cfb_db_yards_allowed_per_coverage_snap": cfb.get("cfb_db_yards_allowed_per_coverage_snap", ""),
            "cfb_db_yards_allowed_per_cov_snap_source": cfb.get("cfb_db_yards_allowed_per_cov_snap_source", ""),
            "cfb_db_cov_weight": cfb.get("cfb_db_cov_weight", ""),
            "cfb_db_yacs_weight": cfb.get("cfb_db_yacs_weight", ""),
            "cfb_db_int": cfb.get("cfb_db_int", ""),
            "cfb_db_pbu": cfb.get("cfb_db_pbu", ""),
            "cfb_db_tackles": cfb.get("cfb_db_tackles", ""),
            "cfb_db_tfl": cfb.get("cfb_db_tfl", ""),
            "sg_advanced_signal": cfb.get("sg_advanced_signal", ""),
            "sg_advanced_available_count": cfb.get("sg_advanced_available_count", 0),
            "sg_advanced_source": cfb.get("sg_advanced_source", ""),
            "sg_qb_pass_grade": cfb.get("sg_qb_pass_grade", ""),
            "sg_qb_btt_rate": cfb.get("sg_qb_btt_rate", ""),
            "sg_qb_twp_rate": cfb.get("sg_qb_twp_rate", ""),
            "sg_qb_pressure_to_sack_rate": cfb.get("sg_qb_pressure_to_sack_rate", ""),
            "sg_qb_pressure_grade": cfb.get("sg_qb_pressure_grade", ""),
            "sg_qb_blitz_grade": cfb.get("sg_qb_blitz_grade", ""),
            "sg_qb_no_screen_grade": cfb.get("sg_qb_no_screen_grade", ""),
            "sg_qb_quick_qb_rating": cfb.get("sg_qb_quick_qb_rating", ""),
            "sg_rb_run_grade": cfb.get("sg_rb_run_grade", ""),
            "sg_rb_elusive_rating": cfb.get("sg_rb_elusive_rating", ""),
            "sg_rb_yco_attempt": cfb.get("sg_rb_yco_attempt", ""),
            "sg_rb_explosive_rate": cfb.get("sg_rb_explosive_rate", ""),
            "sg_rb_breakaway_percent": cfb.get("sg_rb_breakaway_percent", ""),
            "sg_rb_targets_per_route": cfb.get("sg_rb_targets_per_route", ""),
            "sg_rb_yprr": cfb.get("sg_rb_yprr", ""),
            "sg_wrte_route_grade": cfb.get("sg_wrte_route_grade", ""),
            "sg_wrte_yprr": cfb.get("sg_wrte_yprr", ""),
            "sg_wrte_targets_per_route": cfb.get("sg_wrte_targets_per_route", ""),
            "sg_wrte_man_yprr": cfb.get("sg_wrte_man_yprr", ""),
            "sg_wrte_zone_yprr": cfb.get("sg_wrte_zone_yprr", ""),
            "sg_wrte_contested_catch_rate": cfb.get("sg_wrte_contested_catch_rate", ""),
            "sg_wrte_drop_rate": cfb.get("sg_wrte_drop_rate", ""),
            "sg_dl_pass_rush_grade": cfb.get("sg_dl_pass_rush_grade", ""),
            "sg_dl_pass_rush_win_rate": cfb.get("sg_dl_pass_rush_win_rate", ""),
            "sg_dl_prp": cfb.get("sg_dl_prp", ""),
            "sg_dl_true_pass_set_win_rate": cfb.get("sg_dl_true_pass_set_win_rate", ""),
            "sg_dl_true_pass_set_prp": cfb.get("sg_dl_true_pass_set_prp", ""),
            "sg_dl_total_pressures": cfb.get("sg_dl_total_pressures", ""),
            "sg_front_run_def_grade": cfb.get("sg_front_run_def_grade", ""),
            "sg_front_stop_percent": cfb.get("sg_front_stop_percent", ""),
            "sg_def_coverage_grade": cfb.get("sg_def_coverage_grade", ""),
            "sg_def_run_grade": cfb.get("sg_def_run_grade", ""),
            "sg_def_tackle_grade": cfb.get("sg_def_tackle_grade", ""),
            "sg_def_missed_tackle_rate": cfb.get("sg_def_missed_tackle_rate", ""),
            "sg_def_total_pressures": cfb.get("sg_def_total_pressures", ""),
            "sg_def_tackles_for_loss": cfb.get("sg_def_tackles_for_loss", ""),
            "sg_def_tackles": cfb.get("sg_def_tackles", ""),
            "sg_def_pass_break_ups": cfb.get("sg_def_pass_break_ups", ""),
            "sg_def_interceptions": cfb.get("sg_def_interceptions", ""),
            "sg_cov_grade": cfb.get("sg_cov_grade", ""),
            "sg_cov_forced_incompletion_rate": cfb.get("sg_cov_forced_incompletion_rate", ""),
            "sg_cov_snaps_per_target": cfb.get("sg_cov_snaps_per_target", ""),
            "sg_cov_yards_per_snap": cfb.get("sg_cov_yards_per_snap", ""),
            "sg_cov_qb_rating_against": cfb.get("sg_cov_qb_rating_against", ""),
            "sg_source_season": cfb.get("sg_source_season", ""),
            "sg_cov_source_season": cfb.get("sg_cov_source_season", ""),
            "sg_cov_man_grade": cfb.get("sg_cov_man_grade", ""),
            "sg_cov_zone_grade": cfb.get("sg_cov_zone_grade", ""),
            "sg_slot_cov_snaps": cfb.get("sg_slot_cov_snaps", ""),
            "sg_slot_cov_snaps_per_target": cfb.get("sg_slot_cov_snaps_per_target", ""),
            "sg_slot_cov_qb_rating_against": cfb.get("sg_slot_cov_qb_rating_against", ""),
            "sg_slot_cov_yards_per_snap": cfb.get("sg_slot_cov_yards_per_snap", ""),
            "sg_ol_pass_block_grade": cfb.get("sg_ol_pass_block_grade", ""),
            "sg_ol_run_block_grade": cfb.get("sg_ol_run_block_grade", ""),
            "sg_ol_pbe": cfb.get("sg_ol_pbe", ""),
            "sg_ol_pressure_allowed_rate": cfb.get("sg_ol_pressure_allowed_rate", ""),
            "sg_ol_versatility_count": cfb.get("sg_ol_versatility_count", ""),
            "cfb_source": cfb.get("cfb_source", ""),
            "cfb_season": cfb.get("cfb_season", ""),
            "lang_source_count": lang.get("lang_source_count", ""),
            "lang_text_coverage": lang.get("lang_text_coverage", ""),
            "lang_trait_processing": lang.get("lang_trait_processing", ""),
            "lang_trait_technique": lang.get("lang_trait_technique", ""),
            "lang_trait_explosiveness": lang.get("lang_trait_explosiveness", ""),
            "lang_trait_physicality": lang.get("lang_trait_physicality", ""),
            "lang_trait_competitiveness": lang.get("lang_trait_competitiveness", ""),
            "lang_trait_versatility": lang.get("lang_trait_versatility", ""),
            "lang_miller_keyword_hits": lang.get("lang_miller_keyword_hits", ""),
            "lang_miller_coverage": lang.get("lang_miller_coverage", ""),
            "lang_risk_hits": lang.get("lang_risk_hits", ""),
            "lang_risk_flag": lang.get("lang_risk_flag", ""),
            "lang_trait_composite": lang.get("lang_trait_composite", ""),
            "lang_sources": lang.get("lang_sources", ""),
            "lang_report_word_count": language_features.get("lang_report_word_count", 0),
            "lang_positive_trait_rate": language_features.get("lang_positive_trait_rate", ""),
            "lang_developmental_flag_rate": language_features.get("lang_developmental_flag_rate", ""),
            "lang_concern_rate": language_features.get("lang_concern_rate", ""),
            "language_adjustment_raw": language_features.get("language_adjustment_raw", ""),
            "language_adjustment_confidence": language_features.get("language_adjustment_confidence", ""),
            "language_adjustment_applied": language_adjustment_applied,
            "language_adjustment_cap": language_features.get("language_adjustment_cap", ""),
            # combine fields
            "combine_source": combine.get("combine_source", ""),
            "combine_last_updated": combine.get("combine_last_updated", ""),
            "combine_height_in": combine.get("height_in", ""),
            "combine_weight_lb": combine.get("weight_lb", ""),
            "combine_arm_in": combine.get("arm_in", ""),
            "combine_hand_in": combine.get("hand_in", ""),
            "combine_forty": combine.get("forty", ""),
            "combine_ten_split": combine.get("ten_split", ""),
            "combine_vertical": combine.get("vertical", ""),
            "combine_broad": combine.get("broad", ""),
            "combine_shuttle": combine.get("shuttle", ""),
            "combine_three_cone": combine.get("three_cone", ""),
            "combine_bench": combine.get("bench", ""),
            "combine_ras_official": combine.get("ras_official", ""),
            "combine_testing_status": combine.get(
                "combine_testing_status", formula.get("formula_testing_missing_status", "unknown")
            ),
            "combine_testing_event_count": combine.get("combine_testing_event_count", 0),
            "combine_measurement_count": combine.get("combine_measurement_count", ""),
            **md_features,
            "film_traits_source": film.get("source", ""),
            "film_eval_date": film.get("eval_date", ""),
            **grades,
            **formula,
            "weight_prior_tdn_rank": 0.08,
            "weight_prior_ringer_rank": 0.08,
            "weight_prior_bleacher_rank": 0.09,
            "weight_prior_atoz_rank": 0.08,
            "weight_prior_si_rank": 0.03,
            "weight_prior_cbs_rank": 0.10,
            "weight_prior_cbs_wilson_rank": 0.06,
            "weight_prior_tdn_grade_label": 0.04,
            "weight_prior_reliability_layers": "|".join(
                sorted(
                    {
                        str(v.get("layer", "")).strip()
                        for v in prior_diag.values()
                        if str(v.get("layer", "")).strip()
                    }
                )
            ),
            "weight_prior_reliability_year_min": (
                min(
                    int(v.get("selected_year"))
                    for v in prior_diag.values()
                    if str(v.get("selected_year", "")).strip()
                )
                if any(str(v.get("selected_year", "")).strip() for v in prior_diag.values())
                else ""
            ),
            "weight_prior_reliability_year_max": (
                max(
                    int(v.get("selected_year"))
                    for v in prior_diag.values()
                    if str(v.get("selected_year", "")).strip()
                )
                if any(str(v.get("selected_year", "")).strip() for v in prior_diag.values())
                else ""
            ),
            "weight_prior_seed_multiplier": prior_diag.get("seed_rank", {}).get("multiplier", ""),
            "weight_prior_external_multiplier": prior_diag.get("external_rank", {}).get("multiplier", ""),
            "weight_prior_analyst_multiplier": prior_diag.get("analyst_rank", {}).get("multiplier", ""),
            "weight_prior_consensus_multiplier": prior_diag.get("consensus_rank", {}).get("multiplier", ""),
            "weight_prior_kiper_multiplier": prior_diag.get("kiper_rank", {}).get("multiplier", ""),
            "weight_prior_tdn_multiplier": prior_diag.get("tdn_rank", {}).get("multiplier", ""),
            "weight_prior_ringer_multiplier": prior_diag.get("ringer_rank", {}).get("multiplier", ""),
            "weight_prior_bleacher_multiplier": prior_diag.get("bleacher_rank", {}).get("multiplier", ""),
            "weight_prior_atoz_multiplier": prior_diag.get("atoz_rank", {}).get("multiplier", ""),
            "weight_prior_si_multiplier": prior_diag.get("si_rank", {}).get("multiplier", ""),
            "weight_prior_cbs_multiplier": prior_diag.get("cbs_rank", {}).get("multiplier", ""),
            "weight_prior_cbs_wilson_multiplier": prior_diag.get("cbs_wilson_rank", {}).get("multiplier", ""),
            "weight_prior_tdn_grade_multiplier": prior_diag.get("tdn_grade_label", {}).get("multiplier", ""),
            "weight_trait_tdn_text": 0.05,
            "weight_trait_bleacher_text": 0.04,
            "weight_trait_atoz_text": 0.04,
            "weight_trait_si_text": 0.02,
            "weight_trait_cbs_text": 0.05,
            "formula_guardrail_penalty": round(guardrail_penalty, 2),
            "formula_drift_penalty": round(drift_penalty, 2),
            "formula_consensus_confidence_factor": round(consensus_confidence_factor, 3),
            "formula_midband_brake_penalty": round(midband_brake_penalty, 2),
            "formula_soft_ceiling_target": round(soft_ceiling_target, 2) if soft_ceiling_target is not None else "",
            "formula_soft_ceiling_penalty": round(soft_ceiling_penalty, 2),
            "formula_language_adjustment": round(language_adjustment_applied, 4),
            "formula_top75_gate_penalty": round(top75_gate_penalty, 2),
            "formula_hard_cap": round(hard_cap, 2) if hard_cap is not None else "",
            "formula_consensus_outlier_cap": round(outlier_cap, 2) if outlier_cap is not None else "",
            "formula_hard_cap_penalty": round(cap_penalty, 2),
            "formula_consensus_tail_soft_target": consensus_tail_target if consensus_tail_target is not None else "",
            "formula_consensus_tail_soft_penalty": round(consensus_tail_penalty, 2),
            "formula_front7_inflation_penalty": round(front7_inflation_penalty, 2),
            "formula_front7_inflation_reason": front7_inflation_reason,
            "formula_front7_success_prob_pre_brake": front7_success_prob_before_brake,
            "formula_cb_nickel_inflation_penalty": round(cb_nickel_inflation_penalty, 2),
            "formula_cb_nickel_inflation_reason": cb_nickel_inflation_reason,
            "formula_bluechip_floor": round(bluechip_floor, 2) if bluechip_floor is not None else "",
            "formula_bluechip_floor_lift": round(bluechip_floor_lift, 2),
            "roi_pick_band": roi_pick_band,
            "roi_prior_sample_n": roi_sample_n,
            "roi_prior_weighted_mean_surplus": roi_row.get("weighted_mean_surplus", ""),
            "roi_prior_surplus_z": roi_row.get("surplus_z", ""),
            "roi_prior_adjustment": round(roi_base_adjustment, 4),
            "roi_prior_adjustment_applied": roi_adjustment_applied,
            "is_diamond_exception": 1 if is_diamond_exception else 0,
            "diamond_exception_reasons": diamond_exception_reasons,
            "contrarian_score": round(contrarian_score, 2),
            "confidence_score": confidence_profile["confidence_score"],
            "uncertainty_score": confidence_profile["uncertainty_score"],
            "variance_flag": confidence_profile["variance_flag"],
            "calibration_position_delta": round(calibration_pos_delta, 4) if calibration_cfg is not None else "",
            "calibration_grade_adjustment": round(calibration_grade_adjustment, 2) if calibration_cfg is not None else "",
            "calibrated_success_prob": calibrated_success_prob,
            "legacy_final_grade": grades.get("final_grade", ""),
            "legacy_floor_grade": grades.get("floor_grade", ""),
            "legacy_ceiling_grade": grades.get("ceiling_grade", ""),
            "legacy_round_value": grades.get("round_value", ""),
            "final_grade": round(model_score, 2),
            "floor_grade": round(
                max(
                    52.0,
                    float(formula["formula_floor"])
                    + calibration_grade_adjustment
                    + language_adjustment_applied
                    - guardrail_penalty
                    - (0.7 * soft_ceiling_penalty)
                    - (0.8 * cap_penalty)
                    - (0.8 * consensus_tail_penalty)
                    - (0.85 * front7_inflation_penalty)
                    - (0.75 * cb_nickel_inflation_penalty),
                ),
                2,
            ),
            "ceiling_grade": round(
                max(
                    55.0,
                    float(formula["formula_ceiling"])
                    + calibration_grade_adjustment
                    + language_adjustment_applied
                    - (0.5 * guardrail_penalty)
                    - (0.5 * soft_ceiling_penalty)
                    - (0.5 * cap_penalty)
                    - (0.6 * consensus_tail_penalty)
                    - (0.60 * front7_inflation_penalty)
                    - (0.55 * cb_nickel_inflation_penalty),
                ),
                2,
            ),
            "round_value": round_from_grade(model_score),
            "best_team_fit": fit_team,
            "best_team_fit_score": fit_score,
            **comp,
            **ras,
            "ras_benchmark_starter_target": round(starter_target, 2) if starter_target is not None else "",
            "ras_benchmark_impact_target": round(impact_target, 2) if impact_target is not None else "",
            "ras_benchmark_elite_target": round(elite_target, 2) if elite_target is not None else "",
            "ras_meets_starter_target": meets_starter,
            "ras_meets_impact_target": meets_impact,
            "ras_meets_elite_target": meets_elite,
            "ras_benchmark_sample_n": ras_bench.get("sample_n_all", ""),
            **ras_comps,
            "historical_combine_merge_key": build_combine_merge_key(
                player_name=row["player_name"],
                position=pos,
                school=row.get("school", ""),
                year=2026,
            ),
            "historical_combine_source": historical_combine_pack.get("meta", {}).get("path", ""),
            "historical_combine_candidate_count": hist_comp_result.get("candidate_count", 0),
            "historical_combine_overlap_min": hist_comp_result.get("used_overlap_min", ""),
            "historical_combine_comp_1": hist_comp_1.get("player_name", ""),
            "historical_combine_comp_1_year": hist_comp_1.get("year", ""),
            "historical_combine_comp_1_school": hist_comp_1.get("school", ""),
            "historical_combine_comp_1_similarity": hist_comp_1.get("similarity", ""),
            "historical_combine_comp_1_overlap_metrics": hist_comp_1.get("overlap_metrics", ""),
            "historical_combine_comp_1_athlete_id": hist_comp_1.get("athlete_id", ""),
            "historical_combine_comp_1_merge_key": hist_comp_1.get("merge_key", ""),
            "historical_combine_comp_2": hist_comp_2.get("player_name", ""),
            "historical_combine_comp_2_year": hist_comp_2.get("year", ""),
            "historical_combine_comp_2_school": hist_comp_2.get("school", ""),
            "historical_combine_comp_2_similarity": hist_comp_2.get("similarity", ""),
            "historical_combine_comp_2_overlap_metrics": hist_comp_2.get("overlap_metrics", ""),
            "historical_combine_comp_2_athlete_id": hist_comp_2.get("athlete_id", ""),
            "historical_combine_comp_2_merge_key": hist_comp_2.get("merge_key", ""),
            "historical_combine_comp_3": hist_comp_3.get("player_name", ""),
            "historical_combine_comp_3_year": hist_comp_3.get("year", ""),
            "historical_combine_comp_3_school": hist_comp_3.get("school", ""),
            "historical_combine_comp_3_similarity": hist_comp_3.get("similarity", ""),
            "historical_combine_comp_3_overlap_metrics": hist_comp_3.get("overlap_metrics", ""),
            "historical_combine_comp_3_athlete_id": hist_comp_3.get("athlete_id", ""),
            "historical_combine_comp_3_merge_key": hist_comp_3.get("merge_key", ""),
            "production_knn_source": prod_knn_result.get("source", ""),
            "production_knn_candidate_mode": prod_knn_result.get("candidate_mode", ""),
            "production_knn_target_year": prod_knn_result.get("target_year", ""),
            "production_knn_vector_coverage": prod_knn_result.get("coverage", 0),
            "production_knn_vector_metric_count": prod_knn_result.get("metric_count", 0),
            "production_knn_baseline_metrics": ";".join(PROD_POSITION_BASELINES.get(pos, [])),
            "production_knn_reverse_metrics": ";".join(sorted(PROD_REVERSE_METRICS)),
            "production_knn_comp_1": prod_knn_1.get("player_name", ""),
            "production_knn_comp_1_year": prod_knn_1.get("year", ""),
            "production_knn_comp_1_similarity": prod_knn_1.get("similarity", ""),
            "production_knn_comp_1_overlap_metrics": prod_knn_1.get("overlap_metrics", ""),
            "production_knn_comp_2": prod_knn_2.get("player_name", ""),
            "production_knn_comp_2_year": prod_knn_2.get("year", ""),
            "production_knn_comp_2_similarity": prod_knn_2.get("similarity", ""),
            "production_knn_comp_2_overlap_metrics": prod_knn_2.get("overlap_metrics", ""),
            "production_knn_comp_3": prod_knn_3.get("player_name", ""),
            "production_knn_comp_3_year": prod_knn_3.get("year", ""),
            "production_knn_comp_3_similarity": prod_knn_3.get("similarity", ""),
            "production_knn_comp_3_overlap_metrics": prod_knn_3.get("overlap_metrics", ""),
            "scouting_notes": scout_note,
            **scouting_sections,
            "headshot_url": "",
        }
        prior_overhang = max(
            0.0,
            float(report.get("formula_prior_grade", 0.0) or 0.0)
            - float(report.get("formula_calibrated_grade", 0.0) or 0.0),
        )
        uncertainty_drag = max(
            0.0,
            float(report.get("uncertainty_score", 0.0) or 0.0) - 55.0,
        ) * float(RANK_UNCERTAINTY_DRAG_WEIGHT)
        prior_drag = (
            (prior_overhang / max(1.0, float(RANK_PRIOR_OVERHANG_SCALE)))
            * float(RANK_PRIOR_OVERHANG_DRAG_WEIGHT)
        )
        # Fine-tune: cap rank drag for true blue-chip / high-grade profiles so
        # noisy priors do not over-demote otherwise strong evaluations.
        total_drag = uncertainty_drag + prior_drag
        drag_cap = None
        consensus_score_for_rank = float(report.get("consensus_score", 0.0) or 0.0)
        if consensus_score_for_rank >= 88.0 or (external_rank is not None and external_rank <= 16):
            drag_cap = float(RANK_DRAG_CAP_BLUECHIP)
        elif consensus_score_for_rank >= 85.0 or (external_rank is not None and external_rank <= 40):
            drag_cap = float(RANK_DRAG_CAP_HIGHGRADE)
        if drag_cap is not None and total_drag > drag_cap and total_drag > 0:
            scale = drag_cap / total_drag
            uncertainty_drag *= scale
            prior_drag *= scale
            total_drag = drag_cap

        rank_sort_consensus_realign_adjustment = _rank_sort_consensus_realign_adjustment(
            position=pos,
            rank_seed=int(row["rank_seed"]),
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            consensus_rank_std=consensus_rank_std_val,
        )
        rank_sort_score = (
            float(report.get("consensus_score", 0.0) or 0.0)
            - uncertainty_drag
            - prior_drag
            + rank_sort_consensus_realign_adjustment
        )
        bluechip_rank_protection_adjustment = _bluechip_rank_protection_adjustment(
            consensus_score=float(report.get("consensus_score", 0.0) or 0.0),
            external_rank=external_rank,
            consensus_mean_rank=consensus_mean_rank_val,
            consensus_source_count=consensus_source_count_val,
            uncertainty_score=float(report.get("uncertainty_score", 0.0) or 0.0),
            rank_sort_total_drag=float(total_drag),
            evidence_signal_count=top50_evidence_signal_count,
        )
        rank_sort_score += bluechip_rank_protection_adjustment
        report["rank_sort_score_base"] = round(rank_sort_score, 4)
        report["rank_sort_score"] = round(rank_sort_score, 4)
        report["rank_sort_uncertainty_drag"] = round(uncertainty_drag, 4)
        report["rank_sort_prior_overhang"] = round(prior_overhang, 4)
        report["rank_sort_prior_drag"] = round(prior_drag, 4)
        report["rank_sort_total_drag"] = round(total_drag, 4)
        report["rank_sort_drag_cap"] = round(drag_cap, 4) if drag_cap is not None else ""
        report["rank_sort_consensus_realign_adjustment"] = round(
            rank_sort_consensus_realign_adjustment, 4
        )
        report["top50_evidence_signal_count"] = top50_evidence_signal_count
        report["top50_evidence_signal_labels"] = top50_evidence_signal_labels
        report["top50_evidence_min_required"] = int(TOP50_EVIDENCE_MIN_SIGNALS)
        report["top50_evidence_missing_signals"] = top50_evidence_missing_signals
        report["top50_evidence_brake_penalty"] = 0.0
        report["top50_evidence_brake_applied"] = 0
        report["top50_evidence_brake_reason"] = ""
        report["bluechip_rank_protection_adjustment"] = round(
            bluechip_rank_protection_adjustment, 4
        )
        report["rank_driver_summary"] = _build_rank_driver_summary(
            model_score=float(report.get("consensus_score", 0.0) or 0.0),
            formula=formula,
            prior_signal=prior_signal,
            language_adjustment_applied=language_adjustment_applied,
            guardrail_penalty=float(guardrail_penalty),
            drift_penalty=float(drift_penalty),
            soft_ceiling_penalty=float(soft_ceiling_penalty),
            cap_penalty=float(cap_penalty),
            consensus_tail_penalty=float(consensus_tail_penalty),
            front7_inflation_penalty=float(front7_inflation_penalty),
            cb_nickel_inflation_penalty=float(cb_nickel_inflation_penalty),
            bluechip_floor_lift=float(bluechip_floor_lift),
            top50_evidence_brake_penalty=0.0,
            bluechip_rank_protection_adjustment=float(bluechip_rank_protection_adjustment),
        )
        enriched.append(report)

    # Safety dedupe after enrichment.
    # De-dupe by canonical player name (not name+position) to prevent duplicate players in different position buckets.
    # Choose row by strongest position evidence first, then higher formula score.
    final_map = {}
    for row in enriched:
        key = canonical_player_name(row["player_name"])
        existing = final_map.get(key)
        if existing is None:
            final_map[key] = row
            continue

        ex_ev = int(existing.get("position_evidence_score", 0) or 0)
        new_ev = int(row.get("position_evidence_score", 0) or 0)
        ex_score = float(existing.get("consensus_score", 0.0) or 0.0)
        new_score = float(row.get("consensus_score", 0.0) or 0.0)
        ex_market = float(existing.get("market_signal_score", 0.0) or 0.0)
        new_market = float(row.get("market_signal_score", 0.0) or 0.0)

        if (new_ev, new_score, new_market) > (ex_ev, ex_score, ex_market):
            final_map[key] = row

    final_rows = list(final_map.values())
    _apply_top50_evidence_rank_brake(final_rows)
    final_rows.sort(
        key=lambda x: (
            float(x.get("rank_sort_score", x.get("consensus_score", 0.0)) or 0.0),
            float(x.get("consensus_score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    for i, row in enumerate(final_rows, start=1):
        row["consensus_rank"] = i

    contrarian_sorted = sorted(
        final_rows,
        key=lambda x: float(x.get("contrarian_score", 0.0) or 0.0),
        reverse=True,
    )
    contrarian_rank_map = {
        canonical_player_name(r["player_name"]): i for i, r in enumerate(contrarian_sorted, start=1)
    }
    for row in final_rows:
        row["contrarian_rank"] = contrarian_rank_map.get(canonical_player_name(row["player_name"]), "")

    # Hybrid round logic: grade baseline + capped rank-band uplift.
    for row in final_rows:
        rank = int(row.get("consensus_rank", 9999) or 9999)
        grade_only = round_from_grade(float(row.get("final_grade", 0.0) or 0.0))
        rank_band = _round_from_rank_band(rank)
        row["round_value_grade_only"] = grade_only
        row["round_value_rank_band"] = rank_band
        row["round_value"] = _blend_round_projection(grade_only, rank_band, rank)

    # Refresh scouting sections after final rank assignment so cards always show
    # the live model slot (not seed rank / stale prior rank).
    for row in final_rows:
        pos = normalize_pos(str(row.get("position", "")))
        pff_grade_val = _as_float(row.get("pff_grade"))
        sections = _build_scouting_sections(
            name=str(row.get("player_name", "")),
            position=pos,
            school=str(row.get("school", "")),
            final_grade=float(_as_float(row.get("final_grade")) or 0.0),
            round_value=str(row.get("round_value", "")),
            best_role=str(row.get("best_role", "")),
            best_scheme_fit=str(row.get("best_scheme_fit", "")),
            best_team_fit=str(row.get("best_team_fit", "")),
            scouting_notes=str(row.get("scouting_notes", "")),
            kiper_rank=str(row.get("kiper_rank", "")),
            kiper_prev_rank=str(row.get("kiper_prev_rank", "")),
            kiper_rank_delta=str(row.get("kiper_rank_delta", "")),
            kiper_strength_tags=str(row.get("kiper_strength_tags", "")),
            kiper_concern_tags=str(row.get("kiper_concern_tags", "")),
            kiper_statline_2025=str(row.get("kiper_statline_2025", "")),
            tdn_strengths=str(row.get("tdn_strengths", "")),
            tdn_concerns=str(row.get("tdn_concerns", "")),
            br_strengths=str(row.get("br_strengths", "")),
            br_concerns=str(row.get("br_concerns", "")),
            atoz_strengths=str(row.get("atoz_strengths", "")),
            atoz_concerns=str(row.get("atoz_concerns", "")),
            si_strengths=str(row.get("si_strengths", "")),
            si_concerns=str(row.get("si_concerns", "")),
            pff_grade=pff_grade_val,
            espn_qbr=row.get("espn_qbr", ""),
            espn_epa_per_play=row.get("espn_epa_per_play", ""),
            cfb_prod_signal=row.get("cfb_prod_signal", ""),
            cfb_prod_label=_cfb_prod_snapshot_label(pos, row),
            cfb_prod_quality=row.get("cfb_prod_quality_label", ""),
            cfb_prod_reliability=row.get("cfb_prod_reliability", ""),
            consensus_rank=int(row.get("consensus_rank", row.get("rank_seed", 9999)) or 9999),
            historical_combine_comp_1=str(row.get("historical_combine_comp_1", "")),
            historical_combine_comp_1_year=row.get("historical_combine_comp_1_year", ""),
            historical_combine_comp_1_similarity=row.get("historical_combine_comp_1_similarity", ""),
            combine_height_in=row.get("combine_height_in", ""),
            combine_weight_lb=row.get("combine_weight_lb", ""),
            combine_arm_in=row.get("combine_arm_in", ""),
            athletic_pct_forty=row.get("athletic_pct_forty", ""),
            athletic_pct_ten_split=row.get("athletic_pct_ten_split", ""),
            athletic_pct_vertical=row.get("athletic_pct_vertical", ""),
            athletic_pct_broad=row.get("athletic_pct_broad", ""),
            athletic_pct_shuttle=row.get("athletic_pct_shuttle", ""),
            athletic_pct_three_cone=row.get("athletic_pct_three_cone", ""),
            athletic_pct_weight_lb=row.get("athletic_pct_weight_lb", ""),
            athletic_pct_arm_in=row.get("athletic_pct_arm_in", ""),
            cfb_qb_epa_per_play=row.get("cfb_qb_epa_per_play", ""),
            cfb_qb_pressure_signal=row.get("cfb_qb_pressure_signal", ""),
            cfb_qb_pass_int=row.get("cfb_qb_pass_int", ""),
            cfb_wrte_yprr=row.get("cfb_wrte_yprr", ""),
            cfb_wrte_target_share=row.get("cfb_wrte_target_share", ""),
            cfb_rb_explosive_rate=row.get("cfb_rb_explosive_rate", ""),
            cfb_rb_missed_tackles_forced_per_touch=row.get("cfb_rb_missed_tackles_forced_per_touch", ""),
            cfb_edge_pressure_rate=row.get("cfb_edge_pressure_rate", ""),
            cfb_edge_sacks_per_pr_snap=row.get("cfb_edge_sacks_per_pr_snap", ""),
            cfb_edge_qb_hurries=row.get("cfb_edge_qb_hurries", ""),
            cfb_db_coverage_plays_per_target=row.get("cfb_db_coverage_plays_per_target", ""),
            cfb_db_yards_allowed_per_coverage_snap=row.get("cfb_db_yards_allowed_per_coverage_snap", ""),
            cfb_db_int=row.get("cfb_db_int", ""),
            cfb_db_pbu=row.get("cfb_db_pbu", ""),
        )
        row.update(sections)

    write_csv(PROCESSED / "big_board_2026.csv", final_rows)
    write_csv(OUTPUTS / "big_board_2026.csv", final_rows)
    write_top_board_md(OUTPUTS / "big_board_2026_top100.md", final_rows, 100)
    _write_rank_vs_consensus_outputs(final_rows)

    cfb_proxy_watchlist_rows = []
    for row in final_rows:
        if int(row.get("cfb_proxy_fallback_heavy_flag", 0) or 0) != 1:
            continue
        cfb_proxy_watchlist_rows.append(
            {
                "consensus_rank": row.get("consensus_rank", ""),
                "player_name": row.get("player_name", ""),
                "position": row.get("position", ""),
                "school": row.get("school", ""),
                "final_grade": row.get("final_grade", ""),
                "cfb_prod_quality_label": row.get("cfb_prod_quality_label", ""),
                "cfb_prod_reliability": row.get("cfb_prod_reliability", ""),
                "cfb_prod_coverage_count": row.get("cfb_prod_coverage_count", ""),
                "cfb_prod_proxy_fallback_features": row.get("cfb_prod_proxy_fallback_features", ""),
                "cfb_proxy_fallback_heavy_reason": row.get("cfb_proxy_fallback_heavy_reason", ""),
                "cfb_proxy_audit_summary": row.get("cfb_proxy_audit_summary", ""),
            }
        )
    cfb_proxy_watchlist_rows.sort(
        key=lambda x: int(_as_float(x.get("consensus_rank")) or 9999)
    )
    write_csv(OUTPUTS / "cfb_proxy_fallback_watchlist_2026.csv", cfb_proxy_watchlist_rows)
    watch_lines = [
        "CFB Proxy Fallback Watchlist (2026)",
        "",
        "Rank | Player | Pos | Rel | Cov | Fallbacks | Reason | Audit",
    ]
    for row in cfb_proxy_watchlist_rows[:200]:
        watch_lines.append(
            f"{row['consensus_rank']} | {row['player_name']} | {row['position']} | "
            f"{row['cfb_prod_reliability']} | {row['cfb_prod_coverage_count']} | "
            f"{row['cfb_prod_proxy_fallback_features']} | {row['cfb_proxy_fallback_heavy_reason']} | "
            f"{row['cfb_proxy_audit_summary']}"
        )
    (OUTPUTS / "cfb_proxy_fallback_watchlist_2026.txt").write_text("\n".join(watch_lines))
    if CFB_PROXY_FALLBACK_FAIL_ON_HEAVY and cfb_proxy_watchlist_rows:
        print(
            "CFB proxy fallback QA failed: "
            f"{len(cfb_proxy_watchlist_rows)} heavy rows. "
            f"See {OUTPUTS / 'cfb_proxy_fallback_watchlist_2026.csv'}"
        )
        raise SystemExit(2)

    watchlist_rows = []
    for row in final_rows:
        mean_rank = _as_float(row.get("consensus_board_mean_rank"))
        if mean_rank is None:
            continue
        if int(row.get("is_diamond_exception", 0) or 0) != 1:
            continue
        watchlist_rows.append(
            {
                "player_name": row["player_name"],
                "position": row["position"],
                "school": row["school"],
                "consensus_rank": row["consensus_rank"],
                "contrarian_rank": row.get("contrarian_rank", ""),
                "final_grade": row["final_grade"],
                "consensus_board_mean_rank": row["consensus_board_mean_rank"],
                "consensus_board_source_count": row["consensus_board_source_count"],
                "contrarian_score": row.get("contrarian_score", ""),
                "diamond_exception_reasons": row.get("diamond_exception_reasons", ""),
            }
        )
    watchlist_rows.sort(
        key=lambda x: float(str(x.get("contrarian_score", 0.0) or 0.0)),
        reverse=True,
    )
    write_csv(OUTPUTS / "contrarian_watchlist_2026.csv", watchlist_rows)
    watchlist_lines = [
        "2026 Contrarian Watchlist (Diamond Exceptions)",
        "",
        "Rank | Player | Pos | School | Board | Consensus Mean | Score | Reasons",
    ]
    for idx, row in enumerate(watchlist_rows[:80], start=1):
        watchlist_lines.append(
            f"{idx}. {row['player_name']} | {row['position']} | {row['school']} | "
            f"#{row['consensus_rank']} | mean {row['consensus_board_mean_rank']} | "
            f"{row['contrarian_score']} | {row['diamond_exception_reasons']}"
        )
    (OUTPUTS / "contrarian_watchlist_2026.txt").write_text("\n".join(watchlist_lines))

    extreme_rows, extreme_prev = _run_extreme_rank_delta_gate(
        current_rows=final_rows,
        snapshot_dir=OUTPUTS / "stability_snapshots",
    )
    write_csv(OUTPUTS / "extreme_rank_jump_watchlist_2026.csv", extreme_rows)
    extreme_lines = [
        "Extreme Rank Jump Watchlist (No Evidence Increase)",
        "",
        f"Previous snapshot: {extreme_prev if extreme_prev else 'none'}",
        f"Threshold rank rise: {EXTREME_RANK_DELTA_MIN_RISE}",
        f"Flagged rows: {len(extreme_rows)}",
        "",
    ]
    for row in extreme_rows[:80]:
        extreme_lines.append(
            f"{row.get('player_name','')} | {row.get('position','')} | "
            f"{row.get('prev_rank','')} -> {row.get('curr_rank','')} "
            f"(+{row.get('rank_rise','')}) | conf {row.get('prev_confidence','')}->{row.get('curr_confidence','')} "
            f"| miss {row.get('prev_evidence_missing','')}->{row.get('curr_evidence_missing','')} "
            f"| {row.get('rank_driver_summary','')}"
        )
    (OUTPUTS / "extreme_rank_jump_watchlist_2026.txt").write_text("\n".join(extreme_lines))
    if EXTREME_RANK_DELTA_FAIL_ON_TRIGGER and len(extreme_rows) > EXTREME_RANK_DELTA_MAX_ALLOWED:
        print(
            "Extreme rank jump QA failed: "
            f"{len(extreme_rows)} rows exceed no-evidence-rise gate "
            f"(max_allowed={EXTREME_RANK_DELTA_MAX_ALLOWED}). "
            f"See {OUTPUTS / 'extreme_rank_jump_watchlist_2026.csv'}"
        )
        raise SystemExit(2)

    _run_postbuild_eligibility_qa(
        final_rows=final_rows,
        returning_names=returning_names,
        declared_underclassmen=declared_underclassmen,
        already_drafted_names=already_drafted_names,
    )

    with (OUTPUTS / "big_board_2026.json").open("w") as f:
        json.dump(final_rows, f, indent=2)

    _run_locked_stability_checks(
        board_path=OUTPUTS / "big_board_2026.csv",
        watchlist_path=OUTPUTS / "contrarian_watchlist_2026.csv",
    )

    print(f"Seed rows (raw): {len(raw_seed)}")
    print(f"Removed returning players: {len(removed_returning)}")
    print(f"Removed already-drafted NFL players: {len(removed_already_drafted)}")
    print(f"Removed by class/declare eligibility: {len(removed_ineligible_class)}")
    if ENFORCE_NFL_OFFICIAL_UNIVERSE:
        print(f"NFL.com official prospect universe loaded: {len(nfl_official_universe_names)}")
        print(f"Removed outside NFL.com prospect universe: {len(removed_outside_universe)}")
    elif ENFORCE_2026_EVIDENCE_UNIVERSE:
        print(f"Removed outside 2026 evidence universe: {len(removed_outside_universe)}")
    print(f"Seed rows (expanded): {len(expanded_seed)}")
    print(f"Added from external board: {added_external}")
    print(f"Added from analyst feeds: {added_analyst}")
    print(f"Skipped returning/ineligible players: {skipped_ineligible}")
    print(f"Seed rows (deduped): {len(seed)}")
    print(f"Film charts loaded: {len(film_map)}")
    print(f"ESPN signals loaded: {len(espn_by_name_pos)}")
    print(f"PlayerProfiler signals loaded: {len(pp_by_name_pos)}")
    print(f"Analyst language signals loaded: {len(lang_by_name_pos)}")
    print(f"Kiper structured signals loaded: {len(kiper_by_name_pos)}")
    print(f"TDN/Ringer/Bleacher/AtoZ/SI/CBS/CBSWilson signals loaded: {len(tdn_ringer_by_name_pos)}")
    print(f"Consensus board signals loaded: {len(consensus_by_name)}")
    print(f"CFB production signals loaded: {len(cfb_prod_by_name_pos)}")
    sr_meta = source_reliability.get("meta", {}) if isinstance(source_reliability, dict) else {}
    print(
        "Source reliability loaded: "
        f"global={sr_meta.get('global_keys', 0)} "
        f"pos_year_rows={sr_meta.get('pos_year_rows', 0)} "
        f"has_pos_year={sr_meta.get('has_pos_year_table', 0)}"
    )
    print(
        "Historical combine profiles loaded: "
        f"{historical_combine_pack.get('meta', {}).get('rows', 0)} "
        f"(positions={historical_combine_pack.get('meta', {}).get('positions', 0)})"
    )
    print(
        "Historical athletic context loaded: "
        f"{historical_athletic_pack.get('meta', {}).get('rows', 0)} "
        f"(positions={historical_athletic_pack.get('meta', {}).get('positions', 0)})"
    )
    prod_knn_meta = production_knn_pack.get("meta", {})
    print(
        "Production percentile KNN context loaded: "
        f"{prod_knn_meta.get('rows', 0)} "
        f"(positions={prod_knn_meta.get('positions', 0)} years={prod_knn_meta.get('years_min', '')}-{prod_knn_meta.get('years_max', '')})"
    )
    cfb_meta = cfb_prod_pack.get("meta", {})
    print(
        "CFBfastr P0 signals: "
        f"matched={cfb_meta.get('cfbfastr_p0_matches', 0)} "
        f"blend_applied={cfb_meta.get('cfbfastr_p0_blend_applied', 0)} "
        f"p0_only={cfb_meta.get('cfbfastr_p0_only_applied', 0)} "
        f"w={cfb_meta.get('cfbfastr_p0_blend_weight', '')} "
        f"cap={cfb_meta.get('cfbfastr_p0_max_delta', '')} "
        f"qb_cap={cfb_meta.get('cfbfastr_p0_qb_max_delta', '')}"
    )
    print(
        "CFBD opponent-defense context: "
        f"available={cfb_meta.get('cfb_opp_def_context_available', 0)} "
        f"applied={cfb_meta.get('cfb_opp_def_context_applied', 0)} "
        f"cap={cfb_meta.get('cfb_opp_def_adj_max_delta', '')}"
    )
    print(f"CFBD years_played available: {cfb_meta.get('cfb_years_played_available', 0)}")
    da_meta = draft_age_pack.get("meta", {})
    print(
        "Draft age signals: "
        f"valid_birthdates={da_meta.get('valid_birthdates', 0)} "
        f"matched_name_pos={da_meta.get('matched_name_pos', 0)} "
        f"ref_date={da_meta.get('draft_date', '')} "
        f"scoring={'on' if ENABLE_DRAFT_AGE_SCORING else 'off'}"
    )
    ed_meta = early_declare_pack.get("meta", {})
    print(
        "Early declare signals: "
        f"official_rows={ed_meta.get('official_rows', 0)} "
        f"espn_rows={ed_meta.get('espn_underclass_rows', 0)} "
        f"combine_rows={ed_meta.get('combine_invite_rows', 0)} "
        f"matched_name_pos={ed_meta.get('matched_name_pos', 0)} "
        f"scoring={'on' if ENABLE_EARLY_DECLARE_SCORING else 'off'}"
    )
    print(
        "CFB non-position metrics ignored (audit only): "
        f"rows={cfb_meta.get('cfb_nonpos_metrics_ignored_rows', 0)} "
        f"total_values={cfb_meta.get('cfb_nonpos_metrics_ignored_total', 0)}"
    )
    proxy_heavy_count = sum(1 for r in final_rows if int(r.get("cfb_proxy_fallback_heavy_flag", 0) or 0) == 1)
    print(f"CFB proxy fallback watchlist rows: {proxy_heavy_count}")
    print(f"Rank vs consensus audit: {OUTPUTS / 'big_board_2026_rank_vs_consensus.csv'}")
    print(f"Top-100 disagreement audit: {OUTPUTS / 'top100_disagreement_audit_2026.csv'}")
    print(f"Postbuild ineligible QA: {OUTPUTS / 'postbuild_ineligible_players_qa_2026.csv'}")
    print(
        "Extreme rank-jump QA: "
        f"rows={len(extreme_rows)} "
        f"min_rise={EXTREME_RANK_DELTA_MIN_RISE} "
        f"fail_mode={int(EXTREME_RANK_DELTA_FAIL_ON_TRIGGER)}"
    )
    if calibration_cfg is None:
        print("Historical calibration: not loaded (run scripts/calibrate_historical_model.py after real outcomes import)")
    else:
        print(
            f"Historical calibration loaded: sample={calibration_cfg.sample_size} source={calibration_cfg.data_source}"
        )
    print(f"MockDraftable baselines loaded: {len(mockdraftable_baselines)}")
    print(f"RAS benchmark positions loaded: {len(ras_benchmarks)}")
    print(f"Diamond exceptions: {len(watchlist_rows)}")
    print(f"Board rows: {len(final_rows)}")


if __name__ == "__main__":
    main()
