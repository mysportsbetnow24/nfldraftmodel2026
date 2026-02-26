#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.combine_loader import load_combine_results
from src.ingest.cfb_production_loader import load_cfb_production_signals
from src.ingest.consensus_board_loader import load_consensus_board_signals
from src.ingest.eligibility_loader import load_returning_to_school
from src.ingest.eligibility_loader import load_declared_underclassmen, is_senior_class, load_already_in_nfl_exclusions
from src.ingest.espn_loader import load_espn_player_signals
from src.ingest.analyst_language_loader import load_analyst_linguistic_signals
from src.ingest.kiper_loader import load_kiper_structured_signals
from src.ingest.tdn_ringer_loader import load_tdn_ringer_signals
from src.ingest.film_traits_loader import load_film_trait_rows
from src.ingest.prebuild_validation import format_prebuild_report_md, run_prebuild_checks
from src.ingest.playerprofiler_loader import load_playerprofiler_signals
from src.ingest.mockdraftable_loader import load_mockdraftable_baselines
from src.ingest.ras_benchmarks_loader import load_ras_benchmarks
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
from src.modeling.ras import historical_ras_comparison, ras_percentile, ras_tier
from src.modeling.team_fit import best_team_fit
from src.schemas import parse_height_to_inches, round_from_grade

PROCESSED = ROOT / "data" / "processed"
OUTPUTS = ROOT / "data" / "outputs"

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
ENABLE_FILM_WEIGHTING = False
PRODUCTION_SIGNAL_NEUTRAL = float(os.getenv("PRODUCTION_SIGNAL_NEUTRAL", "70.0"))
PRODUCTION_SIGNAL_MULTIPLIER = float(os.getenv("PRODUCTION_SIGNAL_MULTIPLIER", "0.82"))
PRODUCTION_SIGNAL_MAX_DELTA = float(os.getenv("PRODUCTION_SIGNAL_MAX_DELTA", "7.0"))
PRODUCTION_SIGNAL_QB_MULTIPLIER = float(os.getenv("PRODUCTION_SIGNAL_QB_MULTIPLIER", "0.74"))
PRODUCTION_SIGNAL_QB_MAX_DELTA = float(os.getenv("PRODUCTION_SIGNAL_QB_MAX_DELTA", "6.0"))

POSITION_VALUE_ADJUSTMENT = {
    "QB": 0.35,
    "OT": 0.25,
    "EDGE": 0.25,
    "CB": 0.20,
    "WR": 0.15,
    "DT": 0.10,
    "S": 0.10,
    "LB": 0.05,
    "TE": -0.15,
    "IOL": -0.25,
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
) -> tuple[list[dict], int, int, int]:
    merged = list(seed_rows)
    returning_names = returning_names or set()
    already_drafted_names = already_drafted_names or set()
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
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
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



def _scale_waa(pff_waa: float | None) -> float:
    if pff_waa is None:
        return 55.0
    bounded = max(-0.5, min(2.0, pff_waa))
    return 50.0 + bounded * 22.0



def _official_ras_fields(position: str, combine: dict) -> tuple[dict, dict]:
    official = combine.get("ras_official")
    if official is None:
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

    score = round(max(0.0, min(10.0, float(official))), 2)
    tier = ras_tier(score)
    ras = {
        "ras_estimate": score,
        "ras_tier": tier,
        "ras_percentile": ras_percentile(score),
        "ras_source": "combine_official",
    }
    return ras, historical_ras_comparison(position, tier)


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
) -> set[str]:
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


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _guardrail_production_component(position: str, raw_component: float) -> tuple[float, float]:
    neutral = PRODUCTION_SIGNAL_NEUTRAL
    if position == "QB":
        multiplier = PRODUCTION_SIGNAL_QB_MULTIPLIER
        max_delta = PRODUCTION_SIGNAL_QB_MAX_DELTA
    else:
        multiplier = PRODUCTION_SIGNAL_MULTIPLIER
        max_delta = PRODUCTION_SIGNAL_MAX_DELTA

    delta = (float(raw_component) - neutral) * multiplier
    guarded_delta = _clamp(delta, -abs(max_delta), abs(max_delta))
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
    prior_signal: float,
    lang: dict,
    ras: dict,
    md_features: dict,
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
    trait_component = _weighted_mean(trait_parts) or (58.0 if position == "QB" else 60.0)

    prod_parts: list[tuple[float, float]] = []
    if pff_grade is not None:
        prod_parts.append((0.42, pff_grade))
    prod_parts.append((0.24, espn_prod_signal))
    if pp_player_available:
        prod_parts.append((0.12, pp_skill_signal))
    if cfb_player_available:
        cov_boost = min(1.0, max(0.5, float(cfb_prod_coverage_count) / 3.0))
        reliability = max(0.0, min(1.0, float(cfb_prod_reliability)))
        prod_parts.append((0.22 * cov_boost * reliability, cfb_prod_signal))
    production_component_raw = _weighted_mean(prod_parts) or 62.0
    production_component, production_guardrail_delta = _guardrail_production_component(
        position, production_component_raw
    )

    official_ras = _as_float(ras.get("ras_estimate"))
    md_speed_pct = _as_float(md_features.get("md_speed_pct"))
    md_explosion_pct = _as_float(md_features.get("md_explosion_pct"))
    md_agility_pct = _as_float(md_features.get("md_agility_pct"))

    # Critical: do not use size-only MockDraftable proxy as athleticism.
    # Until official combine testing arrives, keep athletic component neutral.
    if official_ras is not None:
        athletic_component = official_ras * 10.0
    else:
        athletic_parts: list[tuple[float, float]] = []
        if md_speed_pct is not None:
            athletic_parts.append((0.45, md_speed_pct))
        if md_explosion_pct is not None:
            athletic_parts.append((0.35, md_explosion_pct))
        if md_agility_pct is not None:
            athletic_parts.append((0.20, md_agility_pct))
        athletic_component = _weighted_mean(athletic_parts)
        if athletic_component is None:
            athletic_component = 68.0 if position == "QB" else 70.0

    size_component = float(grades.get("size_score", 75.0) or 75.0)
    context_component = _class_context_score(class_year)
    if film_enabled and _as_float(grades.get("film_trait_coverage")) and float(grades.get("film_trait_coverage", 0.0) or 0.0) >= 0.75:
        context_component += 1.2
    if _as_float(lang.get("lang_text_coverage")) and float(lang.get("lang_text_coverage", 0.0) or 0.0) >= 120:
        context_component += 0.8

    has_testing_signal = (
        official_ras is not None
        or md_speed_pct is not None
        or md_explosion_pct is not None
        or md_agility_pct is not None
    )

    film_coverage = _as_float(grades.get("film_trait_coverage")) or 0.0
    lang_text_coverage = _as_float(lang.get("lang_text_coverage")) or 0.0
    has_film_signal = film_enabled and film_trait is not None and film_coverage >= 0.45
    has_language_signal = language_trait is not None and lang_text_coverage >= 40.0
    has_market_signal = external_rank is not None
    missing_signal_count = sum(
        0 if present else 1 for present in (has_film_signal, has_language_signal, has_testing_signal, has_market_signal)
    )

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
    # Data-sufficiency penalty: sparse profiles should not sit near the top on neutral defaults.
    if position == "QB":
        risk_penalty += 0.55 * missing_signal_count
        if missing_signal_count >= 3:
            risk_penalty += 0.8
        if not (has_film_signal or has_language_signal or has_testing_signal or has_market_signal):
            risk_penalty += 1.2
    else:
        risk_penalty += 0.22 * missing_signal_count
        if missing_signal_count >= 3:
            risk_penalty += 0.35
    if int(lang.get("lang_risk_flag", 0) or 0) == 1:
        risk_penalty += 0.6
    if position == "QB" and analyst_score < 40:
        risk_penalty += 0.4
    if missing_signal_count >= 3:
        context_component -= 1.0 if position == "QB" else 0.5

    raw_formula_score = (
        0.38 * trait_component
        + 0.24 * production_component
        + 0.18 * athletic_component
        + 0.10 * size_component
        + 0.10 * context_component
        - risk_penalty
    )
    # Calibrate to scouting-grade scale so round-value mapping is realistic.
    calibrated_grade = (1.22 * raw_formula_score) - 2.0
    calibrated_grade = max(55.0, min(95.0, calibrated_grade))
    prior_grade = max(55.0, min(95.0, 60.0 + 0.33 * prior_signal))
    final_grade = (0.72 * calibrated_grade) + (0.28 * prior_grade)
    final_grade += float(POSITION_VALUE_ADJUSTMENT.get(position, 0.0))
    final_grade = max(55.0, min(95.0, final_grade))

    floor = max(52.0, final_grade - (1.8 + risk_penalty))
    ceiling = min(97.0, final_grade + (2.0 if class_year.upper() in {"SO", "RSO", "JR", "RSJ"} else 1.3))

    return {
        "formula_trait_component": round(trait_component, 2),
        "formula_production_component_raw": round(production_component_raw, 2),
        "formula_production_component": round(production_component, 2),
        "formula_production_guardrail_delta": round(production_guardrail_delta, 2),
        "formula_athletic_component": round(athletic_component, 2),
        "formula_size_component": round(size_component, 2),
        "formula_context_component": round(context_component, 2),
        "formula_risk_penalty": round(risk_penalty, 2),
        "formula_raw_score": round(raw_formula_score, 2),
        "formula_calibrated_grade": round(calibrated_grade, 2),
        "formula_prior_signal": round(prior_signal, 2),
        "formula_prior_grade": round(prior_grade, 2),
        "formula_score": round(final_grade, 2),
        "formula_floor": round(floor, 2),
        "formula_ceiling": round(ceiling, 2),
        "formula_round_value": round_from_grade(final_grade),
        "weight_production_multiplier": round(
            PRODUCTION_SIGNAL_QB_MULTIPLIER if position == "QB" else PRODUCTION_SIGNAL_MULTIPLIER, 3
        ),
        "weight_production_max_delta": round(
            PRODUCTION_SIGNAL_QB_MAX_DELTA if position == "QB" else PRODUCTION_SIGNAL_MAX_DELTA, 2
        ),
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


def _cfb_prod_snapshot_label(position: str, cfb: dict) -> str:
    pos = normalize_pos(position)
    if pos == "QB":
        parts = []
        if str(cfb.get("cfb_qb_epa_per_play", "")).strip():
            parts.append(f"QB EPA/play {cfb.get('cfb_qb_epa_per_play')}")
        if str(cfb.get("cfb_qb_pressure_signal", "")).strip():
            parts.append(f"QB pressure signal {cfb.get('cfb_qb_pressure_signal')}")
        return "; ".join(parts)
    if pos in {"WR", "TE"}:
        parts = []
        if str(cfb.get("cfb_wrte_yprr", "")).strip():
            parts.append(f"YPRR {cfb.get('cfb_wrte_yprr')}")
        if str(cfb.get("cfb_wrte_target_share", "")).strip():
            parts.append(f"target share {cfb.get('cfb_wrte_target_share')}")
        return "; ".join(parts)
    if pos == "RB":
        parts = []
        if str(cfb.get("cfb_rb_explosive_rate", "")).strip():
            parts.append(f"explosive run rate {cfb.get('cfb_rb_explosive_rate')}")
        if str(cfb.get("cfb_rb_missed_tackles_forced_per_touch", "")).strip():
            parts.append(f"MTF/touch {cfb.get('cfb_rb_missed_tackles_forced_per_touch')}")
        return "; ".join(parts)
    if pos == "EDGE":
        val = cfb.get("cfb_edge_pressure_rate", "")
        return f"pressure rate {val}" if str(val).strip() else ""
    if pos in {"CB", "S"}:
        val = cfb.get("cfb_db_coverage_plays_per_target", "")
        return f"coverage plays/target {val}" if str(val).strip() else ""
    return ""


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
) -> dict:
    strength_stack = [
        _compact_text(kiper_strength_tags, 90),
        _compact_text(tdn_strengths, 90),
        _compact_text(br_strengths, 90),
        _compact_text(atoz_strengths, 90),
        _compact_text(si_strengths, 90),
    ]
    concern_stack = [
        _compact_text(kiper_concern_tags, 90),
        _compact_text(tdn_concerns, 90),
        _compact_text(br_concerns, 90),
        _compact_text(atoz_concerns, 90),
        _compact_text(si_concerns, 90),
    ]
    strengths = [s for s in strength_stack if s]
    concerns = [c for c in concern_stack if c]

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

    production_parts = []
    if kiper_statline_2025:
        production_parts.append(kiper_statline_2025)
    if pff_grade is not None:
        production_parts.append(f"PFF grade {pff_grade:.1f}")
    if str(espn_qbr or "").strip():
        production_parts.append(f"ESPN QBR {espn_qbr}")
    if str(espn_epa_per_play or "").strip():
        production_parts.append(f"ESPN EPA/play {espn_epa_per_play}")
    if str(cfb_prod_signal or "").strip():
        production_parts.append(f"CFB prod signal {cfb_prod_signal}")
    if str(cfb_prod_label or "").strip():
        production_parts.append(str(cfb_prod_label))
    if str(cfb_prod_quality or "").strip():
        production_parts.append(f"CFB quality {cfb_prod_quality} ({cfb_prod_reliability})")
    production_snapshot = "; ".join(production_parts) if production_parts else "Production snapshot pending structured statline import."

    report = (
        f"{name} ({position}, {school}) holds a model grade of {final_grade:.2f} with a {round_value} projection. "
        f"Current board slot: {consensus_rank}. {scouting_notes}"
    )
    wins = (
        f"Role fit: {best_role}. Preferred usage in {best_scheme_fit}. "
        + (f"Traits repeatedly flagged: {', '.join(strengths[:3])}." if strengths else "Traits: leverage, processing, and role translation remain primary wins.")
    )
    primary_concerns = (
        f"Development checkpoints: {', '.join(concerns[:3])}."
        if concerns
        else "Development checkpoints: consistency, role expansion pace, and matchup stress handling."
    )
    projection = (
        f"Best early team fit: {best_team_fit}. Scheme path: {best_scheme_fit}. "
        f"Expected early deployment: {best_role}."
    )

    return {
        "scouting_report_summary": report,
        "scouting_why_he_wins": wins,
        "scouting_primary_concerns": primary_concerns,
        "scouting_production_snapshot": production_snapshot,
        "scouting_board_movement": move_text,
        "scouting_role_projection": projection,
    }


def main() -> None:
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
    analyst_rows = load_analyst_rows()
    analyst_pos_votes = _build_analyst_pos_votes(analyst_rows)
    external_board_rows = load_external_big_board_rows()
    allowed_universe_names = _build_allowed_universe_names(
        external_rows=external_board_rows,
        analyst_rows=analyst_rows,
        declared_underclassmen=declared_underclassmen,
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
        if ENFORCE_2026_EVIDENCE_UNIVERSE and name_key not in allowed_universe_names:
            removed_outside_universe.append(row["player_name"])
            continue
        raw_seed.append(row)

    if ENABLE_SOURCE_UNIVERSE_EXPANSION:
        expanded_seed, added_external, added_analyst, skipped_ineligible = augment_seed_with_external_and_analyst(
            seed_rows=raw_seed,
            external_rows=external_board_rows,
            analyst_rows=analyst_rows,
            returning_names=returning_names,
            already_drafted_names=already_drafted_names,
        )
    else:
        expanded_seed = list(raw_seed)
        added_external = 0
        added_analyst = 0
        skipped_ineligible = 0

    seed = dedupe_seed_rows(expanded_seed)

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
    calibration_cfg = load_calibration_config()
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
        pp_player_available = bool(str(pp.get("pp_data_coverage", "")).strip())
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
        br_text_trait_signal = float(tdn_ringer.get("br_text_trait_signal", 0.0) or 0.0)
        br_risk_penalty = float(tdn_ringer.get("br_risk_penalty", 0.0) or 0.0)
        atoz_text_trait_signal = float(tdn_ringer.get("atoz_text_trait_signal", 0.0) or 0.0)
        atoz_risk_penalty = float(tdn_ringer.get("atoz_risk_penalty", 0.0) or 0.0)
        si_text_trait_signal = float(tdn_ringer.get("si_text_trait_signal", 0.0) or 0.0)
        si_risk_penalty = float(tdn_ringer.get("si_risk_penalty", 0.0) or 0.0)
        consensus_signal = float(consensus.get("consensus_signal", 0.0) or 0.0)
        consensus_mean_rank = consensus.get("consensus_mean_rank", "")
        consensus_rank_std = consensus.get("consensus_rank_std", "")
        consensus_source_count = consensus.get("consensus_source_count", "")
        consensus_sources = consensus.get("consensus_sources", "")
        consensus_mean_rank_val = _as_float(consensus_mean_rank)
        consensus_rank_std_val = _as_float(consensus_rank_std)
        consensus_source_count_val = int(_as_float(consensus_source_count) or 0)

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

        fit_team, fit_score = best_team_fit(pos)
        comp = assign_comp(pos, row["rank_seed"])
        ras, ras_comps = _official_ras_fields(pos, combine)
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
        language_coverage_val = _as_float(lang.get("lang_text_coverage")) or 0.0

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
        # TDN rank 0.08, Ringer rank 0.08, Bleacher rank 0.09, AtoZ rank 0.08, SI/FCS rank 0.03, TDN grade label 0.04.
        prior_parts = [(0.20, max(1.0, min(100.0, seed_signal / 3.0)))]
        if external_rank is not None:
            prior_parts.append((0.24, max(1.0, min(100.0, external_rank_signal / 3.0))))
        if analyst_score > 0:
            prior_parts.append((0.14, max(1.0, min(100.0, analyst_score))))
        if consensus_signal > 0:
            prior_parts.append((0.24, max(1.0, min(100.0, consensus_signal))))
        if kiper_rank_signal > 0:
            prior_parts.append((0.08, max(1.0, min(100.0, kiper_rank_signal))))
        if tdn_rank_signal > 0:
            prior_parts.append((0.08, max(1.0, min(100.0, tdn_rank_signal))))
        if ringer_rank_signal > 0:
            prior_parts.append((0.08, max(1.0, min(100.0, ringer_rank_signal))))
        if br_rank_signal > 0:
            prior_parts.append((0.09, max(1.0, min(100.0, br_rank_signal))))
        if atoz_rank_signal > 0:
            prior_parts.append((0.08, max(1.0, min(100.0, atoz_rank_signal))))
        if si_rank_signal > 0:
            prior_parts.append((0.03, max(1.0, min(100.0, si_rank_signal))))
        if tdn_grade_label_signal > 0:
            prior_parts.append((0.04, max(1.0, min(100.0, tdn_grade_label_signal))))
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
            prior_signal=prior_signal,
            lang=lang,
            ras=ras,
            md_features=md_features,
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
        )

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
            "pp_risk_flag": int(pp_risk_flag) if pp_player_available else "",
            "pp_profile_tier": pp.get("pp_profile_tier", ""),
            "pp_notes": pp.get("pp_notes", ""),
            "cfb_prod_signal": round(cfb_prod_signal, 2) if cfb_player_available else "",
            "cfb_prod_signal_raw": cfb.get("cfb_prod_signal_raw", ""),
            "cfb_prod_available": 1 if cfb_player_available else 0,
            "cfb_prod_coverage_count": cfb_prod_coverage_count,
            "cfb_prod_quality_label": cfb.get("cfb_prod_quality_label", ""),
            "cfb_prod_reliability": cfb.get("cfb_prod_reliability", ""),
            "cfb_prod_real_features": cfb.get("cfb_prod_real_features", ""),
            "cfb_prod_proxy_features": cfb.get("cfb_prod_proxy_features", ""),
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
            "cfb_rb_explosive_signal": cfb.get("cfb_rb_explosive_signal", ""),
            "cfb_rb_mtf_signal": cfb.get("cfb_rb_mtf_signal", ""),
            "cfb_edge_pressure_signal": cfb.get("cfb_edge_pressure_signal", ""),
            "cfb_db_cov_plays_per_target_signal": cfb.get("cfb_db_cov_plays_per_target_signal", ""),
            "cfb_qb_epa_per_play": cfb.get("cfb_qb_epa_per_play", ""),
            "cfb_wrte_yprr": cfb.get("cfb_wrte_yprr", ""),
            "cfb_wrte_target_share": cfb.get("cfb_wrte_target_share", ""),
            "cfb_rb_explosive_rate": cfb.get("cfb_rb_explosive_rate", ""),
            "cfb_rb_missed_tackles_forced_per_touch": cfb.get("cfb_rb_missed_tackles_forced_per_touch", ""),
            "cfb_edge_pressure_rate": cfb.get("cfb_edge_pressure_rate", ""),
            "cfb_db_coverage_plays_per_target": cfb.get("cfb_db_coverage_plays_per_target", ""),
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
            "weight_prior_tdn_grade_label": 0.04,
            "weight_trait_tdn_text": 0.05,
            "weight_trait_bleacher_text": 0.04,
            "weight_trait_atoz_text": 0.04,
            "weight_trait_si_text": 0.02,
            "formula_guardrail_penalty": round(guardrail_penalty, 2),
            "formula_drift_penalty": round(drift_penalty, 2),
            "formula_consensus_confidence_factor": round(consensus_confidence_factor, 3),
            "formula_midband_brake_penalty": round(midband_brake_penalty, 2),
            "formula_soft_ceiling_target": round(soft_ceiling_target, 2) if soft_ceiling_target is not None else "",
            "formula_soft_ceiling_penalty": round(soft_ceiling_penalty, 2),
            "formula_top75_gate_penalty": round(top75_gate_penalty, 2),
            "formula_hard_cap": round(hard_cap, 2) if hard_cap is not None else "",
            "formula_consensus_outlier_cap": round(outlier_cap, 2) if outlier_cap is not None else "",
            "formula_hard_cap_penalty": round(cap_penalty, 2),
            "is_diamond_exception": 1 if is_diamond_exception else 0,
            "diamond_exception_reasons": diamond_exception_reasons,
            "contrarian_score": round(contrarian_score, 2),
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
                    - guardrail_penalty
                    - (0.7 * soft_ceiling_penalty)
                    - (0.8 * cap_penalty),
                ),
                2,
            ),
            "ceiling_grade": round(
                max(
                    55.0,
                    float(formula["formula_ceiling"])
                    + calibration_grade_adjustment
                    - (0.5 * guardrail_penalty)
                    - (0.5 * soft_ceiling_penalty)
                    - (0.5 * cap_penalty),
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
            "scouting_notes": scout_note,
            **scouting_sections,
            "headshot_url": "",
        }
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
    final_rows.sort(key=lambda x: x["consensus_score"], reverse=True)
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

    write_csv(PROCESSED / "big_board_2026.csv", final_rows)
    write_csv(OUTPUTS / "big_board_2026.csv", final_rows)
    write_top_board_md(OUTPUTS / "big_board_2026_top100.md", final_rows, 100)

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

    with (OUTPUTS / "big_board_2026.json").open("w") as f:
        json.dump(final_rows, f, indent=2)

    print(f"Seed rows (raw): {len(raw_seed)}")
    print(f"Removed returning players: {len(removed_returning)}")
    print(f"Removed already-drafted NFL players: {len(removed_already_drafted)}")
    print(f"Removed by class/declare eligibility: {len(removed_ineligible_class)}")
    if ENFORCE_2026_EVIDENCE_UNIVERSE:
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
    print(f"TDN/Ringer/Bleacher/AtoZ/SI signals loaded: {len(tdn_ringer_by_name_pos)}")
    print(f"Consensus board signals loaded: {len(consensus_by_name)}")
    print(f"CFB production signals loaded: {len(cfb_prod_by_name_pos)}")
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
