from __future__ import annotations

import csv
import os
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
MANUAL_DIR = ROOT / "data" / "sources" / "manual"
PROCESSED_DIR = ROOT / "data" / "processed"

DEFAULT_PATH_CANDIDATES = [
    MANUAL_DIR / "cfb_production_2025.csv",
    PROCESSED_DIR / "cfb_production_features_2025.csv",
]
SG_ADVANCED_PATH = MANUAL_DIR / "scoutinggrade_advanced_2025.csv"

TARGET_POSITIONS = {"QB", "WR", "TE", "RB", "EDGE", "DT", "LB", "CB", "S", "OT", "IOL"}
POSITION_FAMILY_MAP = {
    "CB": "DB",
    "S": "DB",
    "EDGE": "DL",
    "DT": "DL",
}
P0_BLEND_WEIGHT = float(os.getenv("CFBFASTR_P0_BLEND_WEIGHT", "0.35"))
P0_MAX_DELTA = float(os.getenv("CFBFASTR_P0_MAX_DELTA", "4.0"))
P0_QB_MAX_DELTA = float(os.getenv("CFBFASTR_P0_QB_MAX_DELTA", "3.0"))
P0_SOLO_MULTIPLIER = float(os.getenv("CFBFASTR_P0_SOLO_MULTIPLIER", "0.65"))
CFB_PERCENTILE_BLEND_WEIGHT = float(os.getenv("CFB_PERCENTILE_BLEND_WEIGHT", "0.45"))
CFB_OPP_DEF_ADJ_MAX_DELTA = float(os.getenv("CFB_OPP_DEF_ADJ_MAX_DELTA", "2.4"))
WRTE_TPR_DIRECT_WEIGHT = float(os.getenv("WRTE_TPR_DIRECT_WEIGHT", "0.20"))
WRTE_TPR_DERIVED_WEIGHT = float(os.getenv("WRTE_TPR_DERIVED_WEIGHT", "0.10"))
EDGE_SACK_DIRECT_WEIGHT = float(os.getenv("EDGE_SACK_DIRECT_WEIGHT", "0.28"))
EDGE_SACK_DERIVED_WEIGHT = float(os.getenv("EDGE_SACK_DERIVED_WEIGHT", "0.16"))
EDGE_SACK_PRESSURE_ONLY_WEIGHT = float(os.getenv("EDGE_SACK_PRESSURE_ONLY_WEIGHT", "0.08"))
DB_YACS_DIRECT_WEIGHT = float(os.getenv("DB_YACS_DIRECT_WEIGHT", "0.45"))
DB_YACS_DERIVED_WEIGHT = float(os.getenv("DB_YACS_DERIVED_WEIGHT", "0.20"))

POSITION_SCOPE_KEYS = {
    "QB": {
        "qb_qbr",
        "qb_epa_per_play",
        "qb_epa_per_pl",
        "qb_success_rate",
        "qb_pressure_to_sack_rate",
        "qb_under_pressure_epa",
        "qb_under_pressure_success_rate",
        "qb_ppa_overall",
        "qb_ppa_passing",
        "qb_ppa_standard_downs",
        "qb_ppa_passing_downs",
        "qb_wepa_passing",
        "qb_usage_rate",
        "qb_adjusted_passing",
        "qb_adjusted_rushing",
        "qb_adjusted_total",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "opp_def_ppa_allowed_avg",
        "opp_def_success_rate_allowed_avg",
        "opp_def_toughness_index",
        "opp_def_adjustment_multiplier",
        "sg_qb_pass_grade",
        "sg_qb_btt_rate",
        "sg_qb_twp_rate",
        "sg_qb_pressure_to_sack_rate",
        "sg_qb_pressure_grade",
        "sg_qb_blitz_grade",
        "sg_qb_no_screen_grade",
        "sg_qb_quick_qb_rating",
    },
    "WR": {
        "yprr",
        "target_share",
        "targets_per_route_run",
        "wrte_ppa_overall",
        "wrte_ppa_passing_downs",
        "wrte_wepa_receiving",
        "wrte_usage_rate",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "opp_def_ppa_allowed_avg",
        "opp_def_success_rate_allowed_avg",
        "opp_def_toughness_index",
        "opp_def_adjustment_multiplier",
        "sg_wrte_route_grade",
        "sg_wrte_yprr",
        "sg_wrte_targets_per_route",
        "sg_wrte_man_yprr",
        "sg_wrte_zone_yprr",
        "sg_wrte_contested_catch_rate",
        "sg_wrte_drop_rate",
    },
    "TE": {
        "yprr",
        "target_share",
        "targets_per_route_run",
        "wrte_ppa_overall",
        "wrte_ppa_passing_downs",
        "wrte_wepa_receiving",
        "wrte_usage_rate",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "opp_def_ppa_allowed_avg",
        "opp_def_success_rate_allowed_avg",
        "opp_def_toughness_index",
        "opp_def_adjustment_multiplier",
        "sg_rb_run_grade",
        "sg_rb_elusive_rating",
        "sg_rb_yco_attempt",
        "sg_rb_explosive_rate",
        "sg_rb_breakaway_percent",
        "sg_rb_targets_per_route",
        "sg_rb_yprr",
    },
    "RB": {
        "explosive_run_rate",
        "missed_tackles_forced_per_touch",
        "rb_ppa_rushing",
        "rb_ppa_standard_downs",
        "rb_wepa_rushing",
        "rb_usage_rate",
        "rb_adjusted_rushing",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "opp_def_ppa_allowed_avg",
        "opp_def_success_rate_allowed_avg",
        "opp_def_toughness_index",
        "opp_def_adjustment_multiplier",
    },
    "EDGE": {
        "pressure_rate",
        "pressures_per_pass_rush_snap",
        "sacks_per_pass_rush_snap",
        "edge_pressures",
        "edge_sacks",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "sg_dl_pass_rush_grade",
        "sg_dl_pass_rush_win_rate",
        "sg_dl_prp",
        "sg_dl_true_pass_set_win_rate",
        "sg_dl_true_pass_set_prp",
        "sg_front_run_def_grade",
        "sg_front_stop_percent",
    },
    "DT": {
        "pressure_rate",
        "pressures_per_pass_rush_snap",
        "sacks_per_pass_rush_snap",
        "edge_pressures",
        "edge_sacks",
        "edge_tfl",
        "edge_tackles",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "sg_dl_pass_rush_grade",
        "sg_dl_pass_rush_win_rate",
        "sg_dl_prp",
        "sg_dl_true_pass_set_win_rate",
        "sg_dl_true_pass_set_prp",
        "sg_front_run_def_grade",
        "sg_front_stop_percent",
    },
    "LB": {
        "lb_usage_rate",
        "defense_snap_rate",
        "defensive_snap_rate",
        "lb_def_snaps",
        "defensive_snaps",
        "edge_tackles",
        "edge_tfl",
        "edge_sacks",
        "edge_qb_hurries",
        "db_tackles",
        "db_tfl",
        "db_int",
        "db_pbu",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "sg_def_run_grade",
        "sg_def_coverage_grade",
        "sg_def_tackle_grade",
        "sg_def_missed_tackle_rate",
        "sg_front_stop_percent",
        "sg_cov_yards_per_snap",
        "sg_cov_qb_rating_against",
    },
    "CB": {
        "coverage_plays_per_target",
        "yards_allowed_per_coverage_snap",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "sg_cov_grade",
        "sg_cov_forced_incompletion_rate",
        "sg_cov_snaps_per_target",
        "sg_cov_yards_per_snap",
        "sg_cov_qb_rating_against",
        "sg_cov_man_grade",
        "sg_cov_zone_grade",
        "sg_slot_cov_yards_per_snap",
    },
    "S": {
        "coverage_plays_per_target",
        "yards_allowed_per_coverage_snap",
        "game_consistency_index",
        "late_season_trend_index",
        "top_defense_performance_index",
        "sg_cov_grade",
        "sg_cov_forced_incompletion_rate",
        "sg_cov_snaps_per_target",
        "sg_cov_yards_per_snap",
        "sg_cov_qb_rating_against",
        "sg_cov_man_grade",
        "sg_cov_zone_grade",
        "sg_slot_cov_yards_per_snap",
    },
    "OT": {
        "years_played",
        "ol_usage_rate",
        "offense_snap_rate",
        "offensive_snap_rate",
        "ol_starts",
        "career_starts",
        "starts",
        "sg_ol_pass_block_grade",
        "sg_ol_run_block_grade",
        "sg_ol_pbe",
        "sg_ol_pressure_allowed_rate",
        "sg_ol_versatility_count",
    },
    "IOL": {
        "years_played",
        "ol_usage_rate",
        "offense_snap_rate",
        "offensive_snap_rate",
        "ol_starts",
        "career_starts",
        "starts",
        "sg_ol_pass_block_grade",
        "sg_ol_run_block_grade",
        "sg_ol_pbe",
        "sg_ol_pressure_allowed_rate",
        "sg_ol_versatility_count",
    },
}
ALL_SCOPE_KEYS = sorted({k for keys in POSITION_SCOPE_KEYS.values() for k in keys})


def _safe_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _first_float(row: dict, keys: Iterable[str]) -> float | None:
    lowered = {str(k).strip().lower(): v for k, v in row.items()}
    for key in keys:
        if key in lowered:
            value = _safe_float(lowered[key])
            if value is not None:
                return value
    return None


def _score_linear(value: float | None, low: float, high: float) -> float | None:
    if value is None:
        return None
    if high <= low:
        return None
    clipped = max(low, min(high, float(value)))
    pct = (clipped - low) / (high - low)
    return round(20.0 + (75.0 * pct), 2)


def _score_inverse(value: float | None, low: float, high: float) -> float | None:
    # Lower is better (e.g., pressure-to-sack rate, yards allowed per coverage snap).
    if value is None:
        return None
    lo = min(float(low), float(high))
    hi = max(float(low), float(high))
    if hi <= lo:
        return None
    clipped = max(lo, min(hi, float(value)))
    pct = (hi - clipped) / (hi - lo)
    return round(20.0 + (75.0 * pct), 2)


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
    return round(num / den, 2)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _conference_key(row: dict) -> str:
    for key in ("conference", "team_conference", "school_conference", "conf", "team_conf"):
        val = str(row.get(key, "")).strip().upper()
        if val:
            return val
    return ""


def _score_percentile(value: float | None, population: list[float]) -> float | None:
    if value is None or not population:
        return None
    vals = sorted(float(v) for v in population)
    if not vals:
        return None
    le = sum(1 for v in vals if v <= float(value))
    percentile = le / float(len(vals))
    return round(20.0 + (75.0 * percentile), 2)


def _position_scope_diagnostics(position: str, row: dict) -> dict:
    """
    Audit non-position metric leakage in manual CSV rows.
    We ignore these values in scoring; this just makes it visible.
    """
    allowed = POSITION_SCOPE_KEYS.get(position, set())
    ignored = []
    for key in ALL_SCOPE_KEYS:
        if key in allowed:
            continue
        val = _safe_float(row.get(key))
        if val is not None:
            ignored.append(key)
    return {
        "cfb_nonpos_metrics_ignored_count": len(ignored),
        "cfb_nonpos_metrics_ignored_fields": "|".join(sorted(ignored)) if ignored else "",
    }


def _legacy_position_signal(
    position: str,
    qb_eff_sig: float | None,
    qb_pressure_sig: float | None,
    wrte_sig: float | None,
    rb_sig: float | None,
    edge_sig: float | None,
    lb_sig: float | None,
    db_sig: float | None,
    ol_proxy_sig: float | None,
) -> float | None:
    if position == "QB":
        return _weighted_mean(
            [(0.62, qb_eff_sig), (0.38, qb_pressure_sig)] if qb_eff_sig is not None and qb_pressure_sig is not None
            else [(1.0, qb_eff_sig)] if qb_eff_sig is not None
            else [(1.0, qb_pressure_sig)] if qb_pressure_sig is not None
            else []
        )
    if position in {"WR", "TE"}:
        return wrte_sig
    if position == "RB":
        return rb_sig
    if position == "EDGE":
        return edge_sig
    if position == "DT":
        return edge_sig
    if position == "LB":
        return lb_sig
    if position in {"CB", "S"}:
        return db_sig
    if position in {"OT", "IOL"}:
        return ol_proxy_sig
    return None


def _usage_rate(position: str, row: dict) -> float | None:
    if position == "QB":
        return _first_float(row, ["qb_usage_rate", "qb_usage", "usage_rate"])
    if position in {"WR", "TE"}:
        return _first_float(row, ["wrte_usage_rate", "wr_usage_rate", "te_usage_rate", "target_share", "targets_share"])
    if position == "RB":
        return _first_float(row, ["rb_usage_rate", "rb_usage", "usage_rate"])
    if position == "EDGE":
        return _first_float(row, ["edge_usage_rate", "pass_rush_snap_rate", "usage_rate"])
    if position == "DT":
        return _first_float(row, ["dt_usage_rate", "pass_rush_snap_rate", "usage_rate"])
    if position == "LB":
        return _first_float(
            row,
            [
                "lb_usage_rate",
                "defense_snap_rate",
                "defensive_snap_rate",
                "usage_rate",
            ],
        )
    if position in {"CB", "S"}:
        return _first_float(row, ["db_usage_rate", "coverage_snap_rate", "usage_rate"])
    if position in {"OT", "IOL"}:
        return _first_float(
            row,
            [
                "ol_usage_rate",
                "offense_snap_rate",
                "offensive_snap_rate",
                "usage_rate",
            ],
        )
    return None


def _usage_context_multiplier(position: str, usage: float | None) -> float:
    if usage is None:
        return 0.90
    if position == "QB":
        floor, target, min_mult = 0.10, 0.20, 0.78
    elif position in {"WR", "TE"}:
        floor, target, min_mult = 0.10, 0.28, 0.74
    elif position == "RB":
        floor, target, min_mult = 0.08, 0.30, 0.74
    elif position == "EDGE":
        floor, target, min_mult = 0.12, 0.45, 0.80
    elif position == "DT":
        floor, target, min_mult = 0.12, 0.42, 0.80
    elif position == "LB":
        floor, target, min_mult = 0.28, 0.82, 0.82
    elif position in {"CB", "S"}:
        floor, target, min_mult = 0.25, 0.75, 0.82
    elif position in {"OT", "IOL"}:
        floor, target, min_mult = 0.42, 0.90, 0.86
    else:
        floor, target, min_mult = 0.10, 0.30, 0.84
    scaled = _clamp((float(usage) - floor) / max(0.01, (target - floor)), 0.0, 1.0)
    return round(min_mult + ((1.0 - min_mult) * scaled), 3)


def _opponent_defense_adjustment(position: str, row: dict) -> dict:
    """
    Opponent-defense schedule context (offense positions only).
    Higher toughness index => modest positive production adjustment.
    Missing data stays neutral (0 delta) by design.
    """
    payload = {
        "cfb_opp_def_ppa_allowed_avg": "",
        "cfb_opp_def_success_rate_allowed_avg": "",
        "cfb_opp_def_toughness_index": "",
        "cfb_opp_def_adjustment_multiplier": "",
        "cfb_opp_def_adjustment_delta": "",
        "cfb_opp_def_context_applied": 0,
        "cfb_opp_def_context_source": "",
    }

    if position not in {"QB", "WR", "TE", "RB"}:
        return payload

    opp_ppa = _first_float(
        row,
        [
            "opp_def_ppa_allowed_avg",
            "opponent_def_ppa_allowed_avg",
            "opp_def_ppa",
        ],
    )
    opp_sr = _first_float(
        row,
        [
            "opp_def_success_rate_allowed_avg",
            "opponent_def_success_rate_allowed_avg",
            "opp_def_success_rate",
        ],
    )
    tough_idx = _first_float(
        row,
        [
            "opp_def_toughness_index",
            "opponent_def_toughness_index",
        ],
    )
    adj_mult = _first_float(
        row,
        [
            "opp_def_adjustment_multiplier",
            "opponent_def_adjustment_multiplier",
        ],
    )
    source = str(row.get("opp_def_context_source", "")).strip()

    if tough_idx is None:
        # Build toughness index from raw defensive allowance if explicit index is missing.
        ppa_tough = _clamp((0.46 - float(opp_ppa)) / 0.26, 0.0, 1.0) if opp_ppa is not None else None
        sr_tough = _clamp((0.53 - float(opp_sr)) / 0.19, 0.0, 1.0) if opp_sr is not None else None
        parts = []
        if ppa_tough is not None:
            parts.append((0.55, ppa_tough))
        if sr_tough is not None:
            parts.append((0.45, sr_tough))
        if parts:
            den = sum(w for w, _ in parts)
            tough_idx = _clamp(sum(w * v for w, v in parts) / den, 0.0, 1.0) if den > 0 else None
        elif adj_mult is not None:
            tough_idx = _clamp(((float(adj_mult) - 1.0) / 0.20) + 0.5, 0.0, 1.0)

    if tough_idx is None:
        return payload

    delta = (float(tough_idx) - 0.5) * (2.0 * CFB_OPP_DEF_ADJ_MAX_DELTA)
    delta = _clamp(delta, -abs(CFB_OPP_DEF_ADJ_MAX_DELTA), abs(CFB_OPP_DEF_ADJ_MAX_DELTA))
    if adj_mult is None:
        adj_mult = _clamp(1.0 + ((float(tough_idx) - 0.5) * 0.20), 0.90, 1.10)

    payload.update(
        {
            "cfb_opp_def_ppa_allowed_avg": round(opp_ppa, 4) if opp_ppa is not None else "",
            "cfb_opp_def_success_rate_allowed_avg": round(opp_sr, 4) if opp_sr is not None else "",
            "cfb_opp_def_toughness_index": round(float(tough_idx), 4),
            "cfb_opp_def_adjustment_multiplier": round(float(adj_mult), 4),
            "cfb_opp_def_adjustment_delta": round(float(delta), 2),
            "cfb_opp_def_context_applied": 1 if abs(float(delta)) > 0.01 else 0,
            "cfb_opp_def_context_source": source,
        }
    )
    return payload


def _apply_p0_guardrail(position: str, legacy_signal: float | None, p0_signal: float | None) -> tuple[float | None, dict]:
    if p0_signal is None:
        return legacy_signal, {
            "cfbfastr_p0_available": 0,
            "cfbfastr_p0_signal_raw": "",
            "cfbfastr_p0_mode": "",
            "cfbfastr_p0_applied_delta": "",
            "cfbfastr_p0_max_delta": "",
        }

    max_delta = P0_QB_MAX_DELTA if position == "QB" else P0_MAX_DELTA
    p0_signal = float(p0_signal)

    if legacy_signal is None:
        neutral = 55.0
        delta = (p0_signal - neutral) * P0_SOLO_MULTIPLIER
        guarded_delta = _clamp(delta, -abs(max_delta), abs(max_delta))
        guarded = neutral + guarded_delta
        return round(guarded, 2), {
            "cfbfastr_p0_available": 1,
            "cfbfastr_p0_signal_raw": round(p0_signal, 2),
            "cfbfastr_p0_mode": "p0_only_guarded",
            "cfbfastr_p0_applied_delta": round(guarded_delta, 2),
            "cfbfastr_p0_max_delta": round(max_delta, 2),
        }

    legacy_signal = float(legacy_signal)
    blended = ((1.0 - P0_BLEND_WEIGHT) * legacy_signal) + (P0_BLEND_WEIGHT * p0_signal)
    delta = blended - legacy_signal
    guarded_delta = _clamp(delta, -abs(max_delta), abs(max_delta))
    guarded = legacy_signal + guarded_delta
    return round(guarded, 2), {
        "cfbfastr_p0_available": 1,
        "cfbfastr_p0_signal_raw": round(p0_signal, 2),
        "cfbfastr_p0_mode": "legacy_plus_p0_guarded",
        "cfbfastr_p0_applied_delta": round(guarded_delta, 2),
        "cfbfastr_p0_max_delta": round(max_delta, 2),
    }


def _quality_and_reliability(row: dict, coverage_count: int, source: str) -> tuple[str, float]:
    quality = str(row.get("cfb_prod_quality_label", "")).strip().lower()
    if quality not in {"real", "mixed", "proxy"}:
        source_l = source.lower()
        if "proxy" in source_l:
            quality = "proxy"
        elif coverage_count >= 2:
            quality = "mixed"
        else:
            quality = "proxy"

    rel = _safe_float(row.get("cfb_prod_reliability"))
    if rel is None:
        if quality == "real":
            rel = 1.0
        elif quality == "mixed":
            rel = 0.75
        else:
            rel = min(0.65, 0.38 + (0.05 * coverage_count))
    # Proxy-only rows with very thin coverage should stay conservative so a single metric
    # cannot dominate board movement.
    rel = max(0.0, min(1.0, float(rel)))
    if quality == "proxy":
        if coverage_count <= 1:
            rel = min(rel, 0.30)
        elif coverage_count == 2:
            rel = min(rel, 0.42)
        else:
            rel = min(rel, 0.55)
    return quality, round(rel, 2)


def _qb_eff_signal(row: dict) -> tuple[float | None, float | None]:
    qbr = _first_float(row, ["qb_qbr", "qbr", "espn_qbr"])
    epa = _first_float(row, ["qb_epa_per_play", "qb_epa_per_pl", "epa_per_play", "qb_efficiency"])
    success = _first_float(row, ["qb_success_rate", "success_rate"])
    qb_pass_int = _first_float(row, ["qb_pass_int", "cfb_qb_pass_int"])
    qb_pass_att = _first_float(row, ["qb_pass_att", "cfb_qb_pass_att"])

    parts = []
    qbr_sig = _score_linear(qbr, 45.0, 90.0)
    if qbr_sig is not None:
        parts.append((0.38, qbr_sig))
    epa_sig = _score_linear(epa, -0.20, 0.45)
    if epa_sig is not None:
        parts.append((0.30, epa_sig))
    succ_sig = _score_linear(success, 0.35, 0.60)
    if succ_sig is not None:
        parts.append((0.17, succ_sig))
    int_sig = None
    if qb_pass_int is not None and qb_pass_att is not None and qb_pass_att >= 120:
        # Lower interception rate should score better for QB efficiency context.
        int_rate = qb_pass_int / max(qb_pass_att, 1.0)
        int_sig = _score_inverse(int_rate, 0.045, 0.010)
    elif qb_pass_int is not None:
        # Fallback when attempts are missing: still apply a bounded turnover penalty.
        int_sig = _score_inverse(qb_pass_int, 14.0, 3.0)
    if int_sig is not None:
        parts.append((0.15, int_sig))
    return _weighted_mean(parts), epa


def _qb_pressure_signal(row: dict) -> float | None:
    pressure_to_sack = _first_float(
        row,
        [
            "qb_pressure_to_sack_rate",
            "pressure_to_sack_rate",
            "p2s_rate",
        ],
    )
    under_pressure_epa = _first_float(row, ["qb_under_pressure_epa", "under_pressure_epa"])
    pressure_success = _first_float(row, ["qb_under_pressure_success_rate", "under_pressure_success_rate"])

    parts = []
    p2s_sig = _score_inverse(pressure_to_sack, 0.28, 0.10)
    if p2s_sig is not None:
        parts.append((0.45, p2s_sig))
    up_epa_sig = _score_linear(under_pressure_epa, -0.45, 0.20)
    if up_epa_sig is not None:
        parts.append((0.35, up_epa_sig))
    up_succ_sig = _score_linear(pressure_success, 0.25, 0.50)
    if up_succ_sig is not None:
        parts.append((0.20, up_succ_sig))
    return _weighted_mean(parts)


def _wrte_signal(
    row: dict,
) -> tuple[float | None, float | None, float | None, float | None, dict]:
    yprr = _first_float(row, ["yprr", "yards_per_route_run"])
    target_share = _first_float(row, ["target_share", "targets_share"])
    targets_per_route = _first_float(
        row,
        [
            "targets_per_route_run",
            "targets_per_route",
            "wrte_targets_per_route",
            "target_per_route",
        ],
    )
    tpr_source = "direct"
    tpr_weight = WRTE_TPR_DIRECT_WEIGHT
    tpr_fallback_used = 0
    if targets_per_route is None:
        route_share = _first_float(
            row,
            [
                "wrte_usage_rate",
                "wr_usage_rate",
                "te_usage_rate",
                "route_participation",
                "route_share",
            ],
        )
        if target_share is not None and route_share is not None and route_share >= 0.45:
            targets_per_route = _clamp(target_share / route_share, 0.08, 0.38)
            tpr_source = "derived_target_share_route_share"
            tpr_fallback_used = 1
        elif target_share is not None:
            # Conservative fallback proxy when route participation is unavailable or low-quality.
            targets_per_route = _clamp(0.07 + (0.85 * target_share), 0.08, 0.34)
            tpr_source = "derived_target_share_only"
            tpr_fallback_used = 1
    if targets_per_route is None:
        tpr_source = "missing"
    if tpr_fallback_used == 1:
        tpr_weight = WRTE_TPR_DERIVED_WEIGHT

    yprr_sig = _score_linear(yprr, 1.0, 3.3)
    ts_sig = _score_linear(target_share, 0.12, 0.35)
    tpr_sig = _score_linear(targets_per_route, 0.10, 0.34)
    parts = []
    if yprr_sig is not None:
        parts.append((0.55, yprr_sig))
    if ts_sig is not None:
        parts.append((0.25, ts_sig))
    if tpr_sig is not None:
        parts.append((tpr_weight, tpr_sig))

    diag = {
        "wrte_available_count": int(yprr is not None) + int(target_share is not None) + int(targets_per_route is not None),
        "wrte_fallback_count": tpr_fallback_used,
        "wrte_targets_per_route_source": tpr_source,
        "wrte_targets_per_route_weight": round(float(tpr_weight), 3),
    }
    return _weighted_mean(parts), yprr, target_share, targets_per_route, diag


def _rb_signal(row: dict) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None, dict]:
    explosive_rate = _first_float(row, ["explosive_run_rate", "explosive_rate"])
    mtf = _first_float(
        row,
        [
            "missed_tackles_forced_per_touch",
            "missed_tackles_forced_per_attempt",
            "mtf_per_touch",
        ],
    )
    rb_yac_per_att = _first_float(
        row,
        [
            "rb_yards_after_contact_per_attempt",
            "rb_yac_per_att",
            "yards_after_contact_per_attempt",
            "yac_per_attempt",
        ],
    )
    rb_target_share = _first_float(row, ["rb_target_share", "target_share_rb", "target_share"])
    rb_target_share_source = "direct"
    if rb_target_share is None:
        rb_rec = _first_float(row, ["rb_rec", "cfb_rb_rec"])
        rb_rush_att = _first_float(row, ["rb_rush_att", "cfb_rb_rush_att"])
        if rb_rec is not None and rb_rush_att is not None and (rb_rec + rb_rush_att) > 0:
            # Conservative usage proxy when true target share is unavailable.
            rb_target_share = _clamp(rb_rec / (rb_rec + rb_rush_att), 0.03, 0.22)
            rb_target_share_source = "derived_rec_share_of_touches"
        else:
            rb_target_share_source = "missing"
    rb_rec_yds = _first_float(row, ["rb_rec_yds", "cfb_rb_rec_yds"])
    rb_rec = _first_float(row, ["rb_rec", "cfb_rb_rec"])
    rb_receiving_eff = None
    if rb_rec_yds is not None and rb_rec is not None and rb_rec > 0:
        rb_receiving_eff = rb_rec_yds / rb_rec

    explosive_sig = _score_linear(explosive_rate, 0.06, 0.22)
    mtf_sig = _score_linear(mtf, 0.08, 0.35)
    yac_sig = _score_linear(rb_yac_per_att, 2.0, 4.2)
    target_share_sig = _score_linear(rb_target_share, 0.05, 0.16)
    receiving_eff_sig = _score_linear(rb_receiving_eff, 5.5, 11.5)
    parts = []
    if explosive_sig is not None:
        parts.append((0.38, explosive_sig))
    if mtf_sig is not None:
        parts.append((0.27, mtf_sig))
    if yac_sig is not None:
        parts.append((0.18, yac_sig))
    if target_share_sig is not None:
        parts.append((0.09, target_share_sig))
    if receiving_eff_sig is not None:
        parts.append((0.08, receiving_eff_sig))
    diag = {
        "rb_available_count": int(explosive_rate is not None)
        + int(mtf is not None)
        + int(rb_yac_per_att is not None)
        + int(rb_target_share is not None)
        + int(rb_receiving_eff is not None),
        "rb_target_share_source": rb_target_share_source,
    }
    return _weighted_mean(parts), explosive_rate, mtf, rb_yac_per_att, rb_target_share, rb_receiving_eff, diag


def _lb_signal(row: dict) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None, float | None, dict]:
    lb_snaps = _first_float(
        row,
        [
            "lb_def_snaps",
            "defensive_snaps",
            "def_snaps",
            "snap_count",
        ],
    )
    lb_usage = _first_float(
        row,
        [
            "lb_usage_rate",
            "defensive_snap_rate",
            "defense_snap_rate",
            "usage_rate",
        ],
    )
    tackles = _first_float(row, ["lb_tackles", "db_tackles", "edge_tackles"])
    tfl = _first_float(row, ["lb_tfl", "db_tfl", "edge_tfl"])
    sacks = _first_float(row, ["lb_sacks", "edge_sacks", "sacks"])
    hurries = _first_float(row, ["lb_qb_hurries", "edge_qb_hurries", "qb_hurries"])
    pbu = _first_float(row, ["lb_pbu", "db_pbu", "pbu"])
    ints = _first_float(row, ["lb_int", "db_int", "interceptions"])

    tackle_rate = None
    tfl_rate = None
    rush_impact_rate = None
    if lb_snaps is not None and lb_snaps >= 120:
        tackle_rate = tackles / lb_snaps if tackles is not None else None
        tfl_rate = tfl / lb_snaps if tfl is not None else None
        rush_impact_rate = ((sacks or 0.0) + (hurries or 0.0)) / lb_snaps

    tackle_sig = _score_linear(tackle_rate, 0.055, 0.145) if tackle_rate is not None else _score_linear(tackles, 34.0, 112.0)
    tfl_sig = _score_linear(tfl_rate, 0.006, 0.028) if tfl_rate is not None else _score_linear(tfl, 3.0, 16.0)
    rush_sig = _score_linear(rush_impact_rate, 0.015, 0.085) if rush_impact_rate is not None else _score_linear(((sacks or 0.0) + (hurries or 0.0)), 6.0, 28.0)
    ball_sig = _score_linear(((ints or 0.0) + (pbu or 0.0)), 1.0, 8.0)

    parts = []
    if tackle_sig is not None:
        parts.append((0.34, tackle_sig))
    if tfl_sig is not None:
        parts.append((0.24, tfl_sig))
    if rush_sig is not None:
        parts.append((0.24, rush_sig))
    if ball_sig is not None:
        parts.append((0.18, ball_sig))

    diag = {
        "lb_available_count": int(tackles is not None)
        + int(tfl is not None)
        + int((sacks is not None) or (hurries is not None))
        + int((ints is not None) or (pbu is not None))
        + int(lb_snaps is not None)
        + int(lb_usage is not None),
        "lb_rate_source": "snap_normalized" if lb_snaps is not None and lb_snaps >= 120 else "counting_fallback",
    }
    return _weighted_mean(parts), tackles, tfl, sacks, hurries, lb_usage, lb_snaps, diag


def _ol_proxy_signal(row: dict) -> tuple[float | None, float | None, float | None, float | None, dict]:
    years_played = _first_float(row, ["years_played", "cfb_years_played"])
    starts = _first_float(row, ["ol_starts", "career_starts", "starts", "season_starts"])
    usage = _first_float(row, ["ol_usage_rate", "offensive_snap_rate", "offense_snap_rate", "usage_rate"])

    years_sig = _score_linear(years_played, 1.0, 5.0)
    starts_sig = _score_linear(starts, 6.0, 46.0)
    usage_sig = _score_linear(usage, 0.42, 0.92)

    parts = []
    if years_sig is not None:
        parts.append((0.46, years_sig))
    if starts_sig is not None:
        parts.append((0.34, starts_sig))
    if usage_sig is not None:
        parts.append((0.20, usage_sig))

    diag = {
        "ol_available_count": int(years_played is not None) + int(starts is not None) + int(usage is not None),
        "ol_proxy_quality_label": "proxy_supported" if len(parts) >= 2 else "proxy_thin" if len(parts) == 1 else "missing",
    }
    return _weighted_mean(parts), years_played, starts, usage, diag


def _edge_signal(row: dict) -> tuple[float | None, float | None, float | None, dict]:
    pressure_rate = _first_float(
        row,
        [
            "pressures_per_pass_rush_snap",
            "pressures_per_pr_snap",
            "edge_pressures_per_pr_snap",
            "pressure_rate",
            "pass_rush_pressure_rate",
        ],
    )
    sacks_per_pr_snap = _first_float(
        row,
        [
            "sacks_per_pass_rush_snap",
            "sacks_per_pr_snap",
            "edge_sacks_per_pr_snap",
        ],
    )
    sack_source = "direct"
    sack_fallback_used = 0
    sack_weight = EDGE_SACK_DIRECT_WEIGHT
    if sacks_per_pr_snap is None:
        sacks = _first_float(row, ["edge_sacks", "sacks", "defensive_sacks"])
        pressures = _first_float(row, ["edge_pressures", "pressures", "pass_rush_pressures"])
        if pressure_rate is not None and sacks is not None and pressures is not None and pressures > 0:
            finish = _clamp(sacks / pressures, 0.08, 0.40)
            sacks_per_pr_snap = _clamp(pressure_rate * finish, 0.010, 0.075)
            sack_source = "derived_sacks_pressures_finish"
            sack_fallback_used = 1
            sack_weight = EDGE_SACK_DERIVED_WEIGHT
        elif pressure_rate is not None:
            # Conservative fallback conversion when only pressure rate exists.
            sacks_per_pr_snap = _clamp(pressure_rate * 0.20, 0.010, 0.060)
            sack_source = "derived_pressure_only_conversion"
            sack_fallback_used = 1
            sack_weight = EDGE_SACK_PRESSURE_ONLY_WEIGHT
    if sacks_per_pr_snap is None:
        sack_source = "missing"

    pressure_sig = _score_linear(pressure_rate, 0.10, 0.17)
    sack_sig = _score_linear(sacks_per_pr_snap, 0.02, 0.055)
    parts = []
    pressure_weight = 1.0
    if pressure_sig is not None and sack_sig is not None:
        pressure_weight = max(0.0, 1.0 - float(sack_weight))
    if pressure_sig is not None:
        parts.append((pressure_weight, pressure_sig))
    if sack_sig is not None:
        parts.append((sack_weight, sack_sig))

    diag = {
        "edge_available_count": int(pressure_rate is not None) + int(sacks_per_pr_snap is not None),
        "edge_fallback_count": sack_fallback_used,
        "edge_sacks_per_pr_snap_source": sack_source,
        "edge_pressure_weight": round(float(pressure_weight), 3),
        "edge_sack_weight": round(float(sack_weight), 3) if sack_sig is not None else "",
    }
    return _weighted_mean(parts), pressure_rate, sacks_per_pr_snap, diag


def _db_signal(row: dict) -> tuple[float | None, float | None, float | None, dict]:
    cov_plays_per_target = _first_float(
        row,
        ["coverage_plays_per_target", "pbu_int_per_target", "coverage_impact_per_target"],
    )
    yards_allowed_per_cov_snap = _first_float(
        row,
        [
            "yards_allowed_per_coverage_snap",
            "db_yards_allowed_per_coverage_snap",
            "yards_allowed_per_cov_snap",
        ],
    )
    yacs_source = "direct"
    yacs_fallback_used = 0
    yacs_weight = DB_YACS_DIRECT_WEIGHT
    if yards_allowed_per_cov_snap is None and cov_plays_per_target is not None and cov_plays_per_target > 0:
        # coverage_plays_per_target here is a small-rate proxy metric (not literal plays/target),
        # so map it directly to a conservative yards-allowed-per-coverage-snap range.
        norm = _clamp((float(cov_plays_per_target) - 0.08) / 0.22, 0.0, 1.0)
        yards_allowed_per_cov_snap = _clamp(1.85 - (1.10 * norm), 0.55, 1.85)
        yacs_source = "derived_from_cov_plays_per_target"
        yacs_fallback_used = 1
        yacs_weight = DB_YACS_DERIVED_WEIGHT
    if yards_allowed_per_cov_snap is None:
        yacs_source = "missing"

    cov_sig = _score_linear(cov_plays_per_target, 0.08, 0.30)
    yacs_sig = _score_inverse(yards_allowed_per_cov_snap, 1.80, 0.55)
    parts = []
    cov_weight = 1.0
    if cov_sig is not None and yacs_sig is not None:
        cov_weight = max(0.0, 1.0 - float(yacs_weight))
    if cov_sig is not None:
        parts.append((cov_weight, cov_sig))
    if yacs_sig is not None:
        parts.append((yacs_weight, yacs_sig))

    diag = {
        "db_available_count": int(cov_plays_per_target is not None) + int(yards_allowed_per_cov_snap is not None),
        "db_fallback_count": yacs_fallback_used,
        "db_yards_allowed_per_cov_snap_source": yacs_source,
        "db_cov_weight": round(float(cov_weight), 3),
        "db_yacs_weight": round(float(yacs_weight), 3) if yacs_sig is not None else "",
    }
    return _weighted_mean(parts), cov_plays_per_target, yards_allowed_per_cov_snap, diag


SG_ADV_BLEND_WEIGHTS = {
    "QB": 0.58,
    "RB": 0.55,
    "WR": 0.58,
    "TE": 0.58,
    "EDGE": 0.60,
    "DT": 0.60,
    "LB": 0.56,
    "CB": 0.60,
    "S": 0.60,
    "OT": 0.72,
    "IOL": 0.72,
}


def _sg_qb_signal(row: dict) -> tuple[float | None, int]:
    parts = []
    grade = _score_linear(_first_float(row, ["sg_qb_pass_grade"]), 55.0, 92.0)
    if grade is not None:
        parts.append((0.28, grade))
    btt = _score_linear(_first_float(row, ["sg_qb_btt_rate"]), 1.0, 8.0)
    if btt is not None:
        parts.append((0.12, btt))
    twp = _score_inverse(_first_float(row, ["sg_qb_twp_rate"]), 0.06, 0.01)
    if twp is not None:
        parts.append((0.16, twp))
    p2s = _score_inverse(
        _first_float(row, ["sg_qb_pressure_to_sack_rate", "sg_qb_pressure_pressure_to_sack_rate"]),
        0.30,
        0.10,
    )
    if p2s is not None:
        parts.append((0.14, p2s))
    pressure_grade = _score_linear(_first_float(row, ["sg_qb_pressure_grade"]), 40.0, 90.0)
    if pressure_grade is not None:
        parts.append((0.12, pressure_grade))
    blitz_grade = _score_linear(_first_float(row, ["sg_qb_blitz_grade"]), 45.0, 90.0)
    if blitz_grade is not None:
        parts.append((0.08, blitz_grade))
    ns_grade = _score_linear(_first_float(row, ["sg_qb_no_screen_grade"]), 50.0, 90.0)
    if ns_grade is not None:
        parts.append((0.05, ns_grade))
    quick = _score_linear(_first_float(row, ["sg_qb_quick_qb_rating"]), 60.0, 145.0)
    if quick is not None:
        parts.append((0.05, quick))
    return _weighted_mean(parts), len(parts)


def _sg_wrte_signal(row: dict) -> tuple[float | None, int]:
    parts = []
    route_grade = _score_linear(_first_float(row, ["sg_wrte_route_grade"]), 55.0, 92.0)
    if route_grade is not None:
        parts.append((0.28, route_grade))
    yprr = _score_linear(_first_float(row, ["sg_wrte_yprr"]), 0.8, 3.2)
    if yprr is not None:
        parts.append((0.20, yprr))
    tpr = _score_linear(_first_float(row, ["sg_wrte_targets_per_route"]), 0.10, 0.34)
    if tpr is not None:
        parts.append((0.18, tpr))
    man = _score_linear(_first_float(row, ["sg_wrte_man_yprr"]), 0.6, 3.0)
    if man is not None:
        parts.append((0.12, man))
    zone = _score_linear(_first_float(row, ["sg_wrte_zone_yprr"]), 0.6, 3.0)
    if zone is not None:
        parts.append((0.08, zone))
    contested = _score_linear(_first_float(row, ["sg_wrte_contested_catch_rate"]), 0.25, 0.70)
    if contested is not None:
        parts.append((0.06, contested))
    drop = _score_inverse(_first_float(row, ["sg_wrte_drop_rate"]), 0.12, 0.02)
    if drop is not None:
        parts.append((0.08, drop))
    return _weighted_mean(parts), len(parts)


def _sg_rb_signal(row: dict) -> tuple[float | None, int]:
    parts = []
    run_grade = _score_linear(_first_float(row, ["sg_rb_run_grade"]), 55.0, 92.0)
    if run_grade is not None:
        parts.append((0.18, run_grade))
    elusive = _score_linear(_first_float(row, ["sg_rb_elusive_rating"]), 20.0, 130.0)
    if elusive is not None:
        parts.append((0.22, elusive))
    yco = _score_linear(_first_float(row, ["sg_rb_yco_attempt"]), 1.5, 5.0)
    if yco is not None:
        parts.append((0.18, yco))
    explosive = _score_linear(_first_float(row, ["sg_rb_explosive_rate"]), 5.0, 40.0)
    if explosive is not None:
        parts.append((0.16, explosive))
    breakaway = _score_linear(_first_float(row, ["sg_rb_breakaway_percent"]), 10.0, 55.0)
    if breakaway is not None:
        parts.append((0.14, breakaway))
    receiving = _score_linear(_first_float(row, ["sg_rb_targets_per_route"]), 0.03, 0.22)
    if receiving is not None:
        parts.append((0.12, receiving))
    return _weighted_mean(parts), len(parts)


def _sg_dl_signal(row: dict) -> tuple[float | None, int]:
    parts = []
    rush_grade = _score_linear(_first_float(row, ["sg_dl_pass_rush_grade"]), 55.0, 92.0)
    if rush_grade is not None:
        parts.append((0.24, rush_grade))
    win = _score_linear(
        _first_float(row, ["sg_dl_true_pass_set_win_rate", "sg_dl_pass_rush_win_rate"]),
        5.0,
        28.0,
    )
    if win is not None:
        parts.append((0.24, win))
    prp = _score_linear(_first_float(row, ["sg_dl_true_pass_set_prp", "sg_dl_prp"]), 4.0, 15.0)
    if prp is not None:
        parts.append((0.20, prp))
    pressures = _score_linear(_first_float(row, ["sg_dl_true_pass_set_total_pressures", "sg_dl_total_pressures"]), 5.0, 70.0)
    if pressures is not None:
        parts.append((0.12, pressures))
    run_grade = _score_linear(_first_float(row, ["sg_front_run_def_grade"]), 40.0, 85.0)
    if run_grade is not None:
        parts.append((0.12, run_grade))
    stops = _score_linear(_first_float(row, ["sg_front_stop_percent"]), 3.0, 12.0)
    if stops is not None:
        parts.append((0.08, stops))
    return _weighted_mean(parts), len(parts)


def _sg_lb_signal(row: dict) -> tuple[float | None, int]:
    parts = []
    run_grade = _score_linear(_first_float(row, ["sg_def_run_grade"]), 45.0, 90.0)
    if run_grade is not None:
        parts.append((0.18, run_grade))
    cov_grade = _score_linear(_first_float(row, ["sg_def_coverage_grade"]), 40.0, 90.0)
    if cov_grade is not None:
        parts.append((0.18, cov_grade))
    tackle_grade = _score_linear(_first_float(row, ["sg_def_tackle_grade"]), 45.0, 90.0)
    if tackle_grade is not None:
        parts.append((0.10, tackle_grade))
    missed = _score_inverse(_first_float(row, ["sg_def_missed_tackle_rate"]), 0.25, 0.05)
    if missed is not None:
        parts.append((0.10, missed))
    stop = _score_linear(_first_float(row, ["sg_front_stop_percent"]), 3.0, 12.0)
    if stop is not None:
        parts.append((0.12, stop))
    ypcs = _score_inverse(_first_float(row, ["sg_cov_yards_per_snap"]), 2.0, 0.4)
    if ypcs is not None:
        parts.append((0.08, ypcs))
    qbr = _score_inverse(_first_float(row, ["sg_cov_qb_rating_against"]), 130.0, 40.0)
    if qbr is not None:
        parts.append((0.08, qbr))
    pressure_utility = _score_linear(_first_float(row, ["sg_def_total_pressures"]), 2.0, 28.0)
    if pressure_utility is not None:
        parts.append((0.08, pressure_utility))
    box_disruption = _score_linear(_first_float(row, ["sg_def_tackles_for_loss"]), 2.0, 14.0)
    if box_disruption is not None:
        parts.append((0.05, box_disruption))
    slot_deterrence = _score_linear(_first_float(row, ["sg_slot_cov_snaps_per_target"]), 2.0, 14.0)
    if slot_deterrence is not None:
        parts.append((0.03, slot_deterrence))
    return _weighted_mean(parts), len(parts)


def _sg_db_signal(row: dict) -> tuple[float | None, int]:
    parts = []
    cov_grade = _score_linear(_first_float(row, ["sg_cov_grade"]), 45.0, 92.0)
    if cov_grade is not None:
        parts.append((0.24, cov_grade))
    fi = _score_linear(_first_float(row, ["sg_cov_forced_incompletion_rate"]), 0.05, 0.30)
    if fi is not None:
        parts.append((0.14, fi))
    spt = _score_linear(_first_float(row, ["sg_cov_snaps_per_target"]), 2.0, 18.0)
    if spt is not None:
        parts.append((0.14, spt))
    ypcs = _score_inverse(_first_float(row, ["sg_cov_yards_per_snap"]), 2.0, 0.30)
    if ypcs is not None:
        parts.append((0.18, ypcs))
    qbr = _score_inverse(_first_float(row, ["sg_cov_qb_rating_against"]), 140.0, 35.0)
    if qbr is not None:
        parts.append((0.12, qbr))
    man = _score_linear(_first_float(row, ["sg_cov_man_grade"]), 40.0, 90.0)
    if man is not None:
        parts.append((0.06, man))
    zone = _score_linear(_first_float(row, ["sg_cov_zone_grade"]), 40.0, 90.0)
    if zone is not None:
        parts.append((0.06, zone))
    pressure_utility = _score_linear(_first_float(row, ["sg_def_total_pressures"]), 1.0, 12.0)
    if pressure_utility is not None:
        parts.append((0.04, pressure_utility))
    box_disruption = _score_linear(_first_float(row, ["sg_def_tackles_for_loss"]), 0.0, 8.0)
    if box_disruption is not None:
        parts.append((0.04, box_disruption))
    slot_tax = _score_linear(_first_float(row, ["sg_slot_cov_snaps_per_target"]), 2.0, 14.0)
    if slot_tax is not None:
        parts.append((0.06, slot_tax))
    slot_qbr = _score_inverse(_first_float(row, ["sg_slot_cov_qb_rating_against"]), 140.0, 35.0)
    if slot_qbr is not None:
        parts.append((0.06, slot_qbr))
    return _weighted_mean(parts), len(parts)


def _sg_ol_signal(row: dict) -> tuple[float | None, int]:
    parts = []
    pass_grade = _score_linear(_first_float(row, ["sg_ol_pass_block_grade"]), 50.0, 90.0)
    if pass_grade is not None:
        parts.append((0.34, pass_grade))
    run_grade = _score_linear(_first_float(row, ["sg_ol_run_block_grade"]), 45.0, 90.0)
    if run_grade is not None:
        parts.append((0.18, run_grade))
    pbe = _score_linear(_first_float(row, ["sg_ol_pbe"]), 94.0, 99.8)
    if pbe is not None:
        parts.append((0.28, pbe))
    pressure_rate = _score_inverse(_first_float(row, ["sg_ol_pressure_allowed_rate"]), 0.08, 0.0)
    if pressure_rate is not None:
        parts.append((0.14, pressure_rate))
    versatility = _score_linear(_first_float(row, ["sg_ol_versatility_count"]), 1.0, 4.0)
    if versatility is not None:
        parts.append((0.06, versatility))
    return _weighted_mean(parts), len(parts)


def _qb_p0_signal(row: dict) -> tuple[float | None, int]:
    ppa_overall = _first_float(row, ["qb_ppa_overall", "ppa_overall", "qb_ppa_all"])
    ppa_pass = _first_float(row, ["qb_ppa_passing", "ppa_passing", "qb_ppa_pass"])
    ppa_pd = _first_float(row, ["qb_ppa_passing_downs", "ppa_passing_downs", "qb_ppa_pd"])
    ppa_sd = _first_float(row, ["qb_ppa_standard_downs", "ppa_standard_downs", "qb_ppa_sd"])
    wepa_pass = _first_float(row, ["qb_wepa_passing", "wepa_passing", "pass_wepa"])
    adj_pass = _first_float(row, ["qb_adjusted_passing", "qb_adj_passing", "adjusted_passing"])
    adj_rush = _first_float(row, ["qb_adjusted_rushing", "qb_adj_rushing", "adjusted_rushing"])
    adj_total = _first_float(row, ["qb_adjusted_total", "qb_adj_total", "adjusted_total"])
    usage = _first_float(row, ["qb_usage_rate", "qb_usage", "usage_rate"])

    parts = []
    s_adj_total = _score_linear(adj_total, -0.15, 0.45)
    if s_adj_total is not None:
        parts.append((0.18, s_adj_total))
    s_adj_pass = _score_linear(adj_pass, -0.15, 0.45)
    if s_adj_pass is not None:
        parts.append((0.16, s_adj_pass))
    s_adj_rush = _score_linear(adj_rush, -0.10, 0.35)
    if s_adj_rush is not None:
        parts.append((0.06, s_adj_rush))
    s_wepa = _score_linear(wepa_pass, -0.2, 0.6)
    if s_wepa is not None:
        parts.append((0.24, s_wepa))
    s_pass = _score_linear(ppa_pass, -0.2, 0.65)
    if s_pass is not None:
        parts.append((0.16, s_pass))
    s_overall = _score_linear(ppa_overall, -0.15, 0.55)
    if s_overall is not None:
        parts.append((0.10, s_overall))
    s_pd = _score_linear(ppa_pd, -0.25, 0.45)
    if s_pd is not None:
        parts.append((0.06, s_pd))
    s_sd = _score_linear(ppa_sd, -0.2, 0.45)
    if s_sd is not None:
        parts.append((0.02, s_sd))
    s_usage = _score_linear(usage, 0.08, 0.24)
    if s_usage is not None:
        parts.append((0.02, s_usage))
    score = _weighted_mean(parts)
    return score, len(parts)


def _wrte_p0_signal(row: dict) -> tuple[float | None, int]:
    wepa_recv = _first_float(
        row,
        ["wrte_wepa_receiving", "wepa_receiving", "wr_wepa_receiving", "te_wepa_receiving"],
    )
    ppa_overall = _first_float(
        row,
        ["wrte_ppa_overall", "wr_ppa_overall", "te_ppa_overall", "ppa_overall"],
    )
    ppa_pd = _first_float(
        row,
        ["wrte_ppa_passing_downs", "wr_ppa_passing_downs", "te_ppa_passing_downs"],
    )
    usage = _first_float(row, ["wrte_usage_rate", "wr_usage_rate", "te_usage_rate", "usage_rate"])

    parts = []
    s_wepa = _score_linear(wepa_recv, -0.2, 0.55)
    if s_wepa is not None:
        parts.append((0.45, s_wepa))
    s_ppa = _score_linear(ppa_overall, -0.15, 0.50)
    if s_ppa is not None:
        parts.append((0.30, s_ppa))
    s_pd = _score_linear(ppa_pd, -0.25, 0.45)
    if s_pd is not None:
        parts.append((0.15, s_pd))
    s_usage = _score_linear(usage, 0.12, 0.38)
    if s_usage is not None:
        parts.append((0.10, s_usage))
    score = _weighted_mean(parts)
    return score, len(parts)


def _rb_p0_signal(row: dict) -> tuple[float | None, int]:
    wepa_rush = _first_float(row, ["rb_wepa_rushing", "wepa_rushing", "rush_wepa"])
    ppa_rush = _first_float(row, ["rb_ppa_rushing", "ppa_rushing", "rb_ppa"])
    ppa_sd = _first_float(row, ["rb_ppa_standard_downs", "ppa_standard_downs", "rb_ppa_sd"])
    adj_rush = _first_float(row, ["rb_adjusted_rushing", "rb_adj_rushing", "adjusted_rushing"])
    usage = _first_float(row, ["rb_usage_rate", "rb_usage", "usage_rate"])

    parts = []
    s_adj = _score_linear(adj_rush, -0.1, 0.35)
    if s_adj is not None:
        parts.append((0.22, s_adj))
    s_wepa = _score_linear(wepa_rush, -0.2, 0.45)
    if s_wepa is not None:
        parts.append((0.34, s_wepa))
    s_ppa = _score_linear(ppa_rush, -0.25, 0.5)
    if s_ppa is not None:
        parts.append((0.27, s_ppa))
    s_sd = _score_linear(ppa_sd, -0.2, 0.4)
    if s_sd is not None:
        parts.append((0.09, s_sd))
    s_usage = _score_linear(usage, 0.10, 0.42)
    if s_usage is not None:
        parts.append((0.08, s_usage))
    score = _weighted_mean(parts)
    return score, len(parts)


def _game_context_signal(row: dict) -> tuple[float | None, int, dict]:
    consistency = _first_float(row, ["game_consistency_index", "weekly_consistency_index"])
    trend = _first_float(row, ["late_season_trend_index", "late_trend_index"])
    top_def = _first_float(row, ["top_defense_performance_index", "top_def_index"])
    parts = []
    s_consistency = _score_linear(consistency, 0.20, 0.95)
    if s_consistency is not None:
        parts.append((0.45, s_consistency))
    s_trend = _score_linear(trend, -0.40, 0.40)
    if s_trend is not None:
        parts.append((0.20, s_trend))
    s_top_def = _score_linear(top_def, 0.55, 1.25)
    if s_top_def is not None:
        parts.append((0.35, s_top_def))
    return _weighted_mean(parts), len(parts), {
        "game_context_source": str(row.get("game_context_source", "")).strip(),
        "game_context_top_def_games": int(_first_float(row, ["top_defense_games"]) or 0),
        "game_context_weekly_sample_games": int(_first_float(row, ["weekly_sample_games"]) or 0),
    }


def _load_rows(path: Path | None) -> list[dict]:
    if path is None or not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _merge_scoutinggrade_advanced_rows(rows: list[dict], target_season: int) -> list[dict]:
    if not SG_ADVANCED_PATH.exists():
        return rows

    merged: dict[tuple[str, str], dict] = {}
    family_keys_by_name: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for row in rows:
        name = canonical_player_name(str(row.get("player_name", "")).strip())
        pos = normalize_pos(str(row.get("position", "")).strip())
        season = int(_safe_float(row.get("season")) or target_season)
        if not name or not pos or season != target_season:
            continue
        key = (name, pos)
        merged[key] = dict(row)
        family = POSITION_FAMILY_MAP.get(pos)
        if family:
            family_keys_by_name[(name, family)].append(key)

    with SG_ADVANCED_PATH.open() as f:
        for row in csv.DictReader(f):
            name = canonical_player_name(str(row.get("player_name", "")).strip())
            pos = normalize_pos(str(row.get("position", "")).strip())
            season = int(_safe_float(row.get("season")) or target_season)
            if not name or not pos or season != target_season:
                continue
            merge_key = (name, pos)
            cur = merged.get(merge_key, {})
            if not cur:
                family = POSITION_FAMILY_MAP.get(pos)
                family_candidates = family_keys_by_name.get((name, family), []) if family else []
                if len(family_candidates) == 1:
                    merge_key = family_candidates[0]
                    cur = merged.get(merge_key, {})
            base = dict(cur) if cur else {
                "player_name": row.get("player_name", ""),
                "school": row.get("school", ""),
                "position": pos,
                "season": target_season,
            }
            for field, value in row.items():
                if value not in {"", None}:
                    base[field] = value
            merged[merge_key] = base
    return list(merged.values())


def _discover_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path
    for candidate in DEFAULT_PATH_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def load_cfb_production_signals(path: Path | None = None, target_season: int = 2025) -> dict:
    src_path = _discover_path(path)
    rows = _load_rows(src_path)
    rows = _merge_scoutinggrade_advanced_rows(rows, target_season)
    if not rows:
        return {
            "by_name_pos": {},
            "by_name": {},
            "meta": {
                "status": "missing_cfb_production_file",
                "path": str(src_path) if src_path else "",
                "rows": 0,
            },
        }

    seasonal_rows: list[dict] = []
    for row in rows:
        name = str(row.get("player_name", "")).strip()
        position = normalize_pos(str(row.get("position", "")).strip())
        if not name or not position:
            continue
        season = int(_safe_float(row.get("season")) or target_season)
        if season != target_season:
            continue
        if position not in TARGET_POSITIONS:
            continue
        seasonal_rows.append(row)

    # Build position + conference distributions from the same season for percentile normalization.
    pop_by_pos: dict[str, list[float]] = defaultdict(list)
    pop_by_pos_conf: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in seasonal_rows:
        position = normalize_pos(str(row.get("position", "")).strip())
        qb_eff_sig, _qb_epa = _qb_eff_signal(row)
        qb_pressure_sig = _qb_pressure_signal(row)
        wrte_sig, _yprr, _target_share, _targets_per_route, _wrte_diag = _wrte_signal(row)
        rb_sig, _explosive_rate, _mtf, _rb_yac_per_att, _rb_target_share, _rb_receiving_eff, _rb_diag = _rb_signal(row)
        edge_sig, _pressure_rate, _sacks_per_pr_snap, _edge_diag = _edge_signal(row)
        lb_sig, _lb_tackles, _lb_tfl, _lb_sacks, _lb_hurries, _lb_usage, _lb_snaps, _lb_diag = _lb_signal(row)
        db_sig, _cov_plays_per_target, _yards_allowed_per_cov_snap, _db_diag = _db_signal(row)
        ol_proxy_sig, _ol_years, _ol_starts, _ol_usage, _ol_diag = _ol_proxy_signal(row)
        game_ctx_sig, _game_ctx_cov, _game_ctx_diag = _game_context_signal(row)
        legacy_sig = _legacy_position_signal(
            position=position,
            qb_eff_sig=qb_eff_sig,
            qb_pressure_sig=qb_pressure_sig,
            wrte_sig=wrte_sig,
            rb_sig=rb_sig,
            edge_sig=edge_sig,
            lb_sig=lb_sig,
            db_sig=db_sig,
            ol_proxy_sig=ol_proxy_sig,
        )
        if legacy_sig is None:
            continue
        pop_by_pos[position].append(float(legacy_sig))
        conf = _conference_key(row)
        if conf:
            pop_by_pos_conf[(position, conf)].append(float(legacy_sig))

    by_name_pos: dict[tuple[str, str], dict] = {}
    by_name: dict[str, dict] = {}
    p0_matches = 0
    p0_blend_applied = 0
    p0_only_applied = 0
    opp_def_context_available = 0
    opp_def_context_applied = 0
    nonpos_metrics_ignored_rows = 0
    nonpos_metrics_ignored_total = 0
    years_played_available = 0
    sg_advanced_matches = 0

    for row in seasonal_rows:
        name = str(row.get("player_name", "")).strip()
        position = normalize_pos(str(row.get("position", "")).strip())
        if not name or not position:
            continue

        season = int(_safe_float(row.get("season")) or target_season)
        name_key = canonical_player_name(name)

        qb_eff_sig, qb_epa = _qb_eff_signal(row)
        qb_pressure_sig = _qb_pressure_signal(row)
        wrte_sig, yprr, target_share, targets_per_route, wrte_diag = _wrte_signal(row)
        rb_sig, explosive_rate, mtf, rb_yac_per_att, rb_target_share, rb_receiving_eff, rb_diag = _rb_signal(row)
        edge_sig, pressure_rate, sacks_per_pr_snap, edge_diag = _edge_signal(row)
        lb_sig, lb_tackles, lb_tfl, lb_sacks, lb_hurries, lb_usage_rate, lb_def_snaps, lb_diag = _lb_signal(row)
        db_sig, cov_plays_per_target, yards_allowed_per_cov_snap, db_diag = _db_signal(row)
        ol_proxy_sig, ol_years_played, ol_starts, ol_usage_rate, ol_diag = _ol_proxy_signal(row)
        sg_signal = None
        sg_cov = 0
        if position == "QB":
            sg_signal, sg_cov = _sg_qb_signal(row)
        elif position in {"WR", "TE"}:
            sg_signal, sg_cov = _sg_wrte_signal(row)
        elif position == "RB":
            sg_signal, sg_cov = _sg_rb_signal(row)
        elif position in {"EDGE", "DT"}:
            sg_signal, sg_cov = _sg_dl_signal(row)
        elif position == "LB":
            sg_signal, sg_cov = _sg_lb_signal(row)
        elif position in {"CB", "S"}:
            sg_signal, sg_cov = _sg_db_signal(row)
        elif position in {"OT", "IOL"}:
            sg_signal, sg_cov = _sg_ol_signal(row)

        cfb_prod_signal_legacy = _legacy_position_signal(
            position=position,
            qb_eff_sig=qb_eff_sig,
            qb_pressure_sig=qb_pressure_sig,
            wrte_sig=wrte_sig,
            rb_sig=rb_sig,
            edge_sig=edge_sig,
            lb_sig=lb_sig,
            db_sig=db_sig,
            ol_proxy_sig=ol_proxy_sig,
        )

        # Coverage count is position-specific to avoid unrelated feature inflation.
        fallback_metric_count = 0
        if position == "QB":
            coverage_count = int(qb_eff_sig is not None) + int(qb_pressure_sig is not None)
        elif position in {"WR", "TE"}:
            coverage_count = int(wrte_diag.get("wrte_available_count", 0) or 0)
            fallback_metric_count = int(wrte_diag.get("wrte_fallback_count", 0) or 0)
        elif position == "RB":
            coverage_count = int(rb_diag.get("rb_available_count", 0) or 0)
        elif position in {"EDGE", "DT"}:
            coverage_count = int(edge_diag.get("edge_available_count", 0) or 0)
            fallback_metric_count = int(edge_diag.get("edge_fallback_count", 0) or 0)
        elif position == "LB":
            coverage_count = int(lb_diag.get("lb_available_count", 0) or 0)
        elif position in {"CB", "S"}:
            coverage_count = int(db_diag.get("db_available_count", 0) or 0)
            fallback_metric_count = int(db_diag.get("db_fallback_count", 0) or 0)
        elif position in {"OT", "IOL"}:
            coverage_count = int(ol_diag.get("ol_available_count", 0) or 0)
        else:
            coverage_count = 0
        coverage_count += sg_cov
        if position in {"QB", "WR", "TE", "RB", "EDGE", "DT", "LB", "CB", "S"}:
            coverage_count += _game_ctx_cov
        if sg_cov > 0:
            sg_advanced_matches += 1

        p0_signal = None
        p0_cov = 0
        if position == "QB":
            p0_signal, p0_cov = _qb_p0_signal(row)
        elif position in {"WR", "TE"}:
            p0_signal, p0_cov = _wrte_p0_signal(row)
        elif position == "RB":
            p0_signal, p0_cov = _rb_p0_signal(row)

        if p0_cov > 0:
            coverage_count += p0_cov
            p0_matches += 1

        conf = _conference_key(row)
        conf_pop = pop_by_pos_conf.get((position, conf), [])
        pos_pop = pop_by_pos.get(position, [])
        pop_for_percentile = conf_pop if len(conf_pop) >= 18 else pos_pop
        percentile_signal = _score_percentile(cfb_prod_signal_legacy, pop_for_percentile)

        cfb_prod_contextual_signal = cfb_prod_signal_legacy
        if cfb_prod_contextual_signal is not None and percentile_signal is not None:
            cfb_prod_contextual_signal = (
                ((1.0 - CFB_PERCENTILE_BLEND_WEIGHT) * float(cfb_prod_contextual_signal))
                + (CFB_PERCENTILE_BLEND_WEIGHT * float(percentile_signal))
            )
        if sg_signal is not None:
            sg_weight = float(SG_ADV_BLEND_WEIGHTS.get(position, 0.55))
            if cfb_prod_contextual_signal is not None:
                cfb_prod_contextual_signal = (
                    ((1.0 - sg_weight) * float(cfb_prod_contextual_signal))
                    + (sg_weight * float(sg_signal))
                )
            else:
                cfb_prod_contextual_signal = 55.0 + ((float(sg_signal) - 55.0) * 0.92)
        if cfb_prod_contextual_signal is not None and game_ctx_sig is not None:
            game_weight = 0.14 if position in {"QB", "WR", "TE", "RB"} else 0.12 if position in {"EDGE", "DT", "LB", "CB", "S"} else 0.0
            if game_weight > 0:
                cfb_prod_contextual_signal = (
                    ((1.0 - game_weight) * float(cfb_prod_contextual_signal))
                    + (game_weight * float(game_ctx_sig))
                )
        usage = _usage_rate(position, row)
        usage_mult = _usage_context_multiplier(position, usage)
        if cfb_prod_contextual_signal is not None:
            cfb_prod_contextual_signal = 55.0 + ((float(cfb_prod_contextual_signal) - 55.0) * usage_mult)

        opp_adj = _opponent_defense_adjustment(position, row)
        if str(opp_adj.get("cfb_opp_def_toughness_index", "")).strip():
            opp_def_context_available += 1
        if cfb_prod_contextual_signal is not None and str(opp_adj.get("cfb_opp_def_adjustment_delta", "")).strip():
            cfb_prod_contextual_signal = float(cfb_prod_contextual_signal) + float(
                opp_adj.get("cfb_opp_def_adjustment_delta") or 0.0
            )
            if int(opp_adj.get("cfb_opp_def_context_applied") or 0) == 1:
                opp_def_context_applied += 1

        cfb_prod_signal, p0_diag = _apply_p0_guardrail(position, cfb_prod_contextual_signal, p0_signal)
        if p0_diag.get("cfbfastr_p0_mode") == "legacy_plus_p0_guarded":
            p0_blend_applied += 1
        elif p0_diag.get("cfbfastr_p0_mode") == "p0_only_guarded":
            p0_only_applied += 1

        cfb_source = str(row.get("source", "")).strip()
        quality_label, reliability = _quality_and_reliability(row, coverage_count, cfb_source)
        scope_diag = _position_scope_diagnostics(position, row)
        years_played = _first_float(row, ["years_played", "cfb_years_played"])
        years_played_seasons = str(row.get("years_played_seasons", "")).strip()
        years_played_source = str(row.get("years_played_source", "")).strip()
        if years_played is not None:
            years_played_available += 1
        qb_pass_att = _first_float(row, ["qb_pass_att", "cfb_qb_pass_att"])
        qb_pass_comp = _first_float(row, ["qb_pass_comp", "cfb_qb_pass_comp"])
        qb_pass_yds = _first_float(row, ["qb_pass_yds", "cfb_qb_pass_yds"])
        qb_pass_td = _first_float(row, ["qb_pass_td", "cfb_qb_pass_td"])
        qb_pass_int = _first_float(row, ["qb_pass_int", "cfb_qb_pass_int"])
        qb_int_rate = None
        if qb_pass_int is not None and qb_pass_att is not None and qb_pass_att > 0:
            qb_int_rate = qb_pass_int / qb_pass_att
        qb_rush_yds = _first_float(row, ["qb_rush_yds", "cfb_qb_rush_yds"])
        qb_rush_td = _first_float(row, ["qb_rush_td", "cfb_qb_rush_td"])
        wrte_rec = _first_float(row, ["wrte_rec", "cfb_wrte_rec"])
        wrte_rec_yds = _first_float(row, ["wrte_rec_yds", "cfb_wrte_rec_yds"])
        wrte_rec_td = _first_float(row, ["wrte_rec_td", "cfb_wrte_rec_td"])
        rb_rush_att = _first_float(row, ["rb_rush_att", "cfb_rb_rush_att"])
        rb_rush_yds = _first_float(row, ["rb_rush_yds", "cfb_rb_rush_yds"])
        rb_rush_td = _first_float(row, ["rb_rush_td", "cfb_rb_rush_td"])
        rb_rec = _first_float(row, ["rb_rec", "cfb_rb_rec"])
        rb_rec_yds = _first_float(row, ["rb_rec_yds", "cfb_rb_rec_yds"])
        rb_rec_td = _first_float(row, ["rb_rec_td", "cfb_rb_rec_td"])
        edge_sacks_count = _first_float(row, ["edge_sacks", "cfb_edge_sacks"])
        edge_qb_hurries = _first_float(row, ["edge_qb_hurries", "cfb_edge_qb_hurries"])
        edge_tfl = _first_float(row, ["edge_tfl", "cfb_edge_tfl"])
        edge_tackles = _first_float(row, ["edge_tackles", "cfb_edge_tackles"])
        db_int = _first_float(row, ["db_int", "cfb_db_int"])
        db_pbu = _first_float(row, ["db_pbu", "cfb_db_pbu"])
        db_tackles = _first_float(row, ["db_tackles", "cfb_db_tackles"])
        db_tfl = _first_float(row, ["db_tfl", "cfb_db_tfl"])
        ignored_count = int(scope_diag.get("cfb_nonpos_metrics_ignored_count", 0) or 0)
        if ignored_count > 0:
            nonpos_metrics_ignored_rows += 1
            nonpos_metrics_ignored_total += ignored_count
        if cfb_prod_signal is not None:
            adjusted_signal = (reliability * float(cfb_prod_signal)) + ((1.0 - reliability) * 55.0)
            # Extra brake: proxy-only single-metric production should refine, not drive.
            if quality_label == "proxy" and coverage_count <= 1:
                if position in {"EDGE", "DT"}:
                    adjusted_signal = min(adjusted_signal, 66.0)
                elif position in {"CB", "S"}:
                    adjusted_signal = min(adjusted_signal, 64.0)
        else:
            adjusted_signal = None

        real_features = int(_safe_float(row.get("cfb_prod_real_features")) or 0)
        proxy_features = int(_safe_float(row.get("cfb_prod_proxy_features")) or coverage_count)

        payload = {
            "cfb_prod_signal": round(adjusted_signal, 2) if adjusted_signal is not None else "",
            "cfb_prod_signal_raw": round(cfb_prod_signal_legacy, 2) if cfb_prod_signal_legacy is not None else "",
            "cfb_prod_signal_contextual_raw": round(cfb_prod_contextual_signal, 2)
            if cfb_prod_contextual_signal is not None
            else "",
            "sg_advanced_signal": round(sg_signal, 2) if sg_signal is not None else "",
            "sg_advanced_available_count": sg_cov,
            "sg_advanced_source": "scoutinggrade_advanced_2025"
            if sg_cov > 0
            else "",
            "sg_qb_pass_grade": row.get("sg_qb_pass_grade", ""),
            "sg_qb_btt_rate": row.get("sg_qb_btt_rate", ""),
            "sg_qb_twp_rate": row.get("sg_qb_twp_rate", ""),
            "sg_qb_pressure_to_sack_rate": row.get("sg_qb_pressure_to_sack_rate", ""),
            "sg_qb_pressure_grade": row.get("sg_qb_pressure_grade", ""),
            "sg_qb_blitz_grade": row.get("sg_qb_blitz_grade", ""),
            "sg_qb_no_screen_grade": row.get("sg_qb_no_screen_grade", ""),
            "sg_qb_quick_qb_rating": row.get("sg_qb_quick_qb_rating", ""),
            "sg_rb_run_grade": row.get("sg_rb_run_grade", ""),
            "sg_rb_elusive_rating": row.get("sg_rb_elusive_rating", ""),
            "sg_rb_yco_attempt": row.get("sg_rb_yco_attempt", ""),
            "sg_rb_explosive_rate": row.get("sg_rb_explosive_rate", ""),
            "sg_rb_breakaway_percent": row.get("sg_rb_breakaway_percent", ""),
            "sg_rb_targets_per_route": row.get("sg_rb_targets_per_route", ""),
            "sg_rb_yprr": row.get("sg_rb_yprr", ""),
            "sg_wrte_route_grade": row.get("sg_wrte_route_grade", ""),
            "sg_wrte_yprr": row.get("sg_wrte_yprr", ""),
            "sg_wrte_targets_per_route": row.get("sg_wrte_targets_per_route", ""),
            "sg_wrte_man_yprr": row.get("sg_wrte_man_yprr", ""),
            "sg_wrte_zone_yprr": row.get("sg_wrte_zone_yprr", ""),
            "sg_wrte_contested_catch_rate": row.get("sg_wrte_contested_catch_rate", ""),
            "sg_wrte_drop_rate": row.get("sg_wrte_drop_rate", ""),
            "sg_dl_pass_rush_grade": row.get("sg_dl_pass_rush_grade", ""),
            "sg_dl_pass_rush_win_rate": row.get("sg_dl_pass_rush_win_rate", ""),
            "sg_dl_prp": row.get("sg_dl_prp", ""),
            "sg_dl_true_pass_set_win_rate": row.get("sg_dl_true_pass_set_win_rate", ""),
            "sg_dl_true_pass_set_prp": row.get("sg_dl_true_pass_set_prp", ""),
            "sg_dl_total_pressures": row.get("sg_dl_total_pressures", ""),
            "sg_front_run_def_grade": row.get("sg_front_run_def_grade", ""),
            "sg_front_stop_percent": row.get("sg_front_stop_percent", ""),
            "sg_def_coverage_grade": row.get("sg_def_coverage_grade", ""),
            "sg_def_run_grade": row.get("sg_def_run_grade", ""),
            "sg_def_tackle_grade": row.get("sg_def_tackle_grade", ""),
            "sg_def_missed_tackle_rate": row.get("sg_def_missed_tackle_rate", ""),
            "sg_def_total_pressures": row.get("sg_def_total_pressures", ""),
            "sg_def_tackles_for_loss": row.get("sg_def_tackles_for_loss", ""),
            "sg_def_tackles": row.get("sg_def_tackles", ""),
            "sg_def_pass_break_ups": row.get("sg_def_pass_break_ups", ""),
            "sg_def_interceptions": row.get("sg_def_interceptions", ""),
            "sg_cov_grade": row.get("sg_cov_grade", ""),
            "sg_cov_forced_incompletion_rate": row.get("sg_cov_forced_incompletion_rate", ""),
            "sg_cov_snaps_per_target": row.get("sg_cov_snaps_per_target", ""),
            "sg_cov_yards_per_snap": row.get("sg_cov_yards_per_snap", ""),
            "sg_cov_qb_rating_against": row.get("sg_cov_qb_rating_against", ""),
            "sg_source_season": row.get("sg_source_season", ""),
            "sg_cov_man_grade": row.get("sg_cov_man_grade", ""),
            "sg_cov_zone_grade": row.get("sg_cov_zone_grade", ""),
            "sg_slot_cov_snaps": row.get("sg_slot_cov_snaps", ""),
            "sg_slot_cov_snaps_per_target": row.get("sg_slot_cov_snaps_per_target", ""),
            "sg_slot_cov_qb_rating_against": row.get("sg_slot_cov_qb_rating_against", ""),
            "sg_slot_cov_yards_per_snap": row.get("sg_slot_cov_yards_per_snap", ""),
            "sg_cov_source_season": row.get("sg_cov_source_season", ""),
            "sg_ol_pass_block_grade": row.get("sg_ol_pass_block_grade", ""),
            "sg_ol_run_block_grade": row.get("sg_ol_run_block_grade", ""),
            "sg_ol_pbe": row.get("sg_ol_pbe", ""),
            "sg_ol_pressure_allowed_rate": row.get("sg_ol_pressure_allowed_rate", ""),
            "sg_ol_versatility_count": row.get("sg_ol_versatility_count", ""),
            "cfb_prod_percentile_signal": round(percentile_signal, 2) if percentile_signal is not None else "",
            "cfb_prod_percentile_population_n": len(pop_for_percentile),
            "cfb_prod_usage_rate": round(usage, 4) if usage is not None else "",
            "cfb_prod_usage_multiplier": usage_mult,
            "cfb_prod_context_conference": conf,
            "cfb_opp_def_ppa_allowed_avg": opp_adj.get("cfb_opp_def_ppa_allowed_avg", ""),
            "cfb_opp_def_success_rate_allowed_avg": opp_adj.get("cfb_opp_def_success_rate_allowed_avg", ""),
            "cfb_opp_def_toughness_index": opp_adj.get("cfb_opp_def_toughness_index", ""),
            "cfb_opp_def_adjustment_multiplier": opp_adj.get("cfb_opp_def_adjustment_multiplier", ""),
            "cfb_opp_def_adjustment_delta": opp_adj.get("cfb_opp_def_adjustment_delta", ""),
            "cfb_opp_def_context_applied": opp_adj.get("cfb_opp_def_context_applied", 0),
            "cfb_opp_def_context_source": opp_adj.get("cfb_opp_def_context_source", ""),
            "cfb_prod_available": 1 if cfb_prod_signal is not None else 0,
            "cfb_prod_coverage_count": coverage_count,
            "cfb_prod_quality_label": quality_label,
            "cfb_prod_reliability": reliability,
            "cfb_prod_real_features": real_features,
            "cfb_prod_proxy_features": proxy_features,
            "cfb_prod_proxy_fallback_features": fallback_metric_count,
            "cfb_years_played": int(round(years_played)) if years_played is not None else "",
            "cfb_years_played_seasons": years_played_seasons,
            "cfb_years_played_source": years_played_source,
            "cfb_nonpos_metrics_ignored_count": scope_diag.get("cfb_nonpos_metrics_ignored_count", 0),
            "cfb_nonpos_metrics_ignored_fields": scope_diag.get("cfb_nonpos_metrics_ignored_fields", ""),
            "cfb_prod_provenance": str(row.get("cfb_prod_provenance", "")).strip(),
            "cfbfastr_p0_signal_raw": p0_diag.get("cfbfastr_p0_signal_raw", ""),
            "cfbfastr_p0_available": p0_diag.get("cfbfastr_p0_available", 0),
            "cfbfastr_p0_mode": p0_diag.get("cfbfastr_p0_mode", ""),
            "cfbfastr_p0_applied_delta": p0_diag.get("cfbfastr_p0_applied_delta", ""),
            "cfbfastr_p0_max_delta": p0_diag.get("cfbfastr_p0_max_delta", ""),
            "cfbfastr_p0_coverage_count": p0_cov,
            "cfb_qb_eff_signal": round(qb_eff_sig, 2) if qb_eff_sig is not None else "",
            "cfb_qb_pressure_signal": round(qb_pressure_sig, 2) if qb_pressure_sig is not None else "",
            "cfb_game_context_signal": round(game_ctx_sig, 2) if game_ctx_sig is not None else "",
            "cfb_game_context_source": _game_ctx_diag.get("game_context_source", ""),
            "cfb_game_context_top_def_games": _game_ctx_diag.get("game_context_top_def_games", 0),
            "cfb_game_context_weekly_sample_games": _game_ctx_diag.get("game_context_weekly_sample_games", 0),
            "cfb_wrte_yprr_signal": round(_score_linear(yprr, 1.0, 3.3), 2) if yprr is not None else "",
            "cfb_wrte_target_share_signal": round(_score_linear(target_share, 0.12, 0.35), 2) if target_share is not None else "",
            "cfb_wrte_targets_per_route_signal": round(_score_linear(targets_per_route, 0.10, 0.34), 2)
            if targets_per_route is not None
            else "",
            "cfb_rb_explosive_signal": round(_score_linear(explosive_rate, 0.06, 0.22), 2) if explosive_rate is not None else "",
            "cfb_rb_mtf_signal": round(_score_linear(mtf, 0.08, 0.35), 2) if mtf is not None else "",
            "cfb_rb_yac_per_att_signal": round(_score_linear(rb_yac_per_att, 2.0, 4.2), 2)
            if rb_yac_per_att is not None
            else "",
            "cfb_rb_target_share_signal": round(_score_linear(rb_target_share, 0.05, 0.16), 2)
            if rb_target_share is not None
            else "",
            "cfb_rb_receiving_eff_signal": round(_score_linear(rb_receiving_eff, 5.5, 11.5), 2)
            if rb_receiving_eff is not None
            else "",
            "cfb_edge_pressure_signal": round(edge_sig, 2) if edge_sig is not None else "",
            "cfb_edge_sacks_per_pr_snap_signal": round(_score_linear(sacks_per_pr_snap, 0.02, 0.055), 2)
            if sacks_per_pr_snap is not None
            else "",
            "cfb_lb_signal": round(lb_sig, 2) if lb_sig is not None else "",
            "cfb_lb_tackle_signal": round(
                _score_linear((lb_tackles / lb_def_snaps), 0.055, 0.145)
                if (lb_tackles is not None and lb_def_snaps is not None and lb_def_snaps >= 120)
                else _score_linear(lb_tackles, 34.0, 112.0),
                2,
            )
            if lb_tackles is not None
            else "",
            "cfb_lb_tfl_signal": round(
                _score_linear((lb_tfl / lb_def_snaps), 0.006, 0.028)
                if (lb_tfl is not None and lb_def_snaps is not None and lb_def_snaps >= 120)
                else _score_linear(lb_tfl, 3.0, 16.0),
                2,
            )
            if lb_tfl is not None
            else "",
            "cfb_lb_rush_impact_signal": round(
                _score_linear(((lb_sacks or 0.0) + (lb_hurries or 0.0)) / lb_def_snaps, 0.015, 0.085)
                if (lb_def_snaps is not None and lb_def_snaps >= 120)
                else _score_linear((lb_sacks or 0.0) + (lb_hurries or 0.0), 6.0, 28.0),
                2,
            )
            if (lb_sacks is not None or lb_hurries is not None)
            else "",
            "cfb_ol_proxy_signal": round(ol_proxy_sig, 2) if ol_proxy_sig is not None else "",
            "cfb_db_cov_plays_per_target_signal": round(db_sig, 2) if db_sig is not None else "",
            "cfb_db_yards_allowed_per_cov_snap_signal": round(_score_inverse(yards_allowed_per_cov_snap, 1.80, 0.55), 2)
            if yards_allowed_per_cov_snap is not None
            else "",
            "cfb_qb_epa_per_play": round(qb_epa, 4) if qb_epa is not None else "",
            "cfb_qb_pass_att": int(round(qb_pass_att)) if qb_pass_att is not None else "",
            "cfb_qb_pass_comp": int(round(qb_pass_comp)) if qb_pass_comp is not None else "",
            "cfb_qb_pass_yds": int(round(qb_pass_yds)) if qb_pass_yds is not None else "",
            "cfb_qb_pass_td": int(round(qb_pass_td)) if qb_pass_td is not None else "",
            "cfb_qb_pass_int": int(round(qb_pass_int)) if qb_pass_int is not None else "",
            "cfb_qb_int_rate": round(qb_int_rate, 4) if qb_int_rate is not None else "",
            "cfb_qb_rush_yds": int(round(qb_rush_yds)) if qb_rush_yds is not None else "",
            "cfb_qb_rush_td": int(round(qb_rush_td)) if qb_rush_td is not None else "",
            "cfb_wrte_yprr": round(yprr, 3) if yprr is not None else "",
            "cfb_wrte_target_share": round(target_share, 4) if target_share is not None else "",
            "cfb_wrte_targets_per_route": round(targets_per_route, 4) if targets_per_route is not None else "",
            "cfb_wrte_targets_per_route_source": wrte_diag.get("wrte_targets_per_route_source", ""),
            "cfb_wrte_targets_per_route_weight": wrte_diag.get("wrte_targets_per_route_weight", ""),
            "cfb_wrte_rec": int(round(wrte_rec)) if wrte_rec is not None else "",
            "cfb_wrte_rec_yds": int(round(wrte_rec_yds)) if wrte_rec_yds is not None else "",
            "cfb_wrte_rec_td": int(round(wrte_rec_td)) if wrte_rec_td is not None else "",
            "cfb_rb_explosive_rate": round(explosive_rate, 4) if explosive_rate is not None else "",
            "cfb_rb_missed_tackles_forced_per_touch": round(mtf, 4) if mtf is not None else "",
            "cfb_rb_yards_after_contact_per_attempt": round(rb_yac_per_att, 4) if rb_yac_per_att is not None else "",
            "cfb_rb_target_share": round(rb_target_share, 4) if rb_target_share is not None else "",
            "cfb_rb_receiving_efficiency": round(rb_receiving_eff, 4) if rb_receiving_eff is not None else "",
            "cfb_rb_target_share_source": rb_diag.get("rb_target_share_source", ""),
            "cfb_rb_rush_att": int(round(rb_rush_att)) if rb_rush_att is not None else "",
            "cfb_rb_rush_yds": int(round(rb_rush_yds)) if rb_rush_yds is not None else "",
            "cfb_rb_rush_td": int(round(rb_rush_td)) if rb_rush_td is not None else "",
            "cfb_rb_rec": int(round(rb_rec)) if rb_rec is not None else "",
            "cfb_rb_rec_yds": int(round(rb_rec_yds)) if rb_rec_yds is not None else "",
            "cfb_rb_rec_td": int(round(rb_rec_td)) if rb_rec_td is not None else "",
            "cfb_lb_tackles": int(round(lb_tackles)) if lb_tackles is not None else "",
            "cfb_lb_tfl": int(round(lb_tfl)) if lb_tfl is not None else "",
            "cfb_lb_sacks": round(lb_sacks, 1) if lb_sacks is not None else "",
            "cfb_lb_qb_hurries": int(round(lb_hurries)) if lb_hurries is not None else "",
            "cfb_lb_usage_rate": round(lb_usage_rate, 4) if lb_usage_rate is not None else "",
            "cfb_lb_def_snaps": int(round(lb_def_snaps)) if lb_def_snaps is not None else "",
            "cfb_lb_rate_source": lb_diag.get("lb_rate_source", ""),
            "cfb_ol_years_played": int(round(ol_years_played)) if ol_years_played is not None else "",
            "cfb_ol_starts": int(round(ol_starts)) if ol_starts is not None else "",
            "cfb_ol_usage_rate": round(ol_usage_rate, 4) if ol_usage_rate is not None else "",
            "cfb_ol_proxy_quality_label": ol_diag.get("ol_proxy_quality_label", ""),
            "cfb_edge_pressure_rate": round(pressure_rate, 4) if pressure_rate is not None else "",
            "cfb_edge_sacks_per_pr_snap": round(sacks_per_pr_snap, 4) if sacks_per_pr_snap is not None else "",
            "cfb_edge_sacks_per_pr_snap_source": edge_diag.get("edge_sacks_per_pr_snap_source", ""),
            "cfb_edge_pressure_weight": edge_diag.get("edge_pressure_weight", ""),
            "cfb_edge_sack_weight": edge_diag.get("edge_sack_weight", ""),
            "cfb_edge_sacks": int(round(edge_sacks_count)) if edge_sacks_count is not None else "",
            "cfb_edge_qb_hurries": int(round(edge_qb_hurries)) if edge_qb_hurries is not None else "",
            "cfb_edge_tfl": int(round(edge_tfl)) if edge_tfl is not None else "",
            "cfb_edge_tackles": int(round(edge_tackles)) if edge_tackles is not None else "",
            "cfb_db_coverage_plays_per_target": round(cov_plays_per_target, 4) if cov_plays_per_target is not None else "",
            "cfb_db_yards_allowed_per_coverage_snap": round(yards_allowed_per_cov_snap, 4)
            if yards_allowed_per_cov_snap is not None
            else "",
            "cfb_db_yards_allowed_per_cov_snap_source": db_diag.get("db_yards_allowed_per_cov_snap_source", ""),
            "cfb_db_cov_weight": db_diag.get("db_cov_weight", ""),
            "cfb_db_yacs_weight": db_diag.get("db_yacs_weight", ""),
            "cfb_db_int": int(round(db_int)) if db_int is not None else "",
            "cfb_db_pbu": int(round(db_pbu)) if db_pbu is not None else "",
            "cfb_db_tackles": int(round(db_tackles)) if db_tackles is not None else "",
            "cfb_db_tfl": int(round(db_tfl)) if db_tfl is not None else "",
            "cfb_source": cfb_source,
            "cfb_season": season,
        }

        key = (name_key, position)
        existing = by_name_pos.get(key)
        if existing is None or int(payload["cfb_prod_coverage_count"]) > int(existing.get("cfb_prod_coverage_count", 0) or 0):
            by_name_pos[key] = payload

        name_existing = by_name.get(name_key)
        if name_existing is None or int(payload["cfb_prod_coverage_count"]) > int(name_existing.get("cfb_prod_coverage_count", 0) or 0):
            by_name[name_key] = payload

    return {
        "by_name_pos": by_name_pos,
        "by_name": by_name,
        "meta": {
            "status": "ok",
            "path": str(src_path) if src_path else "",
            "rows": len(rows),
            "seasonal_rows": len(seasonal_rows),
            "target_season": target_season,
            "matched_name_pos": len(by_name_pos),
            "matched_name": len(by_name),
            "cfbfastr_p0_matches": p0_matches,
            "cfbfastr_p0_blend_applied": p0_blend_applied,
            "cfbfastr_p0_only_applied": p0_only_applied,
            "cfbfastr_p0_blend_weight": P0_BLEND_WEIGHT,
            "cfbfastr_p0_max_delta": P0_MAX_DELTA,
            "cfbfastr_p0_qb_max_delta": P0_QB_MAX_DELTA,
            "cfb_percentile_blend_weight": CFB_PERCENTILE_BLEND_WEIGHT,
            "cfb_opp_def_adj_max_delta": CFB_OPP_DEF_ADJ_MAX_DELTA,
            "wrte_tpr_direct_weight": WRTE_TPR_DIRECT_WEIGHT,
            "wrte_tpr_derived_weight": WRTE_TPR_DERIVED_WEIGHT,
            "edge_sack_direct_weight": EDGE_SACK_DIRECT_WEIGHT,
            "edge_sack_derived_weight": EDGE_SACK_DERIVED_WEIGHT,
            "edge_sack_pressure_only_weight": EDGE_SACK_PRESSURE_ONLY_WEIGHT,
            "db_yacs_direct_weight": DB_YACS_DIRECT_WEIGHT,
            "db_yacs_derived_weight": DB_YACS_DERIVED_WEIGHT,
            "cfb_opp_def_context_available": opp_def_context_available,
            "cfb_opp_def_context_applied": opp_def_context_applied,
            "cfb_nonpos_metrics_ignored_rows": nonpos_metrics_ignored_rows,
            "cfb_nonpos_metrics_ignored_total": nonpos_metrics_ignored_total,
            "cfb_years_played_available": years_played_available,
            "sg_advanced_matches": sg_advanced_matches,
        },
    }
