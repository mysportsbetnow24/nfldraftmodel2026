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

TARGET_POSITIONS = {"QB", "WR", "TE", "RB", "EDGE", "CB", "S"}
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
        "opp_def_ppa_allowed_avg",
        "opp_def_success_rate_allowed_avg",
        "opp_def_toughness_index",
        "opp_def_adjustment_multiplier",
    },
    "WR": {
        "yprr",
        "target_share",
        "targets_per_route_run",
        "wrte_ppa_overall",
        "wrte_ppa_passing_downs",
        "wrte_wepa_receiving",
        "wrte_usage_rate",
        "opp_def_ppa_allowed_avg",
        "opp_def_success_rate_allowed_avg",
        "opp_def_toughness_index",
        "opp_def_adjustment_multiplier",
    },
    "TE": {
        "yprr",
        "target_share",
        "targets_per_route_run",
        "wrte_ppa_overall",
        "wrte_ppa_passing_downs",
        "wrte_wepa_receiving",
        "wrte_usage_rate",
        "opp_def_ppa_allowed_avg",
        "opp_def_success_rate_allowed_avg",
        "opp_def_toughness_index",
        "opp_def_adjustment_multiplier",
    },
    "RB": {
        "explosive_run_rate",
        "missed_tackles_forced_per_touch",
        "rb_ppa_rushing",
        "rb_ppa_standard_downs",
        "rb_wepa_rushing",
        "rb_usage_rate",
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
    },
    "CB": {"coverage_plays_per_target", "yards_allowed_per_coverage_snap"},
    "S": {"coverage_plays_per_target", "yards_allowed_per_coverage_snap"},
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
    db_sig: float | None,
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
    if position in {"CB", "S"}:
        return db_sig
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
    if position in {"CB", "S"}:
        return _first_float(row, ["db_usage_rate", "coverage_snap_rate", "usage_rate"])
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
    elif position in {"CB", "S"}:
        floor, target, min_mult = 0.25, 0.75, 0.82
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

    parts = []
    qbr_sig = _score_linear(qbr, 45.0, 90.0)
    if qbr_sig is not None:
        parts.append((0.45, qbr_sig))
    epa_sig = _score_linear(epa, -0.20, 0.45)
    if epa_sig is not None:
        parts.append((0.35, epa_sig))
    succ_sig = _score_linear(success, 0.35, 0.60)
    if succ_sig is not None:
        parts.append((0.20, succ_sig))
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


def _rb_signal(row: dict) -> tuple[float | None, float | None, float | None]:
    explosive_rate = _first_float(row, ["explosive_run_rate", "explosive_rate"])
    mtf = _first_float(
        row,
        [
            "missed_tackles_forced_per_touch",
            "missed_tackles_forced_per_attempt",
            "mtf_per_touch",
        ],
    )
    explosive_sig = _score_linear(explosive_rate, 0.06, 0.22)
    mtf_sig = _score_linear(mtf, 0.08, 0.35)
    parts = []
    if explosive_sig is not None:
        parts.append((0.55, explosive_sig))
    if mtf_sig is not None:
        parts.append((0.45, mtf_sig))
    return _weighted_mean(parts), explosive_rate, mtf


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


def _qb_p0_signal(row: dict) -> tuple[float | None, int]:
    ppa_overall = _first_float(row, ["qb_ppa_overall", "ppa_overall", "qb_ppa_all"])
    ppa_pass = _first_float(row, ["qb_ppa_passing", "ppa_passing", "qb_ppa_pass"])
    ppa_pd = _first_float(row, ["qb_ppa_passing_downs", "ppa_passing_downs", "qb_ppa_pd"])
    ppa_sd = _first_float(row, ["qb_ppa_standard_downs", "ppa_standard_downs", "qb_ppa_sd"])
    wepa_pass = _first_float(row, ["qb_wepa_passing", "wepa_passing", "pass_wepa"])
    usage = _first_float(row, ["qb_usage_rate", "qb_usage", "usage_rate"])

    parts = []
    s_wepa = _score_linear(wepa_pass, -0.2, 0.6)
    if s_wepa is not None:
        parts.append((0.34, s_wepa))
    s_pass = _score_linear(ppa_pass, -0.2, 0.65)
    if s_pass is not None:
        parts.append((0.24, s_pass))
    s_overall = _score_linear(ppa_overall, -0.15, 0.55)
    if s_overall is not None:
        parts.append((0.17, s_overall))
    s_pd = _score_linear(ppa_pd, -0.25, 0.45)
    if s_pd is not None:
        parts.append((0.13, s_pd))
    s_sd = _score_linear(ppa_sd, -0.2, 0.45)
    if s_sd is not None:
        parts.append((0.07, s_sd))
    s_usage = _score_linear(usage, 0.08, 0.24)
    if s_usage is not None:
        parts.append((0.05, s_usage))
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
    usage = _first_float(row, ["rb_usage_rate", "rb_usage", "usage_rate"])

    parts = []
    s_wepa = _score_linear(wepa_rush, -0.2, 0.45)
    if s_wepa is not None:
        parts.append((0.42, s_wepa))
    s_ppa = _score_linear(ppa_rush, -0.25, 0.5)
    if s_ppa is not None:
        parts.append((0.33, s_ppa))
    s_sd = _score_linear(ppa_sd, -0.2, 0.4)
    if s_sd is not None:
        parts.append((0.15, s_sd))
    s_usage = _score_linear(usage, 0.10, 0.42)
    if s_usage is not None:
        parts.append((0.10, s_usage))
    score = _weighted_mean(parts)
    return score, len(parts)


def _load_rows(path: Path | None) -> list[dict]:
    if path is None or not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


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
        rb_sig, _explosive_rate, _mtf = _rb_signal(row)
        edge_sig, _pressure_rate, _sacks_per_pr_snap, _edge_diag = _edge_signal(row)
        db_sig, _cov_plays_per_target, _yards_allowed_per_cov_snap, _db_diag = _db_signal(row)
        legacy_sig = _legacy_position_signal(
            position=position,
            qb_eff_sig=qb_eff_sig,
            qb_pressure_sig=qb_pressure_sig,
            wrte_sig=wrte_sig,
            rb_sig=rb_sig,
            edge_sig=edge_sig,
            db_sig=db_sig,
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
        rb_sig, explosive_rate, mtf = _rb_signal(row)
        edge_sig, pressure_rate, sacks_per_pr_snap, edge_diag = _edge_signal(row)
        db_sig, cov_plays_per_target, yards_allowed_per_cov_snap, db_diag = _db_signal(row)

        cfb_prod_signal_legacy = _legacy_position_signal(
            position=position,
            qb_eff_sig=qb_eff_sig,
            qb_pressure_sig=qb_pressure_sig,
            wrte_sig=wrte_sig,
            rb_sig=rb_sig,
            edge_sig=edge_sig,
            db_sig=db_sig,
        )

        # Coverage count is position-specific to avoid unrelated feature inflation.
        fallback_metric_count = 0
        if position == "QB":
            coverage_count = int(qb_eff_sig is not None) + int(qb_pressure_sig is not None)
        elif position in {"WR", "TE"}:
            coverage_count = int(wrte_diag.get("wrte_available_count", 0) or 0)
            fallback_metric_count = int(wrte_diag.get("wrte_fallback_count", 0) or 0)
        elif position == "RB":
            coverage_count = int(explosive_rate is not None) + int(mtf is not None)
        elif position == "EDGE":
            coverage_count = int(edge_diag.get("edge_available_count", 0) or 0)
            fallback_metric_count = int(edge_diag.get("edge_fallback_count", 0) or 0)
        elif position in {"CB", "S"}:
            coverage_count = int(db_diag.get("db_available_count", 0) or 0)
            fallback_metric_count = int(db_diag.get("db_fallback_count", 0) or 0)
        else:
            coverage_count = 0

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
        ignored_count = int(scope_diag.get("cfb_nonpos_metrics_ignored_count", 0) or 0)
        if ignored_count > 0:
            nonpos_metrics_ignored_rows += 1
            nonpos_metrics_ignored_total += ignored_count
        if cfb_prod_signal is not None:
            adjusted_signal = (reliability * float(cfb_prod_signal)) + ((1.0 - reliability) * 55.0)
            # Extra brake: proxy-only single-metric production should refine, not drive.
            if quality_label == "proxy" and coverage_count <= 1:
                if position == "EDGE":
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
            "cfb_wrte_yprr_signal": round(_score_linear(yprr, 1.0, 3.3), 2) if yprr is not None else "",
            "cfb_wrte_target_share_signal": round(_score_linear(target_share, 0.12, 0.35), 2) if target_share is not None else "",
            "cfb_wrte_targets_per_route_signal": round(_score_linear(targets_per_route, 0.10, 0.34), 2)
            if targets_per_route is not None
            else "",
            "cfb_rb_explosive_signal": round(_score_linear(explosive_rate, 0.06, 0.22), 2) if explosive_rate is not None else "",
            "cfb_rb_mtf_signal": round(_score_linear(mtf, 0.08, 0.35), 2) if mtf is not None else "",
            "cfb_edge_pressure_signal": round(edge_sig, 2) if edge_sig is not None else "",
            "cfb_edge_sacks_per_pr_snap_signal": round(_score_linear(sacks_per_pr_snap, 0.02, 0.055), 2)
            if sacks_per_pr_snap is not None
            else "",
            "cfb_db_cov_plays_per_target_signal": round(db_sig, 2) if db_sig is not None else "",
            "cfb_db_yards_allowed_per_cov_snap_signal": round(_score_inverse(yards_allowed_per_cov_snap, 1.80, 0.55), 2)
            if yards_allowed_per_cov_snap is not None
            else "",
            "cfb_qb_epa_per_play": round(qb_epa, 4) if qb_epa is not None else "",
            "cfb_wrte_yprr": round(yprr, 3) if yprr is not None else "",
            "cfb_wrte_target_share": round(target_share, 4) if target_share is not None else "",
            "cfb_wrte_targets_per_route": round(targets_per_route, 4) if targets_per_route is not None else "",
            "cfb_wrte_targets_per_route_source": wrte_diag.get("wrte_targets_per_route_source", ""),
            "cfb_wrte_targets_per_route_weight": wrte_diag.get("wrte_targets_per_route_weight", ""),
            "cfb_rb_explosive_rate": round(explosive_rate, 4) if explosive_rate is not None else "",
            "cfb_rb_missed_tackles_forced_per_touch": round(mtf, 4) if mtf is not None else "",
            "cfb_edge_pressure_rate": round(pressure_rate, 4) if pressure_rate is not None else "",
            "cfb_edge_sacks_per_pr_snap": round(sacks_per_pr_snap, 4) if sacks_per_pr_snap is not None else "",
            "cfb_edge_sacks_per_pr_snap_source": edge_diag.get("edge_sacks_per_pr_snap_source", ""),
            "cfb_edge_pressure_weight": edge_diag.get("edge_pressure_weight", ""),
            "cfb_edge_sack_weight": edge_diag.get("edge_sack_weight", ""),
            "cfb_db_coverage_plays_per_target": round(cov_plays_per_target, 4) if cov_plays_per_target is not None else "",
            "cfb_db_yards_allowed_per_coverage_snap": round(yards_allowed_per_cov_snap, 4)
            if yards_allowed_per_cov_snap is not None
            else "",
            "cfb_db_yards_allowed_per_cov_snap_source": db_diag.get("db_yards_allowed_per_cov_snap_source", ""),
            "cfb_db_cov_weight": db_diag.get("db_cov_weight", ""),
            "cfb_db_yacs_weight": db_diag.get("db_yacs_weight", ""),
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
        },
    }
