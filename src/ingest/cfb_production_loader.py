from __future__ import annotations

import csv
import os
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
    # Lower is better (e.g., pressure-to-sack rate).
    out = _score_linear(value, low, high)
    if out is None:
        return None
    return round(115.0 - out, 2)


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
    rel = max(0.0, min(1.0, float(rel)))
    return quality, round(rel, 2)


def _qb_eff_signal(row: dict) -> tuple[float | None, float | None]:
    qbr = _first_float(row, ["qb_qbr", "qbr", "espn_qbr"])
    epa = _first_float(row, ["qb_epa_per_play", "epa_per_play", "qb_efficiency"])
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


def _wrte_signal(row: dict) -> tuple[float | None, float | None, float | None]:
    yprr = _first_float(row, ["yprr", "yards_per_route_run"])
    target_share = _first_float(row, ["target_share", "targets_share"])
    yprr_sig = _score_linear(yprr, 1.0, 3.3)
    ts_sig = _score_linear(target_share, 0.12, 0.35)
    parts = []
    if yprr_sig is not None:
        parts.append((0.65, yprr_sig))
    if ts_sig is not None:
        parts.append((0.35, ts_sig))
    return _weighted_mean(parts), yprr, target_share


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


def _edge_signal(row: dict) -> tuple[float | None, float | None]:
    pressure_rate = _first_float(row, ["pressure_rate", "pass_rush_pressure_rate"])
    return _score_linear(pressure_rate, 0.07, 0.22), pressure_rate


def _db_signal(row: dict) -> tuple[float | None, float | None]:
    cov_plays_per_target = _first_float(
        row,
        ["coverage_plays_per_target", "pbu_int_per_target", "coverage_impact_per_target"],
    )
    return _score_linear(cov_plays_per_target, 0.08, 0.30), cov_plays_per_target


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

    by_name_pos: dict[tuple[str, str], dict] = {}
    by_name: dict[str, dict] = {}
    p0_matches = 0
    p0_blend_applied = 0
    p0_only_applied = 0

    for row in rows:
        name = str(row.get("player_name", "")).strip()
        position = normalize_pos(str(row.get("position", "")).strip())
        if not name or not position:
            continue

        season = int(_safe_float(row.get("season")) or target_season)
        if season != target_season:
            continue

        name_key = canonical_player_name(name)
        coverage_count = 0

        qb_eff_sig, qb_epa = _qb_eff_signal(row)
        qb_pressure_sig = _qb_pressure_signal(row)
        wrte_sig, yprr, target_share = _wrte_signal(row)
        rb_sig, explosive_rate, mtf = _rb_signal(row)
        edge_sig, pressure_rate = _edge_signal(row)
        db_sig, cov_plays_per_target = _db_signal(row)

        if qb_eff_sig is not None:
            coverage_count += 1
        if qb_pressure_sig is not None:
            coverage_count += 1
        if wrte_sig is not None:
            coverage_count += 1
        if rb_sig is not None:
            coverage_count += 1
        if edge_sig is not None:
            coverage_count += 1
        if db_sig is not None:
            coverage_count += 1

        cfb_prod_signal_legacy = None
        if position == "QB":
            cfb_prod_signal_legacy = _weighted_mean(
                [(0.62, qb_eff_sig), (0.38, qb_pressure_sig)] if qb_eff_sig is not None and qb_pressure_sig is not None
                else [(1.0, qb_eff_sig)] if qb_eff_sig is not None
                else [(1.0, qb_pressure_sig)] if qb_pressure_sig is not None
                else []
            )
        elif position in {"WR", "TE"}:
            cfb_prod_signal_legacy = wrte_sig
        elif position == "RB":
            cfb_prod_signal_legacy = rb_sig
        elif position == "EDGE":
            cfb_prod_signal_legacy = edge_sig
        elif position in {"CB", "S"}:
            cfb_prod_signal_legacy = db_sig

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

        cfb_prod_signal, p0_diag = _apply_p0_guardrail(position, cfb_prod_signal_legacy, p0_signal)
        if p0_diag.get("cfbfastr_p0_mode") == "legacy_plus_p0_guarded":
            p0_blend_applied += 1
        elif p0_diag.get("cfbfastr_p0_mode") == "p0_only_guarded":
            p0_only_applied += 1

        cfb_source = str(row.get("source", "")).strip()
        quality_label, reliability = _quality_and_reliability(row, coverage_count, cfb_source)
        if cfb_prod_signal is not None:
            adjusted_signal = (reliability * float(cfb_prod_signal)) + ((1.0 - reliability) * 55.0)
        else:
            adjusted_signal = None

        real_features = int(_safe_float(row.get("cfb_prod_real_features")) or 0)
        proxy_features = int(_safe_float(row.get("cfb_prod_proxy_features")) or coverage_count)

        payload = {
            "cfb_prod_signal": round(adjusted_signal, 2) if adjusted_signal is not None else "",
            "cfb_prod_signal_raw": round(cfb_prod_signal_legacy, 2) if cfb_prod_signal_legacy is not None else "",
            "cfb_prod_available": 1 if cfb_prod_signal is not None else 0,
            "cfb_prod_coverage_count": coverage_count,
            "cfb_prod_quality_label": quality_label,
            "cfb_prod_reliability": reliability,
            "cfb_prod_real_features": real_features,
            "cfb_prod_proxy_features": proxy_features,
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
            "cfb_rb_explosive_signal": round(_score_linear(explosive_rate, 0.06, 0.22), 2) if explosive_rate is not None else "",
            "cfb_rb_mtf_signal": round(_score_linear(mtf, 0.08, 0.35), 2) if mtf is not None else "",
            "cfb_edge_pressure_signal": round(edge_sig, 2) if edge_sig is not None else "",
            "cfb_db_cov_plays_per_target_signal": round(db_sig, 2) if db_sig is not None else "",
            "cfb_qb_epa_per_play": round(qb_epa, 4) if qb_epa is not None else "",
            "cfb_wrte_yprr": round(yprr, 3) if yprr is not None else "",
            "cfb_wrte_target_share": round(target_share, 4) if target_share is not None else "",
            "cfb_rb_explosive_rate": round(explosive_rate, 4) if explosive_rate is not None else "",
            "cfb_rb_missed_tackles_forced_per_touch": round(mtf, 4) if mtf is not None else "",
            "cfb_edge_pressure_rate": round(pressure_rate, 4) if pressure_rate is not None else "",
            "cfb_db_coverage_plays_per_target": round(cov_plays_per_target, 4) if cov_plays_per_target is not None else "",
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
            "target_season": target_season,
            "matched_name_pos": len(by_name_pos),
            "matched_name": len(by_name),
            "cfbfastr_p0_matches": p0_matches,
            "cfbfastr_p0_blend_applied": p0_blend_applied,
            "cfbfastr_p0_only_applied": p0_only_applied,
            "cfbfastr_p0_blend_weight": P0_BLEND_WEIGHT,
            "cfbfastr_p0_max_delta": P0_MAX_DELTA,
            "cfbfastr_p0_qb_max_delta": P0_QB_MAX_DELTA,
        },
    }
