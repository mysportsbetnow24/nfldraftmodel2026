from __future__ import annotations

import csv
import math
import random
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

from src.modeling.team_fit import gm_tendency_score, load_team_profiles, need_score, scheme_score


ROOT = Path(__file__).resolve().parents[2]
ROUND1_ORDER_PATH = ROOT / "data" / "sources" / "draft_order_2026_round1.csv"
FULL_ORDER_PATH = ROOT / "data" / "sources" / "draft_order_2026_full.csv"
TEAM_PROFILES_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"
BOARD_PATH = ROOT / "data" / "processed" / "big_board_2026.csv"
COMP_PICKS_PATH = ROOT / "data" / "sources" / "comp_picks_2026.csv"
DRAFT_VALUES_PATH = (
    ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data" / "notebook" / "drafts" / "draft_values.csv"
)
TEAM_ATHLETIC_THRESHOLDS_PATH = ROOT / "data" / "outputs" / "team_athletic_thresholds_2026_inferred.csv"
TEAM_ATHLETIC_THRESHOLDS_BY_POS_PATH = ROOT / "data" / "outputs" / "team_athletic_thresholds_2026_by_position.csv"
NFLVERSE_PLAYERS_PATH = ROOT / "data" / "sources" / "external" / "nflverse" / "players.parquet"
TEAM_NEEDS_CONTEXT_PATH = ROOT / "data" / "sources" / "team_needs_context_2026.csv"

POSITION_ATHLETIC_BUCKET = {
    "QB": "premium",
    "OT": "premium",
    "EDGE": "premium",
    "CB": "premium",
    "WR": "mid",
    "S": "mid",
    "DT": "mid",
    "LB": "mid",
    "IOL": "low",
    "TE": "low",
    "RB": "low",
}

# Team-athletic fit should be a soft tie-breaker, not a rank rewriter.
# These settings damp low-confidence thresholds and cap max impact.
POSITION_SCALE_MIN = 0.18
POSITION_SCALE_GAIN = 0.42
POSITION_SCALE_EXP = 1.20
BUCKET_SCALE = 0.28
POSITION_CAP_BASE = 0.010
POSITION_CAP_GAIN = 0.016
BUCKET_CAP = 0.012
CURRENT_DRAFT_YEAR = 2026
RECENT_INVESTMENT_LOOKBACK_YEARS = 3
OT_VALUE_PREMIUM_EARLY = 0.040
OT_VALUE_PREMIUM_MID = 0.031
OT_VALUE_PREMIUM_LATE = 0.022
IOL_VALUE_PREMIUM_EARLY = 0.027
IOL_VALUE_PREMIUM_MID = 0.020
IOL_VALUE_PREMIUM_LATE = 0.014
QB_VALUE_PREMIUM_EARLY = 0.050
QB_VALUE_PREMIUM_MID = 0.033
QB_VALUE_PREMIUM_LATE = 0.014
SOFTMAX_MIN_TEMPERATURE = 0.03
DEFAULT_SOFTMAX_TEMPERATURE = 0.13
POSITION_VALUE_CURVE = {
    # Softly discourage low-positional-value round-1/2 over-drafts unless profile is elite.
    "RB": {"elite_grade": 89.5, "elite_rank": 12, "round_penalty": {1: 0.13, 2: 0.08, 3: 0.03}},
    "TE": {"elite_grade": 89.0, "elite_rank": 14, "round_penalty": {1: 0.10, 2: 0.06, 3: 0.02}},
    "IOL": {"elite_grade": 89.2, "elite_rank": 16, "round_penalty": {1: 0.11, 2: 0.07, 3: 0.02}},
    "LB": {"elite_grade": 89.0, "elite_rank": 16, "round_penalty": {1: 0.08, 2: 0.05, 3: 0.02}},
    "S": {"elite_grade": 89.1, "elite_rank": 14, "round_penalty": {1: 0.09, 2: 0.05, 3: 0.02}},
}

MODEL_POSITIONS = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]
POSITION_BASE_TARGET = {
    "QB": 0.1,
    "RB": 0.8,
    "WR": 1.0,
    "TE": 0.4,
    "OT": 0.8,
    "IOL": 0.8,
    "EDGE": 1.0,
    "DT": 0.8,
    "LB": 0.9,
    "CB": 1.0,
    "S": 0.8,
}
POSITION_MAX_CAP = {
    "QB": 1,
    "RB": 2,
    "WR": 3,
    "TE": 2,
    "OT": 2,
    "IOL": 2,
    "EDGE": 3,
    "DT": 2,
    "LB": 3,
    "CB": 3,
    "S": 2,
}
POSITION_ROSTER_BASELINE = {
    "QB": 3.0,
    "RB": 4.0,
    "WR": 6.0,
    "TE": 3.0,
    "OT": 4.0,
    "IOL": 5.0,
    "EDGE": 5.0,
    "DT": 5.0,
    "LB": 6.0,
    "CB": 6.0,
    "S": 5.0,
}

NFL_POS_TO_BOARD_POS = {
    "QB": "QB",
    "RB": "RB",
    "WR": "WR",
    "TE": "TE",
    "OT": "OT",
    "T": "OT",
    "OL": "OT",
    "G": "IOL",
    "OG": "IOL",
    "C": "IOL",
    "DE": "EDGE",
    "EDGE": "EDGE",
    "DL": "DT",
    "DT": "DT",
    "NT": "DT",
    "LB": "LB",
    "ILB": "LB",
    "OLB": "LB",
    "CB": "CB",
    "DB": "CB",
    "S": "S",
    "SAF": "S",
    "FS": "S",
    "SS": "S",
}



def _canon_name(name: str) -> str:
    s = (name or "").lower().strip().replace(".", "").replace("'", "")
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    return re.sub(r"\s+", " ", s)



def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _normalize_nfl_position(raw_pos: str) -> str:
    key = str(raw_pos or "").strip().upper()
    return NFL_POS_TO_BOARD_POS.get(key, "")


def _draft_capital_weight(draft_round: int, draft_pick: int) -> float:
    base = {
        1: 1.00,
        2: 0.62,
        3: 0.38,
        4: 0.22,
        5: 0.14,
        6: 0.09,
        7: 0.06,
    }.get(int(draft_round), 0.05)
    bonus = 0.0
    if int(draft_round) == 1:
        if draft_pick <= 10:
            bonus = 0.45
        elif draft_pick <= 20:
            bonus = 0.25
        elif draft_pick <= 32:
            bonus = 0.12
    elif int(draft_round) == 2 and draft_pick <= 50:
        bonus = 0.08
    return base + bonus


def load_recent_draft_investment(
    path: Path | None = None,
    *,
    current_draft_year: int = CURRENT_DRAFT_YEAR,
    lookback_years: int = RECENT_INVESTMENT_LOOKBACK_YEARS,
) -> Dict[Tuple[str, str], dict]:
    path = path or NFLVERSE_PLAYERS_PATH
    if not path.exists():
        return {}

    try:
        import polars as pl
    except Exception:
        return {}

    start_year = int(current_draft_year) - int(lookback_years)
    end_year = int(current_draft_year) - 1
    if end_year < start_year:
        return {}

    cols = ["draft_team", "draft_year", "draft_round", "draft_pick", "position"]
    df = pl.read_parquet(path).select(cols)
    df = df.drop_nulls(["draft_team", "draft_year", "draft_round", "draft_pick", "position"])
    df = df.filter((pl.col("draft_year") >= start_year) & (pl.col("draft_year") <= end_year))
    if df.height == 0:
        return {}

    out: Dict[Tuple[str, str], dict] = {}
    for row in df.iter_rows(named=True):
        team = str(row.get("draft_team") or "").strip().upper()
        pos = _normalize_nfl_position(str(row.get("position") or ""))
        if not team or not pos:
            continue

        year = int(row.get("draft_year") or 0)
        rnd = int(row.get("draft_round") or 0)
        pick = int(row.get("draft_pick") or 999)
        if year <= 0 or rnd <= 0:
            continue

        year_delta = int(current_draft_year) - year
        if year_delta <= 0 or year_delta > lookback_years:
            continue

        recency_mult = {1: 1.00, 2: 0.72, 3: 0.50}.get(year_delta, 0.40)
        weighted_capital = _draft_capital_weight(rnd, pick) * recency_mult

        key = (team, pos)
        node = out.setdefault(
            key,
            {
                "capital_score": 0.0,
                "pick_count": 0,
                "y1_r1_count": 0,
                "y1_r12_count": 0,
                "y2_r1_count": 0,
                "best_pick_recent": 999,
                "latest_year": 0,
            },
        )
        node["capital_score"] += weighted_capital
        node["pick_count"] += 1
        node["best_pick_recent"] = min(int(node["best_pick_recent"]), pick)
        node["latest_year"] = max(int(node["latest_year"]), year)
        if year_delta == 1 and rnd == 1:
            node["y1_r1_count"] += 1
        if year_delta == 1 and rnd <= 2:
            node["y1_r12_count"] += 1
        if year_delta <= 2 and rnd == 1:
            node["y2_r1_count"] += 1

    for node in out.values():
        node["capital_score"] = round(float(node["capital_score"]), 3)
    return out


def _recent_investment_modifier(
    *,
    team: str,
    position: str,
    round_no: int,
    investment_map: Dict[Tuple[str, str], dict],
) -> dict:
    neutral = {
        "modifier": 0.0,
        "capital_score": 0.0,
        "y1_r1_count": 0,
        "y1_r12_count": 0,
        "y2_r1_count": 0,
        "best_pick_recent": "",
        "reason": "none",
    }
    node = investment_map.get((str(team).upper(), str(position).upper()))
    if not node:
        return neutral

    capital_score = float(node.get("capital_score", 0.0) or 0.0)
    y1_r1 = int(node.get("y1_r1_count", 0) or 0)
    y1_r12 = int(node.get("y1_r12_count", 0) or 0)
    y2_r1 = int(node.get("y2_r1_count", 0) or 0)
    best_pick_recent = int(node.get("best_pick_recent", 999) or 999)

    penalty = 0.0
    reasons: List[str] = []
    pos = str(position).upper()

    if pos == "QB":
        if y1_r1 > 0 and best_pick_recent <= 10:
            penalty -= 0.36
            reasons.append("qb_y1_r1_top10")
        elif y1_r1 > 0:
            penalty -= 0.30
            reasons.append("qb_y1_r1")
        elif y1_r12 > 0:
            penalty -= 0.22
            reasons.append("qb_y1_r12")
        elif y2_r1 > 0:
            penalty -= 0.16
            reasons.append("qb_y2_r1")
        penalty -= min(0.14, capital_score * 0.08)
        if round_no <= 2 and y1_r12 > 0:
            penalty -= 0.04
    else:
        if y1_r1 > 0:
            penalty -= 0.12
            reasons.append("y1_r1_same_pos")
        elif y1_r12 > 0:
            penalty -= 0.08
            reasons.append("y1_r12_same_pos")
        elif y2_r1 > 0:
            penalty -= 0.05
            reasons.append("y2_r1_same_pos")
        penalty -= min(0.12, capital_score * 0.06)
        if round_no <= 2 and y1_r12 > 0:
            penalty -= 0.03

    if pos == "QB":
        penalty = max(-0.45, penalty)
    else:
        penalty = max(-0.25, penalty)

    return {
        "modifier": round(float(penalty), 4),
        "capital_score": round(capital_score, 3),
        "y1_r1_count": y1_r1,
        "y1_r12_count": y1_r12,
        "y2_r1_count": y2_r1,
        "best_pick_recent": "" if best_pick_recent >= 999 else best_pick_recent,
        "reason": "|".join(reasons) if reasons else "capital_decay_only",
    }


def _intra_draft_position_modifier(
    *,
    team: str,
    position: str,
    round_no: int,
    history: Dict[str, List[dict]],
) -> dict:
    prior = [p for p in history.get(team, []) if str(p.get("position", "")).upper() == str(position).upper()]
    count = len(prior)
    if count == 0:
        return {
            "modifier": 0.0,
            "same_pos_count_before": 0,
            "first_round_taken": "",
            "reason": "none",
        }

    pos = str(position).upper()
    first_round = min(int(p.get("round", 99) or 99) for p in prior)
    max_by_pos = {
        "QB": 1,
        "RB": 2,
        "TE": 2,
        "IOL": 2,
        "OT": 2,
        "S": 2,
        "WR": 3,
        "CB": 3,
        "EDGE": 3,
        "DT": 3,
        "LB": 3,
    }
    pos_max = int(max_by_pos.get(pos, 2))
    if count >= pos_max:
        return {
            "modifier": -0.60,
            "same_pos_count_before": count,
            "first_round_taken": first_round if first_round < 99 else "",
            "reason": f"pos_cap_reached_{pos_max}",
        }

    # Hard duplicate suppression for QB in the same mock cycle.
    if pos == "QB" and round_no <= 4:
        return {
            "modifier": -0.45,
            "same_pos_count_before": count,
            "first_round_taken": first_round if first_round < 99 else "",
            "reason": "qb_duplicate_block",
        }

    # Prevent unrealistic early duplicate classes at lower-value positions.
    if pos in {"RB", "TE"} and count >= 1 and round_no <= 3:
        return {
            "modifier": -0.48,
            "same_pos_count_before": count,
            "first_round_taken": first_round if first_round < 99 else "",
            "reason": f"{pos.lower()}_early_duplicate_block",
        }

    base = 0.09 if count == 1 else (0.17 if count == 2 else 0.24)
    round_factor = 1.5 if round_no <= 3 else (1.1 if round_no <= 5 else 0.85)
    pos_mult = {
        "QB": 3.0,
        "RB": 2.4,
        "TE": 1.8,
        "IOL": 1.6,
        "S": 1.4,
        "LB": 1.3,
        "DT": 1.2,
        "CB": 1.0,
        "WR": 1.0,
        "EDGE": 0.9,
        "OT": 0.9,
    }.get(pos, 1.1)

    penalty = -(base * round_factor * pos_mult)
    if first_round <= 2:
        penalty *= 1.35

    cap = {
        "QB": -0.45,
        "RB": -0.40,
        "TE": -0.30,
        "IOL": -0.30,
    }.get(pos, -0.26)
    if penalty < cap:
        penalty = cap

    return {
        "modifier": round(float(penalty), 4),
        "same_pos_count_before": count,
        "first_round_taken": first_round if first_round < 99 else "",
        "reason": f"dup_count_{count}_round_{round_no}",
    }


def load_team_athletic_thresholds(path: Path | None = None) -> Dict[str, dict]:
    path = path or TEAM_ATHLETIC_THRESHOLDS_PATH
    if not path.exists():
        return {}

    out: Dict[str, dict] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            team = str(row.get("team", "")).strip()
            if not team:
                continue
            out[team] = {
                "tier": row.get("ras_2021_2025_tier", "") or row.get("ras_2021_2024_tier", ""),
                "premium_min_ras": _to_float(
                    row.get("suggested_ras_threshold_premium_pos_qb_ot_edge_cb")
                ),
                "mid_min_ras": _to_float(
                    row.get("suggested_ras_threshold_mid_value_pos_wr_s_dt_lb_s")
                ),
                "low_min_ras": _to_float(
                    row.get("suggested_ras_threshold_low_value_pos_iol_te_rb")
                ),
                "by_position": {},
            }

    by_pos_path = TEAM_ATHLETIC_THRESHOLDS_BY_POS_PATH
    if by_pos_path.exists():
        with by_pos_path.open() as f:
            for row in csv.DictReader(f):
                team = str(row.get("team", "")).strip()
                pos = str(row.get("position", "")).strip().upper()
                if not team or not pos or team not in out:
                    continue
                threshold = _to_float(row.get("team_position_threshold_ras"))
                if threshold is None:
                    continue
                out[team]["by_position"][pos] = {
                    "threshold": threshold,
                    "confidence": _to_float(row.get("position_threshold_confidence_weight")),
                    "sample_n": _to_float(row.get("sample_n_2021_2025")),
                }
    return out


def _player_athletic_proxy(player: dict) -> Tuple[float | None, str]:
    ras = _to_float(player.get("ras_estimate"))
    if ras is not None and ras > 0:
        return float(ras), "ras_estimate"

    formula_ath = _to_float(player.get("formula_athletic_component"))
    if formula_ath is not None and formula_ath > 0:
        return float(formula_ath) / 10.0, "formula_athletic_component"

    athletic_score = _to_float(player.get("athletic_score"))
    if athletic_score is not None and athletic_score > 0:
        return float(athletic_score) / 10.0, "athletic_score"

    return None, "missing"


def _threshold_for_position(team_threshold_row: dict, position: str) -> tuple[float | None, float, str]:
    pos_key = str(position or "").strip().upper()
    by_position = team_threshold_row.get("by_position", {})
    if isinstance(by_position, dict):
        node = by_position.get(pos_key)
        if isinstance(node, dict):
            exact = _to_float(node.get("threshold"))
            conf = _to_float(node.get("confidence"))
            if exact is not None:
                return exact, max(0.0, min(1.0, float(conf or 0.0))), "position"
        else:
            exact = _to_float(node)
            if exact is not None:
                return exact, 0.35, "position"

    bucket = POSITION_ATHLETIC_BUCKET.get(pos_key, "mid")
    if bucket == "premium":
        return _to_float(team_threshold_row.get("premium_min_ras")), 0.30, "bucket"
    if bucket == "low":
        return _to_float(team_threshold_row.get("low_min_ras")), 0.30, "bucket"
    return _to_float(team_threshold_row.get("mid_min_ras")), 0.30, "bucket"


def _team_athletic_fit_modifier(
    *,
    enabled: bool,
    team: str,
    player: dict,
    team_thresholds: Dict[str, dict],
) -> dict:
    neutral = {
        "modifier": 0.0,
        "player_athletic_proxy": "",
        "player_athletic_source": "none",
        "team_athletic_target_ras": "",
        "team_athletic_tier": "",
        "threshold_mode": "",
        "threshold_confidence": "",
        "reason": "disabled",
    }
    if not enabled:
        return neutral

    team_row = team_thresholds.get(team)
    if not team_row:
        neutral["reason"] = "no_team_threshold_row"
        return neutral

    player_ath, ath_source = _player_athletic_proxy(player)
    threshold, threshold_conf, threshold_mode = _threshold_for_position(team_row, player.get("position", ""))
    if player_ath is None or threshold is None:
        neutral["player_athletic_source"] = ath_source
        neutral["team_athletic_tier"] = str(team_row.get("tier", ""))
        neutral["team_athletic_target_ras"] = threshold if threshold is not None else ""
        neutral["threshold_mode"] = threshold_mode
        neutral["threshold_confidence"] = threshold_conf
        neutral["reason"] = "missing_player_or_threshold"
        return neutral

    delta = float(player_ath) - float(threshold)
    if delta >= 1.0:
        modifier = 0.045
    elif delta >= 0.5:
        modifier = 0.030
    elif delta >= 0.2:
        modifier = 0.015
    elif delta > -0.2:
        modifier = 0.000
    elif delta > -0.5:
        modifier = -0.015
    elif delta > -1.0:
        modifier = -0.030
    else:
        modifier = -0.045

    # Confidence-weighted soft effect:
    # - low-confidence thresholds are damped strongly
    # - bucket fallback is always lighter
    conf = max(0.0, min(1.0, float(threshold_conf)))
    if threshold_mode == "position":
        scale = POSITION_SCALE_MIN + (POSITION_SCALE_GAIN * (conf**POSITION_SCALE_EXP))
        max_abs = POSITION_CAP_BASE + (POSITION_CAP_GAIN * conf)
    else:
        scale = BUCKET_SCALE
        max_abs = BUCKET_CAP

    applied_modifier = modifier * scale
    if applied_modifier > max_abs:
        applied_modifier = max_abs
    elif applied_modifier < -max_abs:
        applied_modifier = -max_abs

    return {
        "modifier": round(applied_modifier, 4),
        "player_athletic_proxy": round(float(player_ath), 2),
        "player_athletic_source": ath_source,
        "team_athletic_target_ras": round(float(threshold), 2),
        "team_athletic_tier": str(team_row.get("tier", "")),
        "threshold_mode": threshold_mode,
        "threshold_confidence": round(float(threshold_conf), 3),
        "reason": (
            f"{threshold_mode}_delta_{round(delta, 2)}"
            f"_scale_{round(scale, 2)}_cap_{round(max_abs, 3)}"
        ),
    }


def load_round1_order(path: Path | None = None) -> List[str]:
    path = path or ROUND1_ORDER_PATH
    with path.open() as f:
        return [row["team"] for row in csv.DictReader(f)]



def load_round_orders(rounds: int = 7, full_path: Path | None = None) -> Dict[int, List[dict]]:
    full_path = full_path or FULL_ORDER_PATH
    by_round: Dict[int, List[dict]] = {}

    if full_path.exists():
        with full_path.open() as f:
            for row in csv.DictReader(f):
                try:
                    rnd = int(row["round"])
                    pick = int(row["pick_in_round"])
                except Exception:
                    continue

                payload = {
                    "round": rnd,
                    "pick_in_round": pick,
                    "overall_pick": int(row["overall_pick"]) if row.get("overall_pick") else None,
                    "current_team": row.get("current_team", "").strip(),
                    "original_team": row.get("original_team", "").strip() or row.get("current_team", "").strip(),
                    "acquired_via": row.get("acquired_via", "").strip(),
                    "source_url": row.get("source_url", "").strip(),
                }
                if payload["current_team"]:
                    by_round.setdefault(rnd, []).append(payload)

    for rnd in range(1, rounds + 1):
        if rnd in by_round and by_round[rnd]:
            by_round[rnd] = sorted(by_round[rnd], key=lambda r: r["pick_in_round"])
            continue

        fallback = []
        for i, team in enumerate(load_round1_order(), start=1):
            fallback.append(
                {
                    "round": rnd,
                    "pick_in_round": i,
                    "overall_pick": (rnd - 1) * 32 + i,
                    "current_team": team,
                    "original_team": team,
                    "acquired_via": "",
                    "source_url": "",
                }
            )
        by_round[rnd] = fallback

    return by_round



def load_board(path: Path | None = None) -> List[dict]:
    path = path or BOARD_PATH
    with path.open() as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        row["consensus_rank"] = int(row["consensus_rank"])
        row["final_grade"] = float(row["final_grade"])

    rows.sort(key=lambda x: x["consensus_rank"])

    # Safety dedupe: canonical name + position (keep best consensus rank row)
    unique = {}
    for row in rows:
        key = (_canon_name(row["player_name"]), row["position"])
        cur = unique.get(key)
        if cur is None or row["consensus_rank"] < cur["consensus_rank"]:
            unique[key] = row

    out = list(unique.values())
    out.sort(key=lambda x: x["consensus_rank"])
    return out



def load_comp_picks(path: Path | None = None) -> Dict[int, List[dict]]:
    path = path or COMP_PICKS_PATH
    if not path.exists():
        return {}
    by_round: Dict[int, List[dict]] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            team = row.get("team", "").strip()
            if not team:
                continue
            rnd = int(row["round"])
            row["pick_after"] = int(row["pick_after"])
            by_round.setdefault(rnd, []).append(row)
    return by_round



def _team_map() -> Dict[str, dict]:
    return {row["team"]: row for row in load_team_profiles(TEAM_PROFILES_PATH)}


def _need_rank(team_row: dict, position: str) -> int:
    pos = str(position or "").upper()
    if pos == str(team_row.get("need_1", "")).upper():
        return 1
    if pos == str(team_row.get("need_2", "")).upper():
        return 2
    if pos == str(team_row.get("need_3", "")).upper():
        return 3
    return 4


def _demand_target_for_row(team_row: dict, position: str, ctx: dict) -> dict:
    pos = str(position or "").upper()
    need_rank = _need_rank(team_row, pos)
    depth = _to_float(ctx.get("depth_chart_pressure")) or 0.5
    fa = _to_float(ctx.get("free_agent_pressure")) or 0.5
    contract = _to_float(ctx.get("contract_year_pressure")) or 0.5
    cliff_1y = _to_float(ctx.get("starter_cliff_1y_pressure")) or 0.5
    cliff_2y = _to_float(ctx.get("starter_cliff_2y_pressure")) or 0.5
    future_1y = _to_float(ctx.get("future_need_pressure_1y")) or 0.5
    future_2y = _to_float(ctx.get("future_need_pressure_2y")) or 0.5
    starter = _to_float(ctx.get("starter_quality")) or 0.5
    roster_count = _to_float(ctx.get("roster_player_count"))
    if roster_count is None:
        roster_count = POSITION_ROSTER_BASELINE.get(pos, 5.0)

    pressure = max(
        0.0,
        min(
            1.0,
            (
                0.28 * depth
                + 0.16 * fa
                + 0.15 * contract
                + 0.14 * cliff_1y
                + 0.09 * cliff_2y
                + 0.12 * future_1y
                + 0.04 * future_2y
                + 0.02 * (1.0 - starter)
            ),
        ),
    )
    need_bonus = {1: 1.00, 2: 0.60, 3: 0.30}.get(need_rank, 0.0)

    baseline = POSITION_ROSTER_BASELINE.get(pos, 5.0)
    roster_shortage = max(-1.0, min(1.0, (baseline - roster_count) / max(baseline, 1.0)))

    base_target = POSITION_BASE_TARGET.get(pos, 0.7)
    target_f = (
        base_target
        + need_bonus
        + ((pressure - 0.50) * 1.20)
        + (0.60 * roster_shortage)
        + (0.12 * (future_1y - 0.5))
        + (0.08 * (future_2y - 0.5))
    )

    max_cap = POSITION_MAX_CAP.get(pos, 2)
    target = int(round(target_f))
    target = max(0, min(max_cap, target))
    if need_rank == 1:
        target = max(1, target)
    if pos == "QB":
        # Keep QB demand conservative even with noisy pressure inputs.
        target = min(1, target)
        max_cap = 1

    # Position-demand urgency by round curve:
    # need_1 should be front-loaded; lower-priority positions can wait.
    round_progress_exp = {1: 0.72, 2: 0.86, 3: 1.0}.get(need_rank, 1.14)
    urgency = 0.45 + (0.55 * pressure)

    return {
        "team": team_row.get("team", ""),
        "position": pos,
        "need_rank": need_rank,
        "pressure": round(float(pressure), 4),
        "target_total": target,
        "max_cap": max_cap,
        "round_progress_exp": round_progress_exp,
        "urgency": round(float(urgency), 4),
        "roster_count": round(float(roster_count), 2),
        "starter_quality": round(float(starter), 4),
        "future_need_1y": round(float(future_1y), 4),
        "future_need_2y": round(float(future_2y), 4),
        "cliff_1y": round(float(cliff_1y), 4),
        "cliff_2y": round(float(cliff_2y), 4),
    }


def load_team_position_demand_plan(
    team_context_path: Path | None = None,
) -> Dict[Tuple[str, str], dict]:
    team_map = _team_map()
    context_path = team_context_path or TEAM_NEEDS_CONTEXT_PATH
    out: Dict[Tuple[str, str], dict] = {}

    # Start with profile-only defaults so every team/position pair has a plan.
    for team, team_row in team_map.items():
        for pos in MODEL_POSITIONS:
            node = _demand_target_for_row(
                team_row,
                pos,
                {
                    "depth_chart_pressure": 0.5,
                    "free_agent_pressure": 0.5,
                    "contract_year_pressure": 0.5,
                    "starter_cliff_1y_pressure": 0.5,
                    "starter_cliff_2y_pressure": 0.5,
                    "future_need_pressure_1y": 0.5,
                    "future_need_pressure_2y": 0.5,
                    "starter_quality": 0.5,
                    "roster_player_count": POSITION_ROSTER_BASELINE.get(pos, 5.0),
                },
            )
            out[(team, pos)] = node

    if not context_path.exists():
        return out

    with context_path.open() as f:
        for row in csv.DictReader(f):
            team = str(row.get("team", "")).strip().upper()
            pos = str(row.get("position", "")).strip().upper()
            team_row = team_map.get(team)
            if not team_row or pos not in MODEL_POSITIONS:
                continue
            out[(team, pos)] = _demand_target_for_row(team_row, pos, row)
    return out


def _position_demand_modifier(
    *,
    team: str,
    position: str,
    round_no: int,
    history: Dict[str, List[dict]],
    demand_plan: Dict[Tuple[str, str], dict],
) -> dict:
    neutral = {
        "modifier": 0.0,
        "target_total": 0,
        "max_cap": 0,
        "picked_before": 0,
        "expected_to_date": 0.0,
        "need_rank": 4,
        "pressure": 0.5,
        "reason": "none",
    }
    team_key = str(team or "").upper()
    pos_key = str(position or "").upper()
    plan = demand_plan.get((team_key, pos_key))
    if not plan:
        return neutral

    prior = [p for p in history.get(team_key, []) if str(p.get("position", "")).upper() == pos_key]
    picked = len(prior)
    target_total = int(plan.get("target_total", 0) or 0)
    max_cap = int(plan.get("max_cap", POSITION_MAX_CAP.get(pos_key, 2)) or 0)
    need_rank = int(plan.get("need_rank", 4) or 4)
    pressure = float(plan.get("pressure", 0.5) or 0.5)
    round_progress_exp = float(plan.get("round_progress_exp", 1.0) or 1.0)
    urgency = float(plan.get("urgency", 0.7) or 0.7)

    if picked >= max_cap:
        return {
            "modifier": -0.62,
            "target_total": target_total,
            "max_cap": max_cap,
            "picked_before": picked,
            "expected_to_date": round(float(target_total), 2),
            "need_rank": need_rank,
            "pressure": round(pressure, 3),
            "reason": "demand_cap_reached",
        }

    progress = (max(1, min(7, int(round_no))) / 7.0) ** max(0.55, min(1.4, round_progress_exp))
    expected = float(target_total) * progress
    gap = expected - float(picked)

    modifier = 0.0
    reasons: List[str] = []

    if gap > 0:
        boost = min(0.12, gap * 0.055 * (0.65 + urgency))
        modifier += boost
        reasons.append("under_target")
    elif gap < 0:
        penalty = min(0.20, abs(gap) * 0.070 * (0.75 + (1.0 - pressure)))
        modifier -= penalty
        reasons.append("over_target")

    if need_rank == 1 and round_no <= 2 and picked == 0 and target_total >= 1:
        modifier += 0.05
        reasons.append("need1_early_push")
    if need_rank >= 3 and round_no <= 2 and picked >= 1:
        modifier -= 0.04
        reasons.append("low_need_early_brake")
    if round_no >= 6 and picked < target_total:
        modifier += 0.025
        reasons.append("late_round_catchup")

    modifier = max(-0.26, min(0.16, modifier))
    return {
        "modifier": round(float(modifier), 4),
        "target_total": target_total,
        "max_cap": max_cap,
        "picked_before": picked,
        "expected_to_date": round(expected, 2),
        "need_rank": need_rank,
        "pressure": round(pressure, 3),
        "reason": "|".join(reasons) if reasons else "on_track",
    }


def _qb_realism_modifier(
    *,
    team: str,
    position: str,
    round_no: int,
    demand_plan: Dict[Tuple[str, str], dict],
    investment_map: Dict[Tuple[str, str], dict],
) -> dict:
    neutral = {
        "modifier": 0.0,
        "starter_quality": "",
        "future_need_1y": "",
        "future_need_2y": "",
        "pressure": "",
        "need_rank": "",
        "y1_r1_count": 0,
        "best_pick_recent": "",
        "reason": "none",
    }
    pos = str(position or "").upper()
    if pos != "QB":
        return neutral

    team_key = str(team or "").upper()
    plan = demand_plan.get((team_key, "QB"), {}) or {}
    inv = investment_map.get((team_key, "QB"), {}) or {}

    starter_quality = float(plan.get("starter_quality", 0.5) or 0.5)
    future_need_1y = float(plan.get("future_need_1y", 0.5) or 0.5)
    future_need_2y = float(plan.get("future_need_2y", 0.5) or 0.5)
    cliff_1y = float(plan.get("cliff_1y", 0.5) or 0.5)
    pressure = float(plan.get("pressure", 0.5) or 0.5)
    need_rank = int(plan.get("need_rank", 4) or 4)

    y1_r1 = int(inv.get("y1_r1_count", 0) or 0)
    y1_r12 = int(inv.get("y1_r12_count", 0) or 0)
    y2_r1 = int(inv.get("y2_r1_count", 0) or 0)
    best_pick_recent = int(inv.get("best_pick_recent", 999) or 999)
    capital_score = float(inv.get("capital_score", 0.0) or 0.0)

    penalty = 0.0
    reasons: List[str] = []

    # Early QB re-investment should be rare after recent high-capital picks.
    if round_no <= 3:
        if y1_r1 > 0:
            p = 0.42 if round_no <= 2 else 0.30
            if best_pick_recent <= 10:
                p += 0.08
            penalty -= p
            reasons.append("recent_qb_r1_investment")
        elif y1_r12 > 0 and round_no <= 2:
            penalty -= 0.24
            reasons.append("recent_qb_day2_investment")
        elif y2_r1 > 0 and round_no <= 2:
            penalty -= 0.18
            reasons.append("recent_qb_r1_2yr")

    # If incumbent starter profile is healthy, strongly brake early QB picks.
    if round_no <= 2:
        if starter_quality >= 0.88 and future_need_1y <= 0.40 and cliff_1y <= 0.45:
            penalty -= 0.30
            reasons.append("stable_incumbent_early")
        elif starter_quality >= 0.80 and future_need_1y <= 0.45 and pressure <= 0.55:
            penalty -= 0.18
            reasons.append("viable_incumbent_early")

        if need_rank >= 2 and starter_quality >= 0.75 and future_need_1y <= 0.55:
            penalty -= 0.12
            reasons.append("qb_not_top_need_early")

    # Mild capital decay tie-breaker.
    penalty -= min(0.10, capital_score * 0.04)

    # Emergency escape hatch: allow QB when pressure is truly urgent.
    emergency = (
        need_rank == 1
        and (
            future_need_1y >= 0.82
            or cliff_1y >= 0.80
            or starter_quality <= 0.58
            or pressure >= 0.78
        )
    )
    if emergency and penalty < 0:
        penalty *= 0.35
        reasons.append("qb_emergency_escape")

    if round_no >= 4:
        penalty *= 0.55
    if round_no >= 6:
        penalty *= 0.35

    floor = -0.85 if round_no <= 2 else (-0.60 if round_no <= 3 else -0.35)
    penalty = max(floor, penalty)

    return {
        "modifier": round(float(penalty), 4),
        "starter_quality": round(float(starter_quality), 4),
        "future_need_1y": round(float(future_need_1y), 4),
        "future_need_2y": round(float(future_need_2y), 4),
        "pressure": round(float(pressure), 4),
        "need_rank": need_rank,
        "y1_r1_count": y1_r1,
        "best_pick_recent": "" if best_pick_recent >= 999 else best_pick_recent,
        "reason": "|".join(reasons) if reasons else "none",
    }


def _softmax_select(
    scored: List[tuple],
    *,
    temperature: float,
    rng: random.Random,
) -> tuple | None:
    if not scored:
        return None
    temp = max(SOFTMAX_MIN_TEMPERATURE, float(temperature))
    values = [float(row[0]) for row in scored]
    max_val = max(values)
    weights: List[float] = []
    total = 0.0
    for val in values:
        w = math.exp((val - max_val) / temp)
        weights.append(w)
        total += w
    if total <= 0:
        return scored[0]

    draw = rng.random() * total
    running = 0.0
    for idx, row in enumerate(scored):
        running += weights[idx]
        if draw <= running:
            return row
    return scored[-1]


def _position_value_curve_modifier(
    *,
    team_row: dict,
    player: dict,
    round_no: int,
) -> dict:
    pos = str(player.get("position", "")).upper()
    curve = POSITION_VALUE_CURVE.get(pos)
    neutral = {
        "modifier": 0.0,
        "reason": "none",
        "tier": "",
        "elite_gate": "",
    }
    if not curve:
        return neutral

    penalty_map = curve.get("round_penalty", {}) or {}
    base = float(penalty_map.get(int(round_no), 0.0) or 0.0)
    if base <= 0:
        return neutral

    grade = float(player.get("final_grade", 75.0) or 75.0)
    rank = int(player.get("consensus_rank", 999) or 999)
    elite_grade = float(curve.get("elite_grade", 90.0) or 90.0)
    elite_rank = int(curve.get("elite_rank", 12) or 12)

    grade_elite = grade >= elite_grade
    rank_elite = rank <= elite_rank
    if grade_elite or rank_elite:
        return {
            "modifier": round(0.008 if round_no == 1 else 0.004, 4),
            "reason": "elite_exception",
            "tier": "elite_clear",
            "elite_gate": f"grade>={elite_grade}" if grade_elite else f"rank<={elite_rank}",
        }

    grade_gap = max(0.0, elite_grade - grade)
    rank_gap = max(0.0, float(rank - elite_rank))
    grade_scale = min(1.0, grade_gap / 6.0)
    rank_scale = min(1.0, rank_gap / 35.0)
    severity = max(0.20, min(1.0, (0.60 * grade_scale) + (0.40 * rank_scale)))

    # If this is a top need, keep the curve as a brake, not a block.
    need_val = need_score(team_row, pos)
    need_mult = 0.70 if need_val >= 0.99 else (0.82 if need_val >= 0.70 else 1.0)
    penalty = -(base * severity * need_mult)

    tier = "mild" if severity < 0.45 else ("moderate" if severity < 0.75 else "strong")
    return {
        "modifier": round(float(penalty), 4),
        "reason": f"round{round_no}_curve",
        "tier": tier,
        "elite_gate": f"grade<{elite_grade}&rank>{elite_rank}",
    }


def _top_pick_drivers(
    *,
    need_component: float,
    value_component: float,
    guardrail_component: float,
    investment_component: float,
    athletic_component: float,
    need_reason: str,
    value_reason: str,
    guardrail_reason: str,
    investment_reason: str,
    athletic_reason: str,
) -> list[dict]:
    rows = [
        {"code": "need", "value": float(need_component), "reason": str(need_reason or "")},
        {"code": "value", "value": float(value_component), "reason": str(value_reason or "")},
        {"code": "guardrail", "value": float(guardrail_component), "reason": str(guardrail_reason or "")},
        {"code": "investment", "value": float(investment_component), "reason": str(investment_reason or "")},
        {"code": "athletic_fit", "value": float(athletic_component), "reason": str(athletic_reason or "")},
    ]
    rows.sort(key=lambda r: abs(float(r.get("value", 0.0))), reverse=True)
    out = []
    for r in rows[:5]:
        out.append(
            {
                "code": str(r["code"]),
                "value": round(float(r["value"]), 4),
                "reason": str(r["reason"]),
            }
        )
    return out



def load_draft_value_chart(path: Path | None = None) -> Dict[int, float]:
    path = path or DRAFT_VALUES_PATH
    if not path.exists():
        return {}

    out: Dict[int, float] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            try:
                pick = int(float(row.get("pick", "") or 0))
            except Exception:
                continue
            if pick <= 0:
                continue

            def _as_float(key: str) -> float | None:
                txt = str(row.get(key, "")).strip()
                if not txt:
                    return None
                try:
                    return float(txt)
                except ValueError:
                    return None

            otc = _as_float("otc")
            johnson = _as_float("johnson")
            hill = _as_float("hill")
            pff = _as_float("pff")
            parts: List[Tuple[float, float]] = []
            if otc is not None:
                parts.append((0.50, otc))
            if johnson is not None:
                parts.append((0.25, johnson))
            if hill is not None:
                parts.append((0.15, hill))
            if pff is not None:
                parts.append((0.10, pff))
            if not parts:
                continue
            num = sum(w * v for w, v in parts)
            den = sum(w for w, _ in parts)
            if den <= 0:
                continue
            out[pick] = num / den
    return out


def _value_for_pick(value_chart: Dict[int, float], pick: int | None) -> float:
    if not value_chart or pick is None or pick <= 0:
        return 0.0
    if pick in value_chart:
        return float(value_chart[pick])
    nearest = min(value_chart.keys(), key=lambda p: abs(int(pick) - p))
    return float(value_chart.get(nearest, 0.0))


def _pos_run_pressure(remaining: List[dict], upcoming_teams: List[str], team_map: Dict[str, dict]) -> Dict[str, float]:
    pressure: Dict[str, float] = {}
    for player in remaining[:80]:
        pos = player["position"]
        demand = 0.0
        for team in upcoming_teams:
            if team not in team_map:
                continue
            demand += need_score(team_map[team], pos)
        pressure[pos] = max(pressure.get(pos, 0.0), demand / max(len(upcoming_teams), 1))
    return pressure



def _scarcity_bonus(remaining: List[dict], position: str) -> float:
    top_pos = [r for r in remaining[:60] if r["position"] == position]
    if len(top_pos) <= 2:
        return 0.9
    if len(top_pos) <= 4:
        return 0.6
    if len(top_pos) <= 7:
        return 0.3
    return 0.0



def _pick_score(
    team_row: dict,
    player: dict,
    run_pressure: Dict[str, float],
    scarcity: float,
    *,
    team_code: str,
    round_no: int,
    enable_team_athletic_bias: bool,
    team_athletic_thresholds: Dict[str, dict],
    recent_draft_investment: Dict[Tuple[str, str], dict],
    team_position_demand_plan: Dict[Tuple[str, str], dict],
    draft_history: Dict[str, List[dict]],
) -> tuple[float, dict, dict, dict, dict, dict, dict, float, list[dict]]:
    board_value = max(1.0, 101.0 - player["consensus_rank"]) / 100.0
    pos = player["position"]
    need_val = need_score(team_row, pos)
    gm_profile = str(team_row.get("gm_profile", "")).strip().lower()
    team_fit = (
        0.50 * need_val
        + 0.25 * scheme_score(team_row, pos)
        + 0.15 * 0.75
        + 0.10 * gm_tendency_score(team_row, pos)
    )
    run = min(1.0, run_pressure.get(pos, 0.15))
    athletic_bias = _team_athletic_fit_modifier(
        enabled=enable_team_athletic_bias,
        team=team_code,
        player=player,
        team_thresholds=team_athletic_thresholds,
    )
    investment_bias = _recent_investment_modifier(
        team=team_code,
        position=pos,
        round_no=round_no,
        investment_map=recent_draft_investment,
    )
    intra_draft_bias = _intra_draft_position_modifier(
        team=team_code,
        position=pos,
        round_no=round_no,
        history=draft_history,
    )
    demand_bias = _position_demand_modifier(
        team=team_code,
        position=pos,
        round_no=round_no,
        history=draft_history,
        demand_plan=team_position_demand_plan,
    )
    qb_realism_bias = _qb_realism_modifier(
        team=team_code,
        position=pos,
        round_no=round_no,
        demand_plan=team_position_demand_plan,
        investment_map=recent_draft_investment,
    )
    value_curve_bias = _position_value_curve_modifier(
        team_row=team_row,
        player=player,
        round_no=round_no,
    )
    demand_node = team_position_demand_plan.get((str(team_code).upper(), str(pos).upper()), {}) or {}
    position_value_modifier = 0.0
    if pos == "OT":
        if round_no <= 2:
            base = OT_VALUE_PREMIUM_EARLY
        elif round_no <= 4:
            base = OT_VALUE_PREMIUM_MID
        else:
            base = OT_VALUE_PREMIUM_LATE
        need_factor = 0.35 + (0.65 * float(need_val))
        trenches_mult = 1.08 if "trenches" in gm_profile else 1.0
        position_value_modifier = base * need_factor * trenches_mult
    elif pos == "IOL":
        if round_no <= 2:
            base = IOL_VALUE_PREMIUM_EARLY
        elif round_no <= 4:
            base = IOL_VALUE_PREMIUM_MID
        else:
            base = IOL_VALUE_PREMIUM_LATE
        need_factor = 0.35 + (0.65 * float(need_val))
        trenches_mult = 1.10 if "trenches" in gm_profile else 1.0
        position_value_modifier = base * need_factor * trenches_mult
    elif pos == "QB":
        if round_no <= 2:
            base = QB_VALUE_PREMIUM_EARLY
        elif round_no <= 4:
            base = QB_VALUE_PREMIUM_MID
        else:
            base = QB_VALUE_PREMIUM_LATE
        need_factor = 0.30 + (0.70 * float(need_val))
        starter_quality = float(demand_node.get("starter_quality", 0.5) or 0.5)
        future_need_1y = float(demand_node.get("future_need_1y", 0.5) or 0.5)
        pressure = float(demand_node.get("pressure", 0.5) or 0.5)
        urgency = max(0.0, min(1.0, 0.45 * future_need_1y + 0.35 * pressure + 0.20 * (1.0 - starter_quality)))
        position_value_modifier = base * (0.80 + 0.60 * urgency) * need_factor

    score = (
        0.55 * board_value
        + 0.30 * team_fit
        + 0.10 * run
        + 0.05 * scarcity
        + athletic_bias["modifier"]
        + investment_bias["modifier"]
        + intra_draft_bias["modifier"]
        + demand_bias["modifier"]
        + qb_realism_bias["modifier"]
        + value_curve_bias["modifier"]
        + position_value_modifier
    )
    need_component = (0.30 * team_fit) + float(demand_bias.get("modifier", 0.0) or 0.0)
    value_component = (
        (0.55 * board_value)
        + (0.10 * run)
        + (0.05 * scarcity)
        + float(position_value_modifier)
    )
    guardrail_component = (
        float(intra_draft_bias.get("modifier", 0.0) or 0.0)
        + float(qb_realism_bias.get("modifier", 0.0) or 0.0)
        + float(value_curve_bias.get("modifier", 0.0) or 0.0)
    )
    investment_component = float(investment_bias.get("modifier", 0.0) or 0.0)
    athletic_component = float(athletic_bias.get("modifier", 0.0) or 0.0)
    top_drivers = _top_pick_drivers(
        need_component=need_component,
        value_component=value_component,
        guardrail_component=guardrail_component,
        investment_component=investment_component,
        athletic_component=athletic_component,
        need_reason=f"need={round(float(need_val),3)}|demand={demand_bias.get('reason','')}",
        value_reason=f"board={round(float(board_value),3)}|run={round(float(run),3)}|scarcity={round(float(scarcity),3)}",
        guardrail_reason="",
        investment_reason=str(investment_bias.get("reason", "")),
        athletic_reason=str(athletic_bias.get("reason", "")),
    )
    # Patch guardrail reason cleanly without constructing it in f-string logic above.
    if top_drivers:
        for row in top_drivers:
            if row.get("code") == "guardrail":
                row["reason"] = (
                    f"intra={intra_draft_bias.get('reason','')}"
                    f"|curve={value_curve_bias.get('reason','')}"
                    f"|qb={qb_realism_bias.get('reason','')}"
                )

    return (
        score,
        athletic_bias,
        investment_bias,
        intra_draft_bias,
        demand_bias,
        qb_realism_bias,
        value_curve_bias,
        round(float(position_value_modifier), 4),
        top_drivers,
    )



def _maybe_trade_down(
    order_rows: List[dict],
    idx: int,
    remaining: List[dict],
    team_map: Dict[str, dict],
    value_chart: Dict[int, float],
) -> Tuple[List[dict], bool, dict]:
    if idx >= len(order_rows) - 4:
        return order_rows, False, {}

    current_team = order_rows[idx]["current_team"]
    if current_team not in team_map:
        return order_rows, False, {}

    team_row = team_map[current_team]
    top_need = team_row["need_1"]
    top_ten_positions = {p["position"] for p in remaining[:10]}

    qb_pressure = 0
    for later in order_rows[idx + 1 : idx + 6]:
        t = later["current_team"]
        if t in team_map and team_map[t]["need_1"] == "QB":
            qb_pressure += 1

    if top_need not in top_ten_positions and qb_pressure >= 1 and idx < 20:
        current_pick = order_rows[idx].get("overall_pick")
        down_to_idx = min(idx + 2, len(order_rows) - 1)
        down_to_pick = order_rows[down_to_idx].get("overall_pick")
        if current_pick in (None, ""):
            current_pick = idx + 1
        if down_to_pick in (None, ""):
            down_to_pick = down_to_idx + 1

        value_out = _value_for_pick(value_chart, int(current_pick))
        value_in_now = _value_for_pick(value_chart, int(down_to_pick))
        # Proxy future compensation when a team pays to move up.
        future_pick = min(262, int(down_to_pick) + 40)
        future_val = _value_for_pick(value_chart, future_pick) * (0.55 if qb_pressure >= 2 else 0.45)
        deal_in = value_in_now + future_val
        fairness = (deal_in / value_out) if value_out > 0 else 1.0
        if fairness < 0.98:
            return order_rows, False, {}

        new_order = order_rows[:]
        mover = new_order.pop(idx)
        new_order.insert(down_to_idx, mover)
        return new_order, True, {
            "value_out": round(value_out, 2),
            "value_in_now": round(value_in_now, 2),
            "value_in_future_proxy": round(future_val, 2),
            "fairness_ratio": round(fairness, 3),
            "from_pick": int(current_pick),
            "to_pick": int(down_to_pick),
        }

    return order_rows, False, {}



def _insert_comp_picks(order_rows: List[dict], round_no: int, comp_picks: Dict[int, List[dict]]) -> List[dict]:
    rows = order_rows[:]
    for comp in sorted(comp_picks.get(round_no, []), key=lambda x: x["pick_after"], reverse=True):
        idx = max(0, min(comp["pick_after"], len(rows)))
        rows.insert(
            idx,
            {
                "round": round_no,
                "pick_in_round": idx + 1,
                "overall_pick": None,
                "current_team": comp["team"],
                "original_team": comp["team"],
                "acquired_via": comp.get("comp_reason", "Comp pick"),
                "source_url": "",
            },
        )

    # normalize pick numbers after insertion
    for i, row in enumerate(rows, start=1):
        row["pick_in_round"] = i
    return rows



def simulate_round(
    order_rows: List[dict],
    board: List[dict],
    round_no: int,
    allow_simulated_trades: bool = False,
    value_chart: Dict[int, float] | None = None,
    enable_team_athletic_bias: bool = False,
    team_athletic_thresholds: Dict[str, dict] | None = None,
    recent_draft_investment: Dict[Tuple[str, str], dict] | None = None,
    team_position_demand_plan: Dict[Tuple[str, str], dict] | None = None,
    draft_history: Dict[str, List[dict]] | None = None,
    selection_mode: str = "top",
    softmax_temperature: float = DEFAULT_SOFTMAX_TEMPERATURE,
    rng: random.Random | None = None,
) -> Tuple[List[dict], List[dict], List[dict]]:
    team_map = _team_map()
    value_chart = value_chart or {}
    if team_athletic_thresholds is None:
        team_athletic_thresholds = {}
    if recent_draft_investment is None:
        recent_draft_investment = {}
    if team_position_demand_plan is None:
        team_position_demand_plan = {}
    if draft_history is None:
        draft_history = {}
    if rng is None:
        rng = random.Random(2026 + round_no)
    picks: List[dict] = []
    trades: List[dict] = []
    remaining = board[:]

    mutable_order = order_rows[:]
    for idx in range(len(mutable_order)):
        if round_no == 1 and allow_simulated_trades:
            mutable_order, did_trade, trade_meta = _maybe_trade_down(
                mutable_order, idx, remaining, team_map, value_chart
            )
            if did_trade:
                trades.append(
                    {
                        "round": round_no,
                        "pick": idx + 1,
                        "team": mutable_order[idx]["current_team"],
                        "trade_note": (
                            "Trade-down heuristic triggered by need/tier gap + QB pressure + blended draft-value fairness."
                        ),
                        "trade_value_out": trade_meta.get("value_out", ""),
                        "trade_value_in_now": trade_meta.get("value_in_now", ""),
                        "trade_value_in_future_proxy": trade_meta.get("value_in_future_proxy", ""),
                        "trade_fairness_ratio": trade_meta.get("fairness_ratio", ""),
                        "trade_from_pick": trade_meta.get("from_pick", ""),
                        "trade_to_pick": trade_meta.get("to_pick", ""),
                    }
                )

        pick_row = mutable_order[idx]
        team = pick_row["current_team"]
        if team not in team_map:
            continue

        upcoming = [r["current_team"] for r in mutable_order[idx + 1 : idx + 9]]
        run_pressure = _pos_run_pressure(remaining, upcoming, team_map)
        team_row = team_map[team]

        candidate_pool = remaining[:60]
        scored = []
        for player in candidate_pool:
            scarcity = _scarcity_bonus(remaining, player["position"])
            score, athletic_bias, investment_bias, intra_draft_bias, demand_bias, qb_realism_bias, value_curve_bias, position_value_modifier, top_drivers = _pick_score(
                team_row,
                player,
                run_pressure,
                scarcity,
                team_code=team,
                round_no=round_no,
                enable_team_athletic_bias=enable_team_athletic_bias,
                team_athletic_thresholds=team_athletic_thresholds,
                recent_draft_investment=recent_draft_investment,
                team_position_demand_plan=team_position_demand_plan,
                draft_history=draft_history,
            )
            scored.append(
                (
                    score,
                    player,
                    athletic_bias,
                    investment_bias,
                    intra_draft_bias,
                    demand_bias,
                    qb_realism_bias,
                    value_curve_bias,
                    position_value_modifier,
                    top_drivers,
                )
            )
        scored.sort(key=lambda x: x[0], reverse=True)

        if not scored:
            break

        selected_row = scored[0]
        if str(selection_mode).lower() == "sample":
            sampled = _softmax_select(scored, temperature=softmax_temperature, rng=rng)
            if sampled is not None:
                selected_row = sampled

        (
            selected_pick_score,
            selected,
            selected_athletic_bias,
            selected_investment_bias,
            selected_intra_draft_bias,
            selected_demand_bias,
            selected_qb_realism_bias,
            selected_value_curve_bias,
            selected_position_value_modifier,
            selected_top_drivers,
        ) = selected_row
        remaining = [p for p in remaining if p["player_uid"] != selected["player_uid"]]

        overall_pick = pick_row.get("overall_pick")
        if overall_pick in (None, ""):
            overall_pick = (round_no - 1) * 32 + (idx + 1)

        picks.append(
            {
                "round": round_no,
                "pick": idx + 1,
                "overall_pick": int(overall_pick),
                "team": team,
                "original_pick_owner": pick_row.get("original_team", team),
                "acquired_via": pick_row.get("acquired_via", ""),
                "player_name": selected["player_name"],
                "player_uid": selected.get("player_uid", ""),
                "position": selected["position"],
                "school": selected["school"],
                "final_grade": selected["final_grade"],
                "round_value": selected["round_value"],
                "pick_score": round(float(selected_pick_score), 4),
                "selection_mode": str(selection_mode).lower(),
                "softmax_temperature": round(float(softmax_temperature), 3) if str(selection_mode).lower() == "sample" else "",
                "team_athletic_bias_enabled": int(enable_team_athletic_bias),
                "team_athletic_fit_modifier": selected_athletic_bias.get("modifier", 0.0),
                "team_athletic_target_ras": selected_athletic_bias.get("team_athletic_target_ras", ""),
                "player_athletic_proxy": selected_athletic_bias.get("player_athletic_proxy", ""),
                "player_athletic_source": selected_athletic_bias.get("player_athletic_source", ""),
                "team_athletic_tier": selected_athletic_bias.get("team_athletic_tier", ""),
                "team_athletic_threshold_mode": selected_athletic_bias.get("threshold_mode", ""),
                "team_athletic_threshold_confidence": selected_athletic_bias.get("threshold_confidence", ""),
                "team_athletic_bias_reason": selected_athletic_bias.get("reason", ""),
                "recent_pos_investment_modifier": selected_investment_bias.get("modifier", 0.0),
                "recent_pos_investment_capital_score": selected_investment_bias.get("capital_score", 0.0),
                "recent_pos_investment_y1_r1_count": selected_investment_bias.get("y1_r1_count", 0),
                "recent_pos_investment_y1_r12_count": selected_investment_bias.get("y1_r12_count", 0),
                "recent_pos_investment_y2_r1_count": selected_investment_bias.get("y2_r1_count", 0),
                "recent_pos_investment_best_pick_recent": selected_investment_bias.get("best_pick_recent", ""),
                "recent_pos_investment_reason": selected_investment_bias.get("reason", ""),
                "intra_draft_pos_modifier": selected_intra_draft_bias.get("modifier", 0.0),
                "intra_draft_pos_count_before": selected_intra_draft_bias.get("same_pos_count_before", 0),
                "intra_draft_pos_first_round_taken": selected_intra_draft_bias.get("first_round_taken", ""),
                "intra_draft_pos_reason": selected_intra_draft_bias.get("reason", ""),
                "position_demand_modifier": selected_demand_bias.get("modifier", 0.0),
                "position_demand_target_total": selected_demand_bias.get("target_total", 0),
                "position_demand_expected_to_date": selected_demand_bias.get("expected_to_date", 0.0),
                "position_demand_picked_before": selected_demand_bias.get("picked_before", 0),
                "position_demand_max_cap": selected_demand_bias.get("max_cap", 0),
                "position_demand_need_rank": selected_demand_bias.get("need_rank", 4),
                "position_demand_pressure": selected_demand_bias.get("pressure", 0.5),
                "position_demand_reason": selected_demand_bias.get("reason", ""),
                "qb_realism_modifier": selected_qb_realism_bias.get("modifier", 0.0),
                "qb_realism_starter_quality": selected_qb_realism_bias.get("starter_quality", ""),
                "qb_realism_future_need_1y": selected_qb_realism_bias.get("future_need_1y", ""),
                "qb_realism_future_need_2y": selected_qb_realism_bias.get("future_need_2y", ""),
                "qb_realism_pressure": selected_qb_realism_bias.get("pressure", ""),
                "qb_realism_need_rank": selected_qb_realism_bias.get("need_rank", ""),
                "qb_realism_y1_r1_count": selected_qb_realism_bias.get("y1_r1_count", 0),
                "qb_realism_best_pick_recent": selected_qb_realism_bias.get("best_pick_recent", ""),
                "qb_realism_reason": selected_qb_realism_bias.get("reason", ""),
                "position_value_curve_modifier": selected_value_curve_bias.get("modifier", 0.0),
                "position_value_curve_reason": selected_value_curve_bias.get("reason", ""),
                "position_value_curve_tier": selected_value_curve_bias.get("tier", ""),
                "position_value_curve_elite_gate": selected_value_curve_bias.get("elite_gate", ""),
                "position_value_modifier": selected_position_value_modifier,
                "pick_driver_top5": "|".join(
                    [
                        f"{d.get('code','')}:{float(d.get('value',0.0)):+.4f}:{d.get('reason','')}"
                        for d in (selected_top_drivers or [])
                    ]
                ),
                "pick_driver_1_code": (selected_top_drivers[0].get("code", "") if len(selected_top_drivers) > 0 else ""),
                "pick_driver_1_value": (selected_top_drivers[0].get("value", "") if len(selected_top_drivers) > 0 else ""),
                "pick_driver_1_reason": (selected_top_drivers[0].get("reason", "") if len(selected_top_drivers) > 0 else ""),
                "pick_driver_2_code": (selected_top_drivers[1].get("code", "") if len(selected_top_drivers) > 1 else ""),
                "pick_driver_2_value": (selected_top_drivers[1].get("value", "") if len(selected_top_drivers) > 1 else ""),
                "pick_driver_2_reason": (selected_top_drivers[1].get("reason", "") if len(selected_top_drivers) > 1 else ""),
                "pick_driver_3_code": (selected_top_drivers[2].get("code", "") if len(selected_top_drivers) > 2 else ""),
                "pick_driver_3_value": (selected_top_drivers[2].get("value", "") if len(selected_top_drivers) > 2 else ""),
                "pick_driver_3_reason": (selected_top_drivers[2].get("reason", "") if len(selected_top_drivers) > 2 else ""),
                "pick_driver_4_code": (selected_top_drivers[3].get("code", "") if len(selected_top_drivers) > 3 else ""),
                "pick_driver_4_value": (selected_top_drivers[3].get("value", "") if len(selected_top_drivers) > 3 else ""),
                "pick_driver_4_reason": (selected_top_drivers[3].get("reason", "") if len(selected_top_drivers) > 3 else ""),
                "pick_driver_5_code": (selected_top_drivers[4].get("code", "") if len(selected_top_drivers) > 4 else ""),
                "pick_driver_5_value": (selected_top_drivers[4].get("value", "") if len(selected_top_drivers) > 4 else ""),
                "pick_driver_5_reason": (selected_top_drivers[4].get("reason", "") if len(selected_top_drivers) > 4 else ""),
            }
        )
        draft_history.setdefault(team, []).append(
            {
                "round": int(round_no),
                "overall_pick": int(overall_pick),
                "position": selected["position"],
                "player_name": selected["player_name"],
            }
        )

    return picks, remaining, trades



def simulate_full_draft(
    board: List[dict],
    rounds: int = 7,
    allow_simulated_trades: bool = False,
    enable_team_athletic_bias: bool = False,
    selection_mode: str = "top",
    softmax_temperature: float = DEFAULT_SOFTMAX_TEMPERATURE,
    random_seed: int = 2026,
    round_orders: Dict[int, List[dict]] | None = None,
    comp_picks: Dict[int, List[dict]] | None = None,
    value_chart: Dict[int, float] | None = None,
    team_athletic_thresholds: Dict[str, dict] | None = None,
    recent_draft_investment: Dict[Tuple[str, str], dict] | None = None,
    team_position_demand_plan: Dict[Tuple[str, str], dict] | None = None,
) -> Tuple[List[dict], List[dict], List[dict]]:
    round_orders = round_orders or load_round_orders(rounds=rounds)
    comp_picks = comp_picks if comp_picks is not None else load_comp_picks()
    value_chart = value_chart if value_chart is not None else load_draft_value_chart()
    if enable_team_athletic_bias:
        team_athletic_thresholds = (
            team_athletic_thresholds
            if team_athletic_thresholds is not None
            else load_team_athletic_thresholds()
        )
    else:
        team_athletic_thresholds = {}
    recent_draft_investment = (
        recent_draft_investment
        if recent_draft_investment is not None
        else load_recent_draft_investment()
    )
    team_position_demand_plan = (
        team_position_demand_plan
        if team_position_demand_plan is not None
        else load_team_position_demand_plan()
    )
    draft_history: Dict[str, List[dict]] = {}
    rng = random.Random(int(random_seed))
    remaining = board[:]
    all_picks: List[dict] = []
    round1_picks: List[dict] = []
    all_trades: List[dict] = []

    for rnd in range(1, rounds + 1):
        order_rows = _insert_comp_picks(round_orders[rnd], rnd, comp_picks)
        picks, remaining, trades = simulate_round(
            order_rows,
            remaining,
            rnd,
            allow_simulated_trades=allow_simulated_trades,
            value_chart=value_chart,
            enable_team_athletic_bias=enable_team_athletic_bias,
            team_athletic_thresholds=team_athletic_thresholds,
            recent_draft_investment=recent_draft_investment,
            team_position_demand_plan=team_position_demand_plan,
            draft_history=draft_history,
            selection_mode=selection_mode,
            softmax_temperature=softmax_temperature,
            rng=rng,
        )
        if rnd == 1:
            round1_picks = picks[:]
        all_picks.extend(picks)
        all_trades.extend(trades)

    return round1_picks, all_picks, all_trades



def simulate_full_draft_monte_carlo(
    board: List[dict],
    *,
    rounds: int = 7,
    simulations: int = 1000,
    allow_simulated_trades: bool = False,
    enable_team_athletic_bias: bool = False,
    softmax_temperature: float = DEFAULT_SOFTMAX_TEMPERATURE,
    random_seed: int = 2026,
) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
    sims = max(1, int(simulations))
    round_orders = load_round_orders(rounds=rounds)
    comp_picks = load_comp_picks()
    value_chart = load_draft_value_chart()
    team_athletic_thresholds = load_team_athletic_thresholds() if enable_team_athletic_bias else {}
    recent_draft_investment = load_recent_draft_investment()
    team_position_demand_plan = load_team_position_demand_plan()

    player_meta: Dict[str, dict] = {}
    player_picks: Dict[str, List[int]] = defaultdict(list)
    player_round1_hits: Counter = Counter()
    player_top50_hits: Counter = Counter()
    player_team_counts: Dict[str, Counter] = defaultdict(Counter)

    for sim_idx in range(sims):
        sim_seed = int(random_seed) + sim_idx
        _, full7, _ = simulate_full_draft(
            board,
            rounds=rounds,
            allow_simulated_trades=allow_simulated_trades,
            enable_team_athletic_bias=enable_team_athletic_bias,
            selection_mode="sample",
            softmax_temperature=softmax_temperature,
            random_seed=sim_seed,
            round_orders=round_orders,
            comp_picks=comp_picks,
            value_chart=value_chart,
            team_athletic_thresholds=team_athletic_thresholds,
            recent_draft_investment=recent_draft_investment,
            team_position_demand_plan=team_position_demand_plan,
        )
        for pick in full7:
            uid = str(pick.get("player_uid") or "").strip()
            if not uid:
                uid = f"{_canon_name(str(pick.get('player_name', '')))}|{str(pick.get('position', '')).upper()}"
            overall = int(pick.get("overall_pick", 999) or 999)
            player_meta.setdefault(
                uid,
                {
                    "player_uid": uid,
                    "player_name": pick.get("player_name", ""),
                    "position": pick.get("position", ""),
                    "school": pick.get("school", ""),
                },
            )
            player_picks[uid].append(overall)
            if overall <= 32:
                player_round1_hits[uid] += 1
            if overall <= 50:
                player_top50_hits[uid] += 1
            player_team_counts[uid][str(pick.get("team", ""))] += 1

    median_pick_map: Dict[str, float] = {}
    summary_rows: List[dict] = []
    for uid, picks in player_picks.items():
        if not picks:
            continue
        sorted_picks = sorted(picks)
        med = float(statistics.median(sorted_picks))
        median_pick_map[uid] = med
        mean_pick = float(statistics.mean(sorted_picks))
        variance = float(statistics.pvariance(sorted_picks)) if len(sorted_picks) > 1 else 0.0
        std_dev = variance ** 0.5
        drafted_rate = len(sorted_picks) / sims
        round1_rate = player_round1_hits[uid] / sims
        top50_rate = player_top50_hits[uid] / sims
        team_counter = player_team_counts.get(uid, Counter())
        top_team = ""
        top_team_rate = 0.0
        if team_counter:
            top_team, top_count = team_counter.most_common(1)[0]
            top_team_rate = top_count / max(1, len(sorted_picks))
        meta = player_meta.get(uid, {})
        summary_rows.append(
            {
                "player_uid": uid,
                "player_name": meta.get("player_name", ""),
                "position": meta.get("position", ""),
                "school": meta.get("school", ""),
                "sim_drafted_count": len(sorted_picks),
                "sim_drafted_rate": round(drafted_rate, 4),
                "median_pick": round(med, 2),
                "mean_pick": round(mean_pick, 2),
                "pick_variance": round(variance, 3),
                "pick_std_dev": round(std_dev, 3),
                "best_pick": min(sorted_picks),
                "worst_pick": max(sorted_picks),
                "round1_rate": round(round1_rate, 4),
                "top50_rate": round(top50_rate, 4),
                "most_common_team": top_team,
                "most_common_team_share": round(top_team_rate, 4),
            }
        )
    summary_rows.sort(
        key=lambda r: (
            float(r.get("median_pick", 9999)),
            -float(r.get("sim_drafted_rate", 0.0)),
            str(r.get("player_name", "")),
        )
    )

    # Pass 2: choose the most "median-like" sampled class for coherent output.
    best_score = float("inf")
    best_round1: List[dict] = []
    best_full7: List[dict] = []
    best_trades: List[dict] = []
    for sim_idx in range(sims):
        sim_seed = int(random_seed) + sim_idx
        round1, full7, trades = simulate_full_draft(
            board,
            rounds=rounds,
            allow_simulated_trades=allow_simulated_trades,
            enable_team_athletic_bias=enable_team_athletic_bias,
            selection_mode="sample",
            softmax_temperature=softmax_temperature,
            random_seed=sim_seed,
            round_orders=round_orders,
            comp_picks=comp_picks,
            value_chart=value_chart,
            team_athletic_thresholds=team_athletic_thresholds,
            recent_draft_investment=recent_draft_investment,
            team_position_demand_plan=team_position_demand_plan,
        )
        deltas: List[float] = []
        for pick in full7:
            uid = str(pick.get("player_uid") or "").strip()
            if not uid:
                uid = f"{_canon_name(str(pick.get('player_name', '')))}|{str(pick.get('position', '')).upper()}"
            med = median_pick_map.get(uid)
            if med is None:
                continue
            deltas.append(abs(float(pick.get("overall_pick", 999) or 999) - med))
        score = float(statistics.mean(deltas)) if deltas else float("inf")
        if score < best_score:
            best_score = score
            best_round1 = round1
            best_full7 = full7
            best_trades = trades

    for row in summary_rows:
        row["representative_class_distance"] = round(best_score, 3)
        row["simulations"] = sims
        row["softmax_temperature"] = round(float(softmax_temperature), 3)

    return best_round1, best_full7, best_trades, summary_rows


def write_csv(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
