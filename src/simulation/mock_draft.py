from __future__ import annotations

import csv
import re
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
    enable_team_athletic_bias: bool,
    team_athletic_thresholds: Dict[str, dict],
) -> tuple[float, dict]:
    board_value = max(1.0, 101.0 - player["consensus_rank"]) / 100.0
    pos = player["position"]
    team_fit = (
        0.50 * need_score(team_row, pos)
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

    score = (
        0.55 * board_value
        + 0.30 * team_fit
        + 0.10 * run
        + 0.05 * scarcity
        + athletic_bias["modifier"]
    )
    return score, athletic_bias



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
) -> Tuple[List[dict], List[dict], List[dict]]:
    team_map = _team_map()
    value_chart = value_chart or {}
    team_athletic_thresholds = team_athletic_thresholds or {}
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

        candidate_pool = remaining[:45]
        scored = []
        for player in candidate_pool:
            scarcity = _scarcity_bonus(remaining, player["position"])
            score, athletic_bias = _pick_score(
                team_row,
                player,
                run_pressure,
                scarcity,
                team_code=team,
                enable_team_athletic_bias=enable_team_athletic_bias,
                team_athletic_thresholds=team_athletic_thresholds,
            )
            scored.append((score, player, athletic_bias))
        scored.sort(key=lambda x: x[0], reverse=True)

        if not scored:
            break

        _, selected, selected_athletic_bias = scored[0]
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
                "position": selected["position"],
                "school": selected["school"],
                "final_grade": selected["final_grade"],
                "round_value": selected["round_value"],
                "team_athletic_bias_enabled": int(enable_team_athletic_bias),
                "team_athletic_fit_modifier": selected_athletic_bias.get("modifier", 0.0),
                "team_athletic_target_ras": selected_athletic_bias.get("team_athletic_target_ras", ""),
                "player_athletic_proxy": selected_athletic_bias.get("player_athletic_proxy", ""),
                "player_athletic_source": selected_athletic_bias.get("player_athletic_source", ""),
                "team_athletic_tier": selected_athletic_bias.get("team_athletic_tier", ""),
                "team_athletic_threshold_mode": selected_athletic_bias.get("threshold_mode", ""),
                "team_athletic_threshold_confidence": selected_athletic_bias.get("threshold_confidence", ""),
                "team_athletic_bias_reason": selected_athletic_bias.get("reason", ""),
            }
        )

    return picks, remaining, trades



def simulate_full_draft(
    board: List[dict],
    rounds: int = 7,
    allow_simulated_trades: bool = False,
    enable_team_athletic_bias: bool = False,
) -> Tuple[List[dict], List[dict], List[dict]]:
    round_orders = load_round_orders(rounds=rounds)
    comp_picks = load_comp_picks()
    value_chart = load_draft_value_chart()
    team_athletic_thresholds = load_team_athletic_thresholds() if enable_team_athletic_bias else {}
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
        )
        if rnd == 1:
            round1_picks = picks[:]
        all_picks.extend(picks)
        all_trades.extend(trades)

    return round1_picks, all_picks, all_trades



def write_csv(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
