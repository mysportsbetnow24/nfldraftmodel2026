from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Tuple

from src.modeling.team_fit import load_team_profiles, need_score, scheme_score, gm_tendency_score


ROOT = Path(__file__).resolve().parents[2]
ROUND1_ORDER_PATH = ROOT / "data" / "sources" / "draft_order_2026_round1.csv"
TEAM_PROFILES_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"
BOARD_PATH = ROOT / "data" / "processed" / "big_board_2026.csv"
COMP_PICKS_PATH = ROOT / "data" / "sources" / "comp_picks_2026.csv"



def load_round1_order(path: Path | None = None) -> List[str]:
    path = path or ROUND1_ORDER_PATH
    with path.open() as f:
        return [row["team"] for row in csv.DictReader(f)]



def load_board(path: Path | None = None) -> List[dict]:
    path = path or BOARD_PATH
    with path.open() as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["consensus_rank"] = int(row["consensus_rank"])
        row["final_grade"] = float(row["final_grade"])
    rows.sort(key=lambda x: x["consensus_rank"])
    return rows


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



def _pos_run_pressure(remaining: List[dict], upcoming_teams: List[str], team_map: Dict[str, dict]) -> Dict[str, float]:
    pressure: Dict[str, float] = {}
    for player in remaining[:80]:
        pos = player["position"]
        demand = 0.0
        for team in upcoming_teams:
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



def _pick_score(team_row: dict, player: dict, run_pressure: Dict[str, float], scarcity: float) -> float:
    board_value = max(1.0, 101.0 - player["consensus_rank"]) / 100.0
    pos = player["position"]
    team_fit = (
        0.50 * need_score(team_row, pos)
        + 0.25 * scheme_score(team_row, pos)
        + 0.15 * 0.75
        + 0.10 * gm_tendency_score(team_row, pos)
    )
    run = min(1.0, run_pressure.get(pos, 0.15))

    return 0.55 * board_value + 0.30 * team_fit + 0.10 * run + 0.05 * scarcity



def _maybe_trade_down(order: List[str], idx: int, remaining: List[dict], team_map: Dict[str, dict]) -> Tuple[List[str], bool]:
    if idx >= len(order) - 4:
        return order, False

    team = order[idx]
    team_row = team_map[team]
    top_need = team_row["need_1"]
    top_ten_positions = {p["position"] for p in remaining[:10]}

    # Trade-down trigger: top-need not available in current tier and a QB run signal exists behind.
    qb_pressure = sum(1 for t in order[idx + 1 : idx + 6] if team_map[t]["need_1"] == "QB")
    if top_need not in top_ten_positions and qb_pressure >= 1 and idx < 20:
        new_order = order[:]
        mover = new_order.pop(idx)
        new_order.insert(min(idx + 2, len(new_order)), mover)
        return new_order, True

    return order, False



def simulate_round(order: List[str], board: List[dict], round_no: int) -> Tuple[List[dict], List[dict], List[dict]]:
    team_map = _team_map()
    picks: List[dict] = []
    trades: List[dict] = []
    remaining = board[:]

    mutable_order = order[:]
    for idx, team in enumerate(mutable_order):
        if round_no == 1:
            mutable_order, did_trade = _maybe_trade_down(mutable_order, idx, remaining, team_map)
            team = mutable_order[idx]
            if did_trade:
                trades.append(
                    {
                        "round": round_no,
                        "pick": idx + 1,
                        "team": team,
                        "trade_note": "Trade-down heuristic triggered by need/tier gap and QB pressure.",
                    }
                )

        upcoming = mutable_order[idx + 1 : idx + 9]
        run_pressure = _pos_run_pressure(remaining, upcoming, team_map)
        team_row = team_map[team]

        candidate_pool = remaining[:45]
        scored = []
        for player in candidate_pool:
            scarcity = _scarcity_bonus(remaining, player["position"])
            score = _pick_score(team_row, player, run_pressure, scarcity)
            scored.append((score, player))
        scored.sort(key=lambda x: x[0], reverse=True)

        _, selected = scored[0]
        remaining = [p for p in remaining if p["player_uid"] != selected["player_uid"]]

        picks.append(
            {
                "round": round_no,
                "pick": idx + 1,
                "overall_pick": (round_no - 1) * 32 + (idx + 1),
                "team": team,
                "player_name": selected["player_name"],
                "position": selected["position"],
                "school": selected["school"],
                "final_grade": selected["final_grade"],
                "round_value": selected["round_value"],
            }
        )

    return picks, remaining, trades



def simulate_full_draft(board: List[dict], rounds: int = 7) -> Tuple[List[dict], List[dict], List[dict]]:
    round1 = load_round1_order()
    comp_picks = load_comp_picks()
    remaining = board[:]
    all_picks: List[dict] = []
    round1_picks: List[dict] = []
    all_trades: List[dict] = []

    for rnd in range(1, rounds + 1):
        order = round1[:]
        for comp in sorted(comp_picks.get(rnd, []), key=lambda x: x["pick_after"], reverse=True):
            idx = max(0, min(comp["pick_after"], len(order)))
            order.insert(idx, comp["team"])
        picks, remaining, trades = simulate_round(order, remaining, rnd)
        if rnd == 1:
            round1_picks = picks[:]
        all_picks.extend(picks)
        all_trades.extend(trades)

    return round1_picks, all_picks, all_trades



def write_csv(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
