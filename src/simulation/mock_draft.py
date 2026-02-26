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



def _canon_name(name: str) -> str:
    s = (name or "").lower().strip().replace(".", "").replace("'", "")
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    return re.sub(r"\s+", " ", s)



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



def _maybe_trade_down(order_rows: List[dict], idx: int, remaining: List[dict], team_map: Dict[str, dict]) -> Tuple[List[dict], bool]:
    if idx >= len(order_rows) - 4:
        return order_rows, False

    current_team = order_rows[idx]["current_team"]
    if current_team not in team_map:
        return order_rows, False

    team_row = team_map[current_team]
    top_need = team_row["need_1"]
    top_ten_positions = {p["position"] for p in remaining[:10]}

    qb_pressure = 0
    for later in order_rows[idx + 1 : idx + 6]:
        t = later["current_team"]
        if t in team_map and team_map[t]["need_1"] == "QB":
            qb_pressure += 1

    if top_need not in top_ten_positions and qb_pressure >= 1 and idx < 20:
        new_order = order_rows[:]
        mover = new_order.pop(idx)
        new_order.insert(min(idx + 2, len(new_order)), mover)
        return new_order, True

    return order_rows, False



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
) -> Tuple[List[dict], List[dict], List[dict]]:
    team_map = _team_map()
    picks: List[dict] = []
    trades: List[dict] = []
    remaining = board[:]

    mutable_order = order_rows[:]
    for idx in range(len(mutable_order)):
        if round_no == 1 and allow_simulated_trades:
            mutable_order, did_trade = _maybe_trade_down(mutable_order, idx, remaining, team_map)
            if did_trade:
                trades.append(
                    {
                        "round": round_no,
                        "pick": idx + 1,
                        "team": mutable_order[idx]["current_team"],
                        "trade_note": "Trade-down heuristic triggered by need/tier gap and QB pressure.",
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
            score = _pick_score(team_row, player, run_pressure, scarcity)
            scored.append((score, player))
        scored.sort(key=lambda x: x[0], reverse=True)

        if not scored:
            break

        _, selected = scored[0]
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
            }
        )

    return picks, remaining, trades



def simulate_full_draft(
    board: List[dict],
    rounds: int = 7,
    allow_simulated_trades: bool = False,
) -> Tuple[List[dict], List[dict], List[dict]]:
    round_orders = load_round_orders(rounds=rounds)
    comp_picks = load_comp_picks()
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
