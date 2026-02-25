from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[2]
TEAM_PROFILE_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"


def load_team_profiles(path: Path | None = None) -> List[dict]:
    path = path or TEAM_PROFILE_PATH
    with path.open() as f:
        return list(csv.DictReader(f))


def need_score(team_row: dict, position: str) -> float:
    if position == team_row.get("need_1"):
        return 1.0
    if position == team_row.get("need_2"):
        return 0.72
    if position == team_row.get("need_3"):
        return 0.48
    return 0.15


def scheme_score(team_row: dict, position: str) -> float:
    off = team_row.get("off_scheme", "")
    deff = team_row.get("def_scheme", "")

    if position in {"QB", "WR", "OT", "TE", "RB", "IOL"}:
        if off in {"shanahan", "wide_zone"} and position in {"OT", "TE", "RB"}:
            return 0.92
        if off in {"spread", "shotgun_spread", "vertical"} and position in {"QB", "WR"}:
            return 0.9
        return 0.7

    if position in {"EDGE", "DT", "LB", "CB", "S"}:
        if deff in {"3-4", "multiple"} and position in {"EDGE", "LB", "CB", "S"}:
            return 0.9
        if deff in {"4-3", "4-2-5"} and position in {"DT", "EDGE", "CB", "S"}:
            return 0.88
        return 0.68

    return 0.7


def gm_tendency_score(team_row: dict, position: str) -> float:
    gm = team_row.get("gm_profile", "")
    if gm in {"trench_focus", "bpa_trenches"} and position in {"OT", "IOL", "EDGE", "DT"}:
        return 0.9
    if gm in {"speed_priority", "traits_speed"} and position in {"WR", "CB", "EDGE", "RB"}:
        return 0.88
    if gm in {"reset_qb", "offense_first"} and position in {"QB", "WR", "OT"}:
        return 0.9
    return 0.72


def best_team_fit(position: str) -> Tuple[str, float]:
    best_team = ""
    best_score = -1.0
    for row in load_team_profiles():
        score = (
            0.50 * need_score(row, position)
            + 0.25 * scheme_score(row, position)
            + 0.15 * 0.75
            + 0.10 * gm_tendency_score(row, position)
        )
        if score > best_score:
            best_score = score
            best_team = row["team"]
    return best_team, round(best_score * 100.0, 2)


def team_pick_needs() -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for row in load_team_profiles():
        out[row["team"]] = [row["need_1"], row["need_2"], row["need_3"]]
    return out
