from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[2]
TEAM_PROFILE_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"
TEAM_CONTEXT_PATH = ROOT / "data" / "sources" / "team_needs_context_2026.csv"
MODEL_POSITIONS = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]
NFL_TEAMS = {
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE",
    "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LAC", "LAR", "LV", "MIA", "MIN", "NE", "NO", "NYG",
    "NYJ", "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WAS",
}
REQUIRED_CONTEXT_COLUMNS = {
    "team",
    "position",
    "depth_chart_pressure",
    "free_agent_pressure",
    "contract_year_pressure",
    "starter_quality",
}
CFBD_BLOCKLIST_COLUMNS = {
    "season",
    "conference",
    "offense",
    "defense",
    "ppa",
    "totalppa",
    "successrate",
    "explosiveness",
    "lineyards",
    "openfieldyards",
    "secondlevelyards",
    "havoc",
    "powersuccess",
    "stuffrate",
    "passingdowns",
    "standarddowns",
    "pointsperopportunity",
    "fieldposition",
    "totalopportunies",
    "totalopportunities",
}


def load_team_profiles(path: Path | None = None) -> List[dict]:
    path = path or TEAM_PROFILE_PATH
    with path.open() as f:
        return list(csv.DictReader(f))


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _to_float(v: str | None, default: float = 0.5) -> float:
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _validate_team_needs_context_source(path: Path, fieldnames: list[str] | None) -> None:
    path_l = str(path).lower()
    if "/cfbd/" in path_l or path_l.endswith(".json") or "team_advanced_stats" in path_l or "team_ppa" in path_l:
        raise ValueError(
            f"Invalid team-needs context source: {path}. "
            "CFBD team datasets are blocked; use NFL team_needs_context_2026.csv schema only."
        )

    headers = [str(c or "").strip().lower() for c in (fieldnames or [])]
    header_set = set(headers)
    missing = [col for col in REQUIRED_CONTEXT_COLUMNS if col not in header_set]
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(
            f"Invalid team-needs context schema in {path}: missing required columns [{missing_str}]."
        )

    blocked = [col for col in headers if col in CFBD_BLOCKLIST_COLUMNS]
    if blocked:
        blocked_str = ", ".join(sorted(set(blocked)))
        raise ValueError(
            f"Invalid team-needs context schema in {path}: CFBD/team-stat columns are blocked [{blocked_str}]."
        )


def load_team_needs_context(path: Path | None = None) -> Dict[Tuple[str, str], dict]:
    path = path or TEAM_CONTEXT_PATH
    if not path.exists():
        return {}
    out: Dict[Tuple[str, str], dict] = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        _validate_team_needs_context_source(path, reader.fieldnames)
        for row in reader:
            team = (row.get("team") or "").strip()
            if team and team not in NFL_TEAMS:
                continue
            pos = (row.get("position") or "").strip().upper()
            if not team or pos not in MODEL_POSITIONS:
                continue
            out[(team, pos)] = {
                "depth_chart_pressure": _clamp(_to_float(row.get("depth_chart_pressure"), 0.5)),
                "free_agent_pressure": _clamp(_to_float(row.get("free_agent_pressure"), 0.5)),
                "contract_year_pressure": _clamp(_to_float(row.get("contract_year_pressure"), 0.5)),
                "starter_quality": _clamp(_to_float(row.get("starter_quality"), 0.5)),
            }
    return out


def need_score(team_row: dict, position: str) -> float:
    if position == team_row.get("need_1"):
        return 1.0
    if position == team_row.get("need_2"):
        return 0.72
    if position == team_row.get("need_3"):
        return 0.48
    return 0.15


def role_pressure_score(
    team: str,
    position: str,
    context_map: Dict[Tuple[str, str], dict] | None = None,
) -> float:
    """
    Role-aware pressure from depth chart, free-agency exposure, and contract runway.
    Higher score = stronger short-term pressure to draft this position.
    """
    context_map = context_map if context_map is not None else load_team_needs_context()
    ctx = context_map.get((team, position))
    if not ctx:
        return 0.5

    depth = float(ctx["depth_chart_pressure"])
    fa = float(ctx["free_agent_pressure"])
    contract = float(ctx["contract_year_pressure"])
    starter_q = float(ctx["starter_quality"])
    pressure = 0.40 * depth + 0.30 * fa + 0.25 * contract + 0.05 * (1.0 - starter_q)
    return round(_clamp(pressure), 4)


def composite_need_score(
    team_row: dict,
    position: str,
    context_map: Dict[Tuple[str, str], dict] | None = None,
) -> float:
    base = need_score(team_row, position)
    team = team_row.get("team", "")
    role = role_pressure_score(team, position, context_map=context_map)
    return round(0.60 * base + 0.40 * role, 4)


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
    ctx_map = load_team_needs_context()
    best_team = ""
    best_score = -1.0
    for row in load_team_profiles():
        score = (
            0.50 * composite_need_score(row, position, context_map=ctx_map)
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
