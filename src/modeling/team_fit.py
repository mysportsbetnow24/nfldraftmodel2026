from __future__ import annotations

import csv
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[2]
TEAM_PROFILE_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"
TEAM_CONTEXT_PATH = ROOT / "data" / "sources" / "team_needs_context_2026.csv"
DRAFT_ORDER_PATH = ROOT / "data" / "sources" / "draft_order_2026_full.csv"
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

_POSITION_TEAM_REPEAT_COUNTS: dict[str, dict[str, int]] = defaultdict(dict)
_POSITION_ROLE_TEAM_REPEAT_COUNTS: dict[str, dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(dict))


def reset_team_fit_state() -> None:
    _POSITION_TEAM_REPEAT_COUNTS.clear()
    _POSITION_ROLE_TEAM_REPEAT_COUNTS.clear()


def load_team_profiles(path: Path | None = None) -> List[dict]:
    path = path or TEAM_PROFILE_PATH
    with path.open() as f:
        return list(csv.DictReader(f))


@lru_cache(maxsize=1)
def load_team_pick_order(path: Path | None = None) -> Dict[str, List[int]]:
    path = path or DRAFT_ORDER_PATH
    out: Dict[str, List[int]] = {}
    if not path.exists():
        return out
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            team = str(
                row.get("current_team")
                or row.get("team")
                or row.get("current")
                or row.get("club")
                or ""
            ).strip().upper()
            if team not in NFL_TEAMS:
                continue
            try:
                overall = int(float(row.get("overall_pick") or 0))
            except (TypeError, ValueError):
                continue
            if overall <= 0:
                continue
            out.setdefault(team, []).append(overall)
    for team in out:
        out[team] = sorted(set(out[team]))
    return out


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
                "starter_cliff_1y_pressure": _clamp(_to_float(row.get("starter_cliff_1y_pressure"), 0.5)),
                "starter_cliff_2y_pressure": _clamp(_to_float(row.get("starter_cliff_2y_pressure"), 0.5)),
                "future_need_pressure_1y": _clamp(_to_float(row.get("future_need_pressure_1y"), 0.5)),
                "future_need_pressure_2y": _clamp(_to_float(row.get("future_need_pressure_2y"), 0.5)),
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
    cliff_1y = float(ctx.get("starter_cliff_1y_pressure", 0.5))
    cliff_2y = float(ctx.get("starter_cliff_2y_pressure", 0.5))
    future_1y = float(ctx.get("future_need_pressure_1y", 0.5))
    future_2y = float(ctx.get("future_need_pressure_2y", 0.5))
    starter_q = float(ctx["starter_quality"])
    pressure = (
        0.30 * depth
        + 0.18 * fa
        + 0.16 * contract
        + 0.12 * cliff_1y
        + 0.08 * cliff_2y
        + 0.11 * future_1y
        + 0.03 * future_2y
        + 0.02 * (1.0 - starter_q)
    )
    return round(_clamp(pressure), 4)


def composite_need_score(
    team_row: dict,
    position: str,
    context_map: Dict[Tuple[str, str], dict] | None = None,
    prospect_rank_seed: int | None = None,
    athletic_score: float | None = None,
    role_hint: str = "",
) -> float:
    base = need_score(team_row, position)
    team = team_row.get("team", "")
    context_map = context_map if context_map is not None else load_team_needs_context()
    ctx = context_map.get((team, position))
    if not ctx:
        return round(0.35 * base + 0.65 * role_pressure_score(team, position, context_map=context_map), 4)

    depth = float(ctx["depth_chart_pressure"])
    fa = float(ctx["free_agent_pressure"])
    contract = float(ctx["contract_year_pressure"])
    cliff_1y = float(ctx.get("starter_cliff_1y_pressure", 0.5))
    cliff_2y = float(ctx.get("starter_cliff_2y_pressure", 0.5))
    future_1y = float(ctx.get("future_need_pressure_1y", 0.5))
    future_2y = float(ctx.get("future_need_pressure_2y", 0.5))
    starter_q = float(ctx["starter_quality"])

    rank = int(prospect_rank_seed or 180)
    role_text = str(role_hint or "").lower()
    explosive_role = any(
        token in role_text
        for token in {
            "franchise",
            "cornerstone",
            "field-stretching",
            "alignment-flexible",
            "three-down pressure",
            "coverage eraser",
            "outside matchup",
            "one-gap interior disruptor",
        }
    )
    premium_ath = athletic_score is not None and athletic_score >= 86.0

    if rank <= 40 or explosive_role or premium_ath:
        # Early premium prospects should be pulled more toward teams lacking
        # quality starters and with near-future need, not just current depth.
        role = (
            0.16 * depth
            + 0.10 * fa
            + 0.10 * contract
            + 0.14 * cliff_1y
            + 0.10 * cliff_2y
            + 0.20 * future_1y
            + 0.06 * future_2y
            + 0.14 * (1.0 - starter_q)
        )
        base_w = {
            "QB": 0.28,
            "RB": 0.12,
            "WR": 0.20,
            "TE": 0.10,
            "OT": 0.18,
            "IOL": 0.10,
            "EDGE": 0.16,
            "DT": 0.16,
            "LB": 0.10,
            "CB": 0.18,
            "S": 0.10,
        }.get(position, 0.14)
    elif rank <= 110:
        role = (
            0.25 * depth
            + 0.16 * fa
            + 0.14 * contract
            + 0.13 * cliff_1y
            + 0.08 * cliff_2y
            + 0.12 * future_1y
            + 0.04 * future_2y
            + 0.08 * (1.0 - starter_q)
        )
        base_w = {
            "QB": 0.32,
            "RB": 0.16,
            "WR": 0.24,
            "TE": 0.14,
            "OT": 0.24,
            "IOL": 0.14,
            "EDGE": 0.22,
            "DT": 0.22,
            "LB": 0.14,
            "CB": 0.24,
            "S": 0.14,
        }.get(position, 0.20)
    else:
        role = (
            0.32 * depth
            + 0.22 * fa
            + 0.18 * contract
            + 0.10 * cliff_1y
            + 0.04 * cliff_2y
            + 0.08 * future_1y
            + 0.02 * future_2y
            + 0.04 * (1.0 - starter_q)
        )
        base_w = {
            "QB": 0.34,
            "RB": 0.18,
            "WR": 0.26,
            "TE": 0.16,
            "OT": 0.26,
            "IOL": 0.16,
            "EDGE": 0.24,
            "DT": 0.24,
            "LB": 0.16,
            "CB": 0.26,
            "S": 0.16,
        }.get(position, 0.22)
    role = _clamp(role)
    return round(base_w * base + (1.0 - base_w) * role, 4)


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


def _lb_role_scheme_bonus(team_row: dict, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    """
    Small additive bonus so off-ball LB team fit is role-aware (not position-only).
    Keeps impact bounded so need/scheme/GM priors still dominate.
    """
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    def_scheme = str(team_row.get("def_scheme", "")).strip().lower()
    gm = str(team_row.get("gm_profile", "")).strip().lower()

    bonus = 0.0

    if any(token in role for token in {"coverage", "will", "overhang", "star"}):
        if def_scheme in {"4-2-5", "multiple"}:
            bonus += 0.090
        elif def_scheme in {"4-3"}:
            bonus += 0.040
        elif def_scheme in {"3-4"}:
            bonus -= 0.100

    if any(token in role for token in {"mike", "thumper", "stack-and-shed"}):
        if def_scheme in {"3-4", "4-3"}:
            bonus += 0.070
        elif def_scheme in {"multiple"}:
            bonus += 0.035
        elif def_scheme in {"4-2-5"}:
            bonus -= 0.050

    if any(token in role for token in {"sam", "pressure"}):
        if def_scheme in {"3-4", "multiple"}:
            bonus += 0.080
        elif def_scheme in {"4-3"}:
            bonus -= 0.040

    if "run-and-chase" in role and def_scheme in {"4-2-5", "4-3", "multiple"}:
        bonus += 0.030

    if "sim-pressure" in scheme and def_scheme in {"3-4", "multiple"}:
        bonus += 0.035
    elif "sim-pressure" in scheme and def_scheme in {"4-3"}:
        bonus -= 0.025
    if any(token in scheme for token in {"two-high", "split-safety", "match"}) and def_scheme in {"4-2-5", "multiple"}:
        bonus += 0.030
    elif any(token in scheme for token in {"two-high", "split-safety"}) and def_scheme in {"3-4"}:
        bonus -= 0.030
    if "single-high" in scheme and def_scheme in {"3-4", "4-3"}:
        bonus += 0.015
    elif "single-high" in scheme and def_scheme in {"4-2-5"}:
        bonus -= 0.015

    if athletic_score is not None:
        if athletic_score >= 87.0 and gm in {"speed_priority", "traits_speed"}:
            bonus += 0.015
        elif athletic_score <= 80.0 and gm in {"trench_focus", "bpa_trenches"}:
            bonus += 0.010

    return max(-0.12, min(0.12, bonus))


def _qb_role_scheme_bonus(team_row: dict, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    off = str(team_row.get("off_scheme", "")).strip().lower()
    gm = str(team_row.get("gm_profile", "")).strip().lower()
    bonus = 0.0

    if any(token in role for token in {"movement", "creator"}):
        if off in {"spread", "shotgun_spread", "shanahan", "play_action"}:
            bonus += 0.09
        elif off in {"under_center"}:
            bonus -= 0.05
    if any(token in role for token in {"distributor", "structure"}):
        if off in {"west_coast", "multiple", "under_center"}:
            bonus += 0.08
        elif off in {"shotgun_spread"}:
            bonus -= 0.03
    if "rpo" in scheme or "spread" in scheme:
        if off in {"spread", "shotgun_spread"}:
            bonus += 0.05
    if "boot" in scheme or "play-action" in scheme:
        if off in {"shanahan", "play_action", "multiple"}:
            bonus += 0.05
    if athletic_score is not None:
        if athletic_score >= 87.0 and gm in {"traits_speed", "speed_priority", "reset_qb"}:
            bonus += 0.02
        elif athletic_score <= 80.0 and gm in {"offense_first", "balanced_value"}:
            bonus += 0.01
    return max(-0.14, min(0.14, bonus))


def _rb_role_scheme_bonus(team_row: dict, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    off = str(team_row.get("off_scheme", "")).strip().lower()
    gm = str(team_row.get("gm_profile", "")).strip().lower()
    bonus = 0.0

    if any(token in role for token in {"every-down", "passing-down utility", "feature"}):
        if off in {"spread", "west_coast", "shanahan", "play_action"}:
            bonus += 0.08
    if any(token in role for token in {"slasher", "one-cut"}):
        if off in {"shanahan", "wide_zone", "play_action"} or "wide-zone" in scheme:
            bonus += 0.10
    if any(token in role for token in {"bruiser", "grinder"}):
        if off in {"under_center", "multiple_gap", "multiple"} or "gap" in scheme:
            bonus += 0.09
        elif off in {"shotgun_spread"}:
            bonus -= 0.03
    if athletic_score is not None and athletic_score >= 87.0 and gm in {"speed_priority", "traits_speed"}:
        bonus += 0.02
    return max(-0.14, min(0.14, bonus))


def _wr_te_role_scheme_bonus(team_row: dict, position: str, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    off = str(team_row.get("off_scheme", "")).strip().lower()
    gm = str(team_row.get("gm_profile", "")).strip().lower()
    bonus = 0.0

    if position == "WR":
        if any(token in role for token in {"field-stretching", "vertical", "boundary"}):
            if off in {"vertical", "play_action", "shanahan", "multiple"} or "vertical" in scheme:
                bonus += 0.09
        if any(token in role for token in {"alignment-flexible", "movement", "separator"}):
            if off in {"spread", "west_coast", "shotgun_spread", "multiple"}:
                bonus += 0.08
        if "progression" in scheme or "spacing" in scheme:
            if off in {"spread", "west_coast", "multiple"}:
                bonus += 0.05
    else:
        if any(token in role for token in {"move", "detached", "mismatch"}):
            if off in {"spread", "multiple", "shanahan"}:
                bonus += 0.09
        if any(token in role for token in {"in-line", "attach"}):
            if off in {"under_center", "shanahan", "play_action", "multiple"}:
                bonus += 0.08
    if athletic_score is not None and athletic_score >= 87.0 and gm in {"traits_speed", "speed_priority", "athletic_length"}:
        bonus += 0.02
    return max(-0.14, min(0.14, bonus))


def _ol_role_scheme_bonus(team_row: dict, position: str, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    off = str(team_row.get("off_scheme", "")).strip().lower()
    gm = str(team_row.get("gm_profile", "")).strip().lower()
    bonus = 0.0

    if any(token in role for token in {"movement", "zone"}):
        if off in {"shanahan", "wide_zone", "play_action"} or "zone" in scheme:
            bonus += 0.09
    if any(token in role for token in {"power", "drive", "displacement"}):
        if off in {"under_center", "multiple_gap"} or "gap" in scheme or "duo" in scheme:
            bonus += 0.09
    if any(token in role for token in {"pass-pro", "pocket-control", "translator"}):
        if off in {"west_coast", "multiple", "spread"}:
            bonus += 0.07
    if position == "OT" and "swing" in role and gm in {"draft_develop", "balanced_value", "bpa_balanced"}:
        bonus += 0.03
    if athletic_score is not None:
        if athletic_score >= 86.0 and gm in {"athletic_length", "athletic_thresholds", "traits_speed"}:
            bonus += 0.02
        elif athletic_score <= 80.0 and gm in {"trench_focus", "bpa_trenches"}:
            bonus += 0.01
    return max(-0.14, min(0.14, bonus))


def _front_role_scheme_bonus(team_row: dict, position: str, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    deff = str(team_row.get("def_scheme", "")).strip().lower()
    gm = str(team_row.get("gm_profile", "")).strip().lower()
    bonus = 0.0

    if position == "EDGE":
        if any(token in role for token in {"wide-alignment", "speed pressure", "rush specialist"}):
            if deff in {"4-3", "multiple"}:
                bonus += 0.09
            elif deff in {"3-4"}:
                bonus += 0.03
        if any(token in role for token in {"power edge", "edge-setting"}):
            if deff in {"3-4", "multiple"}:
                bonus += 0.09
        if "multiple-front" in scheme and deff in {"3-4", "multiple"}:
            bonus += 0.05
        if "sub-package" in scheme and deff in {"4-3", "4-2-5", "multiple"}:
            bonus += 0.04
    else:
        if any(token in role for token in {"one-gap", "three-tech", "disruptor"}):
            if deff in {"4-3", "4-2-5"}:
                bonus += 0.10
            elif deff in {"3-4"}:
                bonus -= 0.03
        if any(token in role for token in {"anchor", "double-team", "shade"}):
            if deff in {"3-4", "multiple"}:
                bonus += 0.09
        if "odd-front" in scheme and deff in {"3-4", "multiple"}:
            bonus += 0.05
        if "upfield" in scheme and deff in {"4-3", "4-2-5"}:
            bonus += 0.05
    if athletic_score is not None and athletic_score >= 87.0 and gm in {"traits_speed", "athletic_length", "speed_priority"}:
        bonus += 0.02
    return max(-0.16, min(0.16, bonus))


def _db_role_scheme_bonus(team_row: dict, position: str, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    deff = str(team_row.get("def_scheme", "")).strip().lower()
    gm = str(team_row.get("gm_profile", "")).strip().lower()
    bonus = 0.0

    if position == "CB":
        if "outside matchup" in role or "travel-capable" in scheme or "press-match" in scheme:
            if deff in {"3-4", "4-3"}:
                bonus += 0.09
        if "off-zone" in role or "pattern-match zone" in scheme:
            if deff in {"4-2-5", "4-3", "multiple"}:
                bonus += 0.08
        if "nickel" in role or "big-nickel" in scheme:
            if deff in {"4-2-5", "multiple"}:
                bonus += 0.09
    else:
        if any(token in role for token in {"eraser", "range-first", "split safety"}):
            if deff in {"4-2-5", "multiple"}:
                bonus += 0.09
            elif deff in {"3-4", "4-3"}:
                bonus += 0.04
        if any(token in role for token in {"matchup", "big nickel"}):
            if deff in {"4-2-5", "multiple"}:
                bonus += 0.09
        if any(token in role for token in {"robber", "box", "pressure"}):
            if deff in {"3-4", "4-3", "multiple"}:
                bonus += 0.08
    if athletic_score is not None and athletic_score >= 87.0 and gm in {"traits_speed", "speed_priority", "athletic_length", "athletic_thresholds"}:
        bonus += 0.02
    return max(-0.14, min(0.14, bonus))


def _role_scheme_bonus(team_row: dict, position: str, role_hint: str, scheme_hint: str, athletic_score: float | None) -> float:
    if position == "QB":
        return _qb_role_scheme_bonus(team_row, role_hint, scheme_hint, athletic_score)
    if position == "RB":
        return _rb_role_scheme_bonus(team_row, role_hint, scheme_hint, athletic_score)
    if position in {"WR", "TE"}:
        return _wr_te_role_scheme_bonus(team_row, position, role_hint, scheme_hint, athletic_score)
    if position in {"OT", "IOL"}:
        return _ol_role_scheme_bonus(team_row, position, role_hint, scheme_hint, athletic_score)
    if position in {"EDGE", "DT"}:
        return _front_role_scheme_bonus(team_row, position, role_hint, scheme_hint, athletic_score)
    if position == "LB":
        return _lb_role_scheme_bonus(team_row, role_hint, scheme_hint, athletic_score)
    if position in {"CB", "S"}:
        return _db_role_scheme_bonus(team_row, position, role_hint, scheme_hint, athletic_score)
    return 0.0


def _target_pick_window(position: str, prospect_rank_seed: int | None) -> tuple[float, float]:
    seed = max(1, int(prospect_rank_seed or 120))
    premium_pos = {"QB", "OT", "EDGE", "CB", "WR"}
    pos_shift = -2 if position in premium_pos and seed <= 48 else 0
    target = max(1.0, seed + pos_shift)
    if seed <= 8:
        spread = 5.0
    elif seed <= 20:
        spread = 8.0
    elif seed <= 45:
        spread = 12.0
    elif seed <= 90:
        spread = 18.0
    elif seed <= 140:
        spread = 22.0
    else:
        spread = 28.0
    return target, spread


def _draft_order_score(team: str, position: str, prospect_rank_seed: int | None) -> float:
    if prospect_rank_seed is None:
        return 0.5
    picks = load_team_pick_order().get(team, [])
    if not picks:
        return 0.25
    target, spread = _target_pick_window(position, prospect_rank_seed)
    best = 0.0
    for pick in picks:
        distance = abs(pick - target)
        score = max(0.0, 1.0 - (distance / spread))
        if pick >= target and distance <= spread * 0.55:
            score += 0.08
        elif pick < target and distance <= spread * 0.18:
            score += 0.02
        elif pick < target and distance > spread * 0.45:
            score -= 0.10
        best = max(best, score)
    return _clamp(best)


def _draft_component_weight(prospect_rank_seed: int | None) -> float:
    seed = int(prospect_rank_seed or 120)
    if seed <= 12:
        return 0.38
    if seed <= 32:
        return 0.34
    if seed <= 75:
        return 0.28
    if seed <= 130:
        return 0.22
    return 0.18


def _minimum_draft_plausibility(prospect_rank_seed: int | None) -> float:
    seed = int(prospect_rank_seed or 120)
    if seed <= 12:
        return 0.58
    if seed <= 32:
        return 0.48
    if seed <= 75:
        return 0.36
    if seed <= 130:
        return 0.24
    return 0.14


def _draft_plausibility_multiplier(
    draft_component: float,
    prospect_rank_seed: int | None,
) -> float:
    seed = int(prospect_rank_seed or 120)
    if seed <= 12:
        return 0.45 + 0.55 * draft_component
    if seed <= 32:
        return 0.52 + 0.48 * draft_component
    if seed <= 75:
        return 0.60 + 0.40 * draft_component
    if seed <= 130:
        return 0.70 + 0.30 * draft_component
    return 0.78 + 0.22 * draft_component


def _fit_component_weights(prospect_rank_seed: int | None) -> tuple[float, float, float, float, float]:
    draft_weight = _draft_component_weight(prospect_rank_seed)
    if draft_weight >= 0.34:
        return 0.28, 0.16, 0.08, 0.10, draft_weight
    if draft_weight >= 0.28:
        return 0.34, 0.16, 0.08, 0.10, draft_weight
    if draft_weight >= 0.22:
        return 0.40, 0.16, 0.08, 0.10, draft_weight
    return 0.46, 0.16, 0.08, 0.12, draft_weight


def _fit_pool_sizes(position: str, prospect_rank_seed: int | None) -> tuple[int, int]:
    seed = prospect_rank_seed or 9999
    if seed <= 12:
        return 8, 8
    if seed <= 32:
        return 7, 7
    if seed <= 75:
        return 6, 6
    if position == "QB":
        return 7, 6
    return 5, 5


def _repeat_fit_tolerance(position: str, prospect_rank_seed: int | None) -> int:
    seed = int(prospect_rank_seed or 120)
    if position in {"WR", "EDGE"}:
        return 2 if seed <= 75 else 1
    if position in {"LB"}:
        return 2 if seed <= 40 else 1
    if position in {"QB"}:
        return 1
    return 1


def _repeat_fit_penalty(position: str, team: str, prospect_rank_seed: int | None) -> float:
    team_counts = _POSITION_TEAM_REPEAT_COUNTS.get(position, {})
    count = int(team_counts.get(team, 0))
    tolerance = _repeat_fit_tolerance(position, prospect_rank_seed)
    if count < tolerance:
        return 0.0
    step = {
        "QB": 0.080,
        "RB": 0.060,
        "WR": 0.050,
        "TE": 0.075,
        "OT": 0.065,
        "IOL": 0.070,
        "EDGE": 0.050,
        "DT": 0.060,
        "LB": 0.050,
        "CB": 0.060,
        "S": 0.065,
    }.get(position, 0.060)
    penalty = (count - tolerance + 1) * step
    return min(0.20, penalty)


def _role_bucket(position: str, role_hint: str, scheme_hint: str) -> str:
    role = str(role_hint or "").lower()
    scheme = str(scheme_hint or "").lower()
    text = f"{role} {scheme}"

    if position == "QB":
        if any(token in text for token in {"movement", "creator", "rpo", "boot", "play-action"}):
            return "movement_creator"
        if any(token in text for token in {"distributor", "structure", "timing", "pocket"}):
            return "structure_distributor"
        return "balanced_qb"

    if position == "RB":
        if any(token in text for token in {"one-cut", "slasher", "wide-zone", "zone"}):
            return "zone_runner"
        if any(token in text for token in {"bruiser", "grinder", "gap", "duo", "power"}):
            return "power_runner"
        if any(token in text for token in {"every-down", "passing-down utility", "feature", "receiving"}):
            return "feature_back"
        return "balanced_back"

    if position == "WR":
        if any(token in text for token in {"field-stretching", "vertical", "boundary", "x receiver"}):
            return "boundary_vertical"
        if any(token in text for token in {"slot", "movement", "separator", "alignment-flexible", "z receiver"}):
            return "separator_space"
        return "balanced_receiver"

    if position == "TE":
        if any(token in text for token in {"move", "detached", "mismatch", "flex"}):
            return "move_te"
        if any(token in text for token in {"in-line", "attach", "y-tight", "inline"}):
            return "inline_te"
        return "balanced_te"

    if position in {"OT", "IOL"}:
        if any(token in text for token in {"movement", "zone", "wide-zone"}):
            return "zone_mover"
        if any(token in text for token in {"power", "drive", "displacement", "gap", "duo"}):
            return "power_displacer"
        if any(token in text for token in {"pass-pro", "pocket-control", "translator"}):
            return "pass_pro"
        return "balanced_ol"

    if position == "EDGE":
        if any(token in text for token in {"wide-alignment", "speed pressure", "rush specialist", "upfield"}):
            return "speed_edge"
        if any(token in text for token in {"power edge", "edge-setting", "compress", "long-arm"}):
            return "power_edge"
        if any(token in text for token in {"stand-up", "sam", "pressure linebacker", "hybrid"}):
            return "hybrid_edge"
        return "balanced_edge"

    if position == "DT":
        if any(token in text for token in {"one-gap", "three-tech", "disruptor", "upfield"}):
            return "penetrating_dt"
        if any(token in text for token in {"anchor", "double-team", "shade", "nose"}):
            return "anchor_dt"
        return "balanced_dt"

    if position == "LB":
        if any(token in text for token in {"coverage", "will", "overhang", "star", "run-and-chase"}):
            return "coverage_lb"
        if any(token in text for token in {"mike", "thumper", "stack-and-shed"}):
            return "box_lb"
        if any(token in text for token in {"sam", "pressure", "blitz"}):
            return "pressure_lb"
        return "balanced_lb"

    if position == "CB":
        if any(token in text for token in {"outside matchup", "press-match", "travel-capable", "boundary"}):
            return "outside_cb"
        if any(token in text for token in {"off-zone", "pattern-match zone"}):
            return "zone_cb"
        if any(token in text for token in {"nickel", "slot"}):
            return "nickel_cb"
        return "balanced_cb"

    if position == "S":
        if any(token in text for token in {"eraser", "range-first", "split safety", "deep"}):
            return "deep_safety"
        if any(token in text for token in {"robber", "box", "pressure", "downhill"}):
            return "box_safety"
        if any(token in text for token in {"big nickel", "matchup", "slot"}):
            return "big_nickel"
        return "balanced_safety"

    return "balanced"


def _role_bucket_repeat_penalty(
    position: str,
    role_bucket: str,
    team: str,
    prospect_rank_seed: int | None,
) -> float:
    if not role_bucket or role_bucket.startswith("balanced"):
        return 0.0
    team_counts = _POSITION_ROLE_TEAM_REPEAT_COUNTS.get(position, {}).get(role_bucket, {})
    count = int(team_counts.get(team, 0))
    if count == 0:
        return 0.0
    seed = int(prospect_rank_seed or 120)
    step = 0.060 if seed <= 40 else 0.045 if seed <= 100 else 0.030
    return min(0.16, count * step)


def _record_team_fit(position: str, team: str) -> None:
    team_counts = _POSITION_TEAM_REPEAT_COUNTS.setdefault(position, {})
    team_counts[team] = int(team_counts.get(team, 0)) + 1


def _record_role_bucket_fit(position: str, role_bucket: str, team: str) -> None:
    if not role_bucket or role_bucket.startswith("balanced"):
        return
    bucket_counts = _POSITION_ROLE_TEAM_REPEAT_COUNTS[position].setdefault(role_bucket, {})
    bucket_counts[team] = int(bucket_counts.get(team, 0)) + 1


def _candidate_team_pool(
    position: str,
    team_rows: list[dict],
    *,
    context_map: dict[str, dict[str, float | str]] | None,
    role_hint: str,
    scheme_hint: str,
    athletic_score: float | None,
    prospect_rank_seed: int | None,
) -> list[dict]:
    need_count, fit_count = _fit_pool_sizes(position, prospect_rank_seed)
    draft_floor = _minimum_draft_plausibility(prospect_rank_seed)
    role_bucket = _role_bucket(position, role_hint, scheme_hint)

    draft_scores = {
        str(row.get("team", "")): _draft_order_score(str(row.get("team", "")), position, prospect_rank_seed)
        for row in team_rows
    }

    plausible_rows = [
        row for row in team_rows if draft_scores.get(str(row.get("team", "")), 0.0) >= draft_floor
    ]
    if len(plausible_rows) < max(8, need_count + 2):
        plausible_rows = team_rows

    need_ranked = sorted(
        plausible_rows,
        key=lambda row: composite_need_score(
            row,
            position,
            context_map=context_map,
            prospect_rank_seed=prospect_rank_seed,
            athletic_score=athletic_score,
            role_hint=role_hint,
        ),
        reverse=True,
    )

    draft_ranked = sorted(
        team_rows,
        key=lambda row: draft_scores.get(str(row.get("team", "")), 0.0),
        reverse=True,
    )

    fit_ranked = sorted(
        plausible_rows,
        key=lambda row: (
            0.60 * scheme_score(row, position)
            + 0.25 * gm_tendency_score(row, position)
            + _role_scheme_bonus(
                row,
                position=position,
                role_hint=role_hint,
                scheme_hint=scheme_hint,
                athletic_score=athletic_score,
            )
            - _role_bucket_repeat_penalty(
                position,
                role_bucket,
                str(row.get("team", "")),
                prospect_rank_seed,
            )
        ),
        reverse=True,
    )

    selected: list[dict] = []
    seen: set[str] = set()
    draft_count = 7 if (prospect_rank_seed or 9999) <= 40 else 5
    for row in need_ranked[:need_count] + fit_ranked[:fit_count] + draft_ranked[:draft_count]:
        team = str(row.get("team", ""))
        if not team or team in seen:
            continue
        seen.add(team)
        selected.append(row)
    return selected or team_rows


def best_team_fit(
    position: str,
    *,
    role_hint: str = "",
    scheme_hint: str = "",
    athletic_score: float | None = None,
    prospect_rank_seed: int | None = None,
) -> Tuple[str, float]:
    ctx_map = load_team_needs_context()
    team_rows = load_team_profiles()
    role_bucket = _role_bucket(position, role_hint, scheme_hint)
    candidate_rows = _candidate_team_pool(
        position,
        team_rows,
        context_map=ctx_map,
        role_hint=role_hint,
        scheme_hint=scheme_hint,
        athletic_score=athletic_score,
        prospect_rank_seed=prospect_rank_seed,
    )
    best_team = ""
    best_score = -1.0
    for row in candidate_rows:
        team = str(row.get("team", ""))
        need_component = composite_need_score(
            row,
            position,
            context_map=ctx_map,
            prospect_rank_seed=prospect_rank_seed,
            athletic_score=athletic_score,
            role_hint=role_hint,
        )
        scheme_component = scheme_score(row, position)
        gm_component = gm_tendency_score(row, position)
        draft_component = _draft_order_score(team, position, prospect_rank_seed)
        role_bonus = _role_scheme_bonus(
            row,
            position=position,
            role_hint=role_hint,
            scheme_hint=scheme_hint,
            athletic_score=athletic_score,
        )
        need_weight, scheme_weight, gm_weight, role_weight, draft_weight = _fit_component_weights(prospect_rank_seed)
        score = (
            need_weight * need_component
            + scheme_weight * scheme_component
            + gm_weight * gm_component
            + role_weight * max(0.0, min(1.0, 0.5 + role_bonus))
            + draft_weight * draft_component
        )
        score *= _draft_plausibility_multiplier(draft_component, prospect_rank_seed)
        score -= _repeat_fit_penalty(position, team, prospect_rank_seed)
        score -= _role_bucket_repeat_penalty(position, role_bucket, team, prospect_rank_seed)
        if score > best_score:
            best_score = score
            best_team = team
    if best_team:
        _record_team_fit(position, best_team)
        _record_role_bucket_fit(position, role_bucket, best_team)
    return best_team, round(best_score * 100.0, 2)


def team_pick_needs() -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for row in load_team_profiles():
        out[row["team"]] = [row["need_1"], row["need_2"], row["need_3"]]
    return out
