#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import polars as pl
except Exception:  # pragma: no cover
    pl = None


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "data" / "outputs"
ASTRO_DATA = ROOT / "astro-site" / "src" / "data"

BOARD_CSV = OUTPUTS / "big_board_2026.csv"
ROUND1_CSV = OUTPUTS / "mock_2026_round1.csv"
ROUND7_CSV = OUTPUTS / "mock_2026_7round.csv"
TEAM_NEEDS_CSV = ROOT / "data" / "sources" / "team_needs_context_2026.csv"
CBS_TRANSACTIONS_CSV = ROOT / "data" / "processed" / "cbs_nfl_transactions_2026.csv"
TRANSACTION_OVERRIDES_CSV = ROOT / "data" / "sources" / "manual" / "transactions_overrides_2026.csv"
INSIDER_TRANSACTIONS_CSV = ROOT / "data" / "sources" / "manual" / "insider_transactions_feed_2026.csv"
ESPN_PROSPECTS_CSV = ROOT / "data" / "sources" / "external" / "espn_nfl_draft_prospect_data" / "nfl_draft_prospects.csv"
DELTA_AUDIT_LATEST_CSV = OUTPUTS / "delta_audit_2026_latest.csv"
STABILITY_SNAPSHOTS_DIR = OUTPUTS / "stability_snapshots"
CURRENT_DRAFT_YEAR = 2026
NFLVERSE_DIR = ROOT / "data" / "sources" / "external" / "nflverse"
NFLVERSE_ROSTERS = NFLVERSE_DIR / "rosters_weekly.parquet"
NFLVERSE_CONTRACTS = NFLVERSE_DIR / "contracts.parquet"


PRODUCTION_METRIC_KEYS = [
    "cfb_qb_epa_per_play",
    "cfb_qb_pressure_signal",
    "cfb_qb_pass_td",
    "cfb_qb_pass_int",
    "cfb_qb_int_rate",
    "cfb_wrte_yprr",
    "cfb_wrte_target_share",
    "cfb_wrte_rec_td",
    "cfb_wrte_rec_yds",
    "cfb_rb_explosive_rate",
    "cfb_rb_missed_tackles_forced_per_touch",
    "cfb_rb_rush_td",
    "cfb_rb_rush_yds",
    "cfb_edge_pressure_rate",
    "cfb_edge_sacks",
    "cfb_edge_qb_hurries",
    "cfb_edge_tfl",
    "cfb_db_coverage_plays_per_target",
    "cfb_db_yards_allowed_per_coverage_snap",
    "cfb_db_int",
    "cfb_db_pbu",
    "cfb_lb_tackles",
    "cfb_lb_tfl",
    "cfb_lb_sacks",
    "cfb_lb_qb_hurries",
    "cfb_lb_signal",
    "cfb_ol_years_played",
    "cfb_ol_starts",
    "cfb_ol_usage_rate",
    "cfb_ol_proxy_signal",
]


CANONICAL_SCHOOL_ALIASES = {
    "alabama": "Alabama Crimson Tide",
    "alabama crimson tide": "Alabama Crimson Tide",
    "arizona": "Arizona Wildcats",
    "arizona wildcats": "Arizona Wildcats",
    "arizona state": "Arizona State Sun Devils",
    "arizona state sun devils": "Arizona State Sun Devils",
    "arkansas": "Arkansas Razorbacks",
    "arkansas razorbacks": "Arkansas Razorbacks",
    "auburn": "Auburn Tigers",
    "auburn tigers": "Auburn Tigers",
    "baylor": "Baylor Bears",
    "baylor bears": "Baylor Bears",
    "boise state": "Boise State Broncos",
    "boise state broncos": "Boise State Broncos",
    "boston college": "Boston College Eagles",
    "boston college eagles": "Boston College Eagles",
    "buffalo": "Buffalo Bulls",
    "buffalo bulls": "Buffalo Bulls",
    "california": "California Golden Bears",
    "california golden bears": "California Golden Bears",
    "cincinnati": "Cincinnati Bearcats",
    "cincinnati bearcats": "Cincinnati Bearcats",
    "clemson": "Clemson Tigers",
    "clemson tigers": "Clemson Tigers",
    "duke": "Duke Blue Devils",
    "duke blue devils": "Duke Blue Devils",
    "florida": "Florida Gators",
    "florida gators": "Florida Gators",
    "florida state": "Florida State Seminoles",
    "florida state seminoles": "Florida State Seminoles",
    "georgia": "Georgia Bulldogs",
    "georgia bulldogs": "Georgia Bulldogs",
    "georgia state": "Georgia State Panthers",
    "georgia state panthers": "Georgia State Panthers",
    "georgia tech": "Georgia Tech Yellow Jackets",
    "georgia tech yellow jackets": "Georgia Tech Yellow Jackets",
    "houston": "Houston Cougars",
    "houston cougars": "Houston Cougars",
    "illinois": "Illinois Fighting Illini",
    "illinois fighting illini": "Illinois Fighting Illini",
    "incarnate word": "Incarnate Word Cardinals",
    "incarnate word cardinals": "Incarnate Word Cardinals",
    "indiana": "Indiana Hoosiers",
    "indiana hoosiers": "Indiana Hoosiers",
    "iowa": "Iowa Hawkeyes",
    "iowa hawkeyes": "Iowa Hawkeyes",
    "iowa state": "Iowa State Cyclones",
    "iowa state cyclones": "Iowa State Cyclones",
    "kansas": "Kansas Jayhawks",
    "kansas jayhawks": "Kansas Jayhawks",
    "kansas state": "Kansas State Wildcats",
    "kansas state wildcats": "Kansas State Wildcats",
    "kentucky": "Kentucky Wildcats",
    "kentucky wildcats": "Kentucky Wildcats",
    "lsu": "LSU Tigers",
    "lsu tigers": "LSU Tigers",
    "louisville": "Louisville Cardinals",
    "louisville cardinals": "Louisville Cardinals",
    "miami": "Miami (FL) Hurricanes",
    "miami fl hurricanes": "Miami (FL) Hurricanes",
    "miami (fl) hurricanes": "Miami (FL) Hurricanes",
    "michigan": "Michigan Wolverines",
    "michigan wolverines": "Michigan Wolverines",
    "mississippi state": "Mississippi State Bulldogs",
    "mississippi state bulldogs": "Mississippi State Bulldogs",
    "missouri": "Missouri Tigers",
    "missouri tigers": "Missouri Tigers",
    "nc state": "North Carolina State Wolfpack",
    "north carolina state wolfpack": "North Carolina State Wolfpack",
    "nebraska": "Nebraska Cornhuskers",
    "nebraska cornhuskers": "Nebraska Cornhuskers",
    "north dakota state": "North Dakota State Bison",
    "north dakota state bison": "North Dakota State Bison",
    "northwestern": "Northwestern Wildcats",
    "northwestern wildcats": "Northwestern Wildcats",
    "notre dame": "Notre Dame Fighting Irish",
    "notre dame fighting irish": "Notre Dame Fighting Irish",
    "ohio state": "Ohio State Buckeyes",
    "ohio state buckeyes": "Ohio State Buckeyes",
    "oklahoma": "Oklahoma Sooners",
    "oklahoma sooners": "Oklahoma Sooners",
    "oregon": "Oregon Ducks",
    "oregon ducks": "Oregon Ducks",
    "penn state": "Penn State Nittany Lions",
    "penn state nittany lions": "Penn State Nittany Lions",
    "pittsburgh": "Pittsburgh Panthers",
    "pittsburgh panthers": "Pittsburgh Panthers",
    "purdue": "Purdue Boilermakers",
    "purdue boilermakers": "Purdue Boilermakers",
    "smu": "SMU Mustangs",
    "smu mustangs": "SMU Mustangs",
    "south carolina": "South Carolina Gamecocks",
    "south carolina gamecocks": "South Carolina Gamecocks",
    "stanford": "Stanford Cardinal",
    "stanford cardinal": "Stanford Cardinal",
    "tcu": "TCU Horned Frogs",
    "tcu horned frogs": "TCU Horned Frogs",
    "tennessee": "Tennessee Volunteers",
    "tennessee volunteers": "Tennessee Volunteers",
    "texas": "Texas Longhorns",
    "texas longhorns": "Texas Longhorns",
    "texas a&m": "Texas A&M Aggies",
    "texas am": "Texas A&M Aggies",
    "texas a&m aggies": "Texas A&M Aggies",
    "texas am aggies": "Texas A&M Aggies",
    "texas tech": "Texas Tech Red Raiders",
    "texas tech red raiders": "Texas Tech Red Raiders",
    "usc": "USC Trojans",
    "usc trojans": "USC Trojans",
    "toledo": "Toledo Rockets",
    "toledo rockets": "Toledo Rockets",
    "ucf": "UCF Knights",
    "ucf knights": "UCF Knights",
    "uconn": "Connecticut Huskies",
    "utah": "Utah Utes",
    "utah utes": "Utah Utes",
    "vanderbilt": "Vanderbilt Commodores",
    "vanderbilt commodores": "Vanderbilt Commodores",
    "wake forest": "Wake Forest Demon Deacons",
    "wake forest demon deacons": "Wake Forest Demon Deacons",
    "washington": "Washington Huskies",
    "washington huskies": "Washington Huskies",
}


def _safe_float(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_int(value, default: int = 0) -> int:
    val = _safe_float(value)
    if val is None:
        return default
    return int(round(val))


def _is_truthy(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _norm_school_key(value: str) -> str:
    text = str(value or "").strip().lower()
    cleaned = "".join(ch for ch in text if ch.isalnum() or ch.isspace())
    return " ".join(cleaned.split())


def _norm_player_key(value: str) -> str:
    text = str(value or "").strip().lower()
    return "".join(ch for ch in text if ch.isalnum())


def _norm_comp_identity_key(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    # Treat suffix variants as the same player (e.g., "Jr.", "III").
    tokens = re.sub(r"[^a-z0-9\s]", " ", text).split()
    suffix_tokens = {"jr", "sr", "ii", "iii", "iv", "v"}
    filtered = [tok for tok in tokens if tok not in suffix_tokens]
    if not filtered:
        filtered = tokens
    return "".join(filtered)


def _clean_token_label(value: str) -> str:
    text = str(value or "").strip().replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _clean_public_snapshot(value: str) -> str:
    """
    Remove internal pipeline/missing-data notes from public snapshot text.
    """
    text = str(value or "")
    lines = []
    banned_tokens = [
        "pending structured 2025 counting-stat import",
        "pending structured 2025 counting stat import",
    ]
    for raw in text.splitlines():
        line = str(raw or "").strip()
        if not line:
            continue
        lowered = line.lower()
        if any(token in lowered for token in banned_tokens):
            continue
        lines.append(line)
    return "\n".join(lines)


TEAM_NEEDS_POS_ORDER = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]
OFFENSE_POS_ORDER = ["QB", "RB", "WR", "TE", "OT", "IOL"]
DEFENSE_POS_ORDER = ["EDGE", "DT", "LB", "CB", "S"]
STARTERS_BY_POSITION = {"QB": 1, "RB": 1, "WR": 2, "TE": 1, "OT": 2, "IOL": 3, "EDGE": 2, "DT": 2, "LB": 2, "CB": 2, "S": 2}


def _pct_score(value: float | None, values: list[float]) -> float:
    if value is None or not values:
        return 0.0
    ordered = sorted(values)
    count = 0
    for item in ordered:
        if item <= value:
            count += 1
    return round((count / max(1, len(ordered))) * 100.0, 2)


def _parse_birth_years(birth_date: str) -> int | None:
    text = str(birth_date or "").strip()
    if not text:
        return None
    try:
        born = datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    today = datetime.now(timezone.utc).date()
    years = today.year - born.year
    if (today.month, today.day) < (born.month, born.day):
        years -= 1
    return years if years >= 0 else None


def _map_team_needs_position(position: str, depth_chart_position: str = "") -> str:
    pos = str(position or "").strip().upper()
    depth = str(depth_chart_position or "").strip().upper()
    if pos in TEAM_NEEDS_POS_ORDER:
        return pos
    if pos in {"HB", "FB"}:
        return "RB"
    if pos in {"T", "LT", "RT"}:
        return "OT"
    if pos in {"G", "LG", "RG", "C", "OC"}:
        return "IOL"
    if pos in {"DE", "ED"}:
        return "EDGE"
    if pos in {"NT", "IDL"}:
        return "DT"
    if pos in {"ILB", "MLB"}:
        return "LB"
    if pos in {"FS", "SS", "SAF"}:
        return "S"
    if pos == "OL":
        if depth in {"LT", "RT", "T"}:
            return "OT"
        if depth in {"LG", "RG", "G", "C", "OC"}:
            return "IOL"
    if pos == "DL":
        if depth in {"DE", "ED", "EDGE", "OLB"}:
            return "EDGE"
        return "DT"
    if pos == "DB":
        if depth in {"LCB", "RCB", "CB", "NB"}:
            return "CB"
        return "S"
    return ""


def _designation_priority(label: str) -> int:
    ordered = [
        "HOF Path",
        "All-Pro",
        "Franchise Cornerstone",
        "In His Prime Star",
        "Blue Chip Prospect",
        "Starter",
        "Older Mentor",
        "Prospect",
        "Backup",
        "FA",
    ]
    lookup = {name: idx for idx, name in enumerate(ordered)}
    return lookup.get(str(label or "").strip(), 999)


def _player_designation(
    *,
    has_contract: bool,
    years_exp: int,
    age: int | None,
    apy_pct: float,
    depth_rank: int,
    model_position: str,
    draft_number: int | None,
) -> str:
    if not has_contract:
        return "FA"
    if years_exp <= 1:
        if draft_number is not None and draft_number > 0 and draft_number <= 32:
            return "Blue Chip Prospect"
        return "Prospect"
    if apy_pct >= 97.0 and years_exp >= 6:
        return "HOF Path"
    if apy_pct >= 90.0:
        return "All-Pro"
    if apy_pct >= 75.0:
        return "Franchise Cornerstone"
    if apy_pct >= 55.0:
        return "In His Prime Star"
    if age is not None and age >= 33 and years_exp >= 8:
        return "Older Mentor"
    if depth_rank <= int(STARTERS_BY_POSITION.get(model_position, 1)):
        return "Starter"
    return "Backup"


def _build_team_depth_context() -> dict[str, dict]:
    if pl is None or not NFLVERSE_ROSTERS.exists():
        return {}

    rosters = pl.read_parquet(NFLVERSE_ROSTERS)
    if rosters.is_empty():
        return {}

    latest_season = int(rosters.select(pl.col("season").max()).item())
    subset = rosters.filter(pl.col("season") == latest_season)
    if "game_type" in subset.columns:
        reg_subset = subset.filter(pl.col("game_type") == "REG")
        if not reg_subset.is_empty():
            subset = reg_subset
    latest_week = int(subset.select(pl.col("week").max()).item())
    subset = subset.filter(pl.col("week") == latest_week)
    subset = subset.unique(subset=["team", "gsis_id"], keep="first")

    contract_rows = []
    apy_pool_by_pos: dict[str, list[float]] = defaultdict(list)
    contract_by_gsis: dict[str, dict] = {}
    contract_by_name: dict[str, dict] = {}
    if NFLVERSE_CONTRACTS.exists():
        contracts = pl.read_parquet(NFLVERSE_CONTRACTS)
        if not contracts.is_empty():
            contract_rows = contracts.filter(pl.col("is_active") == True).iter_rows(named=True)
    for row in contract_rows:
        gsis = str(row.get("gsis_id") or "").strip()
        name = str(row.get("player") or "").strip()
        apy = _safe_float(row.get("apy"))
        years = _safe_int(row.get("years"), 0)
        pos = _map_team_needs_position(row.get("position", ""))
        payload = {
            "gsis_id": gsis,
            "name_key": _norm_player_key(name),
            "apy": apy,
            "years": years,
            "position": pos,
            "team_text": str(row.get("team") or "").strip(),
        }
        if pos and apy is not None and apy > 0:
            apy_pool_by_pos[pos].append(float(apy))
        if gsis:
            existing = contract_by_gsis.get(gsis)
            if existing is None or (_safe_float(existing.get("apy")) or 0.0) < (_safe_float(apy) or 0.0):
                contract_by_gsis[gsis] = payload
        if payload["name_key"]:
            existing = contract_by_name.get(payload["name_key"])
            if existing is None or (_safe_float(existing.get("apy")) or 0.0) < (_safe_float(apy) or 0.0):
                contract_by_name[payload["name_key"]] = payload

    team_players: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in subset.iter_rows(named=True):
        team = str(row.get("team") or "").strip().upper()
        if not team:
            continue
        model_pos = _map_team_needs_position(row.get("position", ""), row.get("depth_chart_position", ""))
        if model_pos not in TEAM_NEEDS_POS_ORDER:
            continue
        name = str(row.get("full_name") or row.get("football_name") or "").strip()
        if not name:
            continue
        gsis_id = str(row.get("gsis_id") or "").strip()
        depth_pos = str(row.get("depth_chart_position") or "").strip()
        years_exp = _safe_int(row.get("years_exp"), 0)
        age = _parse_birth_years(row.get("birth_date", ""))
        draft_number = _safe_int(row.get("draft_number"), 0) or None

        contract = contract_by_gsis.get(gsis_id) or contract_by_name.get(_norm_player_key(name))
        apy = _safe_float(contract.get("apy")) if contract else None
        apy_pct = _pct_score(apy, apy_pool_by_pos.get(model_pos, []))
        years_left = _safe_int(contract.get("years"), 0) if contract else 0
        has_contract = contract is not None
        if has_contract:
            contract_label = f"{years_left}y | ${apy:.1f}M APY" if apy is not None else f"{years_left}y contract"
        else:
            contract_label = "FA"

        team_players[team][model_pos].append(
            {
                "player_name": name,
                "position": model_pos,
                "depth_chart_position": depth_pos,
                "years_exp": years_exp,
                "age": age if age is not None else "",
                "draft_number": draft_number if draft_number is not None else "",
                "has_contract": has_contract,
                "contract_label": contract_label,
                "apy_m": round(apy, 2) if apy is not None else "",
                "apy_pct": apy_pct,
            }
        )

    out: dict[str, dict] = {}
    for team, by_pos in team_players.items():
        offense_lanes = []
        defense_lanes = []
        free_agents = []
        youth = []
        for pos in TEAM_NEEDS_POS_ORDER:
            players = by_pos.get(pos, [])
            players = sorted(
                players,
                key=lambda p: (
                    0 if p.get("has_contract") else 1,
                    -(float(p.get("apy_m") or 0.0)),
                    -int(p.get("years_exp") or 0),
                    int(p.get("draft_number") or 9999),
                    str(p.get("player_name", "")),
                ),
            )
            lane_players = []
            for idx, player in enumerate(players, start=1):
                label = _player_designation(
                    has_contract=bool(player.get("has_contract")),
                    years_exp=int(player.get("years_exp") or 0),
                    age=int(player.get("age")) if str(player.get("age", "")).strip() else None,
                    apy_pct=float(player.get("apy_pct") or 0.0),
                    depth_rank=idx,
                    model_position=pos,
                    draft_number=int(player.get("draft_number")) if str(player.get("draft_number", "")).strip() else None,
                )
                payload = {
                    "player_name": player.get("player_name", ""),
                    "position": pos,
                    "depth_rank": idx,
                    "designation": label,
                    "contract_label": player.get("contract_label", ""),
                    "years_exp": int(player.get("years_exp") or 0),
                    "age": player.get("age", ""),
                    "apy_m": player.get("apy_m", ""),
                }
                lane_players.append(payload)
                if label == "FA" and len(free_agents) < 12:
                    free_agents.append(payload)
                if int(player.get("years_exp") or 0) <= 2 and label in {"Prospect", "Blue Chip Prospect", "Starter", "In His Prime Star"}:
                    youth.append(payload)

            lane = {"position": pos, "players": lane_players[:4]}
            if pos in OFFENSE_POS_ORDER:
                offense_lanes.append(lane)
            if pos in DEFENSE_POS_ORDER:
                defense_lanes.append(lane)

        youth = sorted(
            youth,
            key=lambda p: (
                _designation_priority(p.get("designation", "")),
                -float(p.get("apy_m") or 0.0),
                int(p.get("depth_rank") or 99),
                str(p.get("player_name", "")),
            ),
        )[:8]

        out[team] = {
            "depth_chart": {
                "offense": offense_lanes,
                "defense": defense_lanes,
                "season": latest_season,
                "week": latest_week,
            },
            "free_agents": free_agents[:8],
            "young_players_on_rise": youth,
        }
    return out


def _norm_similarity_pct(value) -> float | None:
    sim = _safe_float(value)
    if sim is None:
        return None
    if sim <= 0:
        return None
    if sim <= 1.0:
        sim *= 100.0
    elif sim <= 10.0:
        sim *= 10.0
    return max(0.0, min(100.0, float(sim)))


def _comp_blend_weights(position: str) -> tuple[float, float]:
    pos = str(position or "").upper()
    # Athletic translation is generally stronger for trench/front-seven roles,
    # while production patterns are more predictive for skill players/QBs.
    if pos == "QB":
        return (0.55, 0.45)
    if pos in {"RB", "WR", "TE"}:
        return (0.60, 0.40)
    if pos in {"OT", "IOL"}:
        return (0.72, 0.28)
    if pos in {"EDGE", "DT", "LB"}:
        return (0.70, 0.30)
    if pos in {"CB", "S"}:
        return (0.62, 0.38)
    return (0.65, 0.35)


def _pct_rank(value: float | None, values: list[float]) -> float | None:
    if value is None or not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        return None
    count = 0
    for v in ordered:
        if v <= value:
            count += 1
    return round((count / n) * 100.0, 1)


def _load_rank_history(window: int = 8) -> dict[str, list[int]]:
    if not STABILITY_SNAPSHOTS_DIR.exists():
        return {}
    files = sorted(STABILITY_SNAPSHOTS_DIR.glob("big_board_2026_snapshot_*.csv"))
    if not files:
        return {}
    files = files[-max(1, int(window)) :]
    out: dict[str, list[int]] = defaultdict(list)
    for path in files:
        for row in _read_csv(path):
            uid = str(row.get("player_uid", "")).strip()
            rank = _safe_int(row.get("consensus_rank"), 0)
            if uid and rank > 0:
                out[uid].append(rank)
    return out


def _canonical_school_name(raw_school: str) -> str:
    text = str(raw_school or "").strip()
    if not text:
        return ""
    return CANONICAL_SCHOOL_ALIASES.get(_norm_school_key(text), text)


def _load_player_school_map() -> dict[str, str]:
    rows = _read_csv(ESPN_PROSPECTS_CSV)
    out: dict[str, str] = {}
    for row in rows:
        key = _norm_player_key(row.get("player_name", ""))
        school = _canonical_school_name(row.get("school", "") or row.get("school_full", ""))
        if key and school:
            out[key] = school
    return out


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _parse_event_date(value: str):
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _status_label(status: str) -> str:
    raw = str(status or "").strip().lower()
    if not raw:
        return "Confirmed"
    labels = {
        "confirmed": "Confirmed",
        "official": "Confirmed",
        "rumored": "Rumored",
        "signed": "Signed",
        "re-signed": "Re-Signed",
        "released": "Released",
        "waived": "Waived",
        "traded": "Traded",
        "retired": "Retired",
    }
    return labels.get(raw, raw.replace("_", " ").title())


def _status_kind(status: str) -> str:
    raw = str(status or "").strip().lower()
    if raw in {"rumored", "rumour", "speculative", "unconfirmed"}:
        return "rumored"
    if raw in {"confirmed", "official", "signed", "re-signed", "released", "waived", "traded", "retired", "activated"}:
        return "confirmed"
    return "other"


def _build_transactions_feed(window_days: int = 14) -> list[dict]:
    min_date = datetime.now(timezone.utc).date() - timedelta(days=max(1, int(window_days)))
    events: list[dict] = []
    seen: set[tuple[str, str, str, str, str]] = set()

    def add_event(
        *,
        team: str,
        event_date,
        player_name: str,
        position: str,
        action_text: str,
        status: str,
        source_url: str,
        source_account: str,
        affects_team_needs: bool,
    ) -> None:
        team_code = str(team or "").strip().upper()
        if not team_code or event_date is None or event_date < min_date:
            return
        player = str(player_name or "").strip()
        pos = str(position or "").strip().upper()
        action = str(action_text or "").strip()
        status_raw = str(status or "").strip().lower() or "confirmed"
        status_kind = _status_kind(status_raw)
        effective_needs_impact = bool(affects_team_needs) and status_kind == "confirmed"
        key = (team_code, event_date.isoformat(), player.lower(), action.lower(), status_raw)
        if key in seen:
            return
        seen.add(key)

        if player and pos and action:
            label = f"{player} ({pos}) {action}"
        elif player and action:
            label = f"{player} {action}"
        else:
            label = action or player or "-"

        events.append(
            {
                "team": team_code,
                "event_date": event_date.isoformat(),
                "status": _status_label(status_raw),
                "status_kind": status_kind,
                "label": label,
                "player_name": player,
                "position": pos,
                "action_text": action,
                "affects_team_needs": effective_needs_impact,
                "source_url": str(source_url or "").strip(),
                "source_account": str(source_account or "").strip(),
            }
        )

    for row in _read_csv(CBS_TRANSACTIONS_CSV):
        add_event(
            team=row.get("team", ""),
            event_date=_parse_event_date(row.get("event_date", "")),
            player_name=row.get("player_name", ""),
            position=row.get("position", ""),
            action_text=row.get("action_text", ""),
            status=row.get("transaction_status", "confirmed"),
            source_url=row.get("source_url", ""),
            source_account="CBS Sports",
            affects_team_needs=True,
        )

    for row in _read_csv(TRANSACTION_OVERRIDES_CSV):
        event_date = _parse_event_date(row.get("event_date", ""))
        player_name = row.get("player_name", "")
        position = row.get("position", "")
        action_text = row.get("action_text", "")
        status = row.get("transaction_status", "confirmed")
        status_kind = _status_kind(status)
        apply_raw = row.get("apply_to_team_needs", "")
        affects_team_needs = _is_truthy(apply_raw) if str(apply_raw or "").strip() else (status_kind == "confirmed")
        source_url = row.get("source_url", "")
        source_account = row.get("source_account", "Manual")
        from_team = str(row.get("from_team", "")).strip().upper()
        to_team = str(row.get("to_team", "")).strip().upper()
        if from_team:
            add_event(
                team=from_team,
                event_date=event_date,
                player_name=player_name,
                position=position,
                action_text=action_text,
                status=status,
                source_url=source_url,
                source_account=source_account,
                affects_team_needs=affects_team_needs,
            )
        if to_team:
            add_event(
                team=to_team,
                event_date=event_date,
                player_name=player_name,
                position=position,
                action_text=action_text,
                status=status,
                source_url=source_url,
                source_account=source_account,
                affects_team_needs=affects_team_needs,
            )

    for row in _read_csv(INSIDER_TRANSACTIONS_CSV):
        status = row.get("transaction_status", "rumored")
        status_kind = _status_kind(status)
        apply_raw = row.get("apply_to_team_needs", "")
        affects_team_needs = _is_truthy(apply_raw) if str(apply_raw or "").strip() else (status_kind == "confirmed")
        add_event(
            team=row.get("team", ""),
            event_date=_parse_event_date(row.get("event_date", "")),
            player_name=row.get("player_name", ""),
            position=row.get("position", ""),
            action_text=row.get("action_text", ""),
            status=status,
            source_url=row.get("source_url", ""),
            source_account=row.get("source_account", ""),
            affects_team_needs=affects_team_needs,
        )

    events.sort(key=lambda r: (r.get("event_date", ""), r.get("team", "")), reverse=True)
    return events


def _build_public_transactions(window_days: int = 14) -> dict[str, list[dict]]:
    by_team: dict[str, list[dict]] = defaultdict(list)
    for event in _build_transactions_feed(window_days=window_days):
        by_team[event.get("team", "")].append(
            {
                "event_date": event.get("event_date", ""),
                "status": event.get("status", ""),
                "status_kind": event.get("status_kind", ""),
                "label": event.get("label", ""),
                "affects_team_needs": bool(event.get("affects_team_needs")),
                "source_url": event.get("source_url", ""),
                "source_account": event.get("source_account", ""),
            }
        )
    return by_team


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _parse_rank_components(summary: str) -> dict[str, float]:
    out: dict[str, float] = {}
    if not summary:
        return out
    parts = [p.strip() for p in str(summary).split("|")]
    for part in parts:
        if ":" not in part:
            continue
        key, raw = part.split(":", 1)
        key = key.strip().lower().replace(" ", "_")
        val = _safe_float(raw)
        if val is None:
            continue
        out[key] = float(val)
    return out


def _top_driver(summary: str) -> tuple[str, float]:
    comps = _parse_rank_components(summary)
    keep = {k: v for k, v in comps.items() if k in {"prior", "athletic", "trait", "production", "risk"}}
    if not keep:
        return ("n/a", 0.0)
    key = max(keep, key=lambda k: abs(keep[k]))
    return key, keep[key]


def _slugify_player(name: str) -> str:
    return (
        (name or "")
        .lower()
        .replace(" ", "-")
        .replace(".", "")
        .replace("'", "")
    )


def _needs_score(row: dict) -> float:
    depth = _safe_float(row.get("depth_chart_pressure")) or 0.0
    fa = _safe_float(row.get("free_agent_pressure")) or 0.0
    cy = _safe_float(row.get("contract_year_pressure")) or 0.0
    fn1 = _safe_float(row.get("future_need_pressure_1y")) or 0.0
    fn2 = _safe_float(row.get("future_need_pressure_2y")) or 0.0
    cliff1 = _safe_float(row.get("starter_cliff_1y_pressure")) or 0.0
    cliff2 = _safe_float(row.get("starter_cliff_2y_pressure")) or 0.0
    quality = _safe_float(row.get("starter_quality")) or 0.0
    quality_risk = max(0.0, 1.0 - quality)
    return (
        0.35 * depth
        + 0.15 * fa
        + 0.15 * cy
        + 0.15 * fn1
        + 0.08 * fn2
        + 0.08 * cliff1
        + 0.04 * cliff2
        + 0.10 * quality_risk
    )


def export_board(player_school_map: dict[str, str]) -> list[dict]:
    rows = _read_csv(BOARD_CSV)
    consensus_mean_population = [
        float(_safe_float(row.get("consensus_board_mean_rank")) or 0.0)
        for row in rows
        if (_safe_float(row.get("consensus_board_mean_rank")) or 0.0) > 0
    ]

    # Position-normalized metric populations for percentile context.
    pos_metric_values: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        pos = str(row.get("position", "")).strip().upper()
        if not pos:
            continue
        for key in PRODUCTION_METRIC_KEYS:
            val = _safe_float(row.get(key))
            if val is None:
                continue
            pos_metric_values[pos][key].append(float(val))

    rank_history = _load_rank_history(window=8)
    out = []
    for row in rows:
        player_name = row.get("player_name", "")
        pos = str(row.get("position", "")).strip().upper()
        school = player_school_map.get(_norm_player_key(player_name), "")
        if not school:
            school = _canonical_school_name(row.get("school", ""))

        slug = _slugify_player(player_name)
        rank_driver_summary = row.get("rank_driver_summary", "")
        top_driver_key, top_driver_delta = _top_driver(rank_driver_summary)
        pff_grade = round(_safe_float(row.get("pff_grade")) or 0.0, 2)
        combine_ras_official = round(_safe_float(row.get("combine_ras_official")) or 0.0, 2)
        ras_estimate = round(_safe_float(row.get("ras_estimate")) or 0.0, 2)
        production_snapshot = _clean_public_snapshot(row.get("scouting_production_snapshot", "") or "")
        low_evidence_flag = (
            ("pending structured" in production_snapshot.lower())
            or (pff_grade <= 0 and combine_ras_official <= 0 and ras_estimate <= 0)
        )
        production_metrics: dict[str, float] = {}
        production_percentiles: dict[str, float] = {}
        for key in PRODUCTION_METRIC_KEYS:
            v = _safe_float(row.get(key))
            if v is None:
                continue
            production_metrics[key] = round(float(v), 4)
            pop = pos_metric_values.get(pos, {}).get(key, [])
            pct = _pct_rank(float(v), pop)
            if pct is not None:
                production_percentiles[key] = pct

        uid = row.get("player_uid", "")
        hist = rank_history.get(uid, [])
        rank_move_window = 0
        if len(hist) >= 2:
            # Positive means moved up board (lower numeric rank).
            rank_move_window = int(hist[0] - hist[-1])

        raw_best_scheme = row.get("best_scheme_fit", "")
        raw_best_role = row.get("best_role", "")

        comp_items: list[dict] = []
        seen_comp_names: set[str] = set()
        player_name_key = _norm_player_key(player_name)
        player_identity_key = _norm_comp_identity_key(player_name)
        comp_blend: dict[str, dict] = {}

        def _ingest_comp(name: str, year_value, sim_value, source: str, is_production: bool) -> None:
            comp_name = str(name or "").strip()
            if not comp_name:
                return
            name_key = _norm_player_key(comp_name)
            identity_key = _norm_comp_identity_key(comp_name)
            if (
                not identity_key
                or name_key == player_name_key
                or identity_key == player_identity_key
            ):
                return
            year = _safe_int(year_value, 0)
            if year > 0 and year >= CURRENT_DRAFT_YEAR:
                return
            sim = _norm_similarity_pct(sim_value)
            if sim is None:
                return
            slot = comp_blend.get(identity_key)
            if slot is None:
                slot = {
                    "name": comp_name,
                    "year": year if year > 0 else None,
                    "ath_sims": [],
                    "prod_sims": [],
                    "sources": set(),
                }
                comp_blend[identity_key] = slot
            else:
                # Prefer names with more tokens (usually less ambiguous).
                if len(comp_name.split()) > len(str(slot.get("name", "")).split()):
                    slot["name"] = comp_name
                existing_year = slot.get("year")
                if (existing_year is None or existing_year <= 0) and year > 0:
                    slot["year"] = year
            slot["sources"].add(source)
            if is_production:
                slot["prod_sims"].append(sim)
            else:
                slot["ath_sims"].append(sim)

        for idx in (1, 2, 3):
            _ingest_comp(
                row.get(f"historical_combine_comp_{idx}", ""),
                row.get(f"historical_combine_comp_{idx}_year", ""),
                row.get(f"historical_combine_comp_{idx}_similarity", ""),
                source="historical_combine",
                is_production=False,
            )
            _ingest_comp(
                row.get(f"athletic_nn_comp_{idx}", ""),
                row.get(f"athletic_nn_comp_{idx}_year", ""),
                row.get(f"athletic_nn_comp_{idx}_similarity", ""),
                source="athletic_nn",
                is_production=False,
            )
            _ingest_comp(
                row.get(f"production_knn_comp_{idx}", ""),
                row.get(f"production_knn_comp_{idx}_year", ""),
                row.get(f"production_knn_comp_{idx}_similarity", ""),
                source="production_knn",
                is_production=True,
            )

        ath_w, prod_w = _comp_blend_weights(pos)
        for identity_key, slot in comp_blend.items():
            ath_sim = max(slot.get("ath_sims") or []) if slot.get("ath_sims") else None
            prod_sim = max(slot.get("prod_sims") or []) if slot.get("prod_sims") else None
            if ath_sim is not None and prod_sim is not None:
                blend_score = (ath_w * ath_sim) + (prod_w * prod_sim)
            elif ath_sim is not None:
                blend_score = ath_sim * 0.94
            elif prod_sim is not None:
                blend_score = prod_sim * 0.90
            else:
                continue
            if identity_key in seen_comp_names:
                continue
            seen_comp_names.add(identity_key)
            comp_items.append(
                {
                    "name": slot.get("name", ""),
                    "similarity": round(blend_score, 3),
                    "year": slot.get("year"),
                }
            )

        comp_items = sorted(
            comp_items,
            key=lambda r: (r.get("similarity") is None, -(r.get("similarity") or 0.0)),
        )
        if len(comp_items) >= 3:
            mid_idx = len(comp_items) // 2
            comp_ceiling = comp_items[0]
            comp_median = comp_items[mid_idx]
            comp_floor = comp_items[-1]
        elif len(comp_items) == 2:
            comp_ceiling = comp_items[0]
            comp_median = comp_items[1]
            comp_floor = {}
        elif len(comp_items) == 1:
            comp_ceiling = comp_items[0]
            comp_median = {}
            comp_floor = {}
        else:
            comp_ceiling = {}
            comp_median = {}
            comp_floor = {}

        consensus_mean_rank_val = _safe_float(row.get("consensus_board_mean_rank"))
        if consensus_mean_rank_val is None or consensus_mean_rank_val <= 0:
            consensus_mean_rank_val = float(_safe_int(row.get("consensus_rank"), 9999))
        market_rank_pct = _pct_rank(consensus_mean_rank_val, consensus_mean_population) if consensus_mean_population else 50.0
        market_signal_pct = 100.0 - float(market_rank_pct)

        out.append(
            {
                "player_uid": row.get("player_uid", ""),
                "slug": slug,
                "consensus_rank": _safe_int(row.get("consensus_rank"), 9999),
                "player_name": player_name,
                "position": pos,
                "school": school,
                "final_grade": round(_safe_float(row.get("final_grade")) or 0.0, 2),
                "round_value": row.get("round_value", ""),
                "consensus_board_mean_rank": row.get("consensus_board_mean_rank", ""),
                "market_signal_pct": round(market_signal_pct, 1),
                "pff_grade": pff_grade,
                "trait_score": round(_safe_float(row.get("trait_score")) or 0.0, 2),
                "combine_ras_official": combine_ras_official,
                "ras_estimate": ras_estimate,
                "confidence_score": round(_safe_float(row.get("confidence_score")) or 0.0, 2),
                "uncertainty_score": round(_safe_float(row.get("uncertainty_score")) or 0.0, 2),
                "best_role": _clean_token_label(raw_best_role),
                "best_scheme_fit": _clean_token_label(raw_best_scheme),
                "rank_driver_summary": rank_driver_summary,
                "top_rank_driver": top_driver_key,
                "top_rank_driver_delta": round(top_driver_delta, 2),
                "rank_history": hist,
                "rank_move_window": rank_move_window,
                "low_evidence_flag": low_evidence_flag,
                "production_metrics": production_metrics,
                "production_percentiles": production_percentiles,
                "historical_comp_floor": comp_floor,
                "historical_comp_median": comp_median,
                "historical_comp_ceiling": comp_ceiling,
                "comp_confidence": str(row.get("comp_confidence", "")).strip(),
                "scouting_report_summary": row.get("scouting_report_summary", ""),
                "scouting_why_he_wins": row.get("scouting_why_he_wins", ""),
                "scouting_primary_concerns": row.get("scouting_primary_concerns", ""),
                "scouting_production_snapshot": production_snapshot,
                "scouting_role_projection": row.get("scouting_role_projection", ""),
                "player_report_url": f"/players/{slug}",
            }
        )
    out.sort(key=lambda r: r["consensus_rank"])
    return out


def export_mock(path: Path) -> list[dict]:
    return export_mock_with_school_map(path, {}, {})


def export_mock_with_school_map(
    path: Path,
    player_school_map: dict[str, str],
    player_url_map: dict[str, str] | None = None,
) -> list[dict]:
    rows = _read_csv(path)
    out = []
    player_url_map = player_url_map or {}
    for row in rows:
        player_name = row.get("player_name", "")
        name_key = _norm_player_key(player_name)
        school = player_school_map.get(_norm_player_key(player_name), "")
        if not school:
            school = _canonical_school_name(row.get("school", ""))
        player_uid = row.get("player_uid", "")
        player_report_url = ""
        if player_uid and str(player_uid).strip() in player_url_map:
            player_report_url = player_url_map[str(player_uid).strip()]
        elif name_key in player_url_map:
            player_report_url = player_url_map[name_key]
        else:
            player_report_url = f"/players/{_slugify_player(player_name)}"

        out.append(
            {
                "round": _safe_int(row.get("round"), 0),
                "pick": _safe_int(row.get("pick"), 0),
                "overall_pick": _safe_int(row.get("overall_pick"), 0),
                "team": row.get("team", ""),
                "player_uid": player_uid,
                "player_name": player_name,
                "player_report_url": player_report_url,
                "position": row.get("position", ""),
                "school": school,
                "final_grade": round(_safe_float(row.get("final_grade")) or 0.0, 2),
                "round_value": row.get("round_value", ""),
                "pick_score": round(_safe_float(row.get("pick_score")) or 0.0, 4),
                "rank_driver_summary": row.get("rank_driver_summary", ""),
            }
        )
    out.sort(key=lambda r: r["overall_pick"])
    return out


def export_round7_team_groups(round7_rows: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for row in round7_rows:
        team = row.get("team", "")
        if not team:
            continue
        groups.setdefault(team, []).append(row)
    for team in list(groups.keys()):
        groups[team] = sorted(groups[team], key=lambda r: r.get("overall_pick", 9999))
    return groups


def export_team_needs() -> list[dict]:
    rows = _read_csv(TEAM_NEEDS_CSV)
    public_tx_by_team = _build_public_transactions(window_days=14)
    depth_context_by_team = _build_team_depth_context()

    by_team: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        team = (row.get("team") or "").strip()
        position = (row.get("position") or "").strip()
        if not team or not position:
            continue
        score = _needs_score(row)
        by_team[team].append(
            {
                "position": position,
                "need_score": round(score, 4),
                "depth_chart_pressure": round(_safe_float(row.get("depth_chart_pressure")) or 0.0, 3),
                "future_need_pressure_1y": round(_safe_float(row.get("future_need_pressure_1y")) or 0.0, 3),
                "future_need_pressure_2y": round(_safe_float(row.get("future_need_pressure_2y")) or 0.0, 3),
                "starter_quality": round(_safe_float(row.get("starter_quality")) or 0.0, 3),
            }
        )

    out: list[dict] = []
    for team, items in sorted(by_team.items(), key=lambda x: x[0]):
        items = sorted(items, key=lambda x: x["need_score"], reverse=True)
        ctx = depth_context_by_team.get(team, {})
        top_needs = items[:3]
        weakness_positions = [str(item.get("position", "")).upper() for item in top_needs if item.get("position")]
        out.append(
            {
                "team": team,
                "top_needs": top_needs,
                "weakness_positions": weakness_positions,
                "depth_chart": ctx.get("depth_chart", {"offense": [], "defense": [], "season": "", "week": ""}),
                "free_agents": ctx.get("free_agents", []),
                "young_players_on_rise": ctx.get("young_players_on_rise", []),
                "recent_transactions": public_tx_by_team.get(team, [])[:3],
            }
        )
    return out


def export_weekly_changes(board_rows: list[dict]) -> dict:
    rows = _read_csv(DELTA_AUDIT_LATEST_CSV)
    parsed: list[dict] = []
    for row in rows:
        delta = _safe_int(row.get("rank_delta_prev_minus_curr"), 0)
        if delta == 0:
            continue
        parsed.append(
            {
                "player_name": row.get("player_name", ""),
                "position": row.get("position", ""),
                "school": _canonical_school_name(row.get("school", "")),
                "rank_delta_prev_minus_curr": delta,
                "top_driver": (row.get("top_driver") or "").strip(),
            }
        )

    risers = sorted(
        [r for r in parsed if r["rank_delta_prev_minus_curr"] > 0],
        key=lambda r: r["rank_delta_prev_minus_curr"],
        reverse=True,
    )[:8]
    fallers = sorted(
        [r for r in parsed if r["rank_delta_prev_minus_curr"] < 0],
        key=lambda r: r["rank_delta_prev_minus_curr"],
    )[:8]

    qa_watch = sum(1 for r in board_rows if bool(r.get("low_evidence_flag")))
    qa_clear = max(0, len(board_rows) - qa_watch)

    return {
        "delta_source": str(DELTA_AUDIT_LATEST_CSV.name),
        "risers": risers,
        "fallers": fallers,
        "summary": {
            "movers_total": len(parsed),
            "risers_total": sum(1 for r in parsed if r["rank_delta_prev_minus_curr"] > 0),
            "fallers_total": sum(1 for r in parsed if r["rank_delta_prev_minus_curr"] < 0),
            "qa_watch_total": qa_watch,
            "qa_clear_total": qa_clear,
        },
    }


def main() -> None:
    player_school_map = _load_player_school_map()
    board = export_board(player_school_map)
    player_url_map = {}
    for row in board:
        uid = str(row.get("player_uid", "")).strip()
        url = str(row.get("player_report_url", "")).strip()
        if uid and url:
            player_url_map[uid] = url
        name_key = _norm_player_key(row.get("player_name", ""))
        if name_key and url:
            player_url_map[name_key] = url
    round1 = export_mock_with_school_map(ROUND1_CSV, player_school_map, player_url_map)
    round7 = export_mock_with_school_map(ROUND7_CSV, player_school_map, player_url_map)
    by_team = export_round7_team_groups(round7)
    team_needs = export_team_needs()
    weekly_changes = export_weekly_changes(board)
    transactions_feed = _build_transactions_feed(window_days=21)

    _write_json(ASTRO_DATA / "big_board_2026.json", board)
    _write_json(ASTRO_DATA / "mock_2026_round1.json", round1)
    _write_json(ASTRO_DATA / "mock_2026_7round.json", round7)
    _write_json(ASTRO_DATA / "mock_2026_7round_by_team.json", by_team)
    _write_json(ASTRO_DATA / "team_needs_2026.json", team_needs)
    _write_json(ASTRO_DATA / "weekly_changes_2026.json", weekly_changes)
    _write_json(ASTRO_DATA / "transactions_feed_2026.json", transactions_feed)
    _write_json(
        ASTRO_DATA / "build_meta.json",
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rows": {"board": len(board), "round1": len(round1), "round7": len(round7)},
        },
    )

    print(f"Wrote {ASTRO_DATA / 'big_board_2026.json'} ({len(board)} rows)")
    print(f"Wrote {ASTRO_DATA / 'mock_2026_round1.json'} ({len(round1)} rows)")
    print(f"Wrote {ASTRO_DATA / 'mock_2026_7round.json'} ({len(round7)} rows)")
    print(f"Wrote {ASTRO_DATA / 'mock_2026_7round_by_team.json'} ({len(by_team)} teams)")
    print(f"Wrote {ASTRO_DATA / 'team_needs_2026.json'} ({len(team_needs)} teams)")
    print(f"Wrote {ASTRO_DATA / 'weekly_changes_2026.json'}")
    print(f"Wrote {ASTRO_DATA / 'transactions_feed_2026.json'} ({len(transactions_feed)} rows)")
    print(f"Wrote {ASTRO_DATA / 'build_meta.json'}")


if __name__ == "__main__":
    main()
