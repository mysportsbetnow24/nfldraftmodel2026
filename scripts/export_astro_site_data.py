#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import polars as pl
except Exception:  # pragma: no cover
    pl = None


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "data" / "outputs"
ASTRO_DATA = ROOT / "astro-site" / "src" / "data"
INTERNAL_OUTPUTS = OUTPUTS / "internal"

BOARD_CSV = OUTPUTS / "big_board_2026.csv"
ROUND1_CSV = OUTPUTS / "mock_2026_round1.csv"
ROUND7_CSV = OUTPUTS / "mock_2026_7round.csv"
TEAM_NEEDS_CSV = ROOT / "data" / "sources" / "team_needs_context_2026.csv"
CBS_TRANSACTIONS_CSV = ROOT / "data" / "processed" / "cbs_nfl_transactions_2026.csv"
TRANSACTION_OVERRIDES_CSV = ROOT / "data" / "sources" / "manual" / "transactions_overrides_2026.csv"
INSIDER_TRANSACTIONS_CSV = ROOT / "data" / "sources" / "manual" / "insider_transactions_feed_2026.csv"
ESPN_PROSPECTS_CSV = ROOT / "data" / "sources" / "external" / "espn_nfl_draft_prospect_data" / "nfl_draft_prospects.csv"
ESPN_DEPTH_CHARTS_CSV = ROOT / "data" / "sources" / "external" / "espn_depth_charts_2026.csv"
DELTA_AUDIT_LATEST_CSV = OUTPUTS / "delta_audit_2026_latest.csv"
STABILITY_SNAPSHOTS_DIR = OUTPUTS / "stability_snapshots"
CURRENT_DRAFT_YEAR = 2026
NFLVERSE_DIR = ROOT / "data" / "sources" / "external" / "nflverse"
NFLVERSE_ROSTERS = NFLVERSE_DIR / "rosters_weekly.parquet"
NFLVERSE_CONTRACTS = NFLVERSE_DIR / "contracts.parquet"
NFLVERSE_PLAYERS = NFLVERSE_DIR / "players.parquet"
HISTORICAL_DRAFT_COMPILATION = ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data" / "notebook" / "compilations" / "drafts2015To2022.csv"
HISTORICAL_DRAFT_REFINED = ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data" / "old-data" / "pfr-compilations" / "2014To2018Drafts-refined.csv"


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
    if not text:
        return ""
    tokens = re.sub(r"[^a-z0-9\s]", " ", text).split()
    suffix_tokens = {"jr", "sr", "ii", "iii", "iv", "v"}
    filtered = [tok for tok in tokens if tok not in suffix_tokens]
    if not filtered:
        filtered = tokens
    return "".join(filtered)


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
        "pending official combine ras",
        "pending until more verified testing metrics are available",
        "production snapshot pending",
        "summary pending",
        "context pending",
        "projection pending",
        "concerns pending",
        "role pending",
        "no structured 2025 kiper production snapshot ingested yet",
    ]
    for raw in text.splitlines():
        line = str(raw or "").strip()
        if not line:
            continue
        lowered = line.lower()
        if any(token in lowered for token in banned_tokens):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    if not cleaned.strip():
        return ""
    return cleaned.strip()


TEAM_NEEDS_POS_ORDER = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]
OFFENSE_POS_ORDER = ["QB", "RB", "WR", "TE", "OT", "IOL"]
DEFENSE_POS_ORDER = ["EDGE", "DT", "LB", "CB", "S"]
STARTERS_BY_POSITION = {"QB": 1, "RB": 1, "WR": 2, "TE": 1, "OT": 2, "IOL": 3, "EDGE": 2, "DT": 2, "LB": 2, "CB": 2, "S": 2}
OFFENSE_LANE_SLOT_ORDER = {
    "QB": ["QB"],
    "RB": ["RB", "HB", "FB"],
    "WR": ["XWR", "ZWR", "LWR", "RWR", "SWR", "WR"],
    "TE": ["TE"],
    "OT": ["LT", "RT", "T"],
    "IOL": ["LG", "C", "RG", "G", "OC"],
}
DEFENSE_LANE_SLOT_ORDER_34 = {
    "EDGE": ["LOLB", "ROLB", "SLB", "WLB", "OLB", "EDGE", "ED"],
    "DT": ["LDE", "RDE", "DE", "LDT", "RDT", "DT", "NT", "IDL"],
    "LB": ["LILB", "RILB", "ILB", "MLB", "LB"],
    "CB": ["LCB", "RCB", "NB", "CB"],
    "S": ["FS", "SS", "S", "SAF"],
}
DEFENSE_LANE_SLOT_ORDER_43 = {
    "EDGE": ["LDE", "RDE", "DE", "EDGE", "ED"],
    "DT": ["LDT", "RDT", "DT", "NT", "IDL"],
    "LB": ["WLB", "MLB", "SLB", "LOLB", "ROLB", "OLB", "ILB", "LB"],
    "CB": ["LCB", "RCB", "NB", "CB"],
    "S": ["FS", "SS", "S", "SAF"],
}
DEFENSE_LANE_SLOT_ORDER_GENERIC = {
    "EDGE": ["LDE", "RDE", "DE", "LOLB", "ROLB", "OLB", "EDGE", "ED"],
    "DT": ["LDT", "RDT", "DT", "NT", "IDL"],
    "LB": ["WLB", "MLB", "SLB", "LILB", "RILB", "ILB", "LB"],
    "CB": ["LCB", "RCB", "NB", "CB"],
    "S": ["FS", "SS", "S", "SAF"],
}


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
    if pos in {"LWR", "RWR", "SWR", "XWR", "ZWR"}:
        return "WR"
    if pos in {"HB", "FB"}:
        return "RB"
    if pos in {"T", "LT", "RT"}:
        return "OT"
    if pos in {"G", "LG", "RG", "C", "OC"}:
        return "IOL"
    if pos in {"DE", "ED", "LDE", "RDE", "LOLB", "ROLB"}:
        return "EDGE"
    if pos in {"NT", "IDL", "LDT", "RDT"}:
        return "DT"
    if pos in {"ILB", "MLB", "LB", "SLB", "WLB", "LILB", "RILB"}:
        return "LB"
    if pos in {"FS", "SS", "SAF", "RS", "LS"}:
        return "S"
    if pos in {"LCB", "RCB", "NB"}:
        return "CB"
    if pos == "OL":
        if depth in {"LT", "RT", "T"}:
            return "OT"
        if depth in {"LG", "RG", "G", "C", "OC"}:
            return "IOL"
    if pos == "DL":
        if depth in {"DE", "ED", "EDGE", "OLB", "LDE", "RDE", "LOLB", "ROLB"}:
            return "EDGE"
        return "DT"
    if pos == "DB":
        if depth in {"LCB", "RCB", "CB", "NB"}:
            return "CB"
        return "S"
    return ""


def _team_front_family(position_groups: list[str]) -> str:
    counts = Counter()
    for raw in position_groups:
        text = str(raw or "").strip().lower()
        if not text:
            continue
        if "3-4" in text:
            counts["3-4"] += 1
        elif "4-3" in text:
            counts["4-3"] += 1
    if counts["3-4"] >= counts["4-3"] and counts["3-4"] > 0:
        return "3-4"
    if counts["4-3"] > 0:
        return "4-3"
    return "generic"


def _lane_slot_order(position: str, front_family: str) -> list[str]:
    if position in OFFENSE_POS_ORDER:
        return OFFENSE_LANE_SLOT_ORDER.get(position, [position])
    if front_family == "3-4":
        return DEFENSE_LANE_SLOT_ORDER_34.get(position, [position])
    if front_family == "4-3":
        return DEFENSE_LANE_SLOT_ORDER_43.get(position, [position])
    return DEFENSE_LANE_SLOT_ORDER_GENERIC.get(position, [position])


def _slot_display_label(position: str, slot: str, front_family: str, slot_rank: int) -> str:
    slot = str(slot or "").strip().upper()
    if position == "QB":
        return f"QB{slot_rank}"
    if position == "RB":
        if slot == "FB":
            return "Fullback"
        return f"RB{slot_rank}"
    if position == "WR":
        if slot in {"SWR", "SLOT"}:
            return "Slot WR"
        if slot == "XWR":
            return "X WR"
        if slot == "ZWR":
            return "Z WR"
        if slot in {"LWR", "RWR", "WR"}:
            return f"WR{slot_rank}"
    if position == "TE":
        return f"TE{slot_rank}"
    if position == "OT":
        return {"LT": "LT", "RT": "RT", "T": "Swing OT"}.get(slot, f"OT{slot_rank}")
    if position == "IOL":
        return {
            "LG": "LG",
            "C": "C",
            "OC": "C",
            "RG": "RG",
            "G": f"G{slot_rank}",
        }.get(slot, f"IOL{slot_rank}")
    if position == "EDGE":
        if front_family == "3-4":
            if slot in {"LOLB", "ROLB", "OLB"}:
                return f"Rush OLB {slot_rank}"
            if slot in {"SLB", "WLB"}:
                return f"Edge {slot_rank}"
        return f"Edge {slot_rank}"
    if position == "DT":
        if front_family == "3-4":
            if slot in {"LDE", "RDE", "DE"}:
                return "5-Tech"
            if slot == "NT":
                return "Nose"
        return {
            "LDT": "DT",
            "RDT": "DT",
            "DT": "DT",
            "IDL": "IDL",
            "NT": "Nose",
        }.get(slot, f"DT{slot_rank}")
    if position == "LB":
        return {
            "MLB": "Mike",
            "LILB": "ILB",
            "RILB": "ILB",
            "ILB": "ILB",
            "WLB": "Will",
            "SLB": "Sam",
            "LB": f"LB{slot_rank}",
        }.get(slot, f"LB{slot_rank}")
    if position == "CB":
        return {
            "LCB": "CB1",
            "RCB": "CB2",
            "NB": "Nickel",
            "CB": f"CB{slot_rank}",
        }.get(slot, f"CB{slot_rank}")
    if position == "S":
        return {
            "FS": "FS",
            "SS": "SS",
            "S": f"S{slot_rank}",
            "SAF": f"S{slot_rank}",
        }.get(slot, f"S{slot_rank}")
    return f"{position}{slot_rank}"


def _player_sort_tuple(player: dict, slot_priority: dict[str, int]) -> tuple:
    slot = str(player.get("depth_chart_position") or "").strip().upper()
    return (
        slot_priority.get(slot, 99),
        _safe_int(player.get("espn_rank"), 99),
        0 if player.get("has_contract") else 1,
        -(float(player.get("apy_m") or 0.0)),
        -int(player.get("years_exp") or 0),
        int(player.get("draft_number") or 9999),
        str(player.get("player_name", "")),
    )


def _player_detail_line(player: dict, role_label: str) -> str:
    parts = [role_label]
    contract = str(player.get("contract_label") or "").strip()
    if contract:
        parts.append(contract)
    return " | ".join(parts)


def _player_meta_line(player: dict) -> str:
    parts = []
    years_exp = _safe_int(player.get("years_exp"), 0)
    age = _safe_int(player.get("age"), 0)
    if years_exp > 0:
        parts.append(f"{years_exp} yrs exp")
    if age > 0:
        parts.append(f"Age {age}")
    return " | ".join(parts)


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


def _usage_proxy_from_role(model_position: str, role_label: str, depth_rank: int) -> float:
    """
    Lightweight role-based usage proxy for public depth-chart labels.
    This is not true snap share; it approximates on-field importance from
    lane role and depth placement so labels read like football hierarchy.
    """
    role = str(role_label or "").strip().upper()
    pos = str(model_position or "").strip().upper()
    rank = max(1, int(depth_rank or 1))

    role_bases = {
        "QB1": 1.0,
        "QB2": 0.32,
        "QB3": 0.12,
        "RB1": 0.88,
        "RB2": 0.58,
        "RB3": 0.28,
        "FULLBACK": 0.18,
        "X WR": 0.9,
        "Z WR": 0.88,
        "SLOT WR": 0.82,
        "WR1": 0.88,
        "WR2": 0.8,
        "WR3": 0.5,
        "TE1": 0.78,
        "TE2": 0.42,
        "TE3": 0.22,
        "LT": 0.95,
        "RT": 0.9,
        "SWING OT": 0.38,
        "LG": 0.83,
        "C": 0.9,
        "RG": 0.83,
        "G1": 0.76,
        "G2": 0.68,
        "EDGE 1": 0.9,
        "EDGE 2": 0.82,
        "RUSH OLB 1": 0.88,
        "RUSH OLB 2": 0.8,
        "NOSE": 0.78,
        "5-TECH": 0.78,
        "DT": 0.76,
        "IDL": 0.7,
        "MIKE": 0.84,
        "WILL": 0.8,
        "SAM": 0.72,
        "ILB": 0.78,
        "LB1": 0.74,
        "LB2": 0.68,
        "CB1": 0.9,
        "CB2": 0.84,
        "NICKEL": 0.78,
        "FS": 0.82,
        "SS": 0.8,
        "S1": 0.78,
        "S2": 0.72,
    }
    if role in role_bases:
        return role_bases[role]

    if pos in {"QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"}:
        starter_cutoff = int(STARTERS_BY_POSITION.get(pos, 1))
        if rank <= starter_cutoff:
            return max(0.62, 0.9 - (0.08 * (rank - 1)))
        return max(0.12, 0.5 - (0.1 * (rank - starter_cutoff - 1)))
    return 0.25


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _player_designation(
    *,
    has_contract: bool,
    years_exp: int,
    age: int | None,
    apy_pct: float,
    apy_m: float | None,
    contract_years: int,
    depth_rank: int,
    model_position: str,
    draft_number: int | None,
    role_label: str,
) -> str:
    if not has_contract:
        return "FA"
    starter_cutoff = int(STARTERS_BY_POSITION.get(model_position, 1))
    is_top_starter = depth_rank <= starter_cutoff
    usage_proxy = _usage_proxy_from_role(model_position, role_label, depth_rank)
    veteran_inference = (
        years_exp <= 1
        and (
            (age is not None and age >= 28)
            or (contract_years > 0 and contract_years <= 3 and ((apy_m or 0.0) >= 4.0))
            or ((apy_m or 0.0) >= 8.0)
        )
    )
    effective_years_exp = years_exp if years_exp > 0 else (4 if veteran_inference else 0)
    is_early_pick = draft_number is not None and draft_number > 0 and draft_number <= 40
    likely_young_player = (
        (age is not None and age <= 26)
        or is_early_pick
        or years_exp > 0
    )

    if usage_proxy >= 0.95 and apy_pct >= 97.0 and effective_years_exp >= 6:
        return "HOF Path"
    if usage_proxy >= 0.86 and apy_pct >= 90.0 and effective_years_exp >= 4:
        return "All-Pro"
    if usage_proxy >= 0.62 and (
        (age is not None and age >= 33 and effective_years_exp >= 8)
        or (veteran_inference and (apy_m or 0.0) >= 8.0 and contract_years <= 2)
        or (model_position == "QB" and age is not None and age >= 34)
    ):
        return "Older Mentor"
    if usage_proxy >= 0.84 and (
        (apy_pct >= 78.0 and effective_years_exp >= 2)
        or (apy_pct >= 70.0 and (apy_m or 0.0) >= 10.0)
        or (is_early_pick and effective_years_exp <= 2 and is_top_starter)
    ) and (age is None or age <= 31):
        return "Franchise Cornerstone"
    if usage_proxy >= 0.78 and apy_pct >= 70.0 and 3 <= effective_years_exp <= 9 and age is not None and age <= 31:
        return "In His Prime Star"
    if years_exp <= 1 and not veteran_inference:
        if is_early_pick and usage_proxy >= 0.62:
            return "Blue Chip Prospect"
        if usage_proxy >= 0.4 and likely_young_player:
            return "Prospect"
    if usage_proxy >= 0.56:
        return "Starter"
    if years_exp <= 2 and usage_proxy >= 0.35 and likely_young_player:
        return "Prospect"
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

    players_master_by_name: dict[str, dict] = {}
    if NFLVERSE_PLAYERS.exists():
        players_master = pl.read_parquet(NFLVERSE_PLAYERS)
        if not players_master.is_empty():
            for row in players_master.iter_rows(named=True):
                name = str(row.get("display_name") or row.get("football_name") or "").strip()
                key = _norm_player_key(name)
                if not key:
                    continue
                existing = players_master_by_name.get(key)
                rookie_season = _safe_int(row.get("rookie_season"), 0)
                years_of_experience = _safe_int(row.get("years_of_experience"), 0)
                draft_pick = _safe_int(row.get("draft_pick"), 0)
                latest_team = str(row.get("latest_team") or "").strip().upper()
                status = str(row.get("status") or "").strip().upper()
                payload = {
                    "display_name": name,
                    "latest_team": latest_team,
                    "status": status,
                    "rookie_season": rookie_season,
                    "years_of_experience": years_of_experience,
                    "draft_pick": draft_pick,
                    "birth_date": str(row.get("birth_date") or "").strip(),
                    "position": str(row.get("position") or "").strip().upper(),
                }
                if existing is None:
                    players_master_by_name[key] = payload
                    continue
                existing_score = (
                    1 if str(existing.get("latest_team") or "").strip() else 0,
                    1 if str(existing.get("status") or "").strip() in {"ACT", "RES", "INA", "DEV", "EXE", "SUS", "RSR", "RSN"} else 0,
                    int(existing.get("rookie_season") or 0),
                    int(existing.get("years_of_experience") or 0),
                    -(int(existing.get("draft_pick") or 9999)),
                )
                new_score = (
                    1 if latest_team else 0,
                    1 if status in {"ACT", "RES", "INA", "DEV", "EXE", "SUS", "RSR", "RSN"} else 0,
                    rookie_season,
                    years_of_experience,
                    -(draft_pick or 9999),
                )
                if new_score > existing_score:
                    players_master_by_name[key] = payload

    contract_rows = []
    apy_pool_by_pos: dict[str, list[float]] = defaultdict(list)
    contract_by_gsis: dict[str, dict] = {}
    contract_by_name: dict[str, dict] = {}
    contract_players_by_team_pos: dict[tuple[str, str], list[dict]] = defaultdict(list)
    if NFLVERSE_CONTRACTS.exists():
        contracts = pl.read_parquet(NFLVERSE_CONTRACTS)
        if not contracts.is_empty():
            contract_rows = list(contracts.filter(pl.col("is_active") == True).iter_rows(named=True))
    for row in contract_rows:
        pos = _map_team_needs_position(row.get("position", ""))
        apy = _safe_float(row.get("apy"))
        if pos and apy is not None and apy > 0:
            apy_pool_by_pos[pos].append(float(apy))
    for row in contract_rows:
        gsis = str(row.get("gsis_id") or "").strip()
        name = str(row.get("player") or "").strip()
        apy = _safe_float(row.get("apy"))
        years = _safe_int(row.get("years"), 0)
        pos = _map_team_needs_position(row.get("position", ""))
        team_text = str(row.get("team") or "").strip()
        team_norm = ""
        lowered_team = team_text.lower()
        for code, aliases in {
            "ARI": ["arizona cardinals", "cardinals"],
            "ATL": ["atlanta falcons", "falcons"],
            "BAL": ["baltimore ravens", "ravens"],
            "BUF": ["buffalo bills", "bills"],
            "CAR": ["carolina panthers", "panthers"],
            "CHI": ["chicago bears", "bears"],
            "CIN": ["cincinnati bengals", "bengals"],
            "CLE": ["cleveland browns", "browns"],
            "DAL": ["dallas cowboys", "cowboys"],
            "DEN": ["denver broncos", "broncos"],
            "DET": ["detroit lions", "lions"],
            "GB": ["green bay packers", "packers"],
            "HOU": ["houston texans", "texans"],
            "IND": ["indianapolis colts", "colts"],
            "JAX": ["jacksonville jaguars", "jaguars"],
            "KC": ["kansas city chiefs", "chiefs"],
            "LAC": ["los angeles chargers", "chargers"],
            "LAR": ["los angeles rams", "rams"],
            "LV": ["las vegas raiders", "raiders"],
            "MIA": ["miami dolphins", "dolphins"],
            "MIN": ["minnesota vikings", "vikings"],
            "NE": ["new england patriots", "patriots"],
            "NO": ["new orleans saints", "saints"],
            "NYG": ["new york giants", "giants"],
            "NYJ": ["new york jets", "jets"],
            "PHI": ["philadelphia eagles", "eagles"],
            "PIT": ["pittsburgh steelers", "steelers"],
            "SEA": ["seattle seahawks", "seahawks"],
            "SF": ["san francisco 49ers", "49ers", "niners"],
            "TB": ["tampa bay buccaneers", "buccaneers", "bucs"],
            "TEN": ["tennessee titans", "titans"],
            "WAS": ["washington commanders", "commanders"],
        }.items():
            if lowered_team in aliases:
                team_norm = code
                break
        payload = {
            "gsis_id": gsis,
            "name_key": _norm_player_key(name),
            "apy": apy,
            "years": years,
            "position": pos,
            "team_text": team_text,
        }
        if team_norm and pos and name:
            contract_players_by_team_pos[(team_norm, pos)].append(
                {
                    "player_name": name,
                    "position": pos,
                    "depth_chart_position": pos,
                    "years_exp": 0,
                    "age": "",
                    "draft_number": "",
                    "contract_years": years,
                    "has_contract": True,
                    "contract_label": f"{years}y | ${apy:.1f}M APY" if apy is not None else f"{years}y contract",
                    "apy_m": round(apy, 2) if apy is not None else "",
                    "apy_pct": _pct_score(apy, apy_pool_by_pos.get(pos, [])),
                }
            )
        if gsis:
            existing = contract_by_gsis.get(gsis)
            if existing is None or (_safe_float(existing.get("apy")) or 0.0) < (_safe_float(apy) or 0.0):
                contract_by_gsis[gsis] = payload
        if payload["name_key"]:
            existing = contract_by_name.get(payload["name_key"])
            if existing is None or (_safe_float(existing.get("apy")) or 0.0) < (_safe_float(apy) or 0.0):
                contract_by_name[payload["name_key"]] = payload

    team_players: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    roster_lookup_by_team: dict[str, dict[str, dict]] = defaultdict(dict)
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

        roster_lookup_by_team[team][_norm_player_key(name)] = {
            "player_name": name,
            "position": model_pos,
            "depth_chart_position": depth_pos,
            "years_exp": years_exp,
            "age": age if age is not None else "",
            "draft_number": draft_number if draft_number is not None else "",
            "contract_years": years_left,
            "has_contract": has_contract,
            "contract_label": contract_label,
            "apy_m": round(apy, 2) if apy is not None else "",
            "apy_pct": apy_pct,
        }

        team_players[team][model_pos].append(
            {
                "player_name": name,
                "position": model_pos,
                "depth_chart_position": depth_pos,
                "years_exp": years_exp,
                "age": age if age is not None else "",
                "draft_number": draft_number if draft_number is not None else "",
                "contract_years": years_left,
                "has_contract": has_contract,
                "contract_label": contract_label,
                "apy_m": round(apy, 2) if apy is not None else "",
                "apy_pct": apy_pct,
            }
        )

    espn_depth_rows = _read_csv_rows(ESPN_DEPTH_CHARTS_CSV)
    espn_by_team_pos: dict[tuple[str, str], list[dict]] = defaultdict(list)
    espn_by_team_slot: dict[tuple[str, str], list[dict]] = defaultdict(list)
    espn_defense_groups_by_team: dict[str, list[str]] = defaultdict(list)
    skipped_espn_rows: list[dict] = []
    for row in espn_depth_rows:
        team = str(row.get("team") or "").strip().upper()
        player_name = str(row.get("player_name") or "").strip()
        if not team or not player_name:
            continue
        slot = str(row.get("position_slot") or row.get("position_key") or row.get("position_abbreviation") or "").strip().upper()
        model_pos = _map_team_needs_position(
            row.get("position_abbreviation", ""),
            slot,
        )
        if model_pos not in TEAM_NEEDS_POS_ORDER:
            continue
        player_key = _norm_player_key(player_name)
        roster_info = roster_lookup_by_team.get(team, {}).get(player_key, {})
        contract = contract_by_name.get(player_key)
        player_master = players_master_by_name.get(player_key, {})
        latest_team_match = (
            bool(player_master)
            and str(player_master.get("latest_team") or "").strip().upper() == team
            and str(player_master.get("status") or "").strip().upper() not in {"RET", "CUT"}
        )
        if not roster_info and not contract and not latest_team_match:
            skipped_espn_rows.append(
                {
                    "team": team,
                    "player_name": player_name,
                    "position_slot": slot,
                    "model_position": model_pos,
                    "reason": "no_roster_contract_or_players_match",
                    "espn_rank": _safe_int(row.get("rank"), 99),
                }
            )
            continue
        apy = _safe_float(contract.get("apy")) if contract else _safe_float(roster_info.get("apy_m"))
        apy_pct = (
            _pct_score(apy, apy_pool_by_pos.get(model_pos, []))
            if apy is not None
            else float(roster_info.get("apy_pct") or 0.0)
        )
        years_left = _safe_int(contract.get("years"), 0) if contract else 0
        has_contract = bool(contract) or bool(roster_info) or latest_team_match
        if contract:
            contract_label = f"{years_left}y | ${apy:.1f}M APY" if apy is not None else f"{years_left}y contract"
        elif roster_info:
            contract_label = str(roster_info.get("contract_label") or "Rostered")
        elif latest_team_match and _safe_int(player_master.get("rookie_season"), 0) >= 2025:
            contract_label = "Rookie deal"
        elif latest_team_match:
            contract_label = "Rostered"
        else:
            contract_label = "FA"

        payload = {
            "player_name": player_name,
            "position": model_pos,
            "depth_chart_position": slot,
            "years_exp": _safe_int(roster_info.get("years_exp"), 0) or _safe_int(player_master.get("years_of_experience"), 0),
            "age": roster_info.get("age", "") or _parse_birth_years(player_master.get("birth_date", "")) or "",
            "draft_number": roster_info.get("draft_number", "") or _safe_int(player_master.get("draft_pick"), 0) or "",
            "contract_years": years_left if contract else _safe_int(roster_info.get("contract_years"), 0),
            "has_contract": has_contract,
            "contract_label": contract_label,
            "apy_m": round(apy, 2) if apy is not None else roster_info.get("apy_m", ""),
            "apy_pct": apy_pct,
            "depth_source": "espn",
            "espn_rank": _safe_int(row.get("rank"), 99),
            "position_group": str(row.get("position_group") or "").strip(),
        }
        espn_by_team_pos[(team, model_pos)].append(payload)
        if slot:
            espn_by_team_slot[(team, slot)].append(payload)
        if model_pos in DEFENSE_POS_ORDER and payload["position_group"]:
            espn_defense_groups_by_team[team].append(payload["position_group"])

    for (team, pos), rows in espn_by_team_pos.items():
        normalized_players = []
        seen_names: set[str] = set()
        for row in sorted(
            rows,
            key=lambda item: (
                _safe_int(item.get("espn_rank"), 99),
                str(item.get("depth_chart_position") or ""),
                str(item.get("player_name") or ""),
            ),
        ):
            player_name = str(row.get("player_name") or "").strip()
            player_key = _norm_player_key(player_name)
            if not player_key or player_key in seen_names:
                continue
            seen_names.add(player_key)
            normalized_players.append(row)

        if normalized_players:
            existing_players = team_players[team].get(pos, [])
            existing_keys = { _norm_player_key(p.get("player_name", "")) for p in normalized_players }
            for player in existing_players:
                player_key = _norm_player_key(player.get("player_name", ""))
                if player_key and player_key not in existing_keys and len(normalized_players) < 4:
                    normalized_players.append(player)
                    existing_keys.add(player_key)
            for player in sorted(
                contract_players_by_team_pos.get((team, pos), []),
                key=lambda p: (
                    -(float(p.get("apy_m") or 0.0)),
                    str(p.get("player_name", "")),
                ),
            ):
                player_key = _norm_player_key(player.get("player_name", ""))
                if player_key and player_key not in existing_keys and len(normalized_players) < 4:
                    normalized_players.append(player)
                    existing_keys.add(player_key)
            team_players[team][pos] = normalized_players

    for (team, pos), players in contract_players_by_team_pos.items():
        existing_players = team_players[team].get(pos, [])
        existing_keys = {_norm_player_key(p.get("player_name", "")) for p in existing_players}
        merged_players = list(existing_players)
        for player in sorted(
            players,
            key=lambda p: (
                -(float(p.get("apy_m") or 0.0)),
                str(p.get("player_name", "")),
            ),
        ):
            player_key = _norm_player_key(player.get("player_name", ""))
            if player_key and player_key not in existing_keys and len(merged_players) < 4:
                merged_players.append(player)
                existing_keys.add(player_key)
        if merged_players:
            team_players[team][pos] = merged_players

    out: dict[str, dict] = {}
    for team, by_pos in team_players.items():
        offense_lanes = []
        defense_lanes = []
        free_agents = []
        youth = []
        front_family = _team_front_family(espn_defense_groups_by_team.get(team, []))
        for pos in TEAM_NEEDS_POS_ORDER:
            lane_slots = _lane_slot_order(pos, front_family)
            slot_priority = {slot: idx for idx, slot in enumerate(lane_slots)}
            selected_players = []
            selected_keys: set[str] = set()
            for slot in lane_slots:
                for player in sorted(
                    espn_by_team_slot.get((team, slot), []),
                    key=lambda p: (
                        _safe_int(p.get("espn_rank"), 99),
                        0 if p.get("has_contract") else 1,
                        -(float(p.get("apy_m") or 0.0)),
                        str(p.get("player_name", "")),
                    ),
                ):
                    player_key = _norm_player_key(player.get("player_name", ""))
                    if not player_key or player_key in selected_keys:
                        continue
                    selected_players.append(player)
                    selected_keys.add(player_key)
                    if len(selected_players) >= 4:
                        break
                if len(selected_players) >= 4:
                    break

            players = sorted(by_pos.get(pos, []), key=lambda p: _player_sort_tuple(p, slot_priority))
            for player in players:
                player_key = _norm_player_key(player.get("player_name", ""))
                if not player_key or player_key in selected_keys:
                    continue
                selected_players.append(player)
                selected_keys.add(player_key)
                if len(selected_players) >= 4:
                    break

            selected_players = sorted(selected_players, key=lambda p: _player_sort_tuple(p, slot_priority))

            lane_players = []
            slot_counts: dict[str, int] = defaultdict(int)
            for idx, player in enumerate(selected_players, start=1):
                slot = str(player.get("depth_chart_position") or "").strip().upper()
                slot_counts[slot] += 1
                role_label = _slot_display_label(pos, slot, front_family, slot_counts[slot] or idx)
                label = _player_designation(
                    has_contract=bool(player.get("has_contract")),
                    years_exp=int(player.get("years_exp") or 0),
                    age=int(player.get("age")) if str(player.get("age", "")).strip() else None,
                    apy_pct=float(player.get("apy_pct") or 0.0),
                    apy_m=_safe_float(player.get("apy_m")),
                    contract_years=int(player.get("contract_years") or 0),
                    depth_rank=idx,
                    model_position=pos,
                    draft_number=int(player.get("draft_number")) if str(player.get("draft_number", "")).strip() else None,
                    role_label=role_label,
                )
                payload = {
                    "player_name": player.get("player_name", ""),
                    "position": pos,
                    "depth_rank": idx,
                    "designation": label,
                    "role_label": role_label,
                    "detail_label": _player_detail_line(player, role_label),
                    "meta_label": _player_meta_line(player),
                    "contract_label": player.get("contract_label", ""),
                    "years_exp": int(player.get("years_exp") or 0),
                    "age": player.get("age", ""),
                    "apy_m": player.get("apy_m", ""),
                }
                lane_players.append(payload)
                if label == "FA" and len(free_agents) < 12:
                    free_agents.append(payload)
                if (
                    int(player.get("years_exp") or 0) <= 3
                    and int(payload.get("depth_rank") or 99) <= 2
                    and (
                        not str(player.get("age", "")).strip()
                        or _safe_int(player.get("age"), 0) <= 27
                    )
                    and label in {"Prospect", "Blue Chip Prospect", "Starter", "Franchise Cornerstone"}
                ):
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

    INTERNAL_OUTPUTS.mkdir(parents=True, exist_ok=True)
    qa_md = INTERNAL_OUTPUTS / "espn_depth_chart_publish_qa_2026.md"
    lines = [
        "# ESPN Depth Chart Publish QA",
        "",
        f"- Generated: {datetime.now(timezone.utc).isoformat()}",
        f"- ESPN rows scanned: {len(espn_depth_rows)}",
        f"- ESPN rows skipped before publish: {len(skipped_espn_rows)}",
        "",
    ]
    if skipped_espn_rows:
        lines.extend(
            [
                "| Team | Player | Slot | Model Pos | Reason | ESPN Rank |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in sorted(
            skipped_espn_rows,
            key=lambda r: (str(r.get("team", "")), str(r.get("model_position", "")), int(r.get("espn_rank", 99)), str(r.get("player_name", ""))),
        )[:500]:
            lines.append(
                f"| {row.get('team','')} | {row.get('player_name','')} | {row.get('position_slot','')} | {row.get('model_position','')} | {row.get('reason','')} | {row.get('espn_rank','')} |"
            )
        if len(skipped_espn_rows) > 500:
            lines.extend(
                [
                    "",
                    f"_Truncated to first 500 skipped rows; total skipped rows: {len(skipped_espn_rows)}._",
                ]
            )
    else:
        lines.append("No skipped ESPN depth-chart rows.")
    qa_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
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
        return (0.35, 0.65)
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


def _public_comp_dict(comp: dict) -> dict:
    if not comp:
        return {}
    return {
        "name": comp.get("name", ""),
        "similarity": comp.get("similarity"),
        "year": comp.get("year"),
    }


def _load_historical_comp_outcomes() -> dict[tuple[str, int], dict]:
    sources = [HISTORICAL_DRAFT_COMPILATION, HISTORICAL_DRAFT_REFINED]
    outcomes: dict[tuple[str, int], dict] = {}
    for path in sources:
        if not path.exists():
            continue
        with path.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = str(row.get("Player") or row.get("player_name") or "").strip()
                year = _safe_int(row.get("DraftYear") or row.get("Year"), 0)
                if not name or year <= 0 or year >= CURRENT_DRAFT_YEAR:
                    continue
                key = (_norm_comp_identity_key(name), year)
                drav = _safe_float(row.get("DrAV") or row.get("career_value"))
                wav = _safe_float(row.get("wAV") or row.get("CarAV"))
                value_per_year = _safe_float(row.get("ValuePerYear"))
                starter_seasons = _safe_float(row.get("St"))
                pro_bowls = _safe_float(row.get("PB"))
                all_pros = _safe_float(row.get("AP1"))
                games = _safe_float(row.get("G"))
                outcome_score = 0.0
                if drav is not None:
                    outcome_score += min(float(drav), 70.0) / 70.0 * 48.0
                elif wav is not None:
                    outcome_score += min(float(wav), 90.0) / 90.0 * 40.0
                if value_per_year is not None:
                    outcome_score += min(float(value_per_year), 12.0) / 12.0 * 18.0
                if starter_seasons is not None:
                    outcome_score += min(float(starter_seasons), 8.0) / 8.0 * 12.0
                if pro_bowls is not None:
                    outcome_score += min(float(pro_bowls), 8.0) / 8.0 * 12.0
                if all_pros is not None:
                    outcome_score += min(float(all_pros), 4.0) / 4.0 * 10.0
                if games is not None:
                    outcome_score += min(float(games), 120.0) / 120.0 * 6.0
                existing = outcomes.get(key)
                payload = {
                    "name": name,
                    "year": year,
                    "drav": drav,
                    "wav": wav,
                    "value_per_year": value_per_year,
                    "starter_seasons": starter_seasons,
                    "pro_bowls": pro_bowls,
                    "all_pros": all_pros,
                    "games": games,
                    "outcome_score": round(outcome_score, 3),
                }
                if existing is None or float(payload["outcome_score"]) > float(existing.get("outcome_score", 0.0) or 0.0):
                    outcomes[key] = payload
    return outcomes


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
    comp_outcomes = _load_historical_comp_outcomes()
    consensus_mean_population = [
        float(_safe_float(row.get("consensus_board_mean_rank")) or 0.0)
        for row in rows
        if (_safe_float(row.get("consensus_board_mean_rank")) or 0.0) > 0
    ]

    # Position-normalized metric populations for percentile context.
    pos_metric_values: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    pos_athletic_profile_values: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        pos = str(row.get("position", "")).strip().upper()
        if not pos:
            continue
        athletic_profile_score = _safe_float(row.get("athletic_profile_score"))
        if athletic_profile_score is not None and athletic_profile_score > 0:
            pos_athletic_profile_values[pos].append(float(athletic_profile_score))
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
        athletic_profile_score = _safe_float(row.get("athletic_profile_score"))
        athletic_metric_coverage_rate = _safe_float(row.get("athletic_metric_coverage_rate"))
        combine_ras_official = round(_safe_float(row.get("combine_ras_official")) or 0.0, 2)
        ras_estimate = round(_safe_float(row.get("ras_estimate")) or 0.0, 2)
        production_snapshot = _clean_public_snapshot(row.get("scouting_production_snapshot", "") or "")
        low_evidence_flag = pff_grade <= 0 and combine_ras_official <= 0 and ras_estimate <= 0
        athletic_percentile = _pct_rank(
            float(athletic_profile_score),
            pos_athletic_profile_values.get(pos, []),
        ) if athletic_profile_score is not None and athletic_profile_score > 0 else None
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
            outcome = comp_outcomes.get((identity_key, int(slot.get("year") or 0)), {})
            comp_items.append(
                {
                    "name": slot.get("name", ""),
                    "similarity": round(blend_score, 3),
                    "year": slot.get("year"),
                    "outcome_score": outcome.get("outcome_score"),
                }
            )

        comp_items = sorted(
            comp_items,
            key=lambda r: (r.get("similarity") is None, -(r.get("similarity") or 0.0)),
        )
        outcome_pool = [item for item in comp_items[:10] if item.get("outcome_score") is not None]
        if len(outcome_pool) >= 3:
            outcome_sorted = sorted(
                outcome_pool,
                key=lambda r: (
                    float(r.get("outcome_score") or 0.0),
                    -(r.get("similarity") or 0.0),
                ),
            )
            mid_idx = len(outcome_sorted) // 2
            comp_floor = outcome_sorted[0]
            comp_median = outcome_sorted[mid_idx]
            comp_ceiling = outcome_sorted[-1]
        elif len(comp_items) >= 3:
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
                "athletic_profile_score": round(float(athletic_profile_score), 3) if athletic_profile_score is not None and athletic_profile_score > 0 else None,
                "athletic_metric_coverage_rate": round(float(athletic_metric_coverage_rate), 4) if athletic_metric_coverage_rate is not None and athletic_metric_coverage_rate >= 0 else None,
                "athletic_percentile": athletic_percentile,
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
                "historical_comp_floor": _public_comp_dict(comp_floor),
                "historical_comp_median": _public_comp_dict(comp_median),
                "historical_comp_ceiling": _public_comp_dict(comp_ceiling),
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
