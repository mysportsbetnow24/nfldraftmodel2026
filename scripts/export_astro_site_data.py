#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "data" / "outputs"
ASTRO_DATA = ROOT / "astro-site" / "src" / "data"

BOARD_CSV = OUTPUTS / "big_board_2026.csv"
ROUND1_CSV = OUTPUTS / "mock_2026_round1.csv"
ROUND7_CSV = OUTPUTS / "mock_2026_7round.csv"
TEAM_NEEDS_CSV = ROOT / "data" / "sources" / "team_needs_context_2026.csv"
TEAM_NEEDS_ADJUSTMENTS_CSV = ROOT / "data" / "sources" / "team_needs_transaction_adjustments_2026.csv"
ESPN_PROSPECTS_CSV = ROOT / "data" / "sources" / "external" / "espn_nfl_draft_prospect_data" / "nfl_draft_prospects.csv"
DELTA_AUDIT_LATEST_CSV = OUTPUTS / "delta_audit_2026_latest.csv"


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


def _norm_school_key(value: str) -> str:
    text = str(value or "").strip().lower()
    cleaned = "".join(ch for ch in text if ch.isalnum() or ch.isspace())
    return " ".join(cleaned.split())


def _norm_player_key(value: str) -> str:
    text = str(value or "").strip().lower()
    return "".join(ch for ch in text if ch.isalnum())


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
    out = []
    for row in rows:
        player_name = row.get("player_name", "")
        school = player_school_map.get(_norm_player_key(player_name), "")
        if not school:
            school = _canonical_school_name(row.get("school", ""))

        slug = (
            (player_name or "")
            .lower()
            .replace(" ", "-")
            .replace(".", "")
            .replace("'", "")
        )
        rank_driver_summary = row.get("rank_driver_summary", "")
        top_driver_key, top_driver_delta = _top_driver(rank_driver_summary)
        pff_grade = round(_safe_float(row.get("pff_grade")) or 0.0, 2)
        combine_ras_official = round(_safe_float(row.get("combine_ras_official")) or 0.0, 2)
        ras_estimate = round(_safe_float(row.get("ras_estimate")) or 0.0, 2)
        production_snapshot = row.get("scouting_production_snapshot", "") or ""
        low_evidence_flag = (
            ("pending structured" in production_snapshot.lower())
            or (pff_grade <= 0 and combine_ras_official <= 0 and ras_estimate <= 0)
        )
        out.append(
            {
                "player_uid": row.get("player_uid", ""),
                "slug": slug,
                "consensus_rank": _safe_int(row.get("consensus_rank"), 9999),
                "player_name": player_name,
                "position": row.get("position", ""),
                "school": school,
                "final_grade": round(_safe_float(row.get("final_grade")) or 0.0, 2),
                "round_value": row.get("round_value", ""),
                "consensus_board_mean_rank": row.get("consensus_board_mean_rank", ""),
                "pff_grade": pff_grade,
                "trait_score": round(_safe_float(row.get("trait_score")) or 0.0, 2),
                "combine_ras_official": combine_ras_official,
                "ras_estimate": ras_estimate,
                "rank_driver_summary": rank_driver_summary,
                "top_rank_driver": top_driver_key,
                "top_rank_driver_delta": round(top_driver_delta, 2),
                "low_evidence_flag": low_evidence_flag,
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
    return export_mock_with_school_map(path, {})


def export_mock_with_school_map(path: Path, player_school_map: dict[str, str]) -> list[dict]:
    rows = _read_csv(path)
    out = []
    for row in rows:
        player_name = row.get("player_name", "")
        school = player_school_map.get(_norm_player_key(player_name), "")
        if not school:
            school = _canonical_school_name(row.get("school", ""))

        out.append(
            {
                "round": _safe_int(row.get("round"), 0),
                "pick": _safe_int(row.get("pick"), 0),
                "overall_pick": _safe_int(row.get("overall_pick"), 0),
                "team": row.get("team", ""),
                "player_uid": row.get("player_uid", ""),
                "player_name": player_name,
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
    adjustments = _read_csv(TEAM_NEEDS_ADJUSTMENTS_CSV)

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

    tx_by_team: dict[str, list[str]] = defaultdict(list)
    for row in adjustments:
        team = (row.get("team") or "").strip()
        summary = (row.get("event_summary") or "").strip()
        if team and summary:
            tx_by_team[team].append(summary)

    out: list[dict] = []
    for team, items in sorted(by_team.items(), key=lambda x: x[0]):
        items = sorted(items, key=lambda x: x["need_score"], reverse=True)
        out.append(
            {
                "team": team,
                "top_needs": items[:3],
                "recent_transactions": tx_by_team.get(team, [])[:3],
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
    round1 = export_mock_with_school_map(ROUND1_CSV, player_school_map)
    round7 = export_mock_with_school_map(ROUND7_CSV, player_school_map)
    by_team = export_round7_team_groups(round7)
    team_needs = export_team_needs()
    weekly_changes = export_weekly_changes(board)

    _write_json(ASTRO_DATA / "big_board_2026.json", board)
    _write_json(ASTRO_DATA / "mock_2026_round1.json", round1)
    _write_json(ASTRO_DATA / "mock_2026_7round.json", round7)
    _write_json(ASTRO_DATA / "mock_2026_7round_by_team.json", by_team)
    _write_json(ASTRO_DATA / "team_needs_2026.json", team_needs)
    _write_json(ASTRO_DATA / "weekly_changes_2026.json", weekly_changes)
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
    print(f"Wrote {ASTRO_DATA / 'build_meta.json'}")


if __name__ == "__main__":
    main()
