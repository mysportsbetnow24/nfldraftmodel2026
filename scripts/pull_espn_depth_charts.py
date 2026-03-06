#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import ssl
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

try:
    import polars as pl
except Exception:  # pragma: no cover
    pl = None


ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "data" / "sources" / "external" / "espn_depth_charts_2026.csv"
ROSTERS_PATH = ROOT / "data" / "sources" / "external" / "nflverse" / "rosters_weekly.parquet"

TEAM_IDS = {
    "ARI": "22",
    "ATL": "1",
    "BAL": "33",
    "BUF": "2",
    "CAR": "29",
    "CHI": "3",
    "CIN": "4",
    "CLE": "5",
    "DAL": "6",
    "DEN": "7",
    "DET": "8",
    "GB": "9",
    "HOU": "34",
    "IND": "11",
    "JAX": "30",
    "KC": "12",
    "LV": "13",
    "LAC": "24",
    "LAR": "14",
    "MIA": "15",
    "MIN": "16",
    "NE": "17",
    "NO": "18",
    "NYG": "19",
    "NYJ": "20",
    "PHI": "21",
    "PIT": "23",
    "SF": "25",
    "SEA": "26",
    "TB": "27",
    "TEN": "10",
    "WAS": "28",
}

ATHLETE_ID_RE = re.compile(r"/athletes/(\d+)")


def _fetch_json(url: str, *, insecure: bool = False) -> dict:
    context = None
    if insecure:
        context = ssl._create_unverified_context()
    with urlopen(url, context=context) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_athlete_id(ref: str) -> str:
    match = ATHLETE_ID_RE.search(str(ref or ""))
    return match.group(1) if match else ""


def _depth_chart_url(season: int, team_id: str) -> str:
    return f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/teams/{team_id}/depthcharts"


def _build_espn_name_lookup() -> dict[str, str]:
    if pl is None or not ROSTERS_PATH.exists():
        return {}
    rosters = pl.read_parquet(ROSTERS_PATH)
    if rosters.is_empty():
        return {}
    latest_season = int(rosters.select(pl.col("season").max()).item())
    subset = rosters.filter(pl.col("season") == latest_season)
    if "game_type" in subset.columns:
        reg_subset = subset.filter(pl.col("game_type") == "REG")
        if not reg_subset.is_empty():
            subset = reg_subset
    latest_week_by_team = subset.group_by("team").agg(pl.col("week").max().alias("team_latest_week"))
    subset = (
        subset.join(latest_week_by_team, on="team", how="inner")
        .filter(pl.col("week") == pl.col("team_latest_week"))
        .unique(subset=["team", "espn_id"], keep="first")
    )
    lookup: dict[str, str] = {}
    for row in subset.iter_rows(named=True):
        espn_id = str(row.get("espn_id") or "").strip()
        name = str(row.get("full_name") or row.get("football_name") or "").strip()
        if espn_id and name:
            lookup[espn_id] = name
    return lookup


def _normalize_rows(payload: dict, season: int, team: str, team_id: str, pulled_at_utc: str, name_lookup: dict[str, str]) -> list[dict]:
    rows: list[dict] = []
    for chart in payload.get("items", []) or []:
        pos_group_id = str(chart.get("id") or "")
        pos_group = str(chart.get("name") or "")
        positions = chart.get("positions") or {}
        if not isinstance(positions, dict):
            continue
        for slot_key, position in positions.items():
            if not isinstance(position, dict):
                continue
            position_meta = position.get("position") or {}
            pos_id = str(position_meta.get("id") or position.get("positions_id") or "")
            pos_name = str(position_meta.get("name") or "")
            pos_abbr = str(position_meta.get("abbreviation") or "")
            pos_slot = str(position.get("name") or slot_key or "")
            pos_rank = int(position.get("rank") or 0)
            pos_key = str(position.get("abbreviation") or slot_key or pos_abbr or pos_slot)
            athletes = position.get("athletes") or []
            for athlete_idx, athlete in enumerate(athletes, start=1):
                athlete_ref = athlete.get("athlete", {}).get("$ref", "")
                athlete_id = _extract_athlete_id(athlete_ref)
                athlete_name = str(athlete.get("displayName") or athlete.get("shortName") or name_lookup.get(athlete_id) or "")
                rows.append(
                    {
                        "season": season,
                        "team": team,
                        "team_id": team_id,
                        "player_name": athlete_name,
                        "athlete_id": athlete_id,
                        "position_group_id": pos_group_id,
                        "position_group": pos_group,
                        "position_id": pos_id,
                        "position_name": pos_name,
                        "position_abbreviation": pos_abbr,
                        "position_key": pos_key,
                        "position_slot": pos_slot,
                        "rank": athlete_idx if pos_rank <= 0 else pos_rank + athlete_idx - 1,
                        "source": "espn_depth_chart_api",
                        "pulled_at_utc": pulled_at_utc,
                    }
                )
    return rows


def pull_depth_charts(season: int, teams: list[str], *, insecure: bool = False) -> list[dict]:
    pulled_at_utc = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    name_lookup = _build_espn_name_lookup()
    for team in teams:
        team_id = TEAM_IDS[team]
        payload = _fetch_json(_depth_chart_url(season, team_id), insecure=insecure)
        rows.extend(_normalize_rows(payload, season, team, team_id, pulled_at_utc, name_lookup))
    return rows


def write_csv(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_path.write_text("", encoding="utf-8")
        return
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull ESPN NFL depth charts and normalize them for Scouting Grade team-needs export.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--team", action="append", choices=sorted(TEAM_IDS.keys()), help="Optional team abbreviation. Repeat for multiple teams.")
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    parser.add_argument("--insecure", action="store_true", help="Disable SSL verification for environments with broken local cert chains.")
    args = parser.parse_args()

    teams = args.team or sorted(TEAM_IDS.keys())
    try:
        rows = pull_depth_charts(args.season, teams, insecure=args.insecure)
    except (HTTPError, URLError) as exc:
        print(f"Failed to pull ESPN depth charts: {exc}")
        return 1

    write_csv(rows, args.out)
    print(f"Wrote {args.out} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
