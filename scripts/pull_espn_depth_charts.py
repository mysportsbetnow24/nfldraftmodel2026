#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "data" / "sources" / "external" / "espn_depth_charts_2026.csv"

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


def _fetch_json(url: str) -> dict:
    with urlopen(url) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_athlete_id(ref: str) -> str:
    match = ATHLETE_ID_RE.search(str(ref or ""))
    return match.group(1) if match else ""


def _depth_chart_url(season: int, team_id: str) -> str:
    return f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/teams/{team_id}/depthcharts"


def _normalize_rows(payload: dict, season: int, team: str, team_id: str, pulled_at_utc: str) -> list[dict]:
    rows: list[dict] = []
    for chart in payload.get("items", []) or []:
        pos_group_id = str(chart.get("id") or "")
        pos_group = str(chart.get("name") or "")
        for position in chart.get("positions", []) or []:
            position_meta = position.get("position") or {}
            pos_id = str(position_meta.get("id") or position.get("positions_id") or "")
            pos_name = str(position_meta.get("name") or "")
            pos_abbr = str(position_meta.get("abbreviation") or "")
            pos_slot = str(position.get("name") or "")
            pos_rank = int(position.get("rank") or 0)
            pos_key = str(position.get("abbreviation") or pos_abbr or pos_slot)
            athletes = position.get("athletes") or []
            for athlete_idx, athlete in enumerate(athletes, start=1):
                athlete_ref = athlete.get("athlete", {}).get("$ref", "")
                athlete_name = str(athlete.get("displayName") or athlete.get("shortName") or "")
                rows.append(
                    {
                        "season": season,
                        "team": team,
                        "team_id": team_id,
                        "player_name": athlete_name,
                        "athlete_id": _extract_athlete_id(athlete_ref),
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


def pull_depth_charts(season: int, teams: list[str]) -> list[dict]:
    pulled_at_utc = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for team in teams:
        team_id = TEAM_IDS[team]
        payload = _fetch_json(_depth_chart_url(season, team_id))
        rows.extend(_normalize_rows(payload, season, team, team_id, pulled_at_utc))
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
    args = parser.parse_args()

    teams = args.team or sorted(TEAM_IDS.keys())
    try:
        rows = pull_depth_charts(args.season, teams)
    except (HTTPError, URLError) as exc:
        print(f"Failed to pull ESPN depth charts: {exc}")
        return 1

    write_csv(rows, args.out)
    print(f"Wrote {args.out} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
