#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.cfbd_loader import fetch_dataset


OUT_DIR = ROOT / "data" / "sources" / "cfbd"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pull selected CFBD dataset with strict monthly call cap")
    p.add_argument("--dataset", required=True, choices=[
        "player_season_stats",
        "team_season_stats",
        "team_advanced_stats",
        "games",
        "team_game_stats",
        "roster",
        "fbs_teams",
    ])
    p.add_argument("--year", type=int, default=2025)
    p.add_argument("--team", type=str, default=None)
    p.add_argument("--week", type=int, default=None)
    p.add_argument("--season-type", type=str, default="regular", choices=["regular", "postseason", "both"])
    p.add_argument("--max-calls", type=int, default=1000)
    p.add_argument("--execute", action="store_true", help="Actually perform the API call. Without this, script is dry-run.")
    return p


def output_path(dataset: str, year: int, team: str | None, week: int | None) -> Path:
    parts = [dataset, str(year)]
    if team:
        parts.append(team.lower().replace(" ", "_"))
    if week is not None:
        parts.append(f"wk{week}")
    filename = "_".join(parts) + ".json"
    return OUT_DIR / filename


def main() -> None:
    args = build_parser().parse_args()

    result = fetch_dataset(
        dataset=args.dataset,
        year=args.year,
        team=args.team,
        week=args.week,
        season_type=args.season_type,
        execute=args.execute,
        max_calls_per_month=args.max_calls,
    )

    if result.get("dry_run", False):
        print(json.dumps(result, indent=2))
        print("Dry run complete. Re-run with --execute to spend 1 API call.")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = output_path(args.dataset, args.year, args.team, args.week)
    with out.open("w") as f:
        json.dump(result, f, indent=2)

    print(f"Saved CFBD response to: {out}")
    print(f"Calls used: {result['calls_used']} / {result['max_calls']}")
    print(f"Calls remaining: {result['calls_remaining']}")


if __name__ == "__main__":
    main()
