#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.odds_loader import (
    DEFAULT_MAX_CALLS,
    DEFAULT_PLAN_DAYS,
    DEFAULT_PLAN_START,
    SUPPORTED_MARKETS,
    fetch_draft_odds_snapshot,
)

OUT_DIR = ROOT / "data" / "sources" / "odds"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pull Odds API draft market snapshot with campaign quota guard")
    p.add_argument("--market", required=True, choices=SUPPORTED_MARKETS)
    p.add_argument("--sport", default="americanfootball_nfl")
    p.add_argument("--regions", default="us")
    p.add_argument("--bookmakers", default="fanduel,draftkings,betmgm,caesars")
    p.add_argument("--max-calls", type=int, default=DEFAULT_MAX_CALLS)
    p.add_argument("--plan-days", type=int, default=DEFAULT_PLAN_DAYS)
    p.add_argument("--plan-start", type=str, default=DEFAULT_PLAN_START)
    p.add_argument("--execute", action="store_true", help="Actually perform the API call. Without this, script is dry-run.")
    return p


def output_path(market: str) -> Path:
    return OUT_DIR / f"odds_{market}.json"


def main() -> None:
    args = build_parser().parse_args()

    result = fetch_draft_odds_snapshot(
        market=args.market,
        sport=args.sport,
        regions=args.regions,
        bookmakers=args.bookmakers,
        execute=args.execute,
        max_calls=args.max_calls,
        plan_days=args.plan_days,
        plan_start=args.plan_start,
    )

    if result.get("dry_run", False):
        print(json.dumps(result, indent=2))
        print("Dry run complete. Re-run with --execute to spend 1 API call.")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = output_path(args.market)
    with out.open("w") as f:
        json.dump(result, f, indent=2)

    print(f"Saved Odds response to: {out}")
    print(f"Calls used: {result['calls_used']} / {result['max_calls']}")
    print(f"Calls remaining: {result['calls_remaining']}")
    print(f"Campaign window: {result['start_date']} to {result['end_date']}")


if __name__ == "__main__":
    main()
