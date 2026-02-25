#!/usr/bin/env python3
from __future__ import annotations


MARKETS = [
    "first_overall_pick",
    "first_position_drafted",
    "team_to_draft_player",
]

BOOKMAKERS = ["fanduel", "draftkings", "betmgm", "caesars"]
REGIONS = ["us"]
SNAPSHOTS_PER_DAY = 2
DAYS = 120


def main() -> None:
    total = len(MARKETS) * len(BOOKMAKERS) * len(REGIONS) * SNAPSHOTS_PER_DAY * DAYS
    print("Estimated Odds API call plan")
    print(f"Markets: {len(MARKETS)}")
    print(f"Bookmakers: {len(BOOKMAKERS)}")
    print(f"Regions: {len(REGIONS)}")
    print(f"Snapshots/day: {SNAPSHOTS_PER_DAY}")
    print(f"Days: {DAYS}")
    print(f"Estimated total calls: {total}")


if __name__ == "__main__":
    main()
