from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class SourceTable:
    table: str
    description: str
    url: str


NFLVERSE_TABLES: List[SourceTable] = [
    SourceTable("pbp_nfl", "NFL play-by-play", "https://nflverse.nflverse.com"),
    SourceTable("player_stats_nfl", "Player-level weekly/season stats", "https://nflreadr.nflverse.com"),
    SourceTable("rosters_nfl", "Roster snapshots", "https://nflreadr.nflverse.com"),
    SourceTable("draft_history_nfl", "Historical draft outcomes", "https://nflreadr.nflverse.com"),
    SourceTable("injuries_nfl", "Injury reports", "https://nflreadr.nflverse.com"),
]


def list_tables() -> List[dict]:
    return [t.__dict__ for t in NFLVERSE_TABLES]
