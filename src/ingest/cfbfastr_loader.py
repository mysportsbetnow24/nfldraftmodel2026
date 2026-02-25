from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class SourceTable:
    table: str
    description: str
    url: str


CFB_TABLES: List[SourceTable] = [
    SourceTable("pbp_cfb", "College football play-by-play", "https://cfbfastr.sportsdataverse.org"),
    SourceTable("player_stats_cfb", "Player season statistics", "https://cfbfastr.sportsdataverse.org"),
    SourceTable("team_stats_cfb", "Team efficiency summaries", "https://cfbfastr.sportsdataverse.org"),
    SourceTable("rosters_cfb", "Player roster metadata", "https://cfbfastr.sportsdataverse.org"),
]


def list_tables() -> List[dict]:
    return [t.__dict__ for t in CFB_TABLES]
