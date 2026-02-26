from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class SourceTable:
    table: str
    description: str
    url: str
    level: str
    model_use: str
    priority: str


CFB_TABLES: List[SourceTable] = [
    SourceTable(
        "cfbd_metrics_ppa_players_season",
        "Player-level PPA splits (overall/pass/rush/down-and-distance).",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "production_score",
        "P0",
    ),
    SourceTable(
        "cfbd_metrics_wepa_players_passing",
        "Player passing WEPA (opponent/context adjusted passing efficiency).",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "production_score_qb",
        "P0",
    ),
    SourceTable(
        "cfbd_metrics_wepa_players_receiving",
        "Player receiving WEPA (context-adjusted receiving impact).",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "production_score_wr_te",
        "P0",
    ),
    SourceTable(
        "cfbd_metrics_wepa_players_rushing",
        "Player rushing WEPA (context-adjusted rushing impact).",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "production_score_rb_qb",
        "P0",
    ),
    SourceTable(
        "cfbd_player_usage",
        "Player usage share and split rates.",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "role_context_and_risk",
        "P0",
    ),
    SourceTable(
        "cfbd_stats_season_player",
        "Player season counting stats by category.",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "base_production_features",
        "P0",
    ),
    SourceTable(
        "cfbd_player_returning",
        "Returning production context by player/team.",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "context_score_and_risk",
        "P1",
    ),
    SourceTable(
        "cfbd_recruiting_player",
        "Recruiting stars/rating/rank/commit context.",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "pedigree_and_risk",
        "P1",
    ),
    SourceTable(
        "cfbd_draft_picks",
        "Historical college-to-NFL draft outcomes.",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "historical_calibration_labels",
        "P0",
    ),
    SourceTable(
        "cfbd_ratings_srs",
        "Team Simple Rating System by season.",
        "https://cfbfastr.sportsdataverse.org",
        "team",
        "opponent_strength_normalization",
        "P1",
    ),
    SourceTable(
        "cfbd_stats_season_advanced",
        "Team advanced efficiency profile (success rate/explosiveness/havoc/etc.).",
        "https://cfbfastr.sportsdataverse.org",
        "team",
        "opponent_environment_context_only",
        "P2",
    ),
    SourceTable(
        "load_cfb_pbp",
        "Historical CFB play-by-play from sportsdataverse data releases.",
        "https://cfbfastr.sportsdataverse.org",
        "play",
        "custom_feature_engineering",
        "P2",
    ),
    SourceTable(
        "load_cfb_rosters",
        "Roster snapshots and player metadata from sportsdataverse releases.",
        "https://cfbfastr.sportsdataverse.org",
        "player",
        "identity_and_join_quality",
        "P1",
    ),
]


def list_tables() -> List[dict]:
    return [t.__dict__ for t in CFB_TABLES]
