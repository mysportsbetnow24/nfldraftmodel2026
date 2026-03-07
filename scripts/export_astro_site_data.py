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
MANUAL_SOURCES = ROOT / "data" / "sources" / "manual"

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
NFLVERSE_PARTICIPATION = NFLVERSE_DIR / "participation.parquet"
NFLVERSE_SNAP_COUNTS = NFLVERSE_DIR / "snap_counts.parquet"
NFLVERSE_PLAYER_STATS = NFLVERSE_DIR / "player_stats.parquet"
NFLVERSE_NEXTGEN = NFLVERSE_DIR / "nextgen_stats.parquet"
NFLVERSE_PFR_ADVSTATS = NFLVERSE_DIR / "pfr_advstats.parquet"
HISTORICAL_DRAFT_COMPILATION = ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data" / "notebook" / "compilations" / "drafts2015To2022.csv"
HISTORICAL_DRAFT_REFINED = ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data" / "old-data" / "pfr-compilations" / "2014To2018Drafts-refined.csv"
HISTORICAL_DRAFT_2014_2018 = ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data" / "old-data" / "pfr-compilations" / "2014To2018Drafts.csv"
HISTORICAL_DRAFT_2023 = ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data" / "notebook" / "drafts" / "2023Draft.csv"
HISTORICAL_LABELS_LEAGIFY = ROOT / "data" / "processed" / "historical_labels_leagify_2015_2023.csv"
OWNER_SCOUTING_NOTES_CSV = ROOT / "data" / "sources" / "manual" / "owner_scouting_notes_2026.csv"
PRODUCTION_SNAPSHOT_OVERRIDES_CSV = ROOT / "data" / "sources" / "manual" / "production_snapshot_overrides_2026.csv"
PREMIUM_COMP_2024_SOURCES = [
    MANUAL_SOURCES / "passing_summary_2024.csv",
    MANUAL_SOURCES / "passing_pressure_2024.csv",
    MANUAL_SOURCES / "passing_concept_2024.csv",
    MANUAL_SOURCES / "rushing_summary_2024.csv",
    MANUAL_SOURCES / "receiving_scheme_2024.csv",
    MANUAL_SOURCES / "offense_blocking_2024.csv",
    MANUAL_SOURCES / "pass_rush_summary_2024.csv",
    MANUAL_SOURCES / "pass_rush_productivity_2024.csv",
    MANUAL_SOURCES / "run_defense_summary_2024.csv",
    MANUAL_SOURCES / "defense_summary_2024.csv",
    MANUAL_SOURCES / "defense_coverage_summary_2024.csv",
    MANUAL_SOURCES / "defense_coverage_scheme_2024.csv",
    MANUAL_SOURCES / "slot_coverage_2024.csv",
]


PRODUCTION_METRIC_KEYS = [
    "sg_qb_pass_grade",
    "sg_qb_btt_rate",
    "sg_qb_twp_rate",
    "sg_qb_pressure_to_sack_rate",
    "sg_qb_pressure_grade",
    "sg_qb_blitz_grade",
    "cfb_qb_epa_per_play",
    "cfb_qb_pressure_signal",
    "cfb_qb_pass_td",
    "cfb_qb_pass_int",
    "cfb_qb_int_rate",
    "sg_wrte_route_grade",
    "sg_wrte_yprr",
    "sg_wrte_targets_per_route",
    "sg_wrte_man_yprr",
    "sg_wrte_zone_yprr",
    "sg_wrte_contested_catch_rate",
    "sg_wrte_drop_rate",
    "cfb_wrte_yprr",
    "cfb_wrte_target_share",
    "cfb_wrte_rec_td",
    "cfb_wrte_rec_yds",
    "sg_rb_run_grade",
    "sg_rb_elusive_rating",
    "sg_rb_yco_attempt",
    "sg_rb_explosive_rate",
    "sg_rb_breakaway_percent",
    "sg_rb_targets_per_route",
    "cfb_rb_explosive_rate",
    "cfb_rb_missed_tackles_forced_per_touch",
    "cfb_rb_rush_td",
    "cfb_rb_rush_yds",
    "sg_dl_pass_rush_grade",
    "sg_dl_pass_rush_win_rate",
    "sg_dl_prp",
    "sg_dl_true_pass_set_win_rate",
    "sg_dl_true_pass_set_prp",
    "sg_dl_total_pressures",
    "sg_front_run_def_grade",
    "sg_front_stop_percent",
    "cfb_edge_pressure_rate",
    "cfb_edge_sacks",
    "cfb_edge_qb_hurries",
    "cfb_edge_tfl",
    "sg_cov_grade",
    "sg_cov_forced_incompletion_rate",
    "sg_cov_snaps_per_target",
    "sg_cov_yards_per_snap",
    "sg_cov_qb_rating_against",
    "sg_cov_man_grade",
    "sg_cov_zone_grade",
    "sg_slot_cov_snaps",
    "sg_slot_cov_snaps_per_target",
    "sg_slot_cov_qb_rating_against",
    "cfb_db_coverage_plays_per_target",
    "cfb_db_yards_allowed_per_coverage_snap",
    "cfb_db_int",
    "cfb_db_pbu",
    "sg_def_run_grade",
    "sg_def_coverage_grade",
    "sg_def_tackle_grade",
    "sg_def_missed_tackle_rate",
    "sg_def_total_pressures",
    "sg_def_tackles_for_loss",
    "sg_def_tackles",
    "sg_def_pass_break_ups",
    "sg_def_interceptions",
    "cfb_lb_tackles",
    "cfb_lb_tfl",
    "cfb_lb_sacks",
    "cfb_lb_qb_hurries",
    "cfb_lb_signal",
    "cfb_lb_rush_impact_signal",
    "sg_ol_pass_block_grade",
    "sg_ol_run_block_grade",
    "sg_ol_pbe",
    "sg_ol_pressure_allowed_rate",
    "sg_ol_versatility_count",
    "cfb_ol_years_played",
    "cfb_ol_starts",
    "cfb_ol_usage_rate",
    "cfb_ol_proxy_signal",
]

POSITION_ADVANCED_METRIC_CONFIG = {
    "QB": [
        {"key": "sg_qb_pass_grade", "label": "Pass Grade", "fmt": "dec1", "weight": 0.24},
        {"key": "sg_qb_btt_rate", "label": "Big-Time Throw Rate", "fmt": "pct100", "weight": 0.14},
        {"key": "sg_qb_twp_rate", "label": "Turnover-Worthy Play Rate", "fmt": "pct100", "lower_better": True, "weight": 0.16},
        {"key": "sg_qb_pressure_to_sack_rate", "label": "Pressure-to-Sack Rate", "fmt": "pct100", "lower_better": True, "weight": 0.16},
        {"key": "sg_qb_pressure_grade", "label": "Under-Pressure Grade", "fmt": "dec1", "weight": 0.16},
        {"key": "sg_qb_blitz_grade", "label": "Blitz Grade", "fmt": "dec1", "weight": 0.14},
    ],
    "RB": [
        {"key": "sg_rb_run_grade", "label": "Run Grade", "fmt": "dec1", "weight": 0.18},
        {"key": "sg_rb_elusive_rating", "label": "Elusive Rating", "fmt": "dec1", "weight": 0.22},
        {"key": "sg_rb_yco_attempt", "label": "Yards After Contact / Att", "fmt": "dec2", "weight": 0.18},
        {"key": "sg_rb_explosive_rate", "label": "Explosive Run Rate", "fmt": "pct100", "weight": 0.18},
        {"key": "sg_rb_breakaway_percent", "label": "Breakaway Rate", "fmt": "pct100", "weight": 0.14},
        {"key": "sg_rb_targets_per_route", "label": "Targets / Route", "fmt": "dec3", "weight": 0.10},
    ],
    "WR": [
        {"key": "sg_wrte_route_grade", "label": "Route Grade", "fmt": "dec1", "weight": 0.24},
        {"key": "sg_wrte_yprr", "label": "Yards / Route Run", "fmt": "dec2", "weight": 0.20},
        {"key": "sg_wrte_targets_per_route", "label": "Targets / Route", "fmt": "dec3", "weight": 0.18},
        {"key": "sg_wrte_man_yprr", "label": "Man YPRR", "fmt": "dec2", "weight": 0.14},
        {"key": "sg_wrte_zone_yprr", "label": "Zone YPRR", "fmt": "dec2", "weight": 0.12},
        {"key": "sg_wrte_drop_rate", "label": "Drop Rate", "fmt": "pct100", "lower_better": True, "weight": 0.12},
    ],
    "TE": [
        {"key": "sg_wrte_route_grade", "label": "Route Grade", "fmt": "dec1", "weight": 0.24},
        {"key": "sg_wrte_yprr", "label": "Yards / Route Run", "fmt": "dec2", "weight": 0.22},
        {"key": "sg_wrte_targets_per_route", "label": "Targets / Route", "fmt": "dec3", "weight": 0.18},
        {"key": "sg_wrte_man_yprr", "label": "Man YPRR", "fmt": "dec2", "weight": 0.14},
        {"key": "sg_wrte_zone_yprr", "label": "Zone YPRR", "fmt": "dec2", "weight": 0.12},
        {"key": "sg_wrte_drop_rate", "label": "Drop Rate", "fmt": "pct100", "lower_better": True, "weight": 0.10},
    ],
    "EDGE": [
        {"key": "sg_dl_pass_rush_grade", "label": "Pass Rush Grade", "fmt": "dec1", "weight": 0.24},
        {"key": "sg_dl_true_pass_set_win_rate", "label": "True Pass Set Win Rate", "fmt": "pct100", "weight": 0.22},
        {"key": "sg_dl_true_pass_set_prp", "label": "True Pass Set PRP", "fmt": "dec1", "weight": 0.18},
        {"key": "sg_dl_total_pressures", "label": "Total Pressures", "fmt": "int", "weight": 0.12},
        {"key": "sg_front_run_def_grade", "label": "Run Defense Grade", "fmt": "dec1", "weight": 0.14},
        {"key": "sg_front_stop_percent", "label": "Stop Rate", "fmt": "pct100", "weight": 0.10},
    ],
    "DT": [
        {"key": "sg_dl_pass_rush_grade", "label": "Pass Rush Grade", "fmt": "dec1", "weight": 0.20},
        {"key": "sg_dl_true_pass_set_win_rate", "label": "True Pass Set Win Rate", "fmt": "pct100", "weight": 0.18},
        {"key": "sg_dl_true_pass_set_prp", "label": "True Pass Set PRP", "fmt": "dec1", "weight": 0.16},
        {"key": "sg_dl_total_pressures", "label": "Total Pressures", "fmt": "int", "weight": 0.12},
        {"key": "sg_front_run_def_grade", "label": "Run Defense Grade", "fmt": "dec1", "weight": 0.20},
        {"key": "sg_front_stop_percent", "label": "Stop Rate", "fmt": "pct100", "weight": 0.14},
    ],
    "LB": [
        {"key": "sg_def_run_grade", "label": "Run Defense Grade", "fmt": "dec1", "weight": 0.24},
        {"key": "sg_def_coverage_grade", "label": "Coverage Grade", "fmt": "dec1", "weight": 0.22},
        {"key": "sg_def_tackle_grade", "label": "Tackle Grade", "fmt": "dec1", "weight": 0.16},
        {"key": "sg_def_missed_tackle_rate", "label": "Missed Tackle Rate", "fmt": "pct100", "lower_better": True, "weight": 0.14},
        {"key": "sg_front_stop_percent", "label": "Stop Rate", "fmt": "pct100", "weight": 0.12},
        {"key": "sg_cov_yards_per_snap", "label": "Yards Allowed / Coverage Snap", "fmt": "dec2", "lower_better": True, "weight": 0.12},
    ],
    "CB": [
        {"key": "sg_cov_grade", "label": "Coverage Grade", "fmt": "dec1", "weight": 0.24},
        {"key": "sg_cov_forced_incompletion_rate", "label": "Forced Incompletion Rate", "fmt": "pct100", "weight": 0.16},
        {"key": "sg_cov_snaps_per_target", "label": "Coverage Snaps per Target", "fmt": "dec1", "weight": 0.14},
        {"key": "sg_cov_yards_per_snap", "label": "Yards Allowed / Coverage Snap", "fmt": "dec2", "lower_better": True, "weight": 0.18},
        {"key": "sg_cov_qb_rating_against", "label": "Passer Rating Allowed", "fmt": "dec1", "lower_better": True, "weight": 0.16},
        {"key": "sg_cov_man_grade", "label": "Man Coverage Grade", "fmt": "dec1", "weight": 0.12},
        {"key": "sg_slot_cov_snaps_per_target", "label": "Slot Snaps per Target", "fmt": "dec1", "weight": 0.10},
        {"key": "sg_slot_cov_yards_per_snap", "label": "Slot Yards Allowed / Snap", "fmt": "dec2", "lower_better": True, "weight": 0.10},
        {"key": "sg_slot_cov_qb_rating_against", "label": "Slot Passer Rating Allowed", "fmt": "dec1", "lower_better": True, "weight": 0.10},
    ],
    "S": [
        {"key": "sg_cov_grade", "label": "Coverage Grade", "fmt": "dec1", "weight": 0.22},
        {"key": "sg_cov_forced_incompletion_rate", "label": "Forced Incompletion Rate", "fmt": "pct100", "weight": 0.14},
        {"key": "sg_cov_snaps_per_target", "label": "Coverage Snaps per Target", "fmt": "dec1", "weight": 0.14},
        {"key": "sg_cov_yards_per_snap", "label": "Yards Allowed / Coverage Snap", "fmt": "dec2", "lower_better": True, "weight": 0.18},
        {"key": "sg_cov_qb_rating_against", "label": "Passer Rating Allowed", "fmt": "dec1", "lower_better": True, "weight": 0.14},
        {"key": "sg_cov_zone_grade", "label": "Zone Coverage Grade", "fmt": "dec1", "weight": 0.18},
        {"key": "sg_slot_cov_snaps_per_target", "label": "Slot Snaps per Target", "fmt": "dec1", "weight": 0.10},
        {"key": "sg_slot_cov_yards_per_snap", "label": "Slot Yards Allowed / Snap", "fmt": "dec2", "lower_better": True, "weight": 0.10},
        {"key": "sg_slot_cov_qb_rating_against", "label": "Slot Passer Rating Allowed", "fmt": "dec1", "lower_better": True, "weight": 0.10},
    ],
    "OT": [
        {"key": "sg_ol_pass_block_grade", "label": "Pass Block Grade", "fmt": "dec1", "weight": 0.34},
        {"key": "sg_ol_run_block_grade", "label": "Run Block Grade", "fmt": "dec1", "weight": 0.18},
        {"key": "sg_ol_pbe", "label": "Pass Block Efficiency", "fmt": "dec1", "weight": 0.28},
        {"key": "sg_ol_pressure_allowed_rate", "label": "Pressure Allowed Rate", "fmt": "pct", "lower_better": True, "weight": 0.14},
        {"key": "sg_ol_versatility_count", "label": "Alignment Versatility", "fmt": "int", "weight": 0.06},
    ],
    "IOL": [
        {"key": "sg_ol_pass_block_grade", "label": "Pass Block Grade", "fmt": "dec1", "weight": 0.30},
        {"key": "sg_ol_run_block_grade", "label": "Run Block Grade", "fmt": "dec1", "weight": 0.22},
        {"key": "sg_ol_pbe", "label": "Pass Block Efficiency", "fmt": "dec1", "weight": 0.26},
        {"key": "sg_ol_pressure_allowed_rate", "label": "Pressure Allowed Rate", "fmt": "pct", "lower_better": True, "weight": 0.14},
        {"key": "sg_ol_versatility_count", "label": "Alignment Versatility", "fmt": "int", "weight": 0.08},
    ],
}

POSITION_COUNTING_STAT_CONFIG = {
    "QB": [
        {"key": "cfb_qb_pass_td", "label": "Pass TD", "fmt": "int"},
        {"key": "cfb_qb_pass_int", "label": "INT", "fmt": "int"},
        {"key": "cfb_qb_pass_yds", "label": "Pass Yards", "fmt": "int"},
        {"key": "cfb_qb_rush_yds", "label": "Rush Yards", "fmt": "int"},
    ],
    "RB": [
        {"key": "cfb_rb_rush_yds", "label": "Rush Yards", "fmt": "int"},
        {"key": "cfb_rb_rush_td", "label": "Rush TD", "fmt": "int"},
        {"key": "cfb_rb_rec", "label": "Receptions", "fmt": "int"},
        {"key": "cfb_rb_rec_yds", "label": "Rec Yards", "fmt": "int"},
    ],
    "WR": [
        {"key": "cfb_wrte_rec", "label": "Receptions", "fmt": "int"},
        {"key": "cfb_wrte_rec_yds", "label": "Rec Yards", "fmt": "int"},
        {"key": "cfb_wrte_rec_td", "label": "Rec TD", "fmt": "int"},
    ],
    "TE": [
        {"key": "cfb_wrte_rec", "label": "Receptions", "fmt": "int"},
        {"key": "cfb_wrte_rec_yds", "label": "Rec Yards", "fmt": "int"},
        {"key": "cfb_wrte_rec_td", "label": "Rec TD", "fmt": "int"},
    ],
    "EDGE": [
        {"key": "cfb_edge_sacks", "label": "Sacks", "fmt": "dec1"},
        {"key": "cfb_edge_qb_hurries", "label": "QB Hurries", "fmt": "int"},
        {"key": "cfb_edge_tfl", "label": "TFL", "fmt": "int"},
        {"key": "cfb_edge_tackles", "label": "Tackles", "fmt": "int"},
    ],
    "DT": [
        {"key": "cfb_edge_sacks", "label": "Sacks", "fmt": "dec1"},
        {"key": "cfb_edge_qb_hurries", "label": "QB Hurries", "fmt": "int"},
        {"key": "cfb_edge_tfl", "label": "TFL", "fmt": "int"},
        {"key": "cfb_edge_tackles", "label": "Tackles", "fmt": "int"},
    ],
    "LB": [
        {"key": "cfb_lb_tackles", "label": "Tackles", "fmt": "int"},
        {"key": "cfb_lb_tfl", "label": "TFL", "fmt": "int"},
        {"key": "cfb_lb_sacks", "label": "Sacks", "fmt": "dec1"},
        {"key": "cfb_lb_qb_hurries", "label": "QB Hurries", "fmt": "int"},
        {"key": "sg_def_total_pressures", "label": "Pressures", "fmt": "int"},
    ],
    "CB": [
        {"key": "cfb_db_int", "label": "INT", "fmt": "int"},
        {"key": "cfb_db_pbu", "label": "PBU", "fmt": "int"},
        {"key": "cfb_db_tackles", "label": "Tackles", "fmt": "int"},
        {"key": "cfb_db_tfl", "label": "TFL", "fmt": "int"},
        {"key": "sg_def_total_pressures", "label": "Pressures", "fmt": "int"},
    ],
    "S": [
        {"key": "cfb_db_int", "label": "INT", "fmt": "int"},
        {"key": "cfb_db_pbu", "label": "PBU", "fmt": "int"},
        {"key": "cfb_db_tackles", "label": "Tackles", "fmt": "int"},
        {"key": "cfb_db_tfl", "label": "TFL", "fmt": "int"},
        {"key": "sg_def_total_pressures", "label": "Pressures", "fmt": "int"},
    ],
    "OT": [
        {"key": "cfb_ol_years_played", "label": "Years Played", "fmt": "int"},
        {"key": "cfb_ol_starts", "label": "Starts", "fmt": "int"},
    ],
    "IOL": [
        {"key": "cfb_ol_years_played", "label": "Years Played", "fmt": "int"},
        {"key": "cfb_ol_starts", "label": "Starts", "fmt": "int"},
    ],
}

POSITION_FALLBACK_METRIC_CONFIG = {
    "QB": [
        {"key": "cfb_qb_epa_per_play", "label": "EPA / Play", "fmt": "dec3", "weight": 0.28},
        {"key": "cfb_qb_success_rate", "label": "Success Rate", "fmt": "pct100", "weight": 0.22},
        {"key": "cfb_qb_pressure_to_sack_rate", "label": "Pressure-to-Sack Rate", "fmt": "pct100", "lower_better": True, "weight": 0.22},
        {"key": "cfb_qb_under_pressure_success_rate", "label": "Under-Pressure Success", "fmt": "pct100", "weight": 0.14},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.14},
    ],
    "RB": [
        {"key": "cfb_rb_explosive_rate", "label": "Explosive Run Rate", "fmt": "pct100", "weight": 0.28},
        {"key": "cfb_rb_target_share", "label": "Receiving Share", "fmt": "pct100", "weight": 0.18},
        {"key": "cfb_rb_receiving_efficiency", "label": "Receiving Efficiency", "fmt": "dec2", "weight": 0.18},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.18},
        {"key": "top_defense_performance_index", "label": "Top-Defense Performance", "fmt": "dec3", "weight": 0.18},
    ],
    "WR": [
        {"key": "cfb_wrte_yprr", "label": "Yards / Route Run", "fmt": "dec2", "weight": 0.30},
        {"key": "cfb_wrte_target_share", "label": "Target Share", "fmt": "pct100", "weight": 0.22},
        {"key": "cfb_wrte_targets_per_route", "label": "Targets / Route", "fmt": "dec3", "weight": 0.22},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.12},
        {"key": "top_defense_performance_index", "label": "Top-Defense Performance", "fmt": "dec3", "weight": 0.14},
    ],
    "TE": [
        {"key": "cfb_wrte_yprr", "label": "Yards / Route Run", "fmt": "dec2", "weight": 0.30},
        {"key": "cfb_wrte_target_share", "label": "Target Share", "fmt": "pct100", "weight": 0.22},
        {"key": "cfb_wrte_targets_per_route", "label": "Targets / Route", "fmt": "dec3", "weight": 0.22},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.12},
        {"key": "top_defense_performance_index", "label": "Top-Defense Performance", "fmt": "dec3", "weight": 0.14},
    ],
    "EDGE": [
        {"key": "cfb_edge_pressure_rate", "label": "Pressure / Rush Snap", "fmt": "pct100", "weight": 0.34},
        {"key": "cfb_edge_sacks_per_pr_snap", "label": "Sacks / Rush Snap", "fmt": "pct100", "weight": 0.22},
        {"key": "cfb_edge_tfl", "label": "TFL", "fmt": "int", "weight": 0.14},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.14},
        {"key": "top_defense_performance_index", "label": "Top-Offense Performance", "fmt": "dec3", "weight": 0.16},
    ],
    "DT": [
        {"key": "cfb_edge_pressure_rate", "label": "Pressure / Rush Snap", "fmt": "pct100", "weight": 0.28},
        {"key": "cfb_edge_sacks_per_pr_snap", "label": "Sacks / Rush Snap", "fmt": "pct100", "weight": 0.16},
        {"key": "cfb_edge_tfl", "label": "TFL", "fmt": "int", "weight": 0.18},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.18},
        {"key": "top_defense_performance_index", "label": "Top-Offense Performance", "fmt": "dec3", "weight": 0.20},
    ],
    "LB": [
        {"key": "cfb_lb_tfl", "label": "TFL", "fmt": "int", "weight": 0.26},
        {"key": "cfb_lb_sacks", "label": "Sacks", "fmt": "dec1", "weight": 0.18},
        {"key": "cfb_lb_qb_hurries", "label": "QB Hurries", "fmt": "int", "weight": 0.16},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.18},
        {"key": "top_defense_performance_index", "label": "Top-Offense Performance", "fmt": "dec3", "weight": 0.22},
    ],
    "CB": [
        {"key": "cfb_db_coverage_plays_per_target", "label": "Plays on Ball / Target", "fmt": "dec3", "weight": 0.32},
        {"key": "cfb_db_yards_allowed_per_coverage_snap", "label": "Yards / Coverage Snap", "fmt": "dec3", "lower_better": True, "weight": 0.30},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.16},
        {"key": "top_defense_performance_index", "label": "Top-Offense Performance", "fmt": "dec3", "weight": 0.22},
    ],
    "S": [
        {"key": "cfb_db_coverage_plays_per_target", "label": "Plays on Ball / Target", "fmt": "dec3", "weight": 0.30},
        {"key": "cfb_db_yards_allowed_per_coverage_snap", "label": "Yards / Coverage Snap", "fmt": "dec3", "lower_better": True, "weight": 0.26},
        {"key": "game_consistency_index", "label": "Weekly Consistency", "fmt": "dec3", "weight": 0.18},
        {"key": "top_defense_performance_index", "label": "Top-Offense Performance", "fmt": "dec3", "weight": 0.26},
    ],
}


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


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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
TEAM_TEXT_CODE_ALIASES = {
    "ARI": ["ari", "arizona", "arizona cardinals", "cardinals"],
    "ATL": ["atl", "atlanta", "atlanta falcons", "falcons"],
    "BAL": ["bal", "baltimore", "baltimore ravens", "ravens"],
    "BUF": ["buf", "buffalo", "buffalo bills", "bills"],
    "CAR": ["car", "carolina", "carolina panthers", "panthers"],
    "CHI": ["chi", "chicago", "chicago bears", "bears"],
    "CIN": ["cin", "cincinnati", "cincinnati bengals", "bengals"],
    "CLE": ["cle", "cleveland", "cleveland browns", "browns"],
    "DAL": ["dal", "dallas", "dallas cowboys", "cowboys"],
    "DEN": ["den", "denver", "denver broncos", "broncos"],
    "DET": ["det", "detroit", "detroit lions", "lions"],
    "GB": ["gb", "green bay", "green bay packers", "packers"],
    "HOU": ["hou", "houston", "houston texans", "texans"],
    "IND": ["ind", "indianapolis", "indianapolis colts", "colts"],
    "JAX": ["jax", "jacksonville", "jacksonville jaguars", "jaguars"],
    "KC": ["kc", "kansas city", "kansas city chiefs", "chiefs"],
    "LAC": ["lac", "la chargers", "los angeles chargers", "chargers"],
    "LAR": ["lar", "la rams", "los angeles rams", "rams"],
    "LV": ["lv", "las vegas", "las vegas raiders", "raiders"],
    "MIA": ["mia", "miami", "miami dolphins", "dolphins"],
    "MIN": ["min", "minnesota", "minnesota vikings", "vikings"],
    "NE": ["ne", "new england", "new england patriots", "patriots"],
    "NO": ["no", "new orleans", "new orleans saints", "saints"],
    "NYG": ["nyg", "new york giants", "giants"],
    "NYJ": ["nyj", "new york jets", "jets"],
    "PHI": ["phi", "philadelphia", "philadelphia eagles", "eagles"],
    "PIT": ["pit", "pittsburgh", "pittsburgh steelers", "steelers"],
    "SEA": ["sea", "seattle", "seattle seahawks", "seahawks"],
    "SF": ["sf", "san francisco", "san francisco 49ers", "49ers", "niners"],
    "TB": ["tb", "tampa bay", "tampa bay buccaneers", "buccaneers", "bucs"],
    "TEN": ["ten", "tennessee", "tennessee titans", "titans"],
    "WAS": ["was", "washington", "washington commanders", "commanders"],
}
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
    "LB": ["WLB", "MLB", "SLB", "ILB", "LB"],
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
    if pos in {"FS", "SS", "SAF", "RS"}:
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


def _slot_implied_position(depth_chart_position: str) -> str:
    slot = str(depth_chart_position or "").strip().upper()
    if not slot:
        return ""
    if slot in {"QB"}:
        return "QB"
    if slot in {"RB", "HB", "TB", "FB"}:
        return "RB"
    if slot in {"WR", "LWR", "RWR", "XWR", "ZWR", "SWR", "SLOT"}:
        return "WR"
    if slot in {"TE", "Y", "F"}:
        return "TE"
    if slot in {"LT", "RT", "T"}:
        return "OT"
    if slot in {"LG", "RG", "C", "OC", "G"}:
        return "IOL"
    if slot in {"LOLB", "ROLB", "OLB"}:
        return "EDGE"
    if slot in {"LDE", "RDE", "DE", "NT", "DT", "LDT", "RDT", "IDL"}:
        return "DT"
    if slot in {"MLB", "LILB", "RILB", "ILB", "WLB", "SLB", "LB"}:
        return "LB"
    if slot in {"LCB", "RCB", "NB", "CB"}:
        return "CB"
    if slot in {"FS", "SS", "S", "SAF", "RS"}:
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


def _normalize_contract_team_codes(value: str) -> list[str]:
    text = str(value or "").strip().lower()
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"[\\/,&]+", text) if part.strip()]
    if not parts:
        parts = [text]
    codes: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = re.sub(r"[^a-z0-9\s]+", " ", part)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        for code, aliases in TEAM_TEXT_CODE_ALIASES.items():
            if normalized == code.lower() or normalized in aliases:
                if code not in seen:
                    seen.add(code)
                    codes.append(code)
                break
    return codes


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
    model_position = str(player.get("position") or "").strip().upper()
    apy_m = float(player.get("apy_m") or 0.0)
    apy_pct = float(player.get("apy_pct") or 0.0)
    years_exp = _safe_int(player.get("years_exp"), 0)
    snap_count = _safe_int(player.get("snap_count"), 0)
    offense_snaps = _safe_int(player.get("offense_snaps"), 0)
    defense_snaps = _safe_int(player.get("defense_snaps"), 0)
    age = _safe_int(player.get("age"), 0)
    draft_number = _safe_int(player.get("draft_number"), 9999)
    rookie_weight = 0.0
    if draft_number > 0:
        rookie_weight = max(0.0, 1.0 - (min(draft_number, 256) / 300.0))

    starter_signal = apy_pct + min(apy_m, 40.0) + (years_exp * 1.5) + (rookie_weight * 18.0)
    if model_position in {"WR", "TE", "RB"}:
        starter_signal += min(offense_snaps, 1200) / 20.0
        starter_signal += min(snap_count, 1200) / 35.0
    elif model_position in {"CB", "S", "LB", "EDGE", "DT"}:
        starter_signal += min(defense_snaps, 1200) / 20.0
        starter_signal += min(snap_count, 1200) / 35.0
    elif model_position in {"OT", "IOL"}:
        starter_signal += min(offense_snaps, 1200) / 16.0
        starter_signal += min(snap_count, 1200) / 30.0
    if model_position == "QB":
        starter_signal += min(apy_m, 50.0) * 1.2
        starter_signal += apy_pct * 0.45
        if draft_number > 0 and draft_number <= 15 and years_exp <= 2 and 21 <= age <= 26:
            starter_signal += 42.0
        if years_exp <= 2 and 21 <= age <= 26 and apy_m >= 5.0:
            starter_signal += 72.0
        if apy_m >= 18.0 and years_exp <= 3:
            starter_signal += 16.0
        if years_exp >= 4 and 24 <= age <= 34:
            starter_signal += 18.0
        if years_exp >= 10 and age >= 35:
            starter_signal -= 58.0
        if years_exp >= 8 and age >= 33 and apy_m <= 12.0:
            starter_signal -= 38.0
    elif model_position in {"OT", "IOL", "EDGE", "DT", "CB", "S"}:
        starter_signal += apy_pct * 0.2

    return (
        slot_priority.get(slot, 99),
        -round(starter_signal, 4),
        0 if player.get("has_contract") else 1,
        _safe_int(player.get("espn_rank"), 99),
        -apy_m,
        -years_exp,
        draft_number,
        str(player.get("player_name", "")),
    )


def _room_family(position: str) -> str:
    pos = str(position or "").strip().upper()
    if pos in {"QB"}:
        return "QB"
    if pos in {"RB"}:
        return "RB"
    if pos in {"WR", "TE"}:
        return "RECEIVER"
    if pos in {"OT", "IOL"}:
        return "OL"
    if pos in {"EDGE", "DT"}:
        return "DL"
    if pos in {"LB"}:
        return "LB"
    if pos in {"CB", "S"}:
        return "DB"
    return pos


def _canonical_position_score(player: dict, candidate_pos: str) -> float:
    slot = str(player.get("depth_chart_position") or "").strip().upper()
    current_pos = str(player.get("position") or "").strip().upper()
    implied_pos = _slot_implied_position(slot)
    score = 0.0

    if candidate_pos == current_pos:
        score += 60.0
    if implied_pos and candidate_pos == implied_pos:
        score += 32.0

    current_family = _room_family(current_pos)
    candidate_family = _room_family(candidate_pos)
    implied_family = _room_family(implied_pos)
    if current_family and candidate_family and current_family != candidate_family:
        score -= 28.0
    elif current_pos and candidate_pos and current_pos != candidate_pos:
        score -= 10.0
    if implied_family and candidate_family and implied_family != candidate_family:
        score -= 8.0
    if current_pos == "LB" and candidate_pos == "EDGE":
        score -= 18.0
    if current_pos == "EDGE" and candidate_pos == "LB":
        score -= 14.0
    if current_pos == "DT" and candidate_pos == "EDGE":
        score -= 12.0
    if current_pos == "OT" and candidate_pos == "IOL":
        score -= 10.0
    if current_pos == "IOL" and candidate_pos == "OT":
        score -= 10.0
    if current_pos == "CB" and candidate_pos == "S":
        score -= 14.0
    if current_pos == "S" and candidate_pos == "CB":
        score -= 14.0

    if slot in {"LT", "RT", "LG", "RG", "C", "OC"}:
        score += 12.0
    if slot in {"LCB", "RCB", "NB", "FS", "SS", "QB"}:
        score += 10.0

    score += min(float(player.get("apy_m") or 0.0), 40.0) * 0.35
    score += float(player.get("apy_pct") or 0.0) * 0.12
    score += min(float(player.get("snap_count") or 0), 1200.0) / 40.0
    score += min(float(player.get("offense_snaps") or 0), 900.0) / 75.0
    score += min(float(player.get("defense_snaps") or 0), 900.0) / 75.0
    score += max(0, 8 - _safe_int(player.get("espn_rank"), 99)) * 2.0
    if player.get("has_contract"):
        score += 6.0
    if str(player.get("depth_source") or "").strip().lower() == "espn":
        score += 10.0
    return score


def _canonicalize_team_position_rooms(
    team_players: dict[str, dict[str, list[dict]]],
    espn_by_team_pos: dict[tuple[str, str], list[dict]],
    espn_by_team_slot: dict[tuple[str, str], list[dict]],
    contract_players_by_team_pos: dict[tuple[str, str], list[dict]],
) -> None:
    canonical_by_team_player: dict[tuple[str, str], str] = {}

    for team, by_pos in team_players.items():
        candidates: dict[str, list[tuple[str, dict]]] = defaultdict(list)
        for pos, players in by_pos.items():
            for player in players:
                player_key = _norm_player_key(player.get("player_name", ""))
                if player_key:
                    candidates[player_key].append((pos, player))

        for (slot_team, pos), players in espn_by_team_pos.items():
            if slot_team != team:
                continue
            for player in players:
                player_key = _norm_player_key(player.get("player_name", ""))
                if player_key:
                    candidates[player_key].append((pos, player))

        for (contract_team, pos), players in contract_players_by_team_pos.items():
            if contract_team != team:
                continue
            for player in players:
                player_key = _norm_player_key(player.get("player_name", ""))
                if player_key:
                    candidates[player_key].append((pos, player))

        for player_key, entries in candidates.items():
            best_pos = ""
            best_score = float("-inf")
            for pos, player in entries:
                score = _canonical_position_score(player, pos)
                if score > best_score:
                    best_score = score
                    best_pos = pos
            if best_pos:
                canonical_by_team_player[(team, player_key)] = best_pos

    for team, by_pos in list(team_players.items()):
        for pos, players in list(by_pos.items()):
            by_pos[pos] = [
                player
                for player in players
                if canonical_by_team_player.get((team, _norm_player_key(player.get("player_name", ""))), pos) == pos
            ]

    for key, players in list(espn_by_team_pos.items()):
        team, pos = key
        espn_by_team_pos[key] = [
            player
            for player in players
            if canonical_by_team_player.get((team, _norm_player_key(player.get("player_name", ""))), pos) == pos
        ]

    for key, players in list(espn_by_team_slot.items()):
        team = key[0]
        espn_by_team_slot[key] = [
            player
            for player in players
            if canonical_by_team_player.get((team, _norm_player_key(player.get("player_name", ""))), str(player.get("position") or "").strip().upper())
            == str(player.get("position") or "").strip().upper()
        ]

    for key, players in list(contract_players_by_team_pos.items()):
        team, pos = key
        contract_players_by_team_pos[key] = [
            player
            for player in players
            if canonical_by_team_player.get((team, _norm_player_key(player.get("player_name", ""))), pos) == pos
        ]


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


def _youth_priority(payload: dict) -> tuple:
    designation = str(payload.get("designation") or "").strip()
    label_priority = {
        "Franchise Cornerstone": 0,
        "All-Pro": 1,
        "Blue Chip Prospect": 2,
        "In His Prime Star": 3,
        "Starter": 4,
        "Prospect": 5,
        "Backup": 6,
        "Older Mentor": 7,
        "FA": 8,
    }
    age = _safe_int(payload.get("age"), 99)
    years_exp = _safe_int(payload.get("years_exp"), 99)
    draft_number = _safe_int(payload.get("draft_number"), 9999)
    apy_m = _safe_float(payload.get("apy_m")) or 0.0
    depth_rank = _safe_int(payload.get("depth_rank"), 99)
    snap_count = _safe_int(payload.get("snap_count"), 0)
    return (
        label_priority.get(designation, 99),
        -snap_count,
        age,
        years_exp,
        depth_rank,
        draft_number,
        -apy_m,
        str(payload.get("player_name", "")),
    )


def _is_rising_young_player(payload: dict) -> bool:
    designation = str(payload.get("designation") or "").strip()
    age = _safe_int(payload.get("age"), 99)
    years_exp = _safe_int(payload.get("years_exp"), 99)
    depth_rank = _safe_int(payload.get("depth_rank"), 99)
    draft_number = _safe_int(payload.get("draft_number"), 9999)
    apy_pct = float(payload.get("apy_pct") or 0.0)
    apy_m = _safe_float(payload.get("apy_m")) or 0.0
    position = str(payload.get("position") or "").strip().upper()
    role_label = str(payload.get("role_label") or "").strip()
    usage_proxy = _usage_proxy_from_role(position, role_label, depth_rank)
    snap_count = _safe_int(payload.get("snap_count"), 0)
    starter_cutoff = int(STARTERS_BY_POSITION.get(position, 1))

    if age > 26 or years_exp > 4 or depth_rank > max(2, starter_cutoff + 1):
        return False
    if designation in {"Backup", "Older Mentor", "FA"}:
        return False
    if designation in {"Franchise Cornerstone", "All-Pro", "Blue Chip Prospect"}:
        return True
    if designation == "Prospect":
        return ((age <= 24 and draft_number <= 60) or snap_count >= 700)
    if designation == "In His Prime Star":
        return age <= 27 and years_exp <= 4 and (snap_count >= 450 or usage_proxy >= 0.84)
    if designation == "Starter":
        if age <= 24 and snap_count >= 360:
            return True
        if (
            years_exp <= 2
            and depth_rank <= starter_cutoff
            and (draft_number <= 80 or apy_pct >= 65.0 or apy_m >= 4.0)
            and (snap_count >= 500 or usage_proxy >= 0.84)
        ):
            return True
        if position == "QB" and years_exp <= 3 and apy_m >= 8.0 and age <= 26:
            return True
        return False
    return False


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


def _minimum_star_apy(position: str) -> float:
    pos = str(position or "").strip().upper()
    thresholds = {
        "QB": 20.0,
        "RB": 8.0,
        "WR": 14.0,
        "TE": 11.0,
        "OT": 12.0,
        "IOL": 9.0,
        "EDGE": 14.0,
        "DT": 11.0,
        "LB": 8.0,
        "CB": 10.0,
        "S": 10.0,
    }
    return thresholds.get(pos, 10.0)


def _minimum_cornerstone_apy(position: str) -> float:
    pos = str(position or "").strip().upper()
    thresholds = {
        "QB": 30.0,
        "RB": 10.0,
        "WR": 18.0,
        "TE": 14.0,
        "OT": 16.0,
        "IOL": 12.0,
        "EDGE": 18.0,
        "DT": 15.0,
        "LB": 11.0,
        "CB": 14.0,
        "S": 13.0,
    }
    return thresholds.get(pos, 14.0)


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
        "NOSE": 0.82,
        "5-TECH": 0.82,
        "DT": 0.84,
        "IDL": 0.8,
        "MIKE": 0.84,
        "WILL": 0.8,
        "SAM": 0.72,
        "ILB": 0.78,
        "LB1": 0.74,
        "LB2": 0.68,
        "CB1": 0.9,
        "CB2": 0.84,
        "NICKEL": 0.78,
        "FS": 0.88,
        "SS": 0.86,
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


def _split_semis(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(";") if part.strip()]


def _parse_game_teams(game_id: str) -> tuple[str, str]:
    text = str(game_id or "").strip().upper()
    match = re.match(r"^\d{4}_\d{2}_([A-Z0-9]{2,3})_([A-Z0-9]{2,3})$", text)
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def _build_player_snap_counts() -> dict[tuple[str, str], dict[str, int]]:
    if pl is None or not NFLVERSE_PARTICIPATION.exists():
        return {}
    participation = pl.read_parquet(NFLVERSE_PARTICIPATION)
    if participation.is_empty():
        return {}

    p = participation.with_columns(
        pl.col("nflverse_game_id").str.slice(0, 4).cast(pl.Int32, strict=False).alias("season_tag")
    )
    latest_season = _safe_int(p.select(pl.col("season_tag").max()).item(), 0)
    if latest_season:
        p = p.filter(pl.col("season_tag") == latest_season)

    snap_counts: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"snap_count": 0, "offense_snaps": 0, "defense_snaps": 0}
    )
    for row in p.select(["nflverse_game_id", "possession_team", "offense_names", "defense_names"]).iter_rows(named=True):
        offense_team = str(row.get("possession_team") or "").strip().upper()
        away_team, home_team = _parse_game_teams(row.get("nflverse_game_id") or "")
        defense_team = ""
        if offense_team and away_team and home_team:
            if offense_team == away_team:
                defense_team = home_team
            elif offense_team == home_team:
                defense_team = away_team

        for name in _split_semis(row.get("offense_names", "")):
            player_key = _norm_player_key(name)
            if not player_key or not offense_team:
                continue
            payload = snap_counts[(player_key, offense_team)]
            payload["snap_count"] += 1
            payload["offense_snaps"] += 1

        for name in _split_semis(row.get("defense_names", "")):
            player_key = _norm_player_key(name)
            if not player_key or not defense_team:
                continue
            payload = snap_counts[(player_key, defense_team)]
            payload["snap_count"] += 1
            payload["defense_snaps"] += 1

    return snap_counts


def _free_agent_priority(payload: dict) -> tuple:
    position = str(payload.get("position") or "").strip().upper()
    designation = str(payload.get("designation") or "").strip()
    role_label = str(payload.get("role_label") or "").strip()
    depth_rank = _safe_int(payload.get("depth_rank"), 99)
    snap_count = _safe_int(payload.get("snap_count"), 0)
    usage_proxy = _usage_proxy_from_role(position, role_label, depth_rank)
    apy_pct = float(payload.get("apy_pct") or 0.0)
    apy_m = _safe_float(payload.get("apy_m")) or 0.0
    years_exp = _safe_int(payload.get("years_exp"), 0)
    age = _safe_int(payload.get("age"), 99)
    return (
        _designation_priority(designation),
        -snap_count,
        -round(usage_proxy, 4),
        -apy_pct,
        -apy_m,
        TEAM_NEEDS_POS_ORDER.index(position) if position in TEAM_NEEDS_POS_ORDER else 99,
        -years_exp,
        age,
        str(payload.get("player_name", "")),
    )


def _contract_watch_priority(payload: dict) -> tuple:
    position = str(payload.get("position") or "").strip().upper()
    designation = str(payload.get("designation") or "").strip()
    role_label = str(payload.get("role_label") or "").strip()
    depth_rank = _safe_int(payload.get("depth_rank"), 99)
    snap_count = _safe_int(payload.get("snap_count"), 0)
    usage_proxy = _usage_proxy_from_role(position, role_label, depth_rank)
    apy_pct = float(payload.get("apy_pct") or 0.0)
    apy_m = _safe_float(payload.get("apy_m")) or 0.0
    contract_years = _safe_int(payload.get("contract_years"), 0)
    years_exp = _safe_int(payload.get("years_exp"), 0)
    age = _safe_int(payload.get("age"), 99)
    return (
        _designation_priority(designation),
        contract_years,
        -snap_count,
        -round(usage_proxy, 4),
        -apy_pct,
        -apy_m,
        TEAM_NEEDS_POS_ORDER.index(position) if position in TEAM_NEEDS_POS_ORDER else 99,
        -years_exp,
        age,
        str(payload.get("player_name", "")),
    )


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
    apy_value = float(apy_m or 0.0)
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

    if usage_proxy >= 0.95 and apy_pct >= 98.5 and apy_value >= max(20.0, _minimum_cornerstone_apy(model_position)) and effective_years_exp >= 6:
        return "HOF Path"
    if (
        usage_proxy >= 0.88
        and apy_pct >= 96.0
        and apy_value >= _minimum_cornerstone_apy(model_position)
        and effective_years_exp >= 4
        and (age is None or age <= 31)
    ):
        return "All-Pro"
    if usage_proxy >= 0.62 and (
        (age is not None and age >= 33 and effective_years_exp >= 8)
        or (veteran_inference and (apy_m or 0.0) >= 8.0 and contract_years <= 2)
        or (model_position == "QB" and age is not None and age >= 34)
    ):
        return "Older Mentor"
    if (
        usage_proxy >= 0.84
        and is_top_starter
        and years_exp <= 2
        and contract_years >= 3
        and draft_number is not None
        and 0 < draft_number <= 32
        and (age is None or age <= 24)
    ):
        return "Franchise Cornerstone"
    if (
        usage_proxy >= 0.84
        and is_top_starter
        and draft_number is not None
        and 0 < draft_number <= 32
        and 2 <= effective_years_exp <= 4
        and (age is None or age <= 25)
    ):
        return "Franchise Cornerstone"
    if (
        usage_proxy >= 0.76
        and is_top_starter
        and years_exp <= 2
        and contract_years >= 3
        and draft_number is not None
        and 0 < draft_number <= 40
        and (age is None or age <= 25)
    ):
        return "Blue Chip Prospect"
    if usage_proxy >= 0.84 and (
        (
            effective_years_exp >= 2
            and apy_pct >= 88.0
            and apy_value >= _minimum_cornerstone_apy(model_position)
        )
        or (
            model_position == "QB"
            and effective_years_exp >= 2
            and apy_pct >= 80.0
            and apy_value >= 24.0
        )
    ) and (age is None or age <= 31):
        return "Franchise Cornerstone"
    if (
        usage_proxy >= 0.82
        and effective_years_exp >= 6
        and apy_pct >= 95.0
        and apy_value >= _minimum_star_apy(model_position)
        and (age is None or age <= 34)
    ):
        return "All-Pro"
    if (
        is_top_starter
        and usage_proxy >= 0.76
        and 2 <= effective_years_exp <= 6
        and (age is None or age <= 29)
        and apy_pct >= 88.0
        and apy_value >= _minimum_star_apy(model_position)
    ):
        return "In His Prime Star"
    if (
        usage_proxy >= 0.78
        and apy_pct >= 80.0
        and apy_value >= _minimum_star_apy(model_position)
        and 3 <= effective_years_exp <= 10
        and age is not None
        and age <= 33
    ):
        return "In His Prime Star"
    if years_exp <= 1 and not veteran_inference:
        if is_early_pick and usage_proxy >= 0.62:
            return "Blue Chip Prospect"
        if usage_proxy >= 0.46 and likely_young_player and (age is None or age <= 25):
            return "Prospect"
    if usage_proxy >= 0.56:
        return "Starter"
    if years_exp <= 1 and usage_proxy >= 0.4 and likely_young_player and (age is None or age <= 25):
        return "Prospect"
    return "Backup"


def _designation_depth_rank(position: str, slot: str, overall_rank: int, slot_rank: int) -> int:
    pos = str(position or "").strip().upper()
    depth_slot = str(slot or "").strip().upper()
    overall = max(1, int(overall_rank or 1))
    slot_rank = max(1, int(slot_rank or 1))
    if pos == "S" and depth_slot in {"FS", "SS", "S", "SAF", "RS"}:
        return slot_rank
    if pos == "CB" and depth_slot in {"LCB", "RCB", "NB", "CB"}:
        return slot_rank
    if pos == "OT" and depth_slot in {"LT", "RT", "T"}:
        return slot_rank
    if pos == "IOL" and depth_slot in {"LG", "C", "RG", "G"}:
        return slot_rank
    return overall


def _transaction_priority(event: dict) -> int:
    status_kind = str(event.get("status_kind") or "").strip().lower()
    label = str(event.get("label") or "").strip().lower()
    affects_team_needs = bool(event.get("affects_team_needs"))

    score = 0
    if status_kind == "confirmed":
        score += 20
    elif status_kind == "rumored":
        score += 8

    is_resign = "re-signed" in label or "resigned" in label
    if "traded" in label:
        score += 55
    if "released" in label:
        score += 48
    if "cut" in label:
        score += 48
    if "waived" in label:
        score += 46
    if "retired" in label:
        score += 52
    if "franchise tag" in label or "tagged" in label:
        score += 40
    if "signed" in label and not is_resign:
        score += 28
    if "agreed" in label:
        score += 24
    if is_resign:
        score += 4
    if "extension" in label:
        score += 6
    if "tendered" in label:
        score += 10
    if "promoted" in label:
        score += 6
    if "designated" in label:
        score += 8
    if affects_team_needs:
        score += 12
    if str(event.get("player_name") or "").strip():
        score += 4
    return score


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

    transaction_team_override_by_name: dict[str, dict[str, str]] = {}
    retired_or_released_players: set[str] = set()
    for row in _read_csv(TRANSACTION_OVERRIDES_CSV):
        status_kind = _status_kind(row.get("transaction_status", "confirmed"))
        if status_kind != "confirmed":
            continue
        player_key = _norm_player_key(row.get("player_name", ""))
        if not player_key:
            continue
        event_date = _parse_event_date(row.get("event_date", ""))
        action = str(row.get("action_text") or "").strip().lower()
        from_team = str(row.get("from_team", "")).strip().upper()
        to_team = str(row.get("to_team", "")).strip().upper()
        current_team = ""
        if "trade" in action or "signed" in action or "claimed" in action or "agreed" in action:
            current_team = to_team or current_team
        elif any(token in action for token in {"cut", "waived", "released", "retired"}):
            current_team = ""
            retired_or_released_players.add(player_key)
        else:
            current_team = to_team or from_team
        existing = transaction_team_override_by_name.get(player_key)
        if existing is None or str(existing.get("event_date") or "") < str(event_date or ""):
            transaction_team_override_by_name[player_key] = {
                "event_date": event_date.isoformat() if event_date else "",
                "current_team": current_team,
                "from_team": from_team,
                "to_team": to_team,
                "action": action,
            }

    player_snap_counts = _build_player_snap_counts()

    player_team_candidates: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for player_key, override in transaction_team_override_by_name.items():
        current_team = str(override.get("current_team") or "").strip().upper()
        if current_team:
            player_team_candidates[player_key][current_team] += 100.0

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
                latest_team = (
                    transaction_team_override_by_name.get(key, {}).get("current_team", latest_team) or latest_team
                )
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

    all_contract_rows = []
    contract_rows = []
    apy_pool_by_pos: dict[str, list[float]] = defaultdict(list)

    def _contract_end_year(row: dict) -> int:
        years = _safe_int(row.get("years"), 0)
        year_signed = _safe_int(row.get("year_signed"), 0)
        if years and year_signed:
            return year_signed + max(years - 1, 0)
        return 0

    def _contract_years_remaining(row: dict) -> int:
        end_year = _contract_end_year(row)
        if end_year:
            return max(0, end_year - CURRENT_DRAFT_YEAR + 1)
        return _safe_int(row.get("years"), 0)
    contract_by_gsis: dict[str, dict] = {}
    contract_by_name: dict[str, dict] = {}
    contract_history_by_name_team: dict[tuple[str, str], dict] = {}
    contract_players_by_team_pos: dict[tuple[str, str], list[dict]] = defaultdict(list)
    if NFLVERSE_CONTRACTS.exists():
        contracts = pl.read_parquet(NFLVERSE_CONTRACTS)
        if not contracts.is_empty():
            all_contract_rows = list(contracts.iter_rows(named=True))
            contract_rows = []
            for row in all_contract_rows:
                end_year = _contract_end_year(row)
                is_current = bool(row.get("is_active"))
                if end_year:
                    is_current = end_year >= CURRENT_DRAFT_YEAR
                if is_current:
                    contract_rows.append(row)
    for row in all_contract_rows:
        name_key = _norm_player_key(row.get("player") or "")
        if not name_key:
            continue
        years = _safe_int(row.get("years"), 0)
        year_signed = _safe_int(row.get("year_signed"), 0)
        contract_end_year = _contract_end_year(row)
        apy = _safe_float(row.get("apy"))
        for team_code in _normalize_contract_team_codes(row.get("team") or ""):
            key = (name_key, team_code)
            existing = contract_history_by_name_team.get(key)
            existing_score = (
                _safe_int(existing.get("contract_end_year"), 0) if existing else 0,
                _safe_float(existing.get("apy")) or 0.0 if existing else 0.0,
            )
            new_score = (contract_end_year, apy or 0.0)
            if existing is None or new_score > existing_score:
                contract_history_by_name_team[key] = {
                    "team_norm": team_code,
                    "years": years,
                    "year_signed": year_signed,
                    "contract_end_year": contract_end_year,
                    "apy": apy,
                }
    for row in contract_rows:
        pos = _map_team_needs_position(row.get("position", ""))
        apy = _safe_float(row.get("apy"))
        if pos and apy is not None and apy > 0:
            apy_pool_by_pos[pos].append(float(apy))
    for row in contract_rows:
        gsis = str(row.get("gsis_id") or "").strip()
        name = str(row.get("player") or "").strip()
        name_key = _norm_player_key(name)
        apy = _safe_float(row.get("apy"))
        years = _contract_years_remaining(row)
        pos = _map_team_needs_position(row.get("position", ""))
        team_text = str(row.get("team") or "").strip()
        candidate_team_codes = _normalize_contract_team_codes(team_text)
        payload = {
            "gsis_id": gsis,
            "name_key": name_key,
            "apy": apy,
            "years": years,
            "position": pos,
            "team_text": team_text,
            "team_norm": "",
            "team_codes": candidate_team_codes,
        }
        player_master = players_master_by_name.get(payload["name_key"], {})
        latest_team = str(player_master.get("latest_team") or "").strip().upper()
        override_team = str(transaction_team_override_by_name.get(payload["name_key"], {}).get("current_team") or "").strip().upper()
        resolved_team = ""
        if override_team and override_team in candidate_team_codes:
            resolved_team = override_team
        elif latest_team and latest_team in candidate_team_codes:
            resolved_team = latest_team
        elif len(candidate_team_codes) == 1:
            resolved_team = candidate_team_codes[0]
        payload["team_norm"] = resolved_team
        latest_team_match = (
            bool(player_master)
            and latest_team == resolved_team
            and str(player_master.get("status") or "").strip().upper() not in {"RET", "CUT"}
        )
        if resolved_team and pos and name and (latest_team_match or override_team == resolved_team or len(candidate_team_codes) == 1):
            player_team_candidates[payload["name_key"]][resolved_team] += 70.0
            contract_players_by_team_pos[(resolved_team, pos)].append(
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
        player_key = _norm_player_key(row.get("full_name") or row.get("football_name") or "")
        if player_key in retired_or_released_players:
            continue
        override_team = transaction_team_override_by_name.get(player_key, {}).get("current_team", "")
        if override_team and team != override_team:
            continue
        roster_status = str(row.get("status") or row.get("roster_status") or "").strip().upper()
        if roster_status in {"RET", "CUT"}:
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
        player_override = transaction_team_override_by_name.get(_norm_player_key(name), {})
        override_current_team = str(player_override.get("current_team") or "").strip().upper()
        override_action = str(player_override.get("action") or "").strip().lower()
        transaction_rostered = override_current_team == team and any(
            token in override_action for token in {"trade", "signed", "claimed", "agreed"}
        )
        player_master = players_master_by_name.get(_norm_player_key(name), {})
        historical_contract = contract_history_by_name_team.get((_norm_player_key(name), team))
        apy = _safe_float(contract.get("apy")) if contract else None
        apy_pct = _pct_score(apy, apy_pool_by_pos.get(model_pos, []))
        years_left = _safe_int(contract.get("years"), 0) if contract else 0
        historical_contract_valid = (
            bool(historical_contract)
            and _safe_int(historical_contract.get("contract_end_year"), 0) >= CURRENT_DRAFT_YEAR
        )
        latest_team_match = (
            bool(player_master)
            and str(player_master.get("latest_team") or "").strip().upper() == team
            and str(player_master.get("status") or "").strip().upper() not in {"RET", "CUT"}
        )
        inferred_rookie_contract = (
            latest_team_match
            and _safe_int(player_master.get("rookie_season"), 0) >= (latest_season - 1)
            and _safe_int(player_master.get("years_of_experience"), 0) <= 1
        )
        has_contract = bool(contract) or historical_contract_valid or inferred_rookie_contract or transaction_rostered
        contract_status_kind = "active_contract"
        if contract is not None:
            contract_label = f"{years_left}y | ${apy:.1f}M APY" if apy is not None else f"{years_left}y contract"
        elif historical_contract_valid:
            hist_years = _safe_int(historical_contract.get("years"), 0)
            hist_apy = _safe_float(historical_contract.get("apy"))
            years_left = hist_years
            apy = hist_apy
            apy_pct = _pct_score(apy, apy_pool_by_pos.get(model_pos, [])) if apy is not None else apy_pct
            contract_status_kind = "historical_contract"
            contract_label = (
                f"{hist_years}y | ${hist_apy:.1f}M APY"
                if hist_apy is not None and hist_years
                else "Rostered"
            )
        elif inferred_rookie_contract:
            contract_status_kind = "rookie_deal"
            contract_label = "Rookie deal"
        elif transaction_rostered:
            contract_status_kind = "transaction_rostered"
            contract_label = "Rostered"
        else:
            contract_status_kind = "unsigned_watch"
            contract_label = "Contract watch"

        snap_payload = player_snap_counts.get((_norm_player_key(name), team), {})

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
            "contract_status_kind": contract_status_kind,
            "apy_m": round(apy, 2) if apy is not None else "",
            "apy_pct": apy_pct,
            "snap_count": int(snap_payload.get("snap_count", 0)),
            "offense_snaps": int(snap_payload.get("offense_snaps", 0)),
            "defense_snaps": int(snap_payload.get("defense_snaps", 0)),
        }
        if player_key:
            player_team_candidates[player_key][team] += 60.0

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
                "contract_status_kind": contract_status_kind,
                "apy_m": round(apy, 2) if apy is not None else "",
                "apy_pct": apy_pct,
                "snap_count": int(snap_payload.get("snap_count", 0)),
                "offense_snaps": int(snap_payload.get("offense_snaps", 0)),
                "defense_snaps": int(snap_payload.get("defense_snaps", 0)),
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
        if player_key in retired_or_released_players:
            continue
        roster_info = roster_lookup_by_team.get(team, {}).get(player_key, {})
        contract = contract_by_name.get(player_key)
        player_override = transaction_team_override_by_name.get(player_key, {})
        override_current_team = str(player_override.get("current_team") or "").strip().upper()
        override_action = str(player_override.get("action") or "").strip().lower()
        transaction_rostered = override_current_team == team and any(
            token in override_action for token in {"trade", "signed", "claimed", "agreed"}
        )
        moved_with_existing_contract = bool(contract) and transaction_rostered
        contract_team_match = bool(contract) and (
            str(contract.get("team_norm") or "").strip().upper() == team or moved_with_existing_contract
        )
        historical_contract = contract_history_by_name_team.get((player_key, team))
        player_master = players_master_by_name.get(player_key, {})
        latest_team_match = (
            bool(player_master)
            and str(player_master.get("latest_team") or "").strip().upper() == team
            and str(player_master.get("status") or "").strip().upper() not in {"RET", "CUT"}
        )
        if not roster_info and not contract_team_match and not latest_team_match and not transaction_rostered:
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
        if player_key:
            player_team_candidates[player_key][team] += 25.0
            if roster_info:
                player_team_candidates[player_key][team] += 15.0
            if contract_team_match:
                player_team_candidates[player_key][team] += 20.0
            if latest_team_match:
                player_team_candidates[player_key][team] += 15.0
        apy = _safe_float(contract.get("apy")) if contract else _safe_float(roster_info.get("apy_m"))
        apy_pct = (
            _pct_score(apy, apy_pool_by_pos.get(model_pos, []))
            if apy is not None
            else float(roster_info.get("apy_pct") or 0.0)
        )
        years_left = _safe_int(contract.get("years"), 0) if contract else 0
        roster_has_contract = bool(roster_info) and bool(roster_info.get("has_contract"))
        inferred_rookie_contract = (
            latest_team_match
            and _safe_int(player_master.get("rookie_season"), 0) >= (latest_season - 1)
            and _safe_int(player_master.get("years_of_experience"), 0) <= 1
        )
        historical_team_contract = (
            bool(historical_contract)
            and latest_team_match
            and _safe_int(historical_contract.get("contract_end_year"), 0) >= CURRENT_DRAFT_YEAR
        )
        has_contract = contract_team_match or roster_has_contract or inferred_rookie_contract or historical_team_contract or transaction_rostered
        contract_status_kind = "active_contract"
        if contract_team_match and contract:
            contract_label = f"{years_left}y | ${apy:.1f}M APY" if apy is not None else f"{years_left}y contract"
        elif historical_team_contract:
            hist_years = _safe_int(historical_contract.get("years"), 0)
            hist_apy = _safe_float(historical_contract.get("apy"))
            years_left = hist_years
            apy = hist_apy if hist_apy is not None else apy
            apy_pct = _pct_score(apy, apy_pool_by_pos.get(model_pos, [])) if apy is not None else apy_pct
            contract_status_kind = "historical_contract"
            contract_label = (
                f"{hist_years}y | ${hist_apy:.1f}M APY"
                if hist_apy is not None and hist_years
                else "Rostered"
            )
        elif roster_has_contract and roster_info:
            contract_status_kind = str(roster_info.get("contract_status_kind") or "active_contract")
            contract_label = str(roster_info.get("contract_label") or "Rostered")
        elif inferred_rookie_contract:
            contract_status_kind = "rookie_deal"
            contract_label = "Rookie deal"
        elif transaction_rostered:
            contract_status_kind = "transaction_rostered"
            contract_label = "Rostered"
        else:
            contract_status_kind = "unsigned_watch"
            contract_label = "Contract watch"

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
            "contract_status_kind": contract_status_kind,
            "apy_m": round(apy, 2) if apy is not None else roster_info.get("apy_m", ""),
            "apy_pct": apy_pct,
            "depth_source": "espn",
            "espn_rank": _safe_int(row.get("rank"), 99),
            "position_group": str(row.get("position_group") or "").strip(),
            "snap_count": _safe_int(roster_info.get("snap_count"), 0),
            "offense_snaps": _safe_int(roster_info.get("offense_snaps"), 0),
            "defense_snaps": _safe_int(roster_info.get("defense_snaps"), 0),
        }
        espn_by_team_pos[(team, model_pos)].append(payload)
        if slot:
            espn_by_team_slot[(team, slot)].append(payload)
        if model_pos in DEFENSE_POS_ORDER and payload["position_group"]:
            espn_defense_groups_by_team[team].append(payload["position_group"])

    preferred_team_by_player: dict[str, str] = {}
    for player_key, team_scores in player_team_candidates.items():
        if not player_key or not team_scores:
            continue
        preferred_team_by_player[player_key] = max(
            team_scores.items(),
            key=lambda item: (float(item[1]), item[0]),
        )[0]

    for team, by_pos in list(team_players.items()):
        for pos, players in list(by_pos.items()):
            by_pos[pos] = [
                player
                for player in players
                if preferred_team_by_player.get(_norm_player_key(player.get("player_name", "")), team) == team
            ]

    for key, rows in list(espn_by_team_pos.items()):
        team = key[0]
        espn_by_team_pos[key] = [
            row
            for row in rows
            if preferred_team_by_player.get(_norm_player_key(row.get("player_name", "")), team) == team
        ]

    for key, rows in list(espn_by_team_slot.items()):
        team = key[0]
        espn_by_team_slot[key] = [
            row
            for row in rows
            if preferred_team_by_player.get(_norm_player_key(row.get("player_name", "")), team) == team
        ]

    for key, rows in list(contract_players_by_team_pos.items()):
        team = key[0]
        contract_players_by_team_pos[key] = [
            row
            for row in rows
            if preferred_team_by_player.get(_norm_player_key(row.get("player_name", "")), team) == team
        ]

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
                if player_key and player_key not in existing_keys:
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
                if player_key and player_key not in existing_keys:
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
            if player_key and player_key not in existing_keys:
                merged_players.append(player)
                existing_keys.add(player_key)
        if merged_players:
            team_players[team][pos] = merged_players

    _canonicalize_team_position_rooms(
        team_players,
        espn_by_team_pos,
        espn_by_team_slot,
        contract_players_by_team_pos,
    )

    out: dict[str, dict] = {}
    for team, by_pos in team_players.items():
        offense_lanes = []
        defense_lanes = []
        team_payloads = []
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
                    if str(player.get("position") or "").strip().upper() != pos:
                        continue
                    selected_players.append(player)
                    selected_keys.add(player_key)

            players = sorted(by_pos.get(pos, []), key=lambda p: _player_sort_tuple(p, slot_priority))
            for player in players:
                player_key = _norm_player_key(player.get("player_name", ""))
                if not player_key or player_key in selected_keys:
                    continue
                selected_players.append(player)
                selected_keys.add(player_key)

            selected_players = sorted(selected_players, key=lambda p: _player_sort_tuple(p, slot_priority))

            lane_players = []
            slot_counts: dict[str, int] = defaultdict(int)
            for idx, player in enumerate(selected_players, start=1):
                slot = str(player.get("depth_chart_position") or "").strip().upper()
                slot_counts[slot] += 1
                slot_rank = slot_counts[slot] or idx
                role_label = _slot_display_label(pos, slot, front_family, slot_rank)
                designation_depth_rank = _designation_depth_rank(pos, slot, idx, slot_rank)
                label = _player_designation(
                    has_contract=bool(player.get("has_contract")),
                    years_exp=int(player.get("years_exp") or 0),
                    age=int(player.get("age")) if str(player.get("age", "")).strip() else None,
                    apy_pct=float(player.get("apy_pct") or 0.0),
                    apy_m=_safe_float(player.get("apy_m")),
                    contract_years=int(player.get("contract_years") or 0),
                    depth_rank=designation_depth_rank,
                    model_position=pos,
                    draft_number=int(player.get("draft_number")) if str(player.get("draft_number", "")).strip() else None,
                    role_label=role_label,
                )
                payload = {
                    "player_name": player.get("player_name", ""),
                    "position": pos,
                    "depth_rank": designation_depth_rank,
                    "lane_depth_rank": idx,
                    "designation": label,
                    "role_label": role_label,
                    "detail_label": _player_detail_line(player, role_label),
                    "meta_label": _player_meta_line(player),
                    "contract_label": player.get("contract_label", ""),
                    "contract_status_kind": str(player.get("contract_status_kind") or ""),
                    "years_exp": int(player.get("years_exp") or 0),
                    "age": player.get("age", ""),
                    "apy_m": player.get("apy_m", ""),
                    "apy_pct": float(player.get("apy_pct") or 0.0),
                    "draft_number": int(player.get("draft_number")) if str(player.get("draft_number", "")).strip() else "",
                    "snap_count": int(player.get("snap_count") or 0),
                }
                lane_players.append(payload)
                team_payloads.append(payload)

            lane = {"position": pos, "players": lane_players}
            if pos in OFFENSE_POS_ORDER:
                offense_lanes.append(lane)
            if pos in DEFENSE_POS_ORDER:
                defense_lanes.append(lane)

        unique_team_payloads = []
        seen_payload_keys: set[str] = set()
        for payload in sorted(
            team_payloads,
            key=lambda p: (
                TEAM_NEEDS_POS_ORDER.index(str(p.get("position") or "").strip().upper()) if str(p.get("position") or "").strip().upper() in TEAM_NEEDS_POS_ORDER else 99,
                int(p.get("depth_rank") or 99),
                str(p.get("player_name", "")),
            ),
        ):
            player_key = _norm_player_key(payload.get("player_name", ""))
            pos = str(payload.get("position") or "").strip().upper()
            composite_key = f"{pos}:{player_key}"
            if not player_key or composite_key in seen_payload_keys:
                continue
            seen_payload_keys.add(composite_key)
            unique_team_payloads.append(payload)

        unique_player_payloads = []
        seen_player_keys: set[str] = set()
        for payload in sorted(
            unique_team_payloads,
            key=lambda p: (
                _designation_priority(str(p.get("designation") or "").strip()),
                -_safe_int(p.get("snap_count"), 0),
                TEAM_NEEDS_POS_ORDER.index(str(p.get("position") or "").strip().upper()) if str(p.get("position") or "").strip().upper() in TEAM_NEEDS_POS_ORDER else 99,
                str(p.get("player_name", "")),
            ),
        ):
            player_key = _norm_player_key(payload.get("player_name", ""))
            if not player_key or player_key in seen_player_keys:
                continue
            seen_player_keys.add(player_key)
            unique_player_payloads.append(payload)

        free_agents = sorted(
            [p for p in unique_player_payloads if str(p.get("contract_status_kind") or "").strip() == "unsigned_watch"],
            key=_free_agent_priority,
        )
        key_free_agents = free_agents[:2]
        contract_watch_pool = list(free_agents[2:])
        watch_seen = {
            _norm_player_key(p.get("player_name", ""))
            for p in contract_watch_pool
            if _norm_player_key(p.get("player_name", ""))
        }
        for payload in unique_player_payloads:
            status_kind = str(payload.get("contract_status_kind") or "").strip()
            player_key = _norm_player_key(payload.get("player_name", ""))
            contract_years = _safe_int(payload.get("contract_years"), 0)
            snap_count = _safe_int(payload.get("snap_count"), 0)
            if not player_key or player_key in watch_seen:
                continue
            if status_kind != "unsigned_watch" and contract_years <= 1 and snap_count >= 150:
                contract_watch_pool.append(payload)
                watch_seen.add(player_key)
        contract_watch = sorted(contract_watch_pool, key=_contract_watch_priority)[:8]

        youth = sorted(
            [p for p in unique_player_payloads if _is_rising_young_player(p)],
            key=_youth_priority,
        )[:8]

        out[team] = {
            "depth_chart": {
                "offense": offense_lanes,
                "defense": defense_lanes,
                "season": latest_season,
                "week": latest_week,
            },
            "free_agents": key_free_agents,
            "free_agents_full": free_agents,
            "contract_watch": contract_watch,
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


def _metric_config_for_position(position: str, config_map: dict[str, list[dict]]) -> list[dict]:
    pos = str(position or "").strip().upper()
    if pos in config_map:
        return config_map[pos]
    if pos in {"OT", "IOL"} and "OT" in config_map:
        return config_map["OT"]
    return []


def _format_metric_value(value: float, fmt: str) -> str:
    if fmt == "int":
        return str(int(round(value)))
    if fmt == "pct":
        return f"{value * 100:.1f}%"
    if fmt == "pct100":
        return f"{value:.1f}%"
    if fmt == "dec1":
        return f"{value:.1f}"
    if fmt == "dec3":
        return f"{value:.3f}"
    return f"{value:.2f}"


def _build_metric_cards(
    row: dict,
    position: str,
    config_map: dict[str, list[dict]],
    pos_metric_values: dict[str, dict[str, list[float]]],
) -> tuple[list[dict], dict[str, float], dict[str, float]]:
    cards: list[dict] = []
    metrics: dict[str, float] = {}
    percentiles: dict[str, float] = {}
    for cfg in _metric_config_for_position(position, config_map):
        key = str(cfg.get("key", "")).strip()
        if not key:
            continue
        raw = _safe_float(row.get(key))
        if raw is None:
            continue
        metrics[key] = round(float(raw), 4)
        pop = pos_metric_values.get(position, {}).get(key, [])
        pct = _pct_rank(float(raw), pop)
        if pct is not None:
            percentiles[key] = pct
        public_pct = float(pct if pct is not None else 50.0)
        if cfg.get("lower_better"):
            public_pct = 100.0 - public_pct
        public_pct = round(_clamp(public_pct, 0.0, 100.0), 1)
        cards.append(
            {
                "key": key,
                "label": cfg.get("label", key),
                "raw": round(float(raw), 4),
                "display": _format_metric_value(float(raw), str(cfg.get("fmt", "dec2"))),
                "pct": public_pct,
                "fmt": cfg.get("fmt", "dec2"),
                "lower_better": bool(cfg.get("lower_better", False)),
                "weight": round(float(cfg.get("weight", 0.0)), 4),
            }
        )
    return cards, metrics, percentiles


def _build_counting_stat_chips(
    row: dict,
    position: str,
    config_map: dict[str, list[dict]],
) -> tuple[list[dict], dict[str, float]]:
    chips: list[dict] = []
    metrics: dict[str, float] = {}
    for cfg in _metric_config_for_position(position, config_map):
        key = str(cfg.get("key", "")).strip()
        if not key:
            continue
        raw = _safe_float(row.get(key))
        if raw is None:
            continue
        metrics[key] = round(float(raw), 4)
        chips.append(
            {
                "key": key,
                "label": cfg.get("label", key),
                "raw": round(float(raw), 4),
                "display": _format_metric_value(float(raw), str(cfg.get("fmt", "dec2"))),
                "fmt": cfg.get("fmt", "dec2"),
            }
        )
    return chips, metrics


def _weighted_percentile_composite(cards: list[dict]) -> float | None:
    if not cards:
        return None
    weighted_sum = 0.0
    weight_total = 0.0
    for card in cards:
        pct = _safe_float(card.get("pct"))
        weight = _safe_float(card.get("weight"))
        if pct is None:
            continue
        w = float(weight) if weight is not None and weight > 0 else 1.0
        weighted_sum += float(pct) * w
        weight_total += w
    if weight_total <= 0:
        return None
    return round(weighted_sum / weight_total, 1)


def _build_trait_bucket_cards(row: dict) -> tuple[list[dict], float | None, str]:
    family = str(row.get("trait_bucket_family", "")).strip()
    cards: list[dict] = []
    scores: list[float] = []
    for idx in range(1, 6):
        label = str(row.get(f"trait_bucket_{idx}_label", "")).strip()
        score = _safe_float(row.get(f"trait_bucket_{idx}_score"))
        if not label or score is None:
            continue
        value = round(float(score), 2)
        cards.append({"label": label, "score": value})
        scores.append(value)
    bucket_score = _safe_float(row.get("trait_bucket_score"))
    if bucket_score is None and scores:
        bucket_score = sum(scores) / len(scores)
    return cards, (round(float(bucket_score), 2) if bucket_score is not None else None), family


def _metric_public_pct(
    row: dict,
    position: str,
    key: str,
    pos_metric_values: dict[str, dict[str, list[float]]],
    *,
    lower_better: bool = False,
) -> float | None:
    raw = _safe_float(row.get(key))
    if raw is None:
        return None
    pop = pos_metric_values.get(position, {}).get(key, [])
    pct = _pct_rank(float(raw), pop)
    if pct is None:
        return None
    public_pct = 100.0 - float(pct) if lower_better else float(pct)
    return round(_clamp(public_pct, 0.0, 100.0), 1)


def _mean_present(values: list[float | None], fallback: float | None = None) -> float | None:
    usable = [float(v) for v in values if v is not None]
    if not usable:
        return fallback
    return round(sum(usable) / len(usable), 1)


def _build_position_lens(
    row: dict,
    position: str,
    pos_metric_values: dict[str, dict[str, list[float]]],
) -> dict:
    pos = str(position or "").upper()

    def pct(key: str, *, lower_better: bool = False) -> float | None:
        return _metric_public_pct(row, pos, key, pos_metric_values, lower_better=lower_better)

    def row_item(label: str, values: list[float | None], detail: str) -> dict | None:
        value = _mean_present(values)
        if value is None:
            return None
        return {"label": label, "pct": value, "detail": detail}

    title = "ScoutingGrade Lens"
    rows: list[dict] = []
    tags: list[str] = []

    if pos == "QB":
        title = "QB Stress / Process"
        stress = row_item(
            "Pressure Management",
            [pct("sg_qb_pressure_grade"), pct("sg_qb_pressure_to_sack_rate", lower_better=True), pct("sg_qb_blitz_grade")],
            "pressure, sack avoidance, blitz answers",
        )
        decision = row_item(
            "Aggressive Creation",
            [pct("sg_qb_btt_rate"), pct("sg_qb_blitz_grade"), pct("sg_qb_twp_rate", lower_better=True)],
            "downfield aggression with enough turnover control to keep it playable",
        )
        structure = row_item(
            "Structure Passing",
            [pct("sg_qb_pass_grade"), pct("sg_qb_no_screen_grade"), pct("sg_qb_quick_qb_rating")],
            "structure passing and timing-game stability",
        )
        rows = [r for r in [stress, decision, structure] if r]
        if stress and stress["pct"] >= 72:
            tags.append("Pressure Manager")
        if decision and decision["pct"] >= 71:
            tags.append("Aggressive Creator")
        if structure and structure["pct"] >= 74:
            tags.append("Structure Passer")
    elif pos == "RB":
        title = "RB Three-Down Creator"
        contact = row_item(
            "Contact Creation",
            [pct("sg_rb_run_grade"), pct("sg_rb_elusive_rating"), pct("sg_rb_yco_attempt")],
            "run grade, elusive value, yards after contact",
        )
        explosive = row_item(
            "Explosive Running",
            [pct("sg_rb_explosive_rate"), pct("sg_rb_breakaway_percent")],
            "chunk gains and home-run carry profile",
        )
        passing = row_item(
            "Passing-Game Utility",
            [pct("sg_rb_targets_per_route"), pct("sg_rb_yprr"), pct("sg_rb_elusive_rating")],
            "receiving involvement and snap-stay utility",
        )
        rows = [r for r in [contact, explosive, passing] if r]
        if contact and contact["pct"] >= 75:
            tags.append("Contact Creator")
        if explosive and explosive["pct"] >= 75:
            tags.append("Explosive Runner")
        if passing and passing["pct"] >= 70:
            tags.append("Passing-Game Utility")
    elif pos in {"WR", "TE"}:
        title = f"{pos} Route Earner"
        route = row_item(
            "Separation",
            [pct("sg_wrte_route_grade"), pct("sg_wrte_man_yprr"), pct("sg_wrte_yprr")],
            "route quality and man-coverage separation value",
        )
        coverage = row_item(
            "Volume Earning",
            [pct("sg_wrte_targets_per_route"), pct("sg_wrte_yprr"), pct("sg_wrte_zone_yprr")],
            "how consistently the player earns routes into targets and usable volume",
        )
        reliability = row_item(
            "Vertical Stress",
            [pct("sg_wrte_man_yprr"), pct("sg_wrte_contested_catch_rate"), pct("sg_wrte_yprr")],
            "ability to punish coverage downfield and finish high-value targets",
        )
        rows = [r for r in [route, coverage, reliability] if r]
        if route and route["pct"] >= 74:
            tags.append("Separator")
        if coverage and coverage["pct"] >= 74:
            tags.append("Volume Earner")
        if reliability and reliability["pct"] >= 72:
            tags.append("Vertical Stressor")
    elif pos in {"EDGE", "DT"}:
        title = "True-Pass-Set Disruption"
        rush = row_item(
            "True-Pass-Set Wins",
            [pct("sg_dl_pass_rush_grade"), pct("sg_dl_true_pass_set_win_rate"), pct("sg_dl_true_pass_set_prp")],
            "clean pass-rush quality when protections are honest",
        )
        pressure = row_item(
            "Pocket Finish",
            [pct("sg_dl_total_pressures"), pct("sg_dl_true_pass_set_prp"), pct("sg_dl_pass_rush_grade")],
            "how often rush quality turns into actual pocket damage",
        )
        run = row_item(
            "Base-Down Value",
            [pct("sg_front_run_def_grade"), pct("sg_front_stop_percent")],
            "base-down viability alongside rush value",
        )
        rows = [r for r in [rush, pressure, run] if r]
        if pressure and pressure["pct"] >= 72:
            tags.append("Clean-Pocket Finisher")
        if rush and rush["pct"] >= 75:
            tags.append("True-Pass-Set Winner")
        if run and run["pct"] >= 72:
            tags.append("Base-Down Value")
    elif pos == "LB":
        title = "LB Dual-Threat Defender"
        run = row_item(
            "Run-Fit Control",
            [pct("sg_def_run_grade"), pct("sg_front_stop_percent"), pct("sg_def_tackle_grade")],
            "run fits, stop creation, tackle finish",
        )
        coverage = row_item(
            "Coverage Range",
            [pct("sg_def_coverage_grade"), pct("sg_cov_yards_per_snap", lower_better=True), pct("sg_slot_cov_snaps_per_target")],
            "space playability and overhang coverage utility",
        )
        pressure = row_item(
            "Pressure Utility",
            [pct("sg_def_total_pressures"), pct("sg_def_tackles_for_loss"), pct("cfb_lb_rush_impact_signal")],
            "blitz value, backfield disruption, near-the-ball impact",
        )
        rows = [r for r in [run, coverage, pressure] if r]
        if run and run["pct"] >= 75:
            tags.append("Run-Fit Anchor")
        if coverage and coverage["pct"] >= 72:
            tags.append("Coverage Range")
        if pressure and pressure["pct"] >= 70:
            tags.append("Pressure Utility")
    elif pos in {"CB", "S"}:
        title = "DB Coverage Tax"
        deterrence = row_item(
            "Target Deterrence",
            [pct("sg_cov_snaps_per_target"), pct("sg_cov_yards_per_snap", lower_better=True)],
            "how expensive it is to attack this defender",
        )
        disruption = row_item(
            "Ball Disruption",
            [pct("sg_cov_forced_incompletion_rate"), pct("sg_cov_qb_rating_against", lower_better=True), pct("sg_def_pass_break_ups")],
            "forced misses and passer suppression",
        )
        near_ball = row_item(
            "Near-Ball Impact",
            [pct("sg_def_total_pressures"), pct("sg_def_tackles_for_loss"), pct("sg_slot_cov_qb_rating_against", lower_better=True)],
            "pressure utility, box disruption, slot stress response",
        )
        rows = [r for r in [deterrence, disruption, near_ball] if r]
        if deterrence and deterrence["pct"] >= 73:
            tags.append("Target Deterrent")
        if disruption and disruption["pct"] >= 70:
            tags.append("Ball Disruptor")
        man_pct = pct("sg_cov_man_grade")
        zone_pct = pct("sg_cov_zone_grade")
        if (
            near_ball
            and near_ball["pct"] >= 60
            and man_pct is not None
            and zone_pct is not None
            and man_pct >= 62
            and zone_pct >= 62
        ):
            tags.append("Scheme Translator")
    elif pos in {"OT", "IOL"}:
        title = "OL Pass-Pro Translation"
        pass_pro = row_item(
            "Pass-Pro Translation",
            [pct("sg_ol_pass_block_grade"), pct("sg_ol_pbe"), pct("sg_ol_pressure_allowed_rate", lower_better=True)],
            "block quality plus pressure suppression",
        )
        run = row_item(
            "Run-Game Lift",
            [pct("sg_ol_run_block_grade")],
            "movement and run-game support",
        )
        flex = row_item(
            "Alignment Flex",
            [pct("sg_ol_versatility_count")],
            "multi-spot utility and lineup resilience",
        )
        rows = [r for r in [pass_pro, run, flex] if r]
        if pass_pro and pass_pro["pct"] >= 75:
            tags.append("Pass-Pro Translator")
        if run and run["pct"] >= 72:
            tags.append("Run-Game Lift")
        if flex and flex["pct"] >= 70:
            tags.append("Alignment Flex")

    return {"title": title, "rows": rows, "tags": tags}


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
        score = _transaction_priority(event)
        if score < 60:
            continue
        by_team[event.get("team", "")].append(
            {
                "event_date": event.get("event_date", ""),
                "status": event.get("status", ""),
                "status_kind": event.get("status_kind", ""),
                "label": event.get("label", ""),
                "affects_team_needs": bool(event.get("affects_team_needs")),
                "source_url": event.get("source_url", ""),
                "source_account": event.get("source_account", ""),
                "_priority": score,
            }
        )
    for team, rows in by_team.items():
        rows.sort(
            key=lambda item: (
                int(item.get("_priority") or 0),
                str(item.get("event_date") or ""),
                str(item.get("label") or ""),
            ),
            reverse=True,
        )
        by_team[team] = [
            {
                "event_date": row.get("event_date", ""),
                "status": row.get("status", ""),
                "status_kind": row.get("status_kind", ""),
                "label": row.get("label", ""),
                "affects_team_needs": bool(row.get("affects_team_needs")),
                "source_url": row.get("source_url", ""),
                "source_account": row.get("source_account", ""),
            }
            for row in rows[:6]
        ]
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


def _historical_position_family(raw_pos: str) -> str:
    pos = str(raw_pos or "").strip().upper()
    if pos == "QB":
        return "QB"
    if pos in {"RB", "WR", "TE", "FB"}:
        return "SKILL"
    if pos in {"OT", "T", "G", "C", "OG", "OT/G", "OL", "IOL"}:
        return "OL"
    if pos in {"DE", "EDGE", "DT", "NT", "DL"}:
        return "DL"
    if pos in {"LB", "ILB", "OLB", "MLB"}:
        return "LB"
    if pos in {"CB", "S", "FS", "SS", "DB"}:
        return "DB"
    return "OTHER"


def _comp_outcome_window(position: str) -> tuple[int, float]:
    family = _historical_position_family(position)
    if family == "QB":
        return (36, 22.0)
    if family == "SKILL":
        return (32, 20.0)
    if family == "OL":
        return (24, 16.0)
    if family in {"DL", "LB", "DB"}:
        return (24, 15.0)
    return (20, 14.0)


def _comp_pool_thresholds(position: str) -> tuple[float, float]:
    family = _historical_position_family(position)
    if family == "QB":
        return (0.30, 0.75)
    if family == "SKILL":
        return (0.35, 0.78)
    if family == "OL":
        return (0.35, 0.78)
    if family in {"DL", "LB", "DB"}:
        return (0.32, 0.76)
    return (0.34, 0.78)


def _pick_comp_from_pool(items: list[dict], preference: str) -> dict:
    if not items:
        return {}
    if preference == "floor":
        ranked = sorted(
            items,
            key=lambda r: (
                -(r.get("similarity") or 0.0),
                float(r.get("premium_profile_score") or 0.0),
                float(r.get("outcome_score") or 0.0),
            ),
        )
        return ranked[0]
    if preference == "ceiling":
        ranked = sorted(
            items,
            key=lambda r: (
                -(r.get("outcome_score") or 0.0),
                -(r.get("premium_profile_score") or 0.0),
                -(r.get("similarity") or 0.0),
            ),
        )
        return ranked[0]
    ranked = sorted(
        items,
        key=lambda r: (
            -(r.get("similarity") or 0.0),
            -(r.get("outcome_score") or 0.0),
            -(r.get("premium_profile_score") or 0.0),
        ),
    )
    return ranked[0]


def _comp_has_outcome_evidence(item: dict) -> bool:
    if not item:
        return False
    if _safe_float(item.get("outcome_evidence")):
        return True
    for key in (
        "DrAV",
        "wAV",
        "CarAV",
        "ValuePerYear",
        "St",
        "starter_seasons",
        "starter_seasons_proxy",
        "PB",
        "AP1",
        "G",
        "games",
        "second_contract",
        "second_contract_proxy",
        "success_label",
        "success_label_3yr",
        "ceiling_label",
    ):
        raw = item.get(key)
        if raw in ("", None):
            continue
        val = _safe_float(raw)
        if val is not None and val > 0:
            return True
    return False


def _select_comp_triplet(position: str, comp_items: list[dict]) -> tuple[dict, dict, dict]:
    if not comp_items:
        return {}, {}, {}

    top_n, sim_delta = _comp_outcome_window(position)
    candidate_band = comp_items[: max(top_n * 3, 60)]
    if not candidate_band:
        return {}, {}, {}

    best_sim = float(candidate_band[0].get("similarity") or 0.0)
    min_similarity = max(65.0, best_sim - sim_delta)
    eligible = [
        item for item in candidate_band
        if item.get("outcome_score") is not None and float(item.get("similarity") or 0.0) >= min_similarity
    ]
    if len(eligible) < 3:
        eligible = [item for item in candidate_band if item.get("outcome_score") is not None]
    if len(eligible) < 3:
        eligible = candidate_band

    with_outcome_evidence = [item for item in eligible if _comp_has_outcome_evidence(item)]
    if len(with_outcome_evidence) >= 3:
        eligible = with_outcome_evidence

    if len(eligible) == 1:
        return eligible[0], {}, eligible[0]
    if len(eligible) == 2:
        floor, ceiling = sorted(
            eligible,
            key=lambda r: (float(r.get("outcome_score") or 0.0), float(r.get("premium_profile_score") or 0.0)),
        )
        return floor, ceiling, ceiling

    ranked_by_outcome = sorted(
        eligible,
        key=lambda r: (
            float(r.get("outcome_score") or 0.0),
            float(r.get("premium_profile_score") or 0.0),
            -(r.get("similarity") or 0.0),
        ),
    )
    floor_cut, ceiling_cut = _comp_pool_thresholds(position)
    floor_idx = max(1, int(len(ranked_by_outcome) * floor_cut))
    ceiling_idx = min(len(ranked_by_outcome) - 1, max(floor_idx + 1, int(len(ranked_by_outcome) * ceiling_cut)))

    floor_pool = ranked_by_outcome[:floor_idx] or ranked_by_outcome[:1]
    median_pool = ranked_by_outcome[floor_idx:ceiling_idx] or ranked_by_outcome[floor_idx - 1:ceiling_idx]
    ceiling_pool = ranked_by_outcome[ceiling_idx:] or ranked_by_outcome[-1:]

    floor = _pick_comp_from_pool(floor_pool, "floor")
    median = _pick_comp_from_pool(median_pool, "median")
    ceiling = _pick_comp_from_pool(ceiling_pool, "ceiling")

    used = set()
    out = []
    for comp in (floor, median, ceiling):
        name = str(comp.get("name", "")).strip()
        if name and name not in used:
            out.append(comp)
            used.add(name)
            continue
        replacement = next((item for item in ranked_by_outcome if str(item.get("name", "")).strip() and str(item.get("name", "")).strip() not in used), {})
        out.append(replacement)
        used.add(str(replacement.get("name", "")).strip())

    while len(out) < 3:
        out.append({})
    return out[0], out[1], out[2]


def _position_aware_outcome_score(position_family: str, row: dict) -> float:
    raw_position = str(row.get("position") or row.get("Pos") or "").strip().upper()
    drav = _safe_float(row.get("DrAV") or row.get("career_value"))
    wav = _safe_float(row.get("wAV") or row.get("CarAV"))
    value_per_year = _safe_float(row.get("ValuePerYear"))
    starter_seasons = _safe_float(row.get("starter_seasons_proxy") or row.get("starter_seasons") or row.get("St"))
    pro_bowls = _safe_float(row.get("PB"))
    all_pros = _safe_float(row.get("AP1"))
    games = _safe_float(row.get("G"))
    starts = _safe_float(row.get("starts") or row.get("Starts"))
    pass_cmp = _safe_float(row.get("PassCmp"))
    pass_att = _safe_float(row.get("PassAtt"))
    pass_yds = _safe_float(row.get("PassYds"))
    pass_td = _safe_float(row.get("PassTD"))
    pass_int = _safe_float(row.get("PassInt"))
    rush_att = _safe_float(row.get("RushAtt"))
    rush_yds = _safe_float(row.get("RushYds"))
    rush_td = _safe_float(row.get("RushTD"))
    receptions = _safe_float(row.get("Rec"))
    rec_yds = _safe_float(row.get("RecYds"))
    rec_td = _safe_float(row.get("RecTD"))
    solo_tkl = _safe_float(row.get("SoloTkl"))
    interceptions = _safe_float(row.get("Int"))
    sacks = _safe_float(row.get("Sk"))
    second_contract = _safe_float(row.get("second_contract_proxy") or row.get("second_contract"))
    success_label = _safe_float(row.get("success_label_3yr") or row.get("success_label"))
    ceiling_label = _safe_float(row.get("ceiling_label"))
    career_snaps = _safe_float(row.get("career_snaps"))
    peak_snaps = _safe_float(row.get("peak_snaps"))
    modern_efficiency = _safe_float(row.get("modern_efficiency_score"))

    score = 0.0
    if position_family == "QB":
        if starter_seasons is not None:
            score += min(float(starter_seasons), 10.0) / 10.0 * 20.0
        if second_contract is not None:
            score += _clamp(float(second_contract), 0.0, 1.0) * 14.0
        if value_per_year is not None:
            score += min(float(value_per_year), 12.0) / 12.0 * 14.0
        if drav is not None:
            score += min(float(drav), 70.0) / 70.0 * 10.0
        elif wav is not None:
            score += min(float(wav), 90.0) / 90.0 * 10.0
        if pass_yds is not None:
            score += min(float(pass_yds), 45000.0) / 45000.0 * 8.0
        if pass_td is not None:
            score += min(float(pass_td), 320.0) / 320.0 * 8.0
        if pass_cmp is not None and pass_att is not None and pass_att > 0:
            completion_rate = float(pass_cmp) / float(pass_att)
            score += _clamp((completion_rate - 0.5) / 0.22, 0.0, 1.0) * 6.0
        if pass_int is not None:
            score -= min(float(pass_int), 120.0) / 120.0 * 5.0
        if rush_yds is not None:
            score += min(float(rush_yds), 4500.0) / 4500.0 * 4.0
        if rush_td is not None:
            score += min(float(rush_td), 45.0) / 45.0 * 4.0
        if pro_bowls is not None:
            score += min(float(pro_bowls), 8.0) / 8.0 * 10.0
        if all_pros is not None:
            score += min(float(all_pros), 4.0) / 4.0 * 12.0
        if success_label is not None:
            score += _clamp(float(success_label), 0.0, 1.0) * 8.0
        if ceiling_label is not None:
            score += _clamp(float(ceiling_label), 0.0, 1.0) * 8.0
        if games is not None:
            score += min(float(games), 180.0) / 180.0 * 4.0
        if career_snaps is not None:
            score += min(float(career_snaps), 8500.0) / 8500.0 * 4.0
        if peak_snaps is not None:
            score += min(float(peak_snaps), 1150.0) / 1150.0 * 2.0
        if modern_efficiency is not None:
            score += _clamp(float(modern_efficiency), 0.0, 1.0) * 6.0
        return score

    if position_family == "SKILL":
        if second_contract is not None:
            score += _clamp(float(second_contract), 0.0, 1.0) * 16.0
        if value_per_year is not None:
            score += min(float(value_per_year), 12.0) / 12.0 * 22.0
        if drav is not None:
            score += min(float(drav), 70.0) / 70.0 * 18.0
        elif wav is not None:
            score += min(float(wav), 90.0) / 90.0 * 16.0
        if games is not None:
            score += min(float(games), 180.0) / 180.0 * 10.0
        if starter_seasons is not None:
            score += min(float(starter_seasons), 8.0) / 8.0 * 10.0
        if pro_bowls is not None:
            score += min(float(pro_bowls), 8.0) / 8.0 * 10.0
        if all_pros is not None:
            score += min(float(all_pros), 4.0) / 4.0 * 10.0
        if success_label is not None:
            score += _clamp(float(success_label), 0.0, 1.0) * 8.0
        if ceiling_label is not None:
            score += _clamp(float(ceiling_label), 0.0, 1.0) * 6.0
        if raw_position == "RB":
            if rush_yds is not None:
                score += min(float(rush_yds), 9500.0) / 9500.0 * 10.0
            if rush_td is not None:
                score += min(float(rush_td), 90.0) / 90.0 * 8.0
            if receptions is not None:
                score += min(float(receptions), 450.0) / 450.0 * 5.0
            if rec_yds is not None:
                score += min(float(rec_yds), 3500.0) / 3500.0 * 4.0
        else:
            if receptions is not None:
                score += min(float(receptions), 900.0) / 900.0 * 7.0
            if rec_yds is not None:
                score += min(float(rec_yds), 13000.0) / 13000.0 * 10.0
            if rec_td is not None:
                score += min(float(rec_td), 110.0) / 110.0 * 7.0
            if raw_position == "TE" and games is not None:
                score += min(float(games), 220.0) / 220.0 * 3.0
        if career_snaps is not None:
            score += min(float(career_snaps), 9000.0) / 9000.0 * 3.0
        if peak_snaps is not None:
            score += min(float(peak_snaps), 900.0) / 900.0 * 2.0
        if modern_efficiency is not None:
            score += _clamp(float(modern_efficiency), 0.0, 1.0) * 5.0
        return score

    if position_family == "OL":
        if starter_seasons is not None:
            score += min(float(starter_seasons), 10.0) / 10.0 * 22.0
        if starts is not None:
            score += min(float(starts), 120.0) / 120.0 * 16.0
        if second_contract is not None:
            score += _clamp(float(second_contract), 0.0, 1.0) * 16.0
        if drav is not None:
            score += min(float(drav), 70.0) / 70.0 * 12.0
        elif wav is not None:
            score += min(float(wav), 90.0) / 90.0 * 10.0
        if pro_bowls is not None:
            score += min(float(pro_bowls), 8.0) / 8.0 * 10.0
        if all_pros is not None:
            score += min(float(all_pros), 4.0) / 4.0 * 10.0
        if success_label is not None:
            score += _clamp(float(success_label), 0.0, 1.0) * 8.0
        if ceiling_label is not None:
            score += _clamp(float(ceiling_label), 0.0, 1.0) * 6.0
        if career_snaps is not None:
            score += min(float(career_snaps), 8500.0) / 8500.0 * 8.0
        if peak_snaps is not None:
            score += min(float(peak_snaps), 1200.0) / 1200.0 * 4.0
        return score

    if position_family in {"DL", "LB", "DB"}:
        if drav is not None:
            score += min(float(drav), 70.0) / 70.0 * 24.0
        elif wav is not None:
            score += min(float(wav), 90.0) / 90.0 * 18.0
        if starter_seasons is not None:
            score += min(float(starter_seasons), 10.0) / 10.0 * 18.0
        if second_contract is not None:
            score += _clamp(float(second_contract), 0.0, 1.0) * 14.0
        if pro_bowls is not None:
            score += min(float(pro_bowls), 8.0) / 8.0 * 12.0
        if all_pros is not None:
            score += min(float(all_pros), 4.0) / 4.0 * 12.0
        if games is not None:
            score += min(float(games), 180.0) / 180.0 * 8.0
        if value_per_year is not None:
            score += min(float(value_per_year), 12.0) / 12.0 * 6.0
        if success_label is not None:
            score += _clamp(float(success_label), 0.0, 1.0) * 10.0
        if ceiling_label is not None:
            score += _clamp(float(ceiling_label), 0.0, 1.0) * 10.0
        if raw_position in {"EDGE", "DE", "DT", "DL", "NT"}:
            if sacks is not None:
                score += min(float(sacks), 100.0) / 100.0 * 10.0
            if solo_tkl is not None:
                score += min(float(solo_tkl), 450.0) / 450.0 * 3.0
        elif raw_position in {"LB", "ILB", "OLB", "MLB"}:
            if solo_tkl is not None:
                score += min(float(solo_tkl), 900.0) / 900.0 * 8.0
            if sacks is not None:
                score += min(float(sacks), 40.0) / 40.0 * 5.0
            if interceptions is not None:
                score += min(float(interceptions), 15.0) / 15.0 * 4.0
        else:
            if interceptions is not None:
                score += min(float(interceptions), 30.0) / 30.0 * 8.0
            if solo_tkl is not None:
                score += min(float(solo_tkl), 700.0) / 700.0 * 4.0
        if career_snaps is not None:
            score += min(float(career_snaps), 9000.0) / 9000.0 * 4.0
        if peak_snaps is not None:
            score += min(float(peak_snaps), 1200.0) / 1200.0 * 2.0
        if modern_efficiency is not None:
            score += _clamp(float(modern_efficiency), 0.0, 1.0) * 5.0
        return score

    if drav is not None:
        score += min(float(drav), 70.0) / 70.0 * 48.0
    elif wav is not None:
        score += min(float(wav), 90.0) / 90.0 * 40.0
    if value_per_year is not None:
        score += min(float(value_per_year), 12.0) / 12.0 * 18.0
    if starter_seasons is not None:
        score += min(float(starter_seasons), 8.0) / 8.0 * 12.0
    if pro_bowls is not None:
        score += min(float(pro_bowls), 8.0) / 8.0 * 12.0
    if all_pros is not None:
        score += min(float(all_pros), 4.0) / 4.0 * 10.0
    if games is not None:
        score += min(float(games), 120.0) / 120.0 * 6.0
    return score


def _load_historical_comp_outcomes() -> dict[tuple[str, int], dict]:
    sources = [
        HISTORICAL_DRAFT_COMPILATION,
        HISTORICAL_DRAFT_REFINED,
        HISTORICAL_DRAFT_2014_2018,
        HISTORICAL_DRAFT_2023,
        HISTORICAL_LABELS_LEAGIFY,
    ]
    outcomes: dict[tuple[str, int], dict] = {}
    premium_2024_scores = _load_premium_2024_comp_scores()
    for path in sources:
        if not path.exists():
            continue
        with path.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = str(row.get("Player") or row.get("player_name") or "").strip()
                year = _safe_int(row.get("DraftYear") or row.get("draft_year") or row.get("Year"), 0)
                if not name or year <= 0 or year >= CURRENT_DRAFT_YEAR:
                    continue
                key = (_norm_comp_identity_key(name), year)
                existing = outcomes.get(key, {"name": name, "year": year})
                if len(name.split()) > len(str(existing.get("name", "")).split()):
                    existing["name"] = name
                raw_position = row.get("Pos") or row.get("position") or existing.get("position") or ""
                existing["position"] = raw_position
                existing["position_family"] = _historical_position_family(raw_position)

                merge_fields = {
                    "DrAV": row.get("DrAV") or row.get("drav") or row.get("career_value"),
                    "wAV": row.get("wAV") or row.get("wav") or row.get("CarAV"),
                    "ValuePerYear": row.get("ValuePerYear") or row.get("value_per_year"),
                    "St": row.get("St") or row.get("starter_seasons") or row.get("starter_seasons_proxy"),
                    "PB": row.get("PB") or row.get("pb"),
                    "AP1": row.get("AP1") or row.get("ap1"),
                    "G": row.get("G") or row.get("games"),
                    "starts": row.get("starts"),
                    "PassCmp": row.get("PassCmp"),
                    "PassAtt": row.get("PassAtt"),
                    "PassYds": row.get("PassYds"),
                    "PassTD": row.get("PassTD"),
                    "PassInt": row.get("PassInt"),
                    "RushAtt": row.get("RushAtt"),
                    "RushYds": row.get("RushYds"),
                    "RushTD": row.get("RushTD"),
                    "Rec": row.get("Rec"),
                    "RecYds": row.get("RecYds"),
                    "RecTD": row.get("RecTD"),
                    "SoloTkl": row.get("SoloTkl"),
                    "Int": row.get("Int"),
                    "Sk": row.get("Sk"),
                    "second_contract_proxy": row.get("second_contract_proxy") or row.get("second_contract"),
                    "success_label_3yr": row.get("success_label_3yr") or row.get("success_label"),
                    "ceiling_label": row.get("ceiling_label"),
                }
                for field, raw in merge_fields.items():
                    if raw not in ("", None):
                        existing[field] = raw
                if _comp_has_outcome_evidence(existing):
                    existing["outcome_evidence"] = 1
                outcomes[key] = existing

    for key, payload in list(outcomes.items()):
        position_family = str(payload.get("position_family") or "")
        outcome_score = _position_aware_outcome_score(position_family, payload)
        payload["drav"] = _safe_float(payload.get("DrAV") or payload.get("career_value"))
        payload["wav"] = _safe_float(payload.get("wAV") or payload.get("CarAV"))
        payload["value_per_year"] = _safe_float(payload.get("ValuePerYear"))
        payload["starter_seasons"] = _safe_float(payload.get("starter_seasons_proxy") or payload.get("starter_seasons") or payload.get("St"))
        payload["pro_bowls"] = _safe_float(payload.get("PB"))
        payload["all_pros"] = _safe_float(payload.get("AP1"))
        payload["games"] = _safe_float(payload.get("G"))
        payload["second_contract"] = _safe_float(payload.get("second_contract_proxy") or payload.get("second_contract"))
        payload["success_label"] = _safe_float(payload.get("success_label_3yr") or payload.get("success_label"))
        payload["ceiling_label"] = _safe_float(payload.get("ceiling_label"))
        payload["premium_profile_score"] = premium_2024_scores.get(key, 0.0)
        payload.setdefault("career_snaps", None)
        payload.setdefault("peak_snaps", None)
        payload.setdefault("modern_efficiency_score", None)
        payload["outcome_evidence"] = 1 if _comp_has_outcome_evidence(payload) else 0
        payload["outcome_score"] = round(outcome_score, 3)
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


def _load_owner_scouting_notes() -> dict[str, dict[str, str]]:
    rows = _read_csv(OWNER_SCOUTING_NOTES_CSV)
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        slug = str(row.get("slug", "")).strip().lower()
        if not slug:
            slug = _slugify_player(row.get("player_name", ""))
        if not slug:
            continue
        out[slug] = {
            "report_summary": str(row.get("public_report_summary", "")).strip(),
            "why_he_wins": str(row.get("public_why_he_wins", "")).strip(),
            "primary_concerns": str(row.get("public_primary_concerns", "")).strip(),
            "film_notes": str(row.get("public_film_notes", "")).strip(),
            "role_projection": str(row.get("public_role_projection", "")).strip(),
            "seo_description": str(row.get("seo_description", "")).strip(),
        }
    return out


def _parse_manual_counting_stats(raw: str) -> list[dict]:
    chips: list[dict] = []
    for part in str(raw or "").split(";"):
        item = str(part).strip()
        if not item or ":" not in item:
            continue
        label, value = item.split(":", 1)
        label = str(label).strip()
        value = str(value).strip()
        if not label or not value:
            continue
        chips.append(
            {
                "key": _slugify_player(label).replace("-", "_"),
                "label": label,
                "raw": value,
                "display": value,
                "fmt": "manual",
            }
        )
    return chips


def _split_bullet_lines(raw: str) -> list[str]:
    items: list[str] = []
    for line in str(raw or "").replace("\\n", "\n").splitlines():
        text = str(line).strip()
        if not text:
            continue
        text = re.sub(r"^\s*(?:[-*•]|\d+\.)\s*", "", text).strip()
        if not text:
            continue
        items.append(text)
    return items


def _sanitize_primary_concern_text(raw: str) -> list[str]:
    cleaned: list[str] = []
    for item in _split_bullet_lines(raw):
        lower = item.lower()
        if lower in {"none", "n/a", "na", "tbd"}:
            continue
        if re.search(r"scouting concern to verify:\s*[>.\-]*\s*$", item, flags=re.IGNORECASE):
            continue
        if re.search(r"scouting concern to verify:\s*$", item, flags=re.IGNORECASE):
            continue
        cleaned.append(item)
    return cleaned


def _metric_card_lookup(cards: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for card in cards or []:
        key = str(card.get("key", "")).strip()
        if key:
            out[key] = card
    return out


def _fallback_primary_concerns(
    row: dict,
    position: str,
    advanced_metric_cards: list[dict],
    production_composite_pct: float | None,
    athletic_percentile: float | None,
    trait_percentile: float | None,
    comp_confidence: str,
) -> str:
    pos = str(position or "").upper()
    card_map = _metric_card_lookup(advanced_metric_cards)
    bullets: list[str] = []

    def add(text: str) -> None:
        if text and text not in bullets and len(bullets) < 4:
            bullets.append(text)

    def pct_for(key: str) -> float | None:
        raw = _safe_float(card_map.get(key, {}).get("pct"))
        return float(raw) if raw is not None else None

    if production_composite_pct is not None and production_composite_pct < 35:
        add("Current production profile is light versus this draft class at the same position, which narrows the margin for projection error.")
    if athletic_percentile is not None and athletic_percentile < 35:
        add("Athletic profile sits below preferred starter bands for this position, so the transition window is less forgiving against NFL speed and size.")
    if trait_percentile is not None and trait_percentile < 35:
        add("Trait profile is thinner than the top draftable range at this position, which raises the burden on role fit and technical development.")
    if str(comp_confidence or "").strip().upper() == "C":
        add("Historical translation comps are less stable here, so the outcome range is wider than it is for cleaner projection profiles.")

    if pos == "QB":
        if (pct_for("sg_qb_twp_rate") or 100) < 35:
            add("Turnover-worthy decision making still needs to tighten when the pocket muddies and coverage rotates late.")
        if (pct_for("sg_qb_pressure_to_sack_rate") or 100) < 35:
            add("Pressure response currently creates too many dead-end downs; sack avoidance and pocket management remain a key separator.")
        if (pct_for("sg_qb_blitz_grade") or 100) < 35:
            add("Blitz answers do not yet look fully bankable, which can slow early-down trust against NFL pressure packages.")
    elif pos == "RB":
        if (pct_for("sg_rb_explosive_rate") or 100) < 35:
            add("Explosive-run creation is below top-tier back standards, so the profile leans more on efficiency than true chunk-play stress.")
        if (pct_for("sg_rb_targets_per_route") or 100) < 35:
            add("Passing-game involvement is lighter than true three-down back profiles, which can cap early-down-only value if protection also lags.")
        if (pct_for("sg_rb_elusive_rating") or 100) < 35:
            add("Independent creation after contact is less consistent than it needs to be for a back expected to survive NFL traffic density.")
    elif pos in {"WR", "TE"}:
        if (pct_for("sg_wrte_targets_per_route") or 100) < 35:
            add("Target earning is light for the role projection, so NFL volume depends on winning cleaner and earlier against coverage leverage.")
        if (pct_for("sg_wrte_yprr") or 100) < 35:
            add("Per-route production is below premium translation bands, which raises the risk of empty usage without true target command.")
        if (pct_for("sg_wrte_drop_rate") or 100) < 35:
            add("Ball-finish consistency needs to be cleaner, especially on timing throws and contested windows where trust is earned quickly.")
    elif pos in {"EDGE", "DT"}:
        if (pct_for("sg_dl_true_pass_set_win_rate") or 100) < 35:
            add("True pass-set disruption is still thin for a high-end projection, so the rush plan has to win more often on pure NFL dropback downs.")
        if (pct_for("sg_front_run_def_grade") or 100) < 35:
            add("Early-down run defense remains a pressure point, which can narrow usage into more obvious passing situations.")
        if (pct_for("sg_dl_total_pressures") or 100) < 35:
            add("Sustained pressure volume is lighter than top-line draft expectations, so the current profile leans more on flashes than down-to-down finish.")
    elif pos == "LB":
        if (pct_for("sg_def_coverage_grade") or 100) < 35:
            add("Coverage reliability is still the swing trait, especially if offenses force him into space or isolate him on crossers and option routes.")
        if (pct_for("sg_def_missed_tackle_rate") or 100) < 35:
            add("Tackle finish and angle discipline remain important cleanup points before the profile is trustworthy as a full-time linebacker.")
        if (pct_for("sg_def_run_grade") or 100) < 35:
            add("Run-fit consistency is not yet stable enough to assume immediate every-down linebacker value against NFL size and pace.")
    elif pos in {"CB", "S"}:
        if (pct_for("sg_cov_yards_per_snap") or 100) < 35:
            add("Coverage efficiency allowed is still too loose for a clean projection, so leverage control and finish at the catch point matter more here.")
        if (pct_for("sg_cov_snaps_per_target") or 100) < 35:
            add("Target deterrence is lighter than premium coverage profiles, which suggests quarterbacks were still comfortable testing him.")
        if (pct_for("sg_cov_qb_rating_against") or 100) < 35:
            add("Passing efficiency against this coverage profile needs to come down before the transition path looks truly bankable.")
    elif pos in {"OT", "IOL"}:
        if (pct_for("sg_ol_pass_block_grade") or 100) < 35:
            add("Pass-protection consistency needs work before the profile can be trusted against NFL counter sequencing and interior/edge power.")
        if (pct_for("sg_ol_pressure_allowed_rate") or 100) < 35:
            add("Pressure prevention is below ideal starter bands, so hand timing and recovery mechanics still look like the swing variable.")
        if (pct_for("sg_ol_run_block_grade") or 100) < 35:
            add("Run-game movement and leverage do not yet project as cleanly as the pass-game role, which can narrow early deployment paths.")

    if not bullets:
        add("No single fatal flaw stands out in the current data, but the translation path still depends on how this profile handles tighter NFL speed, strength, and processing windows.")
    return "\n".join(f"- {item}" for item in bullets[:4])


def _load_production_snapshot_overrides() -> dict[str, dict[str, object]]:
    rows = _read_csv(PRODUCTION_SNAPSHOT_OVERRIDES_CSV)
    out: dict[str, dict[str, object]] = {}
    for row in rows:
        slug = str(row.get("slug", "")).strip().lower()
        if not slug:
            slug = _slugify_player(row.get("player_name", ""))
        if not slug:
            continue
        out[slug] = {
            "heading": str(row.get("production_snapshot_heading", "")).strip(),
            "text": str(row.get("production_snapshot_text", "")).strip(),
            "counting_stat_chips": _parse_manual_counting_stats(row.get("production_counting_stats", "")),
        }
    return out


def _load_premium_2024_comp_scores() -> dict[tuple[str, int], float]:
    field_weights = {
        "grades_pass": 2.0,
        "btt_rate": 1.0,
        "twp_rate": 1.0,
        "pressure_to_sack_rate": 1.0,
        "grades_run": 1.5,
        "elusive_rating": 1.0,
        "yco_attempt": 1.0,
        "explosive": 1.0,
        "yprr": 1.3,
        "grades_pass_route": 1.7,
        "man_yprr": 0.8,
        "zone_yprr": 0.8,
        "grades_pass_block": 1.8,
        "grades_run_block": 1.2,
        "pbe": 1.5,
        "grades_pass_rush_defense": 1.8,
        "pass_rush_win_rate": 1.4,
        "prp": 1.3,
        "true_pass_set_pass_rush_win_rate": 1.4,
        "true_pass_set_prp": 1.3,
        "grades_run_defense": 1.3,
        "stop_percent": 1.0,
        "grades_coverage_defense": 1.8,
        "forced_incompletion_rate": 1.2,
        "coverage_snaps_per_target": 1.0,
        "yards_per_coverage_snap": 1.1,
        "qb_rating_against": 1.1,
        "man_grades_coverage_defense": 0.8,
        "zone_grades_coverage_defense": 0.8,
        "coverage_snaps": 0.5,
    }
    scores_by_name: dict[str, float] = defaultdict(float)
    for path in PREMIUM_COMP_2024_SOURCES:
        if not path.exists():
            continue
        for row in _read_csv(path):
            player_name = str(row.get("player") or row.get("Player") or "").strip()
            if not player_name:
                continue
            player_key = _norm_comp_identity_key(player_name)
            if not player_key:
                continue
            row_score = 0.0
            for field, weight in field_weights.items():
                raw = _safe_float(row.get(field))
                if raw is None:
                    continue
                row_score += weight
            if row_score > 0:
                scores_by_name[player_key] += row_score
    return {(name, 2025): round(score, 3) for name, score in scores_by_name.items()}


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
    comp_outcomes_by_name: dict[str, list[dict]] = defaultdict(list)
    for payload in comp_outcomes.values():
        identity_key = _norm_comp_identity_key(payload.get("name", ""))
        if identity_key:
            comp_outcomes_by_name[identity_key].append(payload)
    for items in comp_outcomes_by_name.values():
        items.sort(
            key=lambda item: (
                float(item.get("outcome_score") or 0.0),
                float(item.get("premium_profile_score") or 0.0),
                float(item.get("games") or 0.0),
            ),
            reverse=True,
        )
    owner_notes = _load_owner_scouting_notes()
    production_snapshot_overrides = _load_production_snapshot_overrides()
    consensus_mean_population = [
        float(_safe_float(row.get("consensus_board_mean_rank")) or 0.0)
        for row in rows
        if (_safe_float(row.get("consensus_board_mean_rank")) or 0.0) > 0
    ]

    # Position-normalized metric populations for percentile context.
    pos_metric_values: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    pos_athletic_profile_values: dict[str, list[float]] = defaultdict(list)
    pos_trait_values: dict[str, list[float]] = defaultdict(list)
    lb_trait_bucket_values: dict[str, list[float]] = defaultdict(list)
    pos_market_values: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        pos = str(row.get("position", "")).strip().upper()
        if not pos:
            continue
        athletic_profile_score = _safe_float(row.get("athletic_profile_score"))
        if athletic_profile_score is not None and athletic_profile_score > 0:
            pos_athletic_profile_values[pos].append(float(athletic_profile_score))
        trait_score = _safe_float(row.get("trait_score"))
        if trait_score is not None and trait_score > 0:
            pos_trait_values[pos].append(float(trait_score))
            if pos == "LB":
                lb_bucket = str(row.get("lb_archetype", "")).strip()
                if lb_bucket:
                    lb_trait_bucket_values[lb_bucket].append(float(trait_score))
        consensus_rank_value = _safe_float(row.get("consensus_board_mean_rank"))
        if consensus_rank_value is None or consensus_rank_value <= 0:
            consensus_rank_value = float(_safe_int(row.get("consensus_rank"), 0))
        if consensus_rank_value and consensus_rank_value > 0:
            pos_market_values[pos].append(float(consensus_rank_value))
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
        owner_note = owner_notes.get(slug, {})
        production_override = production_snapshot_overrides.get(slug, {})
        rank_driver_summary = row.get("rank_driver_summary", "")
        top_driver_key, top_driver_delta = _top_driver(rank_driver_summary)
        pff_grade = round(_safe_float(row.get("pff_grade")) or 0.0, 2)
        athletic_profile_score = _safe_float(row.get("athletic_profile_score"))
        athletic_metric_coverage_rate = _safe_float(row.get("athletic_metric_coverage_rate"))
        athletic_speed_score = _safe_float(row.get("athletic_speed_score"))
        athletic_explosion_score = _safe_float(row.get("athletic_explosion_score"))
        athletic_agility_score = _safe_float(row.get("athletic_agility_score"))
        athletic_size_adj_score = _safe_float(row.get("athletic_size_adj_score"))
        formula_size_component = _safe_float(row.get("formula_size_component"))
        formula_athletic_source_confidence = _safe_float(row.get("formula_athletic_source_confidence"))
        formula_athletic_source = str(row.get("formula_athletic_source", "")).strip()
        combine_ras_official = round(_safe_float(row.get("combine_ras_official")) or 0.0, 2)
        ras_estimate = round(_safe_float(row.get("ras_estimate")) or 0.0, 2)
        production_snapshot = _clean_public_snapshot(row.get("scouting_production_snapshot", "") or "")
        if production_override.get("text"):
            production_snapshot = str(production_override.get("text", "")).strip()
        advanced_source_season = _safe_int(row.get("sg_source_season"), 0) or _safe_int(row.get("sg_cov_source_season"), 0)
        if advanced_source_season and advanced_source_season < 2025 and not production_override.get("text"):
            note = f"{advanced_source_season} premium data used because equivalent 2025 premium rows are unavailable."
            production_snapshot = f"{note} {production_snapshot}".strip()
        low_evidence_flag = pff_grade <= 0 and combine_ras_official <= 0 and ras_estimate <= 0
        athletic_percentile = _pct_rank(
            float(athletic_profile_score),
            pos_athletic_profile_values.get(pos, []),
        ) if athletic_profile_score is not None and athletic_profile_score > 0 else None
        trait_score = _safe_float(row.get("trait_score"))
        if trait_score is not None and trait_score > 0:
            if pos == "LB":
                lb_bucket = str(row.get("lb_archetype", "")).strip()
                lb_values = lb_trait_bucket_values.get(lb_bucket, [])
                trait_percentile = _pct_rank(float(trait_score), lb_values if len(lb_values) >= 5 else pos_trait_values.get(pos, []))
            else:
                trait_percentile = _pct_rank(float(trait_score), pos_trait_values.get(pos, []))
        else:
            trait_percentile = None
        production_source_tier = "missing"
        advanced_metric_cards, advanced_metrics, advanced_percentiles = _build_metric_cards(
            row=row,
            position=pos,
            config_map=POSITION_ADVANCED_METRIC_CONFIG,
            pos_metric_values=pos_metric_values,
        )
        if advanced_metric_cards:
            production_source_tier = "premium"
        if not advanced_metric_cards:
            advanced_metric_cards, advanced_metrics, advanced_percentiles = _build_metric_cards(
                row=row,
                position=pos,
                config_map=POSITION_FALLBACK_METRIC_CONFIG,
                pos_metric_values=pos_metric_values,
            )
            if advanced_metric_cards:
                production_source_tier = "fallback"
        counting_stat_chips, counting_metrics = _build_counting_stat_chips(
            row=row,
            position=pos,
            config_map=POSITION_COUNTING_STAT_CONFIG,
        )
        if production_override.get("counting_stat_chips"):
            counting_stat_chips = list(production_override.get("counting_stat_chips", []))
        production_metrics: dict[str, float] = {**advanced_metrics, **counting_metrics}
        production_percentiles: dict[str, float] = dict(advanced_percentiles)
        production_composite_pct = _weighted_percentile_composite(advanced_metric_cards)
        if production_composite_pct is None:
            production_composite_pct = _safe_float(row.get("cfb_prod_percentile_signal"))
            if production_composite_pct is not None:
                production_composite_pct = round(float(production_composite_pct), 1)
        testing_parts = [
            float(v)
            for v in (athletic_speed_score, athletic_explosion_score, athletic_agility_score)
            if v is not None and v > 0
        ]
        athletic_testing_score = round(sum(testing_parts) / len(testing_parts), 1) if testing_parts else None
        athletic_frame_score = athletic_size_adj_score
        if (athletic_frame_score is None or athletic_frame_score <= 0) and formula_size_component is not None and formula_size_component > 0:
            athletic_frame_score = formula_size_component
        if athletic_frame_score is not None and athletic_frame_score > 0:
            athletic_frame_score = round(float(athletic_frame_score), 1)
        athletic_source_confidence = None
        if formula_athletic_source_confidence is not None:
            athletic_source_confidence = round(max(0.0, min(1.0, float(formula_athletic_source_confidence))) * 100.0, 1)
        position_lens = _build_position_lens(
            row=row,
            position=pos,
            pos_metric_values=pos_metric_values,
        )
        trait_bucket_cards, trait_bucket_score, trait_bucket_family = _build_trait_bucket_cards(row)

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

        def _resolve_comp_year(comp_name: str) -> int | None:
            identity_key = _norm_comp_identity_key(comp_name)
            if not identity_key:
                return None
            candidates = comp_outcomes_by_name.get(identity_key, [])
            if not candidates:
                return None
            position_family = _historical_position_family(pos)
            family_matches = [
                cand for cand in candidates
                if _historical_position_family(cand.get("position")) == position_family
            ]
            pick_pool = family_matches or candidates
            picked = pick_pool[0] if pick_pool else None
            year = _safe_int((picked or {}).get("year"), 0)
            return year if year > 0 else None

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
        ath_seed_sims: list[float] = []
        for idx in (1, 2, 3):
            for source_prefix in ("historical_combine_comp", "athletic_nn_comp"):
                sim = _norm_similarity_pct(row.get(f"{source_prefix}_{idx}_similarity", ""))
                if sim is not None:
                    ath_seed_sims.append(sim)
        inferred_ras_similarity = None
        if ath_seed_sims:
            inferred_ras_similarity = round(max(84.0, min(95.0, (sum(ath_seed_sims) / len(ath_seed_sims)) - 5.0)), 2)
        else:
            inferred_ras_similarity = 88.0
        for idx in (1, 2, 3):
            ras_name = str(row.get(f"ras_historical_comp_{idx}", "")).strip()
            if not ras_name:
                continue
            _ingest_comp(
                ras_name,
                _resolve_comp_year(ras_name),
                inferred_ras_similarity,
                source="ras_historical",
                is_production=False,
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
                    "premium_profile_score": outcome.get("premium_profile_score"),
                    "outcome_evidence": outcome.get("outcome_evidence"),
                }
            )

        comp_items = sorted(
            comp_items,
            key=lambda r: (r.get("similarity") is None, -(r.get("similarity") or 0.0)),
        )
        comp_floor, comp_median, comp_ceiling = _select_comp_triplet(pos, comp_items)

        consensus_mean_rank_val = _safe_float(row.get("consensus_board_mean_rank"))
        if consensus_mean_rank_val is None or consensus_mean_rank_val <= 0:
            consensus_mean_rank_val = float(_safe_int(row.get("consensus_rank"), 9999))
        market_population = pos_market_values.get(pos) or consensus_mean_population
        market_rank_pct = _pct_rank(consensus_mean_rank_val, market_population) if market_population else 50.0
        market_signal_pct = 100.0 - float(market_rank_pct)
        comp_confidence = str(row.get("comp_confidence", "")).strip()
        owner_primary_concerns = owner_note.get("primary_concerns", "")
        generated_primary_concerns = row.get("scouting_primary_concerns", "")
        public_primary_concern_bullets = (
            _sanitize_primary_concern_text(owner_primary_concerns)
            if str(owner_primary_concerns or "").strip()
            else _sanitize_primary_concern_text(generated_primary_concerns)
        )
        if public_primary_concern_bullets:
            scouting_primary_concerns = "\n".join(
                f"- {item}" for item in public_primary_concern_bullets[:4]
            )
        else:
            scouting_primary_concerns = _fallback_primary_concerns(
                row=row,
                position=pos,
                advanced_metric_cards=advanced_metric_cards,
                production_composite_pct=production_composite_pct,
                athletic_percentile=athletic_percentile,
                trait_percentile=trait_percentile,
                comp_confidence=comp_confidence,
            )

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
                "trait_percentile": trait_percentile,
                "trait_bucket_family": trait_bucket_family,
                "trait_bucket_score": trait_bucket_score,
                "trait_bucket_cards": trait_bucket_cards,
                "athletic_profile_score": round(float(athletic_profile_score), 3) if athletic_profile_score is not None and athletic_profile_score > 0 else None,
                "athletic_metric_coverage_rate": round(float(athletic_metric_coverage_rate), 4) if athletic_metric_coverage_rate is not None and athletic_metric_coverage_rate >= 0 else None,
                "athletic_percentile": athletic_percentile,
                "athletic_testing_score": athletic_testing_score,
                "athletic_frame_score": athletic_frame_score,
                "athletic_source_confidence": athletic_source_confidence,
                "athletic_source_label": formula_athletic_source,
                "combine_ras_official": combine_ras_official,
                "ras_estimate": ras_estimate,
                "confidence_score": round(_safe_float(row.get("confidence_score")) or 0.0, 2),
                "uncertainty_score": round(_safe_float(row.get("uncertainty_score")) or 0.0, 2),
                "best_role": _clean_token_label(raw_best_role),
                "best_scheme_fit": _clean_token_label(raw_best_scheme),
                "lb_archetype": str(row.get("lb_archetype", "")).strip(),
                "rank_driver_summary": rank_driver_summary,
                "top_rank_driver": top_driver_key,
                "top_rank_driver_delta": round(top_driver_delta, 2),
                "rank_history": hist,
                "rank_move_window": rank_move_window,
                "low_evidence_flag": low_evidence_flag,
                "production_metrics": production_metrics,
                "production_percentiles": production_percentiles,
                "advanced_metric_cards": advanced_metric_cards,
                "advanced_metrics": advanced_metrics,
                "advanced_percentiles": advanced_percentiles,
                "counting_stat_chips": counting_stat_chips,
                "counting_metrics": counting_metrics,
                "production_composite_pct": production_composite_pct,
                "production_source_tier": production_source_tier,
                "production_snapshot_heading": (
                    str(production_override.get("heading", "")).strip()
                    or ("2024 Production Snapshot*" if advanced_source_season and advanced_source_season < 2025 else "2025 Production Snapshot")
                ),
                "position_lens": position_lens,
                "historical_comp_floor": _public_comp_dict(comp_floor),
                "historical_comp_median": _public_comp_dict(comp_median),
                "historical_comp_ceiling": _public_comp_dict(comp_ceiling),
                "comp_confidence": comp_confidence,
                "scouting_report_summary": owner_note.get("report_summary") or row.get("scouting_report_summary", ""),
                "scouting_why_he_wins": owner_note.get("why_he_wins") or row.get("scouting_why_he_wins", ""),
                "scouting_primary_concerns": scouting_primary_concerns,
                "scouting_film_notes": owner_note.get("film_notes") or row.get("scouting_film_notes", ""),
                "scouting_production_snapshot": production_snapshot,
                "scouting_role_projection": owner_note.get("role_projection") or row.get("scouting_role_projection", ""),
                "seo_description_override": owner_note.get("seo_description") or "",
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
                "free_agents_full": ctx.get("free_agents_full", ctx.get("free_agents", [])),
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
