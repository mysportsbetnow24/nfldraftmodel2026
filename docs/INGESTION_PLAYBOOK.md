# Ingestion Playbook

## Environment

Set environment variables before live pulls:

```bash
export ODDS_API_KEY="..."
export CFBD_API_KEY="..."
```

## Source adapters

### nflverse / nflreadr
- Adapter: `src/ingest/nflverse_loader.py`
- Intended tables:
  - `pbp_nfl`
  - `player_stats_nfl`
  - `rosters_nfl`
  - `draft_history_nfl`
  - `injuries_nfl`

### cfbfastr / SportsDataverse
- Adapter: `src/ingest/cfbfastr_loader.py`
- Intended tables:
  - `pbp_cfb`
  - `player_stats_cfb`
  - `team_stats_cfb`
  - `rosters_cfb`

### CollegeFootballData.com (CFBD API)
- Adapter: `src/ingest/cfbd_loader.py`
- Pull script: `scripts/pull_cfbd_data.py`
- Usage monitor: `scripts/cfbd_usage_status.py`
- Supported datasets:
  - `player_season_stats`
  - `team_season_stats`
  - `team_advanced_stats`
  - `advanced_game_stats`
  - `player_ppa`
  - `team_ppa`
  - `games`
  - `team_game_stats`
  - `roster`
  - `fbs_teams`
- Monthly safety cap:
  - hard-guarded at 1,000 calls/month by default
  - script defaults to dry-run; use `--execute` to spend one call

### External board/PFF CSV import
- Adapter: `src/ingest/rankings_loader.py`
- File: `data/sources/manual/nfl-draft-bigboard-scout-mode-2026-02-25.csv`
- Imported fields:
  - external rank
  - position
  - school
  - PFF grade
  - PFF WAA

### Consensus/analyst boards
- Adapter: `src/ingest/rankings_loader.py`
- Feeds:
  - Daniel Jeremiah (public)
  - NFL Mock Draft Database (consensus)
  - DraftTek
  - PFF public board pages
  - Manual imports for paywalled boards

### Draft order and comp picks
- Adapter: `src/ingest/rankings_loader.py`
- Sources:
  - NFL.com order tracker
  - NFL Operations release

### Odds API (approved)
- Adapter: `src/ingest/odds_loader.py`
- Pull script: `scripts/pull_odds_data.py`
- Usage monitor: `scripts/odds_usage_status.py`
- Approved campaign budget (Feb 25, 2026):
  - 2,880 calls across 120 days
  - hard-guard enforced in code
  - script defaults to dry-run; use `--execute` to spend one call
- Markets:
  - first overall pick
  - first position drafted
  - player drafted by team

## Data quality checks

- Unique key checks (`player_name + school + position + class_year` provisional key).
- Position normalization map (EDGE/DE/OLB etc).
- Height/weight standardization.
- Duplicate source row suppression.
- Timestamp + source_url provenance columns on every load.

## Legal and paywall notes

- Do not bypass authentication/paywalls.
- Use manual template imports for paid sources.
- Keep source attribution on each ranking row.
