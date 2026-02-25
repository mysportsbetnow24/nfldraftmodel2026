# Odds API Budget Approval

## Approval status

- Approved by user on **February 25, 2026**.
- Budget: **2,880 total calls** across **120 days**.

## Campaign assumptions

- Markets: 3 (`first_overall_pick`, `first_position_drafted`, `team_to_draft_player`)
- Bookmakers: 4 (`fanduel`, `draftkings`, `betmgm`, `caesars`)
- Regions: 1 (`us`)
- Snapshots/day: 2
- Days: 120

Formula:

`total_calls = markets * bookmakers * regions * snapshots_per_day * days`

`total_calls = 3 * 4 * 1 * 2 * 120 = 2880`

## Enforcement in code

- Loader with hard quota guard: `src/ingest/odds_loader.py`
- Pull command (dry-run default): `scripts/pull_odds_data.py`
- Usage command: `scripts/odds_usage_status.py`
- Usage state file: `data/processed/api_usage/odds_usage_campaign.json`

## Commands

Dry run (0 calls):

```bash
python3 scripts/pull_odds_data.py --market first_overall_pick
```

Execute one call:

```bash
python3 scripts/pull_odds_data.py --market first_overall_pick --execute
```

Check budget pace:

```bash
python3 scripts/odds_usage_status.py
```
