# CFBD Call Budget Plan (1,000/month cap)

## Rule

- Hard cap enforced in code: 1,000 calls/month.
- Every executed API request is counted before request dispatch.
- Default script mode is dry-run; calls are only spent with `--execute`.

## Suggested monthly allocation

1. Prospect stats refresh (weekly): 280 calls
2. Team advanced stats refresh (weekly): 120 calls
3. Game/team-game pulls for context modeling: 360 calls
4. Spot pulls for missing players/teams: 140 calls
5. Reserve buffer for draft week: 100 calls

Total: 1,000

## Commands

Dry run (no call spent):
```bash
python3 scripts/pull_cfbd_data.py --dataset team_advanced_stats --year 2025
```

Execute single pull (spends 1 call):
```bash
python3 scripts/pull_cfbd_data.py --dataset team_advanced_stats --year 2025 --execute
```

Check usage:
```bash
python3 scripts/cfbd_usage_status.py
```

## Storage

- Usage state file: `data/processed/api_usage/cfbd_usage_YYYY-MM.json`
- API responses: `data/sources/cfbd/*.json`
