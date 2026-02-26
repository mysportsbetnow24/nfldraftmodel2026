# UnderDog Team Needs Ingest (2026)

Source:
- https://underdognetwork.com/football/news/2026-nfl-team-needs

## What this adds

- Team-level ranked need labels from a public analyst article.
- Normalized position buckets that map to model positions (`QB/RB/WR/TE/OT/IOL/EDGE/DT/LB/CB/S`).
- A chart-ready team x position matrix for dashboards.
- A patch file you can use to refresh `team_profiles_2026.csv` need slots.

## Run

Dry run:

```bash
python3 scripts/pull_underdog_team_needs.py
```

Live pull:

```bash
python3 scripts/pull_underdog_team_needs.py --execute
```

Outputs:
- `data/sources/external/underdog_team_needs_raw_2026.csv`
- `data/sources/external/underdog_team_needs_normalized_2026.csv`
- `data/sources/external/underdog_team_needs_matrix_2026.csv`
- `data/sources/external/underdog_team_profiles_patch_2026.csv`

## How to use in model

1. Compare patch file to `data/sources/team_profiles_2026.csv`.
2. Replace or blend `need_1/need_2/need_3` for each team.
3. Re-run:
   - `python3 scripts/build_big_board.py`
   - `python3 scripts/run_mock_draft.py`

## Notes

- This is analyst-opinion data, not a direct performance metric.
- Best use is as a prior for near-term roster intent, not as ground truth.
- Keep weight moderate so need signals do not overpower BPA and scouting grades.
