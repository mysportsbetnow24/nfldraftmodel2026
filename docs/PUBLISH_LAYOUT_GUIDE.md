# Publish Layout Guide (Web)

## Reference products reviewed

- NFL.com Draft Tracker: `https://www.nfl.com/draft/tracker/players/all-positions/all-colleges/undrafted/2026`
- DraftKings 2026 Mock: `https://dknetwork.draftkings.com/2026/02/11/2026-nfl-mock-draft-first-round-projections/`
- PFF Big Board: `https://www.pff.com/draft/big-board`
- Tankathon Mock Draft UI: `https://www.tankathon.com/nfl/mock_draft`

## What to borrow

1. Sticky filtering and sort controls
- Position, school, conference, trait tier, risk band, team-fit only.

2. Dual-view architecture
- `Board view`: sortable ranking table.
- `Player report view`: full scouting + data cards + historical comps.

3. Confidence and uncertainty layer
- Show floor/ceiling and confidence band near final grade.

4. Tiered visual hierarchy
- Rank tier color chips (blue-chip, rd1-2, day2, day3).

5. Mobile-first interaction
- Single-column report cards with collapsible sections for notes/metrics/comps.

## Recommended publish schema

- `player_uid`
- `consensus_rank`
- `final_grade`
- `round_value`
- `position`
- `school`
- `height`, `weight`
- `core_stat_name`, `core_stat_value`
- `best_team_fit`, `best_scheme_fit`
- `historical_comp`, `comp_confidence`
- `scouting_notes`
- `headshot_url`

## Implementation status in this repo

- HTML profile generation: `scripts/generate_player_reports.py`
- Example output index: `data/outputs/reports_index.html`
- Next step: convert to React/Next.js or Astro static pipeline with the same schema.
