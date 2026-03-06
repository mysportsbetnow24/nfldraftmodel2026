# Team Needs Context Build Report (NFLverse)

- Built at: `2026-03-06T04:45:12.943913+00:00`
- Output: `/Users/nickholz/nfldraftmodel2026/data/sources/team_needs_context_2026.csv`
- Target year: `2026`
- Teams: `32`
- Rows: `352`
- ESPN depth-chart overrides used: `0`

## Input Files

- rosters: `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/rosters_weekly.parquet` (46579 rows)
- contracts: `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/contracts.parquet` (50300 rows)
- players: `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/players.parquet` (24356 rows)
- participation: `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/participation.parquet` (45919 rows)
- espn depth charts: `/Users/nickholz/nfldraftmodel2026/data/sources/external/espn_depth_charts_2026.csv` (not present)

## Notes

- `depth_chart_pressure` combines roster depth/experience and deployment intensity.
- If ESPN depth charts are present, they override roster ordering for starter-quality/depth pressure construction.
- `free_agent_pressure` and `contract_year_pressure` are built from active contract term exposure.
- `starter_cliff_1y_pressure` / `starter_cliff_2y_pressure` capture starter-level age+contract cliff risk.
- `future_need_pressure_1y` / `future_need_pressure_2y` blend contract runway with starter cliff exposure.
- `starter_quality` is roster-based so high pressure does not require poor current quality.