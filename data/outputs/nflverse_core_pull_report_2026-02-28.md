# NFLverse Core Pull Report

- Pulled at: `2026-02-28T00:54:04.845292+00:00`
- Seasons requested: `[2024]`
- Output directory: `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse`

## Dataset Summary

| Dataset | Rows | Columns | Output |
|---|---:|---:|---|
| participation | 45919 | 26 | `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/participation.parquet` |
| rosters_weekly | 46579 | 36 | `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/rosters_weekly.parquet` |
| contracts | 50229 | 25 | `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/contracts.parquet` |
| players | 24356 | 39 | `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/players.parquet` |
| combine | 8649 | 18 | `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/combine.parquet` |

## Key Notes

- nflreadpy currently exposes participation/rosters seasons through 2024 in this environment.
- All raw pulls are persisted as parquet; preview CSVs are for quick manual inspection.