# NFLverse Combine Baseline Build Report

- Input: `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/combine.parquet`
- Output: `/Users/nickholz/nfldraftmodel2026/data/sources/external/nflverse/nflverse_combine_standardized.csv`
- Input rows: `8649`
- Output rows: `8649`

## Notes

- Converted to the same metric column names expected by `athletic_profile_loader.py`.
- Missing arms/hands/10-split in nflverse combine are left blank and handled by coverage penalties.