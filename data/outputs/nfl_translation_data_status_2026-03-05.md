# NFL Translation Data Status (2026-03-05)

## Already Available Locally

### NFL outcomes (historical)
- `data/processed/leagify_historical_outcomes_2015_2023.csv` (2307 rows)
- `data/processed/historical_labels_leagify_2015_2023.csv` (2307 rows)
- `data/sources/manual/historical_draft_outcomes_2016_2025.csv` (2051 rows)

### College feature sources (historical/proxy)
- `data/sources/external/filtered_player_stats_full.csv` (126166 rows)
- `data/sources/external/combine_data_unique_athlete_id_step4.csv` (9060 rows)
- `data/sources/external/nflverse/combine.parquet` (exists)
- `data/sources/manual/cfb_production_2025.csv` (147 rows)

### Calibration outputs (already generated)
- `data/processed/historical_calibration_2016_2025.json`
- `data/outputs/historical_calibration_report_2016_2025.md`
- `data/outputs/historical_calibration_backtest_2016_2025.csv`
- `data/outputs/historical_pickslot_backtest_2016_2025.csv`

## Missing / Incomplete
- ESPN historical feature panel currently empty/missing in this repo state:
  - `data/processed/espn_historical_features_2016_2025.csv` (0 rows)
  - `data/sources/external/espn_nfl_draft_prospect_data/` expected source files not present

## Pull/Build Commands

### 1) Refresh NFL historical outcomes and labels
```bash
python3 scripts/ingest_leagify_historical_outcomes.py
python3 scripts/build_leagify_historical_labels.py
python3 scripts/build_historical_calibration_input.py --min-year 2016 --max-year 2025
python3 scripts/calibrate_historical_model.py --min-year 2016 --max-year 2025
```

### 2) Refresh nflverse supporting data (contracts/rosters/combine)
```bash
python3 scripts/pull_nflverse_core_data.py --seasons 2023,2024
```

### 3) Pull CFBD historical comp inputs (requires CFBD API key in env)
```bash
export CFB_API_KEY='YOUR_KEY'
python3 scripts/pull_cfbd_historical_comp_inputs.py --execute --start-year 2016 --end-year 2025
```

## Recommended Training Setup (no leakage)
1. Build historical draft-year panel from 2016-2025 players.
2. Use only pre-draft features (college production + combine + priors at draft time).
3. Join NFL outcomes (`success_label`, `starter_seasons`, `career_value`, etc.) for those same players.
4. Train position-specific translation mapping.
5. Apply the mapping to 2026 prospects as a capped signal.

## Important note
- 2026 prospects have no NFL stats yet.
- NFL stats are only for model training/calibration, never as direct current-year prospect inputs.
