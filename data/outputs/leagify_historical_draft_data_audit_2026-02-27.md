# Leagify Historical Draft Data Audit (2026-02-27)

## What is in the repo
- Main historical table: `data/sources/external/historical-nfl-draft-data/notebook/compilations/drafts2015To2022.csv`
- Rows: **2048**, Columns: **40**
- Extra class file: `data/sources/external/historical-nfl-draft-data/notebook/drafts/2023Draft.csv` (rows=259, cols=30)
- Draft value chart table: `data/sources/external/historical-nfl-draft-data/notebook/drafts/draft_values.csv` (rows=262, cols=6)

## Year Coverage
- 2015: 256 picks
- 2016: 253 picks
- 2017: 253 picks
- 2018: 256 picks
- 2019: 254 picks
- 2020: 255 picks
- 2021: 259 picks
- 2022: 262 picks

## Field Coverage (main table)
- `wAV`: 2048/2048 non-null
- `DrAV`: 2048/2048 non-null
- `G`: 2048/2048 non-null
- `AP1`: 2048/2048 non-null
- `PB`: 2048/2048 non-null
- `St`: 2048/2048 non-null
- `PredictedAV`: 2048/2048 non-null
- `ValueVsPredictedValue`: 2048/2048 non-null
- `PlayerID`: 2048/2048 non-null
- `College/Univ`: 2046/2048 non-null

## Strong model-use fields
- Draft slot anchors: `Rnd`, `Pick`, `Tm`, `DraftYear`
- Career outcome proxies: `wAV`, `DrAV`, `G`, `AP1`, `PB`, `St`
- Expectation baseline already included: `PredictedAV` (Stuart curve)
- Surplus value signal: `ValueVsPredictedValue` = `wAV - PredictedAV`
- Trade-value chart references: `johnson`, `hill`, `otc`, `pff`

## Derived label sanity checks
- Candidate success label: `(wAV>=12) OR (G>=32) OR (AP1+PB>=1)`
- Success rate by pick bucket:
  - 1-32: 0.770
  - 33-64: 0.695
  - 65-100: 0.615
  - 101-150: 0.542
  - 151-260: 0.352
- Success rate by draft year:
  - 2015: 0.598
  - 2016: 0.672
  - 2017: 0.636
  - 2018: 0.723
  - 2019: 0.646
  - 2020: 0.576
  - 2021: 0.324
  - 2022: 0.011

## Where this helps your current stack
- Fill `historical_draft_outcomes_2016_2025.csv` backbone (partial years now) for real calibration.
- Replace synthetic proxies with true historical outcomes for `calibrate_historical_model.py`.
- Add positional over/under-performance priors using mean `ValueVsPredictedValue` by position.
- Use draft value columns for trade simulation realism (`johnson/hill/otc/pff`).

## Limits / caveats
- Coverage is mainly 2015-2022 (+2023 file). Missing 2024-2025 outcomes.
- Recent classes are right-censored for career value (need years-played adjustment).
- Position labels are mixed granularity (`OL`, `DB`, `LB`, `DE`, etc.) and need mapping to your normalized buckets.
- Some columns may be PFR-derived and should be treated as post-draft outcomes only (never as pre-draft model features).

## Recommended integration plan
1. Build an ingest script that maps this dataset into your historical outcomes schema.
2. Add right-censoring discount by draft year (e.g., down-weight 2021-2023 in career-value targets).
3. Re-run `scripts/calibrate_historical_model.py` and store new calibration artifacts.
4. Add trade-value columns as optional logic in mock draft trade engine.
5. Keep `Pick`, `Rnd`, and all outcome columns out of current-year prospect scoring features.

## Positional ValueVsPredictedAV snapshot (mean)
- NT: 15.00
- ILB: 11.33
- C: 11.11
- G: 9.25
- OLB: 8.94
- K: 6.75
- RB: 6.73
- DT: 6.50
- T: 6.48
- S: 6.45
- P: 4.78
- QB: 4.17