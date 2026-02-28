# Source Reliability By Position/Year Report

- Base source table: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/source_reliability_weights_2026.csv`
- Historical outcomes: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/historical_draft_outcomes_2016_2025.csv`
- Output table: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/source_reliability_by_pos_year_2016_2025.csv`
- Year window: **2016-2025**
- Base sources: **13**
- Position-year cells: **150**
- Rows written: **1950**
- Overall weighted success rate: **0.5737**

## Positions Covered

CB, DL, DT, EDGE, IOL, K, LB, LS, OT, P, QB, RB, S, TE, WR

## Notes

- This table is used as a refinement layer over base source reliability.
- Runtime resolver applies recency decay and shrinkage back to global source defaults.
- If this file is missing, the model falls back to base source-only weights.