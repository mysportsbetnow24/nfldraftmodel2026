# Source Reliability By Position/Year Report

## Inputs
- Base source table: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/source_reliability_weights_2026.csv`
- Historical outcomes (pos-year quality): `/Users/nickholz/nfldraftmodel2026/data/sources/manual/historical_draft_outcomes_2016_2025.csv`
- Historical source-rank panel: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/historical_source_rank_panel_2016_2025.csv`
- Panel join outcomes: `/Users/nickholz/nfldraftmodel2026/data/processed/historical_labels_leagify_2015_2023.csv`

## Outputs
- Reliability table: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/source_reliability_by_pos_year_2016_2025.csv`
- Panel metrics table: `/Users/nickholz/nfldraftmodel2026/data/outputs/source_position_trust_metrics_2016_2025.csv`
- Joined panel (for QA): `/Users/nickholz/nfldraftmodel2026/data/outputs/historical_source_rank_panel_joined_2016_2025.csv`

## Coverage
- Year window: **2016-2025**
- Base sources: **13**
- Panel rows loaded: **0**
- Panel rows joined to outcomes: **0**
- Panel rows unmatched: **0**
- Panel draft years present: **none**
- Reliability rows written: **1430**

## Metrics Used
- pick-slot MAE
- top-32 hit rate
- top-100 hit rate
- rank correlation (Spearman)
- success label rate

## Hierarchical Shrinkage
- Cell metrics shrink toward a blended prior: `0.60 * source-global + 0.40 * position-global`.
- Source-global and position-global priors each shrink toward overall-global using support.
- Final `hit_rate` / `stability` are bounded and blended with base source defaults.

## Notes
- `build_big_board.py` already consumes this file for prior-blend multipliers by source+position+year.
- If panel coverage is sparse, this process remains conservative via shrinkage and base fallback.