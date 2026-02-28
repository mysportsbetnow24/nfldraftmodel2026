# Position ROI Priors Report

- Input: `/Users/nickholz/nfldraftmodel2026/data/processed/historical_labels_leagify_2015_2023.csv`
- Output: `/Users/nickholz/nfldraftmodel2026/data/processed/position_roi_priors_leagify_2016_2023.csv`
- Year window: **2016-2023**
- Rows written: **67**

## Adjustment Rule

- `roi_grade_adjustment = clamp(0.25 * surplus_z, -0.60, +0.60)`
- Used as a small prior only; cannot override core grading.