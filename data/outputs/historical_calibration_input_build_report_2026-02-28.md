# Historical Calibration Input Build Report

- Labels input: `/Users/nickholz/nfldraftmodel2026/data/processed/historical_labels_leagify_2015_2023.csv`
- Optional snapshot input: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/historical_model_grade_snapshots_2016_2025.csv`
- Output: `/Users/nickholz/nfldraftmodel2026/data/sources/manual/historical_draft_outcomes_2016_2025.csv`
- Year window: **2016-2025**
- Rows written: **2051**
- Rows using snapshot model_grade: **0**
- Rows using proxy model_grade: **2051**

## Notes

- `model_grade` uses snapshot values when provided; otherwise uses a proxy from pick, predicted AV, and blended draft value.
- `ras` and `pff_grade` are neutral placeholders for historical calibration context.
- `career_value`, `starter_seasons`, `second_contract`, and `success_label` come from Leagify outcome labels.