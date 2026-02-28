# Leagify Historical Draft Data: Detailed Integration Plan (2026-02-27)

## 1) Exactly what to use

Use these files as primary inputs:
- `data/sources/external/historical-nfl-draft-data/notebook/compilations/drafts2015To2022.csv`
- `data/sources/external/historical-nfl-draft-data/notebook/drafts/2023Draft.csv`
- `data/sources/external/historical-nfl-draft-data/notebook/drafts/draft_values.csv`

Use these fields from `drafts2015To2022.csv`:
- Draft context: `DraftYear`, `Rnd`, `Pick`, `Tm`, `Pos`, `College/Univ`, `PlayerID`
- Outcome labels: `wAV`, `DrAV`, `G`, `AP1`, `PB`, `St`
- Expectation/surplus labels: `PredictedAV`, `ValueVsPredictedValue`
- Trade references copied in row context: `johnson`, `hill`, `otc`, `pff`

Use these fields from `draft_values.csv`:
- `pick`, `johnson`, `hill`, `otc`, `pff`, `stuart`

## 2) What each field should do in your model stack

- `wAV`: long-horizon career value label. Use for calibration/training target only.
- `G`: availability and role stickiness proxy. Use in success label definitions.
- `AP1` + `PB`: high-end outcome labels (ceiling outcomes).
- `St` (starts): starter-quality proxy for early-career outcomes.
- `PredictedAV`: baseline expected value by slot. Use as expected target, never as a 2026 feature.
- `ValueVsPredictedValue`: over/under-performance versus slot baseline. Use to learn archetypes that beat draft slot expectation.
- `johnson/hill/otc/pff` draft value columns: use in trade simulation, not prospect talent grading.

## 3) Labels to create (recommended)

Create multiple labels, not just one:

1. `success_label_3yr`
- 1 if any is true: `wAV >= 10` OR `G >= 32` OR `AP1 + PB >= 1`
- Else 0

2. `starter_label_3yr`
- 1 if `St >= 2` OR `G >= 40`
- Else 0

3. `ceiling_label`
- 1 if `AP1 >= 1` OR `PB >= 2`
- Else 0

4. `surplus_value`
- `surplus_value = ValueVsPredictedValue`
- Use as regression target and as positional ROI diagnostics

## 4) Right-censoring treatment (critical)

Recent classes are not mature yet. Use year-based sample weights:
- 2015-2018: `1.00`
- 2019: `0.85`
- 2020: `0.70`
- 2021: `0.50`
- 2022: `0.30`
- 2023: `0.15`

Apply these weights during calibration training and in any aggregate reports.

## 5) Position normalization for your board schema

Map source `Pos` to your model buckets before training:
- `QB -> QB`
- `RB, FB -> RB`
- `WR -> WR`
- `TE -> TE`
- `T, OT -> OT`
- `G, C, OG -> IOL`
- `DE, OLB, EDGE -> EDGE`
- `DT, NT -> DT`
- `LB, ILB, MLB -> LB`
- `CB -> CB`
- `S, FS, SS, DB -> S`

## 6) How to use it in each subsystem

### A) Historical calibration subsystem (highest value)
Use this dataset to populate your `historical_draft_outcomes_2016_2025.csv` workflow.

Workflow:
1. Ingest and normalize 2015-2023 rows.
2. Build leakage-safe labels (`success_label_*`, `starter_label_*`, `ceiling_label`, `surplus_value`).
3. Merge to your historical model-grade snapshots by year/player/position.
4. Train year-based rolling calibration (train <=Y-1, validate on Y).
5. Export position additive adjustments and grade bin hit rates.

### B) Team-needs + mock trade engine
Use `draft_values.csv` to score trades:

Trade score example:
- `value_out = sum(pick_value[p] for p in picks_sent)`
- `value_in = sum(pick_value[p] for p in picks_received)`
- `trade_fairness = value_in / max(value_out, 1)`

Use a blended chart for realism:
- `blended_pick_value = 0.50*otc + 0.25*johnson + 0.15*hill + 0.10*pff`

### C) Positional ROI priors (small, guarded)
Use mean `surplus_value` by normalized position + pick-band as a weak prior.

Guardrails:
- Max contribution to final grade: `±0.60`
- Weight in final score: `2% to 4%`
- Never let this override consensus + current-year evidence

## 7) Leakage rules (must enforce)

Do not use these as 2026 prospect features:
- `wAV`, `DrAV`, `G`, `AP1`, `PB`, `St`, `PredictedAV`, `ValueVsPredictedValue`, `Pick`, `Rnd`

These are labels/calibration-only or simulation-only.

## 8) Quality checks to run before training

1. Deduplicate on `DraftYear + PlayerID`.
2. Validate pick ranges (1-262/comp-era as applicable).
3. Verify position mapping coverage rate > 98%.
4. Confirm no target leakage into 2026 scoring table.
5. Confirm class-weighting applied for right-censored years.

## 9) Recommended immediate implementation order

1. Build `scripts/ingest_leagify_historical_outcomes.py`
- Output: `data/processed/leagify_historical_outcomes_2015_2023.csv`

2. Build label table
- Output: `data/processed/historical_labels_leagify_2015_2023.csv`

3. Merge with your historical grades and rerun calibration
- Input into `scripts/calibrate_historical_model.py`
- Output updated calibration artifacts in `data/processed/`

4. Add draft value blend into mock trade logic
- Use `draft_values.csv` as chart backend

5. Add positional ROI prior as low-weight, capped adjustment
- Keep max effect very small (`±0.60`) and monitored in delta audits

## 10) What this will improve first

- Better realism in round-value calibration
- Better control of outliers by position/slot expectation
- Stronger trade realism in mock simulations
- More stable weekly rankings with defensible historical anchors
