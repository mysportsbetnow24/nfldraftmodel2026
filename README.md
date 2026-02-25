# NFL Draft Model 1

Offline-first 2026 NFL Draft modeling workspace with:
- Prospect database and top-300 board seed
- Scouting Bible (position charts, traits, thresholds, grading logic)
- Consensus ranking merge pipeline (analyst + public boards)
- Relative Athletic Score (RAS) proxy and historical RAS bucket comparisons
- Team-need and scheme-fit scoring
- Round 1 and 7-round mock draft simulation
- Publish-ready player report templates

## Quick start

```bash
python3 scripts/build_seed_datasets.py
python3 scripts/build_big_board.py
python3 scripts/run_mock_draft.py
python3 scripts/generate_player_reports.py
python3 scripts/cfbd_usage_status.py
python3 scripts/odds_usage_status.py
```

Outputs are written to `data/processed` and `data/outputs`.

## Current constraints

- Shell network is blocked in this environment, so live pulls are implemented as adapters and documented in `docs/INGESTION_PLAYBOOK.md`.
- Odds API pulls are approval-gated and now budget-locked at 2,880 calls across 120 days (approved Feb 25, 2026).
- Paywalled analyst boards (Brugler/McShay/PFF premium) are supported via manual import templates.
- CFBD pulls are hard-capped at 1,000 calls/month with dry-run default.

## CFBD usage-safe pulls

```bash
# Dry run (no call spent)
python3 scripts/pull_cfbd_data.py --dataset team_advanced_stats --year 2025

# Spend exactly one call
python3 scripts/pull_cfbd_data.py --dataset team_advanced_stats --year 2025 --execute
```


## ESPN ingest pipeline (Kaggle)

Place files in:
`data/sources/external/espn_nfl_draft_prospect_data/`

Expected files:
- `nfl_draft_prospects.csv`
- `nfl_draft_profiles.csv`
- `college_qbr.csv`
- `college_stats.csv` (or `college_statistics.csv`)
- `ids.csv`

Run:

```bash
python3 scripts/qa_espn_ingest.py --target-year 2026
python3 scripts/build_big_board.py
python3 scripts/build_espn_training_splits.py --min-year 2016 --max-year 2025
```

What is used:
- Consensus/ranking signals: `ovr_rk`, `pos_rk`, `grade`
- Scouting text tags from `text1-4`: processing, separation, play_strength, motor, instincts, volatility
- Production signals from `college_stats` + QB `college_qbr`
- Size inputs: height/weight

What is intentionally not used (for noise/leakage control):
- Image/profile URL fields
- Non-predictive biography metadata
- Post-draft outcome fields as features (kept as targets only in historical splits)

## PlayerProfiler manual import (Breakout Age + Dominator)

Populate:
- `data/sources/manual/playerprofiler_2026.csv`

Template:
- `data/sources/manual/playerprofiler_2026_template.csv`

Run:

```bash
python3 scripts/build_big_board.py
python3 scripts/qa_playerprofiler_ingest.py
```

Fields used in scoring:
- `breakout_age` (earlier is better)
- `college_dominator` (primarily WR/RB/TE)
- `early_declare` (small bonus)

PP signals are blended conservatively and only applied when a player has PP data coverage.

## MockDraftable baselines

```bash
# Dry run
python3 scripts/pull_mockdraftable_data.py

# Live pull
python3 scripts/pull_mockdraftable_data.py --execute
```

Uses position aggregate baselines for athletic normalization.
See `docs/MOCKDRAFTABLE_INTEGRATION.md` for usage and weighting guidance.

## Core files

- `docs/SOURCES_AND_USE_CASES.md`: source inventory + how each data stream improves model quality
- `docs/SCOUTING_BIBLE_2026.md`: position scouting framework and NFL translation logic
- `docs/MODEL_FORMULAS.md`: grading and simulation formulas
- `config/position_weights.yml`: per-position trait/production/athletic weights
- `config/athletic_thresholds.yml`: baseline thresholds by position
- `data/sources/drafttek_2026_top300_seed.txt`: raw top-300 seed used to build board

## Next upgrades

1. Wire authenticated pulls for paid sources and Odds API.
2. Add historical draft outcome labels (Approximate Value, starts, second-contract hit rate) for model calibration.
3. Train and backtest position-specific success models by era and scheme family.
