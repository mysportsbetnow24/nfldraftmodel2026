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
```

Outputs are written to `data/processed` and `data/outputs`.

## Current constraints

- Shell network is blocked in this environment, so live pulls are implemented as adapters and documented in `docs/INGESTION_PLAYBOOK.md`.
- Odds API pulls are intentionally gated behind user approval.
- Paywalled analyst boards (Brugler/McShay/PFF premium) are supported via manual import templates.
- CFBD pulls are hard-capped at 1,000 calls/month with dry-run default.

## CFBD usage-safe pulls

```bash
# Dry run (no call spent)
python3 scripts/pull_cfbd_data.py --dataset team_advanced_stats --year 2025

# Spend exactly one call
python3 scripts/pull_cfbd_data.py --dataset team_advanced_stats --year 2025 --execute
```

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
