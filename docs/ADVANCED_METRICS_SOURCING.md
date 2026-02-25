# Advanced Metrics Sourcing And Model Refinement

## Requested metrics and where to source them

1. Player Grades (PFF)
- Source: PFF data products / subscriptions (licensed, not fully public API).
- Link: https://www.pff.com/
- Ingestion method: manual import or licensed feed into `data/sources/manual/analyst_import_template.csv`-style table.
- Notes: treat as proprietary scouting signal; keep source attribution and snapshot date.

2. EPA / PPA (Expected/Predicted Points Added)
- Source A (CFBD): player and team PPA endpoints.
  - `GET /ppa/players/season`
  - `GET /ppa/teams`
- Source docs: https://github.com/CFBD/cfbd-python
- Source B (open methodology context): Open Source Football glossary.
  - https://opensourcefootball.com/posts/2020-08-20-nflfastr-ep-wp-and-cp-models/

3. Success Rate
- Source A (CFBD advanced season stats): `GET /stats/season/advanced`
- Field availability: `success_rate` in offense/defense advanced season models.
- Source docs: https://github.com/CFBD/cfbd-python
- Definition reference (college context): Game on Paper glossary.
  - https://gameonpaper.com/cfb

4. IsoPPP+
- Source raw ingredients:
  - play-level PPA (`/ppa/players/season`, `/ppa/teams`)
  - success flags from down/distance/yards
  - opponent adjustments from team defensive profile
- Canonical concept references:
  - Game on Paper glossary (`iso_ppa` and explosiveness context): https://gameonpaper.com/cfb
  - Bill Connelly/Football Study Hall glossary archive (IsoPPP concept): https://www.footballstudyhall.com/pages/college-football-advanced-stats-glossary

5. Tracking Football Athleticism
- Source: Tracking Football Athleticism products (licensed dataset/workbook).
- Link: https://www.trackingfootball.com/
- Ingestion method: manual CSV import with athlete IDs + testing metrics + composite scores.

## How to use these metrics in your draft model

1. Add an `advanced_signal` feature per prospect
- Formula:
  - 35% EPA/PPA per play (position-specific)
  - 25% success rate
  - 20% IsoPPP+
  - 20% PFF grade (if available)
- Benefit: captures efficiency + explosiveness + third-party grading consensus.

2. Position-specific mappings
- QB: EPA/play, late-down EPA, pressure EPA split, turnover-worthy play control.
- RB: rush EPA/play, stuff-rate avoidance, successful run rate, IsoPPP+ on successful carries.
- WR/TE: target EPA/play, success rate vs man/zone, first-down conversion rate, explosive catch rate.
- OL: team EPA when on-field, pressure/sack suppression splits, run success rate by gap.
- EDGE/DT/LB/DB: defensive EPA allowed, havoc/disruption rates, coverage EPA allowed, explosive-play prevention.

3. Add stability controls
- Snap/minimum-play threshold before full weight.
- Opponent and conference adjustment.
- Shrink one-year outliers toward multi-year mean.

4. Upgrade comps
- Include `RAS tier + IsoPPP+ bucket + EPA percentile` in nearest-neighbor comp search.
- This improves role-based comparisons beyond pure height/weight/speed.

## Current repo support

- CFBD adapter with call budget guard:
  - `src/ingest/cfbd_loader.py`
- Pull CLI:
  - `scripts/pull_cfbd_data.py`
- Advanced metrics computation helpers:
  - `src/modeling/advanced_metrics.py`

## Example pull commands (default dry-run: 0 calls spent)

```bash
python3 scripts/pull_cfbd_data.py --dataset player_ppa --year 2025
python3 scripts/pull_cfbd_data.py --dataset team_advanced_stats --year 2025 --conference SEC
python3 scripts/pull_cfbd_data.py --dataset advanced_game_stats --year 2025 --team Texas --season-type regular
```

To execute and spend one call per command:

```bash
python3 scripts/pull_cfbd_data.py --dataset player_ppa --year 2025 --execute
```
