# cfbfastR Data Assessment for NFL Draft Model (2026)

## Bottom line
Yes, cfbfastR data can materially improve your model, especially in four areas:
1. Better player-level efficiency context (PPA/WEPA/usage)
2. Better risk/context features (returning production, recruiting pedigree)
3. Better historical calibration labels (draft picks)
4. Better opponent-strength normalization (SRS)

## Highest-value player datasets to add first

### P0 (most impact)
- `cfbd_metrics_ppa_players_season`
  - Why: true player efficiency splits, better than broad season totals.
  - Use: QB pressure/down splits, WR/TE passing-down value, RB rushing-down efficiency.
- `cfbd_metrics_wepa_players_passing`
- `cfbd_metrics_wepa_players_receiving`
- `cfbd_metrics_wepa_players_rushing`
  - Why: opponent/context-adjusted player impact signal.
  - Use: replace proxy efficiency pieces inside production component.
- `cfbd_player_usage`
  - Why: role share (workload concentration) and deployability.
  - Use: role-context and risk (high production on low usage = volatility risk).
- `cfbd_stats_season_player`
  - Why: standardized base counts for broad coverage and fallback.
  - Use: fallback features where WEPA/PPA missing.
- `cfbd_draft_picks`
  - Why: historical labels for 2016-2025 backtest/calibration.
  - Use: model calibration targets by position/tier.

### P1 (high value, second wave)
- `cfbd_player_returning`
  - Why: returning production context around player role strength.
  - Use: context/risk adjustment; sustainability check.
- `cfbd_recruiting_player`
  - Why: pedigree and development prior.
  - Use: tie-breaker + risk modulation (low pedigree + low efficiency = higher bust risk).
- `cfbd_ratings_srs`
  - Why: strength-of-schedule adjustment at team level.
  - Use: normalize player production by opponent strength.

### P2 (useful but optional)
- `cfbd_stats_season_advanced` (team stats)
- `load_cfb_pbp` / `load_cfb_rosters`
  - Use only for custom feature engineering and join quality.

## Guardrails (already aligned)
- Team-level CFB data is **not** used for NFL team-needs logic.
- Team-level CFB data is **not** allowed to directly drive `final_grade`.
- It can only be used for optional player stat normalization/context.

## Practical integration map to your formula
- `production_score`
  - Add: WEPA + PPA season splits + usage share signals.
- `context_score`
  - Add: returning production context + SRS-normalized environment.
- `risk_penalty`
  - Add: low usage + weak recruiting pedigree + unstable split profile penalties.
- `calibration`
  - Add: `cfbd_draft_picks` historical outcomes for position-specific tuning.

## API budget strategy (1000 calls/month)
- Pull annual player datasets once per season key (not per week) for build cycles.
- Suggested monthly budget:
  - Core (P0): 6-8 calls total
  - Secondary (P1): 3-4 calls total
  - Reserve: >980 calls left for ad hoc checks
- Use dry-run first; execute only approved batches.

## Current state in repo
- Already present and used: `player_season_stats_2025.json`, `player_ppa_2025.json`
- Added richer cfbfastR catalog: `src/ingest/cfbfastr_loader.py`

