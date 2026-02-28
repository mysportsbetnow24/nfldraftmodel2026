# ESPN Ingest QA Report

- Status: `missing_prospects`
- Dataset directory: `/Users/nickholz/nfldraftmodel2026/data/sources/external/espn_nfl_draft_prospect_data`

## Row counts

- prospects: `0`
- profiles: `0`
- college_qbr: `0`
- college_stats: `0`
- ids: `0`

## QA checks

- duplicate (draft_year, player_id): `0`
- non-empty profile text rows: `0`
- board rows checked: `412`
- name+position join rate: `0.0`
- name-only join rate: `0.0`

## Useful fields

- prospects: player_id, player_name, school, position, draft_year, ovr_rk, pos_rk, grade, height_in, weight_lb, draft_round, overall_pick, draft_team
- profiles: player_id, player_name, school, position, text1, text2, text3, text4
- college_qbr: player_id, player_name, school, position, season, qbr, epa_per_play
- college_stats: player_id, player_name, school, position, season, stat_name, stat_value
- ids: player_id, player_name, school, espn_id

## Rejected field categories

- headshots/images/urls
- biographical narrative fields that are not numeric features
- social/profile links
- post-draft descriptive text fields
- duplicate identifier columns that do not improve joins
