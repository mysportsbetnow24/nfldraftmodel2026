# 2026 Scouting Source Scrape Feasibility Check

Date: 2026-02-26

## Scope Tested
- Bleacher Report
- Pro Football & Sports Network (PFSN)
- A to Z Sports
- Sports Illustrated (SI/FCS)
- NFL.com (Daniel Jeremiah)

## Verdict Matrix

| Source | Fetch status | Structured data quality | Recommendation |
|---|---|---|---|
| NFL.com (DJ Top 50) | Strong | High (rank, player, position, school, report text) | Keep as core source (already working) |
| Bleacher Report | Strong on article URLs | High (overall board ranks, positional ranks, grades, superlatives) | Add automated scraper now |
| A to Z Sports | Strong | Medium-High (pros, projection, board rank/grade on many pages) | Add automated scraper now |
| SI (FCS Football Central) | Strong on article URLs | Medium-High (projection, strengths/areas to improve, career stats) | Add automated scraper now (FCS specialist layer) |
| PFSN | Mixed (some pages JS-heavy, article pages good) | Medium (mock/context and some rank pages; inconsistent static payloads) | Use hybrid approach: scrape article/rank pages + manual fallback |

## Practical Ingestion Plan

### 1) `data/processed/analyst_reports_2026.csv`
Add rows for narrative/report text sources:
- `NFL_Daniel_Jeremiah_2026` (already in)
- `B/R_Scouting_2026`
- `AtoZ_Scouting_2026`
- `SI_FCS_Scouting_2026`
- `PFSN_Scouting_2026` (article-driven where rank is not available)

Recommended fields to populate per row:
- `source`, `published_date`, `source_rank` (nullable), `player_name`, `school`, `position`, `source_url`, `report_text`

### 2) `data/sources/analyst_rankings_seed.csv`
Only insert sources with explicit ranked boards:
- DJ Top 50
- B/R Big Board and position rankings
- A to Z Big Board rank (when present in page body)
- PFSN big board/rank pages when rank table is parseable

### 3) `data/processed/consensus_big_boards_2026.csv`
Append normalized rank rows from all ranking-capable sources above.
This should feed consensus priors but not dominate model score.

## Feature Mapping (for `build_big_board.py`)

### Rank signals (prior/consensus blend)
- `br_rank_signal`, `atoz_rank_signal`, `pfsn_rank_signal`, `si_fcs_rank_signal` (FCS-only, lighter)

### Text trait signals (trait component)
- Parse tags from report text:
  - Positive: `processing`, `burst`, `play_strength`, `ball_skills`, `separation`, `motor`
  - Negative: `inconsistent`, `raw`, `limited length`, `injury`, `role-limited`
- Use source-specific caps and position normalization.

### Risk signals (penalty only)
- Penalize only for negative tags and explicit projection limits.
- Do not add positive boost from risk channel.

## Weights Recommendation (starting point)
- B/R rank: 8-10% inside consensus/prior blend
- A to Z rank: 6-8%
- PFSN rank: 5-7% (until parser stability improves)
- SI/FCS rank context: 2-4% (only for FCS prospects)
- Source text traits (combined): 6-10%
- Source risk penalties: penalty-only, capped

## Why this will improve the board
- Better ranked-anchor coverage reduces outlier spikes.
- More report text increases trait and risk realism.
- FCS SI reports improve long-tail coverage where other boards are thin.

## Guardrails to keep board credible
- Require declaration/eligibility filters before merge.
- Apply position-specific data sufficiency penalties (especially QB).
- Cap upside when external rank is missing and report coverage is sparse.
- Keep source text as support signal, not a primary driver over film/production.

## Evidence URLs Used in This Check
- NFL.com DJ Top 50: https://www.nfl.com/news/daniel-jeremiah-s-top-50-2026-nfl-draft-prospect-rankings-2-0
- Bleacher Report Draft hub: https://bleacherreport.com/nfl-draft
- Bleacher Report QB ranks/grades: https://bleacherreport.com/articles/25314815-2026-nfl-draft-quarterback-rankings-and-grades
- Bleacher Report big board: https://bleacherreport.com/articles/25384511-2026-nfl-draft-big-board-br-nfl-scouting-depts-post-senior-bowl-rankings
- PFSN draft hub: https://www.profootballnetwork.com/nfl-draft/
- PFSN prospect directory: https://www.profootballnetwork.com/nfl-draft-hq/prospect-directory/
- A to Z draft hub: https://atozsports.com/nfl-draft/
- A to Z sample scouting report: https://atozsports.com/nfl-draft/caleb-downs-2026-nfl-draft-scouting-report-for-ohio-state-safety/
- SI FCS sample scouting report: https://www.si.com/college/fcs/southland/2026-nfl-draft-scouting-report-incarnate-word-wr-jalen-walthall
