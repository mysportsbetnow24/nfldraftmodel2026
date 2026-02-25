# Analyst Ranking Ingestion Guide

## Target analysts and strategy

1. Daniel Jeremiah (NFL.com)
- Pull public top-50 / positional rankings pages.
- Parse rank + player + position + school.

2. Dane Brugler (The Athletic)
- Usually subscription-gated.
- Workflow: export or manually transcribe into `data/sources/manual/brugler_board.csv`.

3. Todd McShay
- Source often podcast/article-based and may be gated.
- Workflow: manual import template and timestamp each snapshot.

4. NFL Stock Exchange
- Pull board/mocks where public and parse rank+player.

5. PFF draft board
- Public summary pull where available.
- Premium details via manual import.

## Normalization rules

- Convert all positions to internal taxonomy: `QB,RB,WR,TE,OT,IOL,EDGE,DT,LB,CB,S`.
- Resolve school aliases (`Ole Miss` -> `Mississippi`, `Miami (FL)` preserved).
- Keep one row per `(source, source_rank, player_name)`.

## Source weighting (default)

- Consensus/public aggregate: 0.30
- National analyst boards: 0.50 total split across analysts
- Internal model grade: 0.20 (can increase once calibrated)

## Manual template columns

- `source`
- `snapshot_date`
- `source_rank`
- `player_name`
- `school`
- `position`
- `notes`
- `source_url`
