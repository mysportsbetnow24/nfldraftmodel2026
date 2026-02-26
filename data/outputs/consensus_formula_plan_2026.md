# Consensus Boards Integration Plan (2026)

## Sources requested
- Tankathon Big Board: https://www.tankathon.com/nfl/big_board
- NFL Mock Draft Database Consensus Big Board: https://www.nflmockdraftdatabase.com/big-boards/2026/consensus-big-board-2026

## What was added to the model pipeline
- Pull script: `scripts/pull_consensus_big_boards.py`
- Loader: `src/ingest/consensus_board_loader.py`
- Build integration: `scripts/build_big_board.py`

New board fields now available:
- `consensus_board_mean_rank`
- `consensus_board_rank_std`
- `consensus_board_source_count`
- `consensus_board_sources`
- `consensus_board_signal`

## Formula integration (implemented)
Consensus signal is added to the prior blend (1-100 scale):

`prior_signal = weighted_mean(seed_signal, external_rank_signal, analyst_signal, consensus_board_signal)`

Current prior weights:
- Seed rank signal: `0.42`
- External board rank signal: `0.23`
- Analyst aggregate signal: `0.15`
- Consensus-board signal: `0.20` (when present)

This keeps consensus meaningful but not dominant.

## Why this improves accuracy
- Reduces single-source bias from any one board.
- Stabilizes player tiers by adding agreement signal (`source_count`, `rank_std`).
- Preserves your independent scouting formula by only injecting consensus into prior, not directly into trait/production components.

## Recommended production settings
- Keep consensus contribution in `~15-22%` range of prior signal.
- Increase consensus impact only when `consensus_board_source_count >= 2`.
- Add disagreement penalty when `consensus_board_rank_std > 20`.

## Run order
1. Pull consensus:
   - `python3 scripts/pull_consensus_big_boards.py`
2. Rebuild board:
   - `python3 scripts/build_big_board.py`
3. Rebuild mock/reports:
   - `python3 scripts/run_mock_draft.py`
   - `python3 scripts/generate_player_reports.py`

## Fallback for NFLMock anti-bot / JS
If NFLMock page cannot be parsed directly, drop a manual file at:
- `data/sources/manual/nflmock_consensus_2026.csv`
Use template:
- `data/sources/manual/nflmock_consensus_2026_template.csv`
