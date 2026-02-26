#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.simulation.mock_draft import load_board, simulate_full_draft, write_csv

OUT = ROOT / "data" / "outputs"



def main() -> None:
    parser = argparse.ArgumentParser(description="Run 2026 mock drafts from current board and draft order inputs.")
    parser.add_argument(
        "--allow-simulated-trades",
        action="store_true",
        help="Enable synthetic trade-down heuristics (default off, strict draft order).",
    )
    args = parser.parse_args()

    board = load_board()
    round1, full7, trades = simulate_full_draft(
        board,
        rounds=7,
        allow_simulated_trades=args.allow_simulated_trades,
    )

    write_csv(OUT / "mock_2026_round1.csv", round1)
    write_csv(OUT / "mock_2026_7round.csv", full7)
    write_csv(OUT / "mock_2026_trades.csv", trades)

    print(f"Round 1 picks: {len(round1)}")
    print(f"7-round picks: {len(full7)}")
    print(f"Trade events: {len(trades)}")


if __name__ == "__main__":
    main()
