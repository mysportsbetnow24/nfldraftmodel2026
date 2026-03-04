#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.simulation.mock_draft import (
    DEFAULT_SOFTMAX_TEMPERATURE,
    load_board,
    simulate_full_draft,
    simulate_full_draft_monte_carlo,
    write_csv,
)

OUT = ROOT / "data" / "outputs"



def main() -> None:
    parser = argparse.ArgumentParser(description="Run 2026 mock drafts from current board and draft order inputs.")
    parser.add_argument(
        "--allow-simulated-trades",
        action="store_true",
        help="Enable synthetic trade-down heuristics (default off, strict draft order).",
    )
    parser.add_argument(
        "--team-athletic-bias",
        action="store_true",
        help="Enable soft team-athletic-threshold fit modifier in pick scoring.",
    )
    parser.add_argument(
        "--selection-mode",
        choices=["top", "sample"],
        default="top",
        help="Pick selection mode: deterministic top-score or probabilistic softmax sample.",
    )
    parser.add_argument(
        "--softmax-temperature",
        type=float,
        default=DEFAULT_SOFTMAX_TEMPERATURE,
        help="Softmax temperature for sampled picks (lower = more deterministic).",
    )
    parser.add_argument(
        "--simulations",
        type=int,
        default=1,
        help="Number of full-draft simulations. If >1, runs Monte Carlo and outputs median/variance table.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=2026,
        help="Base RNG seed for sampled simulations.",
    )
    args = parser.parse_args()
    effective_selection_mode = "sample" if int(args.simulations) > 1 else args.selection_mode

    board = load_board()
    if int(args.simulations) > 1:
        round1, full7, trades, sim_dist = simulate_full_draft_monte_carlo(
            board,
            rounds=7,
            simulations=int(args.simulations),
            allow_simulated_trades=args.allow_simulated_trades,
            enable_team_athletic_bias=args.team_athletic_bias,
            softmax_temperature=float(args.softmax_temperature),
            random_seed=int(args.random_seed),
        )
        write_csv(OUT / "mock_2026_sim_player_distribution.csv", sim_dist)
    else:
        round1, full7, trades = simulate_full_draft(
            board,
            rounds=7,
            allow_simulated_trades=args.allow_simulated_trades,
            enable_team_athletic_bias=args.team_athletic_bias,
            selection_mode=args.selection_mode,
            softmax_temperature=float(args.softmax_temperature),
            random_seed=int(args.random_seed),
        )

    write_csv(OUT / "mock_2026_round1.csv", round1)
    write_csv(OUT / "mock_2026_7round.csv", full7)
    write_csv(OUT / "mock_2026_trades.csv", trades)

    print(f"Round 1 picks: {len(round1)}")
    print(f"7-round picks: {len(full7)}")
    print(f"Trade events: {len(trades)}")
    print(f"Team athletic bias enabled: {int(args.team_athletic_bias)}")
    print(f"Selection mode: {effective_selection_mode}")
    print(f"Softmax temperature: {float(args.softmax_temperature):.3f}")
    print(f"Simulations: {int(args.simulations)}")
    if int(args.simulations) > 1:
        print(f"Simulation distribution: {OUT / 'mock_2026_sim_player_distribution.csv'}")


if __name__ == "__main__":
    main()
