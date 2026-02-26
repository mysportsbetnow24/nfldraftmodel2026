#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> None:
    print(f"\n$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Daily combine update cycle: QA -> feature build -> board -> mocks -> reports."
    )
    parser.add_argument(
        "--combine",
        type=str,
        default="data/sources/manual/combine_2026_results.csv",
        help="Path to combine CSV.",
    )
    parser.add_argument(
        "--skip-mock",
        action="store_true",
        help="Skip running mock draft simulation.",
    )
    parser.add_argument(
        "--skip-reports",
        action="store_true",
        help="Skip generating HTML scouting reports.",
    )
    args = parser.parse_args()

    combine_path = Path(args.combine)
    if not combine_path.is_absolute():
        combine_path = ROOT / combine_path

    if not combine_path.exists():
        raise SystemExit(f"Combine CSV not found: {combine_path}")

    try:
        _run(
            [
                sys.executable,
                "scripts/qa_build_inputs.py",
                "--combine",
                str(combine_path),
            ]
        )

        _run(
            [
                sys.executable,
                "scripts/build_mockdraftable_features.py",
                "--combine",
                str(combine_path),
            ]
        )
        _run([sys.executable, "scripts/build_big_board.py"])

        if not args.skip_mock:
            _run([sys.executable, "scripts/run_mock_draft.py"])
        if not args.skip_reports:
            _run([sys.executable, "scripts/generate_player_reports.py"])
    except subprocess.CalledProcessError as exc:
        print(f"\nStopped: command failed with exit code {exc.returncode}.")
        raise SystemExit(exc.returncode)

    print("\nCombine update cycle complete.")


if __name__ == "__main__":
    main()
