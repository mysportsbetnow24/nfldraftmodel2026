#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ASTRO_DIR = ROOT / "astro-site"


def _run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=cwd or ROOT, env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh the transactions feed and team-needs transaction deltas without running a full "
            "board rebuild."
        )
    )
    parser.add_argument(
        "--skip-transactions-fetch",
        action="store_true",
        help="Do not fetch CBS transactions before rebuilding transaction adjustments.",
    )
    parser.add_argument(
        "--skip-site-build",
        action="store_true",
        help="Skip the final Astro build after data export.",
    )
    args = parser.parse_args()

    env = os.environ.copy()
    env.setdefault("PYTHONPATH", str(ROOT))

    if not args.skip_transactions_fetch:
        _run([sys.executable, "scripts/pull_cbs_transactions.py"], cwd=ROOT, env=env)

    _run([sys.executable, "scripts/build_team_needs_transaction_adjustments.py"], cwd=ROOT, env=env)
    _run([sys.executable, "scripts/export_astro_site_data.py"], cwd=ROOT, env=env)

    if not args.skip_site_build:
        _run(["npm", "run", "build"], cwd=ASTRO_DIR, env=env)

    print("Transactions refresh pipeline completed.", flush=True)


if __name__ == "__main__":
    main()
