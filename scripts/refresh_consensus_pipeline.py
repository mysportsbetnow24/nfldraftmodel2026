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
    pretty = " ".join(cmd)
    print(f"$ {pretty}", flush=True)
    subprocess.run(cmd, cwd=cwd or ROOT, env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh consensus boards, rebuild the big board, and regenerate site data."
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Do not hit remote consensus sources; reuse manual/local fallback rows only.",
    )
    parser.add_argument(
        "--skip-site-build",
        action="store_true",
        help="Skip the final Astro build after regenerating data.",
    )
    parser.add_argument(
        "--strict-production-knn",
        action="store_true",
        help="Do not auto-enable ALLOW_SINGLE_YEAR_PRODUCTION_KNN for the board rebuild.",
    )
    args = parser.parse_args()

    env = os.environ.copy()
    if not args.strict_production_knn:
        env.setdefault("ALLOW_SINGLE_YEAR_PRODUCTION_KNN", "1")
    env.setdefault("PYTHONPATH", str(ROOT))

    pull_cmd = [sys.executable, "scripts/pull_consensus_big_boards.py"]
    if args.skip_fetch:
        pull_cmd.append("--skip-fetch")
    _run(pull_cmd, cwd=ROOT, env=env)

    _run([sys.executable, "scripts/build_big_board.py"], cwd=ROOT, env=env)
    _run([sys.executable, "scripts/export_astro_site_data.py"], cwd=ROOT, env=env)

    if not args.skip_site_build:
        _run(["npm", "run", "build"], cwd=ASTRO_DIR, env=env)

    print("Consensus refresh pipeline completed.", flush=True)


if __name__ == "__main__":
    main()
