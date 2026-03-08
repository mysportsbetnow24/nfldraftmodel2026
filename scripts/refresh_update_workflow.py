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
            "Regular ScoutingGrade refresh workflow: consensus, depth charts, team-needs "
            "context, transactions, then mocks/site outputs."
        )
    )
    parser.add_argument(
        "--skip-consensus-fetch",
        action="store_true",
        help="Do not hit remote consensus sources; use local/manual fallback rows only.",
    )
    parser.add_argument(
        "--skip-depth-charts-fetch",
        action="store_true",
        help="Do not fetch ESPN depth charts before rebuilding team-needs context.",
    )
    parser.add_argument(
        "--skip-team-needs-context",
        action="store_true",
        help="Skip rebuilding team_needs_context_2026.csv from nflverse + ESPN inputs.",
    )
    parser.add_argument(
        "--skip-spotrac-fetch",
        action="store_true",
        help="Do not fetch Spotrac contract/free-agent tables before transactions/export.",
    )
    parser.add_argument(
        "--skip-transactions-fetch",
        action="store_true",
        help="Do not fetch CBS transactions before rebuilding transaction adjustments.",
    )
    parser.add_argument(
        "--skip-mocks",
        action="store_true",
        help="Skip the mock draft rebuild.",
    )
    parser.add_argument(
        "--skip-site-build",
        action="store_true",
        help="Skip the final Astro build after data export.",
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

    refresh_cmd = [sys.executable, "scripts/refresh_consensus_pipeline.py"]
    if args.skip_consensus_fetch:
        refresh_cmd.append("--skip-fetch")
    refresh_cmd.extend(["--skip-export", "--skip-site-build"])
    if args.strict_production_knn:
        refresh_cmd.append("--strict-production-knn")
    _run(refresh_cmd, cwd=ROOT, env=env)

    if not args.skip_depth_charts_fetch:
        _run([sys.executable, "scripts/pull_espn_depth_charts.py", "--season", "2026"], cwd=ROOT, env=env)

    if not args.skip_team_needs_context:
        _run([sys.executable, "scripts/build_team_needs_context_from_nflverse.py"], cwd=ROOT, env=env)

    if not args.skip_spotrac_fetch:
        _run([sys.executable, "scripts/pull_spotrac_contracts.py"], cwd=ROOT, env=env)

    if not args.skip_transactions_fetch:
        _run([sys.executable, "scripts/pull_cbs_transactions.py"], cwd=ROOT, env=env)

    _run([sys.executable, "scripts/build_team_needs_transaction_adjustments.py"], cwd=ROOT, env=env)

    if not args.skip_mocks:
        _run([sys.executable, "scripts/run_mock_draft.py"], cwd=ROOT, env=env)

    _run([sys.executable, "scripts/export_astro_site_data.py"], cwd=ROOT, env=env)

    if not args.skip_site_build:
        _run(["npm", "run", "build"], cwd=ASTRO_DIR, env=env)

    print("Regular update workflow completed.", flush=True)


if __name__ == "__main__":
    main()
