#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import nflreadpy as nfl
import polars as pl


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "sources" / "external" / "nflverse"
REPORT_PATH = ROOT / "data" / "outputs" / "nflverse_core_pull_report_2026-02-28.md"


def _write_preview_csv(df, path: Path, n: int = 2000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    head_df = df.head(n)
    cols = []
    for c in head_df.columns:
        try:
            s = head_df.select(pl.col(c).cast(pl.Utf8, strict=False)).to_series()
            s = s.rename(c)
            cols.append(s)
        except Exception:
            # Skip nested/unsupported columns in CSV preview.
            continue
    if not cols:
        path.write_text("")
        return
    pl.DataFrame(cols).write_csv(path)


def _parse_seasons(raw: str) -> list[int]:
    vals = []
    for part in (raw or "").split(","):
        txt = part.strip()
        if not txt:
            continue
        vals.append(int(txt))
    return sorted(set(vals))


def main() -> None:
    p = argparse.ArgumentParser(description="Pull core nflverse datasets for team-needs and combine baselines.")
    p.add_argument(
        "--seasons",
        type=str,
        default="2024",
        help="Comma-separated seasons for participation and rosters_weekly (e.g. 2023,2024).",
    )
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    p.add_argument("--report", type=Path, default=REPORT_PATH)
    args = p.parse_args()

    seasons = _parse_seasons(args.seasons)
    if not seasons:
        raise ValueError("No seasons parsed. Pass --seasons like '2024' or '2023,2024'.")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    participation = nfl.load_participation(seasons=seasons)
    rosters_weekly = nfl.load_rosters_weekly(seasons=seasons)
    contracts = nfl.load_contracts()
    players = nfl.load_players()
    combine = nfl.load_combine(seasons=True)

    paths = {
        "participation_parquet": args.out_dir / "participation.parquet",
        "rosters_weekly_parquet": args.out_dir / "rosters_weekly.parquet",
        "contracts_parquet": args.out_dir / "contracts.parquet",
        "players_parquet": args.out_dir / "players.parquet",
        "combine_parquet": args.out_dir / "combine.parquet",
        "participation_preview_csv": args.out_dir / "participation_preview.csv",
        "rosters_weekly_preview_csv": args.out_dir / "rosters_weekly_preview.csv",
        "contracts_preview_csv": args.out_dir / "contracts_preview.csv",
        "players_preview_csv": args.out_dir / "players_preview.csv",
        "combine_preview_csv": args.out_dir / "combine_preview.csv",
    }

    participation.write_parquet(paths["participation_parquet"])
    rosters_weekly.write_parquet(paths["rosters_weekly_parquet"])
    contracts.write_parquet(paths["contracts_parquet"])
    players.write_parquet(paths["players_parquet"])
    combine.write_parquet(paths["combine_parquet"])

    _write_preview_csv(participation, paths["participation_preview_csv"], n=3000)
    _write_preview_csv(rosters_weekly, paths["rosters_weekly_preview_csv"], n=3000)
    _write_preview_csv(contracts, paths["contracts_preview_csv"], n=3000)
    _write_preview_csv(players, paths["players_preview_csv"], n=3000)
    _write_preview_csv(combine, paths["combine_preview_csv"], n=3000)

    lines = [
        "# NFLverse Core Pull Report",
        "",
        f"- Pulled at: `{datetime.now(UTC).isoformat()}`",
        f"- Seasons requested: `{seasons}`",
        f"- Output directory: `{args.out_dir}`",
        "",
        "## Dataset Summary",
        "",
        "| Dataset | Rows | Columns | Output |",
        "|---|---:|---:|---|",
        f"| participation | {participation.height} | {len(participation.columns)} | `{paths['participation_parquet']}` |",
        f"| rosters_weekly | {rosters_weekly.height} | {len(rosters_weekly.columns)} | `{paths['rosters_weekly_parquet']}` |",
        f"| contracts | {contracts.height} | {len(contracts.columns)} | `{paths['contracts_parquet']}` |",
        f"| players | {players.height} | {len(players.columns)} | `{paths['players_parquet']}` |",
        f"| combine | {combine.height} | {len(combine.columns)} | `{paths['combine_parquet']}` |",
        "",
        "## Key Notes",
        "",
        "- nflreadpy currently exposes participation/rosters seasons through 2024 in this environment.",
        "- All raw pulls are persisted as parquet; preview CSVs are for quick manual inspection.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Pulled seasons: {seasons}")
    print(f"Output dir: {args.out_dir}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
