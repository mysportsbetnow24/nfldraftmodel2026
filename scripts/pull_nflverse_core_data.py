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


def _season_window(seasons: list[int], min_year: int | None = None, max_year: int | None = None) -> list[int]:
    vals = []
    for season in seasons:
        if min_year is not None and season < min_year:
            continue
        if max_year is not None and season > max_year:
            continue
        vals.append(season)
    return vals


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
    p.add_argument(
        "--include-historical-outcomes",
        action="store_true",
        help="Also pull player_stats, snap_counts, nextgen_stats, and pfr_advstats for historical comp outcomes.",
    )
    args = p.parse_args()

    seasons = _parse_seasons(args.seasons)
    if not seasons:
        raise ValueError("No seasons parsed. Pass --seasons like '2024' or '2023,2024'.")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    core_seasons = _season_window(seasons, min_year=2016, max_year=2024)
    if not core_seasons:
        raise ValueError("No supported core seasons remain after applying nflverse window (2016-2024).")

    participation = nfl.load_participation(seasons=core_seasons)
    rosters_weekly = nfl.load_rosters_weekly(seasons=core_seasons)
    contracts = nfl.load_contracts()
    players = nfl.load_players()
    combine = nfl.load_combine(seasons=True)

    player_stats = None
    snap_counts = None
    nextgen_stats = None
    pfr_advstats = None
    if args.include_historical_outcomes:
        player_stats = nfl.load_player_stats(seasons=_season_window(seasons, min_year=1999), summary_level="reg")
        snap_counts = nfl.load_snap_counts(seasons=_season_window(seasons, min_year=2012))
        nextgen_frames = []
        for stat_type in ("passing", "receiving", "rushing"):
            df = nfl.load_nextgen_stats(seasons=_season_window(seasons, min_year=2016), stat_type=stat_type)
            if "stat_type" not in df.columns:
                df = df.with_columns(pl.lit(stat_type).alias("stat_type"))
            nextgen_frames.append(df)
        nextgen_stats = pl.concat(nextgen_frames, how="diagonal_relaxed") if nextgen_frames else pl.DataFrame()

        pfr_frames = []
        for stat_type in ("pass", "rush", "rec", "def"):
            df = nfl.load_pfr_advstats(
                seasons=_season_window(seasons, min_year=2018),
                stat_type=stat_type,
                summary_level="season",
            )
            if "stat_type" not in df.columns:
                df = df.with_columns(pl.lit(stat_type).alias("stat_type"))
            pfr_frames.append(df)
        pfr_advstats = pl.concat(pfr_frames, how="diagonal_relaxed") if pfr_frames else pl.DataFrame()

    paths = {
        "participation_parquet": args.out_dir / "participation.parquet",
        "rosters_weekly_parquet": args.out_dir / "rosters_weekly.parquet",
        "contracts_parquet": args.out_dir / "contracts.parquet",
        "players_parquet": args.out_dir / "players.parquet",
        "combine_parquet": args.out_dir / "combine.parquet",
        "player_stats_parquet": args.out_dir / "player_stats.parquet",
        "snap_counts_parquet": args.out_dir / "snap_counts.parquet",
        "nextgen_stats_parquet": args.out_dir / "nextgen_stats.parquet",
        "pfr_advstats_parquet": args.out_dir / "pfr_advstats.parquet",
        "participation_preview_csv": args.out_dir / "participation_preview.csv",
        "rosters_weekly_preview_csv": args.out_dir / "rosters_weekly_preview.csv",
        "contracts_preview_csv": args.out_dir / "contracts_preview.csv",
        "players_preview_csv": args.out_dir / "players_preview.csv",
        "combine_preview_csv": args.out_dir / "combine_preview.csv",
        "player_stats_preview_csv": args.out_dir / "player_stats_preview.csv",
        "snap_counts_preview_csv": args.out_dir / "snap_counts_preview.csv",
        "nextgen_stats_preview_csv": args.out_dir / "nextgen_stats_preview.csv",
        "pfr_advstats_preview_csv": args.out_dir / "pfr_advstats_preview.csv",
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
    if player_stats is not None:
        player_stats.write_parquet(paths["player_stats_parquet"])
        _write_preview_csv(player_stats, paths["player_stats_preview_csv"], n=3000)
    if snap_counts is not None:
        snap_counts.write_parquet(paths["snap_counts_parquet"])
        _write_preview_csv(snap_counts, paths["snap_counts_preview_csv"], n=3000)
    if nextgen_stats is not None:
        nextgen_stats.write_parquet(paths["nextgen_stats_parquet"])
        _write_preview_csv(nextgen_stats, paths["nextgen_stats_preview_csv"], n=3000)
    if pfr_advstats is not None:
        pfr_advstats.write_parquet(paths["pfr_advstats_parquet"])
        _write_preview_csv(pfr_advstats, paths["pfr_advstats_preview_csv"], n=3000)

    lines = [
        "# NFLverse Core Pull Report",
        "",
        f"- Pulled at: `{datetime.now(UTC).isoformat()}`",
        f"- Seasons requested: `{seasons}`",
        f"- Core team-needs seasons pulled: `{core_seasons}`",
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
        f"| player_stats | {player_stats.height if player_stats is not None else 0} | {len(player_stats.columns) if player_stats is not None else 0} | `{paths['player_stats_parquet']}` |" if player_stats is not None else "",
        f"| snap_counts | {snap_counts.height if snap_counts is not None else 0} | {len(snap_counts.columns) if snap_counts is not None else 0} | `{paths['snap_counts_parquet']}` |" if snap_counts is not None else "",
        f"| nextgen_stats | {nextgen_stats.height if nextgen_stats is not None else 0} | {len(nextgen_stats.columns) if nextgen_stats is not None else 0} | `{paths['nextgen_stats_parquet']}` |" if nextgen_stats is not None else "",
        f"| pfr_advstats | {pfr_advstats.height if pfr_advstats is not None else 0} | {len(pfr_advstats.columns) if pfr_advstats is not None else 0} | `{paths['pfr_advstats_parquet']}` |" if pfr_advstats is not None else "",
        "",
        "## Key Notes",
        "",
        "- participation/rosters_weekly are clamped to the supported 2016-2024 window in this environment.",
        "- historical outcome datasets (player_stats / snap_counts / nextgen / pfr_advstats) can still pull deeper season ranges when available.",
        "- All raw pulls are persisted as parquet; preview CSVs are for quick manual inspection.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Pulled seasons: {seasons}")
    print(f"Output dir: {args.out_dir}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
