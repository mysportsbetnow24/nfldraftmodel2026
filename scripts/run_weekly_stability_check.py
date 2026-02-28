#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "data" / "outputs"
SNAPSHOT_DIR = OUTPUTS / "stability_snapshots"

DEFAULT_BOARD = OUTPUTS / "big_board_2026.csv"
DEFAULT_WATCHLIST = OUTPUTS / "contrarian_watchlist_2026.csv"
FALLBACK_PRE_BOARD = OUTPUTS / "big_board_2026_pre_midband_brake.csv"


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _to_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _board_index(rows: list[dict]) -> dict[str, dict]:
    return {row.get("player_name", "").strip(): row for row in rows if row.get("player_name", "").strip()}


def _top_set(rows: list[dict], n: int) -> set[str]:
    out = set()
    for row in rows:
        rank = _to_int(row.get("consensus_rank"), 9999)
        if rank <= n:
            name = row.get("player_name", "").strip()
            if name:
                out.add(name)
    return out


def _watchlist_from_board(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        if _to_int(row.get("is_diamond_exception"), 0) != 1:
            continue
        out.append(
            {
                "player_name": row.get("player_name", "").strip(),
                "position": row.get("position", ""),
                "consensus_rank": row.get("consensus_rank", ""),
                "contrarian_score": row.get("contrarian_score", ""),
            }
        )
    out.sort(key=lambda r: _to_float(r.get("contrarian_score"), 0.0), reverse=True)
    return out


def _snapshot_files(snapshot_dir: Path) -> tuple[list[Path], list[Path]]:
    boards = sorted(snapshot_dir.glob("big_board_2026_snapshot_*.csv"))
    watchlists = sorted(snapshot_dir.glob("contrarian_watchlist_2026_snapshot_*.csv"))
    return boards, watchlists


def run_check(board_path: Path, watchlist_path: Path, snapshot_dir: Path) -> tuple[Path, Path]:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    OUTPUTS.mkdir(parents=True, exist_ok=True)

    now = dt.datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    day_label = now.strftime("%Y-%m-%d")

    board_snapshot = snapshot_dir / f"big_board_2026_snapshot_{stamp}.csv"
    watch_snapshot = snapshot_dir / f"contrarian_watchlist_2026_snapshot_{stamp}.csv"
    shutil.copy2(board_path, board_snapshot)
    if watchlist_path.exists():
        shutil.copy2(watchlist_path, watch_snapshot)

    board_snaps, watch_snaps = _snapshot_files(snapshot_dir)
    prev_board_path: Path | None = None
    prev_watch_path: Path | None = None

    if len(board_snaps) >= 2:
        prev_board_path = board_snaps[-2]
    elif FALLBACK_PRE_BOARD.exists():
        prev_board_path = FALLBACK_PRE_BOARD

    if len(watch_snaps) >= 2:
        prev_watch_path = watch_snaps[-2]

    curr_board = _read_rows(board_path)
    prev_board = _read_rows(prev_board_path) if prev_board_path else []
    curr_watch = _read_rows(watchlist_path) if watchlist_path.exists() else _watchlist_from_board(curr_board)
    prev_watch = _read_rows(prev_watch_path) if prev_watch_path else _watchlist_from_board(prev_board)

    curr_board_idx = _board_index(curr_board)
    prev_board_idx = _board_index(prev_board)

    curr_top32 = _top_set(curr_board, 32)
    prev_top32 = _top_set(prev_board, 32)
    added_top32 = sorted(curr_top32 - prev_top32)
    dropped_top32 = sorted(prev_top32 - curr_top32)
    overlap_top32 = len(curr_top32 & prev_top32)
    churn_rate = (len(added_top32) + len(dropped_top32)) / 32.0 if prev_top32 else 0.0

    curr_top50_rows = sorted(curr_board, key=lambda r: _to_int(r.get("consensus_rank"), 9999))[:50]
    delta_rows = []
    abs_deltas = []
    for row in curr_top50_rows:
        name = row.get("player_name", "").strip()
        curr_rank = _to_int(row.get("consensus_rank"), 9999)
        prev_rank = _to_int(prev_board_idx.get(name, {}).get("consensus_rank"), 9999) if prev_board_idx else 9999
        delta = "" if prev_rank == 9999 else prev_rank - curr_rank
        if delta != "":
            abs_deltas.append(abs(delta))
        delta_rows.append(
            {
                "player_name": name,
                "position": row.get("position", ""),
                "curr_rank": curr_rank,
                "prev_rank": "" if prev_rank == 9999 else prev_rank,
                "rank_change_prev_minus_curr": delta,
                "curr_grade": row.get("final_grade", ""),
                "curr_round_value": row.get("round_value", ""),
                "consensus_mean_rank": row.get("consensus_board_mean_rank", ""),
                "midband_brake_penalty": row.get("formula_midband_brake_penalty", ""),
                "soft_ceiling_penalty": row.get("formula_soft_ceiling_penalty", ""),
                "consensus_tail_soft_penalty": row.get("formula_consensus_tail_soft_penalty", ""),
                "front7_inflation_penalty": row.get("formula_front7_inflation_penalty", ""),
                "cb_nickel_inflation_penalty": row.get("formula_cb_nickel_inflation_penalty", ""),
            }
        )
    mean_abs_top50_delta = (sum(abs_deltas) / len(abs_deltas)) if abs_deltas else 0.0

    curr_watch_idx = {r.get("player_name", "").strip(): r for r in curr_watch if r.get("player_name", "").strip()}
    prev_watch_idx = {r.get("player_name", "").strip(): r for r in prev_watch if r.get("player_name", "").strip()}
    curr_watch_names = set(curr_watch_idx.keys())
    prev_watch_names = set(prev_watch_idx.keys())
    watch_added = sorted(curr_watch_names - prev_watch_names)
    watch_dropped = sorted(prev_watch_names - curr_watch_names)
    watch_retained = sorted(curr_watch_names & prev_watch_names)

    watch_trend_rows = []
    for name in sorted(curr_watch_names | prev_watch_names):
        curr = curr_watch_idx.get(name, {})
        prev = prev_watch_idx.get(name, {})
        curr_board_rank = _to_int(curr_board_idx.get(name, {}).get("consensus_rank"), 9999)
        prev_board_rank = _to_int(prev_board_idx.get(name, {}).get("consensus_rank"), 9999) if prev_board_idx else 9999
        watch_trend_rows.append(
            {
                "player_name": name,
                "position": curr.get("position", prev.get("position", "")),
                "status": "retained" if name in watch_retained else "added" if name in watch_added else "dropped",
                "curr_watch_contrarian_score": curr.get("contrarian_score", ""),
                "prev_watch_contrarian_score": prev.get("contrarian_score", ""),
                "curr_board_rank": "" if curr_board_rank == 9999 else curr_board_rank,
                "prev_board_rank": "" if prev_board_rank == 9999 else prev_board_rank,
                "board_rank_change_prev_minus_curr": ""
                if curr_board_rank == 9999 or prev_board_rank == 9999
                else prev_board_rank - curr_board_rank,
            }
        )

    delta_csv = OUTPUTS / f"weekly_top50_rank_delta_{stamp}.csv"
    trend_csv = OUTPUTS / f"weekly_outlier_watchlist_trend_{stamp}.csv"
    report_txt = OUTPUTS / f"weekly_stability_check_{day_label}.txt"
    latest_txt = OUTPUTS / "weekly_stability_check_latest.txt"

    with delta_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(delta_rows[0].keys()) if delta_rows else ["player_name"])
        writer.writeheader()
        writer.writerows(delta_rows)

    with trend_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(watch_trend_rows[0].keys()) if watch_trend_rows else ["player_name"])
        writer.writeheader()
        writer.writerows(watch_trend_rows)

    lines = [
        "WEEKLY STABILITY CHECK - 2026 BOARD",
        "",
        f"Run date: {day_label}",
        f"Current board: {board_path}",
        f"Previous board baseline: {prev_board_path if prev_board_path else 'N/A'}",
        f"Current watchlist: {watchlist_path if watchlist_path.exists() else 'derived from board'}",
        f"Previous watchlist baseline: {prev_watch_path if prev_watch_path else 'derived from previous board'}",
        "",
        "Top-32 Churn",
        f"- overlap: {overlap_top32}/32",
        f"- churn_rate: {churn_rate:.3f}",
        f"- added: {', '.join(added_top32[:12]) if added_top32 else 'none'}",
        f"- dropped: {', '.join(dropped_top32[:12]) if dropped_top32 else 'none'}",
        "",
        "Top-50 Rank Delta",
        f"- players with prior rank: {len(abs_deltas)}",
        f"- mean_abs_rank_delta: {mean_abs_top50_delta:.2f}",
        f"- detail_csv: {delta_csv}",
        "",
        "Outlier Watchlist Trend",
        f"- current_count: {len(curr_watch_names)}",
        f"- previous_count: {len(prev_watch_names)}",
        f"- retained: {len(watch_retained)}",
        f"- added: {len(watch_added)}",
        f"- dropped: {len(watch_dropped)}",
        f"- trend_csv: {trend_csv}",
        "",
    ]

    report_txt.write_text("\n".join(lines))
    latest_txt.write_text("\n".join(lines))
    return report_txt, latest_txt


def main() -> None:
    parser = argparse.ArgumentParser(description="Run weekly stability checks for 2026 big board and watchlist trends.")
    parser.add_argument("--board", type=Path, default=DEFAULT_BOARD)
    parser.add_argument("--watchlist", type=Path, default=DEFAULT_WATCHLIST)
    parser.add_argument("--snapshot-dir", type=Path, default=SNAPSHOT_DIR)
    args = parser.parse_args()

    report_txt, latest_txt = run_check(args.board, args.watchlist, args.snapshot_dir)
    print(f"Wrote: {report_txt}")
    print(f"Wrote: {latest_txt}")


if __name__ == "__main__":
    main()
