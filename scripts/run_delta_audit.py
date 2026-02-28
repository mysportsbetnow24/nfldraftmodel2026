#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name


OUTPUTS = ROOT / "data" / "outputs"
SNAPSHOTS = OUTPUTS / "stability_snapshots"
DEFAULT_CURRENT = OUTPUTS / "big_board_2026.csv"

GUARDRAIL_FIELDS = [
    "formula_guardrail_penalty",
    "formula_drift_penalty",
    "formula_midband_brake_penalty",
    "formula_soft_ceiling_penalty",
    "formula_consensus_tail_soft_penalty",
    "formula_top75_gate_penalty",
    "formula_hard_cap_penalty",
    "formula_front7_inflation_penalty",
    "formula_cb_nickel_inflation_penalty",
    "formula_evidence_guardrail_penalty",
]


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _latest_previous_snapshot(snapshot_dir: Path) -> Path | None:
    snaps = sorted(snapshot_dir.glob("big_board_2026_snapshot_*.csv"))
    if len(snaps) >= 2:
        return snaps[-2]
    return None


def _guardrail_total(row: dict) -> float:
    return sum(_to_float(row.get(col), 0.0) for col in GUARDRAIL_FIELDS)


def _top_reason(delta_ath: float, delta_prod: float, delta_prior: float, guardrail_relief: float) -> str:
    parts = {
        "athletic": delta_ath,
        "production": delta_prod,
        "prior": delta_prior,
        "guardrail_relief": guardrail_relief,
    }
    top_key = max(parts.keys(), key=lambda k: abs(parts[k]))
    val = parts[top_key]
    direction = "up" if val >= 0 else "down"
    return f"{top_key} ({direction} {abs(val):.2f})"


def run_audit(current_path: Path, previous_path: Path, top_n: int = 25) -> tuple[Path, Path]:
    curr_rows = _read_rows(current_path)
    prev_rows = _read_rows(previous_path)
    if not curr_rows:
        raise RuntimeError(f"Current board not found or empty: {current_path}")
    if not prev_rows:
        raise RuntimeError(f"Previous board not found or empty: {previous_path}")

    prev_idx = {canonical_player_name(r.get("player_name", "")): r for r in prev_rows if r.get("player_name", "").strip()}
    deltas: list[dict] = []

    for curr in curr_rows:
        name = curr.get("player_name", "").strip()
        key = canonical_player_name(name)
        if not key or key not in prev_idx:
            continue
        prev = prev_idx[key]
        curr_rank = _to_int(curr.get("consensus_rank"), 9999)
        prev_rank = _to_int(prev.get("consensus_rank"), 9999)
        if curr_rank == 9999 or prev_rank == 9999:
            continue

        rank_delta = prev_rank - curr_rank
        grade_delta = _to_float(curr.get("final_grade")) - _to_float(prev.get("final_grade"))
        delta_ath = _to_float(curr.get("formula_athletic_component")) - _to_float(prev.get("formula_athletic_component"))
        delta_prod = _to_float(curr.get("formula_production_component")) - _to_float(prev.get("formula_production_component"))
        delta_prior = _to_float(curr.get("formula_prior_signal")) - _to_float(prev.get("formula_prior_signal"))
        guardrail_relief = _guardrail_total(prev) - _guardrail_total(curr)

        deltas.append(
            {
                "player_name": name,
                "position": curr.get("position", ""),
                "school": curr.get("school", ""),
                "prev_rank": prev_rank,
                "curr_rank": curr_rank,
                "rank_delta_prev_minus_curr": rank_delta,
                "prev_grade": round(_to_float(prev.get("final_grade")), 2),
                "curr_grade": round(_to_float(curr.get("final_grade")), 2),
                "grade_delta": round(grade_delta, 2),
                "athletic_delta": round(delta_ath, 2),
                "production_delta": round(delta_prod, 2),
                "prior_delta": round(delta_prior, 2),
                "guardrail_relief_delta": round(guardrail_relief, 2),
                "top_driver": _top_reason(delta_ath, delta_prod, delta_prior, guardrail_relief),
            }
        )

    deltas.sort(key=lambda r: _to_int(r["rank_delta_prev_minus_curr"]), reverse=True)
    risers = [r for r in deltas if _to_int(r["rank_delta_prev_minus_curr"]) > 0][:top_n]
    fallers = sorted(
        [r for r in deltas if _to_int(r["rank_delta_prev_minus_curr"]) < 0],
        key=lambda r: _to_int(r["rank_delta_prev_minus_curr"]),
    )[:top_n]

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_out = OUTPUTS / f"delta_audit_2026_{stamp}.csv"
    txt_out = OUTPUTS / f"delta_audit_2026_{stamp}.txt"
    latest_csv = OUTPUTS / "delta_audit_2026_latest.csv"
    latest_txt = OUTPUTS / "delta_audit_2026_latest.txt"

    if deltas:
        with csv_out.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(deltas[0].keys()))
            writer.writeheader()
            writer.writerows(deltas)
        with latest_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(deltas[0].keys()))
            writer.writeheader()
            writer.writerows(deltas)

    lines = [
        "2026 DELTA AUDIT",
        "",
        f"Current board: {current_path}",
        f"Previous board: {previous_path}",
        f"Rows compared: {len(deltas)}",
        "",
        "Top Risers (prev rank - curr rank > 0)",
    ]
    for idx, row in enumerate(risers, start=1):
        lines.append(
            f"{idx}. {row['player_name']} ({row['position']}) {row['prev_rank']} -> {row['curr_rank']} "
            f"(+{row['rank_delta_prev_minus_curr']}); driver: {row['top_driver']} "
            f"[ath {row['athletic_delta']}, prod {row['production_delta']}, prior {row['prior_delta']}, guardrail {row['guardrail_relief_delta']}]"
        )
    lines.extend(["", "Top Fallers (prev rank - curr rank < 0)"])
    for idx, row in enumerate(fallers, start=1):
        lines.append(
            f"{idx}. {row['player_name']} ({row['position']}) {row['prev_rank']} -> {row['curr_rank']} "
            f"({row['rank_delta_prev_minus_curr']}); driver: {row['top_driver']} "
            f"[ath {row['athletic_delta']}, prod {row['production_delta']}, prior {row['prior_delta']}, guardrail {row['guardrail_relief_delta']}]"
        )

    txt_out.write_text("\n".join(lines))
    latest_txt.write_text("\n".join(lines))
    return txt_out, csv_out


def main() -> None:
    parser = argparse.ArgumentParser(description="Run rank delta audit and explain movers.")
    parser.add_argument("--current", type=Path, default=DEFAULT_CURRENT)
    parser.add_argument("--previous", type=Path, default=None)
    parser.add_argument("--top", type=int, default=25)
    args = parser.parse_args()

    previous = args.previous or _latest_previous_snapshot(SNAPSHOTS)
    if previous is None:
        raise RuntimeError(
            "No previous snapshot found. Run weekly stability snapshot first or pass --previous explicitly."
        )

    txt_out, csv_out = run_audit(args.current, previous, top_n=max(1, args.top))
    print(f"Report: {txt_out}")
    print(f"Rows: {csv_out}")


if __name__ == "__main__":
    main()
