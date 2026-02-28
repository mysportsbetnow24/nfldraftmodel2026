#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.simulation.mock_draft import load_board, simulate_full_draft, write_csv


OUT = ROOT / "data" / "outputs"


def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(str(value or "").strip())
    except ValueError:
        return default


def _key_pick(row: dict) -> tuple[int, int]:
    return int(row.get("round", 0) or 0), int(row.get("pick", 0) or 0)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    seen = set(fieldnames)
    for row in rows[1:]:
        for k in row.keys():
            if k not in seen:
                fieldnames.append(k)
                seen.add(k)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _movement_rows(round1_off: list[dict], round1_on: list[dict]) -> list[dict]:
    off_by_player = {str(r["player_name"]): r for r in round1_off}
    on_by_player = {str(r["player_name"]): r for r in round1_on}
    players = sorted(set(off_by_player.keys()) | set(on_by_player.keys()))

    rows: list[dict] = []
    for name in players:
        off = off_by_player.get(name)
        on = on_by_player.get(name)
        off_pick = int(off["overall_pick"]) if off else None
        on_pick = int(on["overall_pick"]) if on else None
        if off_pick is None:
            movement = "entered_round1"
            pick_delta = ""
        elif on_pick is None:
            movement = "exited_round1"
            pick_delta = ""
        else:
            delta = off_pick - on_pick  # positive => moved up
            pick_delta = delta
            if delta > 0:
                movement = f"up_{delta}"
            elif delta < 0:
                movement = f"down_{abs(delta)}"
            else:
                movement = "no_change"

        rows.append(
            {
                "player_name": name,
                "position": (on or off).get("position", ""),
                "team_off": off.get("team", "") if off else "",
                "overall_pick_off": off_pick if off_pick is not None else "",
                "team_on": on.get("team", "") if on else "",
                "overall_pick_on": on_pick if on_pick is not None else "",
                "pick_delta_prev_minus_curr": pick_delta,
                "movement": movement,
            }
        )
    rows.sort(
        key=lambda r: (
            0 if str(r.get("movement")) not in {"entered_round1", "exited_round1"} else 1,
            -abs(int(r["pick_delta_prev_minus_curr"])) if str(r.get("pick_delta_prev_minus_curr")).strip() else -999,
        ),
    )
    return rows


def _impact_by_team_position(round1_on: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], dict] = {}
    for row in round1_on:
        key = (str(row.get("team", "")), str(row.get("position", "")))
        g = grouped.get(key)
        if g is None:
            g = {
                "team": key[0],
                "position": key[1],
                "picks": 0,
                "modifier_sum": 0.0,
                "abs_modifier_sum": 0.0,
                "confidence_sum": 0.0,
                "threshold_mode_position_count": 0,
                "threshold_mode_bucket_count": 0,
            }
            grouped[key] = g

        mod = _as_float(row.get("team_athletic_fit_modifier"), 0.0)
        conf = _as_float(row.get("team_athletic_threshold_confidence"), 0.0)
        mode = str(row.get("team_athletic_threshold_mode", "")).strip()
        g["picks"] += 1
        g["modifier_sum"] += mod
        g["abs_modifier_sum"] += abs(mod)
        g["confidence_sum"] += conf
        if mode == "position":
            g["threshold_mode_position_count"] += 1
        elif mode == "bucket":
            g["threshold_mode_bucket_count"] += 1

    rows: list[dict] = []
    for g in grouped.values():
        picks = max(1, int(g["picks"]))
        rows.append(
            {
                "team": g["team"],
                "position": g["position"],
                "picks": picks,
                "avg_modifier": round(g["modifier_sum"] / picks, 4),
                "avg_abs_modifier": round(g["abs_modifier_sum"] / picks, 4),
                "avg_threshold_confidence": round(g["confidence_sum"] / picks, 4),
                "threshold_mode_position_count": g["threshold_mode_position_count"],
                "threshold_mode_bucket_count": g["threshold_mode_bucket_count"],
            }
        )
    rows.sort(key=lambda r: abs(_as_float(r["avg_modifier"])), reverse=True)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run no-bias vs athletic-bias mock delta audit (Round 1 + impact summary)."
    )
    parser.add_argument(
        "--allow-simulated-trades",
        action="store_true",
        help="Enable synthetic trade-down heuristics in both runs.",
    )
    args = parser.parse_args()

    board = load_board()

    round1_off, full7_off, _ = simulate_full_draft(
        board,
        rounds=7,
        allow_simulated_trades=args.allow_simulated_trades,
        enable_team_athletic_bias=False,
    )
    round1_on, full7_on, _ = simulate_full_draft(
        board,
        rounds=7,
        allow_simulated_trades=args.allow_simulated_trades,
        enable_team_athletic_bias=True,
    )

    # Persist both runs for side-by-side inspection.
    write_csv(OUT / "mock_2026_round1_bias_off.csv", round1_off)
    write_csv(OUT / "mock_2026_round1_bias_on.csv", round1_on)
    write_csv(OUT / "mock_2026_7round_bias_off.csv", full7_off)
    write_csv(OUT / "mock_2026_7round_bias_on.csv", full7_on)

    # Also mirror current outputs to bias_on so normal pipeline files reflect the active setting.
    shutil.copyfile(OUT / "mock_2026_round1_bias_on.csv", OUT / "mock_2026_round1.csv")
    shutil.copyfile(OUT / "mock_2026_7round_bias_on.csv", OUT / "mock_2026_7round.csv")

    off_by_pick = {_key_pick(r): r for r in round1_off}
    on_by_pick = {_key_pick(r): r for r in round1_on}
    delta_rows: list[dict] = []
    changed = 0
    for key in sorted(set(off_by_pick.keys()) | set(on_by_pick.keys())):
        off = off_by_pick.get(key, {})
        on = on_by_pick.get(key, {})
        changed_pick = str(off.get("player_name", "")) != str(on.get("player_name", ""))
        if changed_pick:
            changed += 1
        delta_rows.append(
            {
                "round": key[0],
                "pick": key[1],
                "overall_pick_off": off.get("overall_pick", ""),
                "team_off": off.get("team", ""),
                "player_off": off.get("player_name", ""),
                "position_off": off.get("position", ""),
                "overall_pick_on": on.get("overall_pick", ""),
                "team_on": on.get("team", ""),
                "player_on": on.get("player_name", ""),
                "position_on": on.get("position", ""),
                "changed": int(changed_pick),
                "athletic_modifier_on": on.get("team_athletic_fit_modifier", ""),
                "athletic_target_ras_on": on.get("team_athletic_target_ras", ""),
                "athletic_proxy_on": on.get("player_athletic_proxy", ""),
                "athletic_source_on": on.get("player_athletic_source", ""),
                "athletic_tier_on": on.get("team_athletic_tier", ""),
                "athletic_threshold_mode_on": on.get("team_athletic_threshold_mode", ""),
                "athletic_threshold_confidence_on": on.get("team_athletic_threshold_confidence", ""),
                "athletic_reason_on": on.get("team_athletic_bias_reason", ""),
            }
        )

    movement_rows = _movement_rows(round1_off, round1_on)
    impact_rows = _impact_by_team_position(round1_on)

    _write_csv(OUT / "mock_2026_round1_athletic_bias_delta.csv", delta_rows)
    _write_csv(OUT / "mock_2026_round1_athletic_bias_movement.csv", movement_rows)
    _write_csv(OUT / "mock_2026_round1_athletic_bias_impact_by_team_position.csv", impact_rows)

    movers_up = [r for r in movement_rows if str(r["movement"]).startswith("up_")]
    movers_down = [r for r in movement_rows if str(r["movement"]).startswith("down_")]
    entered = [r for r in movement_rows if r["movement"] == "entered_round1"]
    exited = [r for r in movement_rows if r["movement"] == "exited_round1"]

    lines = [
        "2026 Mock Athletic-Bias Delta Audit",
        "",
        f"Round 1 changed picks: {changed} / {len(delta_rows)}",
        f"Players moved up: {len(movers_up)}",
        f"Players moved down: {len(movers_down)}",
        f"Players entered Round 1: {len(entered)}",
        f"Players exited Round 1: {len(exited)}",
        "",
        "Top Upward Movers (pick delta prev-minus-curr):",
    ]
    for row in movers_up[:10]:
        lines.append(
            f"- {row['player_name']} ({row['position']}): {row['overall_pick_off']} -> {row['overall_pick_on']} [{row['movement']}]"
        )

    lines.extend(["", "Top Downward Movers (pick delta prev-minus-curr):"])
    for row in movers_down[:10]:
        lines.append(
            f"- {row['player_name']} ({row['position']}): {row['overall_pick_off']} -> {row['overall_pick_on']} [{row['movement']}]"
        )

    lines.extend(["", "Top Team/Position Athletic Impact (avg modifier):"])
    for row in impact_rows[:12]:
        lines.append(
            f"- {row['team']} {row['position']}: avg_mod {row['avg_modifier']}, "
            f"avg_abs_mod {row['avg_abs_modifier']}, avg_conf {row['avg_threshold_confidence']}"
        )

    (OUT / "mock_2026_round1_athletic_bias_delta.txt").write_text("\n".join(lines))

    print(f"Wrote: {OUT / 'mock_2026_round1_bias_off.csv'}")
    print(f"Wrote: {OUT / 'mock_2026_round1_bias_on.csv'}")
    print(f"Wrote: {OUT / 'mock_2026_round1_athletic_bias_delta.csv'}")
    print(f"Wrote: {OUT / 'mock_2026_round1_athletic_bias_movement.csv'}")
    print(f"Wrote: {OUT / 'mock_2026_round1_athletic_bias_impact_by_team_position.csv'}")
    print(f"Wrote: {OUT / 'mock_2026_round1_athletic_bias_delta.txt'}")
    print(f"Changed picks: {changed}/{len(delta_rows)}")


if __name__ == "__main__":
    main()
