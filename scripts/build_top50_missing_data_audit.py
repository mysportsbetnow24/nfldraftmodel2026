#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BOARD_PATH = ROOT / "data" / "processed" / "big_board_2026.csv"
CSV_OUT = ROOT / "data" / "outputs" / "top50_missing_data_audit_2026.csv"
MD_OUT = ROOT / "data" / "outputs" / "top50_missing_data_audit_2026.md"
QUEUE_CSV_OUT = ROOT / "data" / "outputs" / "top50_tuning_queue_2026.csv"
QUEUE_MD_OUT = ROOT / "data" / "outputs" / "top50_tuning_queue_2026.md"


def main() -> None:
    df = pd.read_csv(BOARD_PATH)
    board = df.sort_values(["consensus_rank", "final_grade"], ascending=[True, False]).head(50).copy()

    keep_cols = [
        "consensus_rank",
        "player_name",
        "position",
        "school",
        "final_grade",
        "consensus_board_mean_rank",
        "athletic_metric_missing_count",
        "athletic_metric_coverage_rate",
        "film_trait_coverage",
        "cfb_prod_reliability",
        "ras_score",
        "forty_yard_dash",
        "vertical_jump",
        "broad_jump",
        "short_shuttle",
        "three_cone",
        "bench_press",
        "arm_length_inches",
        "hand_size_inches",
        "wingspan_inches",
        "best_team_fit",
    ]
    keep_cols = [c for c in keep_cols if c in board.columns]

    def audit_flags(row: pd.Series) -> str:
        flags: list[str] = []
        if float(row.get("athletic_metric_coverage_rate", 0.0) or 0.0) < 0.50:
            flags.append("athletic_coverage_thin")
        if int(row.get("athletic_metric_missing_count", 0) or 0) >= 6:
            flags.append("many_athletic_blanks")
        if float(row.get("film_trait_coverage", 0.0) or 0.0) <= 0.05:
            flags.append("film_traits_missing")
        if float(row.get("cfb_prod_reliability", 0.0) or 0.0) < 0.70:
            flags.append("production_signal_light")
        return "; ".join(flags)

    board["audit_flags"] = board.apply(audit_flags, axis=1)
    board["priority_score"] = (
        (1.0 - board.get("athletic_metric_coverage_rate", 0.0).fillna(0.0)) * 2.0
        + board.get("athletic_metric_missing_count", 0).fillna(0.0) * 0.15
        + (1.0 - board.get("film_trait_coverage", 0.0).fillna(0.0)) * 1.75
        + (1.0 - board.get("cfb_prod_reliability", 0.0).fillna(0.0)) * 0.75
    ).round(3)
    board = board.sort_values(["priority_score", "consensus_rank"], ascending=[False, True])

    market_mean = pd.to_numeric(board.get("consensus_board_mean_rank"), errors="coerce")
    board_rank = pd.to_numeric(board.get("consensus_rank"), errors="coerce")
    draft_age_available = pd.to_numeric(board.get("draft_age_available", 0), errors="coerce").fillna(0.0)

    board["market_gap"] = (market_mean - board_rank).round(2)
    board["market_gap_abs"] = (market_mean - board_rank).abs().fillna(0.0).round(2)
    board["top_board_weight"] = (1.0 / board_rank.clip(lower=1.0)).round(4)
    board["draft_age_missing"] = (draft_age_available < 0.5).astype(int)
    board["testing_action_needed"] = (
        (pd.to_numeric(board.get("athletic_metric_coverage_rate", 0.0), errors="coerce").fillna(0.0) < 0.60)
        | (pd.to_numeric(board.get("athletic_metric_missing_count", 0), errors="coerce").fillna(0.0) >= 5)
    ).astype(int)
    board["film_action_needed"] = (
        pd.to_numeric(board.get("film_trait_coverage", 0.0), errors="coerce").fillna(0.0) <= 0.05
    ).astype(int)
    board["production_action_needed"] = (
        pd.to_numeric(board.get("cfb_prod_reliability", 0.0), errors="coerce").fillna(0.0) < 0.70
    ).astype(int)
    board["disagreement_action_needed"] = (board["market_gap_abs"] >= 10.0).astype(int)

    def recommended_actions(row: pd.Series) -> str:
        actions: list[str] = []
        if int(row.get("testing_action_needed", 0) or 0):
            actions.append("fill testing")
        if int(row.get("film_action_needed", 0) or 0):
            actions.append("add film traits")
        if int(row.get("production_action_needed", 0) or 0):
            actions.append("verify production context")
        if int(row.get("draft_age_missing", 0) or 0):
            actions.append("add age data")
        if int(row.get("disagreement_action_needed", 0) or 0):
            actions.append("audit model-vs-market gap")
        return "; ".join(actions)

    def tuning_lane(row: pd.Series) -> str:
        actions = recommended_actions(row)
        if "add film traits" in actions and "audit model-vs-market gap" in actions:
            return "film + disagreement"
        if "fill testing" in actions and "add film traits" in actions:
            return "testing + film"
        if "add film traits" in actions:
            return "film-first"
        if "fill testing" in actions:
            return "testing-first"
        if "audit model-vs-market gap" in actions:
            return "disagreement review"
        return "maintenance"

    board["recommended_actions"] = board.apply(recommended_actions, axis=1)
    board["tuning_lane"] = board.apply(tuning_lane, axis=1)
    board["tuning_priority_score"] = (
        board["priority_score"]
        + board["market_gap_abs"].fillna(0.0) * 0.08
        + (1.0 - board["top_board_weight"].rank(pct=True)) * 0.75
        + board["draft_age_missing"] * 0.25
    ).round(3)
    tuning_queue = board.sort_values(
        ["tuning_priority_score", "priority_score", "consensus_rank"],
        ascending=[False, False, True],
    ).copy()
    tuning_queue["queue_rank"] = range(1, len(tuning_queue) + 1)

    CSV_OUT.parent.mkdir(parents=True, exist_ok=True)
    board[[c for c in keep_cols + ["audit_flags", "priority_score"] if c in board.columns]].to_csv(CSV_OUT, index=False, quoting=csv.QUOTE_MINIMAL)

    lines = [
        "# Top-50 Missing Data Audit",
        "",
        f"- source: `{BOARD_PATH}`",
        f"- rows: `{len(board)}`",
        "",
        "## Highest-Priority Cleanup Targets",
        "",
        "| Rank | Player | Pos | School | Priority | Flags | Athletic Coverage | Film Coverage | Prod Reliability |",
        "|---:|---|---|---|---:|---|---:|---:|---:|",
    ]
    for _, row in board.head(25).iterrows():
        lines.append(
            f"| {int(row.get('consensus_rank', 0))} | {row.get('player_name', '')} | {row.get('position', '')} | "
            f"{row.get('school', '')} | {float(row.get('priority_score', 0.0)):.3f} | {row.get('audit_flags', '')} | "
            f"{float(row.get('athletic_metric_coverage_rate', 0.0)):.3f} | {float(row.get('film_trait_coverage', 0.0)):.3f} | "
            f"{float(row.get('cfb_prod_reliability', 0.0)):.3f} |"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `athletic_coverage_thin`: fewer than half of the tracked athletic fields are populated.",
            "- `many_athletic_blanks`: six or more athletic measurement fields are still empty.",
            "- `film_traits_missing`: structured film-trait coverage is effectively missing.",
            "- `production_signal_light`: current production reliability is below the strong-confidence band.",
            "",
            f"Full CSV: `{CSV_OUT}`",
        ]
    )
    MD_OUT.write_text("\n".join(lines))

    queue_cols = [
        "queue_rank",
        "consensus_rank",
        "player_name",
        "position",
        "school",
        "final_grade",
        "consensus_board_mean_rank",
        "market_gap",
        "priority_score",
        "tuning_priority_score",
        "tuning_lane",
        "recommended_actions",
        "audit_flags",
        "athletic_metric_coverage_rate",
        "film_trait_coverage",
        "cfb_prod_reliability",
        "best_team_fit",
    ]
    tuning_queue[[c for c in queue_cols if c in tuning_queue.columns]].to_csv(
        QUEUE_CSV_OUT, index=False, quoting=csv.QUOTE_MINIMAL
    )

    queue_lines = [
        "# Top-50 Tuning Queue",
        "",
        f"- source: `{BOARD_PATH}`",
        f"- rows: `{len(tuning_queue)}`",
        "",
        "## Highest-Leverage Tuning Targets",
        "",
        "| Queue | Rank | Player | Pos | Priority | Market Gap | Lane | Recommended Actions | Current Fit |",
        "|---:|---:|---|---|---:|---:|---|---|---|",
    ]
    for _, row in tuning_queue.head(25).iterrows():
        queue_lines.append(
            f"| {int(row.get('queue_rank', 0))} | {int(row.get('consensus_rank', 0))} | {row.get('player_name', '')} | "
            f"{row.get('position', '')} | {float(row.get('tuning_priority_score', 0.0)):.3f} | "
            f"{float(row.get('market_gap', 0.0)):+.2f} | {row.get('tuning_lane', '')} | "
            f"{row.get('recommended_actions', '')} | {row.get('best_team_fit', '')} |"
        )
    queue_lines.extend(
        [
            "",
            "## Lane Definitions",
            "",
            "- `testing + film`: player needs both verified athletic coverage and structured film traits before the rank should harden.",
            "- `film + disagreement`: player is thin on film coverage and also far from market consensus; strongest candidate for focused review.",
            "- `film-first`: athletic and production coverage are usable, but the profile still lacks structured scouting traits.",
            "- `testing-first`: most important next step is verified combine/pro-day coverage.",
            "- `disagreement review`: coverage is serviceable, but the model is taking a strong stand versus the market.",
            "",
            f"Full CSV: `{QUEUE_CSV_OUT}`",
        ]
    )
    QUEUE_MD_OUT.write_text("\n".join(queue_lines))
    print(f"Wrote {CSV_OUT}")
    print(f"Wrote {MD_OUT}")
    print(f"Wrote {QUEUE_CSV_OUT}")
    print(f"Wrote {QUEUE_MD_OUT}")


if __name__ == "__main__":
    main()
