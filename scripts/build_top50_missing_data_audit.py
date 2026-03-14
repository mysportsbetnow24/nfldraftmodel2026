#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BOARD_PATH = ROOT / "data" / "processed" / "big_board_2026.csv"
CSV_OUT = ROOT / "data" / "outputs" / "top50_missing_data_audit_2026.csv"
MD_OUT = ROOT / "data" / "outputs" / "top50_missing_data_audit_2026.md"


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
    print(f"Wrote {CSV_OUT}")
    print(f"Wrote {MD_OUT}")


if __name__ == "__main__":
    main()
