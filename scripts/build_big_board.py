#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import analyst_aggregate_score, load_analyst_rows, normalize_pos
from src.modeling.comp_model import assign_comp
from src.modeling.grading import grade_player, scouting_note
from src.modeling.team_fit import best_team_fit
from src.schemas import parse_height_to_inches

PROCESSED = ROOT / "data" / "processed"
OUTPUTS = ROOT / "data" / "outputs"



def read_seed(path: Path) -> list[dict]:
    with path.open() as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["rank_seed"] = int(row["rank_seed"])
        row["weight_lb"] = int(row["weight_lb"])
        row["seed_row_id"] = int(row["seed_row_id"])
    return rows



def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)



def write_top_board_md(path: Path, rows: list[dict], limit: int = 100) -> None:
    lines = ["# 2026 Big Board (Top 100)", "", "| Rank | Player | Pos | School | Grade | Round | Best Team Fit |", "|---:|---|---|---|---:|---|---|"]
    for row in rows[:limit]:
        lines.append(
            f"| {row['consensus_rank']} | {row['player_name']} | {row['position']} | {row['school']} | {row['final_grade']} | {row['round_value']} | {row['best_team_fit']} |"
        )
    path.write_text("\n".join(lines))



def main() -> None:
    seed = read_seed(PROCESSED / "prospect_seed_2026.csv")
    analyst_rows = load_analyst_rows()
    analyst_scores = analyst_aggregate_score(analyst_rows)

    enriched = []
    for row in seed:
        pos = normalize_pos(row["pos_raw"])
        height_in = parse_height_to_inches(row["height"]) or 74

        grades = grade_player(
            position=pos,
            rank_seed=row["rank_seed"],
            class_year=row["class_year"],
            height_in=height_in,
            weight_lb=row["weight_lb"],
        )

        analyst_score = analyst_scores.get(row["player_name"], 35.0)
        consensus_score = 0.70 * (301 - row["rank_seed"]) + 0.30 * analyst_score

        fit_team, fit_score = best_team_fit(pos)
        comp = assign_comp(pos, row["rank_seed"])

        report = {
            **row,
            "player_uid": f"{row['seed_row_id']}-{row['player_name'].lower().replace(' ', '-')}",
            "position": pos,
            "height_in": height_in,
            "analyst_signal": round(analyst_score, 2),
            "consensus_score": round(consensus_score, 2),
            **grades,
            "best_team_fit": fit_team,
            "best_team_fit_score": fit_score,
            **comp,
            "scouting_notes": scouting_note(pos, grades["final_grade"], row["rank_seed"]),
            "headshot_url": "",
        }
        enriched.append(report)

    enriched.sort(key=lambda x: x["consensus_score"], reverse=True)
    for i, row in enumerate(enriched, start=1):
        row["consensus_rank"] = i

    write_csv(PROCESSED / "big_board_2026.csv", enriched)
    write_csv(OUTPUTS / "big_board_2026.csv", enriched)
    write_top_board_md(OUTPUTS / "big_board_2026_top100.md", enriched, 100)

    with (OUTPUTS / "big_board_2026.json").open("w") as f:
        json.dump(enriched, f, indent=2)

    print(f"Board rows: {len(enriched)}")


if __name__ == "__main__":
    main()
