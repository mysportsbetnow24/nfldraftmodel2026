#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "data" / "outputs"
ASTRO_DATA = ROOT / "astro-site" / "src" / "data"

BOARD_CSV = OUTPUTS / "big_board_2026.csv"
ROUND1_CSV = OUTPUTS / "mock_2026_round1.csv"
ROUND7_CSV = OUTPUTS / "mock_2026_7round.csv"


def _safe_float(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_int(value, default: int = 0) -> int:
    val = _safe_float(value)
    if val is None:
        return default
    return int(round(val))


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def export_board() -> list[dict]:
    rows = _read_csv(BOARD_CSV)
    out = []
    for row in rows:
        slug = (
            (row.get("player_name", "") or "")
            .lower()
            .replace(" ", "-")
            .replace(".", "")
            .replace("'", "")
        )
        out.append(
            {
                "player_uid": row.get("player_uid", ""),
                "consensus_rank": _safe_int(row.get("consensus_rank"), 9999),
                "player_name": row.get("player_name", ""),
                "position": row.get("position", ""),
                "school": row.get("school", ""),
                "final_grade": round(_safe_float(row.get("final_grade")) or 0.0, 2),
                "round_value": row.get("round_value", ""),
                "consensus_board_mean_rank": row.get("consensus_board_mean_rank", ""),
                "rank_driver_summary": row.get("rank_driver_summary", ""),
                "scouting_report_summary": row.get("scouting_report_summary", ""),
                "scouting_why_he_wins": row.get("scouting_why_he_wins", ""),
                "scouting_primary_concerns": row.get("scouting_primary_concerns", ""),
                "scouting_production_snapshot": row.get("scouting_production_snapshot", ""),
                "scouting_role_projection": row.get("scouting_role_projection", ""),
                "player_report_url": f"/player_reports_html/{slug}.html",
            }
        )
    out.sort(key=lambda r: r["consensus_rank"])
    return out


def export_mock(path: Path) -> list[dict]:
    rows = _read_csv(path)
    out = []
    for row in rows:
        out.append(
            {
                "round": _safe_int(row.get("round"), 0),
                "pick": _safe_int(row.get("pick"), 0),
                "overall_pick": _safe_int(row.get("overall_pick"), 0),
                "team": row.get("team", ""),
                "player_uid": row.get("player_uid", ""),
                "player_name": row.get("player_name", ""),
                "position": row.get("position", ""),
                "school": row.get("school", ""),
                "final_grade": round(_safe_float(row.get("final_grade")) or 0.0, 2),
                "round_value": row.get("round_value", ""),
                "pick_score": round(_safe_float(row.get("pick_score")) or 0.0, 4),
                "rank_driver_summary": row.get("rank_driver_summary", ""),
            }
        )
    out.sort(key=lambda r: r["overall_pick"])
    return out


def export_round7_team_groups(round7_rows: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for row in round7_rows:
        team = row.get("team", "")
        if not team:
            continue
        groups.setdefault(team, []).append(row)
    for team in list(groups.keys()):
        groups[team] = sorted(groups[team], key=lambda r: r.get("overall_pick", 9999))
    return groups


def main() -> None:
    board = export_board()
    round1 = export_mock(ROUND1_CSV)
    round7 = export_mock(ROUND7_CSV)
    by_team = export_round7_team_groups(round7)

    _write_json(ASTRO_DATA / "big_board_2026.json", board)
    _write_json(ASTRO_DATA / "mock_2026_round1.json", round1)
    _write_json(ASTRO_DATA / "mock_2026_7round.json", round7)
    _write_json(ASTRO_DATA / "mock_2026_7round_by_team.json", by_team)
    _write_json(
        ASTRO_DATA / "build_meta.json",
        {"generated_at": datetime.now(timezone.utc).isoformat(), "rows": {"board": len(board), "round1": len(round1), "round7": len(round7)}},
    )

    print(f"Wrote {ASTRO_DATA / 'big_board_2026.json'} ({len(board)} rows)")
    print(f"Wrote {ASTRO_DATA / 'mock_2026_round1.json'} ({len(round1)} rows)")
    print(f"Wrote {ASTRO_DATA / 'mock_2026_7round.json'} ({len(round7)} rows)")
    print(f"Wrote {ASTRO_DATA / 'mock_2026_7round_by_team.json'} ({len(by_team)} teams)")
    print(f"Wrote {ASTRO_DATA / 'build_meta.json'}")


if __name__ == "__main__":
    main()
