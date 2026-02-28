#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MOCK_PATH = ROOT / "data" / "outputs" / "mock_2026_round1.csv"
DEFAULT_BOARD_PATH = ROOT / "data" / "outputs" / "big_board_2026.csv"
DEFAULT_TEAM_PROFILES_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"
DEFAULT_TXT_PATH = ROOT / "data" / "outputs" / "mock_2026_round1_with_scouting_notes.txt"
DEFAULT_CSV_PATH = ROOT / "data" / "outputs" / "mock_2026_round1_with_scouting_notes.csv"


def _canon_name(name: str) -> str:
    s = (name or "").lower().strip()
    s = s.replace(".", "").replace("'", "")
    s = re.sub(r"[^a-z0-9\\s-]", "", s)
    return re.sub(r"\\s+", " ", s)


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clean_sentence(text: str, fallback: str) -> str:
    s = (text or "").strip()
    if not s:
        return fallback
    if not s.endswith("."):
        s += "."
    return s


def _load_board(path: Path) -> Tuple[Dict[Tuple[str, str], dict], Dict[str, List[dict]]]:
    by_name_pos: Dict[Tuple[str, str], dict] = {}
    by_name: Dict[str, List[dict]] = {}

    with path.open() as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        row_pos = (row.get("position") or "").strip()
        key_name = _canon_name(row.get("player_name", ""))
        key = (key_name, row_pos)
        cur = by_name_pos.get(key)
        if cur is None or _to_float(row.get("consensus_rank", "9999"), 9999.0) < _to_float(
            cur.get("consensus_rank", "9999"), 9999.0
        ):
            by_name_pos[key] = row
        by_name.setdefault(key_name, []).append(row)

    for key_name, candidates in by_name.items():
        candidates.sort(key=lambda r: _to_float(r.get("consensus_rank", "9999"), 9999.0))
        by_name[key_name] = candidates

    return by_name_pos, by_name


def _load_team_profiles(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    with path.open() as f:
        return {row["team"].strip(): row for row in csv.DictReader(f)}


def _need_fit_blurb(team_profile: dict, position: str) -> str:
    if not team_profile:
        return "Team-need mapping is unavailable, so this is treated as a board-driven selection."

    need_1 = (team_profile.get("need_1") or "").strip()
    need_2 = (team_profile.get("need_2") or "").strip()
    need_3 = (team_profile.get("need_3") or "").strip()

    if position == need_1:
        return f"This directly addresses the club's top listed need at {need_1}."
    if position == need_2:
        return f"This addresses a major secondary need at {need_2}."
    if position == need_3:
        return f"This fills a meaningful tertiary need at {need_3} while preserving value."
    return "This is a value-first board play rather than a strict top-need selection."


def _build_paragraph(pick_row: dict, board_row: dict, team_profile: dict) -> str:
    player = pick_row.get("player_name", "")
    team = pick_row.get("team", "")
    position = pick_row.get("position", "")
    school = pick_row.get("school", "")
    overall_pick = pick_row.get("overall_pick", "")
    final_grade = _to_float(pick_row.get("final_grade", "0"))
    round_value = pick_row.get("round_value", "Unknown range")

    consensus_rank = board_row.get("consensus_rank", "N/A")
    consensus_mean = board_row.get("consensus_board_mean_rank", "")
    best_role = board_row.get("best_role", "role to be defined")
    best_scheme = board_row.get("best_scheme_fit", "multiple fronts")
    core_stat_name = board_row.get("core_stat_name", "Core Translation Metric")
    core_stat_value = board_row.get("core_stat_value", "N/A")
    hist_comp = (board_row.get("historical_combine_comp_1") or "").strip()
    hist_comp_year = (board_row.get("historical_combine_comp_1_year") or "").strip()
    hist_comp_similarity = (board_row.get("historical_combine_comp_1_similarity") or "").strip()

    trait_score = _to_float(board_row.get("trait_score", "0"))
    production_score = _to_float(board_row.get("production_score", "0"))
    athletic_score = _to_float(board_row.get("athletic_score", "0"))
    size_score = _to_float(board_row.get("size_score", "0"))
    context_score = _to_float(board_row.get("context_score", "0"))
    risk_penalty = _to_float(board_row.get("risk_penalty", "0"))

    wins = _clean_sentence(
        board_row.get("scouting_why_he_wins", ""),
        "His profile wins through role clarity, translatable execution details, and stable projection traits",
    )
    concerns = _clean_sentence(
        board_row.get("scouting_primary_concerns", ""),
        "The main checkpoints are consistency against top competition and expansion into a larger workload",
    )

    need_fit = _need_fit_blurb(team_profile, position)
    board_anchor = (
        f"The player sits at consensus rank {consensus_rank}"
        + (f" (multi-board mean {consensus_mean})" if consensus_mean else "")
        + "."
    )
    if hist_comp:
        hist_comp_line = f"Historical combine comp: {hist_comp}"
        if hist_comp_year:
            hist_comp_line += f" ({hist_comp_year})"
        if hist_comp_similarity:
            hist_comp_line += f", similarity {hist_comp_similarity}"
        hist_comp_line += "."
    else:
        hist_comp_line = "Historical combine comp: pending until more verified testing metrics are available."

    return (
        f"Pick {overall_pick}: {team} selects {player} ({position}, {school}). "
        f"Our model grades him at {final_grade:.2f} with a {round_value} projection and projects his best early NFL path as "
        f"'{best_role}' in a {best_scheme} environment. {board_anchor} "
        f"{wins} "
        f"His scoring profile is trait {trait_score:.1f}, production {production_score:.1f}, athletic {athletic_score:.1f}, "
        f"size {size_score:.1f}, and context {context_score:.1f}, with a risk penalty of {risk_penalty:.1f}. "
        f"Unique differentiator: {core_stat_name} ({core_stat_value}). "
        f"{hist_comp_line} "
        f"{need_fit} "
        f"{concerns}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Render Round 1 mock with one-paragraph scouting notes for each pick.")
    parser.add_argument("--mock", type=Path, default=DEFAULT_MOCK_PATH)
    parser.add_argument("--board", type=Path, default=DEFAULT_BOARD_PATH)
    parser.add_argument("--team-profiles", type=Path, default=DEFAULT_TEAM_PROFILES_PATH)
    parser.add_argument("--out-txt", type=Path, default=DEFAULT_TXT_PATH)
    parser.add_argument("--out-csv", type=Path, default=DEFAULT_CSV_PATH)
    args = parser.parse_args()

    with args.mock.open() as f:
        mock_rows = list(csv.DictReader(f))

    board_by_name_pos, board_by_name = _load_board(args.board)
    team_profiles = _load_team_profiles(args.team_profiles)

    enriched_rows: List[dict] = []
    paragraphs: List[str] = []

    for row in mock_rows:
        key = (_canon_name(row.get("player_name", "")), (row.get("position") or "").strip())
        board_row = board_by_name_pos.get(key)
        if board_row is None:
            candidates = board_by_name.get(key[0], [])
            board_row = candidates[0] if candidates else {}

        paragraph = _build_paragraph(row, board_row, team_profiles.get(row.get("team", ""), {}))
        out_row = dict(row)
        out_row["scouting_note_paragraph"] = paragraph
        out_row["core_stat_name"] = board_row.get("core_stat_name", "")
        out_row["core_stat_value"] = board_row.get("core_stat_value", "")
        out_row["best_role"] = board_row.get("best_role", "")
        out_row["best_scheme_fit"] = board_row.get("best_scheme_fit", "")
        out_row["consensus_rank"] = board_row.get("consensus_rank", "")
        out_row["historical_combine_comp_1"] = board_row.get("historical_combine_comp_1", "")
        out_row["historical_combine_comp_1_year"] = board_row.get("historical_combine_comp_1_year", "")
        out_row["historical_combine_comp_1_similarity"] = board_row.get("historical_combine_comp_1_similarity", "")
        enriched_rows.append(out_row)

        paragraphs.append(paragraph)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_txt.parent.mkdir(parents=True, exist_ok=True)

    csv_columns = list(enriched_rows[0].keys()) if enriched_rows else []
    with args.out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(enriched_rows)

    lines = ["2026 MOCK DRAFT - ROUND 1 (PARAGRAPH SCOUTING NOTES)", ""]
    for i, row in enumerate(enriched_rows, start=1):
        lines.append(
            f"Pick {row.get('pick')} ({row.get('team')}): {row.get('player_name')} - "
            f"{row.get('position')}, {row.get('school')} | Grade {row.get('final_grade')} ({row.get('round_value')})"
        )
        lines.append(row.get("scouting_note_paragraph", ""))
        lines.append("")

    args.out_txt.write_text("\n".join(lines).strip() + "\n")

    print(f"Wrote: {args.out_csv}")
    print(f"Wrote: {args.out_txt}")
    print(f"Rows: {len(enriched_rows)}")


if __name__ == "__main__":
    main()
