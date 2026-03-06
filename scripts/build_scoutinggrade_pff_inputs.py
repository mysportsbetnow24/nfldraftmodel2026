#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[1]
DOWNLOADS = Path("/Users/nickholz/Downloads")
MANUAL_DIR = ROOT / "data" / "sources" / "manual"
OUTPUTS_DIR = ROOT / "data" / "outputs"

PFF_BOARD_PATH = DOWNLOADS / "pff-my-big-board-2026-03-06.csv"
PREMIUM_FILES = {
    "passing_summary": DOWNLOADS / "passing_summary (1).csv",
    "passing_pressure": DOWNLOADS / "passing_pressure.csv",
    "passing_concept": DOWNLOADS / "passing_concept.csv",
    "time_in_pocket": DOWNLOADS / "time_in_pocket.csv",
    "receiving_summary": DOWNLOADS / "receiving_summary.csv",
    "receiving_scheme": DOWNLOADS / "receiving_scheme.csv",
    "rushing_summary": DOWNLOADS / "rushing_summary.csv",
    "offense_blocking": DOWNLOADS / "offense_blocking.csv",
    "defense_summary": DOWNLOADS / "defense_summary.csv",
    "pass_rush_summary": DOWNLOADS / "pass_rush_summary.csv",
    "run_defense_summary": DOWNLOADS / "run_defense_summary.csv",
    "defense_coverage_summary": DOWNLOADS / "defense_coverage_summary.csv",
    "defense_coverage_scheme": DOWNLOADS / "defense_coverage_scheme.csv",
    "slot_coverage": DOWNLOADS / "slot_coverage.csv",
}

PFF_BOARD_OUT = MANUAL_DIR / "pff_big_board_2026_latest.csv"
PFF_MASTER_OUT = MANUAL_DIR / "pff_master_2026.csv"
SG_ADVANCED_OUT = MANUAL_DIR / "scoutinggrade_advanced_2025.csv"
REPORT_OUT = OUTPUTS_DIR / "scoutinggrade_pff_build_report_2026.md"
MISSING_PID_OUT = OUTPUTS_DIR / "pff_missing_player_id_review_2026.csv"

SCHOOL_ALIASES = {
    "miami fl": "miami",
    "miami hurricanes": "miami",
    "miami (fl)": "miami",
    "ohio state buckeyes": "ohio state",
    "lsu tigers": "lsu",
    "utah utes": "utah",
    "smu mustangs": "smu",
    "indiana hoosiers": "indiana",
    "notre dame fighting irish": "notre dame",
    "oregon ducks": "oregon",
    "alabama crimson tide": "alabama",
    "ole miss rebels": "ole miss",
    "south carolina gamecocks": "south carolina",
    "arizona state sun devils": "arizona state",
    "nc state": "north carolina state",
    "n c state": "north carolina state",
    "s jose st": "san jose state",
    "san jose st": "san jose state",
    "oregon st": "oregon state",
    "ark state": "arkansas state",
}
MASCOT_TOKENS = {
    "tigers", "buckeyes", "utes", "mustangs", "hoosiers", "hurricanes", "ducks",
    "crimson", "tide", "rebels", "gamecocks", "sun", "devils", "fighting", "irish",
    "wolverines", "bulldogs", "wildcats", "longhorns", "nittany", "lions", "beavers",
    "wolves", "wolfpack", "eagles", "spartans", "aggies", "trojans", "razorbacks",
    "blue", "tar", "heels", "seminoles", "vols", "volunteers", "boilermakers",
    "cardinal", "cardinals", "bruins", "cowboys", "orange", "cyclones", "hokies",
    "bearcats",
}
POS_ALIASES = {
    "ED": "EDGE",
    "DE": "EDGE",
    "OLB": "EDGE",
    "DI": "DT",
    "IDL": "DT",
    "HB": "RB",
    "FB": "RB",
    "G": "IOL",
    "C": "IOL",
    "T": "OT",
}


def _safe_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fields = list(rows[0].keys())
    seen = set(fields)
    for row in rows[1:]:
        for k in row.keys():
            if k not in seen:
                fields.append(k)
                seen.add(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _norm_school(value: str) -> str:
    s = (value or "").lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    words = [w for w in s.split() if w]
    out = [w for w in words if w not in MASCOT_TOKENS]
    norm = " ".join(out)
    return SCHOOL_ALIASES.get(norm, norm)


def _norm_pos(value: str) -> str:
    pos = normalize_pos(str(value or "").strip().upper())
    return POS_ALIASES.get(pos, pos)


def _identity_key(name: str, pos: str, school: str) -> tuple[str, str, str]:
    return (canonical_player_name(name), _norm_pos(pos), _norm_school(school))


def _identity_maps(premium_rows_by_file: dict[str, list[dict]]) -> tuple[dict, dict, dict, dict]:
    by_exact: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    by_name_pos: dict[tuple[str, str], set[str]] = defaultdict(set)
    by_name: dict[str, set[str]] = defaultdict(set)
    by_pid: dict[str, dict] = {}
    for rows in premium_rows_by_file.values():
        for row in rows:
            pid = str(row.get("player_id", "")).strip()
            name = str(row.get("player", "")).strip()
            pos = _norm_pos(row.get("position", ""))
            school = _norm_school(row.get("team_name", ""))
            if not pid or not name or not pos:
                continue
            k_exact = _identity_key(name, pos, school)
            by_exact[k_exact].add(pid)
            by_name_pos[(k_exact[0], k_exact[1])].add(pid)
            by_name[k_exact[0]].add(pid)
            info = by_pid.setdefault(
                pid,
                {"player_id": pid, "player_name": name, "position": pos, "school": school},
            )
            if not info.get("school") and school:
                info["school"] = school
    return by_exact, by_name_pos, by_name, by_pid


def _single_or_blank(values: set[str]) -> str:
    if len(values) == 1:
        return next(iter(values))
    return ""


def _infer_player_id(row: dict, by_exact: dict, by_name_pos: dict, by_name: dict) -> tuple[str, str]:
    k_exact = _identity_key(row["Player"], row["Pos"], row["School"])
    pid = _single_or_blank(by_exact.get(k_exact, set()))
    if pid:
        return pid, "exact"
    pid = _single_or_blank(by_name_pos.get((k_exact[0], k_exact[1]), set()))
    if pid:
        return pid, "name_pos"
    pid = _single_or_blank(by_name.get(k_exact[0], set()))
    if pid:
        return pid, "name_only"
    return "", ""


def _rate(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _versatility_count(row: dict) -> int:
    count = 0
    for key in ("snap_counts_lt", "snap_counts_lg", "snap_counts_ce", "snap_counts_rg", "snap_counts_rt", "snap_counts_te"):
        if (_safe_float(row.get(key)) or 0) > 0:
            count += 1
    return count


def build() -> None:
    if not PFF_BOARD_PATH.exists():
        raise SystemExit(f"Missing PFF board: {PFF_BOARD_PATH}")

    premium_rows_by_file = {name: _read_csv(path) for name, path in PREMIUM_FILES.items() if path.exists()}
    by_exact, by_name_pos, by_name, by_pid = _identity_maps(premium_rows_by_file)

    pff_board_rows = _read_csv(PFF_BOARD_PATH)
    transformed_board_rows: list[dict] = []
    master_rows: list[dict] = []
    missing_pid_rows: list[dict] = []
    matched_pids: set[str] = set()

    for row in pff_board_rows:
        out_row = {
            "Rank": row.get("Rank", "").strip(),
            "Pos": _norm_pos(row.get("Position", "")),
            "Player": row.get("Player", "").strip(),
            "School": row.get("School", "").strip(),
            "PFF Grade": row.get("PFF Grade", "").strip(),
            "PFF WAA": "",
            "Notes": row.get("Analysis", "").strip(),
            "Overall Score": "",
            "Score Label": "",
        }
        transformed_board_rows.append(out_row)
        pid, match_type = _infer_player_id(out_row, by_exact, by_name_pos, by_name)
        if pid:
            matched_pids.add(pid)
        else:
            missing_pid_rows.append(
                {
                    "pff_rank": out_row["Rank"],
                    "player_name": out_row["Player"],
                    "position": out_row["Pos"],
                    "school": out_row["School"],
                    "pff_grade": out_row["PFF Grade"],
                }
            )
        info = by_pid.get(pid, {})
        master_rows.append(
            {
                "pff_rank": out_row["Rank"],
                "player_name": out_row["Player"],
                "position": out_row["Pos"],
                "school": out_row["School"],
                "pff_grade": out_row["PFF Grade"],
                "pff_waa": "",
                "analysis": out_row["Notes"],
                "player_id": pid,
                "match_type": match_type,
                "matched_team_name": info.get("school", ""),
            }
        )

    metrics_by_pid: dict[str, dict] = {pid: {"player_id": pid} for pid in matched_pids}

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("passing_summary", [])):
        if pid not in metrics_by_pid:
            continue
        m = metrics_by_pid[pid]
        m.update(
            {
                "player_name": row.get("player", ""),
                "position": _norm_pos(row.get("position", "")),
                "school": row.get("team_name", ""),
                "sg_qb_pass_grade": row.get("grades_pass", ""),
                "sg_qb_btt_rate": row.get("btt_rate", ""),
                "sg_qb_twp_rate": row.get("twp_rate", ""),
                "sg_qb_pressure_to_sack_rate": row.get("pressure_to_sack_rate", ""),
                "sg_qb_avg_depth_of_target": row.get("avg_depth_of_target", ""),
                "sg_qb_avg_time_to_throw": row.get("avg_time_to_throw", ""),
                "sg_qb_qb_rating": row.get("qb_rating", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("passing_pressure", [])):
        if pid not in metrics_by_pid:
            continue
        m = metrics_by_pid[pid]
        for src, dest in [
            ("pressure_grades_pass", "sg_qb_pressure_grade"),
            ("pressure_qb_rating", "sg_qb_pressure_qb_rating"),
            ("pressure_twp_rate", "sg_qb_pressure_twp_rate"),
            ("pressure_btt_rate", "sg_qb_pressure_btt_rate"),
            ("pressure_pressure_to_sack_rate", "sg_qb_pressure_pressure_to_sack_rate"),
            ("blitz_grades_pass", "sg_qb_blitz_grade"),
            ("blitz_qb_rating", "sg_qb_blitz_qb_rating"),
        ]:
            if src in row:
                m[dest] = row.get(src, "")

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("passing_concept", [])):
        if pid not in metrics_by_pid:
            continue
        m = metrics_by_pid[pid]
        for src, dest in [
            ("no_screen_grades_pass", "sg_qb_no_screen_grade"),
            ("no_screen_btt_rate", "sg_qb_no_screen_btt_rate"),
            ("no_screen_twp_rate", "sg_qb_no_screen_twp_rate"),
            ("pa_grades_pass", "sg_qb_play_action_grade"),
            ("npa_grades_pass", "sg_qb_non_play_action_grade"),
        ]:
            if src in row:
                m[dest] = row.get(src, "")

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("time_in_pocket", [])):
        if pid not in metrics_by_pid:
            continue
        m = metrics_by_pid[pid]
        for src, dest in [
            ("less_qb_rating", "sg_qb_quick_qb_rating"),
            ("less_btt_rate", "sg_qb_quick_btt_rate"),
            ("less_twp_rate", "sg_qb_quick_twp_rate"),
            ("more_qb_rating", "sg_qb_extended_qb_rating"),
            ("more_btt_rate", "sg_qb_extended_btt_rate"),
            ("more_twp_rate", "sg_qb_extended_twp_rate"),
        ]:
            if src in row:
                m[dest] = row.get(src, "")

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("rushing_summary", [])):
        if pid not in metrics_by_pid:
            continue
        m = metrics_by_pid[pid]
        routes = _safe_float(row.get("routes"))
        targets = _safe_float(row.get("targets"))
        tpr = _rate(targets, routes)
        m.update(
            {
                "player_name": row.get("player", m.get("player_name", "")),
                "position": _norm_pos(row.get("position", m.get("position", ""))),
                "school": row.get("team_name", m.get("school", "")),
                "sg_rb_run_grade": row.get("grades_run", ""),
                "sg_rb_elusive_rating": row.get("elusive_rating", ""),
                "sg_rb_mtf": row.get("elu_rush_mtf", ""),
                "sg_rb_yco_attempt": row.get("yco_attempt", ""),
                "sg_rb_explosive_rate": row.get("explosive", ""),
                "sg_rb_breakaway_percent": row.get("breakaway_percent", ""),
                "sg_rb_targets_per_route": round(tpr, 4) if tpr is not None else "",
                "sg_rb_yprr": row.get("yprr", ""),
                "sg_rb_total_touches": row.get("total_touches", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("receiving_summary", [])):
        if pid not in metrics_by_pid:
            continue
        m = metrics_by_pid[pid]
        targets = _safe_float(row.get("targets"))
        routes = _safe_float(row.get("routes"))
        tpr = _rate(targets, routes)
        m.update(
            {
                "player_name": row.get("player", m.get("player_name", "")),
                "position": _norm_pos(row.get("position", m.get("position", ""))),
                "school": row.get("team_name", m.get("school", "")),
                "sg_wrte_route_grade": row.get("grades_pass_route", ""),
                "sg_wrte_yprr": row.get("yprr", ""),
                "sg_wrte_targets_per_route": round(tpr, 4) if tpr is not None else "",
                "sg_wrte_contested_catch_rate": row.get("contested_catch_rate", ""),
                "sg_wrte_drop_rate": row.get("drop_rate", ""),
                "sg_wrte_slot_rate": row.get("slot_rate", ""),
                "sg_wrte_inline_rate": row.get("inline_rate", ""),
                "sg_wrte_wide_rate": row.get("wide_rate", ""),
                "sg_wrte_avoided_tackles": row.get("avoided_tackles", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("receiving_scheme", [])):
        if pid not in metrics_by_pid:
            continue
        metrics_by_pid[pid].update(
            {
                "sg_wrte_man_yprr": row.get("man_yprr", ""),
                "sg_wrte_zone_yprr": row.get("zone_yprr", ""),
                "sg_wrte_man_targets_percent": row.get("man_targets_percent", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("offense_blocking", [])):
        if pid not in metrics_by_pid:
            continue
        m = metrics_by_pid[pid]
        pressures_allowed = _safe_float(row.get("pressures_allowed"))
        pass_block_snaps = _safe_float(row.get("snap_counts_pass_block"))
        pr = _rate(pressures_allowed, pass_block_snaps)
        m.update(
            {
                "player_name": row.get("player", m.get("player_name", "")),
                "position": _norm_pos(row.get("position", m.get("position", ""))),
                "school": row.get("team_name", m.get("school", "")),
                "sg_ol_pass_block_grade": row.get("grades_pass_block", ""),
                "sg_ol_run_block_grade": row.get("grades_run_block", ""),
                "sg_ol_pbe": row.get("pbe", ""),
                "sg_ol_pressure_allowed_rate": round(pr, 5) if pr is not None else "",
                "sg_ol_pressures_allowed": row.get("pressures_allowed", ""),
                "sg_ol_sacks_allowed": row.get("sacks_allowed", ""),
                "sg_ol_hits_allowed": row.get("hits_allowed", ""),
                "sg_ol_hurries_allowed": row.get("hurries_allowed", ""),
                "sg_ol_versatility_count": _versatility_count(row),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("pass_rush_summary", [])):
        if pid not in metrics_by_pid:
            continue
        metrics_by_pid[pid].update(
            {
                "player_name": row.get("player", metrics_by_pid[pid].get("player_name", "")),
                "position": _norm_pos(row.get("position", metrics_by_pid[pid].get("position", ""))),
                "school": row.get("team_name", metrics_by_pid[pid].get("school", "")),
                "sg_dl_pass_rush_grade": row.get("grades_pass_rush_defense", ""),
                "sg_dl_pass_rush_win_rate": row.get("pass_rush_win_rate", ""),
                "sg_dl_prp": row.get("prp", ""),
                "sg_dl_total_pressures": row.get("total_pressures", ""),
                "sg_dl_true_pass_set_win_rate": row.get("true_pass_set_pass_rush_win_rate", ""),
                "sg_dl_true_pass_set_prp": row.get("true_pass_set_prp", ""),
                "sg_dl_true_pass_set_total_pressures": row.get("true_pass_set_total_pressures", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("run_defense_summary", [])):
        if pid not in metrics_by_pid:
            continue
        metrics_by_pid[pid].update(
            {
                "sg_front_run_def_grade": row.get("grades_run_defense", metrics_by_pid[pid].get("sg_front_run_def_grade", "")),
                "sg_front_stop_percent": row.get("stop_percent", metrics_by_pid[pid].get("sg_front_stop_percent", "")),
                "sg_front_missed_tackle_rate": row.get("missed_tackle_rate", metrics_by_pid[pid].get("sg_front_missed_tackle_rate", "")),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("defense_summary", [])):
        if pid not in metrics_by_pid:
            continue
        metrics_by_pid[pid].update(
            {
                "player_name": row.get("player", metrics_by_pid[pid].get("player_name", "")),
                "position": _norm_pos(row.get("position", metrics_by_pid[pid].get("position", ""))),
                "school": row.get("team_name", metrics_by_pid[pid].get("school", "")),
                "sg_def_coverage_grade": row.get("grades_coverage_defense", ""),
                "sg_def_run_grade": row.get("grades_run_defense", ""),
                "sg_def_tackle_grade": row.get("grades_tackle", ""),
                "sg_def_missed_tackle_rate": row.get("missed_tackle_rate", ""),
                "sg_def_qb_rating_against": row.get("qb_rating_against", ""),
                "sg_def_interceptions": row.get("interceptions", ""),
                "sg_def_pass_break_ups": row.get("pass_break_ups", ""),
                "sg_def_tackles": row.get("tackles", ""),
                "sg_def_tackles_for_loss": row.get("tackles_for_loss", ""),
                "sg_def_total_pressures": row.get("total_pressures", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("defense_coverage_summary", [])):
        if pid not in metrics_by_pid:
            continue
        metrics_by_pid[pid].update(
            {
                "sg_cov_grade": row.get("grades_coverage_defense", ""),
                "sg_cov_forced_incompletion_rate": row.get("forced_incompletion_rate", ""),
                "sg_cov_snaps_per_target": row.get("coverage_snaps_per_target", ""),
                "sg_cov_snaps_per_reception": row.get("coverage_snaps_per_reception", ""),
                "sg_cov_yards_per_snap": row.get("yards_per_coverage_snap", ""),
                "sg_cov_qb_rating_against": row.get("qb_rating_against", ""),
                "sg_cov_catch_rate": row.get("catch_rate", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("defense_coverage_scheme", [])):
        if pid not in metrics_by_pid:
            continue
        metrics_by_pid[pid].update(
            {
                "sg_cov_man_grade": row.get("man_grades_coverage_defense", ""),
                "sg_cov_man_qb_rating_against": row.get("man_qb_rating_against", ""),
                "sg_cov_man_yards_per_snap": row.get("man_yards_per_coverage_snap", ""),
                "sg_cov_zone_grade": row.get("zone_grades_coverage_defense", ""),
                "sg_cov_zone_qb_rating_against": row.get("zone_qb_rating_against", ""),
                "sg_cov_zone_yards_per_snap": row.get("zone_yards_per_coverage_snap", ""),
            }
        )

    for pid, row in ((str(r.get("player_id", "")).strip(), r) for r in premium_rows_by_file.get("slot_coverage", [])):
        if pid not in metrics_by_pid:
            continue
        metrics_by_pid[pid].update(
            {
                "sg_slot_cov_snaps": row.get("coverage_snaps", ""),
                "sg_slot_cov_snaps_per_target": row.get("coverage_snaps_per_target", ""),
                "sg_slot_cov_qb_rating_against": row.get("qb_rating_against", ""),
                "sg_slot_cov_yards_per_snap": row.get("yards_per_coverage_snap", ""),
            }
        )

    advanced_rows = []
    for pid in sorted(metrics_by_pid):
        row = metrics_by_pid[pid]
        row["position"] = _norm_pos(row.get("position", ""))
        row["source"] = "scoutinggrade_advanced_signal_2025"
        row["season"] = 2025
        if row.get("player_name") and row.get("position"):
            advanced_rows.append(row)

    _write_csv(PFF_BOARD_OUT, transformed_board_rows)
    _write_csv(PFF_MASTER_OUT, master_rows)
    _write_csv(SG_ADVANCED_OUT, advanced_rows)
    _write_csv(MISSING_PID_OUT, missing_pid_rows)

    report = [
        "# ScoutingGrade PFF Build Report 2026",
        "",
        f"- PFF board rows: `{len(pff_board_rows)}`",
        f"- PFF board rows with inferred premium player_id: `{len(matched_pids)}`",
        f"- PFF board rows missing player_id match: `{len(missing_pid_rows)}`",
        f"- ScoutingGrade advanced rows written: `{len(advanced_rows)}`",
    ]
    REPORT_OUT.write_text("\n".join(report), encoding="utf-8")
    print(f"Wrote {PFF_BOARD_OUT}")
    print(f"Wrote {PFF_MASTER_OUT}")
    print(f"Wrote {SG_ADVANCED_OUT}")
    print(f"Wrote {MISSING_PID_OUT}")
    print(f"Wrote {REPORT_OUT}")


if __name__ == "__main__":
    build()
