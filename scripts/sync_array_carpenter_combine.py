#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import urllib.request
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name, normalize_pos  # noqa: E402


PRO_DAY_URL = (
    "https://raw.githubusercontent.com/array-carpenter/nfl-draft-data/master/data/combine_pro_day.csv"
)
OFFICIAL_URL = (
    "https://raw.githubusercontent.com/array-carpenter/nfl-draft-data/master/data/combine_official.csv"
)

RAW_DIR = ROOT / "data" / "sources" / "external" / "array-carpenter-nfl-draft-data" / "data"
RAW_PRO_DAY = RAW_DIR / "combine_pro_day.csv"
RAW_OFFICIAL = RAW_DIR / "combine_official.csv"
HIST_OUT = ROOT / "data" / "sources" / "external" / "combine_data_unique_athlete_id_step4.csv"
COMBINE_2026_OUT = ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv"
REPORT_OUT = ROOT / "data" / "outputs" / "array_carpenter_combine_sync_report.txt"

PRO_DAY_HEADER = [
    "Year",
    "player",
    "College",
    "POS_GP",
    "POS",
    "athlete_id",
    "Height (in)",
    "Weight (lbs)",
    "Arm Length (in)",
    "Hand Size (in)",
    "40 Yard",
    "10-Yard Split",
    "Vert Leap (in)",
    "Broad Jump (in)",
    "3Cone",
    "Shuttle",
    "Bench Press",
    "Wingspan (in)",
    "nfl_person_id",
]

OFFICIAL_TO_PRO_DAY = {
    "height": "Height (in)",
    "weight": "Weight (lbs)",
    "arm_length": "Arm Length (in)",
    "hand_size": "Hand Size (in)",
    "forty_yard_dash": "40 Yard",
    "ten_yard_split": "10-Yard Split",
    "vertical_jump": "Vert Leap (in)",
    "broad_jump": "Broad Jump (in)",
    "three_cone_drill": "3Cone",
    "twenty_yard_shuttle": "Shuttle",
    "bench_press": "Bench Press",
}

COMBINE_2026_FIELDS = [
    "player_name",
    "school",
    "position",
    "height_in",
    "weight_lb",
    "arm_in",
    "hand_in",
    "forty",
    "ten_split",
    "vertical",
    "broad",
    "shuttle",
    "three_cone",
    "bench",
    "ras_official",
    "source",
    "last_updated",
]
ALLOWED_POSITIONS = {"QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"}

PRO_DAY_TO_2026 = {
    "Height (in)": "height_in",
    "Weight (lbs)": "weight_lb",
    "Arm Length (in)": "arm_in",
    "Hand Size (in)": "hand_in",
    "40 Yard": "forty",
    "10-Yard Split": "ten_split",
    "Vert Leap (in)": "vertical",
    "Broad Jump (in)": "broad",
    "Shuttle": "shuttle",
    "3Cone": "three_cone",
    "Bench Press": "bench",
}


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fetch_csv(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as resp:
        body = resp.read()
    out_path.write_bytes(body)


def _to_year(value: str | int | float | None) -> int | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return int(float(txt))
    except ValueError:
        return None


def _to_num_txt(value: str | None) -> str:
    txt = str(value or "").strip()
    if not txt or txt.upper() in {"NA", "N/A", "NULL", "NONE", "-"}:
        return ""
    return txt


def _sort_key(row: dict) -> tuple[int, str]:
    year = _to_year(row.get("Year")) or 0
    return (year, str(row.get("player", "")).lower())


def _to_pro_day_row_from_official(row: dict) -> dict:
    out = {k: "" for k in PRO_DAY_HEADER}
    out["Year"] = str(row.get("year", "")).strip()
    out["player"] = str(row.get("player", "")).strip()
    out["College"] = str(row.get("college", "")).strip()
    out["POS_GP"] = str(row.get("position_group", "")).strip()
    out["POS"] = str(row.get("position", "")).strip()
    out["athlete_id"] = str(row.get("person_id", "")).strip()
    out["nfl_person_id"] = str(row.get("person_id", "")).strip()
    for src, dest in OFFICIAL_TO_PRO_DAY.items():
        out[dest] = _to_num_txt(row.get(src, ""))
    return out


def _merge_historical(pro_day_rows: list[dict], official_rows: list[dict]) -> tuple[list[dict], dict]:
    merged: dict[tuple[int, str], dict] = {}
    fill_count = 0
    add_count = 0

    for row in pro_day_rows:
        year = _to_year(row.get("Year"))
        name = canonical_player_name(row.get("player", ""))
        if year is None or not name:
            continue
        payload = {k: _to_num_txt(row.get(k, "")) for k in PRO_DAY_HEADER}
        # Preserve text fields verbatim if present.
        for key in ["Year", "player", "College", "POS_GP", "POS", "athlete_id", "nfl_person_id"]:
            payload[key] = str(row.get(key, "")).strip()
        merged[(year, name)] = payload

    for off in official_rows:
        pro = _to_pro_day_row_from_official(off)
        year = _to_year(pro.get("Year"))
        name = canonical_player_name(pro.get("player", ""))
        if year is None or not name:
            continue
        key = (year, name)
        existing = merged.get(key)
        if existing is None:
            merged[key] = pro
            add_count += 1
            continue
        for col in PRO_DAY_HEADER:
            if col in {"Year", "player"}:
                continue
            if not str(existing.get(col, "")).strip() and str(pro.get(col, "")).strip():
                existing[col] = str(pro.get(col, "")).strip()
                fill_count += 1

    rows = list(merged.values())
    rows.sort(key=_sort_key)
    stats = {
        "base_pro_day_rows": len(pro_day_rows),
        "official_rows": len(official_rows),
        "official_new_rows_added": add_count,
        "official_missing_fields_filled": fill_count,
        "merged_rows": len(rows),
    }
    return rows, stats


def _merge_into_combine_2026(merged_rows: list[dict], combine_out: Path) -> dict:
    today = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
    existing = _read_csv(combine_out)
    by_name = {canonical_player_name(r.get("player_name", "")): dict(r) for r in existing if r.get("player_name")}

    created = 0
    field_updates = 0
    rows_2026 = [r for r in merged_rows if (_to_year(r.get("Year")) or 0) == 2026]
    for row in rows_2026:
        player_name = str(row.get("player", "")).strip()
        if not player_name:
            continue
        key = canonical_player_name(player_name)
        target = by_name.get(key)
        if target is None:
            target = {k: "" for k in COMBINE_2026_FIELDS}
            target["player_name"] = player_name
            by_name[key] = target
            created += 1

        school = str(row.get("College", "")).strip()
        pos = normalize_pos(str(row.get("POS", "")).strip())
        if pos not in ALLOWED_POSITIONS:
            pos = ""
        if school:
            target["school"] = school
        if pos:
            target["position"] = pos

        for src_col, dst_col in PRO_DAY_TO_2026.items():
            new_val = _to_num_txt(row.get(src_col, ""))
            if not new_val:
                continue
            cur_val = str(target.get(dst_col, "")).strip()
            # Prefer refreshed array-carpenter value when present.
            if cur_val != new_val:
                target[dst_col] = new_val
                field_updates += 1

        source = str(target.get("source", "")).strip()
        tag = "array-carpenter-combine"
        if not source:
            target["source"] = tag
        elif tag not in source:
            target["source"] = f"{source}; {tag}"
        target["last_updated"] = today

    out_rows = list(by_name.values())
    for r in out_rows:
        pos = normalize_pos(str(r.get("position", "")).strip())
        r["position"] = pos if pos in ALLOWED_POSITIONS else ""
    out_rows.sort(key=lambda r: canonical_player_name(r.get("player_name", "")))
    _write_csv(combine_out, out_rows, COMBINE_2026_FIELDS)
    return {
        "rows_2026_in_source": len(rows_2026),
        "combine_rows_total": len(out_rows),
        "combine_rows_created": created,
        "combine_field_updates": field_updates,
    }


def main() -> None:
    p = argparse.ArgumentParser(
        description="Sync array-carpenter combine data, merge pro_day + official, and update model combine inputs."
    )
    p.add_argument("--execute", action="store_true", help="Download CSVs from GitHub")
    p.add_argument("--pro-day-path", type=str, default=str(RAW_PRO_DAY), help="Local pro_day CSV path")
    p.add_argument("--official-path", type=str, default=str(RAW_OFFICIAL), help="Local official CSV path")
    p.add_argument("--hist-out", type=str, default=str(HIST_OUT), help="Merged historical combine CSV output")
    p.add_argument("--combine-2026-out", type=str, default=str(COMBINE_2026_OUT), help="combine_2026_results output")
    p.add_argument("--report-out", type=str, default=str(REPORT_OUT), help="Sync report path")
    args = p.parse_args()

    pro_day_path = Path(args.pro_day_path)
    official_path = Path(args.official_path)
    hist_out = Path(args.hist_out)
    combine_2026_out = Path(args.combine_2026_out)
    report_out = Path(args.report_out)

    if args.execute:
        _fetch_csv(PRO_DAY_URL, pro_day_path)
        _fetch_csv(OFFICIAL_URL, official_path)

    if not pro_day_path.exists():
        raise SystemExit(f"Missing pro_day CSV: {pro_day_path}")
    if not official_path.exists():
        raise SystemExit(f"Missing official CSV: {official_path}")

    pro_day_rows = _read_csv(pro_day_path)
    official_rows = _read_csv(official_path)
    merged_rows, hist_stats = _merge_historical(pro_day_rows, official_rows)
    _write_csv(hist_out, merged_rows, PRO_DAY_HEADER)

    combine_stats = _merge_into_combine_2026(merged_rows, combine_2026_out)

    lines = [
        f"array-carpenter combine sync @ {dt.datetime.now(dt.UTC).isoformat()}",
        f"pro_day_path: {pro_day_path}",
        f"official_path: {official_path}",
        f"hist_out: {hist_out}",
        f"combine_2026_out: {combine_2026_out}",
        "",
        f"base_pro_day_rows: {hist_stats['base_pro_day_rows']}",
        f"official_rows: {hist_stats['official_rows']}",
        f"official_new_rows_added: {hist_stats['official_new_rows_added']}",
        f"official_missing_fields_filled: {hist_stats['official_missing_fields_filled']}",
        f"merged_rows_written: {hist_stats['merged_rows']}",
        "",
        f"rows_2026_in_source: {combine_stats['rows_2026_in_source']}",
        f"combine_rows_total: {combine_stats['combine_rows_total']}",
        f"combine_rows_created: {combine_stats['combine_rows_created']}",
        f"combine_field_updates: {combine_stats['combine_field_updates']}",
    ]
    report_out.parent.mkdir(parents=True, exist_ok=True)
    report_out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Merged historical combine rows: {hist_stats['merged_rows']}")
    print(f"Updated 2026 combine rows: total={combine_stats['combine_rows_total']} created={combine_stats['combine_rows_created']} field_updates={combine_stats['combine_field_updates']}")
    print(f"Historical output: {hist_out}")
    print(f"2026 combine output: {combine_2026_out}")
    print(f"Report: {report_out}")


if __name__ == "__main__":
    main()
