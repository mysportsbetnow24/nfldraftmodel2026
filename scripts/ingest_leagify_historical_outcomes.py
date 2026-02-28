#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_DIR = ROOT / "data" / "sources" / "external" / "historical-nfl-draft-data"
COMPILATION_PATH = EXTERNAL_DIR / "notebook" / "compilations" / "drafts2015To2022.csv"
DRAFT_2023_PATH = EXTERNAL_DIR / "notebook" / "drafts" / "2023Draft.csv"
OUT_PATH = ROOT / "data" / "processed" / "leagify_historical_outcomes_2015_2023.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "leagify_historical_ingest_report_2026-02-28.md"


POS_MAP = {
    "QB": "QB",
    "RB": "RB",
    "FB": "RB",
    "WR": "WR",
    "TE": "TE",
    "T": "OT",
    "OT": "OT",
    "G": "IOL",
    "OG": "IOL",
    "C": "IOL",
    "OC": "IOL",
    "OL": "IOL",
    "DE": "EDGE",
    "EDGE": "EDGE",
    "OLB": "EDGE",
    "DT": "DT",
    "NT": "DT",
    "ILB": "LB",
    "MLB": "LB",
    "LB": "LB",
    "CB": "CB",
    "S": "S",
    "SS": "S",
    "FS": "S",
    "DB": "S",
}


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _to_int(value) -> int | None:
    f = _to_float(value)
    if f is None:
        return None
    return int(round(f))


def _norm_pos(raw: str) -> str:
    p = str(raw or "").strip().upper()
    return POS_MAP.get(p, p)


def _read_rows(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def _iter_inputs(compilation_rows: Iterable[dict], draft_2023_rows: Iterable[dict]) -> Iterable[dict]:
    for row in compilation_rows:
        row = dict(row)
        row["_source_file"] = "drafts2015To2022.csv"
        yield row
    for row in draft_2023_rows:
        row = dict(row)
        row["_source_file"] = "2023Draft.csv"
        if not str(row.get("DraftYear", "")).strip():
            row["DraftYear"] = "2023"
        yield row


def _row_key(row: dict) -> tuple:
    draft_year = _to_int(row.get("DraftYear")) or 0
    player_id = str(row.get("PlayerID", "")).strip().lower()
    pick = _to_int(row.get("Pick")) or 0
    name = str(row.get("Player", "")).strip().lower()
    if player_id:
        return draft_year, player_id
    return draft_year, f"{pick}:{name}"


def _transform(row: dict) -> dict | None:
    draft_year = _to_int(row.get("DraftYear"))
    pick = _to_int(row.get("Pick"))
    rnd = _to_int(row.get("Rnd"))
    if draft_year is None or pick is None or rnd is None:
        return None
    if draft_year < 2015 or draft_year > 2025:
        return None

    pos = _norm_pos(row.get("Pos", ""))
    if not pos:
        return None

    out = {
        "draft_year": draft_year,
        "draft_round": rnd,
        "overall_pick": pick,
        "team": str(row.get("Tm", "")).strip(),
        "player_name": str(row.get("Player", "")).strip(),
        "player_id": str(row.get("PlayerID", "")).strip(),
        "position": pos,
        "age": _to_int(row.get("Age")),
        "college": str(row.get("College/Univ", "")).strip(),
        "to_year": _to_int(row.get("To")),
        "years_since_draft": _to_int(row.get("YearsSinceDraft")),
        "years_in_career": _to_int(row.get("YearsInCareer")),
        "games_per_year_avg": _to_float(row.get("GamesPerYearAvg")),
        "value_per_year": _to_float(row.get("ValuePerYear")),
        "wav": _to_float(row.get("wAV")),
        "drav": _to_float(row.get("DrAV")),
        "games": _to_float(row.get("G")),
        "starts": _to_float(row.get("St")),
        "ap1": _to_float(row.get("AP1")),
        "pb": _to_float(row.get("PB")),
        "predicted_av": _to_float(row.get("PredictedAV")),
        "surplus_value": _to_float(row.get("ValueVsPredictedValue")),
        "johnson_value": _to_float(row.get("johnson")),
        "hill_value": _to_float(row.get("hill")),
        "otc_value": _to_float(row.get("otc")),
        "pff_value": _to_float(row.get("pff")),
        "source_file": str(row.get("_source_file", "")).strip(),
    }
    return out


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize Leagify historical draft outcomes to project schema.")
    parser.add_argument("--compilation", type=Path, default=COMPILATION_PATH)
    parser.add_argument("--draft-2023", type=Path, default=DRAFT_2023_PATH)
    parser.add_argument("--output", type=Path, default=OUT_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    args = parser.parse_args()

    if not args.compilation.exists():
        raise FileNotFoundError(f"Missing input: {args.compilation}")
    if not args.draft_2023.exists():
        raise FileNotFoundError(f"Missing input: {args.draft_2023}")

    compilation_rows = _read_rows(args.compilation)
    rows_2023 = _read_rows(args.draft_2023)

    best: dict[tuple, dict] = {}
    skipped = 0
    for raw in _iter_inputs(compilation_rows, rows_2023):
        transformed = _transform(raw)
        if transformed is None:
            skipped += 1
            continue
        key = _row_key(raw)
        existing = best.get(key)
        if existing is None:
            best[key] = transformed
            continue
        if str(transformed.get("source_file", "")).startswith("2023"):
            best[key] = transformed

    rows = list(best.values())
    rows.sort(key=lambda r: (int(r["draft_year"]), int(r["overall_pick"])))
    _write_csv(args.output, rows)

    by_year: dict[int, int] = {}
    for row in rows:
        year = int(row["draft_year"])
        by_year[year] = by_year.get(year, 0) + 1

    lines = [
        "# Leagify Historical Ingest Report",
        "",
        f"- Input compilation: `{args.compilation}`",
        f"- Input 2023: `{args.draft_2023}`",
        f"- Output: `{args.output}`",
        f"- Rows written: **{len(rows)}**",
        f"- Rows skipped during transform: **{skipped}**",
        "",
        "## Rows By Draft Year",
        "",
        "| Draft Year | Rows |",
        "|---:|---:|",
    ]
    for year in sorted(by_year):
        lines.append(f"| {year} | {by_year[year]} |")

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Rows: {len(rows)}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
