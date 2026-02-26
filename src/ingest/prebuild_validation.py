from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from src.ingest.combine_loader import REQUIRED_FIELDS as COMBINE_REQUIRED_FIELDS
from src.ingest.rankings_loader import canonical_player_name, normalize_pos
from src.schemas import parse_height_to_inches


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SEED_PATH = ROOT / "data" / "processed" / "prospect_seed_2026.csv"
DEFAULT_COMBINE_PATH = ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv"
DEFAULT_RETURNING_PATH = ROOT / "data" / "sources" / "manual" / "returning_to_school_2026.csv"

DEFAULT_ALLOWED_POSITIONS = {
    "QB",
    "RB",
    "WR",
    "TE",
    "OT",
    "IOL",
    "EDGE",
    "DT",
    "LB",
    "CB",
    "S",
}


_COMBINE_RANGES = {
    "height_in": (64.0, 84.0),
    "weight_lb": (150.0, 420.0),
    "arm_in": (28.0, 39.0),
    "hand_in": (7.0, 12.5),
    "forty": (4.0, 6.5),
    "ten_split": (1.30, 2.50),
    "vertical": (20.0, 50.0),
    "broad": (90.0, 160.0),
    "shuttle": (3.50, 5.50),
    "three_cone": (6.00, 9.50),
    "bench": (1.0, 60.0),
    "ras_official": (0.0, 10.0),
}


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt or txt.upper() in {"N/A", "NA", "NULL", "NONE", "-"}:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _load_returning_names(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open() as f:
        for row in csv.DictReader(f):
            player = (row.get("player_name") or "").strip()
            if player:
                out.add(canonical_player_name(player))
    return out


def _seed_checks(seed_path: Path, allowed_positions: set[str], returning_names: set[str]) -> tuple[list[dict], list[dict], int]:
    errors: list[dict] = []
    warnings: list[dict] = []

    if not seed_path.exists():
        errors.append(
            {
                "category": "seed_file_missing",
                "message": f"Seed file not found: {seed_path}",
            }
        )
        return errors, warnings, 0

    with seed_path.open() as f:
        rows = list(csv.DictReader(f))

    if not rows:
        errors.append({"category": "seed_file_empty", "message": "Seed file has no prospect rows."})
        return errors, warnings, 0

    keys: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        player_name = (row.get("player_name") or "").strip()
        pos_raw = (row.get("pos_raw") or "").strip()
        pos = normalize_pos(pos_raw)
        key = (canonical_player_name(player_name), pos)
        keys.setdefault(key, []).append(row)

    duplicate_groups = [(key, vals) for key, vals in keys.items() if len(vals) > 1]
    if duplicate_groups:
        sample = []
        for (name, pos), vals in duplicate_groups[:20]:
            ids = [str(v.get("seed_row_id", "")) for v in vals]
            sample.append({"player_key": name, "position": pos, "rows": len(vals), "seed_row_ids": ids})
        errors.append(
            {
                "category": "duplicate_seed_player_position",
                "message": f"Found {len(duplicate_groups)} duplicate player+position groups in seed.",
                "sample": sample,
            }
        )

    bad_positions = []
    bad_measurables = []
    undeclared_returning = []

    for row in rows:
        player_name = (row.get("player_name") or "").strip()
        player_key = canonical_player_name(player_name)

        pos = normalize_pos(row.get("pos_raw", ""))
        if pos not in allowed_positions:
            bad_positions.append({"player_name": player_name, "pos_raw": row.get("pos_raw", ""), "normalized": pos})

        height_in = parse_height_to_inches(row.get("height", ""))
        weight_raw = row.get("weight_lb", "")
        try:
            weight_lb = int(weight_raw)
        except (TypeError, ValueError):
            weight_lb = -1

        if height_in is None or not (64 <= height_in <= 84) or not (150 <= weight_lb <= 420):
            bad_measurables.append(
                {"player_name": player_name, "height": row.get("height", ""), "weight_lb": row.get("weight_lb", "")}
            )

        if player_key in returning_names:
            undeclared_returning.append({"player_name": player_name, "school": row.get("school", "")})

    if bad_positions:
        errors.append(
            {
                "category": "seed_missing_or_invalid_position",
                "message": f"{len(bad_positions)} seed rows have invalid or unmapped positions.",
                "sample": bad_positions[:30],
            }
        )

    if bad_measurables:
        errors.append(
            {
                "category": "seed_invalid_measurables",
                "message": f"{len(bad_measurables)} seed rows have invalid height/weight values.",
                "sample": bad_measurables[:30],
            }
        )

    if undeclared_returning:
        errors.append(
            {
                "category": "undeclared_returning_players",
                "message": f"{len(undeclared_returning)} players are marked as returning to school.",
                "sample": undeclared_returning[:30],
            }
        )

    return errors, warnings, len(rows)


def _combine_checks(combine_path: Path, allowed_positions: set[str]) -> tuple[list[dict], list[dict], int]:
    errors: list[dict] = []
    warnings: list[dict] = []

    if not combine_path.exists():
        warnings.append({"category": "combine_file_missing", "message": f"Combine file not found: {combine_path}"})
        return errors, warnings, 0

    with combine_path.open() as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing_cols = COMBINE_REQUIRED_FIELDS - fieldnames
        if missing_cols:
            errors.append(
                {
                    "category": "combine_missing_required_columns",
                    "message": f"Combine file missing required columns: {sorted(missing_cols)}",
                }
            )
            return errors, warnings, 0

        rows = list(reader)

    if not rows:
        warnings.append(
            {
                "category": "combine_no_rows",
                "message": "Combine file has no rows yet. Build can proceed, but combine signals remain pending.",
            }
        )
        return errors, warnings, 0

    seen: dict[str, int] = {}
    dupes = []
    bad_positions = []
    range_errors = []

    for idx, row in enumerate(rows, start=2):
        player_name = (row.get("player_name") or "").strip()
        if not player_name:
            continue

        key = canonical_player_name(player_name)
        seen[key] = seen.get(key, 0) + 1

        pos = normalize_pos(row.get("position", ""))
        if pos and pos not in allowed_positions:
            bad_positions.append({"line": idx, "player_name": player_name, "position": row.get("position", "")})

        for field, (low, high) in _COMBINE_RANGES.items():
            value = _as_float(row.get(field))
            if value is None:
                continue
            if value < low or value > high:
                range_errors.append(
                    {"line": idx, "player_name": player_name, "field": field, "value": value, "expected": f"{low}-{high}"}
                )

    for key, count in seen.items():
        if count > 1:
            dupes.append({"player_key": key, "rows": count})

    if dupes:
        errors.append(
            {
                "category": "combine_duplicate_player_rows",
                "message": f"Found {len(dupes)} players with duplicate combine rows.",
                "sample": dupes[:30],
            }
        )

    if bad_positions:
        errors.append(
            {
                "category": "combine_invalid_position",
                "message": f"{len(bad_positions)} combine rows have invalid positions.",
                "sample": bad_positions[:30],
            }
        )

    if range_errors:
        errors.append(
            {
                "category": "combine_out_of_range_measurable",
                "message": f"{len(range_errors)} combine values are out of expected ranges.",
                "sample": range_errors[:40],
            }
        )

    return errors, warnings, len(rows)


def run_prebuild_checks(
    seed_path: Path | None = None,
    combine_path: Path | None = None,
    returning_path: Path | None = None,
    allowed_positions: Iterable[str] | None = None,
) -> dict:
    seed_path = seed_path or DEFAULT_SEED_PATH
    combine_path = combine_path or DEFAULT_COMBINE_PATH
    returning_path = returning_path or DEFAULT_RETURNING_PATH
    allowed = set(allowed_positions or DEFAULT_ALLOWED_POSITIONS)

    returning_names = _load_returning_names(returning_path)

    seed_errors, seed_warnings, seed_rows = _seed_checks(seed_path=seed_path, allowed_positions=allowed, returning_names=returning_names)
    combine_errors, combine_warnings, combine_rows = _combine_checks(combine_path=combine_path, allowed_positions=allowed)

    errors = seed_errors + combine_errors
    warnings = seed_warnings + combine_warnings

    return {
        "status": "fail" if errors else "pass",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "paths": {
            "seed_path": str(seed_path),
            "combine_path": str(combine_path),
            "returning_path": str(returning_path),
        },
        "counts": {
            "seed_rows": seed_rows,
            "combine_rows": combine_rows,
            "errors": len(errors),
            "warnings": len(warnings),
        },
        "errors": errors,
        "warnings": warnings,
    }


def format_prebuild_report_md(report: dict) -> str:
    lines = [
        "# Prebuild QA Report",
        "",
        f"- status: `{report.get('status', 'unknown')}`",
        f"- checked_at_utc: `{report.get('checked_at_utc', '')}`",
        f"- seed rows: `{report.get('counts', {}).get('seed_rows', 0)}`",
        f"- combine rows: `{report.get('counts', {}).get('combine_rows', 0)}`",
        f"- error count: `{report.get('counts', {}).get('errors', 0)}`",
        f"- warning count: `{report.get('counts', {}).get('warnings', 0)}`",
        "",
    ]

    errors = report.get("errors", [])
    warnings = report.get("warnings", [])

    lines.append("## Errors")
    if not errors:
        lines.append("- none")
    else:
        for err in errors:
            lines.append(f"- `{err.get('category', 'error')}`: {err.get('message', '')}")

    lines.append("")
    lines.append("## Warnings")
    if not warnings:
        lines.append("- none")
    else:
        for warn in warnings:
            lines.append(f"- `{warn.get('category', 'warning')}`: {warn.get('message', '')}")

    return "\n".join(lines) + "\n"
