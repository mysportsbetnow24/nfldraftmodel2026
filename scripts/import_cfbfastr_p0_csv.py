#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


BASE_PATH = ROOT / "data" / "sources" / "manual" / "cfb_production_2025.csv"
TEMPLATE_PATH = ROOT / "data" / "sources" / "manual" / "cfb_production_2025_template.csv"
DEFAULT_REPORT = ROOT / "data" / "outputs" / "cfbfastr_p0_import_report.txt"

NAME_KEYS = ("player_name", "player", "name", "athlete", "athlete_name", "display_name")
SCHOOL_KEYS = ("school", "team", "college")
POS_KEYS = ("position", "pos")
SEASON_KEYS = ("season", "year")

P0_TARGETS = [
    "qb_ppa_overall",
    "qb_ppa_passing",
    "qb_ppa_standard_downs",
    "qb_ppa_passing_downs",
    "qb_wepa_passing",
    "qb_usage_rate",
    "wrte_ppa_overall",
    "wrte_ppa_passing_downs",
    "wrte_wepa_receiving",
    "wrte_usage_rate",
    "rb_ppa_rushing",
    "rb_ppa_standard_downs",
    "rb_wepa_rushing",
    "rb_usage_rate",
]


def _norm_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


def _safe_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _first_value(row_norm: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        val = row_norm.get(_norm_key(key), "")
        if str(val).strip():
            return str(val).strip()
    return ""


def _load_template_fieldnames(template_path: Path) -> list[str]:
    with template_path.open() as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or [])


def _load_base_rows(path: Path) -> tuple[list[str], list[dict]]:
    if not path.exists():
        return [], []
    with path.open() as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else []
        return fieldnames, rows


def _detect_source_type(path: Path, headers_norm: set[str]) -> str:
    label = _norm_key(path.stem)
    if "qb" in label:
        return "qb"
    if "wr" in label or "te" in label or "receiv" in label:
        return "wrte"
    if "rb" in label or "rush" in label:
        return "rb"
    if any(k in headers_norm for k in ("qbr", "qbepaperplay", "qbppaoverall", "qbwepapassing")):
        return "qb"
    if any(k in headers_norm for k in ("wrtewepareceiving", "wepareceiving", "wrteppaoverall")):
        return "wrte"
    if any(k in headers_norm for k in ("rbweparushing", "weparushing", "rbpparushing")):
        return "rb"
    return "generic"


def _target_aliases(source_type: str) -> dict[str, tuple[str, ...]]:
    generic = {
        "qb_ppa_overall": ("qb_ppa_overall", "ppa_overall_qb", "overall_ppa_qb", "ppa_overall"),
        "qb_ppa_passing": ("qb_ppa_passing", "ppa_passing_qb", "passing_ppa_qb", "ppa_passing"),
        "qb_ppa_standard_downs": ("qb_ppa_standard_downs", "ppa_standard_downs_qb", "standard_downs_ppa_qb", "ppa_sd_qb"),
        "qb_ppa_passing_downs": ("qb_ppa_passing_downs", "ppa_passing_downs_qb", "passing_downs_ppa_qb", "ppa_pd_qb"),
        "qb_wepa_passing": ("qb_wepa_passing", "wepa_passing_qb", "passing_wepa_qb", "wepa_passing"),
        "qb_usage_rate": ("qb_usage_rate", "usage_rate_qb", "qb_usage", "usage_qb"),
        "wrte_ppa_overall": ("wrte_ppa_overall", "wr_ppa_overall", "te_ppa_overall", "ppa_overall_receiving"),
        "wrte_ppa_passing_downs": ("wrte_ppa_passing_downs", "wr_ppa_passing_downs", "te_ppa_passing_downs", "ppa_passing_downs_receiving"),
        "wrte_wepa_receiving": ("wrte_wepa_receiving", "wr_wepa_receiving", "te_wepa_receiving", "wepa_receiving"),
        "wrte_usage_rate": ("wrte_usage_rate", "wr_usage_rate", "te_usage_rate", "usage_rate_receiving"),
        "rb_ppa_rushing": ("rb_ppa_rushing", "ppa_rushing_rb", "rushing_ppa_rb", "ppa_rushing"),
        "rb_ppa_standard_downs": ("rb_ppa_standard_downs", "ppa_standard_downs_rb", "standard_downs_ppa_rb"),
        "rb_wepa_rushing": ("rb_wepa_rushing", "wepa_rushing_rb", "rushing_wepa_rb", "wepa_rushing"),
        "rb_usage_rate": ("rb_usage_rate", "usage_rate_rb", "rb_usage", "usage_rb"),
    }
    if source_type == "qb":
        return {k: v for k, v in generic.items() if k.startswith("qb_")}
    if source_type == "wrte":
        return {k: v for k, v in generic.items() if k.startswith("wrte_")}
    if source_type == "rb":
        return {k: v for k, v in generic.items() if k.startswith("rb_")}
    return generic


def _extract_target_values(row_norm: dict[str, str], source_type: str) -> dict[str, str]:
    aliases = _target_aliases(source_type)
    out: dict[str, str] = {}
    for target, names in aliases.items():
        val = ""
        for name in names:
            norm = _norm_key(name)
            if norm in row_norm and str(row_norm[norm]).strip():
                val = str(row_norm[norm]).strip()
                break
        if val:
            f = _safe_float(val)
            out[target] = str(round(f, 4)) if f is not None else val
    return out


def _build_base_index(rows: list[dict]) -> dict[tuple[str, int], list[int]]:
    index: dict[tuple[str, int], list[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        name_key = canonical_player_name(row.get("player_name", ""))
        season = int(_safe_float(row.get("season")) or 0)
        if name_key and season:
            index[(name_key, season)].append(i)
    return index


def _choose_base_row(
    rows: list[dict],
    candidate_idxs: list[int],
    school: str,
    position: str,
) -> int | None:
    if not candidate_idxs:
        return None
    pos = normalize_pos(position)
    school_l = school.strip().lower()

    if pos:
        for idx in candidate_idxs:
            if normalize_pos(rows[idx].get("position", "")) == pos:
                return idx

    if school_l:
        for idx in candidate_idxs:
            if str(rows[idx].get("school", "")).strip().lower() == school_l:
                return idx

    return candidate_idxs[0]


def _write_rows(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Import cfbfastR P0 CSV exports into cfb_production_2025.csv")
    p.add_argument(
        "--input",
        action="append",
        dest="inputs",
        help="Path to cfbfastR export CSV (can be provided multiple times).",
    )
    p.add_argument("--base", type=Path, default=BASE_PATH, help="Existing production CSV to update.")
    p.add_argument("--template", type=Path, default=TEMPLATE_PATH, help="Template CSV with expected schema.")
    p.add_argument("--output", type=Path, default=BASE_PATH, help="Output CSV path.")
    p.add_argument("--season", type=int, default=2025, help="Season filter for import rows.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing populated target values.")
    p.add_argument("--add-missing-players", action="store_true", help="Add players not found in base file.")
    p.add_argument("--dry-run", action="store_true", help="Preview updates without writing output.")
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="Report output path.")
    return p


def main() -> None:
    args = build_parser().parse_args()

    if not args.inputs:
        raise SystemExit("No --input provided. Pass one or more cfbfastR export CSV files.")

    input_paths = [Path(p).expanduser() for p in args.inputs]
    missing_inputs = [p for p in input_paths if not p.exists()]
    if missing_inputs:
        missing_str = ", ".join(str(p) for p in missing_inputs)
        raise SystemExit(f"Missing input file(s): {missing_str}")

    template_fields = _load_template_fieldnames(args.template)
    base_fields, base_rows = _load_base_rows(args.base)
    if not base_rows:
        raise SystemExit(f"Base file is missing or empty: {args.base}")

    fieldnames = list(base_fields) if base_fields else list(template_fields)
    for col in template_fields:
        if col not in fieldnames:
            fieldnames.append(col)
    for row in base_rows:
        for col in fieldnames:
            row.setdefault(col, "")

    base_index = _build_base_index(base_rows)

    updated_cells = 0
    updated_players: set[int] = set()
    added_players = 0
    unmatched_rows = 0
    source_rows_total = 0
    mapped_values = Counter()
    source_type_counts = Counter()

    for input_path in input_paths:
        with input_path.open() as f:
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            headers_norm = {_norm_key(h) for h in headers}
            source_type = _detect_source_type(input_path, headers_norm)
            source_type_counts[source_type] += 1

            for row in reader:
                source_rows_total += 1
                row_norm = {_norm_key(k): str(v or "").strip() for k, v in row.items()}
                name_raw = _first_value(row_norm, NAME_KEYS)
                if not name_raw:
                    unmatched_rows += 1
                    continue

                season_val = _first_value(row_norm, SEASON_KEYS)
                season = int(_safe_float(season_val) or args.season)
                if season != args.season:
                    continue

                school = _first_value(row_norm, SCHOOL_KEYS)
                position = _first_value(row_norm, POS_KEYS)
                values = _extract_target_values(row_norm, source_type)
                if not values:
                    continue

                name_key = canonical_player_name(name_raw)
                candidate_idxs = base_index.get((name_key, season), [])
                target_idx = _choose_base_row(base_rows, candidate_idxs, school, position)

                if target_idx is None and args.add_missing_players:
                    new_row = {c: "" for c in fieldnames}
                    new_row["player_name"] = name_raw
                    new_row["school"] = school
                    new_row["position"] = normalize_pos(position) if position else ""
                    new_row["season"] = str(season)
                    base_rows.append(new_row)
                    target_idx = len(base_rows) - 1
                    base_index[(name_key, season)].append(target_idx)
                    added_players += 1

                if target_idx is None:
                    unmatched_rows += 1
                    continue

                target_row = base_rows[target_idx]
                row_changed = False
                for col, val in values.items():
                    old = str(target_row.get(col, "")).strip()
                    if old and not args.overwrite:
                        continue
                    if old == val:
                        continue
                    target_row[col] = val
                    updated_cells += 1
                    mapped_values[col] += 1
                    row_changed = True
                if row_changed:
                    updated_players.add(target_idx)

    for idx in updated_players:
        row = base_rows[idx]
        source_val = str(row.get("source", "")).strip()
        if source_val and "CFBFASTR" not in source_val.upper():
            row["source"] = f"{source_val}|CFBFASTR_P0_IMPORT"
        elif not source_val:
            row["source"] = "CFBFASTR_P0_IMPORT"
        prov = str(row.get("cfb_prod_provenance", "")).strip()
        if prov and "cfbfastr_p0" not in prov.lower():
            row["cfb_prod_provenance"] = f"{prov}+cfbfastr_p0"
        elif not prov:
            row["cfb_prod_provenance"] = "cfbfastr_p0"

    report_lines = [
        "cfbfastR P0 Import Report",
        "",
        f"base_file: {args.base}",
        f"output_file: {args.output}",
        f"season: {args.season}",
        f"dry_run: {args.dry_run}",
        "",
        f"input_files: {len(input_paths)}",
        f"source_rows_seen: {source_rows_total}",
        f"players_updated: {len(updated_players)}",
        f"cells_updated: {updated_cells}",
        f"players_added: {added_players}",
        f"unmatched_rows: {unmatched_rows}",
        "",
        "source_type_detection:",
    ]
    for k in sorted(source_type_counts):
        report_lines.append(f"- {k}: {source_type_counts[k]}")
    report_lines.append("")
    report_lines.append("mapped_columns_counts:")
    if mapped_values:
        for k in sorted(mapped_values):
            report_lines.append(f"- {k}: {mapped_values[k]}")
    else:
        report_lines.append("- none")

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(report_lines))

    if not args.dry_run:
        _write_rows(args.output, fieldnames, base_rows)

    print(f"Report: {args.report}")
    if args.dry_run:
        print("Dry run only. No files written.")
    else:
        print(f"Wrote: {args.output}")


if __name__ == "__main__":
    main()
