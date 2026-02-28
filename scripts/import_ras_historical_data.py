#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name, normalize_pos  # noqa: E402


DOWNLOAD_DEFAULT = Path.home() / "Downloads" / "New Menu Table (no draft).csv"
SEED_PATH = ROOT / "data" / "processed" / "prospect_seed_2026.csv"
COMBINE_PATH = ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv"

HIST_OUT = ROOT / "data" / "sources" / "manual" / "ras_historical_database.csv"
BENCH_OUT = ROOT / "data" / "processed" / "ras_position_benchmarks.csv"
MATCH_OUT = ROOT / "data" / "processed" / "ras_2026_matches.csv"
REPORT_OUT = ROOT / "data" / "outputs" / "ras_import_report_2026.md"

COMBINE_FIELDS = [
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

POS_MAP = {
    "QB": "QB",
    "RB": "RB",
    "HB": "RB",
    "FB": "RB",
    "WR": "WR",
    "TE": "TE",
    "OT": "OT",
    "T": "OT",
    "LT": "OT",
    "RT": "OT",
    "C": "IOL",
    "G": "IOL",
    "OG": "IOL",
    "LG": "IOL",
    "RG": "IOL",
    "IOL": "IOL",
    "DE": "EDGE",
    "EDGE": "EDGE",
    "ED": "EDGE",
    "OLB": "EDGE",
    "ER": "EDGE",
    "DT": "DT",
    "NT": "DT",
    "DL": "DT",
    "ILB": "LB",
    "MLB": "LB",
    "LB": "LB",
    "CB": "CB",
    "FS": "S",
    "SS": "S",
    "S": "S",
    "SAF": "S",
    "DB": "S",
}

BOARD_POSITIONS = {"QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"}
LINK_RE = re.compile(r'href="([^"]+)"', flags=re.I)


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _to_int(value) -> int | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return int(float(txt))
    except ValueError:
        return None


def _norm_school(value: str) -> str:
    txt = " ".join(str(value or "").lower().replace("&amp;", "&").split())
    txt = re.sub(r"[^a-z0-9&\s]", "", txt)
    return txt.strip()


def _extract_link(raw_html: str) -> str:
    m = LINK_RE.search(str(raw_html or ""))
    return html.unescape(m.group(1)).strip() if m else ""


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    v = sorted(values)
    idx = (len(v) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(v) - 1)
    frac = idx - lo
    return v[lo] * (1.0 - frac) + v[hi] * frac


def _mean(values: Iterable[float]) -> float:
    vals = list(values)
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _load_seed_rows(path: Path) -> list[dict]:
    with path.open() as f:
        rows = list(csv.DictReader(f))
    out = []
    for row in rows:
        out.append(
            {
                "player_name": str(row.get("player_name", "")).strip(),
                "name_key": canonical_player_name(row.get("player_name", "")),
                "school": str(row.get("school", "")).strip(),
                "school_key": _norm_school(row.get("school", "")),
                "position": normalize_pos(row.get("pos_raw", "")),
            }
        )
    return out


def _load_combine_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        rows = list(csv.DictReader(f))
    out = []
    for row in rows:
        payload = {k: str(row.get(k, "") or "") for k in COMBINE_FIELDS}
        out.append(payload)
    return out


def _normalize_ras_position(raw_pos: str) -> str:
    pos = str(raw_pos or "").strip().upper()
    pos = pos.replace("/", "")
    mapped = POS_MAP.get(pos, pos)
    mapped = normalize_pos(mapped)
    return mapped if mapped in BOARD_POSITIONS else ""


def _read_ras_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    out = []
    for row in rows:
        player_name = str(row.get("Name", "")).strip()
        if not player_name:
            continue
        ras_score = _to_float(row.get("RAS"))
        if ras_score is None:
            continue
        pos = _normalize_ras_position(row.get("Pos", ""))
        if not pos:
            continue
        out.append(
            {
                "player_name": player_name,
                "player_key": canonical_player_name(player_name),
                "school": str(row.get("College", "")).strip(),
                "school_key": _norm_school(row.get("College", "")),
                "position_raw": str(row.get("Pos", "")).strip(),
                "position": pos,
                "year": _to_int(row.get("Year")),
                "ras_score": round(float(ras_score), 2),
                "source_url": _extract_link(row.get("Link", "")),
            }
        )
    return out


def _select_best_ras_match(
    seed_row: dict,
    ras_rows_by_name: dict[str, list[dict]],
    *,
    min_year: int,
) -> tuple[dict | None, int]:
    candidates = [c for c in ras_rows_by_name.get(seed_row["name_key"], []) if (c.get("year") or 0) >= min_year]
    if not candidates:
        return None, 0
    seed_pos = seed_row["position"]
    compatible = []
    for c in candidates:
        cand_pos = c["position"]
        if cand_pos == seed_pos:
            compatible.append(c)
            continue
        if {cand_pos, seed_pos} <= {"S", "CB"}:
            compatible.append(c)
            continue
    candidates = compatible
    if not candidates:
        return None, 0

    best = None
    best_score = -999
    for c in candidates:
        score = 0
        if c["position"] == seed_row["position"]:
            score += 3
        elif {c["position"], seed_row["position"]} <= {"S", "CB"}:
            score += 2
        if seed_row["school_key"] and c["school_key"] and seed_row["school_key"] == c["school_key"]:
            score += 2
        elif seed_row["school_key"] and c["school_key"] and (
            seed_row["school_key"] in c["school_key"] or c["school_key"] in seed_row["school_key"]
        ):
            score += 1
        if c.get("year") and c["year"] >= 2024:
            score += 1
        if c.get("year") and c["year"] >= 2025:
            score += 1
        if best is None:
            best = c
            best_score = score
            continue
        best_year = best.get("year") or -1
        cur_year = c.get("year") or -1
        if (score, cur_year, c["ras_score"]) > (best_score, best_year, best["ras_score"]):
            best = c
            best_score = score
    return best, best_score


def _write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def _read_existing_hist_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        rows = list(csv.DictReader(f))
    out = []
    for row in rows:
        out.append(
            {
                "player_name": str(row.get("player_name", "")).strip(),
                "player_key": canonical_player_name(row.get("player_key") or row.get("player_name", "")),
                "school": str(row.get("school", "")).strip(),
                "school_key": _norm_school(row.get("school_key") or row.get("school", "")),
                "position_raw": str(row.get("position_raw", "")).strip(),
                "position": normalize_pos(row.get("position", "")),
                "year": _to_int(row.get("year")),
                "ras_score": round(float(_to_float(row.get("ras_score")) or 0.0), 2),
                "source_url": str(row.get("source_url", "")).strip(),
            }
        )
    return [r for r in out if r.get("player_key") and r.get("position") and r.get("ras_score", 0) > 0]


def _merge_historical_rows(existing_rows: list[dict], new_rows: list[dict]) -> tuple[list[dict], int]:
    """Merge existing + new rows by identity key; new rows overwrite existing on conflict."""
    by_key: dict[tuple[str, int | None, str, str], dict] = {}
    for row in existing_rows:
        key = (
            row.get("player_key", ""),
            row.get("year"),
            row.get("position", ""),
            row.get("school_key", ""),
        )
        by_key[key] = row
    before = len(by_key)
    for row in new_rows:
        key = (
            row.get("player_key", ""),
            row.get("year"),
            row.get("position", ""),
            row.get("school_key", ""),
        )
        by_key[key] = row
    merged = list(by_key.values())
    merged.sort(
        key=lambda r: (
            int(r.get("year") or 0),
            str(r.get("position", "")),
            str(r.get("player_name", "")),
        )
    )
    added = max(0, len(by_key) - before)
    return merged, added


def _build_benchmarks(ras_rows: list[dict]) -> list[dict]:
    by_pos: dict[str, list[dict]] = defaultdict(list)
    for row in ras_rows:
        by_pos[row["position"]].append(row)

    out = []
    for pos in sorted(by_pos.keys()):
        vals_all = [float(r["ras_score"]) for r in by_pos[pos]]
        vals_2016 = [float(r["ras_score"]) for r in by_pos[pos] if (r.get("year") or 0) >= 2016]
        base = vals_2016 if len(vals_2016) >= 50 else vals_all
        starter_target = _quantile(base, 0.60)
        impact_target = _quantile(base, 0.75)
        elite_target = _quantile(base, 0.90)
        out.append(
            {
                "position": pos,
                "sample_n_all": len(vals_all),
                "sample_n_since_2016": len(vals_2016),
                "mean_all": round(_mean(vals_all), 3),
                "p25_all": round(_quantile(vals_all, 0.25), 3),
                "p50_all": round(_quantile(vals_all, 0.50), 3),
                "p75_all": round(_quantile(vals_all, 0.75), 3),
                "p90_all": round(_quantile(vals_all, 0.90), 3),
                "mean_since_2016": round(_mean(vals_2016), 3) if vals_2016 else "",
                "p75_since_2016": round(_quantile(vals_2016, 0.75), 3) if vals_2016 else "",
                "starter_target_ras": round(starter_target, 3),
                "impact_target_ras": round(impact_target, 3),
                "elite_target_ras": round(elite_target, 3),
            }
        )
    return out


def _write_report(
    *,
    input_path: Path,
    ras_rows: list[dict],
    benchmark_rows: list[dict],
    matches: list[dict],
    updated_count: int,
    added_count: int,
) -> None:
    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)
    top_unmatched = [m for m in matches if m["match_status"] == "unmatched"][:20]
    lines = [
        "# RAS Import Report (2026)",
        "",
        f"- run_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- input_csv: `{input_path}`",
        f"- ras_rows_loaded: `{len(ras_rows)}`",
        f"- benchmark_positions: `{len(benchmark_rows)}`",
        f"- 2026_seed_players_checked: `{len(matches)}`",
        f"- combine_rows_updated: `{updated_count}`",
        f"- combine_rows_added: `{added_count}`",
        "",
        "## Position Benchmarks (Starter/Impact/Elite Targets)",
        "",
        "| Pos | N | Starter Target | Impact Target | Elite Target |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in benchmark_rows:
        lines.append(
            f"| {row['position']} | {row['sample_n_all']} | {row['starter_target_ras']:.2f} | {row['impact_target_ras']:.2f} | {row['elite_target_ras']:.2f} |"
        )

    lines.extend(
        [
            "",
            "## Top Unmatched 2026 Prospects",
            "",
            "| Player | Pos | School |",
            "|---|---|---|",
        ]
    )
    for row in top_unmatched:
        lines.append(f"| {row['player_name']} | {row['position']} | {row['school']} |")

    REPORT_OUT.write_text("\n".join(lines))


def main() -> None:
    p = argparse.ArgumentParser(description="Import historical RAS CSV and project benchmarks into model inputs.")
    p.add_argument("--input", type=str, default=str(DOWNLOAD_DEFAULT), help="Path to downloaded RAS CSV.")
    p.add_argument(
        "--replace-historical",
        action="store_true",
        help="Replace historical DB with input rows only (default is safe merge).",
    )
    p.add_argument("--overwrite-existing", action="store_true", help="Overwrite existing ras_official in combine file.")
    p.add_argument(
        "--min-year",
        type=int,
        default=2023,
        help="Minimum RAS year allowed when matching current prospects (prevents stale same-name collisions).",
    )
    args = p.parse_args()

    input_path = Path(args.input).expanduser()
    if not input_path.exists():
        raise SystemExit(f"RAS CSV not found: {input_path}")

    input_rows = _read_ras_csv(input_path)
    if not input_rows:
        raise SystemExit("No valid RAS rows parsed from input CSV.")
    existing_rows = _read_existing_hist_rows(HIST_OUT)
    if args.replace_historical:
        ras_rows = input_rows
        merged_added = 0
    else:
        ras_rows, merged_added = _merge_historical_rows(existing_rows, input_rows)

    _write_csv(
        HIST_OUT,
        ras_rows,
        [
            "player_name",
            "player_key",
            "school",
            "school_key",
            "position_raw",
            "position",
            "year",
            "ras_score",
            "source_url",
        ],
    )

    bench_rows = _build_benchmarks(ras_rows)
    _write_csv(
        BENCH_OUT,
        bench_rows,
        [
            "position",
            "sample_n_all",
            "sample_n_since_2016",
            "mean_all",
            "p25_all",
            "p50_all",
            "p75_all",
            "p90_all",
            "mean_since_2016",
            "p75_since_2016",
            "starter_target_ras",
            "impact_target_ras",
            "elite_target_ras",
        ],
    )

    seed_rows = _load_seed_rows(SEED_PATH)
    combine_rows = _load_combine_rows(COMBINE_PATH)
    # Deterministic refresh: clear prior rows created by this importer before rematching.
    combine_rows = [r for r in combine_rows if str(r.get("source", "")).strip() != "RAS Historical Import"]
    combine_by_name = {canonical_player_name(r.get("player_name", "")): r for r in combine_rows if r.get("player_name")}

    ras_by_name: dict[str, list[dict]] = defaultdict(list)
    for row in ras_rows:
        ras_by_name[row["player_key"]].append(row)

    today = dt.date.today().isoformat()
    matches = []
    updated_count = 0
    added_count = 0

    for seed in seed_rows:
        best, confidence = _select_best_ras_match(seed, ras_by_name, min_year=args.min_year)
        if best is None or confidence < 2:
            matches.append(
                {
                    "player_name": seed["player_name"],
                    "position": seed["position"],
                    "school": seed["school"],
                    "match_status": "unmatched",
                    "match_confidence": confidence,
                    "matched_ras": "",
                    "matched_year": "",
                    "matched_position": "",
                    "matched_school": "",
                    "source_url": "",
                }
            )
            continue

        key = seed["name_key"]
        existing = combine_by_name.get(key)
        ras_str = f"{float(best['ras_score']):.2f}"
        if existing is None:
            new_row = {k: "" for k in COMBINE_FIELDS}
            new_row["player_name"] = seed["player_name"]
            new_row["school"] = seed["school"]
            new_row["position"] = seed["position"]
            new_row["ras_official"] = ras_str
            new_row["source"] = "RAS Historical Import"
            new_row["last_updated"] = today
            combine_rows.append(new_row)
            combine_by_name[key] = new_row
            added_count += 1
            status = "added"
        else:
            cur_ras = str(existing.get("ras_official", "") or "").strip()
            if args.overwrite_existing or not cur_ras:
                existing["ras_official"] = ras_str
                existing["source"] = existing.get("source") or "RAS Historical Import"
                existing["last_updated"] = today
                updated_count += 1
                status = "updated"
            else:
                status = "kept_existing"

        matches.append(
            {
                "player_name": seed["player_name"],
                "position": seed["position"],
                "school": seed["school"],
                "match_status": status,
                "match_confidence": confidence,
                "matched_ras": ras_str,
                "matched_year": best.get("year") or "",
                "matched_position": best.get("position", ""),
                "matched_school": best.get("school", ""),
                "source_url": best.get("source_url", ""),
            }
        )

    combine_rows.sort(key=lambda r: canonical_player_name(r.get("player_name", "")))
    _write_csv(COMBINE_PATH, combine_rows, COMBINE_FIELDS)
    _write_csv(
        MATCH_OUT,
        matches,
        [
            "player_name",
            "position",
            "school",
            "match_status",
            "match_confidence",
            "matched_ras",
            "matched_year",
            "matched_position",
            "matched_school",
            "source_url",
        ],
    )

    _write_report(
        input_path=input_path,
        ras_rows=ras_rows,
        benchmark_rows=bench_rows,
        matches=matches,
        updated_count=updated_count,
        added_count=added_count,
    )

    print(f"RAS rows loaded: {len(ras_rows)}")
    print(f"Input rows parsed: {len(input_rows)}")
    print(f"Existing historical rows before merge: {len(existing_rows)}")
    print(f"Rows added from input merge: {merged_added}")
    print(f"Historical mode: {'replace' if args.replace_historical else 'merge'}")
    print(f"Benchmarks written: {BENCH_OUT}")
    print(f"Historical DB written: {HIST_OUT}")
    print(f"2026 match report: {MATCH_OUT}")
    print(f"Combine updated: {COMBINE_PATH}")
    print(f"Combine rows updated: {updated_count}")
    print(f"Combine rows added: {added_count}")
    print(f"Report: {REPORT_OUT}")


if __name__ == "__main__":
    main()
