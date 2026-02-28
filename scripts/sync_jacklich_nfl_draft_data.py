#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
import subprocess
import tempfile
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEST = ROOT / "data" / "sources" / "external" / "nfl-draft-data"
DEFAULT_REPORT = ROOT / "data" / "outputs" / "jacklich_sync_report.txt"
DEFAULT_REPO = "https://github.com/JackLich10/nfl-draft-data"

REQUIRED_FILES = [
    "nfl_draft_prospects.csv",
    "nfl_draft_profiles.csv",
    "college_qbr.csv",
    "college_statistics.csv",
    "ids.csv",
]


def _count_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in csv.DictReader(f))


def _year_profile(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        rows = list(csv.DictReader(f))
    year_counter: Counter[int] = Counter()
    for r in rows:
        year = ""
        for key in ("draft_year", "year", "season"):
            v = (r.get(key) or "").strip()
            if v.isdigit():
                year = v
                break
        if year:
            year_counter[int(year)] += 1
    if not year_counter:
        return {"min": "", "max": "", "rows_2026": 0, "rows_2025": 0}
    years = sorted(year_counter)
    return {
        "min": years[0],
        "max": years[-1],
        "rows_2026": year_counter.get(2026, 0),
        "rows_2025": year_counter.get(2025, 0),
    }


def _write_report(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _copy_required(src: Path, dest: Path) -> list[str]:
    dest.mkdir(parents=True, exist_ok=True)
    copied = []
    for name in REQUIRED_FILES:
        s = src / name
        if not s.exists():
            continue
        d = dest / name
        shutil.copy2(s, d)
        copied.append(name)

    # Keep a normalized alias that matches older docs.
    stats_src = dest / "college_statistics.csv"
    stats_alias = dest / "college_stats.csv"
    if stats_src.exists():
        shutil.copy2(stats_src, stats_alias)
        if "college_stats.csv" not in copied:
            copied.append("college_stats.csv")

    readme = src / "README.md"
    if readme.exists():
        shutil.copy2(readme, dest / "README.source.md")
        copied.append("README.source.md")
    return copied


def main() -> None:
    p = argparse.ArgumentParser(description="Sync JackLich nfl-draft-data CSVs into local ESPN ingest directory")
    p.add_argument("--repo", type=str, default=DEFAULT_REPO, help="Git repo URL")
    p.add_argument("--execute", action="store_true", help="Clone repo and copy files")
    p.add_argument("--local-src", type=str, default="", help="Use existing local directory instead of cloning")
    p.add_argument("--dest", type=str, default=str(DEFAULT_DEST), help="Destination directory")
    p.add_argument("--report", type=str, default=str(DEFAULT_REPORT), help="Sync report output path")
    args = p.parse_args()

    if not args.execute and not args.local_src:
        print("Dry run only.")
        print(f"Repo: {args.repo}")
        print(f"Destination: {args.dest}")
        print("Use --execute to clone, or --local-src <dir> to copy from an existing checkout.")
        return

    dest = Path(args.dest)
    src_dir: Path
    cleanup_tmp = None

    if args.local_src:
        src_dir = Path(args.local_src)
        if not src_dir.exists():
            raise SystemExit(f"Local source not found: {src_dir}")
    else:
        cleanup_tmp = tempfile.TemporaryDirectory()
        src_dir = Path(cleanup_tmp.name) / "nfl-draft-data"
        subprocess.run(["git", "clone", "--depth", "1", args.repo, str(src_dir)], check=True)

    copied = _copy_required(src_dir, dest)

    missing = [name for name in REQUIRED_FILES if not (dest / name).exists()]
    prospects_path = dest / "nfl_draft_prospects.csv"
    qbr_path = dest / "college_qbr.csv"
    stats_path = dest / "college_statistics.csv"

    row_counts = {}
    for name in REQUIRED_FILES:
        path = dest / name
        row_counts[name] = _count_rows(path) if path.exists() else 0

    prospects_year = _year_profile(prospects_path) if prospects_path.exists() else {}
    qbr_year = _year_profile(qbr_path) if qbr_path.exists() else {}
    stats_year = _year_profile(stats_path) if stats_path.exists() else {}

    lines = [
        "JackLich dataset sync report",
        f"repo: {args.repo}",
        f"source_dir: {src_dir}",
        f"dest_dir: {dest}",
        "",
        f"copied_files: {', '.join(sorted(copied)) if copied else '(none)'}",
        f"missing_required_files: {', '.join(missing) if missing else '(none)'}",
        "",
        "row_counts:",
        f"  nfl_draft_prospects.csv: {row_counts.get('nfl_draft_prospects.csv',0)}",
        f"  nfl_draft_profiles.csv: {row_counts.get('nfl_draft_profiles.csv',0)}",
        f"  college_qbr.csv: {row_counts.get('college_qbr.csv',0)}",
        f"  college_statistics.csv: {row_counts.get('college_statistics.csv',0)}",
        f"  ids.csv: {row_counts.get('ids.csv',0)}",
        "",
        "year_coverage:",
        f"  prospects: min={prospects_year.get('min','')} max={prospects_year.get('max','')} y2026={prospects_year.get('rows_2026',0)}",
        f"  qbr: min={qbr_year.get('min','')} max={qbr_year.get('max','')} y2026={qbr_year.get('rows_2026',0)}",
        f"  college_stats: min={stats_year.get('min','')} max={stats_year.get('max','')} y2026={stats_year.get('rows_2026',0)}",
        "",
        "note: this sync preserves source format; your ESPN loader maps these fields into model signals.",
    ]

    _write_report(Path(args.report), lines)
    print(f"Synced to: {dest}")
    print(f"Report: {args.report}")

    if cleanup_tmp is not None:
        cleanup_tmp.cleanup()


if __name__ == "__main__":
    main()
