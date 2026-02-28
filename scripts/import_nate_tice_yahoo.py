#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.analyst_language_loader import aggregate_linguistic_signals  # noqa: E402
from src.ingest.rankings_loader import normalize_pos  # noqa: E402


SOURCE = "Yahoo_Nate_Tice_2026"
MANUAL_PATH = ROOT / "data" / "sources" / "manual" / "nate_tice_yahoo_2026_manual.csv"
ANALYST_SEED_PATH = ROOT / "data" / "sources" / "analyst_rankings_seed.csv"
REPORTS_PATH = ROOT / "data" / "processed" / "analyst_reports_2026.csv"
LING_PATH = ROOT / "data" / "processed" / "analyst_linguistic_signals_2026.csv"
REPORT_MD = ROOT / "data" / "outputs" / "nate_tice_import_report.md"


def _safe_rank(value, default: int = 9999) -> int:
    try:
        txt = str(value).strip()
        if not txt:
            return default
        return int(float(txt))
    except (TypeError, ValueError):
        return default


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _union_fieldnames(rows: list[dict], preferred: list[str]) -> list[str]:
    out = list(preferred)
    seen = set(out)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                out.append(key)
                seen.add(key)
    return out


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _build_rows(manual_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    rank_rows: list[dict] = []
    report_rows: list[dict] = []

    for row in manual_rows:
        rank = _safe_rank(row.get("source_rank"))
        if rank <= 0 or rank >= 9999:
            continue
        payload_rank = {
            "source": SOURCE,
            "snapshot_date": str(row.get("snapshot_date", "")).strip() or dt.date.today().isoformat(),
            "source_rank": rank,
            "player_name": str(row.get("player_name", "")).strip(),
            "school": str(row.get("school", "")).strip(),
            "position": normalize_pos(str(row.get("position", "")).strip()),
            "source_url": str(row.get("source_url", "")).strip(),
        }
        if not payload_rank["player_name"] or not payload_rank["position"]:
            continue
        rank_rows.append(payload_rank)

        payload_report = dict(payload_rank)
        payload_report["report_text"] = str(row.get("report_text", "")).strip()
        payload_report["nate_summary"] = str(row.get("report_text", "")).strip()
        payload_report["nate_source_note"] = str(row.get("source_note", "")).strip()
        report_rows.append(payload_report)

    rank_rows.sort(key=lambda r: _safe_rank(r.get("source_rank")))
    report_rows.sort(key=lambda r: _safe_rank(r.get("source_rank")))
    return rank_rows, report_rows


def _merge_replace_source(existing: list[dict], new_rows: list[dict]) -> list[dict]:
    keep = [r for r in existing if str(r.get("source", "")).strip() != SOURCE]
    keep.extend(new_rows)
    keep.sort(key=lambda r: (str(r.get("source", "")).strip(), _safe_rank(r.get("source_rank"))))
    return keep


def _write_report(path: Path, manual_count: int, seed_count: int, report_count: int, ling_count: int) -> None:
    lines = [
        "# Nate Tice Yahoo Import Report",
        "",
        f"- generated_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- source: `{SOURCE}`",
        f"- manual rows ingested: `{manual_count}`",
        f"- analyst seed rows now for source: `{seed_count}`",
        f"- analyst report rows now for source: `{report_count}`",
        f"- total linguistic rows regenerated: `{ling_count}`",
        "",
        "Note: This import is built from indexed Yahoo snippet text when direct article fetch is blocked.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def main() -> None:
    manual_rows = _read_csv(MANUAL_PATH)
    if not manual_rows:
        raise SystemExit(f"No manual rows found at {MANUAL_PATH}")

    rank_rows, report_rows = _build_rows(manual_rows)

    seed_existing = _read_csv(ANALYST_SEED_PATH)
    seed_merged = _merge_replace_source(seed_existing, rank_rows)
    _write_csv(
        ANALYST_SEED_PATH,
        seed_merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url"],
    )

    reports_existing = _read_csv(REPORTS_PATH)
    reports_merged = _merge_replace_source(reports_existing, report_rows)
    report_fields = _union_fieldnames(
        reports_merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url", "report_text"],
    )
    _write_csv(REPORTS_PATH, reports_merged, report_fields)

    ling_input_rows = [r for r in reports_merged if str(r.get("report_text", "")).strip()]
    ling_rows = aggregate_linguistic_signals(ling_input_rows)
    _write_csv(
        LING_PATH,
        ling_rows,
        [
            "player_key",
            "position",
            "player_name",
            "school",
            "lang_source_count",
            "lang_text_coverage",
            "lang_trait_processing",
            "lang_trait_technique",
            "lang_trait_explosiveness",
            "lang_trait_physicality",
            "lang_trait_competitiveness",
            "lang_trait_versatility",
            "lang_miller_keyword_hits",
            "lang_miller_coverage",
            "lang_risk_hits",
            "lang_risk_flag",
            "lang_trait_composite",
            "lang_sources",
        ],
    )

    seed_count = sum(1 for r in seed_merged if str(r.get("source", "")).strip() == SOURCE)
    report_count = sum(1 for r in reports_merged if str(r.get("source", "")).strip() == SOURCE)
    _write_report(
        REPORT_MD,
        manual_count=len(rank_rows),
        seed_count=seed_count,
        report_count=report_count,
        ling_count=len(ling_rows),
    )

    print(f"Manual rows imported: {len(rank_rows)}")
    print(f"Analyst seed updated: {ANALYST_SEED_PATH}")
    print(f"Analyst reports updated: {REPORTS_PATH}")
    print(f"Linguistic signals regenerated: {LING_PATH}")
    print(f"Report written: {REPORT_MD}")


if __name__ == "__main__":
    main()
