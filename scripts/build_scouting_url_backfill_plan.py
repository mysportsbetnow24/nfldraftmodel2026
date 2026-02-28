#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

BIG_BOARD_PATH = DATA / "outputs" / "big_board_2026.csv"
ANALYST_REPORTS_PATH = DATA / "processed" / "analyst_reports_2026.csv"

SOURCE_URL_FILES = {
    "TDN_Scouting_2026": DATA / "sources" / "manual" / "tdn_scouting_urls_2026.csv",
    "Bleacher_Report_2026": DATA / "sources" / "manual" / "bleacher_scouting_urls_2026.csv",
    "SI_FCS_Scouting_2026": DATA / "sources" / "manual" / "si_fcs_scouting_urls_2026.csv",
    "AtoZ_Scouting_2026": DATA / "sources" / "manual" / "atoz_scouting_urls_2026.csv",
}

OUT_BACKFILL_CSV = DATA / "outputs" / "scouting_url_backfill_plan_2026.csv"
OUT_REPORT_MD = DATA / "outputs" / "scouting_url_coverage_report_2026.md"


def _safe_int(value: str, default: int = 9999) -> int:
    try:
        txt = str(value or "").strip()
        if not txt:
            return default
        return int(float(txt))
    except (TypeError, ValueError):
        return default


def _norm_name(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _source_domain(source: str) -> str:
    return {
        "TDN_Scouting_2026": "thedraftnetwork.com",
        "Bleacher_Report_2026": "bleacherreport.com",
        "SI_FCS_Scouting_2026": "si.com",
        "AtoZ_Scouting_2026": "atozsports.com",
    }.get(source, "")


def main() -> None:
    top_n = 150
    board_rows = _load_csv(BIG_BOARD_PATH)
    board_rows = sorted(board_rows, key=lambda r: _safe_int(r.get("consensus_rank")))
    top = [r for r in board_rows if _safe_int(r.get("consensus_rank")) <= top_n]

    analyst_rows = _load_csv(ANALYST_REPORTS_PATH)
    by_source_report_names: dict[str, set[str]] = {k: set() for k in SOURCE_URL_FILES}
    for row in analyst_rows:
        src = str(row.get("source", "")).strip()
        if src not in by_source_report_names:
            continue
        if str(row.get("report_text", "")).strip():
            by_source_report_names[src].add(_norm_name(row.get("player_name", "")))

    url_rows_by_source: dict[str, list[dict]] = {}
    url_names_by_source: dict[str, set[str]] = {}
    for source, path in SOURCE_URL_FILES.items():
        rows = _load_csv(path)
        url_rows_by_source[source] = rows
        url_names_by_source[source] = {_norm_name(r.get("player_name", "")) for r in rows if _norm_name(r.get("player_name", ""))}

    backfill_rows: list[dict] = []
    summary_rows: list[dict] = []

    for source in SOURCE_URL_FILES:
        url_names = url_names_by_source[source]
        report_names = by_source_report_names[source]
        missing = []
        for row in top:
            nm = _norm_name(row.get("player_name", ""))
            if not nm:
                continue
            if nm in url_names:
                continue
            missing.append(row)

        for row in missing:
            player = str(row.get("player_name", "")).strip()
            domain = _source_domain(source)
            query = f"https://www.google.com/search?q=site:{domain}+{player.replace(' ', '+')}+2026+nfl+draft+scouting+report"
            backfill_rows.append(
                {
                    "source": source,
                    "consensus_rank": _safe_int(row.get("consensus_rank")),
                    "player_name": player,
                    "position": row.get("position", ""),
                    "school": row.get("school", ""),
                    "url_seed_status": "missing",
                    "search_hint_url": query,
                    "notes": "Add direct player scouting URL.",
                }
            )

        summary_rows.append(
            {
                "source": source,
                "top_n": top_n,
                "top_n_players": len(top),
                "url_seed_rows": len(url_rows_by_source[source]),
                "url_seed_unique_players": len(url_names),
                "analyst_report_players": len(report_names),
                "missing_url_seed_top_n": len(missing),
            }
        )

    backfill_rows.sort(key=lambda r: (r["source"], int(r["consensus_rank"])))

    _write_csv(
        OUT_BACKFILL_CSV,
        backfill_rows,
        [
            "source",
            "consensus_rank",
            "player_name",
            "position",
            "school",
            "url_seed_status",
            "search_hint_url",
            "notes",
        ],
    )

    lines = [
        "# Scouting URL Coverage Report (2026)",
        "",
        f"- generated_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- board_file: `{BIG_BOARD_PATH}`",
        f"- analyst_reports_file: `{ANALYST_REPORTS_PATH}`",
        f"- top_n_scope: `{top_n}`",
        "",
        "## Source Summary",
        "",
        "| Source | URL Seed Rows | URL Seed Players | Report Players | Missing URL Seed In Top-150 |",
        "|---|---:|---:|---:|---:|",
    ]

    for row in summary_rows:
        lines.append(
            f"| {row['source']} | {row['url_seed_rows']} | {row['url_seed_unique_players']} | "
            f"{row['analyst_report_players']} | {row['missing_url_seed_top_n']} |"
        )

    lines.extend(
        [
            "",
            "## Next Action",
            "",
            f"- Fill missing URLs in `{OUT_BACKFILL_CSV}` and merge into source URL seed files.",
            "- Then run source pull scripts and rebuild the board.",
        ]
    )

    OUT_REPORT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT_MD.write_text("\n".join(lines))

    print(f"Wrote: {OUT_BACKFILL_CSV}")
    print(f"Wrote: {OUT_REPORT_MD}")


if __name__ == "__main__":
    main()

