#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import ssl
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.analyst_language_loader import (  # noqa: E402
    DJ_SOURCE,
    DJ_URL,
    ESPN_SOURCE,
    ESPN_URL,
    aggregate_linguistic_signals,
    parse_dj_top50_page,
    parse_espn_top50_page,
)
from src.ingest.rankings_loader import normalize_pos  # noqa: E402


SOURCES_DIR = ROOT / "data" / "sources"
PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUTS_DIR = ROOT / "data" / "outputs"

ANALYST_SEED_PATH = SOURCES_DIR / "analyst_rankings_seed.csv"
REPORTS_PATH = PROCESSED_DIR / "analyst_reports_2026.csv"
LING_PATH = PROCESSED_DIR / "analyst_linguistic_signals_2026.csv"
REPORT_MD = OUTPUTS_DIR / "analyst_language_pull_report.md"


def _safe_rank(value, default: int = 999) -> int:
    try:
        txt = str(value).strip()
        if not txt:
            return default
        return int(float(txt))
    except (TypeError, ValueError):
        return default


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fetch(url: str) -> str:
    req = urllib.request.Request(
        url=url,
        headers={
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    },
    )
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, timeout=45, context=ssl_ctx) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _merge_analyst_seed(existing: list[dict], new_rank_rows: list[dict]) -> list[dict]:
    keep = [r for r in existing if r.get("source") not in {ESPN_SOURCE, DJ_SOURCE}]
    keep.extend(new_rank_rows)
    keep.sort(key=lambda r: (r.get("source", ""), _safe_rank(r.get("source_rank", 999))))
    return keep


def _load_existing_seed(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _load_existing_reports(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _merge_reports(existing: list[dict], new_rows: list[dict]) -> list[dict]:
    keep = [r for r in existing if r.get("source") not in {ESPN_SOURCE, DJ_SOURCE}]
    keep.extend(new_rows)
    keep.sort(key=lambda r: (r.get("source", ""), _safe_rank(r.get("source_rank", 999))))
    return keep


def _union_fieldnames(rows: list[dict], preferred: list[str]) -> list[str]:
    out = list(preferred)
    seen = set(out)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                out.append(key)
                seen.add(key)
    return out


def _build_rank_rows(report_rows: list[dict]) -> list[dict]:
    out = []
    for row in report_rows:
        out.append(
            {
                "source": row["source"],
                "snapshot_date": row["snapshot_date"],
                "source_rank": int(row["source_rank"]),
                "player_name": row["player_name"],
                "school": row["school"],
                "position": normalize_pos(row["position"]),
                "source_url": row["source_url"],
            }
        )
    out.sort(key=lambda r: (r["source"], int(r["source_rank"])))
    return out


def _write_report_md(
    path: Path,
    espn_rows: list[dict],
    dj_rows: list[dict],
    ling_rows: list[dict],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    top = sorted(ling_rows, key=lambda r: float(r.get("lang_trait_composite", 0.0)), reverse=True)[:15]
    risk = sorted(ling_rows, key=lambda r: int(r.get("lang_risk_hits", 0)), reverse=True)[:15]

    lines = [
        "# Analyst Linguistic Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- ESPN rows: `{len(espn_rows)}`",
        f"- Daniel Jeremiah rows: `{len(dj_rows)}`",
        f"- linguistic signal rows: `{len(ling_rows)}`",
        "",
        "## Top Composite Language Scores",
        "",
        "| Player | Pos | Composite | Coverage | Sources |",
        "|---|---|---:|---:|---|",
    ]

    for row in top:
        lines.append(
            f"| {row.get('player_name','')} | {row.get('position','')} | "
            f"{row.get('lang_trait_composite','')} | {row.get('lang_text_coverage','')} | "
            f"{row.get('lang_sources','')} |"
        )

    lines.extend(
        [
            "",
            "## Highest Risk Language Hits",
            "",
            "| Player | Pos | Risk Hits | Risk Flag | Sources |",
            "|---|---|---:|---:|---|",
        ]
    )

    for row in risk:
        lines.append(
            f"| {row.get('player_name','')} | {row.get('position','')} | "
            f"{row.get('lang_risk_hits','')} | {row.get('lang_risk_flag','')} | {row.get('lang_sources','')} |"
        )

    path.write_text("\n".join(lines))


def main() -> None:
    snapshot_date = dt.date.today().isoformat()

    espn_html = _fetch(ESPN_URL)
    dj_html = _fetch(DJ_URL)

    espn_rows = parse_espn_top50_page(espn_html, snapshot_date=snapshot_date, source_url=ESPN_URL)
    dj_rows = parse_dj_top50_page(dj_html, snapshot_date=snapshot_date, source_url=DJ_URL)
    report_rows = espn_rows + dj_rows
    report_rows.sort(key=lambda r: (r["source"], _safe_rank(r.get("source_rank"))))

    rank_rows = _build_rank_rows(report_rows)
    merged_seed = _merge_analyst_seed(_load_existing_seed(ANALYST_SEED_PATH), rank_rows)

    _write_csv(
        ANALYST_SEED_PATH,
        merged_seed,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url"],
    )
    merged_reports = _merge_reports(_load_existing_reports(REPORTS_PATH), report_rows)
    ling_input_rows = [r for r in merged_reports if str(r.get("report_text", "")).strip()]
    ling_rows = aggregate_linguistic_signals(ling_input_rows)
    report_fields = _union_fieldnames(
        merged_reports,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url", "report_text"],
    )
    _write_csv(REPORTS_PATH, merged_reports, report_fields)
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
    _write_report_md(REPORT_MD, espn_rows=espn_rows, dj_rows=dj_rows, ling_rows=ling_rows)

    print(f"ESPN rows parsed: {len(espn_rows)}")
    print(f"Daniel Jeremiah rows parsed: {len(dj_rows)}")
    print(f"Analyst reports written: {REPORTS_PATH}")
    print(f"Linguistic signals written: {LING_PATH}")
    print(f"Analyst rankings seed updated: {ANALYST_SEED_PATH}")
    print(f"Summary report: {REPORT_MD}")


if __name__ == "__main__":
    main()
