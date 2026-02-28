#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import html
import re
import ssl
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import normalize_pos  # noqa: E402


CBS_SOURCE = "CBS_BigBoard_2026"
CBS_URL = "https://www.cbssports.com/nfl/draft/news/2026-nfl-draft-top-prospects-big-board-rankings/"

SOURCES_DIR = ROOT / "data" / "sources"
PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUTS_DIR = ROOT / "data" / "outputs"

ANALYST_SEED_PATH = SOURCES_DIR / "analyst_rankings_seed.csv"
ANALYST_REPORTS_PATH = PROCESSED_DIR / "analyst_reports_2026.csv"
PROCESSED_PATH = PROCESSED_DIR / "cbs_scouting_structured_2026.csv"
REPORT_PATH = OUTPUTS_DIR / "cbs_scouting_pull_report.md"

TAG_RE = re.compile(r"(?s)<[^>]+>")
WS_RE = re.compile(r"\s+")


def _safe_str(value) -> str:
    return str(value or "").strip()


def _to_int(value) -> int | None:
    txt = _safe_str(value)
    if not txt:
        return None
    try:
        return int(float(txt))
    except ValueError:
        return None


def _clean_text(raw: str) -> str:
    txt = html.unescape(TAG_RE.sub(" ", raw or ""))
    return WS_RE.sub(" ", txt).strip()


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


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _load_csv(path: Path) -> list[dict]:
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


def _parse_heading_text(heading_text: str) -> tuple[int | None, str, str, str]:
    # Expected format:
    # "1. Rueben Bain Jr., EDGE, Miami (Fla.)"
    txt = _clean_text(heading_text)
    m = re.match(r"^\s*(\d{1,3})\.\s*(.+?)\s*,\s*([A-Z/]+)\s*,\s*(.+?)\s*$", txt)
    if not m:
        return None, "", "", ""
    rank = _to_int(m.group(1))
    name = _safe_str(m.group(2))
    pos = normalize_pos(_safe_str(m.group(3)).upper())
    if pos == "DL":
        pos = "DT"
    school = _safe_str(m.group(4))
    return rank, name, pos, school


def parse_cbs_big_board(page_html: str, snapshot_date: str) -> list[dict]:
    rows: list[dict] = []
    h2_iter = list(re.finditer(r"(?is)<h2[^>]*>(.*?)</h2>", page_html))
    if not h2_iter:
        return rows

    for idx, m in enumerate(h2_iter):
        heading_html = m.group(1)
        rank, name, pos, school = _parse_heading_text(heading_html)
        if rank is None or not name or not pos:
            continue

        start = m.end()
        end = h2_iter[idx + 1].start() if idx + 1 < len(h2_iter) else len(page_html)
        block = page_html[start:end]
        p_match = re.search(r"(?is)<p[^>]*>(.*?)</p>", block)
        summary = _clean_text(p_match.group(1)) if p_match else ""

        report_text = summary
        if not report_text:
            report_text = f"CBS big-board entry for {name} ({pos}, {school})."

        rows.append(
            {
                "source": CBS_SOURCE,
                "snapshot_date": snapshot_date,
                "source_rank": rank,
                "player_name": name,
                "school": school,
                "position": pos,
                "source_url": CBS_URL,
                "report_text": report_text,
                "cbs_rank": rank,
                "cbs_summary": summary,
            }
        )

    rows.sort(key=lambda r: int(r["source_rank"]))
    return rows


def _merge_analyst_seed(rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_SEED_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != CBS_SOURCE]
    additions = [
        {
            "source": CBS_SOURCE,
            "snapshot_date": r["snapshot_date"],
            "source_rank": r["source_rank"],
            "player_name": r["player_name"],
            "school": r["school"],
            "position": r["position"],
            "source_url": r["source_url"],
        }
        for r in rows
    ]
    merged = keep + additions
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _to_int(r.get("source_rank")) or 9999))
    _write_csv(
        ANALYST_SEED_PATH,
        merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url"],
    )
    return len(additions)


def _merge_analyst_reports(rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_REPORTS_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != CBS_SOURCE]
    merged = keep + rows
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _to_int(r.get("source_rank")) or 9999))
    fields = _union_fieldnames(
        merged,
        [
            "source",
            "snapshot_date",
            "source_rank",
            "player_name",
            "school",
            "position",
            "source_url",
            "report_text",
            "cbs_rank",
            "cbs_summary",
        ],
    )
    _write_csv(ANALYST_REPORTS_PATH, merged, fields)
    return len(rows)


def _write_report(path: Path, rows: list[dict], warnings: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# CBS Scouting Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- rows_loaded: `{len(rows)}`",
        f"- rows_with_report_text: `{sum(1 for r in rows if _safe_str(r.get('report_text')))}`",
        "",
        "## Warnings",
    ]
    if warnings:
        for w in warnings:
            lines.append(f"- {w}")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Sample Rows",
            "",
            "| Rank | Player | Pos | School | Summary Present |",
            "|---:|---|---|---|---|",
        ]
    )
    for r in rows[:20]:
        lines.append(
            f"| {r.get('source_rank','')} | {r.get('player_name','')} | {r.get('position','')} | "
            f"{r.get('school','')} | {'yes' if _safe_str(r.get('cbs_summary')) else 'no'} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    snapshot_date = dt.date.today().isoformat()
    warnings: list[str] = []

    try:
        html_text = _fetch(CBS_URL)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"CBS fetch failed: {exc}") from exc

    rows = parse_cbs_big_board(html_text, snapshot_date=snapshot_date)
    if not rows:
        warnings.append("No rows parsed from CBS big board page.")

    _write_csv(
        PROCESSED_PATH,
        rows,
        [
            "source",
            "snapshot_date",
            "source_rank",
            "player_name",
            "school",
            "position",
            "source_url",
            "report_text",
            "cbs_rank",
            "cbs_summary",
        ],
    )
    added_seed = _merge_analyst_seed(rows) if rows else 0
    added_reports = _merge_analyst_reports(rows) if rows else 0
    _write_report(REPORT_PATH, rows, warnings)

    print(f"CBS rows: {len(rows)}")
    print(f"Analyst seed rows added: {added_seed}")
    print(f"Analyst report rows added: {added_reports}")
    print(f"Processed rows: {PROCESSED_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
