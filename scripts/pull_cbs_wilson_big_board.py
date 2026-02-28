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


CBS_WILSON_SOURCE = "CBS_Wilson_BigBoard_2026"
CBS_WILSON_URL = "https://www.cbssports.com/nfl/draft/news/wilsons-2026-nfl-draft-big-board-top-125-prospect-rankings/"

SOURCES_DIR = ROOT / "data" / "sources"
PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUTS_DIR = ROOT / "data" / "outputs"

ANALYST_SEED_PATH = SOURCES_DIR / "analyst_rankings_seed.csv"
ANALYST_REPORTS_PATH = PROCESSED_DIR / "analyst_reports_2026.csv"
PROCESSED_PATH = PROCESSED_DIR / "cbs_wilson_big_board_2026.csv"
REPORT_PATH = OUTPUTS_DIR / "cbs_wilson_big_board_pull_report.md"

P_RE = re.compile(r"(?is)<p[^>]*>(.*?)</p>")
TAG_RE = re.compile(r"(?is)<[^>]+>")
WS_RE = re.compile(r"\s+")
RANK_LINE_RE = re.compile(r"^\s*(\d{1,3})\.\s*(.+?),\s*([A-Za-z/]+),\s*(.+?)\s*$")


def _safe_str(value) -> str:
    return str(value or "").strip()


def _safe_rank(value, default: int = 9999) -> int:
    txt = _safe_str(value)
    if not txt:
        return default
    try:
        return int(float(txt))
    except ValueError:
        return default


def _clean_text(raw: str) -> str:
    txt = html.unescape(TAG_RE.sub(" ", raw or "")).replace("\u00a0", " ")
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


def parse_cbs_wilson_big_board(page_html: str, snapshot_date: str) -> list[dict]:
    rows: list[dict] = []
    for p_html in P_RE.findall(page_html):
        line = _clean_text(p_html)
        m = RANK_LINE_RE.match(line)
        if not m:
            continue
        rank = int(m.group(1))
        if not (1 <= rank <= 300):
            continue
        player_name = _safe_str(m.group(2))
        pos = normalize_pos(_safe_str(m.group(3)).upper())
        if pos == "DL":
            pos = "DT"
        school = _safe_str(m.group(4))
        if not player_name or not pos:
            continue
        rows.append(
            {
                "source": CBS_WILSON_SOURCE,
                "snapshot_date": snapshot_date,
                "source_rank": rank,
                "player_name": player_name,
                "school": school,
                "position": pos,
                "source_url": CBS_WILSON_URL,
                "report_text": f"CBS Ryan Wilson Top-125 ranking entry: {player_name} ({pos}, {school}) ranked #{rank}.",
                "cbs_wilson_rank": rank,
            }
        )

    best: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (_safe_str(row.get("player_name")).lower(), _safe_str(row.get("position")).upper())
        cur = best.get(key)
        if cur is None or _safe_rank(row.get("source_rank")) < _safe_rank(cur.get("source_rank")):
            best[key] = row
    deduped = list(best.values())
    deduped.sort(key=lambda r: _safe_rank(r.get("source_rank")))
    return deduped


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _union_fieldnames(rows: list[dict], preferred: list[str]) -> list[str]:
    out = list(preferred)
    seen = set(out)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                out.append(key)
                seen.add(key)
    return out


def _merge_analyst_seed(rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_SEED_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != CBS_WILSON_SOURCE]
    additions = [
        {
            "source": CBS_WILSON_SOURCE,
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
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _safe_rank(r.get("source_rank"))))
    _write_csv(
        ANALYST_SEED_PATH,
        merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url"],
    )
    return len(additions)


def _merge_analyst_reports(rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_REPORTS_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != CBS_WILSON_SOURCE]
    merged = keep + rows
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _safe_rank(r.get("source_rank"))))
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
            "cbs_wilson_rank",
        ],
    )
    _write_csv(ANALYST_REPORTS_PATH, merged, fields)
    return len(rows)


def _write_report(path: Path, rows: list[dict], warnings: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# CBS Wilson Big Board Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- rows_loaded: `{len(rows)}`",
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
            "| Rank | Player | Pos | School |",
            "|---:|---|---|---|",
        ]
    )
    for r in rows[:30]:
        lines.append(
            f"| {r.get('source_rank','')} | {r.get('player_name','')} | {r.get('position','')} | {r.get('school','')} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    snapshot_date = dt.date.today().isoformat()
    warnings: list[str] = []

    page_html = _fetch(CBS_WILSON_URL)
    rows = parse_cbs_wilson_big_board(page_html, snapshot_date=snapshot_date)
    if not rows:
        warnings.append("Parser returned 0 rows; page format may have changed.")

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
            "cbs_wilson_rank",
        ],
    )
    _merge_analyst_seed(rows)
    _merge_analyst_reports(rows)
    _write_report(REPORT_PATH, rows, warnings)

    print(f"Processed rows: {PROCESSED_PATH}")
    print(f"Merged seed: {ANALYST_SEED_PATH}")
    print(f"Merged reports: {ANALYST_REPORTS_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
