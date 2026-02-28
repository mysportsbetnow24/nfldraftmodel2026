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


FP_SOURCE = "FantasyPros_Scouting_2026"
FP_URL = "https://www.fantasypros.com/nfl-draft-scouting-reports/"

PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUTS_DIR = ROOT / "data" / "outputs"
REPORTS_PATH = PROCESSED_DIR / "analyst_reports_2026.csv"
FP_STRUCTURED_PATH = PROCESSED_DIR / "fantasypros_scouting_structured_2026.csv"
FP_REPORT_PATH = OUTPUTS_DIR / "fantasypros_scouting_pull_report.md"

TAG_RE = re.compile(r"(?s)<[^>]+>")
WS_RE = re.compile(r"\s+")
FP_PLAYER_RE = re.compile(
    r"^(?P<name>.+?)\s+\((?P<pos>[A-Za-z/]+)\s*[–-]\s*(?P<school>.+?)\)\s*$",
    re.I,
)


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


def _to_text_lines(page_html: str) -> list[str]:
    cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", page_html)
    cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = re.sub(r"(?s)</p>", "\n", cleaned)
    cleaned = re.sub(r"(?s)</h[1-6]>", "\n", cleaned)
    cleaned = TAG_RE.sub(" ", cleaned)
    cleaned = html.unescape(cleaned).replace("\u00a0", " ")
    lines: list[str] = []
    for raw in cleaned.splitlines():
        line = WS_RE.sub(" ", raw).strip()
        if line:
            lines.append(line)
    return lines


def _is_noise(line: str) -> bool:
    low = line.lower()
    if low in {
        "nfl draft scouting reports",
        "2026 nfl draft scouting reports",
        "nfl draft rankings",
        "nfl draft",
    }:
        return True
    if low.startswith("more 2026 nfl draft scouting reports"):
        return True
    if low.startswith("all rights reserved"):
        return True
    if low.startswith("site map"):
        return True
    if low.startswith("contact us"):
        return True
    return False


def parse_fantasypros_scouting_page(page_html: str, snapshot_date: str) -> list[dict]:
    lines = _to_text_lines(page_html)
    rows: list[dict] = []
    current: dict | None = None
    source_rank = 1

    def flush_current() -> None:
        nonlocal current
        if not current:
            return
        summary = _safe_str(current.get("fp_summary"))
        projection = _safe_str(current.get("fp_projection"))
        body = _safe_str(current.get("report_text"))
        # Keep only rows with meaningful scouting content.
        if not summary and not projection and len(body.split()) < 8:
            current = None
            return
        merged = " ".join(x for x in [summary, f"Projection: {projection}" if projection else "", body] if x).strip()
        current["report_text"] = merged or summary or f"{current.get('player_name', '')} scouting entry."
        rows.append(current)
        current = None

    for line in lines:
        m = FP_PLAYER_RE.match(line)
        if m:
            flush_current()
            raw_pos = _safe_str(m.group("pos")).upper()
            raw_pos = {"OL": "IOL", "OC": "IOL", "OG": "IOL", "LT": "OT", "RT": "OT", "FS": "S", "SS": "S"}.get(
                raw_pos, raw_pos
            )
            pos = normalize_pos(raw_pos)
            if pos == "DL":
                pos = "DT"
            current = {
                "source": FP_SOURCE,
                "snapshot_date": snapshot_date,
                "source_rank": source_rank,
                "player_name": _safe_str(m.group("name")),
                "school": _safe_str(m.group("school")),
                "position": pos,
                "source_url": FP_URL,
                "report_text": "",
                "fp_summary": "",
                "fp_projection": "",
            }
            source_rank += 1
            continue

        if not current or _is_noise(line):
            continue

        low = line.lower()
        if low.startswith("projection:"):
            current["fp_projection"] = _safe_str(line.split(":", 1)[1])
            continue

        # First substantial sentence becomes summary; remaining lines are detail text.
        if not current.get("fp_summary") and len(line.split()) >= 7:
            current["fp_summary"] = line
        else:
            prior = _safe_str(current.get("report_text"))
            current["report_text"] = f"{prior} {line}".strip() if prior else line

    flush_current()

    # Deduplicate by player+position, prefer lowest source_rank.
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


def _merge_reports(rows: list[dict]) -> int:
    existing = _load_csv(REPORTS_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != FP_SOURCE]
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
            "fp_summary",
            "fp_projection",
        ],
    )
    _write_csv(REPORTS_PATH, merged, fields)
    return len(rows)


def _write_report(path: Path, rows: list[dict], warnings: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# FantasyPros Scouting Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- rows_loaded: `{len(rows)}`",
        f"- rows_with_summary: `{sum(1 for r in rows if _safe_str(r.get('fp_summary')))}`",
        f"- rows_with_projection: `{sum(1 for r in rows if _safe_str(r.get('fp_projection')))}`",
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
            "| Rank | Player | Pos | School | Projection |",
            "|---:|---|---|---|---|",
        ]
    )
    for row in rows[:25]:
        lines.append(
            f"| {row.get('source_rank','')} | {row.get('player_name','')} | {row.get('position','')} | "
            f"{row.get('school','')} | {row.get('fp_projection','')} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    snapshot_date = dt.date.today().isoformat()
    warnings: list[str] = []

    page_html = _fetch(FP_URL)
    rows = parse_fantasypros_scouting_page(page_html, snapshot_date)
    if not rows:
        warnings.append("Parser returned 0 rows; page format may have changed.")

    _write_csv(
        FP_STRUCTURED_PATH,
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
            "fp_summary",
            "fp_projection",
        ],
    )
    _merge_reports(rows)
    _write_report(FP_REPORT_PATH, rows, warnings)

    print(f"Processed rows: {FP_STRUCTURED_PATH}")
    print(f"Merged reports: {REPORTS_PATH}")
    print(f"Report: {FP_REPORT_PATH}")


if __name__ == "__main__":
    main()
