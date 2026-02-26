#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


SI_SOURCE = "SI_FCS_Scouting_2026"

SOURCES_DIR = ROOT / "data" / "sources"
PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUTS_DIR = ROOT / "data" / "outputs"

URLS_PATH = SOURCES_DIR / "manual" / "si_fcs_scouting_urls_2026.csv"
MANUAL_REPORTS_PATH = SOURCES_DIR / "manual" / "si_fcs_scouting_reports_2026.csv"
ANALYST_SEED_PATH = SOURCES_DIR / "analyst_rankings_seed.csv"
ANALYST_REPORTS_PATH = PROCESSED_DIR / "analyst_reports_2026.csv"
PROCESSED_PATH = PROCESSED_DIR / "si_fcs_scouting_structured_2026.csv"
REPORT_PATH = OUTPUTS_DIR / "si_fcs_scouting_pull_report.md"

POS_RE = r"(?:QB|RB|WR|TE|OT|IOL|EDGE|DT|DL|DE|LB|CB|S|DB|C|G|T)"
TITLE_NAME_RE = re.compile(r"2026\s+NFL Draft Scouting Report:\s*(?P<body>.+)$", re.I)
POS_IN_TITLE_RE = re.compile(rf"\b(?P<pos>{POS_RE})\b", re.I)
RANK_RE = re.compile(r"(?:No\.?|#)\s*(\d{1,3})", re.I)
TAG_RE = re.compile(r"(?s)<[^>]+>")
WS_RE = re.compile(r"\s+")


def _to_int(value) -> int | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return int(float(txt))
    except ValueError:
        return None


def _safe_str(value) -> str:
    return str(value or "").strip()


def _sort_rank(value) -> int:
    rank = _to_int(value)
    return rank if rank is not None else 9999


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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


def _text_lines(page_html: str) -> list[str]:
    cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", page_html)
    cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = TAG_RE.sub("\n", cleaned)
    cleaned = html.unescape(cleaned)
    out = []
    for raw in cleaned.splitlines():
        line = WS_RE.sub(" ", raw).strip()
        if line:
            out.append(line)
    return out


def _find_section(lines: list[str], heading_aliases: set[str], stop_aliases: set[str]) -> str:
    idx = -1
    for i, line in enumerate(lines):
        if line.lower().strip(":") in heading_aliases:
            idx = i
            break
    if idx < 0:
        return ""
    chunks = []
    for j in range(idx + 1, min(idx + 25, len(lines))):
        cur = lines[j].strip()
        low = cur.lower().strip(":")
        if low in stop_aliases:
            break
        if len(cur) < 2:
            continue
        chunks.append(cur)
    return " ".join(chunks).strip()


def _fallback_paragraph_summary(page_html: str) -> str:
    paras = re.findall(r"<p[^>]*>(.*?)</p>", page_html, flags=re.I | re.S)
    texts = []
    for p in paras:
        txt = WS_RE.sub(" ", TAG_RE.sub(" ", html.unescape(p))).strip()
        low = txt.lower()
        if not txt:
            continue
        if "copyright" in low or "advertisement" in low:
            continue
        texts.append(txt)
        if len(texts) >= 4:
            break
    return " ".join(texts).strip()


def _extract_rank(page_html: str, lines: list[str]) -> int | None:
    for m in RANK_RE.finditer(page_html):
        rank = _to_int(m.group(1))
        if rank is not None and 1 <= rank <= 400:
            return rank
    for line in lines[:60]:
        m = RANK_RE.search(line)
        if m:
            rank = _to_int(m.group(1))
            if rank is not None and 1 <= rank <= 400:
                return rank
    return None


def _extract_title_identity(page_html: str) -> tuple[str, str, str]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", page_html, flags=re.I | re.S)
    if not title_match:
        return "", "", ""
    title_txt = WS_RE.sub(" ", TAG_RE.sub(" ", html.unescape(title_match.group(1)))).strip()
    body_m = TITLE_NAME_RE.search(title_txt)
    if not body_m:
        return "", "", ""
    body = _safe_str(body_m.group("body"))
    # Common SI format: "School POS Player Name"
    # fallback parse uses row metadata later if identity cannot be confidently extracted.
    pos_m = POS_IN_TITLE_RE.search(body)
    pos = normalize_pos(_safe_str(pos_m.group("pos")).upper()) if pos_m else ""
    name = body
    school = ""
    if pos_m:
        pos_txt = pos_m.group("pos")
        parts = re.split(rf"\b{re.escape(pos_txt)}\b", body, flags=re.I)
        if len(parts) == 2:
            school = _safe_str(parts[0]).strip("- ")
            name = _safe_str(parts[1]).strip("- ")
    return name, pos, school


def parse_si_report(page_html: str, source_url: str, snapshot_date: str) -> dict:
    lines = _text_lines(page_html)
    name, pos, school = _extract_title_identity(page_html)
    source_rank = _extract_rank(page_html, lines)

    stops = {
        "strengths",
        "weaknesses",
        "areas to improve",
        "summary",
        "projection",
        "bottom line",
        "report",
        "pros",
        "cons",
        "nfl comparison",
        "career stats",
    }
    strengths = _find_section(lines, {"strengths", "pros", "what he does well"}, stops)
    concerns = _find_section(lines, {"weaknesses", "areas to improve", "cons", "concerns"}, stops)
    summary = _find_section(lines, {"summary", "projection", "bottom line", "report", "nfl projection"}, stops)
    if not summary:
        summary = _fallback_paragraph_summary(page_html)

    report_text_parts = []
    if strengths:
        report_text_parts.append(f"Strengths: {strengths}.")
    if concerns:
        report_text_parts.append(f"Concerns: {concerns}.")
    if summary:
        report_text_parts.append(f"Summary: {summary}.")
    report_text = " ".join(report_text_parts).strip()

    return {
        "source": SI_SOURCE,
        "snapshot_date": snapshot_date,
        "source_rank": source_rank if source_rank is not None else "",
        "player_name": name,
        "school": school,
        "position": pos,
        "source_url": source_url,
        "report_text": report_text,
        "si_rank": source_rank if source_rank is not None else "",
        "si_strengths": strengths,
        "si_concerns": concerns,
        "si_summary": summary,
    }


def _load_url_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _load_manual_reports(path: Path, snapshot_date: str) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with path.open() as f:
        for row in csv.DictReader(f):
            source_rank = _to_int(row.get("source_rank", row.get("si_rank", row.get("rank", ""))))
            source_rank_val = source_rank if source_rank is not None else ""
            payload = {
                "source": SI_SOURCE,
                "snapshot_date": _safe_str(row.get("snapshot_date")) or snapshot_date,
                "source_rank": source_rank_val,
                "player_name": _safe_str(row.get("player_name")),
                "school": _safe_str(row.get("school")),
                "position": normalize_pos(_safe_str(row.get("position")).upper()),
                "source_url": _safe_str(row.get("source_url", row.get("url"))),
                "report_text": _safe_str(row.get("report_text")),
                "si_rank": source_rank_val,
                "si_strengths": _safe_str(row.get("si_strengths", row.get("strengths"))),
                "si_concerns": _safe_str(row.get("si_concerns", row.get("concerns"))),
                "si_summary": _safe_str(row.get("si_summary", row.get("summary"))),
            }
            if payload["player_name"] and payload["position"]:
                out.append(payload)
    return out


def _dedupe_best(rows: list[dict]) -> list[dict]:
    best: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (_safe_str(row.get("player_name")).lower(), normalize_pos(_safe_str(row.get("position")).upper()))
        if not key[0] or not key[1]:
            continue
        cur = best.get(key)
        if cur is None:
            best[key] = row
            continue
        new_rank = _sort_rank(row.get("source_rank"))
        cur_rank = _sort_rank(cur.get("source_rank"))
        if new_rank < cur_rank:
            best[key] = row
    out = list(best.values())
    out.sort(key=lambda r: (_sort_rank(r.get("source_rank")), _safe_str(r.get("player_name"))))
    return out


def _merge_analyst_seed(rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_SEED_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != SI_SOURCE]
    additions = []
    for r in rows:
        rank = _to_int(r.get("source_rank"))
        if rank is None:
            continue
        additions.append(
            {
                "source": SI_SOURCE,
                "snapshot_date": r["snapshot_date"],
                "source_rank": rank,
                "player_name": r["player_name"],
                "school": r["school"],
                "position": r["position"],
                "source_url": r["source_url"],
            }
        )
    merged = keep + additions
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _sort_rank(r.get("source_rank"))))
    _write_csv(
        ANALYST_SEED_PATH,
        merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url"],
    )
    return len(additions)


def _merge_analyst_reports(rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_REPORTS_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != SI_SOURCE]
    merged = keep + rows
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _sort_rank(r.get("source_rank"))))
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
            "si_rank",
            "si_strengths",
            "si_concerns",
            "si_summary",
        ],
    )
    _write_csv(ANALYST_REPORTS_PATH, merged, fields)
    return len(rows)


def _write_report(path: Path, rows: list[dict], attempted: int, failed: int, warnings: list[str], added_seed: int, added_reports: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top = rows[:20]
    with_rank = sum(1 for r in rows if _to_int(r.get("source_rank")) is not None)
    lines = [
        "# SI FCS Scouting Reports Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- urls_attempted: `{attempted}`",
        f"- urls_failed: `{failed}`",
        f"- rows_loaded: `{len(rows)}`",
        f"- rows_with_rank: `{with_rank}`",
        f"- analyst_seed_added: `{added_seed}`",
        f"- analyst_reports_added: `{added_reports}`",
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
            "| Rank | Player | Pos | School | Strengths Present | Concerns Present |",
            "|---:|---|---|---|---|---|",
        ]
    )
    for r in top:
        lines.append(
            f"| {_safe_str(r.get('source_rank'))} | {_safe_str(r.get('player_name'))} | {_safe_str(r.get('position'))} | "
            f"{_safe_str(r.get('school'))} | {'yes' if _safe_str(r.get('si_strengths')) else 'no'} | "
            f"{'yes' if _safe_str(r.get('si_concerns')) else 'no'} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    p = argparse.ArgumentParser(description="Pull SI/FCS scouting report rationale into structured scouting features.")
    p.add_argument("--urls-csv", default=str(URLS_PATH), help="CSV file with `source_url`/`url` rows for SI/FCS reports.")
    p.add_argument(
        "--manual-reports-csv",
        default=str(MANUAL_REPORTS_PATH),
        help="Manual fallback CSV with parsed report rows when fetch fails.",
    )
    p.add_argument("--skip-fetch", action="store_true", help="Skip web fetch and only use manual report CSV.")
    args = p.parse_args()

    snapshot_date = dt.date.today().isoformat()
    warnings: list[str] = []
    rows: list[dict] = []
    attempted = 0
    failed = 0

    if not args.skip_fetch:
        url_rows = _load_url_rows(Path(args.urls_csv))
        if not url_rows:
            warnings.append("SI/FCS URLs CSV missing/empty; fetch phase skipped.")
        for row in url_rows:
            url = _safe_str(row.get("source_url", row.get("url")))
            if not url:
                continue
            attempted += 1
            try:
                page_html = _fetch(url)
                parsed = parse_si_report(page_html, source_url=url, snapshot_date=snapshot_date)
                if not parsed.get("player_name"):
                    parsed["player_name"] = _safe_str(row.get("player_name"))
                if not parsed.get("position"):
                    parsed["position"] = normalize_pos(_safe_str(row.get("position")).upper())
                if not parsed.get("school"):
                    parsed["school"] = _safe_str(row.get("school"))
                if not parsed.get("source_rank"):
                    parsed["source_rank"] = _to_int(row.get("source_rank", row.get("rank", ""))) or ""
                    parsed["si_rank"] = parsed["source_rank"]
                if parsed.get("player_name") and parsed.get("position"):
                    rows.append(parsed)
                else:
                    failed += 1
                    warnings.append(f"SI/FCS parse incomplete for URL: {url}")
            except Exception as exc:
                failed += 1
                warnings.append(f"SI/FCS fetch failed for {url}: {exc}")

    rows = _dedupe_best(rows)
    if not rows:
        manual_rows = _load_manual_reports(Path(args.manual_reports_csv), snapshot_date=snapshot_date)
        if manual_rows:
            rows = _dedupe_best(manual_rows)
        else:
            warnings.append("SI/FCS manual reports CSV missing/empty.")

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
            "si_rank",
            "si_strengths",
            "si_concerns",
            "si_summary",
        ],
    )
    added_seed = _merge_analyst_seed(rows) if rows else 0
    added_reports = _merge_analyst_reports(rows) if rows else 0
    _write_report(REPORT_PATH, rows, attempted, failed, warnings, added_seed, added_reports)

    print(f"SI/FCS rows: {len(rows)}")
    print(f"URLs attempted: {attempted}")
    print(f"URLs failed: {failed}")
    print(f"Analyst seed rows added: {added_seed}")
    print(f"Analyst report rows added: {added_reports}")
    print(f"Processed rows: {PROCESSED_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
