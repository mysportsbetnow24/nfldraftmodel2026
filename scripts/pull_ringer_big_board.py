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

from src.ingest.rankings_loader import canonical_player_name, normalize_pos  # noqa: E402


RINGER_SOURCE = "Ringer_NFL_Draft_Guide_2026"
RINGER_URL = "https://www.theringer.com/nfl-draft/2026/big-board"

SOURCES_DIR = ROOT / "data" / "sources"
PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUTS_DIR = ROOT / "data" / "outputs"

ANALYST_SEED_PATH = SOURCES_DIR / "analyst_rankings_seed.csv"
ANALYST_REPORTS_PATH = PROCESSED_DIR / "analyst_reports_2026.csv"
CONSENSUS_PATH = PROCESSED_DIR / "consensus_big_boards_2026.csv"
RINGER_PROCESSED_PATH = PROCESSED_DIR / "ringer_big_board_2026.csv"

MANUAL_FALLBACK_PATH = SOURCES_DIR / "manual" / "ringer_big_board_2026.csv"
REPORT_PATH = OUTPUTS_DIR / "ringer_big_board_pull_report.md"

POS_RE = r"(?:QB|RB|WR|TE|OT|IOL|EDGE|DT|DL|DE|LB|CB|S|DB|C|G|T)"
PLAYER_LINE_RE = re.compile(rf"(?P<name>[^,]+),\s*(?P<pos>{POS_RE}),\s*(?P<school>.+)$", re.I)
CARD_RE = re.compile(
    r'aria-label="Open player card for (?P<name>[^"]+) on the big board"'
    r'.{0,8000}?font-gt-america-expanded[^>]*>\s*(?P<rank>\d{1,3})\s*</span>'
    r'.{0,5000}?text-foreground[^>]*>\s*(?P<pos_full>[^,<]+),\s*(?P<school>[^<]+)\s*</div>',
    re.I | re.S,
)
TAG_RE = re.compile(r"(?s)<[^>]+>")

POS_FULL_TO_ABBR = {
    "quarterback": "QB",
    "running back": "RB",
    "wide receiver": "WR",
    "tight end": "TE",
    "offensive tackle": "OT",
    "offensive guard": "IOL",
    "offensive center": "IOL",
    "center": "IOL",
    "guard": "IOL",
    "offensive lineman": "IOL",
    "edge": "EDGE",
    "edge rusher": "EDGE",
    "defensive end": "EDGE",
    "defensive tackle": "DT",
    "interior defensive line": "DT",
    "linebacker": "LB",
    "cornerback": "CB",
    "safety": "S",
}


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


def _to_text_lines(page_html: str) -> list[str]:
    cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", page_html)
    cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = TAG_RE.sub("\n", cleaned)
    cleaned = html.unescape(cleaned)
    out = []
    for raw in cleaned.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            out.append(line)
    return out


def _normalize_player_line(line: str) -> str:
    text = re.sub(r"^Image\s+(for\s+)?", "", line, flags=re.I).strip()
    text = re.sub(r"^Image[^A-Za-z0-9]+", "", text, flags=re.I).strip()
    return text


def _normalize_pos(value: str) -> str:
    txt = _safe_str(value)
    if not txt:
        return ""
    low = txt.lower()
    if low in POS_FULL_TO_ABBR:
        return POS_FULL_TO_ABBR[low]
    raw = txt.upper().replace(".", "").strip()
    raw = {"ED": "EDGE", "DE": "EDGE", "DL": "DT", "DB": "CB", "C": "IOL", "G": "IOL", "T": "OT"}.get(raw, raw)
    return normalize_pos(raw)


def _dedupe_best(rows: list[dict]) -> list[dict]:
    best: dict[str, dict] = {}
    for row in rows:
        key = canonical_player_name(row.get("player_name", ""))
        if not key:
            continue
        cur = best.get(key)
        if cur is None or int(row["consensus_rank"]) < int(cur["consensus_rank"]):
            best[key] = row
    out = list(best.values())
    out.sort(key=lambda r: int(r["consensus_rank"]))
    return out


def parse_ringer_big_board(page_html: str, snapshot_date: str) -> list[dict]:
    # Primary parser: read player cards directly from DOM.
    rows: list[dict] = []
    seen = set()
    for m in CARD_RE.finditer(page_html):
        rank = _to_int(m.group("rank"))
        if rank is None or rank <= 0:
            continue
        name = _safe_str(m.group("name"))
        key = (rank, canonical_player_name(name))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "source": RINGER_SOURCE,
                "snapshot_date": snapshot_date,
                "consensus_rank": rank,
                "player_name": name,
                "school": _safe_str(m.group("school")),
                "position": _normalize_pos(m.group("pos_full")),
                "source_url": RINGER_URL,
            }
        )
    if rows:
        return _dedupe_best(rows)

    # Fallback parser: flattened text lines.
    lines = _to_text_lines(page_html)
    rows = []
    pending_rank: int | None = None
    seen = set()

    for raw in lines:
        line = _normalize_player_line(raw)
        rank = _to_int(line)
        if rank is not None and 1 <= rank <= 400:
            pending_rank = rank
            continue

        m = PLAYER_LINE_RE.search(line)
        if not m:
            continue
        if pending_rank is None:
            continue

        name = _safe_str(m.group("name"))
        pos = _normalize_pos(m.group("pos"))
        school = _safe_str(m.group("school"))
        key = (pending_rank, canonical_player_name(name))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "source": RINGER_SOURCE,
                "snapshot_date": snapshot_date,
                "consensus_rank": pending_rank,
                "player_name": name,
                "school": school,
                "position": pos,
                "source_url": RINGER_URL,
            }
        )
        pending_rank = None

    return _dedupe_best(rows)


def load_manual_fallback(path: Path, snapshot_date: str) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            rank = _to_int(row.get("consensus_rank", row.get("source_rank", row.get("rank", ""))))
            if rank is None or rank <= 0:
                continue
            name = _safe_str(row.get("player_name", row.get("player", "")))
            if not name:
                continue
            rows.append(
                {
                    "source": RINGER_SOURCE,
                    "snapshot_date": _safe_str(row.get("snapshot_date")) or snapshot_date,
                    "consensus_rank": rank,
                    "player_name": name,
                    "school": _safe_str(row.get("school")),
                    "position": normalize_pos(_safe_str(row.get("position", row.get("pos", ""))).upper()),
                    "source_url": _safe_str(row.get("source_url")) or RINGER_URL,
                }
            )
    return _dedupe_best(rows)


def _merge_analyst_seed(rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_SEED_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != RINGER_SOURCE]
    additions = [
        {
            "source": RINGER_SOURCE,
            "snapshot_date": r["snapshot_date"],
            "source_rank": r["consensus_rank"],
            "player_name": r["player_name"],
            "school": r["school"],
            "position": r["position"],
            "source_url": r["source_url"],
        }
        for r in rows
    ]
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
    keep = [r for r in existing if _safe_str(r.get("source")) != RINGER_SOURCE]
    additions = []
    for r in rows:
        additions.append(
            {
                "source": RINGER_SOURCE,
                "snapshot_date": r["snapshot_date"],
                "source_rank": r["consensus_rank"],
                "player_name": r["player_name"],
                "school": r["school"],
                "position": r["position"],
                "source_url": r["source_url"],
                "report_text": f"Ringer big board rank {r['consensus_rank']} for {r['player_name']} ({r['position']}, {r['school']}).",
            }
        )
    merged = keep + additions
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _sort_rank(r.get("source_rank"))))
    fields = _union_fieldnames(
        merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url", "report_text"],
    )
    _write_csv(ANALYST_REPORTS_PATH, merged, fields)
    return len(additions)


def _merge_consensus(rows: list[dict]) -> int:
    existing = _load_csv(CONSENSUS_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != RINGER_SOURCE]
    additions = list(rows)
    merged = keep + additions
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _sort_rank(r.get("consensus_rank"))))
    _write_csv(
        CONSENSUS_PATH,
        merged,
        ["source", "snapshot_date", "consensus_rank", "player_name", "school", "position", "source_url"],
    )
    return len(additions)


def _write_report(path: Path, rows: list[dict], warnings: list[str], added_seed: int, added_reports: int, added_consensus: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top = rows[:25]
    lines = [
        "# Ringer Big Board Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- rows_loaded: `{len(rows)}`",
        f"- analyst_seed_added: `{added_seed}`",
        f"- analyst_reports_added: `{added_reports}`",
        f"- consensus_added: `{added_consensus}`",
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
            "## Top 25",
            "",
            "| Rank | Player | Pos | School |",
            "|---:|---|---|---|",
        ]
    )
    for r in top:
        lines.append(
            f"| {r.get('consensus_rank','')} | {r.get('player_name','')} | {r.get('position','')} | {r.get('school','')} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    p = argparse.ArgumentParser(description="Pull 2026 Ringer NFL Draft guide big board.")
    p.add_argument("--skip-fetch", action="store_true", help="Skip web fetch and only use manual fallback CSV.")
    p.add_argument("--manual-csv", default=str(MANUAL_FALLBACK_PATH), help="Manual fallback CSV path.")
    args = p.parse_args()

    snapshot_date = dt.date.today().isoformat()
    warnings: list[str] = []
    rows: list[dict] = []

    if not args.skip_fetch:
        try:
            page_html = _fetch(RINGER_URL)
            rows = parse_ringer_big_board(page_html, snapshot_date=snapshot_date)
            if not rows:
                warnings.append("Ringer page fetched but parser returned 0 rows.")
        except Exception as exc:
            warnings.append(f"Ringer fetch failed: {exc}")

    if not rows:
        rows = load_manual_fallback(Path(args.manual_csv), snapshot_date=snapshot_date)
        if not rows:
            warnings.append("Manual Ringer fallback CSV missing/empty.")

    _write_csv(
        RINGER_PROCESSED_PATH,
        rows,
        ["source", "snapshot_date", "consensus_rank", "player_name", "school", "position", "source_url"],
    )
    added_seed = _merge_analyst_seed(rows) if rows else 0
    added_reports = _merge_analyst_reports(rows) if rows else 0
    added_consensus = _merge_consensus(rows) if rows else 0
    _write_report(REPORT_PATH, rows, warnings, added_seed, added_reports, added_consensus)

    print(f"Ringer rows: {len(rows)}")
    print(f"Analyst seed rows added: {added_seed}")
    print(f"Analyst report rows added: {added_reports}")
    print(f"Consensus rows added: {added_consensus}")
    print(f"Processed rows: {RINGER_PROCESSED_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
