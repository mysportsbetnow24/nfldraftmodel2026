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

from src.ingest.analyst_language_loader import aggregate_linguistic_signals  # noqa: E402
from src.ingest.rankings_loader import normalize_pos  # noqa: E402


SOURCE = "Yahoo_Nate_Tice_Mock_2026"
URL = "https://sports.yahoo.com/nfl/article/2026-nfl-mock-draft-025225134.html"
SNAPSHOT_DATE = dt.date.today().isoformat()

ANALYST_SEED_PATH = ROOT / "data" / "sources" / "analyst_rankings_seed.csv"
REPORTS_PATH = ROOT / "data" / "processed" / "analyst_reports_2026.csv"
LING_PATH = ROOT / "data" / "processed" / "analyst_linguistic_signals_2026.csv"
PROCESSED_PATH = ROOT / "data" / "processed" / "nate_tice_mock_2026.csv"
REPORT_MD = ROOT / "data" / "outputs" / "nate_tice_mock_pull_report.md"

HEADING_RE_NO = re.compile(
    r"^No\.\s*(?P<pick>\d{1,2}):\s*(?P<team>.+?)\s+selects\s+"
    r"(?P<player>.+?),\s*(?P<pos>[A-Za-z/]+),\s*(?P<school>.+)$",
    re.I,
)
HEADING_RE_DASH = re.compile(
    r"^(?P<pick>\d{1,2})\.\s*(?P<team>.+?)\s+[—-]\s+"
    r"(?P<player>.+?),\s*(?P<pos>[A-Za-z/]+),\s*(?P<school>.+)$",
    re.I,
)


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
    cleaned = re.sub(r"(?s)<[^>]+>", "\n", cleaned)
    cleaned = html.unescape(cleaned)
    out: list[str] = []
    for raw in cleaned.splitlines():
        line = re.sub(r"\\s+", " ", raw).strip()
        if line:
            out.append(line)
    return out


def _parse_rows(lines: list[str]) -> list[dict]:
    # Repair broken heading lines around trade notes (e.g. "29. Team (" + "..."+") — Player")
    stitched: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.match(r"^\d+\.\d+$", line):
            stitched.append(line)
            i += 1
            continue
        if re.match(r"^\d{1,2}\.", line) and ("—" not in line and "-" not in line):
            merged = line
            j = i + 1
            while j < len(lines) and j <= i + 4:
                merged = f"{merged} {lines[j]}".strip()
                if "—" in lines[j] or " - " in lines[j]:
                    break
                j += 1
            stitched.append(re.sub(r"\s+", " ", merged).strip())
            i = j + 1
            continue
        stitched.append(line)
        i += 1

    rows: list[dict] = []
    i = 0
    stop_tokens = (
        "Who’s your favorite",
        "Would you have made this pick",
        "follow along",
        "Recommended Stories",
    )

    while i < len(stitched):
        line = stitched[i]
        head = HEADING_RE_NO.match(line) or HEADING_RE_DASH.match(line)
        if not head:
            i += 1
            continue

        pick = int(head.group("pick"))
        if not (1 <= pick <= 64):
            i += 1
            continue

        team = head.group("team").strip()
        player = head.group("player").strip()
        pos = normalize_pos(head.group("pos").strip())
        school = head.group("school").strip()

        note_parts: list[str] = []
        j = i + 1
        while j < len(stitched):
            nxt = stitched[j]
            if HEADING_RE_NO.match(nxt) or HEADING_RE_DASH.match(nxt):
                break
            if any(tok.lower() in nxt.lower() for tok in stop_tokens):
                break
            if len(nxt) > 18 and not nxt.startswith("View"):
                note_parts.append(nxt)
            j += 1

        # Keep notes concise and useful for language features.
        note = " ".join(note_parts[:4]).strip()
        if not note:
            note = f"Nate Tice mock placed {player} at pick {pick} for {team}."

        rows.append(
            {
                "source": SOURCE,
                "snapshot_date": SNAPSHOT_DATE,
                "source_rank": pick,
                "pick_number": pick,
                "nfl_team": team,
                "player_name": player,
                "school": school,
                "position": pos,
                "source_url": URL,
                "report_text": note,
            }
        )
        i = j

    rows.sort(key=lambda r: int(r["source_rank"]))
    return rows


def _read_csv(path: Path) -> list[dict]:
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


def _safe_rank(value, default: int = 9999) -> int:
    try:
        txt = str(value).strip()
        if not txt:
            return default
        return int(float(txt))
    except (TypeError, ValueError):
        return default


def _merge_replace_source(existing: list[dict], new_rows: list[dict]) -> list[dict]:
    keep = [r for r in existing if str(r.get("source", "")).strip() != SOURCE]
    keep.extend(new_rows)
    keep.sort(key=lambda r: (str(r.get("source", "")).strip(), _safe_rank(r.get("source_rank"))))
    return keep


def _write_report(rows: list[dict]) -> None:
    lines = [
        "# Nate Tice Mock Pull Report",
        "",
        f"- generated_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- source: `{SOURCE}`",
        f"- source_url: `{URL}`",
        f"- rows_parsed: `{len(rows)}`",
        "",
        "## Top 12 Picks Parsed",
        "",
        "| Pick | Team | Player | Pos | School |",
        "|---:|---|---|---|---|",
    ]
    for row in rows[:12]:
        lines.append(
            f"| {row['source_rank']} | {row['nfl_team']} | {row['player_name']} | {row['position']} | {row['school']} |"
        )
    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.write_text("\n".join(lines))


def main() -> None:
    page_html = _fetch(URL)
    lines = _to_text_lines(page_html)
    rows = _parse_rows(lines)
    if not rows:
        raise SystemExit("No mock rows parsed from Yahoo page.")

    _write_csv(
        PROCESSED_PATH,
        rows,
        [
            "source",
            "snapshot_date",
            "source_rank",
            "pick_number",
            "nfl_team",
            "player_name",
            "school",
            "position",
            "source_url",
            "report_text",
        ],
    )

    seed_rows = [
        {
            "source": r["source"],
            "snapshot_date": r["snapshot_date"],
            "source_rank": r["source_rank"],
            "player_name": r["player_name"],
            "school": r["school"],
            "position": r["position"],
            "source_url": r["source_url"],
        }
        for r in rows
    ]

    seed_existing = _read_csv(ANALYST_SEED_PATH)
    seed_merged = _merge_replace_source(seed_existing, seed_rows)
    _write_csv(
        ANALYST_SEED_PATH,
        seed_merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url"],
    )

    reports_existing = _read_csv(REPORTS_PATH)
    reports_merged = _merge_replace_source(reports_existing, rows)
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

    _write_report(rows)
    print(f"Parsed rows: {len(rows)}")
    print(f"Processed rows written: {PROCESSED_PATH}")
    print(f"Analyst seed updated: {ANALYST_SEED_PATH}")
    print(f"Analyst reports updated: {REPORTS_PATH}")
    print(f"Linguistic signals regenerated: {LING_PATH}")
    print(f"Report: {REPORT_MD}")


if __name__ == "__main__":
    main()
