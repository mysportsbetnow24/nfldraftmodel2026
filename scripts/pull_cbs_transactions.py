#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import html
import re
import ssl
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_PATH = ROOT / "data" / "processed" / "cbs_nfl_transactions_2026.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "cbs_transactions_pull_report_2026.md"
SOURCE_URL = "https://www.cbssports.com/nfl/transactions/"

MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)

LINE_RE = re.compile(
    r"^(?P<event_date>(?:"
    + "|".join(MONTHS)
    + r")\s+\d{2},\s+\d{4})\s+"
    r"(?P<player_name>.+?)\s+"
    r"(?P<position>[A-Z/]+)\s+"
    r"(?P<action_text>.+?)\s+"
    r"(?P<direction>By|To)\s+"
    r"(?P<team_name>.+?)\s+\((?P<team>[A-Z]{2,3})\)$"
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


def _to_lines(page_html: str) -> list[str]:
    cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", page_html)
    cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = re.sub(r"(?s)<[^>]+>", "\n", cleaned)
    cleaned = html.unescape(cleaned)
    lines = [re.sub(r"\s+", " ", x).strip() for x in cleaned.splitlines()]
    return [x for x in lines if x]


def _impact_weight(action_text: str, direction: str) -> float:
    txt = action_text.lower()
    # Positive sign means team lost talent (need up), negative means team added talent.
    if "released" in txt or "waived" in txt or "retired" in txt or "suspended" in txt:
        return 1.0 if direction == "By" else -1.0
    if "traded" in txt:
        return 1.0 if direction == "By" else -1.0
    if "signed" in txt or "claimed" in txt or "activated" in txt:
        return 0.8 if direction == "By" else -0.8
    if "placed on injured reserve" in txt:
        return 0.6 if direction == "By" else -0.6
    if "promoted" in txt:
        return -0.35
    if "demoted" in txt:
        return 0.35
    return 0.0


def _transaction_type(action_text: str) -> str:
    txt = action_text.lower()
    if "re-signed" in txt or "resigned" in txt or "extension" in txt:
        return "re-signed"
    if "signed" in txt:
        return "signed"
    if "released" in txt:
        return "released"
    if "waived" in txt:
        return "waived"
    if "traded" in txt:
        return "traded"
    if "retired" in txt:
        return "retired"
    if "claimed" in txt:
        return "claimed"
    if "activated" in txt:
        return "activated"
    if "injured reserve" in txt:
        return "injured_reserve"
    if "promoted" in txt:
        return "promoted"
    if "demoted" in txt:
        return "demoted"
    return "other"


def parse_transactions(page_html: str) -> list[dict]:
    rows: list[dict] = []
    for line in _to_lines(page_html):
        m = LINE_RE.match(line)
        if not m:
            continue
        action_text = m.group("action_text").strip()
        direction = m.group("direction").strip()
        impact = _impact_weight(action_text, direction)
        rows.append(
            {
                "snapshot_date": dt.date.today().isoformat(),
                "event_date": m.group("event_date").strip(),
                "player_name": m.group("player_name").strip(),
                "position": m.group("position").strip(),
                "action_text": action_text,
                "transaction_type": _transaction_type(action_text),
                "transaction_status": "confirmed",
                "direction": direction,
                "team": m.group("team").strip().upper(),
                "team_name": m.group("team_name").strip(),
                "impact_weight": round(float(impact), 3),
                "source_url": SOURCE_URL,
                "raw_line": line,
            }
        )
    # Deduplicate exact duplicates from page text render artifacts.
    seen: set[tuple[str, str, str, str, str]] = set()
    out: list[dict] = []
    for r in rows:
        key = (
            r["event_date"],
            r["player_name"].lower(),
            r["position"],
            r["action_text"].lower(),
            r["team"],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "snapshot_date",
        "event_date",
        "player_name",
        "position",
        "action_text",
        "transaction_type",
        "transaction_status",
        "direction",
        "team",
        "team_name",
        "impact_weight",
        "source_url",
        "raw_line",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_report(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_day: dict[str, int] = {}
    for row in rows:
        by_day[row["event_date"]] = by_day.get(row["event_date"], 0) + 1
    lines = [
        "# CBS NFL Transactions Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- source_url: `{SOURCE_URL}`",
        f"- parsed_rows: `{len(rows)}`",
        "",
        "## Rows By Event Date",
        "",
        "| Date | Rows |",
        "|---|---:|",
    ]
    for day, count in sorted(by_day.items(), reverse=True):
        lines.append(f"| {day} | {count} |")
    path.write_text("\n".join(lines))


def main() -> None:
    page_html = _fetch(SOURCE_URL)
    rows = parse_transactions(page_html)
    if not rows:
        raise SystemExit("No transaction rows parsed from CBS page.")
    _write_csv(PROCESSED_PATH, rows)
    _write_report(REPORT_PATH, rows)
    print(f"Parsed rows: {len(rows)}")
    print(f"Wrote: {PROCESSED_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
