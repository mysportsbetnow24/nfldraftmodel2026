#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import html
import json
import re
import ssl
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_CSV = ROOT / "data" / "processed" / "external_mock_signals_2026.csv"
OUT_REPORT = ROOT / "data" / "outputs" / "external_mock_signals_pull_report_2026.md"

BR_URL = "https://bleacherreport.com/articles/25401705-2026-nfl-mock-draft-post-combine-predictions-br-nfl-draft-scouting-dept"
ATHLETIC_URL = "https://www.nytimes.com/athletic/7081108/2026/03/04/nfl-mock-draft-2026-combine-mendoza/"

POS_MAP = {
    "G": "IOL",
    "OG": "IOL",
    "C": "IOL",
    "T": "OT",
    "DE": "EDGE",
    "DL": "DT",
}


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
    with urllib.request.urlopen(req, timeout=60, context=ssl_ctx) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _norm_pos(pos: str) -> str:
    k = str(pos or "").strip().upper()
    return POS_MAP.get(k, k)


def _parse_br(html_text: str) -> list[dict]:
    stripped = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
    stripped = re.sub(r"(?is)<style.*?>.*?</style>", " ", stripped)
    stripped = re.sub(r"(?s)<[^>]+>", "\n", stripped)
    stripped = html.unescape(stripped)
    lines = [re.sub(r"\s+", " ", x).strip() for x in stripped.splitlines()]
    lines = [x for x in lines if x]

    pat_player_first = re.compile(r"^(\d{1,2})\.\s+([^:]+):\s+(.+?),\s*([A-Za-z/]+)\s*,\s*(.+)$")
    pat_pos_first = re.compile(r"^(\d{1,2})\.\s+([^:]+):\s+([A-Za-z/]+)\s+(.+?),\s*(.+)$")
    out: list[dict] = []
    for line in lines:
        pick = 0
        team = ""
        player_name = ""
        position = ""
        school = ""

        m1 = pat_player_first.match(line)
        if m1:
            pick = int(m1.group(1))
            team = m1.group(2).strip()
            player_name = m1.group(3).strip()
            position = _norm_pos(m1.group(4))
            school = m1.group(5).strip()
        else:
            m2 = pat_pos_first.match(line)
            if m2:
                pick = int(m2.group(1))
                team = m2.group(2).strip()
                position = _norm_pos(m2.group(3))
                player_name = m2.group(4).strip()
                school = m2.group(5).strip()
        if not pick:
            continue
        if pick < 1 or pick > 32:
            continue
        out.append(
            {
                "source": "bleacher_report",
                "source_url": BR_URL,
                "pick": pick,
                "team": team,
                "player_name": player_name,
                "position": position,
                "school": school,
            }
        )
    return out


def _parse_athletic(html_text: str) -> list[dict]:
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html_text, re.S)
    if not m:
        return []
    payload = json.loads(m.group(1))
    article = payload.get("props", {}).get("pageProps", {}).get("article", {})
    article_body = str(article.get("article_body") or article.get("article_body_desktop") or "")

    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", article_body)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", "\n", text)
    text = html.unescape(text)
    lines = [re.sub(r"\s+", " ", x).strip() for x in text.splitlines()]
    lines = [x for x in lines if x]

    pat = re.compile(r"^(\d{1,2})\.\s+([^:]+):\s+(.+?),\s*([A-Za-z/]+)\s*,\s*(.+)$")
    out: list[dict] = []
    for line in lines:
        mm = pat.match(line)
        if not mm:
            continue
        pick = int(mm.group(1))
        if pick < 1 or pick > 32:
            continue
        out.append(
            {
                "source": "the_athletic",
                "source_url": ATHLETIC_URL,
                "pick": pick,
                "team": mm.group(2).strip(),
                "player_name": mm.group(3).strip(),
                "position": _norm_pos(mm.group(4)),
                "school": mm.group(5).strip(),
            }
        )
    return out


def _write(rows: list[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "snapshot_date",
        "source",
        "source_url",
        "pick",
        "source_rank_pct",
        "team",
        "player_name",
        "position",
        "school",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            pick = int(row["pick"])
            rank_pct = round(1.0 - ((pick - 1) / 31.0), 6)
            writer.writerow(
                {
                    "snapshot_date": dt.date.today().isoformat(),
                    "source": row["source"],
                    "source_url": row["source_url"],
                    "pick": pick,
                    "source_rank_pct": rank_pct,
                    "team": row["team"],
                    "player_name": row["player_name"],
                    "position": row["position"],
                    "school": row["school"],
                }
            )


def _write_report(rows: list[dict]) -> None:
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    by_source: dict[str, list[dict]] = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(r)

    br_players = {r["player_name"].lower() for r in by_source.get("bleacher_report", [])}
    ath_players = {r["player_name"].lower() for r in by_source.get("the_athletic", [])}
    overlap = sorted(br_players & ath_players)

    lines = [
        "# External Mock Signals Pull Report",
        "",
        f"- generated_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- rows: `{len(rows)}`",
        f"- sources: `{', '.join(sorted(by_source.keys()))}`",
        "",
        "## Rows By Source",
        "",
        "| Source | Rows |",
        "|---|---:|",
    ]
    for source, source_rows in sorted(by_source.items()):
        lines.append(f"| {source} | {len(source_rows)} |")
    lines += [
        "",
        "## Cross-Source Player Overlap (Top 32)",
        "",
        f"- overlap_count: `{len(overlap)}`",
        f"- overlap_players: `{', '.join(overlap[:30])}`",
        "",
    ]
    OUT_REPORT.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    rows: list[dict] = []
    rows.extend(_parse_br(_fetch(BR_URL)))
    rows.extend(_parse_athletic(_fetch(ATHLETIC_URL)))

    # De-dup exact source+pick collisions.
    dedup: dict[tuple[str, int], dict] = {}
    for r in rows:
        dedup[(r["source"], int(r["pick"]))] = r
    out_rows = [dedup[k] for k in sorted(dedup.keys(), key=lambda x: (x[0], x[1]))]

    _write(out_rows)
    _write_report(out_rows)
    print(f"Rows: {len(out_rows)}")
    print(f"Wrote: {OUT_CSV}")
    print(f"Report: {OUT_REPORT}")


if __name__ == "__main__":
    main()
