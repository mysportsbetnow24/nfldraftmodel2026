#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name


DEFAULT_URL = (
    "https://www.nfl.com/news/nfl-combine-full-list-of-draft-prospects-invited-to-2026-scouting-event"
)
DEFAULT_OUTPUT = ROOT / "data" / "sources" / "manual" / "nfl_combine_invites_2026.csv"
DEFAULT_REPORT = ROOT / "data" / "outputs" / "nfl_combine_invites_pull_report_2026.txt"
DEFAULT_DECLARED = ROOT / "data" / "sources" / "manual" / "declared_underclassmen_2026_official.csv"

GROUP_TO_POS = {
    "QUARTERBACKS": "QB",
    "RUNNING BACKS": "RB",
    "WIDE RECEIVERS": "WR",
    "TIGHT ENDS": "TE",
    "OFFENSIVE LINEMEN": "OL",
    "DEFENSIVE LINEMEN": "DL",
    "LINEBACKERS": "LB",
    "DEFENSIVE BACKS": "DB",
    "SPECIALISTS": "ST",
}


def _fetch(url: str, timeout: int = 25) -> str:
    try:
        import requests  # type: ignore

        resp = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (NFLDraftModel/2026)"},
        )
        resp.raise_for_status()
        return resp.text
    except ModuleNotFoundError:
        from urllib.request import Request, urlopen

        req = Request(url, headers={"User-Agent": "Mozilla/5.0 (NFLDraftModel/2026)"})
        with urlopen(req, timeout=timeout) as resp:  # nosec B310
            return resp.read().decode("utf-8", errors="ignore")


def _clean_text(raw_html: str) -> str:
    txt = re.sub(r"<(script|style)[^>]*>.*?</\\1>", " ", raw_html, flags=re.I | re.S)
    txt = re.sub(r"<br\\s*/?>", "\n", txt, flags=re.I)
    txt = re.sub(r"</p\\s*>", "\n", txt, flags=re.I)
    txt = re.sub(r"</li\\s*>", "\n", txt, flags=re.I)
    txt = re.sub(r"<h[1-6][^>]*>", "\n", txt, flags=re.I)
    txt = re.sub(r"</h[1-6]\\s*>", "\n", txt, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = html.unescape(txt)
    txt = re.sub(r"[ \\t\\r\\f\\v]+", " ", txt)
    txt = re.sub(r"\\n+", "\n", txt)
    return txt


def _extract_article_body_from_ldjson(raw_html: str) -> str:
    scripts = re.findall(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        raw_html,
        flags=re.I | re.S,
    )
    for block in scripts:
        payload_txt = html.unescape(block).strip()
        if not payload_txt:
            continue
        try:
            payload = json.loads(payload_txt)
        except Exception:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            body = obj.get("articleBody")
            if not isinstance(body, str):
                continue
            if "QUARTERBACKS" in body and "RUNNING BACKS" in body and "DEFENSIVE BACKS" in body:
                return body
    # Fallback: pull articleBody string directly from embedded JSON with a simple string parser.
    marker = '"articleBody":"'
    start = raw_html.find(marker)
    if start != -1:
        i = start + len(marker)
        buf = []
        escaped = False
        while i < len(raw_html):
            ch = raw_html[i]
            if escaped:
                buf.append(ch)
                escaped = False
                i += 1
                continue
            if ch == "\\":
                buf.append(ch)
                escaped = True
                i += 1
                continue
            if ch == '"':
                break
            buf.append(ch)
            i += 1
        raw_body = "".join(buf)
        try:
            decoded = json.loads(f"\"{raw_body}\"")
            if isinstance(decoded, str):
                return decoded
        except Exception:
            pass
    return ""


def _load_declared_set(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out = set()
    with path.open() as f:
        for row in csv.DictReader(f):
            nm = canonical_player_name(row.get("player_name", ""))
            if nm:
                out.add(nm)
    return out


def _extract_rows_from_article_body(article_body: str) -> list[dict]:
    rows = []
    seen: set[str] = set()
    current_group = ""

    for raw_line in article_body.splitlines():
        line = re.sub(r"\\s+", " ", raw_line).strip()
        if not line:
            continue
        upper = line.upper().strip(": ")
        if upper in GROUP_TO_POS:
            current_group = upper
            continue
        body = line.lstrip("*").strip()
        if "," not in body:
            continue
        name, school = body.split(",", 1)
        player_name = re.sub(r"\\s+", " ", name).strip(" .,-")
        school_name = re.sub(r"\\s+", " ", school).strip(" .,-")
        if not player_name or not school_name:
            continue

        key = canonical_player_name(player_name)
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "player_name": player_name,
                "position": GROUP_TO_POS.get(current_group, ""),
                "school": school_name,
            }
        )

    rows.sort(key=lambda r: r["player_name"])
    return rows


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pull NFL combine invite list for 2026.")
    p.add_argument("--url", type=str, default=DEFAULT_URL, help="NFL source URL.")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path.")
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="Output report path.")
    p.add_argument("--declared", type=Path, default=DEFAULT_DECLARED, help="Declared underclassmen CSV.")
    p.add_argument("--execute", action="store_true", help="Perform network request. Dry-run otherwise.")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    if not args.execute:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "url": args.url,
                    "output": str(args.output),
                    "report": str(args.report),
                    "declared": str(args.declared),
                },
                indent=2,
            )
        )
        print("Dry run complete. Re-run with --execute to fetch and parse.")
        return

    declared_names = _load_declared_set(args.declared)
    page_html = _fetch(args.url)
    article_body = _extract_article_body_from_ldjson(page_html)
    rows = _extract_rows_from_article_body(article_body)
    if not rows:
        # Fallback to raw cleaned-text parse if ld+json layout changes.
        cleaned_text = _clean_text(page_html)
        rows = _extract_rows_from_article_body(cleaned_text)

    out_rows = []
    for row in rows:
        nm_key = canonical_player_name(row["player_name"])
        out_rows.append(
            {
                "player_name": row["player_name"],
                "position": row["position"],
                "school": row["school"],
                "early_declare": 1 if nm_key in declared_names else 0,
                "source_url": args.url,
                "source": "nfl_combine_invite_2026",
                "pull_method": "regex_text_extract",
                "status": "ok",
                "notes": "",
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "player_name",
                "position",
                "school",
                "early_declare",
                "source_url",
                "source",
                "pull_method",
                "status",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    lines = [
        "NFL Combine Invites Pull Report (2026)",
        "",
        f"url: {args.url}",
        f"rows_parsed: {len(rows)}",
        f"rows_marked_early_declare: {sum(int(r['early_declare']) for r in out_rows)}",
        f"declared_source: {args.declared}",
        f"output: {args.output}",
        "method: ldjson_article_body_parser",
        "note: verify rows manually after each cycle update.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Report: {args.report}")
    print(f"Rows parsed: {len(rows)}")


if __name__ == "__main__":
    main()
