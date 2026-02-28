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


DEFAULT_URL = "https://www.espn.com/nfl/story/_/id/47600173"
DEFAULT_OUTPUT = ROOT / "data" / "sources" / "manual" / "espn_underclassmen_2026.csv"
DEFAULT_REPORT = ROOT / "data" / "outputs" / "espn_underclassmen_pull_report_2026.txt"
DEFAULT_DECLARED = ROOT / "data" / "sources" / "manual" / "declared_underclassmen_2026_official.csv"

POS_SET = {
    "QB",
    "RB",
    "FB",
    "WR",
    "TE",
    "OT",
    "G",
    "C",
    "OL",
    "DL",
    "DT",
    "DE",
    "EDGE",
    "LB",
    "CB",
    "S",
    "DB",
    "K",
    "P",
    "LS",
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
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = html.unescape(txt)
    txt = re.sub(r"[ \\t\\r\\f\\v]+", " ", txt)
    txt = re.sub(r"\\n+", "\n", txt)
    return txt


def _normalize_for_match(txt: str) -> str:
    clean = re.sub(r"[^a-z0-9]+", " ", str(txt or "").lower())
    return re.sub(r"\s+", " ", clean).strip()


def _normalize_flat(txt: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(txt or "").lower())


def _load_declared_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _extract_rows(cleaned_text: str, declared_rows: list[dict]) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()
    corpus = _normalize_for_match(cleaned_text)
    corpus_flat = _normalize_flat(cleaned_text)
    if not corpus:
        return rows

    for row in declared_rows:
        name = re.sub(r"\s+", " ", str(row.get("player_name", "")).strip())
        pos = re.sub(r"\s+", " ", str(row.get("position", "")).strip()).upper()
        school = (
            re.sub(r"\s+", " ", str(row.get("school", "")).strip())
            or re.sub(r"\s+", " ", str(row.get("college", "")).strip())
        )
        if not name or pos not in POS_SET:
            continue
        key = (name.lower(), pos)
        if key in seen:
            continue
        needle = _normalize_for_match(name)
        if not needle:
            continue
        needle_flat = _normalize_flat(name)
        if f" {needle} " not in f" {corpus} " and needle_flat not in corpus_flat:
            continue
        seen.add(key)
        rows.append(
            {
                "player_name": name,
                "position": pos,
                "school": school,
            }
        )

    rows.sort(key=lambda r: (r["player_name"], r["position"]))
    return rows


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pull ESPN underclassmen list for 2026 NFL draft.")
    p.add_argument("--url", type=str, default=DEFAULT_URL, help="ESPN source URL.")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path.")
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="Output report path.")
    p.add_argument("--declared", type=Path, default=DEFAULT_DECLARED, help="Declared underclassmen reference CSV.")
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
                },
                indent=2,
            )
        )
        print("Dry run complete. Re-run with --execute to fetch and parse.")
        return

    declared_rows = _load_declared_rows(args.declared)
    page_html = _fetch(args.url)
    cleaned_text = _clean_text(page_html)
    rows = _extract_rows(cleaned_text, declared_rows)

    out_rows = []
    for row in rows:
        out_rows.append(
            {
                "player_name": row["player_name"],
                "position": row["position"],
                "school": row["school"],
                "source_url": args.url,
                "source": "espn_underclassmen_2026",
                "pull_method": "official_name_match_in_espn_story",
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
        "ESPN Underclassmen Pull Report (2026)",
        "",
        f"url: {args.url}",
        f"declared_reference_rows: {len(declared_rows)}",
        f"rows_parsed: {len(rows)}",
        f"output: {args.output}",
        f"method: official_name_match_in_espn_story",
        "note: verify rows manually after each cycle update.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Report: {args.report}")
    print(f"Rows parsed: {len(rows)}")


if __name__ == "__main__":
    main()
