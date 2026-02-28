#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_INPUT = ROOT / "data" / "sources" / "manual" / "espn_player_urls_2026.csv"
DEFAULT_OUTPUT = ROOT / "data" / "sources" / "manual" / "espn_birthdates_2026.csv"
DEFAULT_REPORT = ROOT / "data" / "outputs" / "espn_birthdates_pull_report_2026.txt"
DEFAULT_DRAFT_DATE = "2026-04-23"


def _parse_date(raw: str) -> dt.date | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    fmts = ("%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%m/%d/%Y")
    for fmt in fmts:
        try:
            return dt.datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    return None


def _compute_age(birth_date: dt.date, draft_date: dt.date) -> float:
    return round((draft_date - birth_date).days / 365.2425, 3)


def _fetch(url: str, timeout: int = 20) -> str:
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


def _extract_birth_date(page_html: str) -> tuple[str, str]:
    patterns = [
        (r'"dateOfBirth"\s*:\s*"(\d{4}-\d{2}-\d{2})"', "json_dateOfBirth"),
        (r'"birthDate"\s*:\s*"(\d{4}-\d{2}-\d{2})"', "json_birthDate"),
        (r"Birthdate\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", "text_birthdate"),
        (r"Born\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", "text_born"),
    ]
    for pat, method in patterns:
        m = re.search(pat, page_html, flags=re.I | re.S)
        if m:
            return m.group(1).strip(), method
    return "", ""


def _load_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pull ESPN player birthdates and compute draft_age.")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="CSV of player URLs.")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output birthdate CSV.")
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="Run report path.")
    p.add_argument("--draft-date", type=str, default=DEFAULT_DRAFT_DATE, help="Draft date (YYYY-MM-DD).")
    p.add_argument("--sleep-ms", type=int, default=300, help="Delay between requests.")
    p.add_argument("--execute", action="store_true", help="Perform network requests. Without this, dry-run only.")
    return p


def main() -> None:
    args = build_parser().parse_args()

    rows = _load_rows(args.input)
    if not rows:
        raise SystemExit(f"No input rows found at {args.input}")

    draft_date = _parse_date(args.draft_date)
    if draft_date is None:
        raise SystemExit(f"Invalid --draft-date: {args.draft_date}")

    if not args.execute:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "input": str(args.input),
                    "output": str(args.output),
                    "rows": len(rows),
                    "draft_date": draft_date.isoformat(),
                },
                indent=2,
            )
        )
        print("Dry run complete. Re-run with --execute to fetch ESPN pages.")
        return

    out_rows: list[dict] = []
    ok = 0
    failed = 0
    for row in rows:
        player_name = str(row.get("player_name", "")).strip()
        position = str(row.get("position", "")).strip()
        school = str(row.get("school", "")).strip()
        source_url = str(row.get("espn_player_url", "")).strip()
        source_id = str(row.get("espn_player_id", "")).strip()
        birth_raw = str(row.get("birth_date", "")).strip()
        method = "input_birth_date"

        if source_url and not birth_raw:
            try:
                html = _fetch(source_url)
                birth_raw, method = _extract_birth_date(html)
            except Exception:
                birth_raw = ""
                method = "fetch_failed"
            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

        birth_dt = _parse_date(birth_raw)
        if birth_dt is None:
            failed += 1
            out_rows.append(
                {
                    "player_name": player_name,
                    "school": school,
                    "position": position,
                    "birth_date": "",
                    "draft_age": "",
                    "espn_player_id": source_id,
                    "source_url": source_url,
                    "source": "espn_player_page",
                    "pull_method": method or "missing",
                    "status": "missing_birth_date",
                }
            )
            continue

        ok += 1
        out_rows.append(
            {
                "player_name": player_name,
                "school": school,
                "position": position,
                "birth_date": birth_dt.isoformat(),
                "draft_age": _compute_age(birth_dt, draft_date),
                "espn_player_id": source_id,
                "source_url": source_url,
                "source": "espn_player_page",
                "pull_method": method,
                "status": "ok",
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "player_name",
                "school",
                "position",
                "birth_date",
                "draft_age",
                "espn_player_id",
                "source_url",
                "source",
                "pull_method",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    report_lines = [
        "ESPN Birthdate Pull Report",
        "",
        f"input: {args.input}",
        f"output: {args.output}",
        f"draft_date: {draft_date.isoformat()}",
        f"rows_input: {len(rows)}",
        f"rows_ok: {ok}",
        f"rows_missing: {failed}",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(report_lines))

    print(f"Wrote: {args.output}")
    print(f"Report: {args.report}")
    print(f"Rows ok: {ok} | missing: {failed}")


if __name__ == "__main__":
    main()

