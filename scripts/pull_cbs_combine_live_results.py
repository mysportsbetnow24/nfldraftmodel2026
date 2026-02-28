#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.cbs_combine_loader import (  # noqa: E402
    CBS_COMBINE_LIVE_URL,
    DEFAULT_COMBINE_OUT,
    extract_measurements_from_cbs_html,
    fetch_cbs_live_html,
    merge_measurements_into_combine_csv,
)


def _write_report(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description="Pull 2026 combine live results from CBS live blog and merge into combine CSV")
    p.add_argument("--execute", action="store_true", help="Fetch live page over network")
    p.add_argument("--html", type=str, default="", help="Parse from local HTML file instead of live fetch")
    p.add_argument("--url", type=str, default=CBS_COMBINE_LIVE_URL, help="CBS live results URL")
    p.add_argument("--out", type=str, default=str(DEFAULT_COMBINE_OUT), help="Combine CSV output path")
    p.add_argument(
        "--report",
        type=str,
        default=str(ROOT / "data" / "outputs" / f"cbs_combine_pull_report_{datetime.now(UTC).strftime('%Y-%m-%d')}.txt"),
        help="Text report output path",
    )
    args = p.parse_args()

    html_text = ""
    source_desc = ""
    if args.html:
        html_path = Path(args.html)
        if not html_path.exists():
            raise SystemExit(f"Local HTML not found: {html_path}")
        html_text = html_path.read_text(encoding="utf-8", errors="ignore")
        source_desc = f"local_html:{html_path}"
    elif args.execute:
        html_text = fetch_cbs_live_html(url=args.url)
        source_desc = f"live_url:{args.url}"
    else:
        print("Dry run only. Use either --html <path> or --execute.")
        print(f"Default URL: {args.url}")
        return

    measurements = extract_measurements_from_cbs_html(html_text)
    stats = merge_measurements_into_combine_csv(measurements, out_path=Path(args.out))

    by_metric = stats.get("metric_updates", {})
    report_lines = [
        f"CBS combine pull timestamp: {datetime.now(UTC).isoformat()}",
        f"Input source: {source_desc}",
        f"Measurements parsed: {len(measurements)}",
        f"Players total in combine CSV: {stats.get('players_total', 0)}",
        f"Players created: {stats.get('players_created', 0)}",
        f"Fields updated: {stats.get('fields_updated', 0)}",
        "Metric updates:",
        f"  forty: {by_metric.get('forty', 0)}",
        f"  ten_split: {by_metric.get('ten_split', 0)}",
        f"  vertical: {by_metric.get('vertical', 0)}",
        f"  broad: {by_metric.get('broad', 0)}",
        f"  shuttle: {by_metric.get('shuttle', 0)}",
        f"  three_cone: {by_metric.get('three_cone', 0)}",
        f"  bench: {by_metric.get('bench', 0)}",
    ]
    _write_report(Path(args.report), report_lines)

    print(f"Merged combine CSV: {args.out}")
    print(f"Report: {args.report}")
    print(f"Measurements parsed: {len(measurements)}")
    print(f"Players created: {stats.get('players_created', 0)}")
    print(f"Fields updated: {stats.get('fields_updated', 0)}")


if __name__ == "__main__":
    main()
