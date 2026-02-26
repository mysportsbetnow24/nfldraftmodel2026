#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.drafttek_order_loader import (  # noqa: E402
    DEFAULT_FULL_ORDER_PATH,
    DEFAULT_RAW_HTML_PATH,
    DEFAULT_ROUND1_PATH,
    DEFAULT_TRADE_ROWS_PATH,
    DEFAULT_URL,
    fetch_drafttek_trade_value_html,
    parse_drafttek_order,
    write_csv,
)


def _read_html_with_fallback(path: Path) -> str:
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"Could not decode HTML file: {path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Pull and parse Drafttek 2026 draft order + traded picks.")
    p.add_argument("--execute", action="store_true", help="Execute live web pull (default dry-run).")
    p.add_argument("--url", type=str, default=DEFAULT_URL)
    p.add_argument(
        "--html-path",
        type=str,
        default="",
        help="Optional local HTML file to parse instead of live pull.",
    )
    p.add_argument("--out-raw-html", type=str, default=str(ROOT / DEFAULT_RAW_HTML_PATH))
    p.add_argument("--out-full-order", type=str, default=str(ROOT / DEFAULT_FULL_ORDER_PATH))
    p.add_argument("--out-trades", type=str, default=str(ROOT / DEFAULT_TRADE_ROWS_PATH))
    p.add_argument("--out-round1", type=str, default=str(ROOT / DEFAULT_ROUND1_PATH))
    args = p.parse_args()

    if not args.execute and not args.html_path:
        print("Dry run only. No web calls made.")
        print(f"Source URL: {args.url}")
        print("Planned outputs:")
        print(f"- Raw HTML: {args.out_raw_html}")
        print(f"- Full order CSV: {args.out_full_order}")
        print(f"- Trade details CSV: {args.out_trades}")
        print(f"- Round 1 order CSV: {args.out_round1}")
        print("\nUse --execute for live pull or --html-path to parse local HTML.")
        return

    if args.html_path:
        html_path = Path(args.html_path)
        if not html_path.exists():
            raise SystemExit(f"Local HTML not found: {html_path}")
        page_html = _read_html_with_fallback(html_path)
    else:
        page_html = fetch_drafttek_trade_value_html(url=args.url)
        raw_out = Path(args.out_raw_html)
        raw_out.parent.mkdir(parents=True, exist_ok=True)
        raw_out.write_text(page_html)

    full_rows, trade_rows, round1_rows = parse_drafttek_order(page_html=page_html, source_url=args.url)
    if not full_rows:
        raise SystemExit("No draft-order rows parsed from Drafttek page. Review parser patterns.")

    write_csv(full_rows, Path(args.out_full_order))
    write_csv(trade_rows, Path(args.out_trades))
    write_csv(round1_rows, Path(args.out_round1))

    print(f"Full order rows: {len(full_rows)}")
    print(f"Trade rows: {len(trade_rows)}")
    print(f"Round 1 rows: {len(round1_rows)}")
    print(f"Wrote: {args.out_full_order}")
    print(f"Wrote: {args.out_trades}")
    print(f"Wrote: {args.out_round1}")


if __name__ == "__main__":
    main()
