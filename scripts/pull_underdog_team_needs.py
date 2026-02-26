#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.underdog_team_needs_loader import (  # noqa: E402
    DEFAULT_MATRIX_PATH,
    DEFAULT_NORMALIZED_PATH,
    DEFAULT_RAW_PATH,
    DEFAULT_TEAM_PATCH_PATH,
    DEFAULT_URL,
    build_need_matrix,
    build_team_profiles_patch,
    fetch_underdog_team_needs_html,
    parse_team_needs,
    write_csv,
)


def main() -> None:
    p = argparse.ArgumentParser(description="Pull and normalize UnderDog Network 2026 NFL team needs article.")
    p.add_argument("--execute", action="store_true", help="Execute live web pull (default dry-run).")
    p.add_argument("--url", type=str, default=DEFAULT_URL)
    p.add_argument("--out-raw", type=str, default=str(DEFAULT_RAW_PATH))
    p.add_argument("--out-normalized", type=str, default=str(DEFAULT_NORMALIZED_PATH))
    p.add_argument("--out-matrix", type=str, default=str(DEFAULT_MATRIX_PATH))
    p.add_argument("--out-team-patch", type=str, default=str(DEFAULT_TEAM_PATCH_PATH))
    args = p.parse_args()

    if not args.execute:
        print("Dry run only. No web calls made.")
        print(f"Source URL: {args.url}")
        print("Planned outputs:")
        print(f"- Raw rows: {args.out_raw}")
        print(f"- Normalized rows: {args.out_normalized}")
        print(f"- Team/position matrix: {args.out_matrix}")
        print(f"- team_profiles patch: {args.out_team_patch}")
        print("\nUse --execute to pull live data.")
        return

    page_html = fetch_underdog_team_needs_html(url=args.url)
    raw_rows = parse_team_needs(page_html=page_html, source_url=args.url)
    if not raw_rows:
        raise SystemExit("No team-needs rows parsed from page. Review parser patterns.")

    normalized_rows = list(raw_rows)
    matrix_rows = build_need_matrix(normalized_rows)
    team_patch_rows = build_team_profiles_patch(normalized_rows)

    write_csv(raw_rows, Path(args.out_raw))
    write_csv(normalized_rows, Path(args.out_normalized))
    write_csv(matrix_rows, Path(args.out_matrix))
    write_csv(team_patch_rows, Path(args.out_team_patch))

    team_count = len({r.get("team_name", "") for r in raw_rows})
    print(f"Parsed team sections: {team_count}")
    print(f"Raw rows: {len(raw_rows)}")
    print(f"Matrix rows: {len(matrix_rows)}")
    print(f"Wrote: {args.out_raw}")
    print(f"Wrote: {args.out_normalized}")
    print(f"Wrote: {args.out_matrix}")
    print(f"Wrote: {args.out_team_patch}")


if __name__ == "__main__":
    main()
