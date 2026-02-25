#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.mockdraftable_loader import (  # noqa: E402
    DEFAULT_POSITION_MAP,
    POSITION_PAGE,
    pull_position_baselines,
    write_position_baselines,
)

DEFAULT_OUT = ROOT / "data" / "sources" / "mockdraftable_position_baselines.csv"


def _parse_position_map(raw: str) -> dict:
    """
    Parse CSV mapping string like: "QB:QB,RB:RB,IOL:IOL".
    """
    out = {}
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"Invalid mapping segment: {part}. Expected MODEL:SOURCE")
        model_pos, source_pos = part.split(":", 1)
        out[model_pos.strip().upper()] = source_pos.strip().upper()
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Pull MockDraftable position aggregate baselines")
    p.add_argument("--execute", action="store_true", help="Execute live web pulls (default is dry-run)")
    p.add_argument(
        "--position-map",
        type=str,
        default=",".join(f"{k}:{v}" for k, v in DEFAULT_POSITION_MAP.items()),
        help="Position map MODEL:SOURCE pairs, comma-separated",
    )
    p.add_argument("--out", type=str, default=str(DEFAULT_OUT), help="Output CSV path")
    args = p.parse_args()

    position_map = _parse_position_map(args.position_map)

    if not args.execute:
        print("Dry run only. No web calls made.")
        print("Positions to pull:")
        for model_pos, source_pos in position_map.items():
            print(f"- {model_pos}: {POSITION_PAGE.format(position_code=source_pos)}")
        print("\nUse --execute to pull live data.")
        return

    rows = pull_position_baselines(position_map=position_map)
    out_path = Path(args.out)
    write_position_baselines(rows, path=out_path)

    print(f"Pulled {len(rows)} position baseline rows")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
