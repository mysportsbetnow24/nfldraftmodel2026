#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.sportsanalytics_catalog import as_rows
from src.modeling.feature_engineering import load_seed_rows

PROCESSED = ROOT / "data" / "processed"


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    seed_rows = load_seed_rows()
    for i, row in enumerate(seed_rows, start=1):
        row["seed_row_id"] = i

    write_csv(PROCESSED / "prospect_seed_2026.csv", seed_rows)
    write_csv(PROCESSED / "sportsanalytics_package_catalog.csv", as_rows())

    print(f"Seed rows parsed: {len(seed_rows)}")
    print(f"Seed rows retained: {len(seed_rows)}")


if __name__ == "__main__":
    main()
