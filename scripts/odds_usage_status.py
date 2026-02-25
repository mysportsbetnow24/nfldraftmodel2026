#!/usr/bin/env python3
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.odds_loader import OddsQuotaTracker


def main() -> None:
    tracker = OddsQuotaTracker()
    status = tracker.status()

    start = datetime.strptime(status.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(status.end_date, "%Y-%m-%d").date()
    today = datetime.now(timezone.utc).date()

    total_days = (end - start).days + 1
    elapsed_days = max(1, min(total_days, (today - start).days + 1))
    expected_used = status.max_calls * elapsed_days / total_days
    pace_delta = status.calls_used - expected_used

    print(f"Odds campaign window: {status.start_date} -> {status.end_date} ({total_days} days)")
    print(f"Calls used: {status.calls_used}")
    print(f"Calls remaining: {status.calls_remaining}")
    print(f"Campaign cap: {status.max_calls}")
    print(f"Expected used by today: {expected_used:.2f}")
    print(f"Pace delta (used - expected): {pace_delta:.2f}")


if __name__ == "__main__":
    main()
