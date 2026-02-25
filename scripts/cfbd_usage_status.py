#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.cfbd_loader import CFBDQuotaTracker


def main() -> None:
    tracker = CFBDQuotaTracker(max_calls=1000)
    status = tracker.status()
    print(f"CFBD usage month: {status.month}")
    print(f"Calls used: {status.calls_used}")
    print(f"Calls remaining: {status.calls_remaining}")
    print(f"Monthly cap: {status.max_calls}")


if __name__ == "__main__":
    main()
