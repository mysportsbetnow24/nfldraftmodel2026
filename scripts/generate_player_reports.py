#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.publish.render_reports import render_reports


if __name__ == "__main__":
    render_reports()
    print("Player reports generated.")
