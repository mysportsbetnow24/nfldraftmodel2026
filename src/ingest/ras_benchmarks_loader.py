from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict

from src.ingest.rankings_loader import normalize_pos


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = ROOT / "data" / "processed" / "ras_position_benchmarks.csv"


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def load_ras_benchmarks(path: Path | None = None) -> Dict[str, dict]:
    path = path or DEFAULT_PATH
    if not path.exists():
        return {}

    out: Dict[str, dict] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            pos = normalize_pos(row.get("position", ""))
            if not pos:
                continue
            out[pos] = {
                "starter_target_ras": _to_float(row.get("starter_target_ras")),
                "impact_target_ras": _to_float(row.get("impact_target_ras")),
                "elite_target_ras": _to_float(row.get("elite_target_ras")),
                "sample_n_all": row.get("sample_n_all", ""),
            }
    return out
