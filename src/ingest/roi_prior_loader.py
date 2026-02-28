from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = ROOT / "data" / "processed" / "position_roi_priors_leagify_2016_2023.csv"


def pick_band_from_rank(rank: int) -> str:
    if rank <= 32:
        return "R1"
    if rank <= 64:
        return "R2"
    if rank <= 100:
        return "R3"
    if rank <= 150:
        return "R4"
    return "R5+"


def load_position_roi_priors(path: Path | None = None) -> dict[tuple[str, str], dict]:
    path = path or DEFAULT_PATH
    if not path.exists():
        return {}

    out: dict[tuple[str, str], dict] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            pos = str(row.get("position", "")).strip().upper()
            band = str(row.get("pick_band", "")).strip().upper()
            if not pos or not band:
                continue

            def _to_float(value, default: float = 0.0) -> float:
                txt = str(value or "").strip()
                if not txt:
                    return default
                try:
                    return float(txt)
                except ValueError:
                    return default

            out[(pos, band)] = {
                "position": pos,
                "pick_band": band,
                "sample_n": int(_to_float(row.get("sample_n"), 0.0)),
                "weighted_n": _to_float(row.get("weighted_n"), 0.0),
                "weighted_mean_surplus": _to_float(row.get("weighted_mean_surplus"), 0.0),
                "weighted_success_rate": _to_float(row.get("weighted_success_rate"), 0.0),
                "surplus_z": _to_float(row.get("surplus_z"), 0.0),
                "roi_grade_adjustment": _to_float(row.get("roi_grade_adjustment"), 0.0),
            }
    return out
