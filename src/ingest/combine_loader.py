from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMBINE_PATH = ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv"


NUMERIC_FIELDS = {
    "height_in",
    "weight_lb",
    "arm_in",
    "hand_in",
    "forty",
    "ten_split",
    "vertical",
    "broad",
    "shuttle",
    "three_cone",
    "bench",
    "ras_official",
}


REQUIRED_FIELDS = {
    "player_name",
    "school",
    "position",
    "height_in",
    "weight_lb",
    "ras_official",
}


def _to_float_or_none(value: str | None) -> float | None:
    if value is None:
        return None
    txt = value.strip()
    if not txt or txt.upper() in {"N/A", "NA", "NULL", "NONE", "-"}:
        return None
    try:
        return float(txt)
    except ValueError:
        return None



def load_combine_results(path: Path | None = None) -> Dict[str, dict]:
    path = path or DEFAULT_COMBINE_PATH
    if not path.exists():
        return {}

    with path.open() as f:
        reader = csv.DictReader(f)
        fields = set(reader.fieldnames or [])
        missing = REQUIRED_FIELDS - fields
        if missing:
            raise ValueError(f"Combine file missing required columns: {sorted(missing)}")

        out: Dict[str, dict] = {}
        for row in reader:
            player = (row.get("player_name") or "").strip()
            if not player:
                continue

            key = canonical_player_name(player)
            payload = {
                "combine_player_name": player,
                "combine_school": (row.get("school") or "").strip(),
                "combine_position": normalize_pos(row.get("position", "")),
                "combine_source": (row.get("source") or "").strip(),
                "combine_last_updated": (row.get("last_updated") or "").strip(),
            }

            for field in NUMERIC_FIELDS:
                payload[field] = _to_float_or_none(row.get(field))

            out[key] = payload

    return out
