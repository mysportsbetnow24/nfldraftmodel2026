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

ATHLETIC_TEST_FIELDS = {
    "forty",
    "ten_split",
    "vertical",
    "broad",
    "shuttle",
    "three_cone",
    "bench",
}

MEASUREMENT_FIELDS = {
    "height_in",
    "weight_lb",
    "arm_in",
    "hand_in",
}

KNOWN_TESTING_STATUS = {"reported", "pending", "dnp", "unknown"}


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


def _derive_testing_status(row: dict, payload: dict) -> str:
    explicit = str(row.get("testing_status") or row.get("combine_testing_status") or "").strip().lower()
    if explicit in KNOWN_TESTING_STATUS:
        return explicit

    has_athletic = any(payload.get(field) is not None for field in ATHLETIC_TEST_FIELDS)
    has_ras = payload.get("ras_official") is not None
    if has_athletic or has_ras:
        return "reported"

    source_txt = str(row.get("source") or "").strip().lower()
    if "dnp" in source_txt or "did not" in source_txt:
        return "dnp"

    # Source row exists but no athletic tests yet: treat as pending live cycle updates.
    if source_txt:
        return "pending"
    return "unknown"



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

            payload["combine_testing_event_count"] = sum(
                1 for field in ATHLETIC_TEST_FIELDS if payload.get(field) is not None
            )
            payload["combine_measurement_count"] = sum(
                1 for field in MEASUREMENT_FIELDS if payload.get(field) is not None
            )
            payload["combine_testing_status"] = _derive_testing_status(row, payload)

            out[key] = payload

    return out
