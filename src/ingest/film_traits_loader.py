from __future__ import annotations

import csv
from pathlib import Path

from src.modeling.film_traits import ALL_FILM_TRAITS


ROOT = Path(__file__).resolve().parents[2]
FILM_TRAITS_PATH = ROOT / "data" / "sources" / "manual" / "film_traits_2026.csv"


def _to_float(value: str) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _coverage_count(row: dict) -> int:
    return sum(1 for trait in ALL_FILM_TRAITS if _to_float(row.get(trait, "")) is not None)


def load_film_trait_rows(path: Path | None = None) -> list[dict]:
    path = path or FILM_TRAITS_PATH
    if not path.exists():
        return []

    out = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("player_name") or "").strip()
            position = (row.get("position") or "").strip()
            if not name or not position:
                continue

            traits = {trait: _to_float(row.get(trait, "")) for trait in ALL_FILM_TRAITS}
            out.append(
                {
                    "player_name": name,
                    "position": position,
                    "school": (row.get("school") or "").strip(),
                    "source": (row.get("source") or "").strip(),
                    "eval_date": (row.get("eval_date") or "").strip(),
                    "coverage_count": _coverage_count(row),
                    "traits": traits,
                }
            )

    return out
