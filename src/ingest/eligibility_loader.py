from __future__ import annotations

import csv
from pathlib import Path
from typing import Set

from src.ingest.rankings_loader import canonical_player_name


ROOT = Path(__file__).resolve().parents[2]
MANUAL_DIR = ROOT / "data" / "sources" / "manual"
DECLARED_PATH = MANUAL_DIR / "declared_underclassmen_2026_official.csv"
DECLARED_OVERRIDE_PATH = MANUAL_DIR / "declared_overrides_2026.csv"
RETURNING_PATH = MANUAL_DIR / "returning_to_school_2026.csv"
ALREADY_DRAFTED_PATH = MANUAL_DIR / "already_in_nfl_exclusions.csv"


def _load_name_set(path: Path, field_name: str = "player_name") -> Set[str]:
    if not path.exists():
        return set()
    out: Set[str] = set()
    with path.open() as f:
        for row in csv.DictReader(f):
            val = row.get(field_name, "")
            if val:
                out.add(canonical_player_name(val))
    return out


def load_declared_underclassmen() -> Set[str]:
    declared = _load_name_set(DECLARED_PATH)
    declared |= _load_name_set(DECLARED_OVERRIDE_PATH)
    return declared


def load_returning_to_school() -> Set[str]:
    return _load_name_set(RETURNING_PATH)


def load_already_in_nfl_exclusions() -> Set[str]:
    return _load_name_set(ALREADY_DRAFTED_PATH)


def is_senior_class(class_year: str) -> bool:
    cy = (class_year or "").upper().strip()
    return cy.endswith("SR") or cy in {"SR", "RSR", "GRAD", "GSR", "RGSR"}
