from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = ROOT / "data" / "sources"
MANUAL_DIR = SOURCES_DIR / "manual"


POS_MAP = {
    "CBN": "CB",
    "SAF": "S",
    "WRX": "WR",
    "WRZ": "WR",
    "TEY": "TE",
    "IOLC": "IOL",
    "IOLG": "IOL",
    "DT1T": "DT",
    "DT3T": "DT",
    "LBILB": "LB",
    "LBOLB": "LB",
    # external board mappings
    "ED": "EDGE",
    "DI": "DT",
    "IDL": "DT",
    "G": "IOL",
    "C": "IOL",
    "OG": "IOL",
    "OC": "IOL",
    "OL": "IOL",
    "T": "OT",
    "LT": "OT",
    "RT": "OT",
    "HB": "RB",
    "FB": "RB",
    "FS": "S",
    "SS": "S",
}


def normalize_pos(pos: str) -> str:
    return POS_MAP.get((pos or "").strip().upper(), (pos or "").strip().upper())


def canonical_player_name(name: str) -> str:
    normalized = (name or "").lower().strip()
    normalized = normalized.replace(".", "")
    normalized = normalized.replace("'", "")
    normalized = re.sub(r"[^a-z0-9\s-]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


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



def load_analyst_rows(path: Path | None = None) -> List[dict]:
    path = path or SOURCES_DIR / "analyst_rankings_seed.csv"
    rows: List[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["source_rank"] = int(row["source_rank"])
            row["position"] = normalize_pos(row["position"])
            rows.append(row)
    return rows



def analyst_aggregate_score(rows: List[dict]) -> Dict[str, float]:
    by_player = defaultdict(list)
    for row in rows:
        score = max(1.0, 101.0 - float(row["source_rank"]))
        by_player[canonical_player_name(row["player_name"])].append(score)
    return {player: sum(scores) / len(scores) for player, scores in by_player.items()}



def load_external_big_board(path: Path | None = None) -> Dict[str, dict]:
    path = path or (MANUAL_DIR / "nfl-draft-bigboard-scout-mode-2026-02-25.csv")
    if not path.exists():
        return {}

    out: Dict[str, dict] = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(float(row.get("Rank", "") or 0))
            except ValueError:
                continue

            if rank <= 0:
                continue

            name = row.get("Player", "").strip()
            key = canonical_player_name(name)

            payload = {
                "external_rank": rank,
                "external_pos": normalize_pos(row.get("Pos", "")),
                "external_school": row.get("School", "").strip(),
                "pff_grade": _to_float_or_none(row.get("PFF Grade")),
                "pff_waa": _to_float_or_none(row.get("PFF WAA")),
                "external_notes": row.get("Notes", "").strip(),
            }

            existing = out.get(key)
            if existing is None or payload["external_rank"] < existing["external_rank"]:
                out[key] = payload

    return out



def load_external_big_board_rows(path: Path | None = None) -> List[dict]:
    """Returns deduped external board rows with player identity preserved."""
    path = path or (MANUAL_DIR / "nfl-draft-bigboard-scout-mode-2026-02-25.csv")
    if not path.exists():
        return []

    by_key: Dict[str, dict] = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(float(row.get("Rank", "") or 0))
            except ValueError:
                continue
            if rank <= 0:
                continue

            player_name = row.get("Player", "").strip()
            key = canonical_player_name(player_name)
            payload = {
                "player_name": player_name,
                "external_rank": rank,
                "external_pos": normalize_pos(row.get("Pos", "")),
                "external_school": row.get("School", "").strip(),
                "pff_grade": _to_float_or_none(row.get("PFF Grade")),
                "pff_waa": _to_float_or_none(row.get("PFF WAA")),
                "external_notes": row.get("Notes", "").strip(),
            }

            cur = by_key.get(key)
            if cur is None or payload["external_rank"] < cur["external_rank"]:
                by_key[key] = payload

    rows = list(by_key.values())
    rows.sort(key=lambda r: r["external_rank"])
    return rows
