from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = ROOT / "data" / "sources"


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
}


def normalize_pos(pos: str) -> str:
    return POS_MAP.get(pos, pos)


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
        by_player[row["player_name"]].append(score)
    return {player: sum(scores) / len(scores) for player, scores in by_player.items()}
