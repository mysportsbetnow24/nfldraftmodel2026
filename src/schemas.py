from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


POSITION_MAP = {
    "SAF": "S",
    "FS": "S",
    "SS": "S",
    "CBN": "CB",
    "CB": "CB",
    "WRX": "WR",
    "WRZ": "WR",
    "WRS": "WR",
    "TEY": "TE",
    "F": "TE",
    "OT": "OT",
    "IOLC": "IOL",
    "IOLG": "IOL",
    "C": "IOL",
    "G": "IOL",
    "EDGE": "EDGE",
    "DE": "EDGE",
    "OLB": "LB",
    "LBILB": "LB",
    "LBOLB": "LB",
    "DT1T": "DT",
    "DT3T": "DT",
    "DT": "DT",
    "QB": "QB",
    "RB": "RB",
}


ROUND_MAP = [
    (92, "Round 1"),
    (88, "Round 1-2"),
    (84, "Round 2-3"),
    (80, "Round 3-4"),
    (76, "Round 4-5"),
    (72, "Round 5-6"),
    (68, "Round 6-7"),
    (0, "UDFA"),
]


@dataclass
class Prospect:
    rank_seed: int
    player_name: str
    school: str
    pos_raw: str
    height: str
    weight_lb: int
    class_year: str

    @property
    def position(self) -> str:
        return POSITION_MAP.get(self.pos_raw, self.pos_raw)

    @property
    def height_in(self) -> Optional[int]:
        return parse_height_to_inches(self.height)



def parse_height_to_inches(height: str) -> Optional[int]:
    try:
        feet, inches = height.replace('"', "").split("'")
        return int(feet) * 12 + int(inches)
    except Exception:
        return None



def round_from_grade(grade: float) -> str:
    for min_grade, label in ROUND_MAP:
        if grade >= min_grade:
            return label
    return "UDFA"
