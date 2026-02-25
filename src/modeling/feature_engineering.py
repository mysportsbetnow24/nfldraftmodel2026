from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[2]
SEED_PATH = ROOT / "data" / "sources" / "drafttek_2026_top300_seed.txt"


SCHOOL_CANDIDATES = [
    "Miami (FL)", "Ohio State", "South Carolina", "Arizona State", "Florida State", "Penn State",
    "Texas A&M", "North Carolina", "Mississippi State", "Georgia Tech", "West Virginia", "Notre Dame",
    "Texas Tech", "Oregon State", "Wake Forest", "Clemson", "Indiana", "Alabama", "Auburn", "Georgia",
    "Oregon", "Texas", "LSU", "USC", "Utah", "Vanderbilt", "Washington", "Florida", "Pittsburgh",
    "Michigan", "Oklahoma", "Duke", "Cincinnati", "Sacramento State", "Houston", "Louisville", "UCLA",
    "Syracuse", "Minnesota", "Colorado", "Iowa", "Arizona", "Missouri", "Rutgers", "Arkansas", "NC State",
    "Kansas", "Tennessee", "Kentucky", "Purdue", "Baylor", "TCU", "Navy", "SMU", "UTSA", "Mississippi",
    "Boise State", "Wisconsin", "Kansas State", "Virginia Tech",
]
SCHOOL_CANDIDATES = sorted(SCHOOL_CANDIDATES, key=lambda x: len(x.split()), reverse=True)


_LINE_RE = re.compile(
    r"^(?P<rank>\d+)\s+(?:--|\+\d+)?\s*(?P<body>.+?)\s+(?P<pos>[A-Z0-9]+)\s+(?P<height>\d+'\d+\")\s+(?P<weight>\d+)\s+(?P<class>[A-Z0-9.]+)$"
)



def _split_name_school(body: str) -> Tuple[str, str]:
    compact = body.strip()
    for school in SCHOOL_CANDIDATES:
        if compact.endswith(" " + school) or compact == school:
            name = compact[: -len(school)].strip()
            if name:
                return name, school

    # fallback: assume last token belongs to school
    parts = compact.split()
    if len(parts) == 1:
        return compact, "Unknown"
    return " ".join(parts[:-1]), parts[-1]



def parse_seed_line(line: str) -> Dict | None:
    line = line.strip()
    if not line:
        return None

    match = _LINE_RE.match(line)
    if not match:
        return None

    rank = int(match.group("rank"))
    body = match.group("body")
    name, school = _split_name_school(body)

    return {
        "rank_seed": rank,
        "player_name": name,
        "school": school,
        "pos_raw": match.group("pos"),
        "height": match.group("height"),
        "weight_lb": int(match.group("weight")),
        "class_year": match.group("class").replace(".", ""),
        "source_primary": "DraftTek_Top300_2026",
    }



def load_seed_rows(path: Path | None = None) -> List[Dict]:
    path = path or SEED_PATH
    rows: List[Dict] = []
    with path.open() as f:
        for line in f:
            row = parse_seed_line(line)
            if row:
                rows.append(row)
    rows.sort(key=lambda x: x["rank_seed"])
    return rows
