from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HIST_COMBINE_PATH = ROOT / "data" / "sources" / "external" / "combine_data_unique_athlete_id_step4.csv"

METRIC_MAP = {
    "height_in": "Height (in)",
    "weight_lb": "Weight (lbs)",
    "arm_in": "Arm Length (in)",
    "hand_in": "Hand Size (in)",
    "forty": "40 Yard",
    "ten_split": "10-Yard Split",
    "vertical": "Vert Leap (in)",
    "broad": "Broad Jump (in)",
    "three_cone": "3Cone",
    "shuttle": "Shuttle",
    "bench": "Bench Press",
    "wingspan_in": "Wingspan (in)",
}


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _to_int(value) -> int | None:
    f = _to_float(value)
    if f is None:
        return None
    return int(f)


def _school_key(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _extract_position(row: dict) -> str:
    raw_pos = str(row.get("POS", "")).strip()
    if raw_pos:
        pos = normalize_pos(raw_pos)
    else:
        pos_gp = str(row.get("POS_GP", "")).strip().upper()
        if "-" in pos_gp:
            pos_gp = pos_gp.split("-", 1)[0].strip()
        pos = normalize_pos(pos_gp)

    manual_map = {
        "DE": "EDGE",
        "EDG": "EDGE",
        "OLB": "EDGE",
        "ILB": "LB",
        "NT": "DT",
        "DL": "DT",
        "DB": "S",
        "FS": "S",
        "SS": "S",
    }
    pos = manual_map.get(pos, pos)
    return pos


def build_combine_merge_key(
    *,
    player_name: str,
    position: str,
    school: str = "",
    year: int | None = None,
) -> str:
    y = str(int(year)) if year is not None else ""
    return "|".join(
        [
            canonical_player_name(player_name),
            normalize_pos(position),
            _school_key(school),
            y,
        ]
    )


def _metric_stats(rows: list[dict]) -> dict[str, tuple[float, float]]:
    stats: dict[str, tuple[float, float]] = {}
    for metric in METRIC_MAP.keys():
        vals = [float(r[metric]) for r in rows if r.get(metric) is not None]
        if len(vals) < 25:
            continue
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = math.sqrt(max(var, 1e-9))
        stats[metric] = (mean, std)
    return stats


def load_historical_combine_profiles(path: Path | None = None) -> dict:
    path = path or DEFAULT_HIST_COMBINE_PATH
    if not path.exists():
        return {
            "rows": [],
            "by_pos": {},
            "stats_by_pos": {},
            "meta": {"status": "missing", "path": str(path), "rows": 0},
        }

    rows: list[dict] = []
    with path.open() as f:
        for row in csv.DictReader(f):
            player_name = str(row.get("player", "")).strip()
            if not player_name:
                continue
            pos = _extract_position(row)
            if not pos:
                continue
            year = _to_int(row.get("Year"))
            school = str(row.get("College", "")).strip()

            payload = {
                "year": year,
                "player_name": player_name,
                "player_key": canonical_player_name(player_name),
                "school": school,
                "school_key": _school_key(school),
                "position": pos,
                "athlete_id": str(row.get("athlete_id", "")).strip(),
                "nfl_person_id": str(row.get("nfl_person_id", "")).strip(),
                "merge_key": build_combine_merge_key(
                    player_name=player_name,
                    position=pos,
                    school=school,
                    year=year,
                ),
            }

            for metric_key, src_col in METRIC_MAP.items():
                payload[metric_key] = _to_float(row.get(src_col))

            rows.append(payload)

    by_pos: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_pos[r["position"]].append(r)

    stats_by_pos = {pos: _metric_stats(pos_rows) for pos, pos_rows in by_pos.items()}
    return {
        "rows": rows,
        "by_pos": dict(by_pos),
        "stats_by_pos": stats_by_pos,
        "meta": {"status": "ok", "path": str(path), "rows": len(rows), "positions": len(by_pos)},
    }


def _distance_for_candidate(
    metrics: dict,
    candidate: dict,
    stats: dict[str, tuple[float, float]],
) -> tuple[float, int]:
    terms = []
    overlap = 0
    for metric in METRIC_MAP.keys():
        if metric not in stats:
            continue
        current_val = metrics.get(metric)
        cand_val = candidate.get(metric)
        if current_val is None or cand_val is None:
            continue
        mean, std = stats[metric]
        z = (float(current_val) - float(cand_val)) / max(std, 1e-6)
        terms.append(z * z)
        overlap += 1
    if not terms:
        return float("inf"), 0
    return math.sqrt(sum(terms) / len(terms)), overlap


def find_historical_combine_comps(
    *,
    position: str,
    current_metrics: dict,
    pack: dict,
    k: int = 3,
    min_overlap_metrics: int = 3,
) -> dict:
    pos = normalize_pos(position)
    candidates = pack.get("by_pos", {}).get(pos, [])
    stats = pack.get("stats_by_pos", {}).get(pos, {})
    if not candidates or not stats:
        return {"comps": [], "candidate_count": 0, "used_overlap_min": min_overlap_metrics}

    available_metrics = sum(1 for m in METRIC_MAP.keys() if current_metrics.get(m) is not None)
    # Require at least 3 available combine metrics for meaningful comps.
    if available_metrics < 3:
        return {"comps": [], "candidate_count": len(candidates), "used_overlap_min": min_overlap_metrics}
    overlap_min = min_overlap_metrics

    scored = []
    for cand in candidates:
        dist, overlap = _distance_for_candidate(current_metrics, cand, stats)
        if overlap < overlap_min or not math.isfinite(dist):
            continue
        # 0 distance => 100; ~2.5 distance => ~58.
        similarity = max(1.0, min(99.9, 100.0 - (16.5 * dist)))
        scored.append(
            {
                "player_name": cand["player_name"],
                "year": cand.get("year"),
                "school": cand.get("school", ""),
                "distance": round(dist, 4),
                "similarity": round(similarity, 2),
                "overlap_metrics": overlap,
                "merge_key": cand.get("merge_key", ""),
                "athlete_id": cand.get("athlete_id", ""),
            }
        )

    scored.sort(key=lambda x: (x["distance"], -x["overlap_metrics"]))
    return {
        "comps": scored[:k],
        "candidate_count": len(candidates),
        "used_overlap_min": overlap_min,
    }
