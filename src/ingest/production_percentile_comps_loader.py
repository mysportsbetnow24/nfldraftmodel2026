from __future__ import annotations

import bisect
import csv
import math
from collections import defaultdict
from pathlib import Path

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
PATH_CANDIDATES = [
    ROOT / "data" / "sources" / "external" / "filtered_player_stats_full.csv",
    ROOT / "data" / "sources" / "external" / "array-carpenter-nfl-draft-data" / "filtered_player_stats_full.csv",
]

# Position-specific baseline vectors used for percentile normalization and KNN comps.
# These are explainer features only (not grade drivers).
POSITION_BASELINES = {
    "QB": ["passing_ypa", "passing_pct", "passing_td", "passing_int", "rushing_yds", "rushing_td", "fumbles_fum"],
    "RB": ["rushing_ypc", "rushing_yds", "rushing_td", "receiving_rec", "receiving_yds", "fumbles_fum"],
    "WR": ["receiving_rec", "receiving_yds", "receiving_td", "receiving_ypr", "fumbles_fum"],
    "TE": ["receiving_rec", "receiving_yds", "receiving_td", "receiving_ypr", "fumbles_fum"],
    "EDGE": ["defensive_tfl", "defensive_sacks", "defensive_qb_hur", "defensive_tot", "fumbles_fum"],
    "DT": ["defensive_tfl", "defensive_sacks", "defensive_qb_hur", "defensive_tot"],
    "LB": ["defensive_tfl", "defensive_sacks", "defensive_qb_hur", "defensive_tot", "interceptions_int", "defensive_pd"],
    "CB": ["interceptions_int", "defensive_pd", "defensive_tot", "interceptions_avg"],
    "S": ["interceptions_int", "defensive_pd", "defensive_tot", "interceptions_avg"],
}

# Metrics where lower values are better.
REVERSE_DIRECTION_METRICS = {
    "passing_int",
    "fumbles_fum",
}

# Rate metrics are averaged across multi-team seasons; counting stats are summed.
RATE_METRICS = {
    "passing_pct",
    "passing_ypa",
    "rushing_ypc",
    "receiving_ypr",
    "interceptions_avg",
}


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _percentile(values: list[float], value: float, *, lower_better: bool) -> float:
    if not values:
        return 50.0
    n = float(len(values))
    lo = bisect.bisect_left(values, value)
    hi = bisect.bisect_right(values, value)
    rank = (lo + hi) / 2.0
    frac = rank / n
    pct = (1.0 - frac) if lower_better else frac
    return max(0.0, min(1.0, pct)) * 100.0


def _distance(a: dict[str, float], b: dict[str, float], metrics: list[str]) -> tuple[float, int]:
    diffs = []
    overlap = 0
    for m in metrics:
        av = a.get(m)
        bv = b.get(m)
        if av is None or bv is None:
            continue
        overlap += 1
        diffs.append((float(av) - float(bv)) ** 2)
    if overlap == 0:
        return float("inf"), 0
    # Percentiles are 0-100; scale to 0-1 before euclidean distance.
    return math.sqrt(sum(diffs) / overlap) / 100.0, overlap


def _position_aliases(raw: str) -> list[str]:
    pos = normalize_pos(str(raw or "").strip())
    if pos in {"DE", "OLB"}:
        return ["EDGE"]
    if pos in {"DT", "NT"}:
        return ["DT"]
    if pos in {"QB", "RB", "WR", "TE", "LB", "CB", "S", "EDGE", "DT"}:
        return [pos]
    # CFBD-style broad buckets in this source.
    if pos == "DL":
        return ["EDGE", "DT"]
    if pos == "DB":
        return ["CB", "S"]
    return []


def _discover_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path
    for candidate in PATH_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def _aggregate_rows(path: Path) -> list[dict]:
    metric_universe = sorted({m for metrics in POSITION_BASELINES.values() for m in metrics})

    grouped: dict[tuple[str, int, str], dict] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            name = str(row.get("player", "")).strip()
            if not name:
                continue
            year = int(_to_float(row.get("year")) or 0)
            if year <= 0:
                continue
            aliases = _position_aliases(row.get("position", ""))
            if not aliases:
                continue
            name_key = canonical_player_name(name)
            for pos in aliases:
                key = (name_key, year, pos)
                payload = grouped.get(key)
                if payload is None:
                    payload = {
                        "name_key": name_key,
                        "player_name": name,
                        "year": year,
                        "position": pos,
                        "count_n": 0,
                        "sum_metrics": defaultdict(float),
                        "avg_metrics": defaultdict(list),
                    }
                    grouped[key] = payload
                payload["count_n"] += 1
                for metric in metric_universe:
                    val = _to_float(row.get(metric))
                    if val is None:
                        continue
                    if metric in RATE_METRICS:
                        payload["avg_metrics"][metric].append(val)
                    else:
                        payload["sum_metrics"][metric] += val

    out: list[dict] = []
    for payload in grouped.values():
        row_out = {
            "name_key": payload["name_key"],
            "player_name": payload["player_name"],
            "year": payload["year"],
            "position": payload["position"],
            "count_n": payload["count_n"],
        }
        for metric in metric_universe:
            if metric in RATE_METRICS:
                vals = payload["avg_metrics"].get(metric, [])
                row_out[metric] = (sum(vals) / len(vals)) if vals else None
            else:
                val = payload["sum_metrics"].get(metric, 0.0)
                row_out[metric] = val if val != 0.0 else None
        out.append(row_out)
    return out


def load_production_percentile_pack(path: Path | None = None) -> dict:
    src = _discover_path(path)
    if src is None:
        return {
            "meta": {"status": "missing", "path": "", "rows": 0},
            "by_pos": {},
            "by_name_pos": {},
            "position_baselines": POSITION_BASELINES,
            "reverse_metrics": sorted(REVERSE_DIRECTION_METRICS),
        }

    rows = _aggregate_rows(src)
    by_pos: dict[str, list[dict]] = defaultdict(list)
    by_name_pos: dict[tuple[str, str], list[dict]] = defaultdict(list)

    # Per-season normalization before percentiles.
    season_values: dict[tuple[str, int, str], list[float]] = defaultdict(list)
    for r in rows:
        pos = r["position"]
        year = int(r["year"])
        for metric in POSITION_BASELINES.get(pos, []):
            v = _to_float(r.get(metric))
            if v is None:
                continue
            season_values[(pos, year, metric)].append(float(v))
    for key, vals in season_values.items():
        vals.sort()
        season_values[key] = vals

    for r in rows:
        pos = r["position"]
        year = int(r["year"])
        metrics = POSITION_BASELINES.get(pos, [])
        pct_vec: dict[str, float] = {}
        for metric in metrics:
            value = _to_float(r.get(metric))
            values = season_values.get((pos, year, metric), [])
            if value is None or not values:
                continue
            pct_vec[metric] = round(
                _percentile(values, float(value), lower_better=(metric in REVERSE_DIRECTION_METRICS)),
                3,
            )
        row_copy = dict(r)
        row_copy["percentile_vector"] = pct_vec
        by_pos[pos].append(row_copy)
        by_name_pos[(r["name_key"], pos)].append(row_copy)

    years = sorted({int(r["year"]) for r in rows}) if rows else []
    return {
        "meta": {
            "status": "ok",
            "path": str(src),
            "rows": len(rows),
            "years_min": years[0] if years else "",
            "years_max": years[-1] if years else "",
            "years_count": len(years),
            "positions": len(by_pos),
        },
        "by_pos": dict(by_pos),
        "by_name_pos": dict(by_name_pos),
        "position_baselines": POSITION_BASELINES,
        "reverse_metrics": sorted(REVERSE_DIRECTION_METRICS),
    }


def find_production_percentile_comps(
    *,
    player_name: str,
    position: str,
    pack: dict,
    target_season: int = 2025,
    k: int = 3,
    min_overlap: int = 3,
) -> dict:
    pos = normalize_pos(position)
    metrics = list(POSITION_BASELINES.get(pos, []))
    if not metrics:
        return {"comps": [], "coverage": 0, "metric_count": 0, "source": "production_percentile_knn"}

    name_key = canonical_player_name(player_name)
    current_rows = list(pack.get("by_name_pos", {}).get((name_key, pos), []))
    if not current_rows:
        return {"comps": [], "coverage": 0, "metric_count": len(metrics), "source": "production_percentile_knn"}

    # Prefer target season; fallback to most recent available season.
    current_rows.sort(key=lambda r: (int(r.get("year", 0) or 0), len(r.get("percentile_vector", {}))), reverse=True)
    selected = next((r for r in current_rows if int(r.get("year", 0) or 0) == int(target_season)), current_rows[0])
    current_vec = dict(selected.get("percentile_vector", {}))
    coverage = len([m for m in metrics if m in current_vec])
    if coverage == 0:
        return {"comps": [], "coverage": 0, "metric_count": len(metrics), "source": "production_percentile_knn"}

    candidates = pack.get("by_pos", {}).get(pos, [])

    def _score_pool(allow_same_season: bool) -> list[dict]:
        scored_local = []
        for cand in candidates:
            if canonical_player_name(cand.get("player_name", "")) == name_key:
                continue
            cand_year = int(cand.get("year", 0) or 0)
            if (not allow_same_season) and cand_year >= int(target_season):
                continue
            cand_vec = dict(cand.get("percentile_vector", {}))
            dist, overlap = _distance(current_vec, cand_vec, metrics)
            if not math.isfinite(dist) or overlap < min_overlap:
                continue
            similarity = max(1.0, min(99.9, 100.0 - (60.0 * dist)))
            scored_local.append(
                {
                    "player_name": cand.get("player_name", ""),
                    "year": cand_year,
                    "similarity": round(similarity, 2),
                    "overlap_metrics": overlap,
                    "distance": round(dist, 4),
                }
            )
        scored_local.sort(key=lambda r: (r["distance"], -r["overlap_metrics"]))
        return scored_local

    scored = _score_pool(allow_same_season=False)
    candidate_mode = "historical_only"
    if not scored:
        scored = _score_pool(allow_same_season=True)
        candidate_mode = "same_season_fallback"

    top = scored[:k]
    return {
        "comps": top,
        "coverage": coverage,
        "metric_count": len(metrics),
        "target_year": selected.get("year", ""),
        "candidate_mode": candidate_mode,
        "source": "production_percentile_knn",
    }
