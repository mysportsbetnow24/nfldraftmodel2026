from __future__ import annotations

import bisect
import csv
import math
from collections import defaultdict
from pathlib import Path

from src.ingest.rankings_loader import normalize_pos


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = (
    ROOT
    / "data"
    / "sources"
    / "external"
    / "nflverse"
    / "nflverse_combine_standardized.csv"
)

FALLBACK_PATH = (
    ROOT
    / "data"
    / "sources"
    / "external"
    / "nfl-combine-evaluation"
    / "data"
    / "NFLCombineStats1999-2015.csv"
)

# Historical source metric names -> model metric names.
SOURCE_METRIC_MAP = {
    "heightinchestotal": "height_in",
    "weight": "weight_lb",
    "arms": "arm_in",
    "hands": "hand_in",
    "fortyyd": "forty",
    "tenyd": "ten_split",
    "vertical": "vertical",
    "broad": "broad",
    "threecone": "three_cone",
    "twentyss": "shuttle",
    "bench": "bench",
}

METRICS = list(SOURCE_METRIC_MAP.values())
LOWER_BETTER = {"forty", "ten_split", "three_cone", "shuttle"}

POSITION_MAP = {
    "QB": "QB",
    "RB": "RB",
    "FB": "RB",
    "WR": "WR",
    "TE": "TE",
    "OT": "OT",
    "T": "OT",
    "G": "IOL",
    "OG": "IOL",
    "C": "IOL",
    "DE": "EDGE",
    "OLB": "EDGE",
    "EDGE": "EDGE",
    "EDG": "EDGE",
    "DT": "DT",
    "NT": "DT",
    "ILB": "LB",
    "LB": "LB",
    "MLB": "LB",
    "CB": "CB",
    "FS": "S",
    "SS": "S",
    "S": "S",
}

EVENT_GROUPS = {
    "speed": ("forty", "ten_split"),
    "explosion": ("vertical", "broad", "bench"),
    "agility": ("shuttle", "three_cone"),
    "size": ("height_in", "weight_lb", "arm_in", "hand_in"),
}

POSITION_EVENT_WEIGHTS = {
    "QB": {"speed": 0.15, "explosion": 0.15, "agility": 0.25, "size": 0.45},
    "RB": {"speed": 0.35, "explosion": 0.25, "agility": 0.20, "size": 0.20},
    "WR": {"speed": 0.30, "explosion": 0.25, "agility": 0.25, "size": 0.20},
    "TE": {"speed": 0.20, "explosion": 0.20, "agility": 0.20, "size": 0.40},
    "OT": {"speed": 0.10, "explosion": 0.15, "agility": 0.15, "size": 0.60},
    "IOL": {"speed": 0.10, "explosion": 0.20, "agility": 0.15, "size": 0.55},
    "EDGE": {"speed": 0.20, "explosion": 0.25, "agility": 0.20, "size": 0.35},
    "DT": {"speed": 0.10, "explosion": 0.25, "agility": 0.10, "size": 0.55},
    "LB": {"speed": 0.20, "explosion": 0.20, "agility": 0.25, "size": 0.35},
    "CB": {"speed": 0.30, "explosion": 0.20, "agility": 0.30, "size": 0.20},
    "S": {"speed": 0.25, "explosion": 0.20, "agility": 0.30, "size": 0.25},
}


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _map_position(raw_pos: str) -> str:
    pos = str(raw_pos or "").strip().upper()
    mapped = POSITION_MAP.get(pos, "")
    if mapped:
        return mapped
    return normalize_pos(pos)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _metric_stats(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for metric in METRICS:
        vals = sorted(float(r[metric]) for r in rows if r.get(metric) is not None)
        if len(vals) < 20:
            continue
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = math.sqrt(max(var, 1e-9))
        out[metric] = {"values": vals, "mean": mean, "std": std}
    return out


def _percentile(values: list[float], value: float, lower_better: bool) -> float:
    if not values:
        return 50.0
    n = float(len(values))
    lo = bisect.bisect_left(values, value)
    hi = bisect.bisect_right(values, value)
    mid_rank = (lo + hi) / 2.0
    frac = mid_rank / n
    if lower_better:
        pct = (1.0 - frac) * 100.0
    else:
        pct = frac * 100.0
    return _clamp(pct, 0.0, 100.0)


def _metric_score(
    metric: str,
    value: float | None,
    stats_all: dict[str, dict],
    stats_recent: dict[str, dict],
) -> tuple[float | None, float | None, float | None]:
    if value is None:
        return None, None, None
    lower_better = metric in LOWER_BETTER

    all_stats = stats_all.get(metric)
    rec_stats = stats_recent.get(metric)
    if not all_stats and not rec_stats:
        return None, None, None

    pct_all = (
        _percentile(all_stats["values"], value, lower_better)
        if all_stats is not None
        else 50.0
    )
    pct_recent = (
        _percentile(rec_stats["values"], value, lower_better)
        if rec_stats is not None
        else pct_all
    )
    pct_blend = (0.4 * pct_all) + (0.6 * pct_recent)

    z_base = rec_stats if rec_stats is not None else all_stats
    z = (value - z_base["mean"]) / max(z_base["std"], 1e-6)
    if lower_better:
        z *= -1.0
    z_scaled = _clamp(50.0 + (10.0 * z), 0.0, 100.0)

    score = _clamp((0.70 * pct_blend) + (0.30 * z_scaled), 0.0, 100.0)
    return round(score, 3), round(pct_blend, 3), round(z, 3)


def _compute_core(
    *,
    position: str,
    metrics: dict,
    stats_all: dict[str, dict],
    stats_recent: dict[str, dict],
) -> dict:
    pos = normalize_pos(position)
    group_weights = POSITION_EVENT_WEIGHTS.get(
        pos, {"speed": 0.25, "explosion": 0.25, "agility": 0.25, "size": 0.25}
    )
    metric_scores: dict[str, float] = {}
    metric_percentiles: dict[str, float] = {}
    metric_z: dict[str, float] = {}

    for metric in METRICS:
        score, pct, z = _metric_score(
            metric=metric,
            value=_to_float(metrics.get(metric)),
            stats_all=stats_all,
            stats_recent=stats_recent,
        )
        if score is None:
            continue
        metric_scores[metric] = score
        metric_percentiles[metric] = pct
        metric_z[metric] = z

    event_scores: dict[str, float] = {}
    available_weight = 0.0
    weighted_sum = 0.0
    for event, event_metrics in EVENT_GROUPS.items():
        vals = [metric_scores[m] for m in event_metrics if m in metric_scores]
        if not vals:
            continue
        event_score = sum(vals) / len(vals)
        event_scores[event] = round(event_score, 3)
        w = float(group_weights.get(event, 0.0))
        if w <= 0:
            continue
        weighted_sum += w * event_score
        available_weight += w

    base_score = (weighted_sum / available_weight) if available_weight > 0 else 70.0

    expected_metrics = {
        m
        for event, metrics_in_group in EVENT_GROUPS.items()
        if group_weights.get(event, 0.0) > 0
        for m in metrics_in_group
    }
    available_count = sum(1 for m in expected_metrics if m in metric_scores)
    expected_count = len(expected_metrics)
    missing_count = max(0, expected_count - available_count)
    coverage_rate = (available_count / expected_count) if expected_count > 0 else 0.0

    # Neutral defaults + explicit variance penalty for sparse testing profiles.
    # Sparse combine profiles should regress toward neutral, not crater.
    if available_count <= 2:
        blended_base = (0.25 * base_score) + (0.75 * 70.0)
    else:
        blended_base = (coverage_rate * base_score) + ((1.0 - coverage_rate) * 70.0)

    missing_penalty = 0.0
    if coverage_rate < 0.45:
        missing_penalty = min(1.2, (0.45 - coverage_rate) * 2.0)
    variance_penalty = min(0.8, missing_count * 0.08)

    score = _clamp(blended_base - missing_penalty, 55.0, 95.0)

    return {
        "athletic_profile_score": round(score, 3),
        "athletic_speed_score": round(event_scores.get("speed", 70.0), 3),
        "athletic_explosion_score": round(event_scores.get("explosion", 70.0), 3),
        "athletic_agility_score": round(event_scores.get("agility", 70.0), 3),
        "athletic_size_adj_score": round(event_scores.get("size", 70.0), 3),
        "athletic_metric_coverage_count": available_count,
        "athletic_metric_expected_count": expected_count,
        "athletic_metric_missing_count": missing_count,
        "athletic_metric_coverage_rate": round(coverage_rate, 4),
        "athletic_missing_penalty": round(missing_penalty, 3),
        "athletic_variance_penalty": round(variance_penalty, 3),
        "athletic_metric_percentiles": metric_percentiles,
        "athletic_metric_zscores": metric_z,
    }


def _build_hit_bins(rows: list[dict], *, stats_all: dict[str, dict], stats_recent: dict[str, dict], position: str) -> dict[int, dict]:
    bins: dict[int, dict] = defaultdict(lambda: {"sample_n": 0, "round12_hits": 0, "top100_hits": 0})
    for row in rows:
        picktotal = _to_float(row.get("picktotal"))
        if picktotal is None or picktotal <= 0:
            continue
        core = _compute_core(
            position=position,
            metrics=row,
            stats_all=stats_all,
            stats_recent=stats_recent,
        )
        score = float(core["athletic_profile_score"])
        bucket = int(_clamp(math.floor(score / 10.0), 0, 9))
        bins[bucket]["sample_n"] += 1
        if picktotal <= 64:
            bins[bucket]["round12_hits"] += 1
        if picktotal <= 100:
            bins[bucket]["top100_hits"] += 1

    out: dict[int, dict] = {}
    for b, agg in bins.items():
        n = max(1, int(agg["sample_n"]))
        out[b] = {
            "sample_n": int(agg["sample_n"]),
            "round12_hit_rate": round(float(agg["round12_hits"]) / n, 4),
            "top100_hit_rate": round(float(agg["top100_hits"]) / n, 4),
        }
    return out


def _distance(
    *,
    current_metrics: dict,
    candidate: dict,
    stats: dict[str, dict],
) -> tuple[float, int]:
    parts = []
    overlap = 0
    for metric in METRICS:
        if metric not in stats:
            continue
        cv = _to_float(current_metrics.get(metric))
        rv = _to_float(candidate.get(metric))
        if cv is None or rv is None:
            continue
        z = (cv - rv) / max(stats[metric]["std"], 1e-6)
        parts.append(z * z)
        overlap += 1
    if overlap == 0:
        return float("inf"), 0
    return math.sqrt(sum(parts) / len(parts)), overlap


def load_historical_athletic_context(path: Path | None = None) -> dict:
    if path is None:
        path = DEFAULT_PATH if DEFAULT_PATH.exists() else FALLBACK_PATH
    if not path.exists():
        return {
            "rows": [],
            "by_pos": {},
            "stats_all_by_pos": {},
            "stats_recent_by_pos": {},
            "hit_bins_by_pos": {},
            "hit_bins_global": {},
            "meta": {"status": "missing", "path": str(path), "rows": 0, "positions": 0},
        }

    rows: list[dict] = []
    with path.open() as f:
        for row in csv.DictReader(f):
            pos = _map_position(row.get("position", ""))
            if not pos:
                continue
            payload = {
                "year": int(_to_float(row.get("year")) or 0),
                "player_name": str(row.get("name", "")).strip(),
                "position": pos,
                "pickround": _to_float(row.get("pickround")),
                "picktotal": _to_float(row.get("picktotal")),
            }
            for src, dst in SOURCE_METRIC_MAP.items():
                payload[dst] = _to_float(row.get(src))
            rows.append(payload)

    by_pos: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_pos[r["position"]].append(r)

    stats_all_by_pos: dict[str, dict] = {}
    stats_recent_by_pos: dict[str, dict] = {}
    hit_bins_by_pos: dict[str, dict] = {}
    for pos, pos_rows in by_pos.items():
        stats_all = _metric_stats(pos_rows)
        recent_rows = [r for r in pos_rows if int(r.get("year", 0) or 0) >= 2010]
        if len(recent_rows) < 80:
            recent_rows = pos_rows
        stats_recent = _metric_stats(recent_rows)
        stats_all_by_pos[pos] = stats_all
        stats_recent_by_pos[pos] = stats_recent
        hit_bins_by_pos[pos] = _build_hit_bins(
            pos_rows,
            stats_all=stats_all,
            stats_recent=stats_recent,
            position=pos,
        )

    global_stats_all = _metric_stats(rows)
    global_recent_rows = [r for r in rows if int(r.get("year", 0) or 0) >= 2010]
    if len(global_recent_rows) < 200:
        global_recent_rows = rows
    global_stats_recent = _metric_stats(global_recent_rows)
    hit_bins_global = _build_hit_bins(
        rows,
        stats_all=global_stats_all,
        stats_recent=global_stats_recent,
        position="QB",
    )

    return {
        "rows": rows,
        "by_pos": dict(by_pos),
        "stats_all_by_pos": stats_all_by_pos,
        "stats_recent_by_pos": stats_recent_by_pos,
        "stats_global_all": global_stats_all,
        "stats_global_recent": global_stats_recent,
        "hit_bins_by_pos": hit_bins_by_pos,
        "hit_bins_global": hit_bins_global,
        "meta": {
            "status": "ok",
            "path": str(path),
            "rows": len(rows),
            "positions": len(by_pos),
        },
    }


def evaluate_athletic_profile(
    *,
    position: str,
    current_metrics: dict,
    pack: dict,
    nearest_k: int = 3,
) -> dict:
    pos = normalize_pos(position)
    by_pos = pack.get("by_pos", {})
    pos_rows = by_pos.get(pos, [])

    stats_all = pack.get("stats_all_by_pos", {}).get(pos, {})
    stats_recent = pack.get("stats_recent_by_pos", {}).get(pos, {})
    if not stats_all:
        stats_all = pack.get("stats_global_all", {})
    if not stats_recent:
        stats_recent = pack.get("stats_global_recent", stats_all)

    if not stats_all:
        return {
            "athletic_profile_score": 70.0,
            "athletic_speed_score": 70.0,
            "athletic_explosion_score": 70.0,
            "athletic_agility_score": 70.0,
            "athletic_size_adj_score": 70.0,
            "athletic_metric_coverage_count": 0,
            "athletic_metric_expected_count": 0,
            "athletic_metric_missing_count": 0,
            "athletic_metric_coverage_rate": 0.0,
            "athletic_missing_penalty": 0.0,
            "athletic_variance_penalty": 0.0,
            "athletic_hit_bin": "",
            "athletic_hit_bin_sample_n": "",
            "athletic_hit_rate_round12_bin": "",
            "athletic_hit_rate_top100_bin": "",
            "athletic_comp_confidence": "",
            "athletic_nn_comp_1": "",
            "athletic_nn_comp_1_year": "",
            "athletic_nn_comp_1_picktotal": "",
            "athletic_nn_comp_1_similarity": "",
            "athletic_nn_comp_2": "",
            "athletic_nn_comp_2_year": "",
            "athletic_nn_comp_2_picktotal": "",
            "athletic_nn_comp_2_similarity": "",
            "athletic_nn_comp_3": "",
            "athletic_nn_comp_3_year": "",
            "athletic_nn_comp_3_picktotal": "",
            "athletic_nn_comp_3_similarity": "",
        }

    core = _compute_core(
        position=pos,
        metrics=current_metrics,
        stats_all=stats_all,
        stats_recent=stats_recent,
    )
    score = float(core["athletic_profile_score"])
    bucket = int(_clamp(math.floor(score / 10.0), 0, 9))
    hit_bins = pack.get("hit_bins_by_pos", {}).get(pos) or pack.get("hit_bins_global", {})
    hit = hit_bins.get(bucket, {})

    nn = []
    for row in pos_rows:
        dist, overlap = _distance(current_metrics=current_metrics, candidate=row, stats=stats_all)
        if overlap < 3 or not math.isfinite(dist):
            continue
        similarity = _clamp(100.0 - (16.5 * dist), 1.0, 99.9)
        nn.append(
            {
                "player_name": row.get("player_name", ""),
                "year": row.get("year", ""),
                "picktotal": row.get("picktotal", ""),
                "similarity": round(similarity, 2),
                "overlap": overlap,
                "distance": dist,
            }
        )
    nn.sort(key=lambda r: (r["distance"], -r["overlap"]))
    nn = nn[:nearest_k]

    if nn:
        avg_sim = sum(float(r["similarity"]) for r in nn[:2]) / min(2, len(nn))
        coverage_rate = float(core.get("athletic_metric_coverage_rate", 0.0))
        comp_conf = _clamp((0.70 * avg_sim) + (0.30 * (coverage_rate * 100.0)), 1.0, 99.0)
    else:
        comp_conf = 0.0

    metric_percentiles = dict(core.pop("athletic_metric_percentiles", {}))
    metric_zscores = dict(core.pop("athletic_metric_zscores", {}))

    out = {
        **core,
        "athletic_hit_bin": bucket,
        "athletic_hit_bin_sample_n": hit.get("sample_n", ""),
        "athletic_hit_rate_round12_bin": hit.get("round12_hit_rate", ""),
        "athletic_hit_rate_top100_bin": hit.get("top100_hit_rate", ""),
        "athletic_comp_confidence": round(comp_conf, 2) if comp_conf > 0 else "",
    }

    # Flatten nearest-neighbor comps.
    for idx in (1, 2, 3):
        item = nn[idx - 1] if len(nn) >= idx else {}
        out[f"athletic_nn_comp_{idx}"] = item.get("player_name", "")
        out[f"athletic_nn_comp_{idx}_year"] = item.get("year", "")
        out[f"athletic_nn_comp_{idx}_picktotal"] = item.get("picktotal", "")
        out[f"athletic_nn_comp_{idx}_similarity"] = item.get("similarity", "")

    # Export key metric percentiles/zscores as explicit columns for auditability.
    for metric in METRICS:
        out[f"athletic_pct_{metric}"] = metric_percentiles.get(metric, "")
        out[f"athletic_z_{metric}"] = metric_zscores.get(metric, "")

    return out
