from __future__ import annotations

import math
from typing import Dict, Mapping


LOWER_BETTER = {"ten_split", "twenty_split", "forty", "shuttle", "three_cone"}

COMPONENTS = {
    "speed": ["ten_split", "forty"],
    "explosion": ["vertical", "broad", "bench"],
    "agility": ["shuttle", "three_cone"],
    "size": ["height", "weight", "arm", "hand"],
}

COMPONENT_WEIGHTS = {
    "speed": 0.35,
    "explosion": 0.30,
    "agility": 0.20,
    "size": 0.15,
}


def _cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _safe_float(v) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def compute_metric_percentiles(position: str, measurables: Mapping[str, float], baselines: Mapping[str, dict]) -> dict:
    pos = (position or "").upper()
    base = baselines.get(pos)
    if not base:
        return {}

    out = {}
    metrics = base.get("metrics", {})
    for metric_key, stats in metrics.items():
        val = _safe_float(measurables.get(metric_key))
        if val is None:
            continue

        mean = _safe_float(stats.get("mean"))
        std = _safe_float(stats.get("std"))
        if mean is None or std is None or std <= 0:
            continue

        z = (val - mean) / std
        if metric_key in LOWER_BETTER:
            z = -z

        pct = _cdf(z) * 100.0
        out[f"md_{metric_key}_z"] = round(z, 3)
        out[f"md_{metric_key}_pct"] = round(max(0.0, min(100.0, pct)), 2)

    return out


def _component_score(metric_percentiles: Mapping[str, float], metric_list: list[str]) -> float | None:
    vals = []
    for metric in metric_list:
        key = f"md_{metric}_pct"
        if key in metric_percentiles:
            vals.append(float(metric_percentiles[key]))
    if not vals:
        return None
    return sum(vals) / len(vals)


def compute_mockdraftable_composite(position: str, measurables: Mapping[str, float], baselines: Mapping[str, dict]) -> dict:
    metric_percentiles = compute_metric_percentiles(position, measurables, baselines)
    if not metric_percentiles:
        return {
            "md_composite": "",
            "md_speed_pct": "",
            "md_explosion_pct": "",
            "md_agility_pct": "",
            "md_size_pct": "",
            **metric_percentiles,
        }

    comp_scores = {}
    for comp, metric_list in COMPONENTS.items():
        score = _component_score(metric_percentiles, metric_list)
        comp_scores[comp] = round(score, 2) if score is not None else ""

    weighted_sum = 0.0
    weight_sum = 0.0
    for comp, weight in COMPONENT_WEIGHTS.items():
        val = comp_scores.get(comp)
        if val == "":
            continue
        weighted_sum += float(val) * weight
        weight_sum += weight

    composite = round(weighted_sum / weight_sum, 2) if weight_sum > 0 else ""

    return {
        "md_composite": composite,
        "md_speed_pct": comp_scores.get("speed", ""),
        "md_explosion_pct": comp_scores.get("explosion", ""),
        "md_agility_pct": comp_scores.get("agility", ""),
        "md_size_pct": comp_scores.get("size", ""),
        **metric_percentiles,
    }
