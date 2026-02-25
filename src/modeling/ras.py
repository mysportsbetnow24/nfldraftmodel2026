from __future__ import annotations

from typing import Dict, List, Tuple


POSITION_SIZE_TARGETS: Dict[str, Tuple[int, int]] = {
    "QB": (76, 220),
    "RB": (71, 210),
    "WR": (73, 200),
    "TE": (77, 250),
    "OT": (78, 315),
    "IOL": (75, 310),
    "EDGE": (76, 260),
    "DT": (75, 305),
    "LB": (74, 235),
    "CB": (71, 190),
    "S": (72, 205),
}


# Baselines used for combine-derived RAS approximation when official RAS is missing.
POSITION_COMBINE_BASELINES: Dict[str, Dict[str, Tuple[float, float, str]]] = {
    "QB": {
        "forty": (4.76, 0.13, "lower"),
        "ten_split": (1.63, 0.07, "lower"),
        "vertical": (33.5, 3.8, "higher"),
        "broad": (117.0, 8.0, "higher"),
        "shuttle": (4.35, 0.17, "lower"),
        "three_cone": (7.15, 0.20, "lower"),
        "bench": (13.0, 4.0, "higher"),
    },
    "RB": {
        "forty": (4.53, 0.10, "lower"),
        "ten_split": (1.55, 0.05, "lower"),
        "vertical": (35.5, 3.5, "higher"),
        "broad": (122.0, 7.0, "higher"),
        "shuttle": (4.20, 0.15, "lower"),
        "three_cone": (7.00, 0.18, "lower"),
        "bench": (18.0, 4.0, "higher"),
    },
    "WR": {
        "forty": (4.49, 0.09, "lower"),
        "ten_split": (1.53, 0.05, "lower"),
        "vertical": (36.0, 3.6, "higher"),
        "broad": (124.0, 7.5, "higher"),
        "shuttle": (4.18, 0.14, "lower"),
        "three_cone": (6.95, 0.17, "lower"),
        "bench": (14.0, 4.0, "higher"),
    },
    "TE": {
        "forty": (4.69, 0.10, "lower"),
        "ten_split": (1.61, 0.06, "lower"),
        "vertical": (34.0, 3.5, "higher"),
        "broad": (121.0, 7.0, "higher"),
        "shuttle": (4.35, 0.16, "lower"),
        "three_cone": (7.10, 0.18, "lower"),
        "bench": (20.0, 4.0, "higher"),
    },
    "OT": {
        "forty": (5.14, 0.12, "lower"),
        "ten_split": (1.78, 0.07, "lower"),
        "vertical": (29.0, 3.0, "higher"),
        "broad": (105.0, 7.0, "higher"),
        "shuttle": (4.73, 0.17, "lower"),
        "three_cone": (7.80, 0.22, "lower"),
        "bench": (24.0, 5.0, "higher"),
    },
    "IOL": {
        "forty": (5.18, 0.13, "lower"),
        "ten_split": (1.79, 0.07, "lower"),
        "vertical": (28.5, 2.8, "higher"),
        "broad": (103.0, 6.5, "higher"),
        "shuttle": (4.75, 0.18, "lower"),
        "three_cone": (7.85, 0.22, "lower"),
        "bench": (26.0, 5.0, "higher"),
    },
    "EDGE": {
        "forty": (4.70, 0.10, "lower"),
        "ten_split": (1.62, 0.06, "lower"),
        "vertical": (34.0, 3.3, "higher"),
        "broad": (118.0, 7.0, "higher"),
        "shuttle": (4.35, 0.16, "lower"),
        "three_cone": (7.20, 0.20, "lower"),
        "bench": (22.0, 5.0, "higher"),
    },
    "DT": {
        "forty": (4.95, 0.12, "lower"),
        "ten_split": (1.72, 0.07, "lower"),
        "vertical": (31.0, 3.2, "higher"),
        "broad": (111.0, 7.0, "higher"),
        "shuttle": (4.55, 0.17, "lower"),
        "three_cone": (7.55, 0.21, "lower"),
        "bench": (25.0, 5.0, "higher"),
    },
    "LB": {
        "forty": (4.64, 0.10, "lower"),
        "ten_split": (1.59, 0.06, "lower"),
        "vertical": (34.5, 3.3, "higher"),
        "broad": (120.0, 7.0, "higher"),
        "shuttle": (4.28, 0.15, "lower"),
        "three_cone": (7.12, 0.19, "lower"),
        "bench": (21.0, 4.5, "higher"),
    },
    "CB": {
        "forty": (4.47, 0.09, "lower"),
        "ten_split": (1.53, 0.05, "lower"),
        "vertical": (36.5, 3.5, "higher"),
        "broad": (124.0, 7.0, "higher"),
        "shuttle": (4.15, 0.14, "lower"),
        "three_cone": (6.90, 0.17, "lower"),
        "bench": (13.0, 4.0, "higher"),
    },
    "S": {
        "forty": (4.53, 0.09, "lower"),
        "ten_split": (1.55, 0.05, "lower"),
        "vertical": (35.5, 3.3, "higher"),
        "broad": (122.0, 7.0, "higher"),
        "shuttle": (4.20, 0.14, "lower"),
        "three_cone": (6.98, 0.18, "lower"),
        "bench": (15.0, 4.0, "higher"),
    },
}


RAS_HISTORICAL_COMPS: Dict[str, Dict[str, List[str]]] = {
    "QB": {
        "elite": ["Josh Allen", "Cam Newton"],
        "great": ["Justin Herbert", "Trevor Lawrence"],
        "good": ["Dak Prescott", "Jordan Love"],
        "average": ["Jared Goff", "Kirk Cousins"],
        "below_average": ["Brock Purdy", "Tua Tagovailoa"],
    },
    "RB": {
        "elite": ["Saquon Barkley", "Jonathan Taylor"],
        "great": ["Breece Hall", "Jahmyr Gibbs"],
        "good": ["Kyren Williams", "Josh Jacobs"],
        "average": ["David Montgomery", "Rhamondre Stevenson"],
        "below_average": ["Austin Ekeler", "James Conner"],
    },
    "WR": {
        "elite": ["Julio Jones", "DK Metcalf"],
        "great": ["A.J. Brown", "CeeDee Lamb"],
        "good": ["Amon-Ra St. Brown", "Chris Olave"],
        "average": ["Jakobi Meyers", "Courtland Sutton"],
        "below_average": ["Keenan Allen", "Cooper Kupp"],
    },
    "TE": {
        "elite": ["Kyle Pitts", "Vernon Davis"],
        "great": ["George Kittle", "Sam LaPorta"],
        "good": ["Dallas Goedert", "Pat Freiermuth"],
        "average": ["Dalton Schultz", "Tyler Conklin"],
        "below_average": ["Cole Kmet", "Trey McBride"],
    },
    "OT": {
        "elite": ["Tristan Wirfs", "Penei Sewell"],
        "great": ["Rashawn Slater", "Kolton Miller"],
        "good": ["Christian Darrisaw", "Taylor Decker"],
        "average": ["Orlando Brown Jr.", "Rob Havenstein"],
        "below_average": ["Dion Dawkins", "Jawaan Taylor"],
    },
    "IOL": {
        "elite": ["Quenton Nelson", "Creed Humphrey"],
        "great": ["Joe Thuney", "Tyler Smith"],
        "good": ["Frank Ragnow", "Landon Dickerson"],
        "average": ["Graham Glasgow", "Kevin Dotson"],
        "below_average": ["Will Hernandez", "Aaron Banks"],
    },
    "EDGE": {
        "elite": ["Myles Garrett", "Micah Parsons"],
        "great": ["Brian Burns", "Danielle Hunter"],
        "good": ["Trey Hendrickson", "Josh Sweat"],
        "average": ["George Karlaftis", "Zach Allen"],
        "below_average": ["Aidan Hutchinson", "Carl Granderson"],
    },
    "DT": {
        "elite": ["Aaron Donald", "Jalen Carter"],
        "great": ["Chris Jones", "Jeffery Simmons"],
        "good": ["Quinnen Williams", "Christian Wilkins"],
        "average": ["Dexter Lawrence", "D.J. Reader"],
        "below_average": ["Alim McNeill", "Teair Tart"],
    },
    "LB": {
        "elite": ["Micah Parsons", "Devin White"],
        "great": ["Fred Warner", "Roquan Smith"],
        "good": ["Bobby Okereke", "Logan Wilson"],
        "average": ["Cole Holcomb", "Germaine Pratt"],
        "below_average": ["E.J. Speed", "Alex Anzalone"],
    },
    "CB": {
        "elite": ["Patrick Surtain II", "Sauce Gardner"],
        "great": ["Jaire Alexander", "Jaycee Horn"],
        "good": ["Denzel Ward", "Riq Woolen"],
        "average": ["Charvarius Ward", "Carlton Davis"],
        "below_average": ["Kenny Moore II", "Taron Johnson"],
    },
    "S": {
        "elite": ["Derwin James", "Kyle Hamilton"],
        "great": ["Antoine Winfield Jr.", "Jessie Bates III"],
        "good": ["Minkah Fitzpatrick", "Talanoa Hufanga"],
        "average": ["Jabrill Peppers", "Jordan Whitehead"],
        "below_average": ["Julian Love", "Budda Baker"],
    },
}


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))



def ras_tier(ras_score: float) -> str:
    if ras_score >= 9.0:
        return "elite"
    if ras_score >= 8.0:
        return "great"
    if ras_score >= 7.0:
        return "good"
    if ras_score >= 6.0:
        return "average"
    return "below_average"



def _ras_percentile(ras_score: float) -> float:
    pct = ((ras_score / 10.0) ** 1.7) * 100.0
    return round(_clamp(pct, 1.0, 99.8), 2)


def ras_percentile(ras_score: float) -> float:
    return _ras_percentile(ras_score)



def _score_lower_better(value: float, mean: float, stdev: float) -> float:
    z = (mean - value) / max(stdev, 1e-6)
    return _clamp(5.0 + z * 1.7, 0.5, 9.95)



def _score_higher_better(value: float, mean: float, stdev: float) -> float:
    z = (value - mean) / max(stdev, 1e-6)
    return _clamp(5.0 + z * 1.7, 0.5, 9.95)



def estimate_ras(position: str, height_in: int, weight_lb: int, athletic_score: float, rank_seed: int) -> dict:
    target_h, target_w = POSITION_SIZE_TARGETS.get(position, (72, 210))

    height_delta = abs(height_in - target_h)
    weight_delta = abs(weight_lb - target_w)

    size_component = 10.0 - (height_delta / 8.0) * 3.5 - (weight_delta / 70.0) * 3.5
    size_component = _clamp(size_component, 2.0, 10.0)

    athletic_component = ((athletic_score - 60.0) / 35.0) * 10.0
    athletic_component = _clamp(athletic_component, 0.0, 10.0)

    consensus_component = ((301 - rank_seed) / 300.0) * 10.0
    consensus_component = _clamp(consensus_component, 0.0, 10.0)

    ras_score = 0.72 * athletic_component + 0.23 * size_component + 0.05 * consensus_component
    ras_score = round(_clamp(ras_score, 0.0, 10.0), 2)
    tier = ras_tier(ras_score)

    return {
        "ras_estimate": ras_score,
        "ras_tier": tier,
        "ras_percentile": _ras_percentile(ras_score),
        "ras_source": "estimated_profile_proxy",
    }



def ras_from_combine_profile(position: str, combine: dict, fallback_ras: dict) -> dict:
    official = combine.get("ras_official")
    if official is not None:
        score = round(_clamp(float(official), 0.0, 10.0), 2)
        return {
            "ras_estimate": score,
            "ras_tier": ras_tier(score),
            "ras_percentile": _ras_percentile(score),
            "ras_source": "combine_official",
        }

    baselines = POSITION_COMBINE_BASELINES.get(position, {})
    metric_weights = {
        "forty": 0.20,
        "ten_split": 0.08,
        "vertical": 0.12,
        "broad": 0.10,
        "shuttle": 0.12,
        "three_cone": 0.12,
        "bench": 0.08,
    }

    weighted = 0.0
    weight_used = 0.0
    for metric, weight in metric_weights.items():
        value = combine.get(metric)
        base = baselines.get(metric)
        if value is None or base is None:
            continue
        mean, stdev, direction = base
        if direction == "lower":
            sub = _score_lower_better(float(value), mean, stdev)
        else:
            sub = _score_higher_better(float(value), mean, stdev)
        weighted += sub * weight
        weight_used += weight

    # Size component using measured combine height/weight when present.
    target_h, target_w = POSITION_SIZE_TARGETS.get(position, (72, 210))
    h = combine.get("height_in")
    w = combine.get("weight_lb")
    if h is not None and w is not None:
        size_component = 10.0 - (abs(float(h) - target_h) / 8.0) * 3.6 - (abs(float(w) - target_w) / 70.0) * 3.4
        size_component = _clamp(size_component, 2.0, 10.0)
        weighted += size_component * 0.18
        weight_used += 0.18

    if weight_used < 0.35:
        return fallback_ras

    score = round(_clamp(weighted / weight_used, 0.0, 10.0), 2)
    return {
        "ras_estimate": score,
        "ras_tier": ras_tier(score),
        "ras_percentile": _ras_percentile(score),
        "ras_source": "combine_derived_partial",
    }



def historical_ras_comparison(position: str, tier: str) -> dict:
    comps_by_tier = RAS_HISTORICAL_COMPS.get(position, {})
    comps = comps_by_tier.get(tier, ["No comp", "No comp"])
    if len(comps) == 1:
        comps = [comps[0], comps[0]]

    return {
        "ras_historical_comp_1": comps[0],
        "ras_historical_comp_2": comps[1],
        "ras_comparison_note": f"{tier.replace('_', ' ').title()} RAS archetype for {position} profile.",
    }
