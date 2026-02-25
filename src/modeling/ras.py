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
    # Smooth proxy distribution for a 0-10 RAS-like scale.
    pct = ((ras_score / 10.0) ** 1.7) * 100.0
    return round(_clamp(pct, 1.0, 99.8), 2)



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
