from __future__ import annotations

from typing import Dict, Tuple


ARCHETYPE_COMPS: Dict[str, list[Tuple[str, str]]] = {
    "QB": [("Justin Herbert-style", "arm-talent vertical distributor"), ("Brock Purdy-style", "timing/processing distributor")],
    "RB": [("Josh Jacobs-style", "contact and volume runner"), ("Alvin Kamara-style", "space and receiving mismatch")],
    "WR": [("CeeDee Lamb-style", "all-level separator"), ("Mike Evans-style", "size/ball-skill winner")],
    "TE": [("Sam LaPorta-style", "move mismatch YAC profile"), ("George Kittle-style", "in-line force plus run after catch")],
    "OT": [("Tristan Wirfs-style", "athletic pass-protecting tackle"), ("Kolton Miller-style", "length/footwork pass blocker")],
    "IOL": [("Creed Humphrey-style", "anchor + processing center"), ("Joe Thuney-style", "versatile pass-game stabilizer")],
    "EDGE": [("Micah Parsons-style", "explosive alignment-flex rusher"), ("Trey Hendrickson-style", "counter-heavy pressure producer")],
    "DT": [("Chris Jones-style", "interior rush disruptor"), ("Vita Vea-style", "power anchor with pocket push")],
    "LB": [("Fred Warner-style", "range and processing MIKE"), ("Matt Milano-style", "quick-trigger pursuit LB")],
    "CB": [("Patrick Surtain II-style", "press-man technician"), ("Sauce Gardner-style", "length and disruption outside")],
    "S": [("Kyle Hamilton-style", "multiplicity chess-piece safety"), ("Antoine Winfield Jr.-style", "instinctive conflict resolver")],
}


def assign_comp(position: str, rank_seed: int) -> dict:
    comps = ARCHETYPE_COMPS.get(position, [("Generic Pro", "balanced profile")])
    idx = 0 if rank_seed <= 120 else 1 if len(comps) > 1 else 0
    comp_name, comp_style = comps[idx]
    confidence = "A" if rank_seed <= 64 else "B" if rank_seed <= 180 else "C"
    return {
        "historical_comp": comp_name,
        "comp_style": comp_style,
        "comp_confidence": confidence,
    }
