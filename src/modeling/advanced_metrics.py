from __future__ import annotations

from typing import Iterable, Mapping


SUCCESS_THRESHOLDS = {
    1: 0.50,
    2: 0.70,
    3: 1.00,
    4: 1.00,
}


def play_is_success(down: int, yards_gained: float, distance: float) -> bool:
    """Open-source convention: 50% on 1st, 70% on 2nd, 100% on 3rd/4th."""
    if distance <= 0:
        return True
    threshold = SUCCESS_THRESHOLDS.get(down, 1.0)
    return (yards_gained / distance) >= threshold



def success_rate(plays: Iterable[Mapping[str, float]]) -> float:
    rows = list(plays)
    if not rows:
        return 0.0
    successes = 0
    for play in rows:
        if play_is_success(int(play["down"]), float(play["yards_gained"]), float(play["distance"])):
            successes += 1
    return successes / len(rows)



def epa_per_play(plays: Iterable[Mapping[str, float]], key: str = "epa") -> float:
    rows = list(plays)
    if not rows:
        return 0.0
    return sum(float(p.get(key, 0.0)) for p in rows) / len(rows)



def ppa_per_play(plays: Iterable[Mapping[str, float]], key: str = "ppa") -> float:
    rows = list(plays)
    if not rows:
        return 0.0
    return sum(float(p.get(key, 0.0)) for p in rows) / len(rows)



def isoppp(plays: Iterable[Mapping[str, float]], ppa_key: str = "ppa") -> float:
    """IsoPPP: average PPP/ppa on successful plays only."""
    successful = []
    for p in plays:
        if play_is_success(int(p["down"]), float(p["yards_gained"]), float(p["distance"])):
            successful.append(float(p.get(ppa_key, 0.0)))
    if not successful:
        return 0.0
    return sum(successful) / len(successful)



def isoppp_plus(raw_isoppp: float, opponent_adjustment: float, fbs_baseline: float) -> float:
    """Index to 100 baseline after opponent adjustment."""
    if fbs_baseline <= 0:
        return 100.0
    adjusted = raw_isoppp * opponent_adjustment
    return (adjusted / fbs_baseline) * 100.0



def integrate_advanced_signal(
    epa_pp: float,
    success_rate_value: float,
    isoppp_plus_value: float,
    pff_grade: float | None = None,
) -> float:
    """Normalized advanced signal for board upgrades. Output ~0-100."""
    pff_component = ((pff_grade or 70.0) - 50.0) * 1.2
    epa_component = max(0.0, min(40.0, (epa_pp + 0.30) * 65.0))
    sr_component = max(0.0, min(30.0, success_rate_value * 50.0))
    iso_component = max(0.0, min(30.0, (isoppp_plus_value / 100.0) * 20.0))
    total = pff_component + epa_component + sr_component + iso_component
    return max(0.0, min(100.0, total))
