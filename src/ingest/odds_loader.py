from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


@dataclass
class OddsPullPlan:
    market: str
    description: str


DEFAULT_MARKETS: List[OddsPullPlan] = [
    OddsPullPlan("first_overall_pick", "Market-implied top of board probabilities"),
    OddsPullPlan("first_position_drafted", "Positional scarcity pressure"),
    OddsPullPlan("team_to_draft_player", "Fit + market confidence overlay"),
]


def require_api_key() -> str:
    key = os.getenv("ODDS_API_KEY", "")
    if not key:
        raise RuntimeError("ODDS_API_KEY is not set")
    return key


def planned_markets() -> List[dict]:
    return [m.__dict__ for m in DEFAULT_MARKETS]
