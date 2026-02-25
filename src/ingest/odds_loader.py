from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[2]
USAGE_PATH = ROOT / "data" / "processed" / "api_usage" / "odds_usage_campaign.json"


# Default budget plan still models the originally approved 3-market cadence.
DEFAULT_MARKETS = [
    "first_overall_pick",
    "first_position_drafted",
    "team_to_draft_player",
]
SUPPORTED_MARKETS = DEFAULT_MARKETS + ["nfl_draft_first_round"]

MARKET_PARAM_MAP = {
    "first_overall_pick": os.getenv("ODDS_MARKET_FIRST_OVERALL", "first_overall_pick"),
    "first_position_drafted": os.getenv("ODDS_MARKET_FIRST_POSITION", "first_position_drafted"),
    "team_to_draft_player": os.getenv("ODDS_MARKET_TEAM_TO_DRAFT_PLAYER", "team_to_draft_player"),
    # Override this env var once confirmed against your specific sportsbook/event market key.
    "nfl_draft_first_round": os.getenv("ODDS_MARKET_FIRST_ROUND", "draft_round_1"),
}

DEFAULT_BOOKMAKERS = ["fanduel", "draftkings", "betmgm", "caesars"]
DEFAULT_REGIONS = ["us"]
DEFAULT_SNAPSHOTS_PER_DAY = 2
DEFAULT_PLAN_DAYS = 120
DEFAULT_MAX_CALLS = 2880
DEFAULT_PLAN_START = "2026-02-25"


@dataclass
class OddsUsage:
    start_date: str
    end_date: str
    calls_used: int
    max_calls: int

    @property
    def calls_remaining(self) -> int:
        return max(0, self.max_calls - self.calls_used)


@dataclass
class OddsPullPlan:
    market: str
    description: str


ODDS_MARKETS: List[OddsPullPlan] = [
    OddsPullPlan("first_overall_pick", "Market-implied top of board probabilities"),
    OddsPullPlan("first_position_drafted", "Positional scarcity pressure"),
    OddsPullPlan("team_to_draft_player", "Fit + market confidence overlay"),
    OddsPullPlan("nfl_draft_first_round", "First-round specific draft market"),
]


class OddsQuotaExceeded(RuntimeError):
    """Raised when a request would exceed the campaign call cap."""



def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()



def _campaign_bounds(start_date: str, days: int) -> tuple[date, date]:
    start = _parse_date(start_date)
    end = start + timedelta(days=days - 1)
    return start, end



def estimate_total_calls(
    markets: List[str] | None = None,
    bookmakers: List[str] | None = None,
    regions: List[str] | None = None,
    snapshots_per_day: int = DEFAULT_SNAPSHOTS_PER_DAY,
    days: int = DEFAULT_PLAN_DAYS,
) -> int:
    markets = markets or DEFAULT_MARKETS
    bookmakers = bookmakers or DEFAULT_BOOKMAKERS
    regions = regions or DEFAULT_REGIONS
    return len(markets) * len(bookmakers) * len(regions) * snapshots_per_day * days


class OddsQuotaTracker:
    def __init__(
        self,
        usage_path: Path = USAGE_PATH,
        max_calls: int = DEFAULT_MAX_CALLS,
        plan_days: int = DEFAULT_PLAN_DAYS,
        plan_start: str = DEFAULT_PLAN_START,
    ) -> None:
        self.usage_path = usage_path
        self.max_calls = max_calls
        self.plan_days = plan_days
        self.plan_start = plan_start
        self.usage_path.parent.mkdir(parents=True, exist_ok=True)

    def _default_payload(self) -> Dict[str, Any]:
        start, end = _campaign_bounds(self.plan_start, self.plan_days)
        return {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "calls_used": 0,
            "max_calls": self.max_calls,
            "history": [],
        }

    def _load(self) -> Dict[str, Any]:
        if not self.usage_path.exists():
            return self._default_payload()

        with self.usage_path.open() as f:
            data = json.load(f)

        data.setdefault("start_date", self.plan_start)
        if "end_date" not in data:
            start, end = _campaign_bounds(data["start_date"], self.plan_days)
            data["start_date"] = start.isoformat()
            data["end_date"] = end.isoformat()
        data.setdefault("calls_used", 0)
        data.setdefault("max_calls", self.max_calls)
        data.setdefault("history", [])
        return data

    def _save(self, payload: Dict[str, Any]) -> None:
        with self.usage_path.open("w") as f:
            json.dump(payload, f, indent=2)

    def status(self) -> OddsUsage:
        payload = self._load()
        return OddsUsage(
            start_date=payload["start_date"],
            end_date=payload["end_date"],
            calls_used=int(payload["calls_used"]),
            max_calls=int(payload.get("max_calls", self.max_calls)),
        )

    def reserve_call(self, endpoint: str, params: Dict[str, Any] | None = None) -> OddsUsage:
        payload = self._load()
        used = int(payload["calls_used"])
        cap = int(payload.get("max_calls", self.max_calls))

        today = datetime.now(timezone.utc).date()
        end = _parse_date(payload["end_date"])
        if today > end:
            raise OddsQuotaExceeded(
                f"Odds campaign ended on {payload['end_date']}. Create a new plan window before executing pulls."
            )

        if used + 1 > cap:
            raise OddsQuotaExceeded(
                f"Odds campaign cap reached ({used}/{cap}). Refusing new API call."
            )

        payload["calls_used"] = used + 1
        payload["history"].append(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "endpoint": endpoint,
                "params": params or {},
            }
        )
        self._save(payload)

        return OddsUsage(
            start_date=payload["start_date"],
            end_date=payload["end_date"],
            calls_used=int(payload["calls_used"]),
            max_calls=cap,
        )


class OddsClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.the-odds-api.com/v4",
        timeout_seconds: int = 30,
        max_calls: int = DEFAULT_MAX_CALLS,
        plan_days: int = DEFAULT_PLAN_DAYS,
        plan_start: str = DEFAULT_PLAN_START,
    ) -> None:
        self.api_key = api_key or os.getenv("ODDS_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.tracker = OddsQuotaTracker(
            max_calls=max_calls,
            plan_days=plan_days,
            plan_start=plan_start,
        )

    def usage_status(self) -> OddsUsage:
        return self.tracker.status()

    def get(self, endpoint: str, params: Dict[str, Any] | None = None, execute: bool = False) -> Dict[str, Any]:
        endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        url = f"{self.base_url}{endpoint}"
        params = params or {}

        if not execute:
            status = self.tracker.status()
            return {
                "dry_run": True,
                "url": url,
                "params": params,
                "calls_used": status.calls_used,
                "calls_remaining": status.calls_remaining,
                "max_calls": status.max_calls,
                "start_date": status.start_date,
                "end_date": status.end_date,
            }

        if not self.api_key:
            raise RuntimeError("ODDS_API_KEY is not set. Export it in your shell before using --execute.")

        import requests  # type: ignore

        status = self.tracker.reserve_call(endpoint=endpoint, params=params)
        live_params = {**params, "apiKey": self.api_key}

        resp = requests.get(url, params=live_params, timeout=self.timeout_seconds)
        resp.raise_for_status()
        return {
            "dry_run": False,
            "url": url,
            "params": params,
            "calls_used": status.calls_used,
            "calls_remaining": status.calls_remaining,
            "max_calls": status.max_calls,
            "start_date": status.start_date,
            "end_date": status.end_date,
            "data": resp.json(),
        }


def require_api_key() -> str:
    key = os.getenv("ODDS_API_KEY", "")
    if not key:
        raise RuntimeError("ODDS_API_KEY is not set")
    return key


def planned_markets() -> List[dict]:
    return [m.__dict__ for m in ODDS_MARKETS]


def fetch_draft_odds_snapshot(
    market: str,
    sport: str = "americanfootball_nfl",
    regions: str = "us",
    bookmakers: str = "fanduel,draftkings,betmgm,caesars",
    execute: bool = False,
    max_calls: int = DEFAULT_MAX_CALLS,
    plan_days: int = DEFAULT_PLAN_DAYS,
    plan_start: str = DEFAULT_PLAN_START,
) -> Dict[str, Any]:
    if market not in SUPPORTED_MARKETS:
        valid = ", ".join(SUPPORTED_MARKETS)
        raise ValueError(f"Unknown market '{market}'. Valid options: {valid}")

    market_param = MARKET_PARAM_MAP.get(market, market)

    client = OddsClient(max_calls=max_calls, plan_days=plan_days, plan_start=plan_start)
    endpoint = f"/sports/{sport}/odds"
    params = {
        "regions": regions,
        "markets": market_param,
        "bookmakers": bookmakers,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    return client.get(endpoint, params=params, execute=execute)
