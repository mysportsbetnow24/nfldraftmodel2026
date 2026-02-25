from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[2]
USAGE_DIR = ROOT / "data" / "processed" / "api_usage"


class CFBDQuotaExceeded(RuntimeError):
    """Raised when a request would exceed the monthly call cap."""


@dataclass
class CFBDUsage:
    month: str
    calls_used: int
    max_calls: int

    @property
    def calls_remaining(self) -> int:
        return max(0, self.max_calls - self.calls_used)


class CFBDQuotaTracker:
    def __init__(self, max_calls: int = 1000, usage_dir: Path = USAGE_DIR) -> None:
        self.max_calls = max_calls
        self.usage_dir = usage_dir
        self.usage_dir.mkdir(parents=True, exist_ok=True)

    def _month_key(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")

    def _usage_path(self) -> Path:
        return self.usage_dir / f"cfbd_usage_{self._month_key()}.json"

    def _load(self) -> Dict[str, Any]:
        path = self._usage_path()
        if not path.exists():
            return {
                "month": self._month_key(),
                "calls_used": 0,
                "max_calls": self.max_calls,
                "history": [],
            }
        with path.open() as f:
            data = json.load(f)
        data.setdefault("month", self._month_key())
        data.setdefault("calls_used", 0)
        data.setdefault("max_calls", self.max_calls)
        data.setdefault("history", [])
        return data

    def _save(self, payload: Dict[str, Any]) -> None:
        path = self._usage_path()
        with path.open("w") as f:
            json.dump(payload, f, indent=2)

    def status(self) -> CFBDUsage:
        payload = self._load()
        return CFBDUsage(
            month=payload["month"],
            calls_used=int(payload["calls_used"]),
            max_calls=int(payload.get("max_calls", self.max_calls)),
        )

    def reserve_call(self, endpoint: str, params: Dict[str, Any] | None = None) -> CFBDUsage:
        payload = self._load()
        used = int(payload["calls_used"])
        cap = int(payload.get("max_calls", self.max_calls))

        if used + 1 > cap:
            raise CFBDQuotaExceeded(
                f"CFBD monthly cap reached ({used}/{cap}). Refusing new API call."
            )

        payload["calls_used"] = used + 1
        payload["max_calls"] = cap
        payload["history"].append(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "endpoint": endpoint,
                "params": params or {},
            }
        )
        self._save(payload)

        return CFBDUsage(month=payload["month"], calls_used=payload["calls_used"], max_calls=cap)


class CFBDClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.collegefootballdata.com",
        timeout_seconds: int = 30,
        max_calls_per_month: int = 1000,
    ) -> None:
        self.api_key = api_key or os.getenv("CFBD_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.tracker = CFBDQuotaTracker(max_calls=max_calls_per_month)

    def usage_status(self) -> CFBDUsage:
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
            }

        if not self.api_key:
            raise RuntimeError("CFBD_API_KEY is not set. Export it in your shell before using --execute.")

        # Import requests only when executing API calls so dry-run works without dependency installs.
        import requests  # type: ignore

        status = self.tracker.reserve_call(endpoint=endpoint, params=params)

        resp = requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.timeout_seconds,
        )
        resp.raise_for_status()
        return {
            "dry_run": False,
            "url": url,
            "params": params,
            "calls_used": status.calls_used,
            "calls_remaining": status.calls_remaining,
            "max_calls": status.max_calls,
            "data": resp.json(),
        }


DATASET_ENDPOINTS = {
    "player_season_stats": "/stats/player/season",
    "team_season_stats": "/stats/season",
    "team_advanced_stats": "/stats/season/advanced",
    "advanced_game_stats": "/stats/game/advanced",
    "player_ppa": "/ppa/players/season",
    "team_ppa": "/ppa/teams",
    "games": "/games",
    "team_game_stats": "/games/teams",
    "roster": "/roster",
    "fbs_teams": "/teams/fbs",
}


SEASON_TYPE_DATASETS = {"games", "team_game_stats", "advanced_game_stats", "team_ppa"}


def fetch_dataset(
    dataset: str,
    year: int,
    team: str | None = None,
    conference: str | None = None,
    week: int | None = None,
    season_type: str = "regular",
    execute: bool = False,
    max_calls_per_month: int = 1000,
) -> Dict[str, Any]:
    if dataset not in DATASET_ENDPOINTS:
        valid = ", ".join(sorted(DATASET_ENDPOINTS))
        raise ValueError(f"Unknown dataset '{dataset}'. Valid options: {valid}")

    client = CFBDClient(max_calls_per_month=max_calls_per_month)

    params: Dict[str, Any] = {"year": year}
    if team:
        params["team"] = team
    if conference:
        params["conference"] = conference
    if week is not None:
        params["week"] = week
    if dataset in SEASON_TYPE_DATASETS:
        params["seasonType"] = season_type

    return client.get(DATASET_ENDPOINTS[dataset], params=params, execute=execute)
