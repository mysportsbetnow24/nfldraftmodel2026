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
        insecure_ssl = str(os.getenv("CFBD_INSECURE_SSL", "")).strip().lower() in {"1", "true", "yes"}
        status_before = self.tracker.status()
        if status_before.calls_used >= status_before.max_calls:
            raise CFBDQuotaExceeded(
                f"CFBD monthly cap reached ({status_before.calls_used}/{status_before.max_calls}). Refusing new API call."
            )
        try:
            # Prefer requests if available.
            import requests  # type: ignore

            resp = requests.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout_seconds,
                verify=not insecure_ssl,
            )
            resp.raise_for_status()
            try:
                data = resp.json()
            except ValueError as exc:
                snippet = (resp.text or "")[:400]
                raise RuntimeError(
                    f"CFBD returned non-JSON response for {endpoint} "
                    f"(status={resp.status_code}, content_type={resp.headers.get('Content-Type', '')}). "
                    f"Body snippet: {snippet}"
                ) from exc
        except ModuleNotFoundError:
            # Fallback to stdlib so API pulls work without extra installs.
            import json
            import ssl
            from urllib.parse import urlencode
            from urllib.request import Request, urlopen

            query = urlencode(params or {})
            request_url = f"{url}?{query}" if query else url
            req = Request(
                request_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                method="GET",
            )
            ctx = ssl._create_unverified_context() if insecure_ssl else None
            with urlopen(req, timeout=self.timeout_seconds, context=ctx) as resp:  # nosec B310
                payload = resp.read().decode("utf-8")
            data = json.loads(payload)
        # Only record usage after a successful response so local quota tracking
        # does not overcount transient network/cert errors.
        status = self.tracker.reserve_call(endpoint=endpoint, params=params)
        return {
            "dry_run": False,
            "url": url,
            "params": params,
            "calls_used": status.calls_used,
            "calls_remaining": status.calls_remaining,
            "max_calls": status.max_calls,
            "data": data,
        }

    def graphql(self, query: str, execute: bool = False) -> Dict[str, Any]:
        url = os.getenv("CFBD_GRAPHQL_URL", "https://graphql.collegefootballdata.com/v1/graphql").strip()
        if not execute:
            status = self.tracker.status()
            return {
                "dry_run": True,
                "url": url,
                "query": query,
                "calls_used": status.calls_used,
                "calls_remaining": status.calls_remaining,
                "max_calls": status.max_calls,
            }

        if not self.api_key:
            raise RuntimeError("CFBD_API_KEY is not set. Export it in your shell before using --execute.")
        insecure_ssl = str(os.getenv("CFBD_INSECURE_SSL", "")).strip().lower() in {"1", "true", "yes"}
        status_before = self.tracker.status()
        if status_before.calls_used >= status_before.max_calls:
            raise CFBDQuotaExceeded(
                f"CFBD monthly cap reached ({status_before.calls_used}/{status_before.max_calls}). Refusing new API call."
            )
        payload = {"query": query}
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
        }
        try:
            import requests  # type: ignore

            resp = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=self.timeout_seconds,
                verify=not insecure_ssl,
            )
            resp.raise_for_status()
            try:
                data = resp.json()
            except ValueError as exc:
                snippet = (resp.text or "")[:400]
                raise RuntimeError(
                    f"CFBD GraphQL returned non-JSON response "
                    f"(status={resp.status_code}, content_type={resp.headers.get('Content-Type', '')}). "
                    f"Body snippet: {snippet}"
                ) from exc
        except ModuleNotFoundError:
            import ssl
            from urllib.request import Request, urlopen

            req = Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            ctx = ssl._create_unverified_context() if insecure_ssl else None
            with urlopen(req, timeout=self.timeout_seconds, context=ctx) as resp:  # nosec B310
                data = json.loads(resp.read().decode("utf-8"))
        status = self.tracker.reserve_call(endpoint="/graphql", params={"query": "adjustedPlayerMetrics"})
        if isinstance(data, dict) and data.get("errors"):
            raise RuntimeError(f"CFBD GraphQL returned errors: {data.get('errors')}")
        return {
            "dry_run": False,
            "url": url,
            "query": query,
            "calls_used": status.calls_used,
            "calls_remaining": status.calls_remaining,
            "max_calls": status.max_calls,
            "data": data,
        }


DATASET_ENDPOINTS = {
    "player_season_stats": "/stats/player/season",
    "game_player_stats": "/games/players",
    "team_season_stats": "/stats/season",
    "team_advanced_stats": "/stats/season/advanced",
    "advanced_game_stats": "/stats/game/advanced",
    "player_ppa": "/ppa/players/season",
    "player_ppa_games": "/ppa/players/games",
    "player_usage": "/player/usage",
    "team_ppa": "/ppa/teams",
    "games": "/games",
    "team_game_stats": "/games/teams",
    "roster": "/roster",
    "fbs_teams": "/teams/fbs",
}


SEASON_TYPE_DATASETS = {"games", "team_game_stats", "advanced_game_stats", "team_ppa"}


def _iter_season_type_weeks(season_type: str) -> list[tuple[str, int]]:
    normalized = str(season_type or "regular").strip().lower()
    pairs: list[tuple[str, int]] = []
    if normalized in {"regular", "both"}:
        for week in range(1, 17):
            pairs.append(("regular", week))
    if normalized in {"postseason", "both"}:
        for week in range(1, 6):
            pairs.append(("postseason", week))
    return pairs


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


def fetch_game_player_stats(
    year: int,
    season_type: str = "regular",
    execute: bool = False,
    max_calls_per_month: int = 1000,
) -> Dict[str, Any]:
    client = CFBDClient(max_calls_per_month=max_calls_per_month)
    rows: list[dict] = []
    last_status: Dict[str, Any] | None = None
    for season_type_key, week in _iter_season_type_weeks(season_type):
        result = client.get(
            DATASET_ENDPOINTS["game_player_stats"],
            params={"year": year, "week": week, "seasonType": season_type_key},
            execute=execute,
        )
        if result.get("dry_run", False):
            return result
        batch = result.get("data") or []
        if isinstance(batch, list):
            for row in batch:
                if isinstance(row, dict):
                    row.setdefault("_cfbd_pull_week", week)
                    row.setdefault("_cfbd_pull_season_type", season_type_key)
            rows.extend(batch)
        last_status = result
    if last_status is None:
        status = client.usage_status()
        return {
            "dry_run": False,
            "url": f"{client.base_url}{DATASET_ENDPOINTS['game_player_stats']}",
            "calls_used": status.calls_used,
            "calls_remaining": status.calls_remaining,
            "max_calls": status.max_calls,
            "data": [],
        }
    return {
        "dry_run": False,
        "url": f"{client.base_url}{DATASET_ENDPOINTS['game_player_stats']}",
        "calls_used": last_status["calls_used"],
        "calls_remaining": last_status["calls_remaining"],
        "max_calls": last_status["max_calls"],
        "query_type": "game_player_stats_weekly_rollup",
        "data": rows,
    }


def fetch_player_ppa_games(
    year: int,
    season_type: str = "regular",
    execute: bool = False,
    max_calls_per_month: int = 1000,
    threshold: float = 0.0,
) -> Dict[str, Any]:
    client = CFBDClient(max_calls_per_month=max_calls_per_month)
    rows: list[dict] = []
    last_status: Dict[str, Any] | None = None
    for season_type_key, week in _iter_season_type_weeks(season_type):
        params: Dict[str, Any] = {"year": year, "week": week, "seasonType": season_type_key}
        if threshold > 0:
            params["threshold"] = threshold
        result = client.get(DATASET_ENDPOINTS["player_ppa_games"], params=params, execute=execute)
        if result.get("dry_run", False):
            return result
        batch = result.get("data") or []
        if isinstance(batch, list):
            rows.extend(batch)
        last_status = result
    if last_status is None:
        status = client.usage_status()
        return {
            "dry_run": False,
            "url": f"{client.base_url}{DATASET_ENDPOINTS['player_ppa_games']}",
            "calls_used": status.calls_used,
            "calls_remaining": status.calls_remaining,
            "max_calls": status.max_calls,
            "data": [],
        }
    return {
        "dry_run": False,
        "url": f"{client.base_url}{DATASET_ENDPOINTS['player_ppa_games']}",
        "calls_used": last_status["calls_used"],
        "calls_remaining": last_status["calls_remaining"],
        "max_calls": last_status["max_calls"],
        "query_type": "player_ppa_games_weekly_rollup",
        "data": rows,
    }


def fetch_adjusted_player_metrics(
    year: int,
    execute: bool = False,
    max_calls_per_month: int = 1000,
    page_size: int = 5000,
) -> Dict[str, Any]:
    client = CFBDClient(max_calls_per_month=max_calls_per_month)
    offset = 0
    rows: list[dict] = []
    while True:
        query = f"""
        query {{
          adjustedPlayerMetrics(
            limit: {int(page_size)},
            offset: {int(offset)},
            orderBy: [{{athleteId: ASC}}, {{metricType: ASC}}],
            where: {{year: {{_eq: {int(year)}}}}}
          ) {{
            athleteId
            metricType
            metricValue
            plays
            year
            athlete {{
              id
              name
              teamId
              position {{
                abbreviation
              }}
            }}
          }}
        }}
        """.strip()
        result = client.graphql(query=query, execute=execute)
        if result.get("dry_run", False):
            return result
        batch = ((result.get("data") or {}).get("data") or {}).get("adjustedPlayerMetrics") or []
        rows.extend(batch)
        if len(batch) < int(page_size):
            return {
                "dry_run": False,
                "url": os.getenv("CFBD_GRAPHQL_URL", "https://graphql.collegefootballdata.com/v1/graphql").strip(),
                "calls_used": result["calls_used"],
                "calls_remaining": result["calls_remaining"],
                "max_calls": result["max_calls"],
                "query_type": "adjustedPlayerMetrics",
                "data": rows,
            }
        offset += int(page_size)
