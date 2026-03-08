#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "sources" / "external" / "spotrac"
CONTRACTS_OUT = OUT_DIR / "spotrac_contracts_2026.csv"
FREE_AGENTS_OUT = OUT_DIR / "spotrac_free_agents_2026.csv"
REPORT_OUT = ROOT / "data" / "outputs" / "spotrac_refresh_report_2026.md"

CONTRACTS_URL = "https://www.spotrac.com/nfl/contracts"
FREE_AGENTS_URL = "https://www.spotrac.com/nfl/free-agents/_/year/2026"

TEAM_ALIASES = {
    "arizona cardinals": "ARI",
    "atlanta falcons": "ATL",
    "baltimore ravens": "BAL",
    "buffalo bills": "BUF",
    "carolina panthers": "CAR",
    "chicago bears": "CHI",
    "cincinnati bengals": "CIN",
    "cleveland browns": "CLE",
    "dallas cowboys": "DAL",
    "denver broncos": "DEN",
    "detroit lions": "DET",
    "green bay packers": "GB",
    "houston texans": "HOU",
    "indianapolis colts": "IND",
    "jacksonville jaguars": "JAX",
    "kansas city chiefs": "KC",
    "las vegas raiders": "LV",
    "los angeles chargers": "LAC",
    "los angeles rams": "LAR",
    "miami dolphins": "MIA",
    "minnesota vikings": "MIN",
    "new england patriots": "NE",
    "new orleans saints": "NO",
    "new york giants": "NYG",
    "new york jets": "NYJ",
    "philadelphia eagles": "PHI",
    "pittsburgh steelers": "PIT",
    "san francisco 49ers": "SF",
    "seattle seahawks": "SEA",
    "tampa bay buccaneers": "TB",
    "tennessee titans": "TEN",
    "washington commanders": "WAS",
    "arizona": "ARI",
    "atlanta": "ATL",
    "baltimore": "BAL",
    "buffalo": "BUF",
    "carolina": "CAR",
    "chicago": "CHI",
    "cincinnati": "CIN",
    "cleveland": "CLE",
    "dallas": "DAL",
    "denver": "DEN",
    "detroit": "DET",
    "green bay": "GB",
    "houston": "HOU",
    "indianapolis": "IND",
    "jacksonville": "JAX",
    "kansas city": "KC",
    "las vegas": "LV",
    "los angeles chargers": "LAC",
    "los angeles rams": "LAR",
    "miami": "MIA",
    "minnesota": "MIN",
    "new england": "NE",
    "new orleans": "NO",
    "new york giants": "NYG",
    "new york jets": "NYJ",
    "philadelphia": "PHI",
    "pittsburgh": "PIT",
    "san francisco": "SF",
    "seattle": "SEA",
    "tampa bay": "TB",
    "tennessee": "TEN",
    "washington": "WAS",
    "ari": "ARI",
    "atl": "ATL",
    "bal": "BAL",
    "buf": "BUF",
    "car": "CAR",
    "chi": "CHI",
    "cin": "CIN",
    "cle": "CLE",
    "dal": "DAL",
    "den": "DEN",
    "det": "DET",
    "gb": "GB",
    "hou": "HOU",
    "ind": "IND",
    "jax": "JAX",
    "kc": "KC",
    "lv": "LV",
    "lac": "LAC",
    "lar": "LAR",
    "mia": "MIA",
    "min": "MIN",
    "ne": "NE",
    "no": "NO",
    "nyg": "NYG",
    "nyj": "NYJ",
    "phi": "PHI",
    "pit": "PIT",
    "sf": "SF",
    "sea": "SEA",
    "tb": "TB",
    "ten": "TEN",
    "was": "WAS",
}


def _normalize_column(col: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(col or "").strip().lower())


def _normalize_team(value: str) -> str:
    text = re.sub(r"[^a-z0-9\s]+", " ", str(value or "").strip().lower())
    text = re.sub(r"\s+", " ", text).strip()
    return TEAM_ALIASES.get(text, "")


def _normalize_player(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text


def _safe_float(value: object) -> float | None:
    txt = str(value or "").strip()
    if not txt or txt.lower() in {"nan", "none", "-", "—"}:
        return None
    txt = txt.replace(",", "")
    try:
        return float(txt)
    except ValueError:
        return None


def _parse_money_millions(value: object) -> float | None:
    txt = str(value or "").strip()
    if not txt or txt.lower() in {"nan", "none", "-", "—"}:
        return None
    txt = txt.replace(",", "").replace("$", "").strip().lower()
    mult = 1.0
    if txt.endswith("m"):
        mult = 1.0
        txt = txt[:-1]
    elif txt.endswith("k"):
        mult = 0.001
        txt = txt[:-1]
    elif txt.endswith("b"):
        mult = 1000.0
        txt = txt[:-1]
    number = _safe_float(txt)
    return round(number * mult, 3) if number is not None else None


def _parse_int(value: object) -> int | None:
    txt = str(value or "").strip()
    if not txt or txt.lower() in {"nan", "none", "-", "—"}:
        return None
    match = re.search(r"\d+", txt)
    return int(match.group(0)) if match else None


def _fetch_tables(url: str) -> list[pd.DataFrame]:
    resp = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0 Safari/537.36"
            )
        },
    )
    resp.raise_for_status()
    return pd.read_html(StringIO(resp.text))


def _pick_contracts_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for df in tables:
        cols = {_normalize_column(col) for col in df.columns}
        if "player" in cols and ("team" in cols or "currentlywith" in cols) and (
            "pos" in cols or "position" in cols
        ) and ("avgyear" in cols or "averagesalary" in cols or "aav" in cols or "fa" in cols or "freeagency" in cols):
            return df
    raise ValueError("Could not find Spotrac contracts table")


def _pick_free_agents_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for df in tables:
        cols = {_normalize_column(col) for col in df.columns}
        if "player" in cols and ("prevteam" in cols or "team" in cols) and (
            "marketvalueaav" in cols or "marketaav" in cols or "prevaav" in cols
        ):
            return df
    raise ValueError("Could not find Spotrac free agents table")


def _colmap(df: pd.DataFrame) -> dict[str, str]:
    return {_normalize_column(col): col for col in df.columns}


def _normalize_contract_rows(df: pd.DataFrame, pulled_at_utc: str) -> list[dict]:
    cmap = _colmap(df)
    rows: list[dict] = []
    for raw in df.to_dict(orient="records"):
        player_name = _normalize_player(raw.get(cmap.get("player", ""), ""))
        if not player_name:
            continue
        team_raw = str(raw.get(cmap.get("currentlywith", ""), "") or raw.get(cmap.get("team", ""), "")).strip()
        team_norm = _normalize_team(team_raw)
        position = str(raw.get(cmap.get("pos", ""), "") or raw.get(cmap.get("position", ""), "")).strip().upper()
        years = _parse_int(raw.get(cmap.get("yrs", ""), "") or raw.get(cmap.get("years", ""), ""))
        apy_m = _parse_money_millions(
            raw.get(cmap.get("avgyear", ""), "")
            or raw.get(cmap.get("averagesalary", ""), "")
            or raw.get(cmap.get("aav", ""), "")
        )
        fa_year = _parse_int(raw.get(cmap.get("fa", ""), "") or raw.get(cmap.get("freeagency", ""), ""))
        if fa_year is not None:
            years_remaining = max(0, fa_year - 2026)
            contract_end_year = fa_year - 1 if fa_year > 0 else None
        else:
            years_remaining = years or 0
            contract_end_year = 2026 + max((years or 0) - 1, 0) if years else None
        rows.append(
            {
                "player_name": player_name,
                "team": team_raw,
                "team_norm": team_norm,
                "position": position,
                "age": _parse_int(raw.get(cmap.get("age", ""), "")) or "",
                "years": years or "",
                "years_remaining": years_remaining,
                "contract_end_year": contract_end_year or "",
                "apy_m": apy_m if apy_m is not None else "",
                "source_url": CONTRACTS_URL,
                "pulled_at_utc": pulled_at_utc,
            }
        )
    return rows


def _normalize_free_agent_rows(df: pd.DataFrame, pulled_at_utc: str) -> list[dict]:
    cmap = _colmap(df)
    rows: list[dict] = []
    for raw in df.to_dict(orient="records"):
        player_name = _normalize_player(raw.get(cmap.get("player", ""), ""))
        if not player_name:
            continue
        prev_team_raw = str(raw.get(cmap.get("prevteam", ""), "") or raw.get(cmap.get("team", ""), "")).strip()
        prev_team_norm = _normalize_team(prev_team_raw)
        position = str(raw.get(cmap.get("pos", ""), "") or raw.get(cmap.get("position", ""), "")).strip().upper()
        market_value_apy_m = _parse_money_millions(
            raw.get(cmap.get("marketvalueaav", ""), "")
            or raw.get(cmap.get("marketaav", ""), "")
        )
        prev_apy_m = _parse_money_millions(raw.get(cmap.get("prevaav", ""), ""))
        rows.append(
            {
                "player_name": player_name,
                "prev_team": prev_team_raw,
                "prev_team_norm": prev_team_norm,
                "position": position,
                "age": _parse_int(raw.get(cmap.get("age", ""), "")) or "",
                "years_exp": _parse_int(raw.get(cmap.get("yoe", ""), "")) or _parse_int(raw.get(cmap.get("exp", ""), "")) or "",
                "market_value_apy_m": market_value_apy_m if market_value_apy_m is not None else "",
                "prev_apy_m": prev_apy_m if prev_apy_m is not None else "",
                "source_url": FREE_AGENTS_URL,
                "pulled_at_utc": pulled_at_utc,
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull narrow Spotrac NFL contract/free-agent tables for team-needs freshness.")
    parser.add_argument("--contracts-url", default=CONTRACTS_URL)
    parser.add_argument("--free-agents-url", default=FREE_AGENTS_URL)
    parser.add_argument("--contracts-out", type=Path, default=CONTRACTS_OUT)
    parser.add_argument("--free-agents-out", type=Path, default=FREE_AGENTS_OUT)
    args = parser.parse_args()

    pulled_at_utc = datetime.now(timezone.utc).isoformat()
    contract_tables = _fetch_tables(args.contracts_url)
    free_agent_tables = _fetch_tables(args.free_agents_url)
    contract_rows = _normalize_contract_rows(_pick_contracts_table(contract_tables), pulled_at_utc)
    free_agent_rows = _normalize_free_agent_rows(_pick_free_agents_table(free_agent_tables), pulled_at_utc)
    _write_csv(args.contracts_out, contract_rows)
    _write_csv(args.free_agents_out, free_agent_rows)

    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_OUT.write_text(
        "\n".join(
            [
                "# Spotrac Refresh Report",
                "",
                f"- Generated: {pulled_at_utc}",
                f"- Contracts URL: {args.contracts_url}",
                f"- Free agents URL: {args.free_agents_url}",
                f"- Contract rows: {len(contract_rows)}",
                f"- Free agent rows: {len(free_agent_rows)}",
                f"- Contracts out: `{args.contracts_out}`",
                f"- Free agents out: `{args.free_agents_out}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {args.contracts_out} ({len(contract_rows)} rows)")
    print(f"Wrote {args.free_agents_out} ({len(free_agent_rows)} rows)")
    print(f"Wrote {REPORT_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
