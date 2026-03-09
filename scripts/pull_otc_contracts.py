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
OUT_DIR = ROOT / "data" / "sources" / "external" / "otc"
CONTRACTS_OUT = OUT_DIR / "otc_contracts_2026.csv"
FREE_AGENTS_OUT = OUT_DIR / "otc_free_agents_2026.csv"
REPORT_OUT = ROOT / "data" / "outputs" / "otc_refresh_report_2026.md"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)

TEAM_SLUGS = {
    "ARI": "arizona-cardinals",
    "ATL": "atlanta-falcons",
    "BAL": "baltimore-ravens",
    "BUF": "buffalo-bills",
    "CAR": "carolina-panthers",
    "CHI": "chicago-bears",
    "CIN": "cincinnati-bengals",
    "CLE": "cleveland-browns",
    "DAL": "dallas-cowboys",
    "DEN": "denver-broncos",
    "DET": "detroit-lions",
    "GB": "green-bay-packers",
    "HOU": "houston-texans",
    "IND": "indianapolis-colts",
    "JAX": "jacksonville-jaguars",
    "KC": "kansas-city-chiefs",
    "LV": "las-vegas-raiders",
    "LAC": "los-angeles-chargers",
    "LAR": "los-angeles-rams",
    "MIA": "miami-dolphins",
    "MIN": "minnesota-vikings",
    "NE": "new-england-patriots",
    "NO": "new-orleans-saints",
    "NYG": "new-york-giants",
    "NYJ": "new-york-jets",
    "PHI": "philadelphia-eagles",
    "PIT": "pittsburgh-steelers",
    "SF": "san-francisco-49ers",
    "SEA": "seattle-seahawks",
    "TB": "tampa-bay-buccaneers",
    "TEN": "tennessee-titans",
    "WAS": "washington-commanders",
}

TEAM_NAMES = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LV": "Las Vegas Raiders",
    "LAC": "Los Angeles Chargers",
    "LAR": "Los Angeles Rams",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SF": "San Francisco 49ers",
    "SEA": "Seattle Seahawks",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WAS": "Washington Commanders",
}


def _normalize_column(col: object) -> str:
    if isinstance(col, tuple):
        pieces = [str(part or "").strip() for part in col if str(part or "").strip() and not str(part).startswith("Unnamed:")]
        txt = " ".join(pieces)
    else:
        txt = str(col or "").strip()
    return re.sub(r"[^a-z0-9]+", "", txt.lower())


def _normalize_player(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _parse_int(value: object) -> int | None:
    txt = str(value or "").strip()
    if not txt or txt.lower() in {"nan", "none", "-", "—"}:
        return None
    match = re.search(r"\d+", txt.replace(",", ""))
    return int(match.group(0)) if match else None


def _parse_money_millions(value: object) -> float | None:
    txt = str(value or "").strip()
    if not txt or txt.lower() in {"nan", "none", "-", "—"}:
        return None
    txt = txt.replace(",", "").replace("$", "").strip().lower()
    multiplier = 1.0
    if txt.endswith("m"):
        txt = txt[:-1]
    elif txt.endswith("k"):
        txt = txt[:-1]
        multiplier = 0.001
    elif txt.endswith("b"):
        txt = txt[:-1]
        multiplier = 1000.0
    try:
        return round(float(txt) * multiplier, 3)
    except ValueError:
        return None


def _fetch_tables(url: str) -> list[pd.DataFrame]:
    resp = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return pd.read_html(StringIO(resp.text))


def _pick_active_roster_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for df in tables:
        cols = {_normalize_column(col) for col in df.columns}
        if "playerplayer" in cols and "capnumbercapnumber" in cols and (
            "basesalarybasesalary" in cols or "guaranteedsalaryguaranteedsalary" in cols or "proratedbonussigning" in cols
        ):
            return df
    raise ValueError("Could not find OTC active roster table")


def _pick_free_agency_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for df in tables:
        cols = {_normalize_column(col) for col in df.columns}
        if "player" in cols and "pos" in cols and "type" in cols and "snaps" in cols:
            return df
    raise ValueError("Could not find OTC free agency table")


def _colmap(df: pd.DataFrame) -> dict[str, str]:
    return {_normalize_column(col): col for col in df.columns}


def _normalize_contract_rows(df: pd.DataFrame, team_norm: str, team_name: str, pulled_at_utc: str) -> list[dict]:
    cmap = _colmap(df)
    rows: list[dict] = []
    for raw in df.to_dict(orient="records"):
        player_name = _normalize_player(raw.get(cmap.get("playerplayer", ""), ""))
        if not player_name or player_name.lower().startswith("total "):
            continue
        rows.append(
            {
                "player_name": player_name,
                "team": team_name,
                "team_norm": team_norm,
                "position": "",
                "age": "",
                "years": "",
                "years_remaining": "",
                "contract_end_year": "",
                "apy_m": "",
                "source_url": f"https://overthecap.com/salary-cap/{TEAM_SLUGS[team_norm]}",
                "pulled_at_utc": pulled_at_utc,
            }
        )
    return rows


def _normalize_free_agent_rows(df: pd.DataFrame, team_norm: str, team_name: str, pulled_at_utc: str) -> list[dict]:
    cmap = _colmap(df)
    rows: list[dict] = []
    for raw in df.to_dict(orient="records"):
        player_name = _normalize_player(raw.get(cmap.get("player", ""), ""))
        if not player_name:
            continue
        current_team = str(raw.get(cmap.get("2026team", ""), "")).strip()
        # A blank 2026 team means still unsigned for the selected 2026 FA page.
        if current_team:
            continue
        rows.append(
            {
                "player_name": player_name,
                "prev_team": team_name,
                "prev_team_norm": team_norm,
                "position": str(raw.get(cmap.get("pos", ""), "")).strip().upper(),
                "age": _parse_int(raw.get(cmap.get("age", ""), "")) or "",
                "years_exp": "",
                "market_value_apy_m": _parse_money_millions(raw.get(cmap.get("currentapy", ""), "")) or "",
                "prev_apy_m": _parse_money_millions(raw.get(cmap.get("currentapy", ""), "")) or "",
                "snaps": _parse_int(raw.get(cmap.get("snaps", ""), "")) or "",
                "fa_type": str(raw.get(cmap.get("type", ""), "")).strip().upper(),
                "source_url": f"https://overthecap.com/free-agency/{TEAM_SLUGS[team_norm]}",
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
    parser = argparse.ArgumentParser(description="Pull narrow OTC NFL contract/free-agent tables for team-needs freshness.")
    parser.add_argument("--contracts-out", type=Path, default=CONTRACTS_OUT)
    parser.add_argument("--free-agents-out", type=Path, default=FREE_AGENTS_OUT)
    args = parser.parse_args()

    pulled_at_utc = datetime.now(timezone.utc).isoformat()
    contract_rows: list[dict] = []
    free_agent_rows: list[dict] = []
    for team_norm, slug in TEAM_SLUGS.items():
        team_name = TEAM_NAMES[team_norm]
        contract_tables = _fetch_tables(f"https://overthecap.com/salary-cap/{slug}")
        free_agent_tables = _fetch_tables(f"https://overthecap.com/free-agency/{slug}")
        contract_rows.extend(
            _normalize_contract_rows(_pick_active_roster_table(contract_tables), team_norm, team_name, pulled_at_utc)
        )
        free_agent_rows.extend(
            _normalize_free_agent_rows(_pick_free_agency_table(free_agent_tables), team_norm, team_name, pulled_at_utc)
        )

    _write_csv(args.contracts_out, contract_rows)
    _write_csv(args.free_agents_out, free_agent_rows)

    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_OUT.write_text(
        "\n".join(
            [
                "# OTC Refresh Report",
                "",
                f"- Generated: {pulled_at_utc}",
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
