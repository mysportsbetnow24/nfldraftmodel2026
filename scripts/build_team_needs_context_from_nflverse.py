#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
NFLVERSE_DIR = ROOT / "data" / "sources" / "external" / "nflverse"
ROSTERS_PATH = NFLVERSE_DIR / "rosters_weekly.parquet"
CONTRACTS_PATH = NFLVERSE_DIR / "contracts.parquet"
PLAYERS_PATH = NFLVERSE_DIR / "players.parquet"
PARTICIPATION_PATH = NFLVERSE_DIR / "participation.parquet"
TEAM_PROFILES_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"
OUT_PATH = ROOT / "data" / "sources" / "team_needs_context_2026.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "team_needs_context_from_nflverse_report_2026-02-28.md"

MODEL_POSITIONS = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]

POS_MAP = {
    "QB": "QB",
    "RB": "RB",
    "HB": "RB",
    "FB": "RB",
    "WR": "WR",
    "TE": "TE",
    "T": "OT",
    "OT": "OT",
    "LT": "OT",
    "RT": "OT",
    "G": "IOL",
    "OG": "IOL",
    "LG": "IOL",
    "RG": "IOL",
    "C": "IOL",
    "OL": "IOL",
    "EDGE": "EDGE",
    "DE": "EDGE",
    "OLB": "EDGE",
    "DT": "DT",
    "NT": "DT",
    "DL": "DT",
    "LB": "LB",
    "ILB": "LB",
    "MLB": "LB",
    "WLB": "LB",
    "SLB": "LB",
    "CB": "CB",
    "NB": "CB",
    "DB": "CB",
    "S": "S",
    "FS": "S",
    "SS": "S",
}

TEAM_NAME_TO_ABBR = {
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
    "washington football team": "WAS",
    "washington redskins": "WAS",
}


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _norm_pos(raw: str) -> str:
    p = str(raw or "").strip().upper()
    return POS_MAP.get(p, "")


def _norm_team_name_to_abbr(name: str) -> str:
    key = str(name or "").strip().lower()
    if key in TEAM_NAME_TO_ABBR:
        return TEAM_NAME_TO_ABBR[key]
    # handle compound strings like GB/NYJ
    if "/" in key:
        parts = [p.strip() for p in key.split("/") if p.strip()]
        if parts:
            return _norm_team_name_to_abbr(parts[-1])
    return ""


def _read_team_profiles(path: Path) -> list[str]:
    teams = []
    with path.open() as f:
        for row in csv.DictReader(f):
            t = str(row.get("team", "")).strip()
            if t:
                teams.append(t)
    return sorted(set(teams))


def _split_semis(value: str) -> list[str]:
    txt = str(value or "").strip()
    if not txt:
        return []
    return [x.strip().upper() for x in txt.split(";") if x.strip()]


def _build_roster_metrics(rosters: pl.DataFrame) -> dict[tuple[str, str], dict]:
    if rosters.is_empty():
        return {}

    latest_season = int(rosters.select(pl.col("season").max()).item())
    base = rosters.filter(pl.col("season") == latest_season)
    # Use each team's latest regular-season snapshot, not the global max week (which is playoff-only).
    reg = base.filter(pl.col("game_type") == "REG")
    if reg.is_empty():
        reg = base
    latest_week_by_team = reg.group_by("team").agg(pl.col("week").max().alias("team_latest_week"))
    r = reg.join(latest_week_by_team, on="team", how="inner").filter(pl.col("week") == pl.col("team_latest_week"))

    out: dict[tuple[str, str], dict] = {}
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for row in r.iter_rows(named=True):
        team = str(row.get("team", "")).strip()
        depth_pos = _norm_pos(str(row.get("depth_chart_position", "")))
        pos = depth_pos or _norm_pos(str(row.get("position", "")))
        if not team or pos not in MODEL_POSITIONS:
            continue
        grouped[(team, pos)].append(row)

    for key, rows in grouped.items():
        count_players = len(rows)
        exp_vals = sorted((float(r.get("years_exp") or 0.0) for r in rows), reverse=True)
        top_exp = exp_vals[:2] if exp_vals else [0.0]
        avg_top_exp = sum(top_exp) / len(top_exp)
        exp_quality = _clamp(avg_top_exp / 7.0)

        draft_vals = []
        for r in rows:
            dn = r.get("draft_number")
            if dn is None:
                draft_vals.append(0.35)
            else:
                draft_vals.append(1.0 - _clamp(float(dn) / 260.0))
        draft_vals.sort(reverse=True)
        draft_quality = sum(draft_vals[:2]) / max(1, min(2, len(draft_vals)))

        starter_quality = _clamp((0.55 * exp_quality) + (0.45 * draft_quality))
        count_quality = _clamp(count_players / 4.0)
        depth_chart_pressure = _clamp(1.0 - ((0.5 * starter_quality) + (0.5 * count_quality)))

        out[key] = {
            "roster_count": count_players,
            "avg_top_exp": round(avg_top_exp, 3),
            "starter_quality": round(starter_quality, 4),
            "depth_chart_pressure": round(depth_chart_pressure, 4),
            "roster_season": latest_season,
            "roster_week": int(max(float(rw.get("week") or 0.0) for rw in rows)) if rows else "",
        }
    return out


def _build_contract_metrics(contracts: pl.DataFrame, players: pl.DataFrame, target_year: int) -> dict[tuple[str, str], dict]:
    if contracts.is_empty():
        return {}

    player_team = {}
    if not players.is_empty():
        for row in players.select(["gsis_id", "latest_team"]).iter_rows(named=True):
            gid = str(row.get("gsis_id", "")).strip()
            team = str(row.get("latest_team", "")).strip().upper()
            if gid and team:
                player_team[gid] = team

    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in contracts.iter_rows(named=True):
        if not bool(row.get("is_active", False)):
            continue
        pos = _norm_pos(str(row.get("position", "")))
        if pos not in MODEL_POSITIONS:
            continue
        gid = str(row.get("gsis_id", "")).strip()
        team = player_team.get(gid, "")
        if not team:
            team = _norm_team_name_to_abbr(row.get("team", ""))
        if not team:
            continue
        grouped[(team, pos)].append(row)

    out: dict[tuple[str, str], dict] = {}
    for key, rows in grouped.items():
        total_apy = 0.0
        fa_apy = 0.0
        cy_apy = 0.0
        total_players = len(rows)
        fa_count = 0
        cy_count = 0
        for r in rows:
            year_signed = r.get("year_signed")
            years = r.get("years")
            apy = float(r.get("apy") or 0.0)
            total_apy += max(0.0, apy)
            if year_signed is None or years is None:
                continue
            end_year = int(year_signed) + int(years) - 1
            if end_year <= (target_year - 1):
                fa_count += 1
                fa_apy += max(0.0, apy)
            if end_year <= target_year:
                cy_count += 1
                cy_apy += max(0.0, apy)

        if total_players <= 0:
            continue
        if total_apy > 0:
            free_agent_pressure = _clamp(fa_apy / total_apy)
            contract_year_pressure = _clamp(cy_apy / total_apy)
        else:
            free_agent_pressure = _clamp(fa_count / total_players)
            contract_year_pressure = _clamp(cy_count / total_players)

        out[key] = {
            "contract_player_count": total_players,
            "free_agent_pressure": round(free_agent_pressure, 4),
            "contract_year_pressure": round(contract_year_pressure, 4),
            "fa_count": fa_count,
            "contract_year_count": cy_count,
        }
    return out


def _build_participation_deployment(participation: pl.DataFrame) -> dict[tuple[str, str], dict]:
    if participation.is_empty():
        return {}

    # infer latest season from game_id prefix "YYYY_"
    p = participation.with_columns(
        pl.col("nflverse_game_id").str.slice(0, 4).cast(pl.Int32, strict=False).alias("season_tag")
    )
    latest_season = int(p.select(pl.col("season_tag").max()).item())
    p = p.filter(pl.col("season_tag") == latest_season)

    # league averages for deployment share.
    team_pos_counts: dict[tuple[str, str], int] = defaultdict(int)
    team_total_counts: dict[str, int] = defaultdict(int)
    pass_rushers_by_team: dict[str, list[float]] = defaultdict(list)
    box_by_team: dict[str, list[float]] = defaultdict(list)

    for row in p.select(
        [
            "possession_team",
            "offense_positions",
            "defense_positions",
            "number_of_pass_rushers",
            "defenders_in_box",
        ]
    ).iter_rows(named=True):
        team = str(row.get("possession_team", "")).strip().upper()
        if not team:
            continue

        off_pos = [_norm_pos(x) for x in _split_semis(row.get("offense_positions", ""))]
        def_pos = [_norm_pos(x) for x in _split_semis(row.get("defense_positions", ""))]
        pos_list = [x for x in off_pos + def_pos if x in MODEL_POSITIONS]
        if not pos_list:
            continue

        team_total_counts[team] += len(pos_list)
        for pos in pos_list:
            team_pos_counts[(team, pos)] += 1

        npr = row.get("number_of_pass_rushers")
        if npr is not None:
            pass_rushers_by_team[team].append(float(npr))
        dib = row.get("defenders_in_box")
        if dib is not None:
            box_by_team[team].append(float(dib))

    # league baseline share by position.
    league_pos_totals: dict[str, int] = defaultdict(int)
    league_total = 0
    for (team, pos), c in team_pos_counts.items():
        league_pos_totals[pos] += c
        league_total += c
    league_pos_share = {
        pos: (league_pos_totals[pos] / league_total) if league_total > 0 else 0.0 for pos in MODEL_POSITIONS
    }

    out: dict[tuple[str, str], dict] = {}
    for (team, pos), count in team_pos_counts.items():
        team_total = max(1, team_total_counts.get(team, 1))
        team_share = count / team_total
        base_share = max(0.0001, league_pos_share.get(pos, 0.0001))
        deployment_ratio = _clamp(team_share / base_share, 0.60, 1.40)

        avg_rushers = (
            sum(pass_rushers_by_team.get(team, [])) / max(1, len(pass_rushers_by_team.get(team, [])))
            if pass_rushers_by_team.get(team)
            else 4.0
        )
        avg_box = (
            sum(box_by_team.get(team, [])) / max(1, len(box_by_team.get(team, [])))
            if box_by_team.get(team)
            else 6.5
        )

        out[(team, pos)] = {
            "deployment_share": round(team_share, 4),
            "deployment_ratio": round(deployment_ratio, 4),
            "avg_pass_rushers": round(avg_rushers, 3),
            "avg_defenders_in_box": round(avg_box, 3),
            "participation_season": latest_season,
        }
    return out


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Build team_needs_context_2026.csv from nflverse roster/contract/participation data.")
    p.add_argument("--rosters", type=Path, default=ROSTERS_PATH)
    p.add_argument("--contracts", type=Path, default=CONTRACTS_PATH)
    p.add_argument("--players", type=Path, default=PLAYERS_PATH)
    p.add_argument("--participation", type=Path, default=PARTICIPATION_PATH)
    p.add_argument("--team-profiles", type=Path, default=TEAM_PROFILES_PATH)
    p.add_argument("--output", type=Path, default=OUT_PATH)
    p.add_argument("--report", type=Path, default=REPORT_PATH)
    p.add_argument("--target-year", type=int, default=2026)
    args = p.parse_args()

    missing = [str(pth) for pth in [args.rosters, args.contracts, args.players, args.participation, args.team_profiles] if not pth.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required inputs: {missing}")

    rosters = pl.read_parquet(args.rosters)
    contracts = pl.read_parquet(args.contracts)
    players = pl.read_parquet(args.players)
    participation = pl.read_parquet(args.participation)
    teams = _read_team_profiles(args.team_profiles)

    roster_metrics = _build_roster_metrics(rosters)
    contract_metrics = _build_contract_metrics(contracts, players, target_year=args.target_year)
    deployment_metrics = _build_participation_deployment(participation)

    rows: list[dict] = []
    for team in teams:
        for pos in MODEL_POSITIONS:
            r = roster_metrics.get((team, pos), {})
            c = contract_metrics.get((team, pos), {})
            d = deployment_metrics.get((team, pos), {})

            starter_quality = float(r.get("starter_quality", 0.50))
            depth_chart_pressure = float(r.get("depth_chart_pressure", 0.50))
            free_agent_pressure = float(c.get("free_agent_pressure", 0.50))
            contract_year_pressure = float(c.get("contract_year_pressure", 0.50))

            deploy_ratio = float(d.get("deployment_ratio", 1.0))
            depth_chart_pressure = _clamp(depth_chart_pressure * deploy_ratio)

            avg_rushers = float(d.get("avg_pass_rushers", 4.0))
            if pos in {"EDGE", "DT"} and avg_rushers >= 4.2:
                depth_chart_pressure = _clamp(depth_chart_pressure + 0.05)
            avg_box = float(d.get("avg_defenders_in_box", 6.5))
            if pos in {"LB", "S"} and avg_box >= 6.8:
                depth_chart_pressure = _clamp(depth_chart_pressure + 0.03)

            rows.append(
                {
                    "team": team,
                    "position": pos,
                    "depth_chart_pressure": round(depth_chart_pressure, 4),
                    "free_agent_pressure": round(free_agent_pressure, 4),
                    "contract_year_pressure": round(contract_year_pressure, 4),
                    "starter_quality": round(starter_quality, 4),
                    "roster_player_count": int(r.get("roster_count", 0)),
                    "avg_top_exp": r.get("avg_top_exp", ""),
                    "contract_player_count": int(c.get("contract_player_count", 0)),
                    "fa_count": int(c.get("fa_count", 0)),
                    "contract_year_count": int(c.get("contract_year_count", 0)),
                    "deployment_share": d.get("deployment_share", ""),
                    "deployment_ratio": d.get("deployment_ratio", ""),
                    "avg_pass_rushers": d.get("avg_pass_rushers", ""),
                    "avg_defenders_in_box": d.get("avg_defenders_in_box", ""),
                    "data_source": "nflverse_rosters_contracts_participation",
                    "built_at_utc": datetime.now(UTC).isoformat(),
                }
            )

    rows.sort(key=lambda x: (x["team"], x["position"]))
    _write_csv(args.output, rows)

    lines = [
        "# Team Needs Context Build Report (NFLverse)",
        "",
        f"- Built at: `{datetime.now(UTC).isoformat()}`",
        f"- Output: `{args.output}`",
        f"- Target year: `{args.target_year}`",
        f"- Teams: `{len(teams)}`",
        f"- Rows: `{len(rows)}`",
        "",
        "## Input Files",
        "",
        f"- rosters: `{args.rosters}` ({rosters.height} rows)",
        f"- contracts: `{args.contracts}` ({contracts.height} rows)",
        f"- players: `{args.players}` ({players.height} rows)",
        f"- participation: `{args.participation}` ({participation.height} rows)",
        "",
        "## Notes",
        "",
        "- `depth_chart_pressure` combines roster depth/experience and deployment intensity.",
        "- `free_agent_pressure` and `contract_year_pressure` are built from active contract term exposure.",
        "- `starter_quality` is roster-based so high pressure does not require poor current quality.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Rows: {len(rows)}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
