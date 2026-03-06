#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
NFLVERSE_DIR = ROOT / "data" / "sources" / "external" / "nflverse"
ROSTERS_PATH = NFLVERSE_DIR / "rosters_weekly.parquet"
CONTRACTS_PATH = NFLVERSE_DIR / "contracts.parquet"
PLAYERS_PATH = NFLVERSE_DIR / "players.parquet"
PARTICIPATION_PATH = NFLVERSE_DIR / "participation.parquet"
TEAM_PROFILES_PATH = ROOT / "data" / "sources" / "team_profiles_2026.csv"
ESPN_DEPTH_CHARTS_PATH = ROOT / "data" / "sources" / "external" / "espn_depth_charts_2026.csv"
OUT_PATH = ROOT / "data" / "sources" / "team_needs_context_2026.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "team_needs_context_from_nflverse_report_2026-02-28.md"

MODEL_POSITIONS = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]
STARTERS_BY_POSITION = {"QB": 1, "RB": 1, "WR": 2, "TE": 1, "OT": 2, "IOL": 3, "EDGE": 2, "DT": 2, "LB": 2, "CB": 2, "S": 2}
AGE_CLIFF_BY_POSITION = {"QB": 33, "RB": 27, "WR": 29, "TE": 30, "OT": 31, "IOL": 31, "EDGE": 30, "DT": 30, "LB": 29, "CB": 29, "S": 30}

POS_MAP = {
    "QB": "QB",
    "RB": "RB",
    "HB": "RB",
    "FB": "RB",
    "WR": "WR",
    "LWR": "WR",
    "RWR": "WR",
    "SWR": "WR",
    "XWR": "WR",
    "ZWR": "WR",
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
    "LDE": "EDGE",
    "RDE": "EDGE",
    "LOLB": "EDGE",
    "ROLB": "EDGE",
    "DT": "DT",
    "NT": "DT",
    "LDT": "DT",
    "RDT": "DT",
    "DL": "DT",
    "LB": "LB",
    "ILB": "LB",
    "MLB": "LB",
    "WLB": "LB",
    "SLB": "LB",
    "LILB": "LB",
    "RILB": "LB",
    "CB": "CB",
    "LCB": "CB",
    "RCB": "CB",
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


def _parse_date(value: str | None) -> date | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    return None


def _age_on_jan1(birth: date | None, year: int) -> float | None:
    if birth is None:
        return None
    ref = date(int(year), 1, 1)
    return round((ref - birth).days / 365.25, 3)


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


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _build_espn_depth_chart_metrics(
    espn_rows: list[dict],
    rosters: pl.DataFrame,
    contracts: pl.DataFrame,
) -> dict[tuple[str, str], dict]:
    if not espn_rows:
        return {}

    latest_season = int(rosters.select(pl.col("season").max()).item()) if not rosters.is_empty() else 0
    roster_lookup: dict[tuple[str, str], dict] = {}
    if not rosters.is_empty():
        roster_base = rosters.filter(pl.col("season") == latest_season)
        if "game_type" in roster_base.columns:
            reg_subset = roster_base.filter(pl.col("game_type") == "REG")
            if not reg_subset.is_empty():
                roster_base = reg_subset
        if not roster_base.is_empty():
            latest_week_by_team = roster_base.group_by("team").agg(pl.col("week").max().alias("team_latest_week"))
            roster_base = (
                roster_base.join(latest_week_by_team, on="team", how="inner")
                .filter(pl.col("week") == pl.col("team_latest_week"))
                .unique(subset=["team", "gsis_id"], keep="first")
            )
            for row in roster_base.iter_rows(named=True):
                team = str(row.get("team", "")).strip().upper()
                name = str(row.get("full_name") or row.get("football_name") or "").strip()
                if not team or not name:
                    continue
                roster_lookup[(team, "".join(ch for ch in name.lower() if ch.isalnum()))] = row

    contract_by_name: dict[tuple[str, str], dict] = {}
    apy_pool_by_pos: dict[str, list[float]] = defaultdict(list)
    if not contracts.is_empty():
        for row in contracts.iter_rows(named=True):
            if not bool(row.get("is_active", False)):
                continue
            pos = _norm_pos(str(row.get("position", "")))
            if pos not in MODEL_POSITIONS:
                continue
            raw_team = _norm_team_name_to_abbr(row.get("team", ""))
            team = str(raw_team or "").strip().upper()
            name = str(row.get("player", "")).strip()
            if not team or not name:
                continue
            key = (team, "".join(ch for ch in name.lower() if ch.isalnum()))
            existing = contract_by_name.get(key)
            apy = float(row.get("apy") or 0.0)
            if pos and apy > 0:
                apy_pool_by_pos[pos].append(apy)
            if existing is None or float(existing.get("apy") or 0.0) < apy:
                contract_by_name[key] = row

    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in espn_rows:
        team = str(row.get("team") or "").strip().upper()
        player_name = str(row.get("player_name") or "").strip()
        pos = _norm_pos(str(row.get("position_abbreviation") or ""))
        if not pos:
            pos = _norm_pos(str(row.get("position_key") or row.get("position_slot") or ""))
        if not team or not player_name or pos not in MODEL_POSITIONS:
            continue
        grouped[(team, pos)].append(row)

    out: dict[tuple[str, str], dict] = {}
    for key, rows in grouped.items():
        team, pos = key
        unique_players: list[dict] = []
        seen_names: set[str] = set()
        for row in sorted(
            rows,
            key=lambda item: (
                int(item.get("rank") or 99),
                str(item.get("position_slot") or item.get("position_key") or ""),
                str(item.get("player_name") or ""),
            ),
        ):
            player_name = str(row.get("player_name") or "").strip()
            player_key = "".join(ch for ch in player_name.lower() if ch.isalnum())
            if not player_key or player_key in seen_names:
                continue
            seen_names.add(player_key)
            roster_row = roster_lookup.get((team, player_key), {})
            contract_row = contract_by_name.get((team, player_key), {})
            apy = float(contract_row.get("apy") or 0.0) if contract_row else 0.0
            unique_players.append(
                {
                    "player_name": player_name,
                    "years_exp": float(roster_row.get("years_exp") or 0.0) if roster_row else 0.0,
                    "draft_number": roster_row.get("draft_number") if roster_row else None,
                    "apy": apy,
                }
            )

        if not unique_players:
            continue

        starters_n = int(STARTERS_BY_POSITION.get(pos, 2))
        starters = unique_players[:starters_n]
        count_players = len(unique_players)
        exp_vals = sorted((float(p.get("years_exp") or 0.0) for p in starters), reverse=True)
        top_exp = exp_vals[:starters_n] if exp_vals else [0.0]
        avg_top_exp = sum(top_exp) / len(top_exp)
        exp_quality = _clamp(avg_top_exp / 7.0)

        draft_vals: list[float] = []
        for p in starters:
            dn = p.get("draft_number")
            if dn is None or str(dn).strip() == "":
                draft_vals.append(0.35)
            else:
                draft_vals.append(1.0 - _clamp(float(dn) / 260.0))
        draft_quality = sum(draft_vals) / max(1, len(draft_vals))

        apy_scores = [
            _clamp(float(p.get("apy") or 0.0) / max(1.0, max(apy_pool_by_pos.get(pos, [1.0]))))
            for p in starters
        ]
        apy_quality = sum(apy_scores) / max(1, len(apy_scores)) if apy_scores else 0.0

        starter_quality = _clamp((0.45 * exp_quality) + (0.35 * draft_quality) + (0.20 * apy_quality))
        count_quality = _clamp(min(count_players, 4) / 4.0)
        depth_chart_pressure = _clamp(1.0 - ((0.65 * starter_quality) + (0.35 * count_quality)))

        out[key] = {
            "roster_count": count_players,
            "avg_top_exp": round(avg_top_exp, 3),
            "starter_quality": round(starter_quality, 4),
            "depth_chart_pressure": round(depth_chart_pressure, 4),
            "roster_season": latest_season if latest_season else "",
            "roster_week": "espn",
            "depth_chart_source": "espn_depth_charts",
        }
    return out


def _build_contract_metrics(contracts: pl.DataFrame, players: pl.DataFrame, target_year: int) -> dict[tuple[str, str], dict]:
    if contracts.is_empty():
        return {}

    player_info = {}
    if not players.is_empty():
        for row in players.select(["gsis_id", "latest_team", "birth_date"]).iter_rows(named=True):
            gid = str(row.get("gsis_id", "")).strip()
            team = str(row.get("latest_team", "")).strip().upper()
            birth = _parse_date(row.get("birth_date"))
            if gid and team:
                player_info[gid] = {"team": team, "birth_date": birth}

    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in contracts.iter_rows(named=True):
        if not bool(row.get("is_active", False)):
            continue
        pos = _norm_pos(str(row.get("position", "")))
        if pos not in MODEL_POSITIONS:
            continue
        gid = str(row.get("gsis_id", "")).strip()
        info = player_info.get(gid, {})
        team = str(info.get("team", "")).strip().upper()
        if not team:
            team = _norm_team_name_to_abbr(row.get("team", ""))
        if not team:
            continue
        grouped[(team, pos)].append(
            {
                "contract": row,
                "birth_date": info.get("birth_date") or _parse_date(row.get("date_of_birth")),
            }
        )

    out: dict[tuple[str, str], dict] = {}
    for key, rows in grouped.items():
        team, pos = key
        total_apy = 0.0
        fa_apy = 0.0
        cy_apy = 0.0
        total_players = len(rows)
        fa_count = 0
        cy_count = 0
        starter_pool: list[dict] = []
        for node in rows:
            r = node["contract"]
            year_signed = r.get("year_signed")
            years = r.get("years")
            apy = float(r.get("apy") or 0.0)
            total_apy += max(0.0, apy)
            starter_pool.append(
                {
                    "apy": max(0.0, apy),
                    "year_signed": year_signed,
                    "years": years,
                    "birth_date": node.get("birth_date"),
                }
            )
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

        starter_n = int(STARTERS_BY_POSITION.get(pos, 2))
        starters = sorted(
            starter_pool,
            key=lambda x: (float(x.get("apy", 0.0) or 0.0), -int(x.get("years") or 0)),
            reverse=True,
        )[:starter_n]
        starter_value_total = sum(float(s.get("apy", 0.0) or 0.0) for s in starters)
        if starter_value_total <= 0:
            starter_value_total = float(max(1, len(starters)))

        cliff_threshold = float(AGE_CLIFF_BY_POSITION.get(pos, 30))
        cliff1_val = 0.0
        cliff2_val = 0.0
        cliff1_count = 0
        cliff2_count = 0
        starter_age_vals: list[float] = []

        for s in starters:
            year_signed = s.get("year_signed")
            years = s.get("years")
            end_year = None
            if year_signed is not None and years is not None:
                end_year = int(year_signed) + int(years) - 1
            birth = s.get("birth_date")
            age_y1 = _age_on_jan1(birth, target_year) if isinstance(birth, date) else None
            age_y2 = _age_on_jan1(birth, target_year + 1) if isinstance(birth, date) else None
            if age_y1 is not None:
                starter_age_vals.append(float(age_y1))

            risk1 = False
            risk2 = False
            if end_year is not None:
                risk1 = risk1 or (end_year <= target_year)
                risk2 = risk2 or (end_year <= (target_year + 1))
            if age_y1 is not None:
                risk1 = risk1 or (age_y1 >= cliff_threshold)
            if age_y2 is not None:
                risk2 = risk2 or (age_y2 >= cliff_threshold)

            starter_weight = float(s.get("apy", 0.0) or 0.0)
            if starter_weight <= 0:
                starter_weight = 1.0
            if risk1:
                cliff1_val += starter_weight
                cliff1_count += 1
            if risk2:
                cliff2_val += starter_weight
                cliff2_count += 1

        starter_cliff_1y_pressure = _clamp(cliff1_val / max(1e-9, starter_value_total))
        starter_cliff_2y_pressure = _clamp(cliff2_val / max(1e-9, starter_value_total))
        starter_age_avg = (
            round(sum(starter_age_vals) / max(1, len(starter_age_vals)), 3) if starter_age_vals else ""
        )

        future_need_pressure_1y = _clamp((0.65 * contract_year_pressure) + (0.35 * starter_cliff_1y_pressure))
        future_need_pressure_2y = _clamp((0.55 * free_agent_pressure) + (0.45 * starter_cliff_2y_pressure))

        out[key] = {
            "contract_player_count": total_players,
            "free_agent_pressure": round(free_agent_pressure, 4),
            "contract_year_pressure": round(contract_year_pressure, 4),
            "starter_cliff_1y_pressure": round(starter_cliff_1y_pressure, 4),
            "starter_cliff_2y_pressure": round(starter_cliff_2y_pressure, 4),
            "future_need_pressure_1y": round(future_need_pressure_1y, 4),
            "future_need_pressure_2y": round(future_need_pressure_2y, 4),
            "starter_cliff_1y_count": int(cliff1_count),
            "starter_cliff_2y_count": int(cliff2_count),
            "starter_age_avg": starter_age_avg,
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
    p.add_argument("--espn-depth-charts", type=Path, default=ESPN_DEPTH_CHARTS_PATH)
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
    espn_metrics = _build_espn_depth_chart_metrics(_read_csv_rows(args.espn_depth_charts), rosters, contracts)
    contract_metrics = _build_contract_metrics(contracts, players, target_year=args.target_year)
    deployment_metrics = _build_participation_deployment(participation)

    rows: list[dict] = []
    espn_override_count = 0
    for team in teams:
        for pos in MODEL_POSITIONS:
            espn_r = espn_metrics.get((team, pos), {})
            r = espn_r or roster_metrics.get((team, pos), {})
            c = contract_metrics.get((team, pos), {})
            d = deployment_metrics.get((team, pos), {})
            if espn_r:
                espn_override_count += 1

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
                    "starter_cliff_1y_pressure": c.get("starter_cliff_1y_pressure", ""),
                    "starter_cliff_2y_pressure": c.get("starter_cliff_2y_pressure", ""),
                    "future_need_pressure_1y": c.get("future_need_pressure_1y", ""),
                    "future_need_pressure_2y": c.get("future_need_pressure_2y", ""),
                    "starter_cliff_1y_count": int(c.get("starter_cliff_1y_count", 0)),
                    "starter_cliff_2y_count": int(c.get("starter_cliff_2y_count", 0)),
                    "starter_age_avg": c.get("starter_age_avg", ""),
                    "deployment_share": d.get("deployment_share", ""),
                    "deployment_ratio": d.get("deployment_ratio", ""),
                    "avg_pass_rushers": d.get("avg_pass_rushers", ""),
                    "avg_defenders_in_box": d.get("avg_defenders_in_box", ""),
                    "data_source": str(r.get("depth_chart_source") or "nflverse_rosters_contracts_participation"),
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
        f"- ESPN depth-chart overrides used: `{espn_override_count}`",
        "",
        "## Input Files",
        "",
        f"- rosters: `{args.rosters}` ({rosters.height} rows)",
        f"- contracts: `{args.contracts}` ({contracts.height} rows)",
        f"- players: `{args.players}` ({players.height} rows)",
        f"- participation: `{args.participation}` ({participation.height} rows)",
        f"- espn depth charts: `{args.espn_depth_charts}` ({len(_read_csv_rows(args.espn_depth_charts))} rows)" if args.espn_depth_charts.exists() else f"- espn depth charts: `{args.espn_depth_charts}` (not present)",
        "",
        "## Notes",
        "",
        "- `depth_chart_pressure` combines roster depth/experience and deployment intensity.",
        "- If ESPN depth charts are present, they override roster ordering for starter-quality/depth pressure construction.",
        "- `free_agent_pressure` and `contract_year_pressure` are built from active contract term exposure.",
        "- `starter_cliff_1y_pressure` / `starter_cliff_2y_pressure` capture starter-level age+contract cliff risk.",
        "- `future_need_pressure_1y` / `future_need_pressure_2y` blend contract runway with starter cliff exposure.",
        "- `starter_quality` is roster-based so high pressure does not require poor current quality.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Rows: {len(rows)}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
