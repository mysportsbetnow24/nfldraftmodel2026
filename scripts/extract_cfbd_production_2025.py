#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


CFBD_DIR = ROOT / "data" / "sources" / "cfbd"
BOARD_PATH = ROOT / "data" / "outputs" / "big_board_2026.csv"
OUT_PATH = ROOT / "data" / "sources" / "manual" / "cfb_production_2025.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "cfbd_production_extract_report_2025.txt"

TARGET_POS = {"QB", "WR", "TE", "RB", "EDGE", "CB", "S"}


def _safe_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _pos_map(raw_pos: str) -> str:
    raw = normalize_pos(str(raw_pos or "").strip().upper())
    mapping = {
        "DE": "EDGE",
        "OLB": "EDGE",
        "DL": "DT",
        "NT": "DT",
        "DB": "S",
        "FS": "S",
        "SS": "S",
        "HB": "RB",
    }
    return mapping.get(raw, raw)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _pct_rank(value: float | None, values: list[float]) -> float:
    if value is None or not values:
        return 0.5
    sorted_vals = sorted(values)
    idx = 0
    for i, v in enumerate(sorted_vals):
        if v <= value:
            idx = i
    if len(sorted_vals) == 1:
        return 0.5
    return idx / float(len(sorted_vals) - 1)


def _inv_pct_rank(value: float | None, values: list[float]) -> float:
    # Lower-is-better percentile helper (e.g. defensive PPA/success allowed).
    return 1.0 - _pct_rank(value, values)


def _load_json_data(path: Path) -> list[dict]:
    payload = json.loads(path.read_text())
    return list(payload.get("data", []))


def _season_from_path(path: Path, prefix: str) -> int | None:
    m = re.search(rf"{re.escape(prefix)}_(\d{{4}})\.json$", path.name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _is_meaningful_stat_row(category: str, stat_type: str, value: float) -> bool:
    c = str(category or "").strip().lower()
    s = str(stat_type or "").strip().upper()
    v = float(value)
    thresholds = {
        ("passing", "ATT"): 20.0,
        ("rushing", "CAR"): 25.0,
        ("receiving", "REC"): 8.0,
        ("defensive", "TOT"): 15.0,
        ("defensive", "SOLO"): 8.0,
        ("defensive", "AST"): 6.0,
        ("defensive", "TFL"): 4.0,
        ("defensive", "SACKS"): 1.0,
        ("defensive", "PD"): 2.0,
        ("interceptions", "INT"): 1.0,
    }
    threshold = thresholds.get((c, s))
    if threshold is not None:
        return v >= threshold
    if c in {"passing", "rushing", "receiving", "defensive"} and v >= 30.0:
        return True
    return False


def _build_years_played_index() -> dict[str, dict]:
    """
    Count unique seasons with meaningful participation from available local CFBD files:
    - player_season_stats_<year>.json
    - player_ppa_<year>.json
    - player_usage_<year>.json (optional, if pulled)
    """
    player_years: dict[str, set[int]] = defaultdict(set)
    player_sources: dict[str, set[str]] = defaultdict(set)

    stats_files = sorted(CFBD_DIR.glob("player_season_stats_*.json"))
    ppa_files = sorted(CFBD_DIR.glob("player_ppa_*.json"))
    usage_files = sorted(CFBD_DIR.glob("player_usage_*.json"))

    for path in stats_files:
        year = _season_from_path(path, "player_season_stats")
        if year is None:
            continue
        for row in _load_json_data(path):
            name_key = canonical_player_name(row.get("player", ""))
            if not name_key:
                continue
            stat = _safe_float(row.get("stat"))
            if stat is None:
                continue
            if _is_meaningful_stat_row(row.get("category", ""), row.get("statType", ""), stat):
                player_years[name_key].add(year)
                player_sources[name_key].add("cfbd_player_season_stats")

    for path in ppa_files:
        year = _season_from_path(path, "player_ppa")
        if year is None:
            continue
        for row in _load_json_data(path):
            name_key = canonical_player_name(row.get("name", ""))
            if not name_key:
                continue
            avg = row.get("averagePPA", {}) or {}
            total = row.get("totalPPA", {}) or {}
            avg_all = _safe_float(avg.get("all"))
            total_all = _safe_float(total.get("all"))
            meaningful = False
            if avg_all is not None and abs(avg_all) >= 0.02:
                meaningful = True
            if total_all is not None and abs(total_all) >= 1.0:
                meaningful = True
            if meaningful:
                player_years[name_key].add(year)
                player_sources[name_key].add("cfbd_player_ppa")

    for path in usage_files:
        year = _season_from_path(path, "player_usage")
        if year is None:
            continue
        for row in _load_json_data(path):
            name_key = canonical_player_name(
                row.get("name") or row.get("player") or row.get("playerName") or row.get("athlete") or ""
            )
            if not name_key:
                continue
            meaningful = False
            for key, raw in row.items():
                lk = str(key or "").lower()
                if not any(tok in lk for tok in ("usage", "share", "rate", "snap")):
                    continue
                val = _safe_float(raw)
                if val is None:
                    continue
                if 0.0 <= val <= 1.0 and val >= 0.03:
                    meaningful = True
                    break
                if "snap" in lk and val >= 10:
                    meaningful = True
                    break
                if val >= 3.0:
                    meaningful = True
                    break
            if meaningful:
                player_years[name_key].add(year)
                player_sources[name_key].add("cfbd_player_usage")

    out: dict[str, dict] = {}
    for name_key, years in player_years.items():
        seasons = sorted(int(y) for y in years)
        out[name_key] = {
            "years_played": len(seasons),
            "years_played_seasons": "|".join(str(y) for y in seasons),
            "years_played_source": "|".join(sorted(player_sources.get(name_key, set()))),
        }
    return out


def _load_board() -> dict[str, str]:
    rows = list(csv.DictReader(BOARD_PATH.open()))
    out: dict[str, str] = {}
    for row in rows:
        pos = normalize_pos(row.get("position", ""))
        if pos not in TARGET_POS:
            continue
        key = canonical_player_name(row.get("player_name", ""))
        if key and key not in out:
            out[key] = pos
    return out


def _aggregate_stats(rows: list[dict]) -> tuple[dict[tuple[str, str], dict], dict[str, float]]:
    # rec totals for target-share proxy
    team_rec_total: dict[str, float] = defaultdict(float)
    # aggregate by player+mapped_position
    by_player: dict[tuple[str, str], dict] = {}
    for row in rows:
        name_key = canonical_player_name(row.get("player", ""))
        pos = _pos_map(row.get("position", ""))
        team = str(row.get("team", "")).strip()
        category = str(row.get("category", "")).strip().lower()
        stat_type = str(row.get("statType", "")).strip().upper()
        stat = _safe_float(row.get("stat"))
        if not name_key or stat is None:
            continue

        key = (name_key, pos)
        payload = by_player.setdefault(key, {"team": team})
        payload[f"{category}:{stat_type}"] = stat
        if not payload.get("team"):
            payload["team"] = team

        if category == "receiving" and stat_type == "REC" and team:
            team_rec_total[team] += stat
    return by_player, team_rec_total


def _aggregate_ppa(rows: list[dict]) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for row in rows:
        name_key = canonical_player_name(row.get("name", ""))
        pos = _pos_map(row.get("position", ""))
        if not name_key:
            continue
        avg = row.get("averagePPA", {}) or {}
        total = row.get("totalPPA", {}) or {}
        payload = {
            "avg_all": _safe_float(avg.get("all")),
            "avg_pass": _safe_float(avg.get("pass")),
            "avg_rush": _safe_float(avg.get("rush")),
            "avg_sd": _safe_float(avg.get("standardDowns")),
            "avg_pd": _safe_float(avg.get("passingDowns")),
            "total_all": _safe_float(total.get("all")),
        }
        key = (name_key, pos)
        cur = out.get(key)
        cur_score = _safe_float(cur.get("total_all")) if cur else None
        new_score = payload.get("total_all")
        if cur is None or (new_score is not None and (cur_score is None or abs(new_score) > abs(cur_score))):
            out[key] = payload
    return out


def _get_stat(stats: dict, category: str, stat_type: str) -> float | None:
    return _safe_float(stats.get(f"{category}:{stat_type}"))


def _is_populated(value) -> bool:
    return str(value or "").strip() != ""


def _build_opponent_defense_context(team_adv_rows: list[dict], adv_game_rows: list[dict]) -> dict[str, dict]:
    team_defense: dict[str, dict] = {}
    for row in team_adv_rows:
        team = str(row.get("team", "")).strip()
        if not team:
            continue
        defense = row.get("defense", {}) or {}
        def_ppa = _safe_float(defense.get("ppa"))
        def_success = _safe_float(defense.get("successRate"))
        if def_ppa is None or def_success is None:
            continue
        team_defense[team] = {
            "def_ppa_allowed": def_ppa,
            "def_success_rate_allowed": def_success,
        }

    opps_by_team: dict[str, list[str]] = defaultdict(list)
    for row in adv_game_rows:
        team = str(row.get("team", "")).strip()
        opp = str(row.get("opponent", "")).strip()
        if not team or not opp or team == opp:
            continue
        opps_by_team[team].append(opp)

    raw: dict[str, dict] = {}
    for team, opps in opps_by_team.items():
        opp_ppa_vals: list[float] = []
        opp_sr_vals: list[float] = []
        for opp in opps:
            opp_def = team_defense.get(opp)
            if not opp_def:
                continue
            opp_ppa_vals.append(float(opp_def["def_ppa_allowed"]))
            opp_sr_vals.append(float(opp_def["def_success_rate_allowed"]))
        if not opp_ppa_vals or not opp_sr_vals:
            continue
        raw[team] = {
            "opp_def_ppa_allowed_avg": sum(opp_ppa_vals) / len(opp_ppa_vals),
            "opp_def_success_rate_allowed_avg": sum(opp_sr_vals) / len(opp_sr_vals),
            "games_with_context": len(opp_ppa_vals),
        }

    ppa_pop = [float(v["opp_def_ppa_allowed_avg"]) for v in raw.values()]
    sr_pop = [float(v["opp_def_success_rate_allowed_avg"]) for v in raw.values()]

    out: dict[str, dict] = {}
    for team, payload in raw.items():
        ppa_tough = _inv_pct_rank(float(payload["opp_def_ppa_allowed_avg"]), ppa_pop)
        sr_tough = _inv_pct_rank(float(payload["opp_def_success_rate_allowed_avg"]), sr_pop)
        toughness = _clamp((0.55 * ppa_tough) + (0.45 * sr_tough), 0.0, 1.0)
        # Keep adjustment narrow so this refines production and never rewrites it.
        multiplier = _clamp(1.0 + ((toughness - 0.5) * 0.20), 0.90, 1.10)
        out[team] = {
            **payload,
            "opp_def_toughness_index": round(toughness, 4),
            "opp_def_adjustment_multiplier": round(multiplier, 4),
        }
    return out


def main() -> None:
    player_stats_path = CFBD_DIR / "player_season_stats_2025.json"
    player_ppa_path = CFBD_DIR / "player_ppa_2025.json"
    team_adv_path = CFBD_DIR / "team_advanced_stats_2025.json"
    adv_game_path = CFBD_DIR / "advanced_game_stats_2025.json"
    if not player_stats_path.exists() or not player_ppa_path.exists():
        raise SystemExit("Missing CFBD source files. Pull player_season_stats and player_ppa first.")

    board_map = _load_board()
    stats_rows = _load_json_data(player_stats_path)
    ppa_rows = _load_json_data(player_ppa_path)
    team_adv_rows = _load_json_data(team_adv_path) if team_adv_path.exists() else []
    adv_game_rows = _load_json_data(adv_game_path) if adv_game_path.exists() else []
    stats_by_player, team_rec_totals = _aggregate_stats(stats_rows)
    ppa_by_player = _aggregate_ppa(ppa_rows)
    opp_def_context_by_team = _build_opponent_defense_context(team_adv_rows, adv_game_rows)
    years_played_index = _build_years_played_index()

    # Build percentile pools for proxy rate metrics.
    edge_pressures: list[float] = []
    db_cov: list[float] = []
    for (_, pos), payload in stats_by_player.items():
        if pos == "EDGE":
            p = (_get_stat(payload, "defensive", "QB HUR") or 0.0) + (_get_stat(payload, "defensive", "SACKS") or 0.0)
            edge_pressures.append(p)
        if pos in {"CB", "S"}:
            c = (_get_stat(payload, "defensive", "PD") or 0.0) + (_get_stat(payload, "interceptions", "INT") or 0.0)
            db_cov.append(c)

    out_rows: list[dict] = []
    matched = 0
    for name_key, board_pos in board_map.items():
        stats = stats_by_player.get((name_key, board_pos))
        ppa = ppa_by_player.get((name_key, board_pos))

        # fallback by name if exact position wasn't present in CFBD feed
        if stats is None:
            for pos in (board_pos, "S", "CB", "EDGE", "RB", "WR", "TE", "QB"):
                maybe = stats_by_player.get((name_key, pos))
                if maybe is not None:
                    stats = maybe
                    break
        if ppa is None:
            for pos in (board_pos, "S", "CB", "EDGE", "RB", "WR", "TE", "QB"):
                maybe = ppa_by_player.get((name_key, pos))
                if maybe is not None:
                    ppa = maybe
                    break

        if stats is None and ppa is None:
            continue
        matched += 1

        team = str((stats or {}).get("team", "")).strip()
        row = {
            "player_name": name_key.title(),
            "school": team,
            "position": board_pos,
            "season": 2025,
            "years_played": "",
            "years_played_seasons": "",
            "years_played_source": "",
            "qb_qbr": "",
            "qb_epa_per_play": "",
            "qb_success_rate": "",
            "qb_pressure_to_sack_rate": "",
            "qb_under_pressure_epa": "",
            "qb_under_pressure_success_rate": "",
            "yprr": "",
            "target_share": "",
            "targets_per_route_run": "",
            "explosive_run_rate": "",
            "missed_tackles_forced_per_touch": "",
            "pressure_rate": "",
            "pressures_per_pass_rush_snap": "",
            "sacks_per_pass_rush_snap": "",
            "coverage_plays_per_target": "",
            "yards_allowed_per_coverage_snap": "",
            "opp_def_ppa_allowed_avg": "",
            "opp_def_success_rate_allowed_avg": "",
            "opp_def_toughness_index": "",
            "opp_def_adjustment_multiplier": "",
            "opp_def_context_source": "",
            "cfb_prod_quality_label": "",
            "cfb_prod_reliability": "",
            "cfb_prod_real_features": "",
            "cfb_prod_proxy_features": "",
            "cfb_prod_provenance": "cfbd_stats+cfbd_ppa_proxy",
            "source": "CFBD_2025_proxy_extract",
        }
        yp = years_played_index.get(name_key)
        if yp:
            row["years_played"] = yp.get("years_played", "")
            row["years_played_seasons"] = yp.get("years_played_seasons", "")
            row["years_played_source"] = yp.get("years_played_source", "")

        opp_ctx = opp_def_context_by_team.get(team, {})
        if opp_ctx:
            row["opp_def_ppa_allowed_avg"] = round(float(opp_ctx.get("opp_def_ppa_allowed_avg", 0.0)), 4)
            row["opp_def_success_rate_allowed_avg"] = round(
                float(opp_ctx.get("opp_def_success_rate_allowed_avg", 0.0)),
                4,
            )
            row["opp_def_toughness_index"] = round(float(opp_ctx.get("opp_def_toughness_index", 0.5)), 4)
            row["opp_def_adjustment_multiplier"] = round(
                float(opp_ctx.get("opp_def_adjustment_multiplier", 1.0)),
                4,
            )
            row["opp_def_context_source"] = "cfbd_team_advanced_stats+advanced_game_stats"

        if board_pos == "QB":
            att = _get_stat(stats or {}, "passing", "ATT") or 0.0
            comp = _get_stat(stats or {}, "passing", "COMPLETIONS") or 0.0
            ypa = _get_stat(stats or {}, "passing", "YPA")
            td = _get_stat(stats or {}, "passing", "TD") or 0.0
            ints = _get_stat(stats or {}, "passing", "INT") or 0.0
            ppa_pass = _safe_float((ppa or {}).get("avg_pass"))
            ppa_pd = _safe_float((ppa or {}).get("avg_pd"))
            ppa_sd = _safe_float((ppa or {}).get("avg_sd"))
            comp_pct = (comp / att) if att > 0 else None
            td_rate = (td / att) if att > 0 else None

            if att >= 50:
                qbr_proxy = 50.0
                if ypa is not None:
                    qbr_proxy += (ypa - 7.0) * 6.5
                if td_rate is not None:
                    qbr_proxy += (td_rate - 0.045) * 350.0
                if comp_pct is not None:
                    qbr_proxy += (comp_pct - 0.62) * 55.0
                if ppa_pass is not None:
                    qbr_proxy += ppa_pass * 22.0
                row["qb_qbr"] = round(_clamp(qbr_proxy, 35.0, 92.0), 2)
                if ppa_pass is not None:
                    row["qb_epa_per_play"] = round(ppa_pass, 4)
                if comp_pct is not None:
                    row["qb_success_rate"] = round(_clamp(comp_pct, 0.30, 0.75), 4)
                if ppa_pd is not None:
                    row["qb_under_pressure_epa"] = round(ppa_pd, 4)
                    pressure_success = 0.38 + _clamp(ppa_pd, -0.35, 0.25) * 0.20
                    row["qb_under_pressure_success_rate"] = round(_clamp(pressure_success, 0.22, 0.58), 4)
                if ppa_pd is not None and ppa_sd is not None:
                    # Proxy: bigger drop from standard -> passing downs implies more pressure stress.
                    drop = ppa_sd - ppa_pd
                    p2s = 0.15 + (drop * 0.08)
                    row["qb_pressure_to_sack_rate"] = round(_clamp(p2s, 0.09, 0.30), 4)

        elif board_pos in {"WR", "TE"}:
            rec = _get_stat(stats or {}, "receiving", "REC")
            ypr = _get_stat(stats or {}, "receiving", "YPR")
            if ypr is not None:
                yprr_proxy = 0.35 + (0.14 * ypr)
                row["yprr"] = round(_clamp(yprr_proxy, 0.8, 3.5), 3)
            if rec is not None and team and team_rec_totals.get(team, 0.0) > 0:
                row["target_share"] = round(_clamp(rec / team_rec_totals[team], 0.03, 0.60), 4)
            wr_usage = _safe_float((ppa or {}).get("avg_all"))
            target_share = _safe_float(row.get("target_share"))
            if wr_usage is not None and target_share is not None:
                # Proxy route involvement from overall PPA context when explicit routes are unavailable.
                route_share_proxy = _clamp(0.18 + (wr_usage * 0.45), 0.18, 0.78)
                if route_share_proxy >= 0.45:
                    row["targets_per_route_run"] = round(_clamp(target_share / route_share_proxy, 0.08, 0.38), 4)
                else:
                    row["targets_per_route_run"] = round(_clamp(0.07 + (0.85 * target_share), 0.08, 0.34), 4)

        elif board_pos == "RB":
            ypc = _get_stat(stats or {}, "rushing", "YPC")
            ppa_rush = _safe_float((ppa or {}).get("avg_rush"))
            if ypc is not None:
                base = 0.06 + (ypc - 3.6) * 0.03
                if ppa_rush is not None:
                    base += _clamp(ppa_rush, -0.20, 0.40) * 0.15
                row["explosive_run_rate"] = round(_clamp(base, 0.04, 0.26), 4)

        elif board_pos == "EDGE":
            sacks = (_get_stat(stats or {}, "defensive", "SACKS") or 0.0)
            hurries = (_get_stat(stats or {}, "defensive", "QB HUR") or 0.0)
            pressures = hurries + sacks
            if pressures > 0:
                pr = 0.05 + (0.20 * _pct_rank(pressures, edge_pressures))
                row["pressure_rate"] = round(_clamp(pr, 0.05, 0.26), 4)
                row["pressures_per_pass_rush_snap"] = row["pressure_rate"]
                finish = _clamp((sacks / pressures) if pressures > 0 else 0.20, 0.08, 0.40)
                row["sacks_per_pass_rush_snap"] = round(_clamp(float(pr) * finish, 0.010, 0.075), 4)

        elif board_pos in {"CB", "S"}:
            cov_plays = (_get_stat(stats or {}, "defensive", "PD") or 0.0) + (_get_stat(stats or {}, "interceptions", "INT") or 0.0)
            if cov_plays > 0:
                cpt = 0.06 + (0.24 * _pct_rank(cov_plays, db_cov))
                row["coverage_plays_per_target"] = round(_clamp(cpt, 0.06, 0.32), 4)
                norm = _clamp((float(cpt) - 0.08) / 0.22, 0.0, 1.0)
                row["yards_allowed_per_coverage_snap"] = round(_clamp(1.85 - (1.10 * norm), 0.55, 1.85), 4)

        # This extractor currently produces proxy metrics (not charting-grade true stats).
        if board_pos == "QB":
            proxy_fields = [
                "qb_qbr",
                "qb_epa_per_play",
                "qb_success_rate",
                "qb_pressure_to_sack_rate",
                "qb_under_pressure_epa",
                "qb_under_pressure_success_rate",
            ]
        elif board_pos in {"WR", "TE"}:
            proxy_fields = ["yprr", "target_share", "targets_per_route_run"]
        elif board_pos == "RB":
            proxy_fields = ["explosive_run_rate", "missed_tackles_forced_per_touch"]
        elif board_pos == "EDGE":
            proxy_fields = ["pressure_rate", "pressures_per_pass_rush_snap", "sacks_per_pass_rush_snap"]
        elif board_pos in {"CB", "S"}:
            proxy_fields = ["coverage_plays_per_target", "yards_allowed_per_coverage_snap"]
        else:
            proxy_fields = []

        proxy_count = sum(1 for f in proxy_fields if _is_populated(row.get(f)))
        real_count = 0
        if proxy_count > 0:
            quality_label = "proxy"
            reliability = round(min(0.60, 0.35 + (0.06 * proxy_count)), 2)
        else:
            quality_label = "missing"
            reliability = 0.0
        row["cfb_prod_quality_label"] = quality_label
        row["cfb_prod_reliability"] = reliability
        row["cfb_prod_real_features"] = real_count
        row["cfb_prod_proxy_features"] = proxy_count

        out_rows.append(row)

    out_rows.sort(key=lambda r: (r["position"], r["player_name"]))
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "player_name",
                "school",
                "position",
                "season",
                "years_played",
                "years_played_seasons",
                "years_played_source",
                "qb_qbr",
                "qb_epa_per_play",
                "qb_success_rate",
                "qb_pressure_to_sack_rate",
                "qb_under_pressure_epa",
                "qb_under_pressure_success_rate",
                "yprr",
                "target_share",
                "targets_per_route_run",
                "explosive_run_rate",
                "missed_tackles_forced_per_touch",
                "pressure_rate",
                "pressures_per_pass_rush_snap",
                "sacks_per_pass_rush_snap",
                "coverage_plays_per_target",
                "yards_allowed_per_coverage_snap",
                "opp_def_ppa_allowed_avg",
                "opp_def_success_rate_allowed_avg",
                "opp_def_toughness_index",
                "opp_def_adjustment_multiplier",
                "opp_def_context_source",
                "cfb_prod_quality_label",
                "cfb_prod_reliability",
                "cfb_prod_real_features",
                "cfb_prod_proxy_features",
                "cfb_prod_provenance",
                "source",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    by_pos = defaultdict(int)
    quality_counts = defaultdict(int)
    opp_ctx_rows = 0
    years_played_rows = 0
    for r in out_rows:
        by_pos[r["position"]] += 1
        quality_counts[r.get("cfb_prod_quality_label", "unknown")] += 1
        if _is_populated(r.get("opp_def_toughness_index")):
            opp_ctx_rows += 1
        if _is_populated(r.get("years_played")):
            years_played_rows += 1

    lines = [
        "CFBD 2025 Production Extraction Report",
        "",
        f"board_players_targeted: {len(board_map)}",
        f"players_matched_cfbd: {matched}",
        f"rows_written: {len(out_rows)}",
        f"rows_with_opp_def_context: {opp_ctx_rows}",
        f"rows_with_years_played: {years_played_rows}",
        "notes: CFBD does not expose true YPRR/targets/missed_tackles/coverage_targets directly; proxy fields were generated.",
        "",
        "rows_by_position:",
    ]
    for pos in sorted(by_pos):
        lines.append(f"- {pos}: {by_pos[pos]}")
    lines.append("")
    lines.append("quality_counts:")
    for quality in sorted(quality_counts):
        lines.append(f"- {quality}: {quality_counts[quality]}")
    REPORT_PATH.write_text("\n".join(lines))

    print(f"Wrote: {OUT_PATH}")
    print(f"Report: {REPORT_PATH}")
    print(f"Rows: {len(out_rows)}")


if __name__ == "__main__":
    main()
