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

TARGET_POS = {"QB", "WR", "TE", "RB", "EDGE", "DT", "LB", "CB", "S", "OT", "IOL"}


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


def _aggregate_usage(rows: list[dict]) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for row in rows:
        name_key = canonical_player_name(row.get("name", ""))
        pos = _pos_map(row.get("position", ""))
        if not name_key:
            continue
        usage = row.get("usage", {}) or {}
        payload = {
            "overall": _safe_float(usage.get("overall")),
            "pass": _safe_float(usage.get("pass")),
            "rush": _safe_float(usage.get("rush")),
            "firstDown": _safe_float(usage.get("firstDown")),
            "secondDown": _safe_float(usage.get("secondDown")),
            "thirdDown": _safe_float(usage.get("thirdDown")),
            "standardDowns": _safe_float(usage.get("standardDowns")),
            "passingDowns": _safe_float(usage.get("passingDowns")),
        }
        key = (name_key, pos)
        cur = out.get(key)
        cur_score = _safe_float(cur.get("overall")) if cur else None
        new_score = payload.get("overall")
        if cur is None or (new_score is not None and (cur_score is None or abs(new_score) > abs(cur_score))):
            out[key] = payload
    return out


def _aggregate_adjusted_player_metrics(rows: list[dict]) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for row in rows:
        athlete = row.get("athlete", {}) or {}
        name_key = canonical_player_name(athlete.get("name", ""))
        pos = _pos_map(((athlete.get("position") or {}).get("abbreviation", "")))
        metric_type = str(row.get("metricType", "")).strip().lower()
        metric_value = _safe_float(row.get("metricValue"))
        plays = _safe_float(row.get("plays"))
        if not name_key or metric_value is None:
            continue
        payload = out.setdefault((name_key, pos), {})
        if metric_type == "passing":
            payload["adj_passing"] = metric_value
            payload["adj_passing_plays"] = int(round(plays)) if plays is not None else ""
        elif metric_type == "rushing":
            payload["adj_rushing"] = metric_value
            payload["adj_rushing_plays"] = int(round(plays)) if plays is not None else ""
        elif metric_type == "field_goals":
            payload["adj_field_goals"] = metric_value
            payload["adj_field_goal_plays"] = int(round(plays)) if plays is not None else ""
    return out


def _get_stat(stats: dict, category: str, stat_type: str) -> float | None:
    return _safe_float(stats.get(f"{category}:{stat_type}"))


def _is_populated(value) -> bool:
    return str(value or "").strip() != ""


def _count_populated(payload: dict, keys: list[str]) -> int:
    return sum(1 for key in keys if _is_populated(payload.get(key)))


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


def _build_team_defense_lookup(team_adv_rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in team_adv_rows:
        team = str(row.get("team", "")).strip()
        defense = row.get("defense", {}) or {}
        def_ppa = _safe_float(defense.get("ppa"))
        def_success = _safe_float(defense.get("successRate"))
        if not team or def_ppa is None:
            continue
        out[team] = {
            "def_ppa_allowed": def_ppa,
            "def_success_rate_allowed": def_success,
        }
    return out


def _consistency_index(values: list[float]) -> float | None:
    vals = [float(v) for v in values if v is not None]
    if len(vals) < 3:
        return None
    mean = sum(vals) / len(vals)
    if abs(mean) < 1e-9:
        return None
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(var)
    cv = std / max(abs(mean), 0.05)
    return _clamp(1.0 - (cv / 1.25), 0.0, 1.0)


def _late_trend_index(weekly_pairs: list[tuple[int, float]]) -> float | None:
    vals = [(int(w), float(v)) for w, v in weekly_pairs if v is not None]
    if len(vals) < 4:
        return None
    vals.sort(key=lambda x: x[0])
    split = max(2, len(vals) // 2)
    early = [v for _, v in vals[:-split]]
    late = [v for _, v in vals[-split:]]
    if not early or not late:
        return None
    early_avg = sum(early) / len(early)
    late_avg = sum(late) / len(late)
    denom = max(abs(early_avg), 0.05)
    return _clamp((late_avg - early_avg) / denom, -1.0, 1.0)


def _top_defense_threshold(team_defense_lookup: dict[str, dict]) -> float | None:
    vals = sorted(float(v["def_ppa_allowed"]) for v in team_defense_lookup.values() if v.get("def_ppa_allowed") is not None)
    if len(vals) < 8:
        return None
    idx = max(0, min(len(vals) - 1, int(round((len(vals) - 1) * 0.25))))
    return vals[idx]


def _top_defense_performance_index(
    weekly_pairs: list[tuple[int, str, float]],
    team_defense_lookup: dict[str, dict],
    top_def_threshold: float | None,
) -> tuple[float | None, int]:
    if top_def_threshold is None:
        return None, 0
    vals = [float(v) for _, _, v in weekly_pairs if v is not None]
    if len(vals) < 3:
        return None, 0
    season_avg = sum(vals) / len(vals)
    if abs(season_avg) < 1e-9:
        return None, 0
    top_vals: list[float] = []
    for _, opponent, value in weekly_pairs:
        opp = team_defense_lookup.get(str(opponent or "").strip())
        if not opp:
            continue
        def_ppa = _safe_float(opp.get("def_ppa_allowed"))
        if def_ppa is None or def_ppa > float(top_def_threshold):
            continue
        top_vals.append(float(value))
    if not top_vals:
        return None, 0
    return _clamp((sum(top_vals) / len(top_vals)) / max(abs(season_avg), 0.05), 0.0, 1.5), len(top_vals)


def _aggregate_player_ppa_games(rows: list[dict], team_defense_lookup: dict[str, dict]) -> dict[tuple[str, str], dict]:
    weekly: dict[tuple[str, str], list[tuple[int, str, float]]] = defaultdict(list)
    top_def_threshold = _top_defense_threshold(team_defense_lookup)
    for row in rows:
        name_key = canonical_player_name(row.get("name", ""))
        pos = _pos_map(row.get("position", ""))
        week = int(_safe_float(row.get("week")) or 0)
        opp = str(row.get("opponent", "")).strip()
        avg = row.get("averagePPA", {}) or {}
        metric = None
        if pos == "QB":
            metric = _safe_float(avg.get("pass"))
            if metric is None:
                metric = _safe_float(avg.get("all"))
        elif pos == "RB":
            metric = _safe_float(avg.get("rush"))
            if metric is None:
                metric = _safe_float(avg.get("all"))
        elif pos in {"WR", "TE"}:
            metric = _safe_float(avg.get("pass"))
            if metric is None:
                metric = _safe_float(avg.get("all"))
        if not name_key or not pos or metric is None or week <= 0:
            continue
        weekly[(name_key, pos)].append((week, opp, metric))

    out: dict[tuple[str, str], dict] = {}
    for key, samples in weekly.items():
        ordered = sorted(samples, key=lambda x: x[0])
        vals = [v for _, _, v in ordered]
        consistency = _consistency_index(vals)
        late = _late_trend_index([(w, v) for w, _, v in ordered])
        top_def, top_games = _top_defense_performance_index(ordered, team_defense_lookup, top_def_threshold)
        out[key] = {
            "game_consistency_index": round(consistency, 4) if consistency is not None else "",
            "late_season_trend_index": round(late, 4) if late is not None else "",
            "top_defense_performance_index": round(top_def, 4) if top_def is not None else "",
            "top_defense_games": top_games,
            "weekly_sample_games": len(ordered),
            "game_context_source": "cfbd_player_ppa_games",
        }
    return out


def _parse_nested_stat(category: str, stat_type: str, raw_stat: str) -> dict[str, float]:
    category = str(category or "").strip().lower()
    stat_type = str(stat_type or "").strip().upper()
    raw = str(raw_stat or "").strip()
    if not raw:
        return {}
    if category == "passing" and stat_type == "C/ATT" and "/" in raw:
        left, right = raw.split("/", 1)
        comp = _safe_float(left)
        att = _safe_float(right)
        out = {}
        if comp is not None:
            out["passing:COMPLETIONS"] = comp
        if att is not None:
            out["passing:ATT"] = att
        return out
    val = _safe_float(raw)
    if val is None:
        return {}
    key = f"{category}:{stat_type}"
    return {key: val}


def _defensive_game_score(position: str, stats: dict) -> float | None:
    tackles = _safe_float(stats.get("defensive:TOT")) or 0.0
    tfl = _safe_float(stats.get("defensive:TFL")) or 0.0
    sacks = _safe_float(stats.get("defensive:SACKS")) or 0.0
    hurries = _safe_float(stats.get("defensive:QB HUR")) or 0.0
    pbu = _safe_float(stats.get("defensive:PD")) or 0.0
    ints = _safe_float(stats.get("interceptions:INT")) or 0.0
    if position in {"EDGE", "DT"}:
        return (4.0 * sacks) + (1.0 * hurries) + (1.25 * tfl) + (0.08 * tackles)
    if position == "LB":
        return (0.08 * tackles) + (1.25 * tfl) + (3.5 * sacks) + (0.8 * hurries) + (1.2 * pbu) + (3.0 * ints)
    if position in {"CB", "S"}:
        return (3.0 * ints) + (1.1 * pbu) + (0.06 * tackles) + (1.0 * tfl)
    return None


def _aggregate_defense_game_box_scores(
    rows: list[dict],
    board_map: dict[str, str],
    team_defense_lookup: dict[str, dict],
) -> dict[tuple[str, str], dict]:
    weekly_stats: dict[tuple[str, str, int], dict] = {}
    top_def_threshold = _top_defense_threshold(team_defense_lookup)
    for game in rows:
        teams = game.get("teams", []) or []
        if len(teams) < 1:
            continue
        all_team_names = [str(t.get("team", "")).strip() for t in teams]
        week = int(_safe_float(game.get("_cfbd_pull_week")) or 0)
        if week <= 0:
            continue
        for team_entry in teams:
            team = str(team_entry.get("team", "")).strip()
            opponent = next((x for x in all_team_names if x and x != team), "")
            for category in team_entry.get("categories", []) or []:
                cname = str(category.get("name", "")).strip()
                for stat_type in category.get("types", []) or []:
                    tname = str(stat_type.get("name", "")).strip()
                    for athlete in stat_type.get("athletes", []) or []:
                        name_key = canonical_player_name(athlete.get("name", ""))
                        if not name_key or name_key == "team":
                            continue
                        pos = board_map.get(name_key, "")
                        if pos not in {"EDGE", "DT", "LB", "CB", "S"}:
                            continue
                        key = (name_key, pos, week)
                        payload = weekly_stats.setdefault(key, {"opponent": opponent})
                        payload.update(_parse_nested_stat(cname, tname, athlete.get("stat", "")))

    by_player: dict[tuple[str, str], list[tuple[int, str, float]]] = defaultdict(list)
    for (name_key, pos, week), stats in weekly_stats.items():
        score = _defensive_game_score(pos, stats)
        if score is None:
            continue
        by_player[(name_key, pos)].append((week, str(stats.get("opponent", "")), score))

    out: dict[tuple[str, str], dict] = {}
    for key, samples in by_player.items():
        ordered = sorted(samples, key=lambda x: x[0])
        vals = [v for _, _, v in ordered]
        consistency = _consistency_index(vals)
        late = _late_trend_index([(w, v) for w, _, v in ordered])
        top_def, top_games = _top_defense_performance_index(ordered, team_defense_lookup, top_def_threshold)
        out[key] = {
            "game_consistency_index": round(consistency, 4) if consistency is not None else "",
            "late_season_trend_index": round(late, 4) if late is not None else "",
            "top_defense_performance_index": round(top_def, 4) if top_def is not None else "",
            "top_defense_games": top_games,
            "weekly_sample_games": len(ordered),
            "game_context_source": "cfbd_game_player_stats_box",
        }
    return out


def main() -> None:
    player_stats_path = CFBD_DIR / "player_season_stats_2025.json"
    player_ppa_path = CFBD_DIR / "player_ppa_2025.json"
    player_usage_path = CFBD_DIR / "player_usage_2025.json"
    game_player_stats_path = CFBD_DIR / "game_player_stats_2025.json"
    adjusted_player_metrics_path = CFBD_DIR / "adjusted_player_metrics_2025.json"
    team_adv_path = CFBD_DIR / "team_advanced_stats_2025.json"
    adv_game_path = CFBD_DIR / "advanced_game_stats_2025.json"
    if not player_stats_path.exists() or not player_ppa_path.exists():
        raise SystemExit("Missing CFBD source files. Pull player_season_stats and player_ppa first.")

    board_map = _load_board()
    stats_rows = _load_json_data(player_stats_path)
    ppa_rows = _load_json_data(player_ppa_path)
    usage_rows = _load_json_data(player_usage_path) if player_usage_path.exists() else []
    game_player_rows = _load_json_data(game_player_stats_path) if game_player_stats_path.exists() else []
    adjusted_metric_rows = _load_json_data(adjusted_player_metrics_path) if adjusted_player_metrics_path.exists() else []
    player_ppa_game_path = CFBD_DIR / "player_ppa_games_2025.json"
    player_ppa_game_rows = _load_json_data(player_ppa_game_path) if player_ppa_game_path.exists() else []
    team_adv_rows = _load_json_data(team_adv_path) if team_adv_path.exists() else []
    adv_game_rows = _load_json_data(adv_game_path) if adv_game_path.exists() else []
    stats_by_player, team_rec_totals = _aggregate_stats(stats_rows)
    ppa_by_player = _aggregate_ppa(ppa_rows)
    usage_by_player = _aggregate_usage(usage_rows)
    adjusted_by_player = _aggregate_adjusted_player_metrics(adjusted_metric_rows)
    opp_def_context_by_team = _build_opponent_defense_context(team_adv_rows, adv_game_rows)
    team_defense_lookup = _build_team_defense_lookup(team_adv_rows)
    offense_game_context = _aggregate_player_ppa_games(player_ppa_game_rows, team_defense_lookup)
    defense_game_context = _aggregate_defense_game_box_scores(game_player_rows, board_map, team_defense_lookup)
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
        usage = usage_by_player.get((name_key, board_pos))
        adjusted = adjusted_by_player.get((name_key, board_pos))

        # fallback by name if exact position wasn't present in CFBD feed
        if stats is None:
            for pos in (board_pos, "LB", "OT", "IOL", "S", "CB", "EDGE", "RB", "WR", "TE", "QB"):
                maybe = stats_by_player.get((name_key, pos))
                if maybe is not None:
                    stats = maybe
                    break
        if ppa is None:
            for pos in (board_pos, "LB", "OT", "IOL", "S", "CB", "EDGE", "RB", "WR", "TE", "QB"):
                maybe = ppa_by_player.get((name_key, pos))
                if maybe is not None:
                    ppa = maybe
                    break
        if usage is None:
            for pos in (board_pos, "LB", "OT", "IOL", "S", "CB", "EDGE", "RB", "WR", "TE", "QB"):
                maybe = usage_by_player.get((name_key, pos))
                if maybe is not None:
                    usage = maybe
                    break
        if adjusted is None:
            for pos in (board_pos, "LB", "OT", "IOL", "S", "CB", "EDGE", "RB", "WR", "TE", "QB"):
                maybe = adjusted_by_player.get((name_key, pos))
                if maybe is not None:
                    adjusted = maybe
                    break

        yp = years_played_index.get(name_key)
        if stats is None and ppa is None and usage is None and adjusted is None and yp is None:
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
            "qb_pass_att": "",
            "qb_pass_comp": "",
            "qb_pass_yds": "",
            "qb_pass_td": "",
            "qb_pass_int": "",
            "qb_rush_yds": "",
            "qb_rush_td": "",
            "wrte_rec": "",
            "wrte_rec_yds": "",
            "wrte_rec_td": "",
            "rb_rush_att": "",
            "rb_rush_yds": "",
            "rb_rush_td": "",
            "rb_rec": "",
            "rb_rec_yds": "",
            "rb_rec_td": "",
            "edge_sacks": "",
            "edge_qb_hurries": "",
            "edge_tfl": "",
            "edge_tackles": "",
            "db_int": "",
            "db_pbu": "",
            "db_tackles": "",
            "db_tfl": "",
            "opp_def_ppa_allowed_avg": "",
            "opp_def_success_rate_allowed_avg": "",
            "opp_def_toughness_index": "",
            "opp_def_adjustment_multiplier": "",
            "opp_def_context_source": "",
            "qb_ppa_overall": "",
            "qb_ppa_passing": "",
            "qb_ppa_standard_downs": "",
            "qb_ppa_passing_downs": "",
            "qb_wepa_passing": "",
            "qb_usage_rate": "",
            "qb_adjusted_passing": "",
            "qb_adjusted_rushing": "",
            "qb_adjusted_total": "",
            "wrte_ppa_overall": "",
            "wrte_ppa_passing_downs": "",
            "wrte_wepa_receiving": "",
            "wrte_usage_rate": "",
            "rb_ppa_rushing": "",
            "rb_ppa_standard_downs": "",
            "rb_wepa_rushing": "",
            "rb_usage_rate": "",
            "rb_adjusted_rushing": "",
            "lb_tackles": "",
            "lb_tfl": "",
            "lb_sacks": "",
            "lb_qb_hurries": "",
            "lb_pbu": "",
            "lb_int": "",
            "lb_usage_rate": "",
            "ol_starts": "",
            "ol_usage_rate": "",
            "game_consistency_index": "",
            "late_season_trend_index": "",
            "top_defense_performance_index": "",
            "top_defense_games": "",
            "weekly_sample_games": "",
            "game_context_source": "",
            "cfb_prod_quality_label": "",
            "cfb_prod_reliability": "",
            "cfb_prod_real_features": "",
            "cfb_prod_proxy_features": "",
            "cfb_prod_provenance": "cfbd_stats+cfbd_ppa+cfbd_usage",
            "source": "CFBD_2025_proxy_extract",
        }
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

        game_ctx = offense_game_context.get((name_key, board_pos)) or defense_game_context.get((name_key, board_pos)) or {}
        if game_ctx:
            row["game_consistency_index"] = game_ctx.get("game_consistency_index", "")
            row["late_season_trend_index"] = game_ctx.get("late_season_trend_index", "")
            row["top_defense_performance_index"] = game_ctx.get("top_defense_performance_index", "")
            row["top_defense_games"] = game_ctx.get("top_defense_games", "")
            row["weekly_sample_games"] = game_ctx.get("weekly_sample_games", "")
            row["game_context_source"] = game_ctx.get("game_context_source", "")

        if adjusted:
            if board_pos == "QB":
                adj_pass = _safe_float(adjusted.get("adj_passing"))
                adj_rush = _safe_float(adjusted.get("adj_rushing"))
                total_parts = []
                if adj_pass is not None:
                    row["qb_adjusted_passing"] = round(adj_pass, 4)
                    total_parts.append((float(adjusted.get("adj_passing_plays") or 0), adj_pass))
                if adj_rush is not None:
                    row["qb_adjusted_rushing"] = round(adj_rush, 4)
                    total_parts.append((float(adjusted.get("adj_rushing_plays") or 0), adj_rush))
                if total_parts:
                    total_weight = sum(max(1.0, weight) for weight, _ in total_parts)
                    blended = sum(max(1.0, weight) * value for weight, value in total_parts) / total_weight
                    row["qb_adjusted_total"] = round(blended, 4)
            elif board_pos == "RB":
                adj_rush = _safe_float(adjusted.get("adj_rushing"))
                if adj_rush is not None:
                    row["rb_adjusted_rushing"] = round(adj_rush, 4)
        if adjusted:
            row["cfb_prod_provenance"] = f"{row['cfb_prod_provenance']}+cfbd_adjusted_metrics"

        if board_pos == "QB":
            att = _get_stat(stats or {}, "passing", "ATT") or 0.0
            comp = _get_stat(stats or {}, "passing", "COMPLETIONS") or 0.0
            ypa = _get_stat(stats or {}, "passing", "YPA")
            td = _get_stat(stats or {}, "passing", "TD") or 0.0
            ints = _get_stat(stats or {}, "passing", "INT") or 0.0
            pass_yds = _get_stat(stats or {}, "passing", "YDS")
            rush_yds = _get_stat(stats or {}, "rushing", "YDS")
            rush_td = _get_stat(stats or {}, "rushing", "TD")
            ppa_pass = _safe_float((ppa or {}).get("avg_pass"))
            ppa_pd = _safe_float((ppa or {}).get("avg_pd"))
            ppa_sd = _safe_float((ppa or {}).get("avg_sd"))
            ppa_overall = _safe_float((ppa or {}).get("avg_all"))
            usage_overall = _safe_float((usage or {}).get("overall"))
            comp_pct = (comp / att) if att > 0 else None
            td_rate = (td / att) if att > 0 else None
            if att > 0:
                row["qb_pass_att"] = int(round(att))
            if comp > 0:
                row["qb_pass_comp"] = int(round(comp))
            if pass_yds is not None:
                row["qb_pass_yds"] = int(round(pass_yds))
            if td > 0:
                row["qb_pass_td"] = int(round(td))
            if ints >= 0:
                row["qb_pass_int"] = int(round(ints))
            if rush_yds is not None:
                row["qb_rush_yds"] = int(round(rush_yds))
            if rush_td is not None:
                row["qb_rush_td"] = int(round(rush_td))
            if ppa_overall is not None:
                row["qb_ppa_overall"] = round(ppa_overall, 4)
            if ppa_pass is not None:
                row["qb_ppa_passing"] = round(ppa_pass, 4)
            if ppa_sd is not None:
                row["qb_ppa_standard_downs"] = round(ppa_sd, 4)
            if ppa_pd is not None:
                row["qb_ppa_passing_downs"] = round(ppa_pd, 4)
            if usage_overall is not None:
                row["qb_usage_rate"] = round(usage_overall, 4)

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
            rec_yds = _get_stat(stats or {}, "receiving", "YDS")
            rec_td = _get_stat(stats or {}, "receiving", "TD")
            usage_overall = _safe_float((usage or {}).get("overall"))
            usage_pass = _safe_float((usage or {}).get("pass"))
            ppa_overall = _safe_float((ppa or {}).get("avg_all"))
            ppa_pd = _safe_float((ppa or {}).get("avg_pd"))
            if rec is not None:
                row["wrte_rec"] = int(round(rec))
            if rec_yds is not None:
                row["wrte_rec_yds"] = int(round(rec_yds))
            if rec_td is not None:
                row["wrte_rec_td"] = int(round(rec_td))
            if ppa_overall is not None:
                row["wrte_ppa_overall"] = round(ppa_overall, 4)
            if ppa_pd is not None:
                row["wrte_ppa_passing_downs"] = round(ppa_pd, 4)
            if usage_overall is not None:
                row["wrte_usage_rate"] = round(usage_overall, 4)
            if ypr is not None:
                yprr_proxy = 0.35 + (0.14 * ypr)
                row["yprr"] = round(_clamp(yprr_proxy, 0.8, 3.5), 3)
            if usage_pass is not None:
                row["target_share"] = round(_clamp(usage_pass, 0.03, 0.60), 4)
            elif rec is not None and team and team_rec_totals.get(team, 0.0) > 0:
                row["target_share"] = round(_clamp(rec / team_rec_totals[team], 0.03, 0.60), 4)
            wr_usage = ppa_overall
            target_share = _safe_float(row.get("target_share"))
            if wr_usage is not None and target_share is not None:
                # Proxy route involvement from overall PPA context when explicit routes are unavailable.
                route_share_proxy = _clamp(0.18 + (wr_usage * 0.45), 0.18, 0.78)
                if route_share_proxy >= 0.45:
                    row["targets_per_route_run"] = round(_clamp(target_share / route_share_proxy, 0.08, 0.38), 4)
                else:
                    row["targets_per_route_run"] = round(_clamp(0.07 + (0.85 * target_share), 0.08, 0.34), 4)

        elif board_pos == "RB":
            car = _get_stat(stats or {}, "rushing", "CAR")
            yds = _get_stat(stats or {}, "rushing", "YDS")
            td = _get_stat(stats or {}, "rushing", "TD")
            rec = _get_stat(stats or {}, "receiving", "REC")
            rec_yds = _get_stat(stats or {}, "receiving", "YDS")
            rec_td = _get_stat(stats or {}, "receiving", "TD")
            usage_overall = _safe_float((usage or {}).get("overall"))
            usage_rush = _safe_float((usage or {}).get("rush"))
            ppa_rush = _safe_float((ppa or {}).get("avg_rush"))
            ppa_sd = _safe_float((ppa or {}).get("avg_sd"))
            if car is not None:
                row["rb_rush_att"] = int(round(car))
            if yds is not None:
                row["rb_rush_yds"] = int(round(yds))
            if td is not None:
                row["rb_rush_td"] = int(round(td))
            if rec is not None:
                row["rb_rec"] = int(round(rec))
            if rec_yds is not None:
                row["rb_rec_yds"] = int(round(rec_yds))
            if rec_td is not None:
                row["rb_rec_td"] = int(round(rec_td))
            if ppa_rush is not None:
                row["rb_ppa_rushing"] = round(ppa_rush, 4)
            if ppa_sd is not None:
                row["rb_ppa_standard_downs"] = round(ppa_sd, 4)
            if usage_rush is not None:
                row["rb_usage_rate"] = round(usage_rush, 4)
            elif usage_overall is not None:
                row["rb_usage_rate"] = round(usage_overall, 4)
            ypc = _get_stat(stats or {}, "rushing", "YPC")
            if ypc is not None:
                base = 0.06 + (ypc - 3.6) * 0.03
                if ppa_rush is not None:
                    base += _clamp(ppa_rush, -0.20, 0.40) * 0.15
                row["explosive_run_rate"] = round(_clamp(base, 0.04, 0.26), 4)

        elif board_pos in {"EDGE", "DT"}:
            sacks = (_get_stat(stats or {}, "defensive", "SACKS") or 0.0)
            hurries = (_get_stat(stats or {}, "defensive", "QB HUR") or 0.0)
            tfl = (_get_stat(stats or {}, "defensive", "TFL") or 0.0)
            tackles = (_get_stat(stats or {}, "defensive", "TOT") or 0.0)
            if sacks > 0:
                row["edge_sacks"] = int(round(sacks))
            if hurries > 0:
                row["edge_qb_hurries"] = int(round(hurries))
            if tfl > 0:
                row["edge_tfl"] = int(round(tfl))
            if tackles > 0:
                row["edge_tackles"] = int(round(tackles))
            pressures = hurries + sacks
            if pressures > 0:
                pr = 0.05 + (0.20 * _pct_rank(pressures, edge_pressures))
                row["pressure_rate"] = round(_clamp(pr, 0.05, 0.26), 4)
                row["pressures_per_pass_rush_snap"] = row["pressure_rate"]
                finish = _clamp((sacks / pressures) if pressures > 0 else 0.20, 0.08, 0.40)
                row["sacks_per_pass_rush_snap"] = round(_clamp(float(pr) * finish, 0.010, 0.075), 4)

        elif board_pos in {"CB", "S"}:
            cov_plays = (_get_stat(stats or {}, "defensive", "PD") or 0.0) + (_get_stat(stats or {}, "interceptions", "INT") or 0.0)
            ints = (_get_stat(stats or {}, "interceptions", "INT") or 0.0)
            pbu = (_get_stat(stats or {}, "defensive", "PD") or 0.0)
            tackles = (_get_stat(stats or {}, "defensive", "TOT") or 0.0)
            tfl = (_get_stat(stats or {}, "defensive", "TFL") or 0.0)
            if ints > 0:
                row["db_int"] = int(round(ints))
            if pbu > 0:
                row["db_pbu"] = int(round(pbu))
            if tackles > 0:
                row["db_tackles"] = int(round(tackles))
            if tfl > 0:
                row["db_tfl"] = int(round(tfl))
            if cov_plays > 0:
                cpt = 0.06 + (0.24 * _pct_rank(cov_plays, db_cov))
                row["coverage_plays_per_target"] = round(_clamp(cpt, 0.06, 0.32), 4)
                norm = _clamp((float(cpt) - 0.08) / 0.22, 0.0, 1.0)
                row["yards_allowed_per_coverage_snap"] = round(_clamp(1.85 - (1.10 * norm), 0.55, 1.85), 4)

        elif board_pos == "LB":
            tackles = (_get_stat(stats or {}, "defensive", "TOT") or 0.0)
            tfl = (_get_stat(stats or {}, "defensive", "TFL") or 0.0)
            sacks = (_get_stat(stats or {}, "defensive", "SACKS") or 0.0)
            hurries = (_get_stat(stats or {}, "defensive", "QB HUR") or 0.0)
            pbu = (_get_stat(stats or {}, "defensive", "PD") or 0.0)
            ints = (_get_stat(stats or {}, "interceptions", "INT") or 0.0)
            if tackles > 0:
                row["lb_tackles"] = int(round(tackles))
            if tfl > 0:
                row["lb_tfl"] = int(round(tfl))
            if sacks > 0:
                row["lb_sacks"] = int(round(sacks))
            if hurries > 0:
                row["lb_qb_hurries"] = int(round(hurries))
            if pbu > 0:
                row["lb_pbu"] = int(round(pbu))
            if ints > 0:
                row["lb_int"] = int(round(ints))

        elif board_pos in {"OT", "IOL"}:
            usage_overall = _safe_float((usage or {}).get("overall"))
            if usage_overall is not None:
                row["ol_usage_rate"] = round(usage_overall, 4)

        # Proxy metrics are useful, but explicit counting stats should be recognized
        # as real evidence so downstream concern text doesn't overstate proxy risk.
        if board_pos == "QB":
            proxy_fields = [
                "qb_qbr",
                "qb_epa_per_play",
                "qb_success_rate",
                "qb_pressure_to_sack_rate",
                "qb_under_pressure_epa",
                "qb_under_pressure_success_rate",
                "qb_adjusted_passing",
                "qb_adjusted_rushing",
                "qb_adjusted_total",
            ]
            real_fields = [
                "qb_pass_att",
                "qb_pass_comp",
                "qb_pass_yds",
                "qb_pass_td",
                "qb_pass_int",
                "qb_rush_yds",
                "qb_rush_td",
            ]
        elif board_pos in {"WR", "TE"}:
            proxy_fields = ["yprr", "target_share", "targets_per_route_run"]
            real_fields = ["wrte_rec", "wrte_rec_yds", "wrte_rec_td"]
        elif board_pos == "RB":
            proxy_fields = ["explosive_run_rate", "missed_tackles_forced_per_touch", "rb_adjusted_rushing"]
            real_fields = ["rb_rush_att", "rb_rush_yds", "rb_rush_td", "rb_rec", "rb_rec_yds", "rb_rec_td"]
        elif board_pos in {"EDGE", "DT"}:
            proxy_fields = ["pressure_rate", "pressures_per_pass_rush_snap", "sacks_per_pass_rush_snap"]
            real_fields = ["edge_sacks", "edge_qb_hurries", "edge_tfl", "edge_tackles"]
        elif board_pos == "LB":
            proxy_fields = ["lb_usage_rate"]
            real_fields = ["lb_tackles", "lb_tfl", "lb_sacks", "lb_qb_hurries", "lb_pbu", "lb_int"]
        elif board_pos in {"CB", "S"}:
            proxy_fields = ["coverage_plays_per_target", "yards_allowed_per_coverage_snap"]
            real_fields = ["db_int", "db_pbu", "db_tackles", "db_tfl"]
        elif board_pos in {"OT", "IOL"}:
            proxy_fields = ["ol_usage_rate"]
            real_fields = ["years_played", "ol_starts"]
        else:
            proxy_fields = []
            real_fields = []

        proxy_count = sum(1 for f in proxy_fields if _is_populated(row.get(f)))
        real_count = _count_populated(row, real_fields)
        if real_count >= 2 and proxy_count >= 1:
            quality_label = "mixed"
            reliability = round(min(0.82, 0.56 + (0.05 * real_count) + (0.03 * proxy_count)), 2)
        elif real_count >= 2:
            quality_label = "real"
            reliability = round(min(0.90, 0.62 + (0.06 * real_count)), 2)
        elif proxy_count > 0:
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
                "qb_pass_att",
                "qb_pass_comp",
                "qb_pass_yds",
                "qb_pass_td",
                "qb_pass_int",
                "qb_rush_yds",
                "qb_rush_td",
                "wrte_rec",
                "wrte_rec_yds",
                "wrte_rec_td",
                "rb_rush_att",
                "rb_rush_yds",
                "rb_rush_td",
                "rb_rec",
                "rb_rec_yds",
                "rb_rec_td",
                "edge_sacks",
                "edge_qb_hurries",
                "edge_tfl",
                "edge_tackles",
                "db_int",
                "db_pbu",
                "db_tackles",
                "db_tfl",
                "opp_def_ppa_allowed_avg",
                "opp_def_success_rate_allowed_avg",
                "opp_def_toughness_index",
                "opp_def_adjustment_multiplier",
                "opp_def_context_source",
                "qb_ppa_overall",
                "qb_ppa_passing",
                "qb_ppa_standard_downs",
                "qb_ppa_passing_downs",
                "qb_wepa_passing",
                "qb_usage_rate",
                "qb_adjusted_passing",
                "qb_adjusted_rushing",
                "qb_adjusted_total",
                "wrte_ppa_overall",
                "wrte_ppa_passing_downs",
                "wrte_wepa_receiving",
                "wrte_usage_rate",
                "rb_ppa_rushing",
                "rb_ppa_standard_downs",
                "rb_wepa_rushing",
                "rb_usage_rate",
                "rb_adjusted_rushing",
                "lb_tackles",
                "lb_tfl",
                "lb_sacks",
                "lb_qb_hurries",
                "lb_pbu",
                "lb_int",
                "lb_usage_rate",
                "ol_starts",
                "ol_usage_rate",
                "game_consistency_index",
                "late_season_trend_index",
                "top_defense_performance_index",
                "top_defense_games",
                "weekly_sample_games",
                "game_context_source",
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
        f"player_usage_file_present: {int(player_usage_path.exists())}",
        f"game_player_stats_file_present: {int(game_player_stats_path.exists())}",
        f"player_ppa_games_file_present: {int(player_ppa_game_path.exists())}",
        f"adjusted_player_metrics_file_present: {int(adjusted_player_metrics_path.exists())}",
        f"adjusted_player_metrics_rows: {len(adjusted_metric_rows)}",
        "notes: usage/ppa down-split fields are now direct CFBD inputs; game-level consistency/trend/top-defense layers are active when weekly files exist; adjustedPlayerMetrics are active for QB/RB where CFBD returns rows; YPRR, missed tackles forced, and coverage-target stats remain partial/open items.",
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
