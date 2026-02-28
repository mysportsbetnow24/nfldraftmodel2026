from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from statistics import pstdev
from typing import Dict, Iterable, List, Mapping, Tuple

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = ROOT / "data" / "sources"
EXTERNAL_DIR = SOURCES_DIR / "external"

ESPN_DATASET_DIR_CANDIDATES = [
    EXTERNAL_DIR / "espn_nfl_draft_prospect_data",
    EXTERNAL_DIR / "espn-nfl-draft-prospect-data",
    EXTERNAL_DIR / "nfl-draft-data",
]

DEFAULT_SIGNAL_YEAR = 2026


USEFUL_FIELDS = {
    "prospects": [
        "player_id",
        "player_name",
        "school",
        "position",
        "draft_year",
        "ovr_rk",
        "pos_rk",
        "grade",
        "height_in",
        "weight_lb",
        "draft_round",
        "overall_pick",
        "draft_team",
    ],
    "profiles": ["player_id", "player_name", "school", "position", "text1", "text2", "text3", "text4"],
    "college_qbr": ["player_id", "player_name", "school", "position", "season", "qbr", "epa_per_play"],
    "college_stats": ["player_id", "player_name", "school", "position", "season", "stat_name", "stat_value"],
    "ids": ["player_id", "player_name", "school", "espn_id"],
}

REJECTED_FIELD_CATEGORIES = [
    "headshots/images/urls",
    "biographical narrative fields that are not numeric features",
    "social/profile links",
    "post-draft descriptive text fields",
    "duplicate identifier columns that do not improve joins",
]


_TEXT_POSITIVE = {
    "processing": {"processing", "anticipation", "reads", "diagnose", "progression", "decision", "timing"},
    "separation": {"separation", "separate", "route", "release", "burst", "sudden", "create space"},
    "play_strength": {"physical", "play strength", "strong", "anchor", "contact balance", "power", "violent hands"},
    "motor": {"motor", "effort", "relentless", "high-energy", "pursuit", "finisher"},
    "instincts": {"instincts", "awareness", "recognition", "feel", "vision", "trigger"},
}

_TEXT_NEGATIVE = {
    "processing": {"late", "slow processing", "locks on", "predetermined"},
    "separation": {"struggles to separate", "limited burst", "stiff", "cannot create"},
    "play_strength": {"overpowered", "lacks play strength", "narrow frame"},
    "motor": {"inconsistent motor", "coasts", "low effort"},
    "instincts": {"late eyes", "poor instincts", "slow trigger"},
}

_VOLATILITY_TERMS = {
    "raw",
    "inconsistent",
    "developmental",
    "boom-or-bust",
    "boom or bust",
    "needs polish",
    "erratic",
    "streaky",
}


STAT_PATTERNS = {
    "pass_yards": {"passingyards", "passyards", "passyds"},
    "pass_tds": {"passingtd", "passtd", "passingtouchdowns"},
    "interceptions": {"interceptions", "ints", "int"},
    "completions": {"completions", "comp"},
    "pass_attempts": {"attempts", "passattempts", "att"},
    "rush_yards": {"rushingyards", "rushyards", "rushyds"},
    "rush_tds": {"rushingtd", "rushtd", "rushtouchdowns"},
    "rush_attempts": {"carries", "rushattempts", "rushatt"},
    "receptions": {"receptions", "rec"},
    "rec_yards": {"receivingyards", "recyards", "recyds", "receivingyds"},
    "rec_tds": {"receivingtd", "rectd", "receivingtouchdowns"},
    "tfl": {"tacklesforloss", "tfl"},
    "sacks": {"sacks", "sack"},
    "tackles": {"totaltackles", "tackles", "solo+assist"},
    "pass_breakups": {"passbreakups", "passesdefended", "pbus", "pbu"},
    "def_ints": {"interceptionsdef", "definterceptions", "intsdef", "interceptions"},
}


def _clean_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _find_value(row: Mapping[str, str], aliases: Iterable[str]) -> str:
    if not row:
        return ""
    normalized = {_clean_col(k): v for k, v in row.items()}
    for alias in aliases:
        v = normalized.get(_clean_col(alias))
        if v is not None:
            return str(v).strip()
    return ""


def _to_int(value: str) -> int | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return int(float(txt))
    except ValueError:
        return None


def _to_float(value: str) -> float | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _parse_height_to_inches(raw: str) -> int | None:
    txt = (raw or "").strip()
    if not txt:
        return None

    if "'" in txt:
        parts = txt.replace('"', "").split("'")
        if len(parts) == 2:
            feet = _to_int(parts[0])
            inches = _to_int(parts[1])
            if feet is not None and inches is not None:
                return feet * 12 + inches

    if "-" in txt:
        parts = txt.split("-")
        if len(parts) == 2:
            feet = _to_int(parts[0])
            inches = _to_int(parts[1])
            if feet is not None and inches is not None:
                return feet * 12 + inches

    parsed = _to_int(txt)
    if parsed is None:
        return None

    if 58 <= parsed <= 90:
        return parsed
    return None


def _discover_dataset_dir() -> Path | None:
    for path in ESPN_DATASET_DIR_CANDIDATES:
        if path.exists() and path.is_dir():
            return path
    return None


def _discover_file(base_dir: Path | None, candidates: Iterable[str]) -> Path | None:
    if base_dir is None:
        return None
    for name in candidates:
        p = base_dir / name
        if p.exists():
            return p
    return None


def _load_csv(path: Path | None) -> List[dict]:
    if path is None or not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def load_espn_raw_tables(base_dir: Path | None = None) -> dict:
    base = base_dir or _discover_dataset_dir()
    prospects_path = _discover_file(base, ["nfl_draft_prospects.csv"])
    profiles_path = _discover_file(base, ["nfl_draft_profiles.csv"])
    qbr_path = _discover_file(base, ["college_qbr.csv", "qb_qbr.csv"])
    stats_path = _discover_file(base, ["college_stats.csv", "college_statistics.csv"])
    ids_path = _discover_file(base, ["ids.csv"])

    return {
        "base_dir": str(base) if base else "",
        "files": {
            "prospects": str(prospects_path) if prospects_path else "",
            "profiles": str(profiles_path) if profiles_path else "",
            "college_qbr": str(qbr_path) if qbr_path else "",
            "college_stats": str(stats_path) if stats_path else "",
            "ids": str(ids_path) if ids_path else "",
        },
        "prospects": _load_csv(prospects_path),
        "profiles": _load_csv(profiles_path),
        "college_qbr": _load_csv(qbr_path),
        "college_stats": _load_csv(stats_path),
        "ids": _load_csv(ids_path),
    }


def _row_player_core(row: Mapping[str, str]) -> dict:
    name = _find_value(row, ["player_name", "name", "player", "athlete", "prospect"])
    school = _find_value(row, ["school", "college", "team"])
    position = normalize_pos(_find_value(row, ["position", "pos"]))
    year = _to_int(_find_value(row, ["draft_year", "year", "season"]))
    player_id = _find_value(row, ["player_id", "id", "espn_id", "athlete_id"]) or ""

    return {
        "player_name": name,
        "player_key": canonical_player_name(name),
        "school": school,
        "school_key": canonical_player_name(school),
        "position": position,
        "draft_year": year,
        "player_id": player_id,
    }


def _make_join_key(player_key: str, position: str, school_key: str = "") -> Tuple[str, str, str]:
    return (player_key, position, school_key)


def _text_blob(*parts: str) -> str:
    txt = " ".join(p for p in parts if p)
    txt = re.sub(r"\s+", " ", txt).strip().lower()
    return txt


def _text_trait_scores(text: str) -> dict:
    if not text:
        return {
            "espn_text_coverage": 0,
            "espn_trait_processing": 50.0,
            "espn_trait_separation": 50.0,
            "espn_trait_play_strength": 50.0,
            "espn_trait_motor": 50.0,
            "espn_trait_instincts": 50.0,
            "espn_volatility_flag": False,
            "espn_volatility_hits": 0,
        }

    coverage = len(text.split())
    out = {"espn_text_coverage": coverage}

    for trait, positives in _TEXT_POSITIVE.items():
        negs = _TEXT_NEGATIVE.get(trait, set())
        pos_hits = sum(text.count(term) for term in positives)
        neg_hits = sum(text.count(term) for term in negs)
        score = max(20.0, min(95.0, 50.0 + 8.0 * pos_hits - 7.0 * neg_hits))
        out[f"espn_trait_{trait}"] = round(score, 2)

    vol_hits = sum(text.count(term) for term in _VOLATILITY_TERMS)
    out["espn_volatility_hits"] = vol_hits
    out["espn_volatility_flag"] = vol_hits > 0

    return out


def _normalize_stat_name(raw: str) -> str:
    txt = re.sub(r"[^a-z0-9]", "", (raw or "").lower())
    return txt


def _match_stat_bucket(stat_name: str) -> str | None:
    key = _normalize_stat_name(stat_name)
    if not key:
        return None

    for bucket, patterns in STAT_PATTERNS.items():
        if key in patterns:
            return bucket

    # fallback contains checks
    if "receiving" in key and "yard" in key:
        return "rec_yards"
    if "rushing" in key and "yard" in key:
        return "rush_yards"
    if "passing" in key and "yard" in key:
        return "pass_yards"
    if "sack" in key:
        return "sacks"
    if "tfl" in key:
        return "tfl"

    return None


def _extract_grade_stats(rows: List[dict]) -> dict:
    groups: Dict[Tuple[int, str], List[float]] = defaultdict(list)
    for row in rows:
        core = _row_player_core(row)
        yr = core["draft_year"]
        pos = core["position"]
        grade = _to_float(_find_value(row, ["grade", "espn_grade", "overall_grade"]))
        if yr is None or not pos or grade is None:
            continue
        groups[(yr, pos)].append(grade)

    out = {}
    for k, vals in groups.items():
        if not vals:
            continue
        mean = sum(vals) / len(vals)
        std = pstdev(vals) if len(vals) > 1 else 0.0
        out[k] = (mean, std)
    return out


def _aggregate_college_stats(rows: List[dict]) -> Dict[Tuple[str, str], dict]:
    """Aggregate to latest-season stat buckets by (player_key, position)."""
    by_player_year: Dict[Tuple[str, str, int], dict] = {}

    for row in rows:
        core = _row_player_core(row)
        key = core["player_key"]
        pos = core["position"]
        year = core["draft_year"]
        if not key or not pos or year is None:
            continue

        stat_name = _find_value(row, ["stat_name", "statistic", "stat", "category"])
        stat_bucket = _match_stat_bucket(stat_name)
        if stat_bucket is None:
            continue

        stat_value = _to_float(_find_value(row, ["stat_value", "value", "stat_val", "amount"]))
        if stat_value is None:
            continue

        py = (key, pos, year)
        payload = by_player_year.setdefault(py, {"draft_year": year})
        payload[stat_bucket] = stat_value

    # reduce to latest year per player
    latest_map: Dict[Tuple[str, str], dict] = {}
    for (key, pos, year), payload in by_player_year.items():
        kp = (key, pos)
        cur = latest_map.get(kp)
        if cur is None or year > cur.get("draft_year", -1):
            latest_map[kp] = dict(payload)

    return latest_map


def _aggregate_qbr(rows: List[dict]) -> Dict[Tuple[str, str], dict]:
    latest: Dict[Tuple[str, str], dict] = {}
    for row in rows:
        core = _row_player_core(row)
        key = core["player_key"]
        pos = core["position"] or "QB"
        year = core["draft_year"]
        if not key or year is None:
            continue

        qbr = _to_float(_find_value(row, ["qbr", "total_qbr", "espn_qbr"]))
        epa = _to_float(_find_value(row, ["epa_per_play", "epa", "adj_epa_per_play"]))
        payload = {"draft_year": year, "qbr": qbr, "epa_per_play": epa}

        kp = (key, normalize_pos(pos))
        cur = latest.get(kp)
        if cur is None or year > cur.get("draft_year", -1):
            latest[kp] = payload

    return latest


def _production_signal(position: str, stats: Mapping[str, float], qbr_payload: Mapping[str, float]) -> float:
    comps: List[float] = []

    def add(score: float | None) -> None:
        if score is None:
            return
        comps.append(max(20.0, min(95.0, score)))

    pos = normalize_pos(position)

    if pos == "QB":
        qbr = qbr_payload.get("qbr")
        add(qbr)

        pass_tds = stats.get("pass_tds")
        ints = stats.get("interceptions")
        if pass_tds is not None:
            ratio = pass_tds / max(1.0, ints if ints is not None else 8.0)
            add(40.0 + ratio * 12.0)

        pass_yards = stats.get("pass_yards")
        if pass_yards is not None:
            add(45.0 + pass_yards / 120.0)

    elif pos in {"WR", "TE"}:
        rec_yards = stats.get("rec_yards")
        rec_tds = stats.get("rec_tds")
        recs = stats.get("receptions")
        if rec_yards is not None:
            add(45.0 + rec_yards / 40.0)
        if rec_tds is not None:
            add(45.0 + rec_tds * 3.0)
        if rec_yards is not None and recs is not None and recs > 0:
            add(40.0 + (rec_yards / recs) * 2.2)

    elif pos == "RB":
        rush_yards = stats.get("rush_yards")
        rush_tds = stats.get("rush_tds")
        recs = stats.get("receptions")
        if rush_yards is not None:
            add(45.0 + rush_yards / 45.0)
        if rush_tds is not None:
            add(45.0 + rush_tds * 2.8)
        if recs is not None:
            add(45.0 + recs * 0.6)

    elif pos in {"EDGE", "DT", "LB"}:
        sacks = stats.get("sacks")
        tfl = stats.get("tfl")
        tackles = stats.get("tackles")
        if sacks is not None:
            add(45.0 + sacks * 3.2)
        if tfl is not None:
            add(42.0 + tfl * 1.8)
        if tackles is not None and pos == "LB":
            add(40.0 + tackles * 0.45)

    elif pos in {"CB", "S"}:
        ints = stats.get("def_ints")
        pbu = stats.get("pass_breakups")
        tackles = stats.get("tackles")
        if ints is not None:
            add(45.0 + ints * 4.5)
        if pbu is not None:
            add(42.0 + pbu * 2.0)
        if tackles is not None:
            add(40.0 + tackles * 0.35)

    if not comps:
        return 55.0
    return round(sum(comps) / len(comps), 2)


def load_espn_player_signals(target_year: int = DEFAULT_SIGNAL_YEAR, base_dir: Path | None = None) -> dict:
    """
    Returns ESPN signals keyed for board joining.
    Useful fields only; outcome labels are excluded from this signal map.
    """
    raw = load_espn_raw_tables(base_dir=base_dir)
    prospects = raw["prospects"]
    profiles = raw["profiles"]
    qbr_rows = raw["college_qbr"]
    stats_rows = raw["college_stats"]

    if not prospects:
        return {"by_name_pos": {}, "by_name": {}, "meta": {"status": "missing_prospects"}}

    grade_stats = _extract_grade_stats(prospects)

    profiles_by_id: Dict[str, List[dict]] = defaultdict(list)
    profiles_by_name_pos: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for row in profiles:
        core = _row_player_core(row)
        pid = core["player_id"]
        np = (core["player_key"], core["position"])
        if pid:
            profiles_by_id[pid].append(row)
        if np[0] and np[1]:
            profiles_by_name_pos[np].append(row)

    qbr_map = _aggregate_qbr(qbr_rows)
    stats_map = _aggregate_college_stats(stats_rows)

    by_name_pos: Dict[Tuple[str, str], dict] = {}
    by_name: Dict[str, dict] = {}

    for row in prospects:
        core = _row_player_core(row)
        name_key = core["player_key"]
        pos = core["position"]
        school_key = core["school_key"]
        year = core["draft_year"]

        if not name_key or not pos:
            continue

        if year is None:
            continue

        # Keep target-year rows first, else most recent <= target year.
        if year > target_year:
            continue

        ovr_rk = _to_int(_find_value(row, ["ovr_rk", "overall_rank", "overallrk", "rank"]))
        pos_rk = _to_int(_find_value(row, ["pos_rk", "position_rank", "posrank"]))
        grade = _to_float(_find_value(row, ["grade", "espn_grade", "overall_grade"]))

        height_in = _to_int(_find_value(row, ["height_in", "height_inches"]))
        if height_in is None:
            height_in = _parse_height_to_inches(_find_value(row, ["height", "ht"]))

        weight_lb = _to_int(_find_value(row, ["weight_lb", "weight", "wt"]))

        rank_signal = max(1.0, 301.0 - float(ovr_rk)) if ovr_rk is not None else 35.0
        pos_signal = max(1.0, 101.0 - float(pos_rk)) if pos_rk is not None else 35.0

        mean_std = grade_stats.get((year, pos), (grade if grade is not None else 70.0, 0.0))
        mean, std = mean_std
        if grade is None:
            grade_z = 0.0
        elif std <= 1e-6:
            grade_z = 0.0
        else:
            grade_z = (grade - mean) / std
        grade_signal = max(1.0, min(99.0, 50.0 + 15.0 * grade_z))

        # profile text join: prefer player_id, fallback name+position
        profile_candidates = []
        pid = core["player_id"]
        if pid and pid in profiles_by_id:
            profile_candidates = profiles_by_id[pid]
        elif (name_key, pos) in profiles_by_name_pos:
            profile_candidates = profiles_by_name_pos[(name_key, pos)]

        text_parts = []
        for prof in profile_candidates:
            text_parts.extend(
                [
                    _find_value(prof, ["text1", "summary1", "report1"]),
                    _find_value(prof, ["text2", "summary2", "report2"]),
                    _find_value(prof, ["text3", "summary3", "report3"]),
                    _find_value(prof, ["text4", "summary4", "report4"]),
                ]
            )
        text_scores = _text_trait_scores(_text_blob(*text_parts))

        qbr_payload = qbr_map.get((name_key, pos), {})
        stats_payload = stats_map.get((name_key, pos), {})
        prod_signal = _production_signal(pos, stats_payload, qbr_payload)

        payload = {
            "espn_source_year": year,
            "espn_ovr_rank": ovr_rk if ovr_rk is not None else "",
            "espn_pos_rank": pos_rk if pos_rk is not None else "",
            "espn_grade": round(grade, 2) if grade is not None else "",
            "espn_grade_z": round(grade_z, 4),
            "espn_rank_signal": round(rank_signal, 2),
            "espn_pos_signal": round(pos_signal, 2),
            "espn_grade_signal": round(grade_signal, 2),
            "espn_prod_signal": prod_signal,
            "espn_qbr": round(qbr_payload.get("qbr"), 2) if qbr_payload.get("qbr") is not None else "",
            "espn_epa_per_play": round(qbr_payload.get("epa_per_play"), 4)
            if qbr_payload.get("epa_per_play") is not None
            else "",
            "espn_height_in": height_in if height_in is not None else "",
            "espn_weight_lb": weight_lb if weight_lb is not None else "",
            "espn_school_key": school_key,
            **text_scores,
        }

        # choose best row: target year preferred, then best ovr rank
        key_np = (name_key, pos)
        existing = by_name_pos.get(key_np)
        if existing is not None:
            ex_year = int(existing.get("espn_source_year", 0) or 0)
            ex_rank = _to_int(str(existing.get("espn_ovr_rank", ""))) or 999
            new_rank = ovr_rk if ovr_rk is not None else 999

            prefer_new = False
            if year == target_year and ex_year != target_year:
                prefer_new = True
            elif ex_year == year and new_rank < ex_rank:
                prefer_new = True
            elif year > ex_year and ex_year != target_year:
                prefer_new = True

            if not prefer_new:
                continue

        by_name_pos[key_np] = payload

        name_existing = by_name.get(name_key)
        if name_existing is None:
            by_name[name_key] = payload
        else:
            ex_rank = _to_int(str(name_existing.get("espn_ovr_rank", ""))) or 999
            new_rank = ovr_rk if ovr_rk is not None else 999
            if new_rank < ex_rank:
                by_name[name_key] = payload

    return {
        "by_name_pos": by_name_pos,
        "by_name": by_name,
        "meta": {
            "status": "ok",
            "target_year": target_year,
            "prospects_rows": len(prospects),
            "profiles_rows": len(profiles),
            "qbr_rows": len(qbr_rows),
            "college_stats_rows": len(stats_rows),
            "matched_name_pos": len(by_name_pos),
            "matched_name": len(by_name),
        },
    }


def build_historical_training_rows(min_year: int = 2016, max_year: int = 2025, base_dir: Path | None = None) -> List[dict]:
    """
    Build leakage-safe historical rows with pre-draft features + draft outcomes as targets.
    Excludes post-draft performance from features.
    """
    raw = load_espn_raw_tables(base_dir=base_dir)
    prospects = raw["prospects"]
    if not prospects:
        return []

    signal_pack = load_espn_player_signals(target_year=max_year, base_dir=base_dir)
    signals = signal_pack["by_name_pos"]

    rows: List[dict] = []
    for row in prospects:
        core = _row_player_core(row)
        year = core["draft_year"]
        if year is None or year < min_year or year > max_year:
            continue
        key = (core["player_key"], core["position"])
        sig = signals.get(key, {})

        draft_round = _to_int(_find_value(row, ["draft_round", "round"]))
        overall_pick = _to_int(_find_value(row, ["overall_pick", "pick", "overall"]))
        drafted_flag = 1 if draft_round is not None else 0

        out = {
            "draft_year": year,
            "player_name": core["player_name"],
            "player_key": core["player_key"],
            "school": core["school"],
            "position": core["position"],
            # Features (pre-draft only)
            "espn_ovr_rank": sig.get("espn_ovr_rank", ""),
            "espn_pos_rank": sig.get("espn_pos_rank", ""),
            "espn_grade": sig.get("espn_grade", ""),
            "espn_grade_z": sig.get("espn_grade_z", ""),
            "espn_rank_signal": sig.get("espn_rank_signal", 35.0),
            "espn_pos_signal": sig.get("espn_pos_signal", 35.0),
            "espn_grade_signal": sig.get("espn_grade_signal", 50.0),
            "espn_prod_signal": sig.get("espn_prod_signal", 55.0),
            "espn_trait_processing": sig.get("espn_trait_processing", 50.0),
            "espn_trait_separation": sig.get("espn_trait_separation", 50.0),
            "espn_trait_play_strength": sig.get("espn_trait_play_strength", 50.0),
            "espn_trait_motor": sig.get("espn_trait_motor", 50.0),
            "espn_trait_instincts": sig.get("espn_trait_instincts", 50.0),
            "espn_volatility_flag": int(bool(sig.get("espn_volatility_flag", False))),
            "espn_height_in": sig.get("espn_height_in", ""),
            "espn_weight_lb": sig.get("espn_weight_lb", ""),
            # Targets
            "drafted_flag": drafted_flag,
            "draft_round": draft_round if draft_round is not None else "",
            "overall_pick": overall_pick if overall_pick is not None else "",
            "draft_team": _find_value(row, ["draft_team", "team", "nfl_team"]),
        }
        rows.append(out)

    rows.sort(key=lambda r: (int(r["draft_year"]), str(r["position"]), str(r["player_key"])))
    return rows


def leakage_safe_year_splits(rows: List[dict], valid_years: int = 1, test_years: int = 1) -> dict:
    years = sorted({int(r["draft_year"]) for r in rows if str(r.get("draft_year", "")).strip()})
    if len(years) < (valid_years + test_years + 1):
        return {"train_years": years, "valid_years": [], "test_years": []}

    test = years[-test_years:]
    valid = years[-(test_years + valid_years) : -test_years]
    train = [y for y in years if y not in set(valid + test)]
    return {"train_years": train, "valid_years": valid, "test_years": test}


def _missingness(values: Iterable[object]) -> float:
    values = list(values)
    if not values:
        return 0.0
    missing = 0
    for v in values:
        s = str(v).strip() if v is not None else ""
        if s == "":
            missing += 1
    return round(missing / len(values), 4)


def build_espn_feature_qa_report(
    board_rows: List[dict] | None = None,
    target_year: int = DEFAULT_SIGNAL_YEAR,
    base_dir: Path | None = None,
) -> dict:
    raw = load_espn_raw_tables(base_dir=base_dir)
    signals = load_espn_player_signals(target_year=target_year, base_dir=base_dir)

    prospects = raw["prospects"]
    profiles = raw["profiles"]

    # ID integrity by draft_year + player_id
    id_dupes = 0
    id_key_counts: Dict[Tuple[int, str], int] = defaultdict(int)
    for row in prospects:
        core = _row_player_core(row)
        if core["draft_year"] is None or not core["player_id"]:
            continue
        id_key_counts[(core["draft_year"], core["player_id"])] += 1
    id_dupes = sum(1 for _, cnt in id_key_counts.items() if cnt > 1)

    # Missingness by position/year for useful prospect fields
    groups = defaultdict(list)
    for row in prospects:
        core = _row_player_core(row)
        if core["draft_year"] is None or not core["position"]:
            continue
        groups[(core["draft_year"], core["position"])].append(row)

    miss_rows = []
    for (year, pos), grp in sorted(groups.items()):
        miss_rows.append(
            {
                "draft_year": year,
                "position": pos,
                "sample": len(grp),
                "missing_ovr_rk": _missingness(_find_value(r, ["ovr_rk", "overall_rank", "rank"]) for r in grp),
                "missing_pos_rk": _missingness(_find_value(r, ["pos_rk", "position_rank"]) for r in grp),
                "missing_grade": _missingness(_find_value(r, ["grade", "espn_grade"]) for r in grp),
                "missing_height": _missingness(_find_value(r, ["height_in", "height"]) for r in grp),
                "missing_weight": _missingness(_find_value(r, ["weight_lb", "weight"]) for r in grp),
            }
        )

    join_report = {"board_rows": 0, "name_pos_join_rate": 0.0, "name_only_join_rate": 0.0}
    if board_rows:
        by_np = signals.get("by_name_pos", {})
        by_name = signals.get("by_name", {})
        total = len(board_rows)
        hit_np = 0
        hit_name = 0
        for row in board_rows:
            name_key = canonical_player_name(row.get("player_name", ""))
            pos = normalize_pos(row.get("position") or row.get("pos_raw") or "")
            if (name_key, pos) in by_np:
                hit_np += 1
            if name_key in by_name:
                hit_name += 1
        join_report = {
            "board_rows": total,
            "name_pos_join_rate": round(hit_np / total, 4) if total else 0.0,
            "name_only_join_rate": round(hit_name / total, 4) if total else 0.0,
        }

    # profile text coverage snapshot
    profile_text_nonempty = 0
    for row in profiles:
        txt = _text_blob(
            _find_value(row, ["text1", "summary1", "report1"]),
            _find_value(row, ["text2", "summary2", "report2"]),
            _find_value(row, ["text3", "summary3", "report3"]),
            _find_value(row, ["text4", "summary4", "report4"]),
        )
        if txt:
            profile_text_nonempty += 1

    return {
        "status": signals.get("meta", {}).get("status", "unknown"),
        "dataset_base_dir": raw.get("base_dir", ""),
        "files": raw.get("files", {}),
        "row_counts": {
            "prospects": len(prospects),
            "profiles": len(profiles),
            "college_qbr": len(raw["college_qbr"]),
            "college_stats": len(raw["college_stats"]),
            "ids": len(raw["ids"]),
        },
        "signal_meta": signals.get("meta", {}),
        "useful_fields": USEFUL_FIELDS,
        "rejected_field_categories": REJECTED_FIELD_CATEGORIES,
        "qa_checks": {
            "player_id_duplicates_by_year": id_dupes,
            "profile_text_nonempty_rows": profile_text_nonempty,
            "join_coverage": join_report,
        },
        "missingness_by_year_position": miss_rows,
    }


def write_espn_qa_report(
    path: Path,
    board_rows: List[dict] | None = None,
    target_year: int = DEFAULT_SIGNAL_YEAR,
    base_dir: Path | None = None,
) -> dict:
    report = build_espn_feature_qa_report(board_rows=board_rows, target_year=target_year, base_dir=base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(report, f, indent=2)
    return report
