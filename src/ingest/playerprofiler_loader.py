from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Mapping

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
MANUAL_DIR = ROOT / "data" / "sources" / "manual"
DEFAULT_PP_PATH = MANUAL_DIR / "playerprofiler_2026.csv"


_DOM_BASE = {
    "WR": 0.30,
    "TE": 0.22,
    "RB": 0.26,
    "QB": 0.00,
}

_TARGET_SHARE_BASE = {
    "WR": 0.24,
    "TE": 0.18,
    "RB": 0.12,
}

_YPRR_BASE = {
    "WR": 2.20,
    "TE": 1.80,
    "RB": 1.40,
}

_YPTPA_BASE = {
    "WR": 2.70,
    "TE": 2.10,
    "RB": 1.60,
}


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    txt = str(value).strip().replace("%", "")
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _to_bool(value: str | None) -> bool:
    txt = (value or "").strip().lower()
    return txt in {"1", "true", "yes", "y", "t"}


def _norm_share(raw: float | None) -> float | None:
    if raw is None:
        return None
    if raw > 1.5:
        return max(0.0, min(1.0, raw / 100.0))
    return max(0.0, min(1.0, raw))


def _breakout_signal(age: float | None) -> float:
    if age is None:
        return 55.0
    score = ((22.5 - age) / (22.5 - 18.0)) * 100.0
    return round(max(1.0, min(99.0, score)), 2)


def _age_at_draft_signal(age: float | None) -> float:
    if age is None:
        return 55.0
    # Younger prospects generally carry more developmental runway.
    score = ((24.5 - age) / (24.5 - 20.2)) * 100.0
    return round(max(1.0, min(99.0, score)), 2)


def _dominator_signal(position: str, dom: float | None) -> float:
    if dom is None:
        return 55.0
    pos = normalize_pos(position)
    base = _DOM_BASE.get(pos, 0.25)
    score = 50.0 + ((dom - base) / 0.15) * 25.0
    return round(max(1.0, min(99.0, score)), 2)


def _target_share_signal(position: str, share: float | None) -> float:
    if share is None:
        return 55.0
    pos = normalize_pos(position)
    base = _TARGET_SHARE_BASE.get(pos, 0.18)
    score = 50.0 + ((share - base) / 0.12) * 25.0
    return round(max(1.0, min(99.0, score)), 2)


def _yprr_signal(position: str, yprr: float | None) -> float:
    if yprr is None:
        return 55.0
    pos = normalize_pos(position)
    base = _YPRR_BASE.get(pos, 1.7)
    score = 50.0 + ((yprr - base) / 1.4) * 25.0
    return round(max(1.0, min(99.0, score)), 2)


def _yptpa_signal(position: str, yptpa: float | None) -> float:
    if yptpa is None:
        return 55.0
    pos = normalize_pos(position)
    base = _YPTPA_BASE.get(pos, 2.0)
    score = 50.0 + ((yptpa - base) / 1.5) * 25.0
    return round(max(1.0, min(99.0, score)), 2)


def _efficiency_signal(position: str, yprr: float | None, yptpa: float | None) -> float:
    yprr_sig = _yprr_signal(position, yprr)
    yptpa_sig = _yptpa_signal(position, yptpa)
    has_yprr = yprr is not None
    has_yptpa = yptpa is not None

    if has_yprr and has_yptpa:
        return round(0.55 * yprr_sig + 0.45 * yptpa_sig, 2)
    if has_yprr:
        return yprr_sig
    if has_yptpa:
        return yptpa_sig
    return 55.0


def _athletic_signal(speed_score: float | None, burst_score: float | None) -> float:
    if speed_score is None and burst_score is None:
        return 55.0

    comps = []
    if speed_score is not None:
        comps.append(max(1.0, min(99.0, 50.0 + ((speed_score - 100.0) / 20.0) * 25.0)))
    if burst_score is not None:
        comps.append(max(1.0, min(99.0, 50.0 + ((burst_score - 120.0) / 24.0) * 25.0)))

    return round(sum(comps) / len(comps), 2)


def _coverage(
    position: str,
    breakout_age: float | None,
    dom: float | None,
    target_share: float | None,
    yprr: float | None,
    yptpa: float | None,
    age_at_draft: float | None,
) -> float:
    pos = normalize_pos(position)
    if pos in {"WR", "RB", "TE"}:
        checks = [
            breakout_age is not None,
            dom is not None,
            target_share is not None,
            (yprr is not None or yptpa is not None),
            age_at_draft is not None,
        ]
        return round(sum(1 for c in checks if c) / len(checks), 3)

    checks = [breakout_age is not None, age_at_draft is not None]
    return round(sum(1 for c in checks if c) / len(checks), 3)


def _skill_signal(
    position: str,
    breakout_age: float | None,
    dom: float | None,
    target_share: float | None,
    yprr: float | None,
    yptpa: float | None,
    age_at_draft: float | None,
    speed_score: float | None,
    burst_score: float | None,
    early_declare: bool,
) -> float:
    pos = normalize_pos(position)
    b_sig = _breakout_signal(breakout_age)
    d_sig = _dominator_signal(pos, dom)
    t_sig = _target_share_signal(pos, target_share)
    e_sig = _efficiency_signal(pos, yprr, yptpa)
    age_sig = _age_at_draft_signal(age_at_draft)
    ath_sig = _athletic_signal(speed_score, burst_score)

    if pos in {"WR", "RB", "TE"}:
        score = 0.30 * d_sig + 0.24 * b_sig + 0.16 * t_sig + 0.16 * e_sig + 0.10 * age_sig + 0.04 * ath_sig
    else:
        score = 0.70 * b_sig + 0.20 * age_sig + 0.10 * ath_sig

    if early_declare:
        score += 2.0

    return round(max(1.0, min(99.0, score)), 2)


def _risk_flag(
    position: str,
    breakout_age: float | None,
    dom: float | None,
    target_share: float | None,
    eff_signal: float,
    age_at_draft: float | None,
) -> bool:
    pos = normalize_pos(position)
    if age_at_draft is not None and age_at_draft >= 23.6:
        return True

    if pos in {"WR", "RB", "TE"}:
        if breakout_age is not None and breakout_age >= 21.3:
            return True
        if dom is not None and dom < 0.20:
            return True
        if target_share is not None and target_share < 0.16:
            return True
        if eff_signal < 45.0:
            return True

    return False


def _profile_tier(skill_signal: float) -> str:
    if skill_signal >= 85:
        return "elite"
    if skill_signal >= 75:
        return "great"
    if skill_signal >= 65:
        return "good"
    if skill_signal >= 55:
        return "average"
    return "below_average"


def _row_core(row: Mapping[str, str]) -> dict:
    player_name = (row.get("player_name") or "").strip()
    school = (row.get("school") or "").strip()
    position = normalize_pos((row.get("position") or "").strip())

    breakout_age = _to_float(row.get("breakout_age"))
    dom = _norm_share(_to_float(row.get("college_dominator")))
    target_share = _norm_share(_to_float(row.get("target_share")))
    yprr = _to_float(row.get("yards_per_route_run"))
    yptpa = _to_float(row.get("yards_per_team_pass_attempt"))
    age_at_draft = _to_float(row.get("age_at_draft"))
    speed_score = _to_float(row.get("speed_score"))
    burst_score = _to_float(row.get("burst_score"))
    early_declare = _to_bool(row.get("early_declare"))

    breakout_sig = _breakout_signal(breakout_age)
    dom_sig = _dominator_signal(position, dom)
    target_sig = _target_share_signal(position, target_share)
    eff_sig = _efficiency_signal(position, yprr, yptpa)
    age_sig = _age_at_draft_signal(age_at_draft)
    ath_sig = _athletic_signal(speed_score, burst_score)
    skill_sig = _skill_signal(
        position,
        breakout_age,
        dom,
        target_share,
        yprr,
        yptpa,
        age_at_draft,
        speed_score,
        burst_score,
        early_declare,
    )
    coverage = _coverage(position, breakout_age, dom, target_share, yprr, yptpa, age_at_draft)
    risk_flag = _risk_flag(position, breakout_age, dom, target_share, eff_sig, age_at_draft)

    return {
        "player_name": player_name,
        "player_key": canonical_player_name(player_name),
        "school": school,
        "school_key": canonical_player_name(school),
        "position": position,
        "pp_source": (row.get("source") or "playerprofiler").strip() or "playerprofiler",
        "pp_last_updated": (row.get("last_updated") or "").strip(),
        "pp_breakout_age": round(breakout_age, 2) if breakout_age is not None else "",
        "pp_college_dominator": round(dom, 4) if dom is not None else "",
        "pp_target_share": round(target_share, 4) if target_share is not None else "",
        "pp_yards_per_route_run": round(yprr, 3) if yprr is not None else "",
        "pp_yards_per_team_pass_attempt": round(yptpa, 3) if yptpa is not None else "",
        "pp_age_at_draft": round(age_at_draft, 2) if age_at_draft is not None else "",
        "pp_speed_score": round(speed_score, 2) if speed_score is not None else "",
        "pp_burst_score": round(burst_score, 2) if burst_score is not None else "",
        "pp_breakout_signal": breakout_sig,
        "pp_dominator_signal": dom_sig,
        "pp_target_share_signal": target_sig,
        "pp_efficiency_signal": eff_sig,
        "pp_age_signal": age_sig,
        "pp_athletic_signal": ath_sig,
        "pp_skill_signal": skill_sig,
        "pp_data_coverage": coverage,
        "pp_early_declare": int(early_declare),
        "pp_risk_flag": int(risk_flag),
        "pp_profile_tier": _profile_tier(skill_sig),
        "pp_notes": (row.get("notes") or "").strip(),
    }


def load_playerprofiler_signals(path: Path | None = None) -> dict:
    path = path or DEFAULT_PP_PATH
    if not path.exists():
        return {
            "by_name_pos": {},
            "by_name": {},
            "meta": {"status": "missing_playerprofiler_file", "path": str(path), "rows": 0},
        }

    by_name_pos: Dict[tuple, dict] = {}
    by_name: Dict[str, dict] = {}
    row_count = 0

    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            core = _row_core(row)
            row_count += 1

            if not core["player_key"] or not core["position"]:
                continue

            key_np = (core["player_key"], core["position"])
            existing = by_name_pos.get(key_np)
            if existing is None or core["pp_data_coverage"] > existing.get("pp_data_coverage", 0.0):
                by_name_pos[key_np] = core

            name_key = core["player_key"]
            ex_name = by_name.get(name_key)
            if ex_name is None or core["pp_data_coverage"] > ex_name.get("pp_data_coverage", 0.0):
                by_name[name_key] = core

    return {
        "by_name_pos": by_name_pos,
        "by_name": by_name,
        "meta": {
            "status": "ok",
            "path": str(path),
            "rows": row_count,
            "matched_name_pos": len(by_name_pos),
            "matched_name": len(by_name),
        },
    }
