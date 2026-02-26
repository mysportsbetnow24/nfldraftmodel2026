from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, Tuple

from src.ingest.analyst_language_loader import compute_linguistic_features
from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
ANALYST_SEED_PATH = ROOT / "data" / "sources" / "analyst_rankings_seed.csv"
TDN_STRUCTURED_PATH = ROOT / "data" / "processed" / "tdn_scouting_structured_2026.csv"
BR_STRUCTURED_PATH = ROOT / "data" / "processed" / "bleacher_scouting_structured_2026.csv"
ATOZ_STRUCTURED_PATH = ROOT / "data" / "processed" / "atoz_scouting_structured_2026.csv"
SI_STRUCTURED_PATH = ROOT / "data" / "processed" / "si_fcs_scouting_structured_2026.csv"

TDN_SOURCE = "TDN_Scouting_2026"
RINGER_SOURCE = "Ringer_NFL_Draft_Guide_2026"
BR_SOURCE = "Bleacher_Report_2026"
ATOZ_SOURCE = "AtoZ_Scouting_2026"
SI_SOURCE = "SI_FCS_Scouting_2026"


def _to_int(value) -> int | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return int(float(txt))
    except ValueError:
        return None


def _clamp(v: float, lo: float = 1.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _rank_to_signal(rank: int | None) -> float:
    if rank is None:
        return 0.0
    return round(_clamp((301.0 - float(rank)) / 3.0), 2)


def _grade_label_signal(grade_label: str, round_num: int | None) -> float:
    if round_num is not None:
        round_map = {
            1: 95.0,
            2: 88.0,
            3: 82.0,
            4: 76.0,
            5: 71.0,
            6: 66.0,
            7: 61.0,
        }
        return round_map.get(round_num, 57.0)

    txt = (grade_label or "").lower()
    if not txt:
        return 0.0
    if "day 1" in txt or "first round" in txt or "round 1" in txt:
        return 92.0
    if "day 2" in txt or "round 2" in txt:
        return 85.0
    if "round 3" in txt:
        return 80.0
    if "round 4" in txt:
        return 75.0
    if "round 5" in txt:
        return 70.0
    if "round 6" in txt:
        return 65.0
    if "round 7" in txt:
        return 60.0
    if "udfa" in txt or "priority fa" in txt:
        return 55.0
    return 58.0


def _risk_penalty_from_text(text: str) -> tuple[int, float]:
    risk_terms = {
        "raw",
        "inconsistent",
        "developmental",
        "limited",
        "question",
        "concern",
        "streaky",
        "injury",
        "medical",
        "off-field",
        "character",
    }
    clean = (text or "").lower()
    hits = sum(clean.count(t) for t in risk_terms)
    if hits >= 6:
        penalty = 1.3
    elif hits >= 4:
        penalty = 0.9
    elif hits >= 2:
        penalty = 0.5
    else:
        penalty = 0.0
    return hits, penalty


def _extract_round_num(value: str) -> int | None:
    txt = str(value or "")
    m = re.search(r"round\s*(\d)", txt, flags=re.I)
    if m:
        return _to_int(m.group(1))
    return None


def _load_source_rank_rows(path: Path) -> dict[str, dict[Tuple[str, str], dict]]:
    source_maps: dict[str, dict[Tuple[str, str], dict]] = {
        TDN_SOURCE: {},
        RINGER_SOURCE: {},
        BR_SOURCE: {},
        ATOZ_SOURCE: {},
        SI_SOURCE: {},
    }
    if not path.exists():
        return source_maps

    with path.open() as f:
        for row in csv.DictReader(f):
            src = str(row.get("source", "")).strip()
            if src not in source_maps:
                continue
            name = canonical_player_name(row.get("player_name", ""))
            pos = normalize_pos(row.get("position", ""))
            rank = _to_int(row.get("source_rank"))
            if not name or not pos:
                continue
            payload = {
                "source_rank": rank if rank is not None else "",
                "rank_signal": _rank_to_signal(rank) if rank is not None else 0.0,
            }
            key = (name, pos)
            cur = source_maps[src].get(key)
            if cur is None or (_to_int(payload["source_rank"]) or 9999) < (_to_int(cur["source_rank"]) or 9999):
                source_maps[src][key] = payload

    return source_maps


def _load_structured_text_signals(
    *,
    path: Path,
    strength_key: str,
    concern_key: str,
    summary_key: str,
    rank_key: str,
    prefix: str,
) -> dict[Tuple[str, str], dict]:
    by_name_pos: dict[Tuple[str, str], dict] = {}
    if not path.exists():
        return by_name_pos

    with path.open() as f:
        for row in csv.DictReader(f):
            name = canonical_player_name(row.get("player_name", ""))
            pos = normalize_pos(row.get("position", ""))
            if not name or not pos:
                continue
            report_text = str(row.get("report_text", "")).strip()
            strengths = str(row.get(strength_key, "")).strip()
            concerns = str(row.get(concern_key, "")).strip()
            summary = str(row.get(summary_key, "")).strip()
            rank_val = _to_int(row.get(rank_key))

            risk_hits, risk_penalty = _risk_penalty_from_text(" ".join([concerns, summary, report_text]))
            lang_feats = compute_linguistic_features(report_text, pos)
            by_name_pos[(name, pos)] = {
                f"{prefix}_rank": rank_val if rank_val is not None else "",
                f"{prefix}_text_trait_signal": round(float(lang_feats.get("lang_trait_composite", 50.0)), 2),
                f"{prefix}_text_coverage": int(lang_feats.get("lang_text_coverage", 0)),
                f"{prefix}_risk_hits": risk_hits,
                f"{prefix}_risk_flag": 1 if risk_penalty > 0 else 0,
                f"{prefix}_risk_penalty": round(risk_penalty, 2),
                f"{prefix}_strengths": strengths,
                f"{prefix}_concerns": concerns,
                f"{prefix}_summary": summary,
            }
    return by_name_pos


def load_tdn_ringer_signals(
    analyst_seed_path: Path | None = None,
    tdn_structured_path: Path | None = None,
    br_structured_path: Path | None = None,
    atoz_structured_path: Path | None = None,
    si_structured_path: Path | None = None,
) -> dict:
    analyst_seed_path = analyst_seed_path or ANALYST_SEED_PATH
    tdn_structured_path = tdn_structured_path or TDN_STRUCTURED_PATH
    br_structured_path = br_structured_path or BR_STRUCTURED_PATH
    atoz_structured_path = atoz_structured_path or ATOZ_STRUCTURED_PATH
    si_structured_path = si_structured_path or SI_STRUCTURED_PATH

    seed_maps = _load_source_rank_rows(analyst_seed_path)
    tdn_seed_by_name_pos = seed_maps.get(TDN_SOURCE, {})
    ringer_seed_by_name_pos = seed_maps.get(RINGER_SOURCE, {})
    br_seed_by_name_pos = seed_maps.get(BR_SOURCE, {})
    atoz_seed_by_name_pos = seed_maps.get(ATOZ_SOURCE, {})
    si_seed_by_name_pos = seed_maps.get(SI_SOURCE, {})

    tdn_struct_by_name_pos: dict[Tuple[str, str], dict] = {}
    if tdn_structured_path.exists():
        with tdn_structured_path.open() as f:
            for row in csv.DictReader(f):
                name = canonical_player_name(row.get("player_name", ""))
                pos = normalize_pos(row.get("position", ""))
                if not name or not pos:
                    continue
                grade_label = str(row.get("tdn_grade_label", "")).strip()
                round_num = _to_int(row.get("tdn_grade_round")) or _extract_round_num(grade_label)
                report_text = str(row.get("report_text", "")).strip()
                concerns = str(row.get("tdn_concerns", "")).strip()
                summary = str(row.get("tdn_summary", "")).strip()
                risk_hits, risk_penalty = _risk_penalty_from_text(" ".join([concerns, summary, report_text]))
                lang_feats = compute_linguistic_features(report_text, pos)
                tdn_struct_by_name_pos[(name, pos)] = {
                    "tdn_overall_rank": _to_int(row.get("tdn_overall_rank")) or "",
                    "tdn_position_rank": _to_int(row.get("tdn_position_rank")) or "",
                    "tdn_grade_label": grade_label,
                    "tdn_grade_round": round_num if round_num is not None else "",
                    "tdn_grade_label_signal": round(_grade_label_signal(grade_label, round_num), 2),
                    "tdn_text_trait_signal": round(float(lang_feats.get("lang_trait_composite", 50.0)), 2),
                    "tdn_text_coverage": int(lang_feats.get("lang_text_coverage", 0)),
                    "tdn_risk_hits": risk_hits,
                    "tdn_risk_flag": 1 if risk_penalty > 0 else 0,
                    "tdn_risk_penalty": round(risk_penalty, 2),
                    "tdn_strengths": str(row.get("tdn_strengths", "")).strip(),
                    "tdn_concerns": concerns,
                    "tdn_summary": summary,
                }

    br_struct_by_name_pos = _load_structured_text_signals(
        path=br_structured_path,
        strength_key="br_strengths",
        concern_key="br_concerns",
        summary_key="br_summary",
        rank_key="br_rank",
        prefix="br",
    )
    atoz_struct_by_name_pos = _load_structured_text_signals(
        path=atoz_structured_path,
        strength_key="atoz_strengths",
        concern_key="atoz_concerns",
        summary_key="atoz_summary",
        rank_key="atoz_rank",
        prefix="atoz",
    )
    si_struct_by_name_pos = _load_structured_text_signals(
        path=si_structured_path,
        strength_key="si_strengths",
        concern_key="si_concerns",
        summary_key="si_summary",
        rank_key="si_rank",
        prefix="si",
    )

    keys = (
        set(tdn_seed_by_name_pos.keys())
        | set(ringer_seed_by_name_pos.keys())
        | set(br_seed_by_name_pos.keys())
        | set(atoz_seed_by_name_pos.keys())
        | set(si_seed_by_name_pos.keys())
        | set(tdn_struct_by_name_pos.keys())
        | set(br_struct_by_name_pos.keys())
        | set(atoz_struct_by_name_pos.keys())
        | set(si_struct_by_name_pos.keys())
    )
    by_name_pos: Dict[Tuple[str, str], dict] = {}
    by_name: Dict[str, dict] = {}
    for key in keys:
        tdn_seed = tdn_seed_by_name_pos.get(key, {})
        ringer_seed = ringer_seed_by_name_pos.get(key, {})
        br_seed = br_seed_by_name_pos.get(key, {})
        atoz_seed = atoz_seed_by_name_pos.get(key, {})
        si_seed = si_seed_by_name_pos.get(key, {})
        tdn_struct = tdn_struct_by_name_pos.get(key, {})
        br_struct = br_struct_by_name_pos.get(key, {})
        atoz_struct = atoz_struct_by_name_pos.get(key, {})
        si_struct = si_struct_by_name_pos.get(key, {})

        payload = {
            "tdn_rank": tdn_seed.get("source_rank", ""),
            "tdn_rank_signal": round(float(tdn_seed.get("rank_signal", 0.0) or 0.0), 2),
            "ringer_rank": ringer_seed.get("source_rank", ""),
            "ringer_rank_signal": round(float(ringer_seed.get("rank_signal", 0.0) or 0.0), 2),
            "br_rank": br_seed.get("source_rank", br_struct.get("br_rank", "")),
            "br_rank_signal": round(float(br_seed.get("rank_signal", 0.0) or 0.0), 2),
            "atoz_rank": atoz_seed.get("source_rank", atoz_struct.get("atoz_rank", "")),
            "atoz_rank_signal": round(float(atoz_seed.get("rank_signal", 0.0) or 0.0), 2),
            "si_rank": si_seed.get("source_rank", si_struct.get("si_rank", "")),
            "si_rank_signal": round(float(si_seed.get("rank_signal", 0.0) or 0.0), 2),
            "tdn_grade_label": tdn_struct.get("tdn_grade_label", ""),
            "tdn_grade_round": tdn_struct.get("tdn_grade_round", ""),
            "tdn_grade_label_signal": round(float(tdn_struct.get("tdn_grade_label_signal", 0.0) or 0.0), 2),
            "tdn_text_trait_signal": round(float(tdn_struct.get("tdn_text_trait_signal", 0.0) or 0.0), 2),
            "tdn_text_coverage": tdn_struct.get("tdn_text_coverage", ""),
            "tdn_risk_hits": tdn_struct.get("tdn_risk_hits", ""),
            "tdn_risk_flag": tdn_struct.get("tdn_risk_flag", ""),
            "tdn_risk_penalty": round(float(tdn_struct.get("tdn_risk_penalty", 0.0) or 0.0), 2),
            "tdn_strengths": tdn_struct.get("tdn_strengths", ""),
            "tdn_concerns": tdn_struct.get("tdn_concerns", ""),
            "tdn_summary": tdn_struct.get("tdn_summary", ""),
            "br_text_trait_signal": round(float(br_struct.get("br_text_trait_signal", 0.0) or 0.0), 2),
            "br_text_coverage": br_struct.get("br_text_coverage", ""),
            "br_risk_hits": br_struct.get("br_risk_hits", ""),
            "br_risk_flag": br_struct.get("br_risk_flag", ""),
            "br_risk_penalty": round(float(br_struct.get("br_risk_penalty", 0.0) or 0.0), 2),
            "br_strengths": br_struct.get("br_strengths", ""),
            "br_concerns": br_struct.get("br_concerns", ""),
            "br_summary": br_struct.get("br_summary", ""),
            "atoz_text_trait_signal": round(float(atoz_struct.get("atoz_text_trait_signal", 0.0) or 0.0), 2),
            "atoz_text_coverage": atoz_struct.get("atoz_text_coverage", ""),
            "atoz_risk_hits": atoz_struct.get("atoz_risk_hits", ""),
            "atoz_risk_flag": atoz_struct.get("atoz_risk_flag", ""),
            "atoz_risk_penalty": round(float(atoz_struct.get("atoz_risk_penalty", 0.0) or 0.0), 2),
            "atoz_strengths": atoz_struct.get("atoz_strengths", ""),
            "atoz_concerns": atoz_struct.get("atoz_concerns", ""),
            "atoz_summary": atoz_struct.get("atoz_summary", ""),
            "si_text_trait_signal": round(float(si_struct.get("si_text_trait_signal", 0.0) or 0.0), 2),
            "si_text_coverage": si_struct.get("si_text_coverage", ""),
            "si_risk_hits": si_struct.get("si_risk_hits", ""),
            "si_risk_flag": si_struct.get("si_risk_flag", ""),
            "si_risk_penalty": round(float(si_struct.get("si_risk_penalty", 0.0) or 0.0), 2),
            "si_strengths": si_struct.get("si_strengths", ""),
            "si_concerns": si_struct.get("si_concerns", ""),
            "si_summary": si_struct.get("si_summary", ""),
        }
        by_name_pos[key] = payload
        cur = by_name.get(key[0])
        if cur is None:
            by_name[key[0]] = payload
        else:
            cur_score = (
                float(cur.get("tdn_rank_signal", 0.0) or 0.0)
                + float(cur.get("ringer_rank_signal", 0.0) or 0.0)
                + float(cur.get("br_rank_signal", 0.0) or 0.0)
                + float(cur.get("atoz_rank_signal", 0.0) or 0.0)
                + float(cur.get("si_rank_signal", 0.0) or 0.0)
                + float(cur.get("tdn_text_trait_signal", 0.0) or 0.0)
                + float(cur.get("br_text_trait_signal", 0.0) or 0.0)
                + float(cur.get("atoz_text_trait_signal", 0.0) or 0.0)
                + float(cur.get("si_text_trait_signal", 0.0) or 0.0)
            )
            new_score = (
                float(payload.get("tdn_rank_signal", 0.0) or 0.0)
                + float(payload.get("ringer_rank_signal", 0.0) or 0.0)
                + float(payload.get("br_rank_signal", 0.0) or 0.0)
                + float(payload.get("atoz_rank_signal", 0.0) or 0.0)
                + float(payload.get("si_rank_signal", 0.0) or 0.0)
                + float(payload.get("tdn_text_trait_signal", 0.0) or 0.0)
                + float(payload.get("br_text_trait_signal", 0.0) or 0.0)
                + float(payload.get("atoz_text_trait_signal", 0.0) or 0.0)
                + float(payload.get("si_text_trait_signal", 0.0) or 0.0)
            )
            if new_score > cur_score:
                by_name[key[0]] = payload

    return {
        "by_name_pos": by_name_pos,
        "by_name": by_name,
        "meta": {
            "tdn_seed_rows": len(tdn_seed_by_name_pos),
            "ringer_seed_rows": len(ringer_seed_by_name_pos),
            "br_seed_rows": len(br_seed_by_name_pos),
            "atoz_seed_rows": len(atoz_seed_by_name_pos),
            "si_seed_rows": len(si_seed_by_name_pos),
            "tdn_struct_rows": len(tdn_struct_by_name_pos),
            "br_struct_rows": len(br_struct_by_name_pos),
            "atoz_struct_rows": len(atoz_struct_by_name_pos),
            "si_struct_rows": len(si_struct_by_name_pos),
        },
    }
