from __future__ import annotations

import csv
from pathlib import Path

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
MANUAL_DIR = ROOT / "data" / "sources" / "manual"

DECLARED_OFFICIAL_PATH = MANUAL_DIR / "declared_underclassmen_2026_official.csv"
ESPN_UNDERCLASS_PATH = MANUAL_DIR / "espn_underclassmen_2026.csv"
COMBINE_INVITES_PATH = MANUAL_DIR / "nfl_combine_invites_2026.csv"
ESPN_UNDERCLASS_SOURCE_URL = "https://www.espn.com/nfl/story/_/id/47600173"
NFL_COMBINE_INVITE_SOURCE_URL = (
    "https://www.nfl.com/news/nfl-combine-full-list-of-draft-prospects-invited-to-2026-scouting-event"
)


def _load_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _add_source(existing: str, new_src: str) -> str:
    left = [s for s in str(existing or "").split("|") if s]
    if new_src and new_src not in left:
        left.append(new_src)
    return "|".join(left)


def _upsert_payload(
    *,
    by_name_pos: dict,
    by_name: dict,
    player_name: str,
    position: str,
    school: str,
    source_key: str,
    source_url: str,
    early_declare_flag: int,
    combine_invited_flag: int,
) -> None:
    key = canonical_player_name(player_name)
    pos = normalize_pos(position)
    school_key = canonical_player_name(school)
    if not key:
        return

    base = {
        "early_declare": 0,
        "combine_invited": 0,
        "early_declare_sources": "",
        "combine_invite_sources": "",
        "early_declare_source_urls": "",
        "combine_invite_source_urls": "",
        "early_declare_evidence_count": 0,
        "school_key": school_key,
    }

    if pos:
        np = (key, pos)
        payload = dict(by_name_pos.get(np, base))
        if early_declare_flag:
            payload["early_declare"] = 1
            payload["early_declare_sources"] = _add_source(payload.get("early_declare_sources", ""), source_key)
            payload["early_declare_source_urls"] = _add_source(payload.get("early_declare_source_urls", ""), source_url)
        if combine_invited_flag:
            payload["combine_invited"] = 1
            payload["combine_invite_sources"] = _add_source(payload.get("combine_invite_sources", ""), source_key)
            payload["combine_invite_source_urls"] = _add_source(payload.get("combine_invite_source_urls", ""), source_url)
        payload["early_declare_evidence_count"] = len(
            [s for s in str(payload.get("early_declare_sources", "")).split("|") if s]
        )
        payload["school_key"] = payload.get("school_key") or school_key
        by_name_pos[np] = payload

    payload_n = dict(by_name.get(key, base))
    if early_declare_flag:
        payload_n["early_declare"] = 1
        payload_n["early_declare_sources"] = _add_source(payload_n.get("early_declare_sources", ""), source_key)
        payload_n["early_declare_source_urls"] = _add_source(payload_n.get("early_declare_source_urls", ""), source_url)
    if combine_invited_flag:
        payload_n["combine_invited"] = 1
        payload_n["combine_invite_sources"] = _add_source(payload_n.get("combine_invite_sources", ""), source_key)
        payload_n["combine_invite_source_urls"] = _add_source(payload_n.get("combine_invite_source_urls", ""), source_url)
    payload_n["early_declare_evidence_count"] = len(
        [s for s in str(payload_n.get("early_declare_sources", "")).split("|") if s]
    )
    payload_n["school_key"] = payload_n.get("school_key") or school_key
    by_name[key] = payload_n


def load_early_declare_signals() -> dict:
    by_name_pos: dict[tuple[str, str], dict] = {}
    by_name: dict[str, dict] = {}

    official_rows = _load_rows(DECLARED_OFFICIAL_PATH)
    espn_rows = _load_rows(ESPN_UNDERCLASS_PATH)
    combine_rows = _load_rows(COMBINE_INVITES_PATH)

    for row in official_rows:
        school = str(row.get("school", "")).strip() or str(row.get("college", "")).strip()
        _upsert_payload(
            by_name_pos=by_name_pos,
            by_name=by_name,
            player_name=row.get("player_name", ""),
            position=row.get("position", ""),
            school=school,
            source_key=f"official_declared_{row.get('declaration_type','underclass').strip() or 'underclass'}",
            source_url=str(row.get("source_url", "")).strip(),
            early_declare_flag=1,
            combine_invited_flag=0,
        )

    for row in espn_rows:
        school = str(row.get("school", "")).strip() or str(row.get("college", "")).strip()
        _upsert_payload(
            by_name_pos=by_name_pos,
            by_name=by_name,
            player_name=row.get("player_name", ""),
            position=row.get("position", ""),
            school=school,
            source_key="espn_underclassmen_2026",
            source_url=str(row.get("source_url", "")).strip() or ESPN_UNDERCLASS_SOURCE_URL,
            early_declare_flag=1,
            combine_invited_flag=0,
        )

    for row in combine_rows:
        school = str(row.get("school", "")).strip() or str(row.get("college", "")).strip()
        _upsert_payload(
            by_name_pos=by_name_pos,
            by_name=by_name,
            player_name=row.get("player_name", ""),
            position=row.get("position", ""),
            school=school,
            source_key="nfl_combine_invite_2026",
            source_url=str(row.get("source_url", "")).strip() or NFL_COMBINE_INVITE_SOURCE_URL,
            early_declare_flag=int(str(row.get("early_declare", "0")).strip() in {"1", "true", "yes", "y"}),
            combine_invited_flag=1,
        )

    return {
        "by_name_pos": by_name_pos,
        "by_name": by_name,
        "meta": {
            "status": "ok",
            "official_rows": len(official_rows),
            "espn_underclass_rows": len(espn_rows),
            "combine_invite_rows": len(combine_rows),
            "matched_name_pos": len(by_name_pos),
            "matched_name": len(by_name),
        },
    }
