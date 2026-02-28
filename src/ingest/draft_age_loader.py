from __future__ import annotations

import csv
import datetime as dt
import os
from pathlib import Path

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
MANUAL_DIR = ROOT / "data" / "sources" / "manual"
DEFAULT_PATH = MANUAL_DIR / "espn_birthdates_2026.csv"
DEFAULT_DRAFT_DATE = os.getenv("DRAFT_DATE", "2026-04-23")


def _parse_date(raw: str) -> dt.date | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    fmts = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ]
    for fmt in fmts:
        try:
            return dt.datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    return None


def _compute_age_years(birth_date: dt.date, ref_date: dt.date) -> float:
    days = (ref_date - birth_date).days
    return round(days / 365.2425, 4)


def load_draft_age_signals(path: Path | None = None, draft_date: str = DEFAULT_DRAFT_DATE) -> dict:
    src = path or DEFAULT_PATH
    ref_date = _parse_date(draft_date)
    if ref_date is None:
        ref_date = dt.date(2026, 4, 23)

    if not src.exists():
        return {
            "by_name_pos": {},
            "by_name": {},
            "meta": {
                "status": "missing_draft_age_file",
                "path": str(src),
                "rows": 0,
                "draft_date": ref_date.isoformat(),
            },
        }

    with src.open() as f:
        rows = list(csv.DictReader(f))

    by_name_pos: dict[tuple[str, str], dict] = {}
    by_name: dict[str, dict] = {}
    valid_rows = 0

    for row in rows:
        name = str(row.get("player_name", "")).strip()
        if not name:
            continue
        key = canonical_player_name(name)
        pos = normalize_pos(str(row.get("position", "")).strip())
        school = str(row.get("school", "")).strip()
        birth_raw = str(row.get("birth_date", "")).strip()
        birth_dt = _parse_date(birth_raw)
        if birth_dt is None:
            continue

        draft_age = _compute_age_years(birth_dt, ref_date)
        payload = {
            "birth_date": birth_dt.isoformat(),
            "draft_age": round(float(draft_age), 3),
            "draft_age_source": str(row.get("source", "")).strip() or "espn_player_page",
            "draft_age_source_url": str(row.get("source_url", "")).strip(),
            "draft_age_ref_date": ref_date.isoformat(),
            "draft_age_available": 1,
            "draft_age_school_key": canonical_player_name(school),
        }

        valid_rows += 1
        if pos:
            np = (key, pos)
            cur = by_name_pos.get(np)
            if cur is None:
                by_name_pos[np] = payload
        cur_name = by_name.get(key)
        if cur_name is None:
            by_name[key] = payload

    return {
        "by_name_pos": by_name_pos,
        "by_name": by_name,
        "meta": {
            "status": "ok",
            "path": str(src),
            "rows": len(rows),
            "valid_birthdates": valid_rows,
            "matched_name_pos": len(by_name_pos),
            "matched_name": len(by_name),
            "draft_date": ref_date.isoformat(),
        },
    }

