#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.kiper_loader import KIPER_SOURCE  # noqa: E402
from src.ingest.rankings_loader import canonical_player_name, normalize_pos  # noqa: E402


SOURCES_DIR = ROOT / "data" / "sources"
PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUTS_DIR = ROOT / "data" / "outputs"

MANUAL_KIPER_PATH = SOURCES_DIR / "manual" / "kiper_2026_board.csv"
ANALYST_SEED_PATH = SOURCES_DIR / "analyst_rankings_seed.csv"
REPORTS_PATH = PROCESSED_DIR / "analyst_reports_2026.csv"
PROCESSED_KIPER_PATH = PROCESSED_DIR / "kiper_structured_2026.csv"
REPORT_MD = OUTPUTS_DIR / "kiper_structured_ingest_report.md"


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


def _to_float(value) -> float | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _safe_str(value) -> str:
    return str(value or "").strip()


def _sort_rank(value) -> int:
    rank = _to_int(value)
    return rank if rank is not None else 9999


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _union_fieldnames(rows: list[dict], preferred: list[str]) -> list[str]:
    out = list(preferred)
    seen = set(out)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                out.append(key)
                seen.add(key)
    return out


def _render_report_text(row: dict) -> str:
    strength_tags = _safe_str(row.get("kiper_strength_tags", ""))
    concern_tags = _safe_str(row.get("kiper_concern_tags", ""))
    statline = _safe_str(row.get("kiper_statline_2025", ""))
    rank = _to_int(row.get("kiper_rank"))
    prev = _to_int(row.get("kiper_prev_rank"))
    delta = _to_int(row.get("kiper_rank_delta"))
    if delta is None and rank is not None and prev is not None:
        delta = prev - rank
    movement = "stable"
    if delta is not None:
        if delta > 0:
            movement = f"up {delta} spots"
        elif delta < 0:
            movement = f"down {abs(delta)} spots"

    parts = []
    if strength_tags:
        parts.append(f"Strength profile: {strength_tags}.")
    if concern_tags:
        parts.append(f"Concern profile: {concern_tags}.")
    if statline:
        parts.append(f"2025 production snapshot: {statline}.")
    parts.append(f"Board movement context: {movement}.")
    return " ".join(parts).strip()


def _dedupe_best(rows: list[dict]) -> list[dict]:
    best: dict[tuple[str, str], dict] = {}
    for row in rows:
        name_key = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position", ""))
        if not name_key or not pos:
            continue
        rank = _to_int(row.get("kiper_rank"))
        if rank is None:
            rank = _to_int(row.get("source_rank"))
        if rank is None:
            continue
        key = (name_key, pos)
        cur = best.get(key)
        if cur is None:
            best[key] = row
            continue
        cur_rank = _to_int(cur.get("kiper_rank"))
        if cur_rank is None:
            cur_rank = _to_int(cur.get("source_rank")) or 9999
        if rank < cur_rank:
            best[key] = row
    out = list(best.values())
    out.sort(key=lambda r: _to_int(r.get("kiper_rank")) or _to_int(r.get("source_rank")) or 9999)
    return out


def _merge_analyst_seed(kiper_rows: list[dict]) -> int:
    existing = _load_csv(ANALYST_SEED_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != KIPER_SOURCE]

    additions = []
    for row in kiper_rows:
        rank = _to_int(row.get("kiper_rank")) or _to_int(row.get("source_rank"))
        if rank is None:
            continue
        additions.append(
            {
                "source": KIPER_SOURCE,
                "snapshot_date": _safe_str(row.get("snapshot_date")) or dt.date.today().isoformat(),
                "source_rank": rank,
                "player_name": _safe_str(row.get("player_name")),
                "school": _safe_str(row.get("school")),
                "position": normalize_pos(_safe_str(row.get("position"))),
                "source_url": _safe_str(row.get("source_url")),
            }
        )

    merged = keep + additions
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _sort_rank(r.get("source_rank"))))
    _write_csv(
        ANALYST_SEED_PATH,
        merged,
        ["source", "snapshot_date", "source_rank", "player_name", "school", "position", "source_url"],
    )
    return len(additions)


def _merge_analyst_reports(kiper_rows: list[dict]) -> int:
    existing = _load_csv(REPORTS_PATH)
    keep = [r for r in existing if _safe_str(r.get("source")) != KIPER_SOURCE]

    additions = []
    for row in kiper_rows:
        rank = _to_int(row.get("kiper_rank")) or _to_int(row.get("source_rank"))
        if rank is None:
            continue
        prev_rank = _to_int(row.get("kiper_prev_rank"))
        rank_delta = _to_int(row.get("kiper_rank_delta"))
        if rank_delta is None and prev_rank is not None:
            rank_delta = prev_rank - rank
        additions.append(
            {
                "source": KIPER_SOURCE,
                "snapshot_date": _safe_str(row.get("snapshot_date")) or dt.date.today().isoformat(),
                "source_rank": rank,
                "player_name": _safe_str(row.get("player_name")),
                "school": _safe_str(row.get("school")),
                "position": normalize_pos(_safe_str(row.get("position"))),
                "source_url": _safe_str(row.get("source_url")),
                "report_text": _render_report_text(row),
                "kiper_rank": rank,
                "kiper_prev_rank": prev_rank if prev_rank is not None else "",
                "kiper_rank_delta": rank_delta if rank_delta is not None else "",
                "kiper_strength_tags": _safe_str(row.get("kiper_strength_tags")),
                "kiper_concern_tags": _safe_str(row.get("kiper_concern_tags")),
                "kiper_statline_2025": _safe_str(row.get("kiper_statline_2025")),
                "kiper_statline_2025_games": _to_float(row.get("kiper_statline_2025_games")) or "",
                "kiper_statline_2025_yards": _to_float(row.get("kiper_statline_2025_yards")) or "",
                "kiper_statline_2025_tds": _to_float(row.get("kiper_statline_2025_tds")) or "",
                "kiper_statline_2025_efficiency": _to_float(row.get("kiper_statline_2025_efficiency")) or "",
            }
        )

    merged = keep + additions
    merged.sort(key=lambda r: (_safe_str(r.get("source")), _sort_rank(r.get("source_rank"))))
    preferred_fields = [
        "source",
        "snapshot_date",
        "source_rank",
        "player_name",
        "school",
        "position",
        "source_url",
        "report_text",
        "kiper_rank",
        "kiper_prev_rank",
        "kiper_rank_delta",
        "kiper_strength_tags",
        "kiper_concern_tags",
        "kiper_statline_2025",
        "kiper_statline_2025_games",
        "kiper_statline_2025_yards",
        "kiper_statline_2025_tds",
        "kiper_statline_2025_efficiency",
    ]
    fields = _union_fieldnames(merged, preferred_fields)
    _write_csv(REPORTS_PATH, merged, fields)
    return len(additions)


def _write_report_md(path: Path, rows: list[dict], added_seed: int, added_reports: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top = sorted(rows, key=lambda r: _to_int(r.get("kiper_rank")) or 9999)[:20]
    lines = [
        "# Kiper Structured Ingest Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- source rows loaded: `{len(rows)}`",
        f"- analyst_rankings_seed rows added: `{added_seed}`",
        f"- analyst_reports rows added: `{added_reports}`",
        "",
        "## Top Rows",
        "",
        "| Rank | Player | Pos | Prev | Delta | Strength Tags | Concern Tags |",
        "|---:|---|---|---:|---:|---|---|",
    ]
    for r in top:
        lines.append(
            f"| {_to_int(r.get('kiper_rank')) or ''} | {_safe_str(r.get('player_name'))} | "
            f"{normalize_pos(_safe_str(r.get('position')))} | {_to_int(r.get('kiper_prev_rank')) or ''} | "
            f"{_to_int(r.get('kiper_rank_delta')) or ''} | {_safe_str(r.get('kiper_strength_tags'))} | "
            f"{_safe_str(r.get('kiper_concern_tags'))} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    if not MANUAL_KIPER_PATH.exists():
        raise FileNotFoundError(
            f"Kiper manual CSV not found: {MANUAL_KIPER_PATH}. "
            "Create it from kiper_2026_board_template.csv and rerun."
        )

    raw_rows = _load_csv(MANUAL_KIPER_PATH)
    if not raw_rows:
        raise RuntimeError(f"Kiper manual CSV is empty: {MANUAL_KIPER_PATH}")

    kiper_rows = _dedupe_best(raw_rows)
    # Keep processed structured rows for model features and QA.
    processed_rows = []
    for row in kiper_rows:
        rank = _to_int(row.get("kiper_rank")) or _to_int(row.get("source_rank"))
        if rank is None:
            continue
        prev_rank = _to_int(row.get("kiper_prev_rank"))
        rank_delta = _to_int(row.get("kiper_rank_delta"))
        if rank_delta is None and prev_rank is not None:
            rank_delta = prev_rank - rank
        processed_rows.append(
            {
                "source": KIPER_SOURCE,
                "snapshot_date": _safe_str(row.get("snapshot_date")) or dt.date.today().isoformat(),
                "source_rank": rank,
                "player_name": _safe_str(row.get("player_name")),
                "school": _safe_str(row.get("school")),
                "position": normalize_pos(_safe_str(row.get("position"))),
                "source_url": _safe_str(row.get("source_url")),
                "kiper_rank": rank,
                "kiper_prev_rank": prev_rank if prev_rank is not None else "",
                "kiper_rank_delta": rank_delta if rank_delta is not None else "",
                "kiper_strength_tags": _safe_str(row.get("kiper_strength_tags")),
                "kiper_concern_tags": _safe_str(row.get("kiper_concern_tags")),
                "kiper_statline_2025": _safe_str(row.get("kiper_statline_2025")),
                "kiper_statline_2025_games": _to_float(row.get("kiper_statline_2025_games")) or "",
                "kiper_statline_2025_yards": _to_float(row.get("kiper_statline_2025_yards")) or "",
                "kiper_statline_2025_tds": _to_float(row.get("kiper_statline_2025_tds")) or "",
                "kiper_statline_2025_efficiency": _to_float(row.get("kiper_statline_2025_efficiency")) or "",
                "report_text": _render_report_text(row),
            }
        )

    _write_csv(
        PROCESSED_KIPER_PATH,
        processed_rows,
        [
            "source",
            "snapshot_date",
            "source_rank",
            "player_name",
            "school",
            "position",
            "source_url",
            "kiper_rank",
            "kiper_prev_rank",
            "kiper_rank_delta",
            "kiper_strength_tags",
            "kiper_concern_tags",
            "kiper_statline_2025",
            "kiper_statline_2025_games",
            "kiper_statline_2025_yards",
            "kiper_statline_2025_tds",
            "kiper_statline_2025_efficiency",
            "report_text",
        ],
    )

    added_seed = _merge_analyst_seed(processed_rows)
    added_reports = _merge_analyst_reports(processed_rows)
    _write_report_md(REPORT_MD, processed_rows, added_seed=added_seed, added_reports=added_reports)

    print(f"Kiper rows processed: {len(processed_rows)}")
    print(f"Analyst seed rows added: {added_seed}")
    print(f"Analyst report rows added: {added_reports}")
    print(f"Processed Kiper rows: {PROCESSED_KIPER_PATH}")
    print(f"Ingest report: {REPORT_MD}")


if __name__ == "__main__":
    main()
