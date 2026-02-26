from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Tuple

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[2]
PROCESSED_PATH = ROOT / "data" / "processed" / "kiper_structured_2026.csv"
KIPER_SOURCE = "ESPN_Mel_Kiper_2026"


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


def _clamp(v: float, lo: float = 1.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _minmax_norm_by_position(rows: list[dict], field: str) -> Dict[Tuple[str, str], float]:
    by_pos_vals: dict[str, list[float]] = {}
    for row in rows:
        pos = row["position"]
        value = _to_float(row.get(field))
        if value is None:
            continue
        by_pos_vals.setdefault(pos, []).append(value)

    by_pos_bounds: dict[str, tuple[float, float]] = {}
    for pos, vals in by_pos_vals.items():
        by_pos_bounds[pos] = (min(vals), max(vals))

    out: Dict[Tuple[str, str], float] = {}
    for row in rows:
        name_key = row["player_key"]
        pos = row["position"]
        val = _to_float(row.get(field))
        bounds = by_pos_bounds.get(pos)
        if val is None or bounds is None:
            continue
        lo, hi = bounds
        if hi <= lo:
            norm = 50.0
        else:
            norm = ((val - lo) / (hi - lo)) * 100.0
        out[(name_key, pos)] = round(_clamp(norm), 2)
    return out


def load_kiper_structured_signals(path: Path | None = None) -> dict:
    path = path or PROCESSED_PATH
    if not path.exists():
        return {"by_name_pos": {}, "by_name": {}, "meta": {"status": "missing", "rows": 0}}

    rows = []
    with path.open() as f:
        for row in csv.DictReader(f):
            source = str(row.get("source", "")).strip()
            if source != KIPER_SOURCE:
                continue
            name = str(row.get("player_name", "")).strip()
            pos = normalize_pos(str(row.get("position", "")).strip())
            if not name or not pos:
                continue
            name_key = canonical_player_name(name)
            rank = _to_int(row.get("kiper_rank"))
            if rank is None:
                rank = _to_int(row.get("source_rank"))
            prev_rank = _to_int(row.get("kiper_prev_rank"))
            rank_delta = _to_int(row.get("kiper_rank_delta"))
            if rank_delta is None and rank is not None and prev_rank is not None:
                rank_delta = prev_rank - rank

            rows.append(
                {
                    **row,
                    "player_key": name_key,
                    "player_name": name,
                    "position": pos,
                    "kiper_rank": rank,
                    "kiper_prev_rank": prev_rank,
                    "kiper_rank_delta": rank_delta,
                }
            )

    if not rows:
        return {"by_name_pos": {}, "by_name": {}, "meta": {"status": "empty", "rows": 0}}

    games_norm = _minmax_norm_by_position(rows, "kiper_statline_2025_games")
    yards_norm = _minmax_norm_by_position(rows, "kiper_statline_2025_yards")
    tds_norm = _minmax_norm_by_position(rows, "kiper_statline_2025_tds")
    eff_norm = _minmax_norm_by_position(rows, "kiper_statline_2025_efficiency")

    by_name_pos: Dict[Tuple[str, str], dict] = {}
    by_name: Dict[str, dict] = {}
    for row in rows:
        name_key = row["player_key"]
        pos = row["position"]
        key = (name_key, pos)

        rank = row.get("kiper_rank")
        rank_signal = _clamp((301.0 - float(rank)) / 3.0) if rank is not None else 35.0
        rank_delta = row.get("kiper_rank_delta")
        delta_abs = abs(int(rank_delta)) if rank_delta is not None else 0
        if delta_abs >= 12:
            vol_penalty = 1.0
        elif delta_abs >= 8:
            vol_penalty = 0.6
        elif delta_abs >= 5:
            vol_penalty = 0.3
        else:
            vol_penalty = 0.0
        vol_flag = 1 if delta_abs >= 8 else 0

        stat_norm_parts = []
        for mapping in (games_norm, yards_norm, tds_norm, eff_norm):
            if key in mapping:
                stat_norm_parts.append(mapping[key])
        stat_norm = round(sum(stat_norm_parts) / len(stat_norm_parts), 2) if stat_norm_parts else None

        payload = {
            "kiper_rank": rank or "",
            "kiper_prev_rank": row.get("kiper_prev_rank") if row.get("kiper_prev_rank") is not None else "",
            "kiper_rank_delta": rank_delta if rank_delta is not None else "",
            "kiper_rank_signal": round(rank_signal, 2),
            "kiper_strength_tags": str(row.get("kiper_strength_tags", "")).strip(),
            "kiper_concern_tags": str(row.get("kiper_concern_tags", "")).strip(),
            "kiper_statline_2025": str(row.get("kiper_statline_2025", "")).strip(),
            "kiper_statline_2025_games": _to_float(row.get("kiper_statline_2025_games")) or "",
            "kiper_statline_2025_yards": _to_float(row.get("kiper_statline_2025_yards")) or "",
            "kiper_statline_2025_tds": _to_float(row.get("kiper_statline_2025_tds")) or "",
            "kiper_statline_2025_efficiency": _to_float(row.get("kiper_statline_2025_efficiency")) or "",
            "kiper_games_norm": games_norm.get(key, ""),
            "kiper_yards_norm": yards_norm.get(key, ""),
            "kiper_tds_norm": tds_norm.get(key, ""),
            "kiper_efficiency_norm": eff_norm.get(key, ""),
            "kiper_statline_2025_norm": stat_norm if stat_norm is not None else "",
            "kiper_volatility_flag": vol_flag,
            "kiper_volatility_penalty": round(vol_penalty, 2),
            "kiper_source_url": str(row.get("source_url", "")).strip(),
        }

        by_name_pos[key] = payload
        by_name.setdefault(name_key, payload)

    return {"by_name_pos": by_name_pos, "by_name": by_name, "meta": {"status": "ok", "rows": len(rows)}}

