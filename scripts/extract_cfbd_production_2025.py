#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
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


def _load_json_data(path: Path) -> list[dict]:
    payload = json.loads(path.read_text())
    return list(payload.get("data", []))


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


def main() -> None:
    player_stats_path = CFBD_DIR / "player_season_stats_2025.json"
    player_ppa_path = CFBD_DIR / "player_ppa_2025.json"
    if not player_stats_path.exists() or not player_ppa_path.exists():
        raise SystemExit("Missing CFBD source files. Pull player_season_stats and player_ppa first.")

    board_map = _load_board()
    stats_rows = _load_json_data(player_stats_path)
    ppa_rows = _load_json_data(player_ppa_path)
    stats_by_player, team_rec_totals = _aggregate_stats(stats_rows)
    ppa_by_player = _aggregate_ppa(ppa_rows)

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
            "qb_qbr": "",
            "qb_epa_per_play": "",
            "qb_success_rate": "",
            "qb_pressure_to_sack_rate": "",
            "qb_under_pressure_epa": "",
            "qb_under_pressure_success_rate": "",
            "yprr": "",
            "target_share": "",
            "explosive_run_rate": "",
            "missed_tackles_forced_per_touch": "",
            "pressure_rate": "",
            "coverage_plays_per_target": "",
            "cfb_prod_quality_label": "",
            "cfb_prod_reliability": "",
            "cfb_prod_real_features": "",
            "cfb_prod_proxy_features": "",
            "cfb_prod_provenance": "cfbd_stats+cfbd_ppa_proxy",
            "source": "CFBD_2025_proxy_extract",
        }

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

        elif board_pos == "RB":
            ypc = _get_stat(stats or {}, "rushing", "YPC")
            ppa_rush = _safe_float((ppa or {}).get("avg_rush"))
            if ypc is not None:
                base = 0.06 + (ypc - 3.6) * 0.03
                if ppa_rush is not None:
                    base += _clamp(ppa_rush, -0.20, 0.40) * 0.15
                row["explosive_run_rate"] = round(_clamp(base, 0.04, 0.26), 4)

        elif board_pos == "EDGE":
            pressures = (_get_stat(stats or {}, "defensive", "QB HUR") or 0.0) + (_get_stat(stats or {}, "defensive", "SACKS") or 0.0)
            if pressures > 0:
                pr = 0.05 + (0.20 * _pct_rank(pressures, edge_pressures))
                row["pressure_rate"] = round(_clamp(pr, 0.05, 0.26), 4)

        elif board_pos in {"CB", "S"}:
            cov_plays = (_get_stat(stats or {}, "defensive", "PD") or 0.0) + (_get_stat(stats or {}, "interceptions", "INT") or 0.0)
            if cov_plays > 0:
                cpt = 0.06 + (0.24 * _pct_rank(cov_plays, db_cov))
                row["coverage_plays_per_target"] = round(_clamp(cpt, 0.06, 0.32), 4)

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
            proxy_fields = ["yprr", "target_share"]
        elif board_pos == "RB":
            proxy_fields = ["explosive_run_rate", "missed_tackles_forced_per_touch"]
        elif board_pos == "EDGE":
            proxy_fields = ["pressure_rate"]
        elif board_pos in {"CB", "S"}:
            proxy_fields = ["coverage_plays_per_target"]
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
                "qb_qbr",
                "qb_epa_per_play",
                "qb_success_rate",
                "qb_pressure_to_sack_rate",
                "qb_under_pressure_epa",
                "qb_under_pressure_success_rate",
                "yprr",
                "target_share",
                "explosive_run_rate",
                "missed_tackles_forced_per_touch",
                "pressure_rate",
                "coverage_plays_per_target",
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
    for r in out_rows:
        by_pos[r["position"]] += 1
        quality_counts[r.get("cfb_prod_quality_label", "unknown")] += 1

    lines = [
        "CFBD 2025 Production Extraction Report",
        "",
        f"board_players_targeted: {len(board_map)}",
        f"players_matched_cfbd: {matched}",
        f"rows_written: {len(out_rows)}",
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
