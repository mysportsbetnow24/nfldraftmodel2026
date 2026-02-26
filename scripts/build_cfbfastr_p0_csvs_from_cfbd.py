#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name


CFBD_DIR = ROOT / "data" / "sources" / "cfbd"
MANUAL_DIR = ROOT / "data" / "sources" / "manual"
OUT_REPORT = ROOT / "data" / "outputs" / "cfbfastr_p0_build_report.txt"


def _safe_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _load_cfbd_data(path: Path) -> list[dict]:
    payload = json.loads(path.read_text())
    return list(payload.get("data", []))


def _normalize_team(text: str) -> str:
    return str(text or "").strip()


def _round(value: float | None, n: int = 4) -> str:
    if value is None:
        return ""
    return str(round(float(value), n))


def _build_stats_maps(stats_rows: list[dict]) -> dict:
    # Team totals.
    team_pass_att = defaultdict(float)
    team_rush_car = defaultdict(float)
    team_rec = defaultdict(float)

    # Player totals keyed by canonical name + team.
    player_pass_att = defaultdict(float)
    player_rush_car = defaultdict(float)
    player_rec = defaultdict(float)

    for row in stats_rows:
        team = _normalize_team(row.get("team", ""))
        if not team:
            continue
        name_key = canonical_player_name(row.get("player", ""))
        if not name_key:
            continue
        cat = str(row.get("category", "")).strip().lower()
        stat_type = str(row.get("statType", "")).strip().upper()
        stat = _safe_float(row.get("stat"))
        if stat is None:
            continue

        key = (name_key, team)
        if cat == "passing" and stat_type == "ATT":
            team_pass_att[team] += stat
            player_pass_att[key] += stat
        elif cat == "rushing" and stat_type == "CAR":
            team_rush_car[team] += stat
            player_rush_car[key] += stat
        elif cat == "receiving" and stat_type == "REC":
            team_rec[team] += stat
            player_rec[key] += stat

    return {
        "team_pass_att": team_pass_att,
        "team_rush_car": team_rush_car,
        "team_rec": team_rec,
        "player_pass_att": player_pass_att,
        "player_rush_car": player_rush_car,
        "player_rec": player_rec,
    }


def _build_qb_rows(ppa_rows: list[dict], maps: dict, season: int) -> list[dict]:
    out = []
    for row in ppa_rows:
        if str(row.get("position", "")).strip().upper() != "QB":
            continue
        team = _normalize_team(row.get("team", ""))
        name = str(row.get("name", "")).strip()
        if not team or not name:
            continue
        name_key = canonical_player_name(name)
        avg = row.get("averagePPA", {}) or {}
        usage = None
        team_att = maps["team_pass_att"].get(team, 0.0)
        if team_att > 0:
            usage = maps["player_pass_att"].get((name_key, team), 0.0) / team_att
        out.append(
            {
                "player_name": name,
                "school": team,
                "position": "QB",
                "season": season,
                "qb_ppa_overall": _round(_safe_float(avg.get("all"))),
                "qb_ppa_passing": _round(_safe_float(avg.get("pass"))),
                "qb_ppa_standard_downs": _round(_safe_float(avg.get("standardDowns"))),
                "qb_ppa_passing_downs": _round(_safe_float(avg.get("passingDowns"))),
                "qb_wepa_passing": "",
                "qb_usage_rate": _round(usage),
            }
        )
    return out


def _build_wrte_rows(ppa_rows: list[dict], maps: dict, season: int) -> list[dict]:
    out = []
    for row in ppa_rows:
        pos = str(row.get("position", "")).strip().upper()
        if pos not in {"WR", "TE"}:
            continue
        team = _normalize_team(row.get("team", ""))
        name = str(row.get("name", "")).strip()
        if not team or not name:
            continue
        name_key = canonical_player_name(name)
        avg = row.get("averagePPA", {}) or {}
        usage = None
        team_rec = maps["team_rec"].get(team, 0.0)
        if team_rec > 0:
            usage = maps["player_rec"].get((name_key, team), 0.0) / team_rec
        out.append(
            {
                "player_name": name,
                "school": team,
                "position": pos,
                "season": season,
                "wrte_ppa_overall": _round(_safe_float(avg.get("all"))),
                "wrte_ppa_passing_downs": _round(_safe_float(avg.get("passingDowns"))),
                "wrte_wepa_receiving": "",
                "wrte_usage_rate": _round(usage),
            }
        )
    return out


def _build_rb_rows(ppa_rows: list[dict], maps: dict, season: int) -> list[dict]:
    out = []
    for row in ppa_rows:
        if str(row.get("position", "")).strip().upper() != "RB":
            continue
        team = _normalize_team(row.get("team", ""))
        name = str(row.get("name", "")).strip()
        if not team or not name:
            continue
        name_key = canonical_player_name(name)
        avg = row.get("averagePPA", {}) or {}
        usage = None
        team_car = maps["team_rush_car"].get(team, 0.0)
        if team_car > 0:
            usage = maps["player_rush_car"].get((name_key, team), 0.0) / team_car
        out.append(
            {
                "player_name": name,
                "school": team,
                "position": "RB",
                "season": season,
                "rb_ppa_rushing": _round(_safe_float(avg.get("rush"))),
                "rb_ppa_standard_downs": _round(_safe_float(avg.get("standardDowns"))),
                "rb_wepa_rushing": "",
                "rb_usage_rate": _round(usage),
            }
        )
    return out


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build cfbfastR-style P0 CSVs from CFBD API pulls")
    p.add_argument("--season", type=int, default=2025)
    p.add_argument("--ppa-path", type=Path, default=CFBD_DIR / "player_ppa_2025.json")
    p.add_argument("--stats-path", type=Path, default=CFBD_DIR / "player_season_stats_2025.json")
    p.add_argument("--out-dir", type=Path, default=MANUAL_DIR)
    p.add_argument("--report", type=Path, default=OUT_REPORT)
    return p


def main() -> None:
    args = build_parser().parse_args()

    if not args.ppa_path.exists() or not args.stats_path.exists():
        raise SystemExit(
            "Missing CFBD source files. Pull these first:\n"
            "- player_ppa\n- player_season_stats"
        )

    ppa_rows = _load_cfbd_data(args.ppa_path)
    stats_rows = _load_cfbd_data(args.stats_path)
    maps = _build_stats_maps(stats_rows)

    qb_rows = _build_qb_rows(ppa_rows, maps, args.season)
    wrte_rows = _build_wrte_rows(ppa_rows, maps, args.season)
    rb_rows = _build_rb_rows(ppa_rows, maps, args.season)

    qb_path = args.out_dir / f"cfbfastr_qb_p0_{args.season}.csv"
    wrte_path = args.out_dir / f"cfbfastr_wrte_p0_{args.season}.csv"
    rb_path = args.out_dir / f"cfbfastr_rb_p0_{args.season}.csv"

    _write_csv(
        qb_path,
        qb_rows,
        [
            "player_name",
            "school",
            "position",
            "season",
            "qb_ppa_overall",
            "qb_ppa_passing",
            "qb_ppa_standard_downs",
            "qb_ppa_passing_downs",
            "qb_wepa_passing",
            "qb_usage_rate",
        ],
    )
    _write_csv(
        wrte_path,
        wrte_rows,
        [
            "player_name",
            "school",
            "position",
            "season",
            "wrte_ppa_overall",
            "wrte_ppa_passing_downs",
            "wrte_wepa_receiving",
            "wrte_usage_rate",
        ],
    )
    _write_csv(
        rb_path,
        rb_rows,
        [
            "player_name",
            "school",
            "position",
            "season",
            "rb_ppa_rushing",
            "rb_ppa_standard_downs",
            "rb_wepa_rushing",
            "rb_usage_rate",
        ],
    )

    lines = [
        "cfbfastR-style P0 CSV Build Report",
        "",
        f"season: {args.season}",
        f"source_ppa: {args.ppa_path}",
        f"source_stats: {args.stats_path}",
        "",
        f"qb_rows: {len(qb_rows)}",
        f"wrte_rows: {len(wrte_rows)}",
        f"rb_rows: {len(rb_rows)}",
        "",
        "outputs:",
        f"- {qb_path}",
        f"- {wrte_path}",
        f"- {rb_path}",
        "",
        "notes:",
        "- WEPA fields are left blank in this build because CFBD API payloads used here do not include WEPA directly.",
        "- Usage rates are derived from player/team season shares: QB pass attempts share, WR/TE receptions share, RB carry share.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {qb_path}")
    print(f"Wrote: {wrte_path}")
    print(f"Wrote: {rb_path}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
