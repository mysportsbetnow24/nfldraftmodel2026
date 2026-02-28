#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from src.ingest.rankings_loader import canonical_player_name, normalize_pos


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = ROOT / "data" / "sources" / "external" / "espn_nfl_draft_prospect_data"
CONSENSUS_PATH = ROOT / "data" / "processed" / "consensus_big_boards_2026.csv"
ANALYST_REPORTS_PATH = ROOT / "data" / "processed" / "analyst_reports_2026.csv"
SEED_PATH = ROOT / "data" / "processed" / "prospect_seed_2026.csv"
BOARD_PATH = ROOT / "data" / "outputs" / "big_board_2026.csv"
CFB_PROD_PATH = ROOT / "data" / "sources" / "manual" / "cfb_production_2025.csv"
CFBD_STATS_PATH = ROOT / "data" / "sources" / "cfbd" / "player_season_stats_2025.json"
RETURNING_PATH = ROOT / "data" / "sources" / "manual" / "returning_to_school_2026.csv"
IN_NFL_PATH = ROOT / "data" / "sources" / "manual" / "already_in_nfl_exclusions.csv"

PROSPECTS_FILE = "nfl_draft_prospects.csv"
PROFILES_FILE = "nfl_draft_profiles.csv"
QBR_FILE = "college_qbr.csv"
STATS_FILE = "college_statistics.csv"
IDS_FILE = "ids.csv"
README_FILE = "README.txt"

VALID_POS = {"QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"}

BAD_TEXT_PATTERNS = [
    "css-",
    "@media",
    "self.__next",
    "mui",
    "http://www.w3.org",
]


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _height_to_inches(raw: str) -> int | None:
    txt = (raw or "").strip()
    if not txt:
        return None
    m = re.match(r"^\s*(\d+)\s*'\s*(\d+)\s*\"?\s*$", txt)
    if m:
        return int(m.group(1)) * 12 + int(m.group(2))
    if txt.isdigit():
        v = int(txt)
        if 58 <= v <= 90:
            return v
    return None


def _safe_text(value: str, max_len: int = 320) -> str:
    txt = (value or "").strip()
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    if not txt:
        return ""
    lower = txt.lower()
    if any(p in lower for p in BAD_TEXT_PATTERNS):
        return ""
    if len(txt) > max_len:
        txt = txt[: max_len - 3].rstrip() + "..."
    return txt


def _abbr_school(school: str) -> str:
    toks = [t for t in re.split(r"\s+", (school or "").strip()) if t]
    if not toks:
        return ""
    if len(toks) == 1:
        return toks[0][:4].upper()
    return "".join(t[0] for t in toks[:4]).upper()


def _load_exclusions() -> set[str]:
    out: set[str] = set()
    for p, key in [(RETURNING_PATH, "player_name"), (IN_NFL_PATH, "player_name")]:
        for row in _load_csv(p):
            name = canonical_player_name(row.get(key, ""))
            if name:
                out.add(name)
    return out


def _aggregate_consensus_players() -> dict[str, dict]:
    rows = _load_csv(CONSENSUS_PATH)
    by_name: dict[str, dict] = {}
    for r in rows:
        name = (r.get("player_name") or "").strip()
        if not name:
            continue
        key = canonical_player_name(name)
        pos = normalize_pos(r.get("position", ""))
        if pos not in VALID_POS:
            continue
        rank_raw = (r.get("consensus_rank") or "").strip()
        try:
            rank = float(rank_raw)
        except ValueError:
            continue
        item = by_name.setdefault(
            key,
            {
                "player_name": name,
                "positions": Counter(),
                "schools": Counter(),
                "ranks": [],
                "sources": set(),
            },
        )
        item["player_name"] = name if len(name) > len(item["player_name"]) else item["player_name"]
        item["positions"][pos] += 1
        item["schools"][(r.get("school") or "").strip()] += 1
        item["ranks"].append(rank)
        item["sources"].add((r.get("source") or "").strip())
    return by_name


def _seed_size_map() -> dict[tuple[str, str], dict]:
    size_map: dict[tuple[str, str], dict] = {}
    for row in _load_csv(SEED_PATH):
        name_key = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("pos_raw", ""))
        if not name_key or not pos:
            continue
        h_in = _height_to_inches(row.get("height", ""))
        w = (row.get("weight_lb") or "").strip()
        try:
            wv = int(float(w)) if w else None
        except ValueError:
            wv = None
        size_map[(name_key, pos)] = {"height_in": h_in, "weight_lb": wv}

    # Backfill from current board where available.
    for row in _load_csv(BOARD_PATH):
        name_key = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position") or row.get("pos_raw") or "")
        if not name_key or not pos:
            continue
        h_raw = (row.get("height_in") or "").strip()
        w_raw = (row.get("weight_lb_effective") or row.get("weight_lb") or "").strip()
        try:
            h_in = int(float(h_raw)) if h_raw else None
        except ValueError:
            h_in = None
        try:
            wv = int(float(w_raw)) if w_raw else None
        except ValueError:
            wv = None
        cur = size_map.get((name_key, pos), {})
        if not cur:
            size_map[(name_key, pos)] = {"height_in": h_in, "weight_lb": wv}
        else:
            if cur.get("height_in") is None and h_in is not None:
                cur["height_in"] = h_in
            if cur.get("weight_lb") is None and wv is not None:
                cur["weight_lb"] = wv
    return size_map


def _build_text_map(valid_players: set[str]) -> dict[str, list[str]]:
    text_map: dict[str, list[str]] = defaultdict(list)
    for row in _load_csv(ANALYST_REPORTS_PATH):
        key = canonical_player_name(row.get("player_name", ""))
        if key not in valid_players:
            continue
        fields = [
            row.get("tdn_strengths", ""),
            row.get("tdn_concerns", ""),
            row.get("tdn_summary", ""),
            row.get("br_strengths", ""),
            row.get("br_concerns", ""),
            row.get("br_summary", ""),
            row.get("si_strengths", ""),
            row.get("si_concerns", ""),
            row.get("si_summary", ""),
            row.get("report_text", ""),
        ]
        cleaned = [_safe_text(f, max_len=300) for f in fields]
        cleaned = [c for c in cleaned if c]
        if cleaned:
            text_map[key].extend(cleaned[:3])
    return text_map


def _build_qbr_rows(prospects: list[dict]) -> list[dict]:
    qbs = {canonical_player_name(r["player_name"]) for r in prospects if normalize_pos(r.get("position", "")) == "QB"}
    rows = []
    for r in _load_csv(CFB_PROD_PATH):
        key = canonical_player_name(r.get("player_name", ""))
        if key not in qbs:
            continue
        qbr = (r.get("qb_qbr") or "").strip()
        epa = (r.get("qb_epa_per_play") or "").strip()
        if not qbr and not epa:
            continue
        rows.append(
            {
                "season": "2025",
                "player_id": f"qbr_{key}",
                "player_name": r.get("player_name", ""),
                "school": r.get("school", ""),
                "position": "QB",
                "qbr": qbr,
                "epa_per_play": epa,
            }
        )
    return rows


def _build_college_stats_rows(prospects: list[dict]) -> list[dict]:
    valid = {canonical_player_name(r["player_name"]): r for r in prospects}
    if not CFBD_STATS_PATH.exists():
        return []

    cfbd = json.loads(CFBD_STATS_PATH.read_text())
    items = cfbd.get("data", [])

    stat_map = {
        ("passing", "ATT"): "passattempts",
        ("passing", "COMPLETIONS"): "completions",
        ("passing", "INT"): "interceptions",
        ("passing", "TD"): "passingtd",
        ("passing", "YDS"): "passingyards",
        ("rushing", "CAR"): "carries",
        ("rushing", "TD"): "rushingtd",
        ("rushing", "YDS"): "rushingyards",
        ("receiving", "REC"): "receptions",
        ("receiving", "TD"): "receivingtd",
        ("receiving", "YDS"): "receivingyards",
        ("defensive", "SACKS"): "sacks",
        ("defensive", "TFL"): "tacklesforloss",
        ("defensive", "TOT"): "totaltackles",
        ("defensive", "PD"): "passbreakups",
        ("interceptions", "INT"): "interceptionsdef",
    }

    out = []
    for row in items:
        if str(row.get("season", "")) != "2025":
            continue
        key = canonical_player_name(row.get("player", ""))
        if key not in valid:
            continue
        cat = (row.get("category") or "").strip()
        st = (row.get("statType") or "").strip()
        mapped = stat_map.get((cat, st))
        if not mapped:
            continue
        stat = (row.get("stat") or "").strip()
        if stat == "":
            continue
        try:
            float(stat)
        except ValueError:
            continue
        p = valid[key]
        out.append(
            {
                "player_id": p["player_id"],
                "alt_player_id": "",
                "player_name": p["player_name"],
                "pos_abbr": p["position"],
                "school": p["school"],
                "school_abbr": _abbr_school(p["school"]),
                "school_primary_color": "",
                "school_alt_color": "",
                "season": "2025",
                "statistic": mapped,
                "value": stat,
                "active": "TRUE",
                "all_star": "",
            }
        )
    return out


def build_dataset(out_dir: Path) -> dict:
    exclusions = _load_exclusions()
    size_map = _seed_size_map()
    consensus = _aggregate_consensus_players()

    # Build ranked player list from consensus aggregation.
    players = []
    for key, item in consensus.items():
        if key in exclusions:
            continue
        name = item["player_name"]
        pos = item["positions"].most_common(1)[0][0] if item["positions"] else ""
        if pos not in VALID_POS:
            continue
        school = item["schools"].most_common(1)[0][0] if item["schools"] else ""
        mean_rank = sum(item["ranks"]) / max(1, len(item["ranks"]))
        players.append(
            {
                "key": key,
                "player_name": name,
                "position": pos,
                "school": school,
                "mean_rank": mean_rank,
                "source_count": len(item["sources"]),
            }
        )

    players.sort(key=lambda r: (r["mean_rank"], r["player_name"]))

    # Add seed-only players if not in consensus.
    existing = {p["key"] for p in players}
    for row in _load_csv(SEED_PATH):
        key = canonical_player_name(row.get("player_name", ""))
        if not key or key in existing or key in exclusions:
            continue
        pos = normalize_pos(row.get("pos_raw", ""))
        if pos not in VALID_POS:
            continue
        try:
            seed_rank = float((row.get("rank_seed") or "500").strip())
        except ValueError:
            seed_rank = 500.0
        players.append(
            {
                "key": key,
                "player_name": row.get("player_name", "").strip(),
                "position": pos,
                "school": row.get("school", "").strip(),
                "mean_rank": max(350.0, seed_rank + 200.0),
                "source_count": 1,
            }
        )
        existing.add(key)

    players.sort(key=lambda r: (r["mean_rank"], r["player_name"]))

    # Assign ovr/pos rank + grade.
    pos_counters: dict[str, int] = defaultdict(int)
    text_map = _build_text_map({p["key"] for p in players})

    prospects_rows: list[dict] = []
    profiles_rows: list[dict] = []
    ids_rows: list[dict] = []

    for i, p in enumerate(players, start=1):
        pos = p["position"]
        pos_counters[pos] += 1
        pos_rk = pos_counters[pos]
        src_n = int(p["source_count"])

        # Soft grade curve from rank + confidence.
        grade = 94.5 - (i - 1) * 0.05 + min(1.2, 0.22 * max(0, src_n - 1))
        grade = max(70.0, min(95.0, grade))

        sz = size_map.get((p["key"], pos), {})
        h_in = sz.get("height_in")
        w_lb = sz.get("weight_lb")

        player_id = f"2026_{i:04d}_{p['key'].replace(' ', '_')[:40]}"
        profile_snips = text_map.get(p["key"], [])

        text1 = profile_snips[0] if len(profile_snips) > 0 else f"Consensus profile built from {src_n} sources."
        text2 = profile_snips[1] if len(profile_snips) > 1 else f"Current market slot: overall rank {i}, position rank {pos_rk}."
        text3 = profile_snips[2] if len(profile_snips) > 2 else "Verify full All-22 and testing stack before lock."
        text4 = f"Board movement note: anchored by consensus source count {src_n}."

        prospect = {
            "draft_year": "2026",
            "player_id": player_id,
            "player_name": p["player_name"],
            "position": pos,
            "pos_abbr": pos,
            "school": p["school"],
            "school_name": p["school"],
            "school_abbr": _abbr_school(p["school"]),
            "link": "",
            "pick": "",
            "overall": "",
            "round": "",
            "traded": "",
            "trade_note": "",
            "team": "",
            "team_abbr": "",
            "team_logo_espn": "",
            "pos_rk": str(pos_rk),
            "ovr_rk": str(i),
            "grade": f"{grade:.2f}",
            "player_image": "",
            "height": str(h_in) if h_in is not None else "",
            "weight": str(w_lb) if w_lb is not None else "",
        }
        prospects_rows.append(prospect)

        profile = {
            "player_id": player_id,
            "guid": player_id,
            "alt_player_id": "",
            "player_name": p["player_name"],
            "position": pos,
            "pos_abbr": pos,
            "weight": str(w_lb) if w_lb is not None else "",
            "height": str(h_in) if h_in is not None else "",
            "player_image": "",
            "link": "",
            "school_logo": "",
            "school": p["school"],
            "school_abbr": _abbr_school(p["school"]),
            "school_name": p["school"],
            "pos_rk": str(pos_rk),
            "ovr_rk": str(i),
            "grade": f"{grade:.2f}",
            "text1": text1,
            "text2": text2,
            "text3": text3,
            "text4": text4,
        }
        profiles_rows.append(profile)

        ids_rows.append(
            {
                "player_id": player_id,
                "player_name": p["player_name"],
                "school": p["school"],
                "espn_id": player_id,
            }
        )

    qbr_rows = _build_qbr_rows(prospects_rows)
    stats_rows = _build_college_stats_rows(prospects_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        out_dir / PROSPECTS_FILE,
        prospects_rows,
        [
            "draft_year",
            "player_id",
            "player_name",
            "position",
            "pos_abbr",
            "school",
            "school_name",
            "school_abbr",
            "link",
            "pick",
            "overall",
            "round",
            "traded",
            "trade_note",
            "team",
            "team_abbr",
            "team_logo_espn",
            "pos_rk",
            "ovr_rk",
            "grade",
            "player_image",
            "height",
            "weight",
        ],
    )
    _write_csv(
        out_dir / PROFILES_FILE,
        profiles_rows,
        [
            "player_id",
            "guid",
            "alt_player_id",
            "player_name",
            "position",
            "pos_abbr",
            "weight",
            "height",
            "player_image",
            "link",
            "school_logo",
            "school",
            "school_abbr",
            "school_name",
            "pos_rk",
            "ovr_rk",
            "grade",
            "text1",
            "text2",
            "text3",
            "text4",
        ],
    )
    _write_csv(
        out_dir / QBR_FILE,
        qbr_rows,
        ["season", "player_id", "player_name", "school", "position", "qbr", "epa_per_play"],
    )
    _write_csv(
        out_dir / STATS_FILE,
        stats_rows,
        [
            "player_id",
            "alt_player_id",
            "player_name",
            "pos_abbr",
            "school",
            "school_abbr",
            "school_primary_color",
            "school_alt_color",
            "season",
            "statistic",
            "value",
            "active",
            "all_star",
        ],
    )
    _write_csv(out_dir / IDS_FILE, ids_rows, ["player_id", "player_name", "school", "espn_id"])
    (out_dir / "college_stats.csv").write_text((out_dir / STATS_FILE).read_text(encoding="utf-8"), encoding="utf-8")

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "out_dir": str(out_dir),
        "prospects_rows": len(prospects_rows),
        "profiles_rows": len(profiles_rows),
        "college_qbr_rows": len(qbr_rows),
        "college_stats_rows": len(stats_rows),
        "ids_rows": len(ids_rows),
    }
    (out_dir / README_FILE).write_text(
        "\n".join(
            [
                "Generated ESPN-style 2026 dataset from live local sources.",
                f"generated_at={report['generated_at']}",
                f"prospects_rows={report['prospects_rows']}",
                f"profiles_rows={report['profiles_rows']}",
                f"college_qbr_rows={report['college_qbr_rows']}",
                f"college_stats_rows={report['college_stats_rows']}",
                f"ids_rows={report['ids_rows']}",
                "",
                "Inputs:",
                f"- {CONSENSUS_PATH}",
                f"- {ANALYST_REPORTS_PATH}",
                f"- {SEED_PATH}",
                f"- {BOARD_PATH}",
                f"- {CFB_PROD_PATH}",
                f"- {CFBD_STATS_PATH}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ESPN-style 2026 draft dataset from live project sources")
    parser.add_argument("--out-dir", type=str, default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()

    report = build_dataset(Path(args.out_dir))
    print(f"Wrote ESPN-style dataset to: {report['out_dir']}")
    print(
        "Rows:",
        f"prospects={report['prospects_rows']}",
        f"profiles={report['profiles_rows']}",
        f"qbr={report['college_qbr_rows']}",
        f"stats={report['college_stats_rows']}",
        f"ids={report['ids_rows']}",
    )


if __name__ == "__main__":
    main()
