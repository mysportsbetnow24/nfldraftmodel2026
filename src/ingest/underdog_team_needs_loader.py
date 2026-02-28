from __future__ import annotations

import csv
import datetime as dt
import html
import re
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = ROOT / "data" / "sources"
EXTERNAL_DIR = SOURCES_DIR / "external"

DEFAULT_URL = "https://underdognetwork.com/football/news/2026-nfl-team-needs"
DEFAULT_RAW_PATH = EXTERNAL_DIR / "underdog_team_needs_raw_2026.csv"
DEFAULT_NORMALIZED_PATH = EXTERNAL_DIR / "underdog_team_needs_normalized_2026.csv"
DEFAULT_MATRIX_PATH = EXTERNAL_DIR / "underdog_team_needs_matrix_2026.csv"
DEFAULT_TEAM_PATCH_PATH = EXTERNAL_DIR / "underdog_team_profiles_patch_2026.csv"

TEAM_TO_ABBR = {
    "49ers": "SF",
    "Bears": "CHI",
    "Bengals": "CIN",
    "Bills": "BUF",
    "Broncos": "DEN",
    "Browns": "CLE",
    "Buccaneers": "TB",
    "Cardinals": "ARI",
    "Chargers": "LAC",
    "Chiefs": "KC",
    "Colts": "IND",
    "Commanders": "WAS",
    "Cowboys": "DAL",
    "Dolphins": "MIA",
    "Eagles": "PHI",
    "Falcons": "ATL",
    "Giants": "NYG",
    "Jaguars": "JAX",
    "Jets": "NYJ",
    "Lions": "DET",
    "Packers": "GB",
    "Panthers": "CAR",
    "Patriots": "NE",
    "Raiders": "LV",
    "Rams": "LAR",
    "Ravens": "BAL",
    "Saints": "NO",
    "Seahawks": "SEA",
    "Steelers": "PIT",
    "Texans": "HOU",
    "Titans": "TEN",
    "Vikings": "MIN",
}

MODEL_POSITIONS = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]

_NEED_WEIGHTS = [1.00, 0.85, 0.72, 0.60, 0.50, 0.42, 0.35, 0.30]

_TEAM_NEEDS_PATTERN = re.compile(
    # Supports both legacy markdown format and current plain-text heading format:
    # "1. ## Raiders Team Needs (...)" OR "Raiders Team Needs (...)"
    r"(?:^|\n)\s*(?:\d+\.\s*##\s*)?([A-Za-z0-9&.' -]+?)\s+Team Needs\s*\(([^)]*)\)",
    flags=re.IGNORECASE,
)


def fetch_underdog_team_needs_html(url: str = DEFAULT_URL, timeout: int = 20) -> str:
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError("requests is required for live team-needs pull. Install requirements first.") from exc

    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _clean_text_from_html(page_html: str) -> str:
    text = html.unescape(page_html or "")
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _split_needs(needs_blob: str) -> List[str]:
    return [n.strip() for n in (needs_blob or "").split(",") if n.strip()]


def normalize_need(raw_need: str) -> str:
    txt = (raw_need or "").strip().lower()

    if "qb" in txt:
        return "QB"
    if txt in {"rb", "running back"}:
        return "RB"
    if "wr" in txt or "receiver" in txt:
        return "WR"
    if txt == "te" or "tight end" in txt:
        return "TE"

    if txt in {"lt", "rt", "ot", "t", "tackle"}:
        return "OT"
    if txt in {"c", "g", "rg", "lg", "og", "iolg", "iolc"} or "guard" in txt or "center" in txt or "c/rg" in txt:
        return "IOL"

    if txt in {"edge", "de"} or "edge" in txt:
        return "EDGE"
    if txt in {"dt", "nt", "idl"} or "tackle" in txt and "def" in txt:
        return "DT"
    if txt == "lb" or "linebacker" in txt:
        return "LB"

    if txt in {"cb", "outside cb", "slot cb", "nickel"} or "corner" in txt:
        return "CB"
    if txt in {"fs", "ss", "s", "saf"} or "safety" in txt:
        return "S"

    return "OTHER"


def parse_team_needs(page_html: str, source_url: str = DEFAULT_URL) -> List[dict]:
    text = _clean_text_from_html(page_html)
    matches = _TEAM_NEEDS_PATTERN.findall(text)
    out: List[dict] = []

    for idx, (team_name, needs_blob) in enumerate(matches, start=1):
        team_name = team_name.strip()
        team_abbr = TEAM_TO_ABBR.get(team_name, "")
        needs = _split_needs(needs_blob)
        for need_rank, need_raw in enumerate(needs, start=1):
            need_norm = normalize_need(need_raw)
            out.append(
                {
                    "article_rank": idx,
                    "team_name": team_name,
                    "team": team_abbr,
                    "need_rank": need_rank,
                    "need_raw": need_raw,
                    "need_norm": need_norm,
                    "need_weight": _NEED_WEIGHTS[min(need_rank - 1, len(_NEED_WEIGHTS) - 1)],
                    "source_url": source_url,
                    "pulled_on": dt.date.today().isoformat(),
                }
            )
    return out


def build_need_matrix(rows: List[dict]) -> List[dict]:
    by_team: Dict[str, dict] = {}
    for row in rows:
        team = row.get("team") or ""
        if not team:
            continue
        cur = by_team.get(team)
        if cur is None:
            cur = {"team": team}
            for pos in MODEL_POSITIONS:
                cur[pos] = 0.0
            by_team[team] = cur

        pos = row.get("need_norm", "")
        if pos in MODEL_POSITIONS:
            cur[pos] = max(float(cur[pos]), float(row.get("need_weight", 0.0)))

    out = list(by_team.values())
    out.sort(key=lambda r: r["team"])
    for row in out:
        for pos in MODEL_POSITIONS:
            row[pos] = round(float(row[pos]), 2)
    return out


def build_team_profiles_patch(rows: List[dict]) -> List[dict]:
    by_team: Dict[str, List[str]] = {}
    for row in sorted(rows, key=lambda r: (r.get("team", ""), int(r.get("need_rank", 999)))):
        team = row.get("team", "")
        pos = row.get("need_norm", "")
        if not team or pos not in MODEL_POSITIONS:
            continue
        lst = by_team.setdefault(team, [])
        if pos not in lst:
            lst.append(pos)

    out: List[dict] = []
    for team in sorted(by_team):
        needs = by_team[team]
        out.append(
            {
                "team": team,
                "need_1": needs[0] if len(needs) > 0 else "",
                "need_2": needs[1] if len(needs) > 1 else "",
                "need_3": needs[2] if len(needs) > 2 else "",
                "source": "underdog_team_needs_2026",
                "source_url": DEFAULT_URL,
                "pulled_on": dt.date.today().isoformat(),
            }
        )
    return out


def write_csv(rows: List[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
