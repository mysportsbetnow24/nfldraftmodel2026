from __future__ import annotations

import csv
import html
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Tuple

from src.ingest.rankings_loader import canonical_player_name, normalize_pos
from src.modeling.film_traits import POSITION_FILM_TRAIT_WEIGHTS


ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT / "data" / "processed"
DEFAULT_SIGNAL_PATH = PROCESSED_DIR / "analyst_linguistic_signals_2026.csv"

ESPN_SOURCE = "ESPN_Jordan_Reid_2026"
DJ_SOURCE = "NFL_Daniel_Jeremiah_2026"

ESPN_URL = (
    "https://www.espn.com/nfl/draft2026/story/_/id/47027232/"
    "2026-nfl-draft-rankings-jordan-reid-top-prospects-players-positions"
)
DJ_URL = "https://www.nfl.com/news/daniel-jeremiah-s-top-50-2026-nfl-draft-prospect-rankings-2-0"

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"[a-z0-9']+")


LANG_POSITIVE = {
    "processing": {"processing", "diagnose", "recognition", "awareness", "instincts", "read", "decision"},
    "technique": {"footwork", "hand usage", "mechanics", "leverage", "pad level", "route running", "timing"},
    "explosiveness": {"burst", "explosive", "acceleration", "sudden", "speed", "get-off", "bend"},
    "physicality": {"power", "strength", "anchor", "violent", "contact balance", "play strength"},
    "competitiveness": {"motor", "effort", "tough", "relentless", "competitive", "finisher"},
    "versatility": {"versatile", "multiple", "scheme", "alignment", "inside", "outside", "hybrid"},
}

LANG_RISK = {
    "risk": {
        "raw",
        "inconsistent",
        "developmental",
        "streaky",
        "limited",
        "must improve",
        "question",
        "concern",
        "struggles",
    }
}


POS_KEYWORD_HINTS: Dict[str, set[str]] = {
    "QB": {"pocket presence", "arm talent", "anticipation", "ball placement", "progression", "timing"},
    "RB": {"vision", "contact balance", "burst", "cutback", "pass protection", "home run"},
    "WR": {"release", "separation", "route running", "ball skills", "yac", "contested catch"},
    "TE": {"inline", "detach", "seam", "route running", "block", "mismatch"},
    "OT": {"kick slide", "anchor", "mirror", "hand usage", "set point", "recovery"},
    "IOL": {"leverage", "anchor", "hand placement", "combo block", "reach", "power"},
    "EDGE": {"get-off", "bend", "counter", "rush plan", "long arm", "edge set"},
    "DT": {"first-step", "leverage", "stack and shed", "hand usage", "power", "pad level"},
    "LB": {"trigger", "diagnose", "range", "fit", "block deconstruction", "coverage"},
    "CB": {"press", "off-man", "transition", "recovery", "ball skills", "mirror"},
    "S": {"angles", "range", "trigger", "communication", "alley", "coverage"},
}

# Add film-chart traits as Miller-style linguistic anchors by position.
for _pos, _weights in POSITION_FILM_TRAIT_WEIGHTS.items():
    hints = POS_KEYWORD_HINTS.setdefault(_pos, set())
    for _trait in _weights:
        hints.add(_trait.replace("_", " "))


def _strip_tags(raw: str) -> str:
    txt = TAG_RE.sub(" ", raw or "")
    txt = html.unescape(txt)
    return WS_RE.sub(" ", txt).strip()


def _clean_html_text(raw: str) -> str:
    txt = _strip_tags(raw)
    txt = txt.replace("\u00a0", " ")
    return WS_RE.sub(" ", txt).strip()


def _safe_int(value: object) -> int | None:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _count_hits(text: str, terms: Iterable[str]) -> int:
    return sum(text.count(term) for term in terms)


def _clamp(v: float, lo: float = 20.0, hi: float = 95.0) -> float:
    return max(lo, min(hi, v))


def parse_espn_top50_page(page_html: str, snapshot_date: str, source_url: str = ESPN_URL) -> List[dict]:
    out: List[dict] = []
    rank_block = re.compile(
        r"<h2>\s*(?P<rank>\d+)\.\s*(?P<title>.*?)</h2>(?P<body>.*?)(?=(?:<h2>\s*\d+\.)|<h2>Rankings at every position|</article>)",
        re.I | re.S,
    )

    for m in rank_block.finditer(page_html):
        rank = _safe_int(m.group("rank"))
        if rank is None or rank < 1 or rank > 50:
            continue

        title_txt = _clean_html_text(m.group("title"))
        parts = [p.strip(" .") for p in title_txt.split(",")]
        if len(parts) < 3:
            continue

        player_name = parts[0]
        position = normalize_pos(parts[1])
        school = parts[2]

        paragraphs = re.findall(r"<p>(.*?)</p>", m.group("body"), flags=re.I | re.S)
        notes: List[str] = []
        for p in paragraphs:
            txt = _clean_html_text(p)
            low = txt.lower()
            if "height:" in low and "weight:" in low:
                continue
            if not txt:
                continue
            notes.append(txt)
        report_text = " ".join(notes).strip()

        out.append(
            {
                "source": ESPN_SOURCE,
                "snapshot_date": snapshot_date,
                "source_rank": rank,
                "player_name": player_name,
                "school": school,
                "position": position,
                "source_url": source_url,
                "report_text": report_text,
            }
        )

    out.sort(key=lambda r: int(r["source_rank"]))
    return out


def parse_dj_top50_page(page_html: str, snapshot_date: str, source_url: str = DJ_URL) -> List[dict]:
    out: List[dict] = []
    block_re = re.compile(
        r"(?P<item><div class=\"nfl-o-ranked-item nfl-is-ranked-player\".*?</div>\s*</div>\s*</div>)\s*"
        r"(?P<report><div class=\"nfl-c-body-part--text\">.*?</div>)",
        re.I | re.S,
    )

    for m in block_re.finditer(page_html):
        item_html = m.group("item")
        report_html = m.group("report")

        rank_match = re.search(r"nfl-o-ranked-item__label--second\">\s*(\d+)\s*<", item_html, flags=re.I)
        if not rank_match:
            continue
        rank = _safe_int(rank_match.group(1))
        if rank is None or rank < 1 or rank > 50:
            continue

        name_match = re.search(r"nfl-o-ranked-item__title\">\s*<a [^>]*>(.*?)</a>", item_html, flags=re.I | re.S)
        if not name_match:
            continue
        player_name = _clean_html_text(name_match.group(1))

        info_match = re.search(r"nfl-o-ranked-item__info\">(.*?)</div>", item_html, flags=re.I | re.S)
        if not info_match:
            continue
        spans = [_clean_html_text(s) for s in re.findall(r"<span>(.*?)</span>", info_match.group(1), flags=re.I | re.S)]
        spans = [s for s in spans if s and s != "·" and s != "&middot;"]
        school = spans[0] if spans else ""
        pos_block = spans[1] if len(spans) > 1 else ""
        pos_txt = pos_block.split("·", 1)[0].strip()
        position = normalize_pos(pos_txt)

        report_paras = re.findall(r"<p>(.*?)</p>", report_html, flags=re.I | re.S)
        report_text = " ".join(_clean_html_text(p) for p in report_paras if _clean_html_text(p)).strip()

        out.append(
            {
                "source": DJ_SOURCE,
                "snapshot_date": snapshot_date,
                "source_rank": rank,
                "player_name": player_name,
                "school": school,
                "position": position,
                "source_url": source_url,
                "report_text": report_text,
            }
        )

    out.sort(key=lambda r: int(r["source_rank"]))
    return out


def compute_linguistic_features(text: str, position: str) -> dict:
    clean = (text or "").strip().lower()
    words = WORD_RE.findall(clean)
    word_count = len(words)

    if not clean:
        return {
            "lang_text_coverage": 0,
            "lang_trait_processing": 50.0,
            "lang_trait_technique": 50.0,
            "lang_trait_explosiveness": 50.0,
            "lang_trait_physicality": 50.0,
            "lang_trait_competitiveness": 50.0,
            "lang_trait_versatility": 50.0,
            "lang_miller_keyword_hits": 0,
            "lang_miller_coverage": 0.0,
            "lang_risk_hits": 0,
            "lang_risk_flag": 0,
            "lang_trait_composite": 50.0,
        }

    trait_scores = {}
    for bucket, terms in LANG_POSITIVE.items():
        pos_hits = _count_hits(clean, terms)
        neg_hits = _count_hits(clean, LANG_RISK["risk"])
        trait_scores[bucket] = round(_clamp(50.0 + 7.0 * pos_hits - 2.5 * neg_hits), 2)

    pos_terms = POS_KEYWORD_HINTS.get(position, set())
    unique_hits = sum(1 for term in pos_terms if term in clean)
    coverage = round(unique_hits / len(pos_terms), 4) if pos_terms else 0.0

    risk_hits = _count_hits(clean, LANG_RISK["risk"])
    risk_flag = 1 if risk_hits >= 2 else 0

    base_comp = (
        0.20 * trait_scores["processing"]
        + 0.20 * trait_scores["technique"]
        + 0.15 * trait_scores["explosiveness"]
        + 0.15 * trait_scores["physicality"]
        + 0.15 * trait_scores["competitiveness"]
        + 0.15 * trait_scores["versatility"]
    )
    miller_bonus = 12.0 * coverage
    verbosity_bonus = min(6.0, word_count / 45.0)
    risk_penalty = 2.5 * risk_hits
    composite = round(_clamp(base_comp + miller_bonus + verbosity_bonus - risk_penalty), 2)

    return {
        "lang_text_coverage": word_count,
        "lang_trait_processing": trait_scores["processing"],
        "lang_trait_technique": trait_scores["technique"],
        "lang_trait_explosiveness": trait_scores["explosiveness"],
        "lang_trait_physicality": trait_scores["physicality"],
        "lang_trait_competitiveness": trait_scores["competitiveness"],
        "lang_trait_versatility": trait_scores["versatility"],
        "lang_miller_keyword_hits": unique_hits,
        "lang_miller_coverage": coverage,
        "lang_risk_hits": risk_hits,
        "lang_risk_flag": risk_flag,
        "lang_trait_composite": composite,
    }


def aggregate_linguistic_signals(rows: List[dict]) -> List[dict]:
    grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for row in rows:
        name = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position", ""))
        if not name or not pos:
            continue
        grouped[(name, pos)].append(row)

    out = []
    for (name_key, pos), grp in sorted(grouped.items()):
        feature_rows = [compute_linguistic_features(g.get("report_text", ""), pos) for g in grp]
        sample = grp[0]

        def mean(field: str) -> float:
            vals = [float(r[field]) for r in feature_rows]
            return round(sum(vals) / len(vals), 4) if vals else 0.0

        out.append(
            {
                "player_key": name_key,
                "position": pos,
                "player_name": sample.get("player_name", ""),
                "school": sample.get("school", ""),
                "lang_source_count": len(grp),
                "lang_text_coverage": int(round(mean("lang_text_coverage"))),
                "lang_trait_processing": round(mean("lang_trait_processing"), 2),
                "lang_trait_technique": round(mean("lang_trait_technique"), 2),
                "lang_trait_explosiveness": round(mean("lang_trait_explosiveness"), 2),
                "lang_trait_physicality": round(mean("lang_trait_physicality"), 2),
                "lang_trait_competitiveness": round(mean("lang_trait_competitiveness"), 2),
                "lang_trait_versatility": round(mean("lang_trait_versatility"), 2),
                "lang_miller_keyword_hits": int(round(mean("lang_miller_keyword_hits"))),
                "lang_miller_coverage": round(mean("lang_miller_coverage"), 4),
                "lang_risk_hits": int(round(mean("lang_risk_hits"))),
                "lang_risk_flag": 1 if mean("lang_risk_flag") >= 0.5 else 0,
                "lang_trait_composite": round(mean("lang_trait_composite"), 2),
                "lang_sources": "|".join(sorted({str(g.get("source", "")).strip() for g in grp if g.get("source")})),
            }
        )

    return out


def load_analyst_linguistic_signals(path: Path | None = None) -> dict:
    path = path or DEFAULT_SIGNAL_PATH
    if not path.exists():
        return {"by_name_pos": {}, "by_name": {}, "meta": {"status": "missing"}}

    with path.open() as f:
        rows = list(csv.DictReader(f))

    by_name_pos: Dict[Tuple[str, str], dict] = {}
    by_name: Dict[str, dict] = {}
    for row in rows:
        name_key = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position", ""))
        if not name_key or not pos:
            continue

        payload = {
            "lang_source_count": _safe_int(row.get("lang_source_count")) or 0,
            "lang_text_coverage": _safe_int(row.get("lang_text_coverage")) or 0,
            "lang_trait_processing": float(row.get("lang_trait_processing") or 50.0),
            "lang_trait_technique": float(row.get("lang_trait_technique") or 50.0),
            "lang_trait_explosiveness": float(row.get("lang_trait_explosiveness") or 50.0),
            "lang_trait_physicality": float(row.get("lang_trait_physicality") or 50.0),
            "lang_trait_competitiveness": float(row.get("lang_trait_competitiveness") or 50.0),
            "lang_trait_versatility": float(row.get("lang_trait_versatility") or 50.0),
            "lang_miller_keyword_hits": _safe_int(row.get("lang_miller_keyword_hits")) or 0,
            "lang_miller_coverage": float(row.get("lang_miller_coverage") or 0.0),
            "lang_risk_hits": _safe_int(row.get("lang_risk_hits")) or 0,
            "lang_risk_flag": _safe_int(row.get("lang_risk_flag")) or 0,
            "lang_trait_composite": float(row.get("lang_trait_composite") or 50.0),
            "lang_sources": row.get("lang_sources", ""),
        }

        by_name_pos[(name_key, pos)] = payload
        by_name.setdefault(name_key, payload)

    return {
        "by_name_pos": by_name_pos,
        "by_name": by_name,
        "meta": {"status": "ok", "rows": len(rows)},
    }
