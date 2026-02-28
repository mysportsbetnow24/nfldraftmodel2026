from __future__ import annotations

import csv
import html
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import requests

from src.ingest.rankings_loader import canonical_player_name


CBS_COMBINE_LIVE_URL = (
    "https://www.cbssports.com/nfl/news/"
    "nfl-combine-2026-live-updates-results-dl-edge-lb/live/"
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMBINE_OUT = ROOT / "data" / "sources" / "manual" / "combine_2026_results.csv"

CSV_COLUMNS = [
    "player_name",
    "school",
    "position",
    "height_in",
    "weight_lb",
    "arm_in",
    "hand_in",
    "forty",
    "ten_split",
    "vertical",
    "broad",
    "shuttle",
    "three_cone",
    "bench",
    "ras_official",
    "source",
    "last_updated",
]

METRIC_FIELDS = {"forty", "ten_split", "vertical", "broad", "shuttle", "three_cone", "bench"}

RANKED_ITEM_RE = re.compile(
    r"(?:(?<=^)|(?<=[^A-Za-z]))(?:T-?|t-?)?\d{1,2}[.)-]?\s*"
    r"([A-Z][A-Za-z.'\-\s]+?)\s*"
    r"\(([^)]+)\)\s*(?:--|:)\s*"
    r"([0-9]{1,3}(?:\.[0-9]{1,2})?)\s*(\"|seconds?|sec|s)?"
    r"(?=(?:\s*(?:T-?|t-?)?\d{1,2}[.)-])|$)",
    re.IGNORECASE,
)

UNNUMBERED_ITEM_RE = re.compile(
    r"([A-Z][A-Za-z.'\-\s]+?)\s*"
    r"\(([^)]+)\)\s*(?:--|:)\s*"
    r"([0-9]{1,3}(?:\.[0-9]{1,2})?)\s*(\"|seconds?|sec|s)?"
    r"(?=(?:[A-Z][A-Za-z.'\-\s]+?\s*\()|$)",
    re.IGNORECASE,
)

SINGLE_BROAD_RE = re.compile(
    r"([A-Z][A-Za-z.'\-\s]+):\s*([0-9]{2,3}(?:\.[0-9]{1,2})?)\s*BROAD",
    re.IGNORECASE,
)

UNOFFICIAL_40_SPLIT_RE = re.compile(
    r"([A-Z][A-Za-z.'\-\s]+)\s+([0-9]\.[0-9]{2})\s+unofficial\s+40\.?\s*([0-9]\.[0-9]{1,2})?\s*10\s*yard\s*split?",
    re.IGNORECASE,
)


@dataclass
class Measurement:
    player_name: str
    school: str
    metric: str
    value: float
    last_updated: str


def fetch_cbs_live_html(url: str = CBS_COMBINE_LIVE_URL, timeout: int = 30) -> str:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _clean_text(raw: str) -> str:
    txt = html.unescape(raw or "")
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = txt.replace("\xa0", " ")
    txt = txt.replace("&nbsp;", " ")
    txt = txt.replace("—", " ")
    # CBS often concatenates tie markers into the next player token (e.g., "4.88tJackie").
    txt = re.sub(r"([0-9])t([A-Z])", r"\1 \2", txt)
    txt = re.sub(r"([0-9])T-([0-9])", r"\1 T-\2", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _extract_liveblog_updates(page_html: str) -> list[dict]:
    blocks = re.findall(
        r'<script type="application/ld\+json">\s*(\{.*?\})\s*</script>',
        page_html,
        flags=re.DOTALL,
    )
    for block in blocks:
        try:
            parsed = json.loads(block)
        except json.JSONDecodeError:
            continue
        if parsed.get("@type") == "LiveBlogPosting":
            return parsed.get("liveBlogUpdate", [])
    return []


def _infer_metric(text: str, values: list[float]) -> str | None:
    lower = text.lower()
    if "3-cone" in lower or "three-cone" in lower:
        return "three_cone"
    if "shuttle" in lower:
        return "shuttle"
    if "bench" in lower:
        return "bench"
    if "10 yard split" in lower or "10-yard split" in lower:
        return "ten_split"
    if "broad" in lower and all(v >= 80 for v in values):
        return "broad"
    if ("vert" in lower or "vertical" in lower or '"' in text) and all(v >= 20 for v in values):
        return "vertical"
    if all(3.8 <= v <= 6.5 for v in values):
        return "forty"
    if "broad" in lower:
        return "broad"
    return None


def _extract_list_entries(text: str) -> list[tuple[str, str, float]]:
    out: list[tuple[str, str, float]] = []
    for m in RANKED_ITEM_RE.finditer(text):
        name = " ".join((m.group(1) or "").split())
        name = re.sub(r"^t\.?\s*(?=[A-Z])", "", name)
        name = re.sub(r"^[tT](?=[A-Z])", "", name)
        school = " ".join((m.group(2) or "").split())
        value = float(m.group(3))
        out.append((name, school, value))
    if out:
        return out

    for m in UNNUMBERED_ITEM_RE.finditer(text):
        name = " ".join((m.group(1) or "").split())
        name = re.sub(r"^t\.?\s*(?=[A-Z])", "", name)
        name = re.sub(r"^[tT](?=[A-Z])", "", name)
        school = " ".join((m.group(2) or "").split())
        value = float(m.group(3))
        out.append((name, school, value))
    return out


def _extract_measurements_from_update(update: dict) -> list[Measurement]:
    raw_body = update.get("articleBody", "") or ""
    body = _clean_text(raw_body)
    if not body:
        return []

    published = (update.get("datePublished") or "").strip()
    last_updated = published[:10] if published else datetime.utcnow().strftime("%Y-%m-%d")

    out: list[Measurement] = []

    for m in UNOFFICIAL_40_SPLIT_RE.finditer(body):
        name = " ".join((m.group(1) or "").split())
        forty = float(m.group(2))
        ten = m.group(3)
        out.append(Measurement(player_name=name, school="", metric="forty", value=forty, last_updated=last_updated))
        if ten:
            out.append(
                Measurement(
                    player_name=name,
                    school="",
                    metric="ten_split",
                    value=float(ten),
                    last_updated=last_updated,
                )
            )

    for m in SINGLE_BROAD_RE.finditer(body):
        name = " ".join((m.group(1) or "").split())
        broad = float(m.group(2))
        out.append(Measurement(player_name=name, school="", metric="broad", value=broad, last_updated=last_updated))

    entries = _extract_list_entries(body)
    if entries:
        values = [v for _, _, v in entries]
        metric = _infer_metric(body, values)
        if metric in METRIC_FIELDS:
            for name, school, val in entries:
                out.append(
                    Measurement(
                        player_name=name,
                        school=school,
                        metric=metric,
                        value=val,
                        last_updated=last_updated,
                    )
                )

    deduped: dict[tuple[str, str], Measurement] = {}
    for m in out:
        key = (canonical_player_name(m.player_name), m.metric)
        deduped[key] = m
    return list(deduped.values())


def extract_measurements_from_cbs_html(page_html: str) -> list[Measurement]:
    updates = _extract_liveblog_updates(page_html)
    out: list[Measurement] = []
    for u in updates:
        out.extend(_extract_measurements_from_update(u))
    return out


def _fmt(value: float) -> str:
    s = f"{value:.3f}".rstrip("0").rstrip(".")
    return s


def _load_existing_rows(path: Path) -> tuple[list[str], dict[str, dict]]:
    if not path.exists():
        return CSV_COLUMNS[:], {}

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames[:] if reader.fieldnames else CSV_COLUMNS[:]
        rows: dict[str, dict] = {}
        for row in reader:
            name = (row.get("player_name") or "").strip()
            if not name:
                continue
            rows[canonical_player_name(name)] = row
        return fieldnames, rows


def _blank_row() -> dict:
    return {c: "" for c in CSV_COLUMNS}


def merge_measurements_into_combine_csv(
    measurements: Iterable[Measurement],
    out_path: Path = DEFAULT_COMBINE_OUT,
) -> dict:
    fieldnames, rows = _load_existing_rows(out_path)
    for c in CSV_COLUMNS:
        if c not in fieldnames:
            fieldnames.append(c)

    created_players = 0
    updated_fields = 0
    metric_updates = {m: 0 for m in METRIC_FIELDS}

    for m in measurements:
        if m.metric not in METRIC_FIELDS:
            continue
        key = canonical_player_name(m.player_name)
        if not key:
            continue

        row = rows.get(key)
        if row is None:
            row = _blank_row()
            row["player_name"] = m.player_name
            row["school"] = m.school
            row["position"] = ""
            row["source"] = "CBS Live Results"
            row["last_updated"] = m.last_updated
            rows[key] = row
            created_players += 1

        if not (row.get("school") or "").strip() and m.school:
            row["school"] = m.school

        prev = (row.get(m.metric) or "").strip()
        next_val = _fmt(m.value)
        if prev != next_val:
            row[m.metric] = next_val
            updated_fields += 1
            metric_updates[m.metric] += 1

        src = (row.get("source") or "").strip()
        if not src:
            row["source"] = "CBS Live Results"
        elif "CBS Live Results" not in src:
            row["source"] = f"{src}; CBS Live Results"

        row["last_updated"] = m.last_updated or row.get("last_updated", "")

    # Clean obvious tie-prefix artifacts from earlier pulls (e.g., "tJackie Marshall").
    tie_keys = []
    for key, row in rows.items():
        name = (row.get("player_name") or "").strip()
        if re.match(r"^t\.?\s*[A-Z]", name):
            clean_name = re.sub(r"^t\.?\s*", "", name)
            clean_key = canonical_player_name(clean_name)
            if clean_key != key:
                tie_keys.append((key, clean_key, clean_name))
        elif re.match(r"^[tT][A-Z]", name):
            clean_name = name[1:]
            clean_key = canonical_player_name(clean_name)
            if clean_key != key:
                tie_keys.append((key, clean_key, clean_name))
    for old_key, new_key, clean_name in tie_keys:
        old_row = rows.get(old_key)
        if not old_row:
            continue
        target = rows.get(new_key)
        if target is None:
            old_row["player_name"] = clean_name
            rows[new_key] = old_row
        else:
            for f in METRIC_FIELDS:
                if (old_row.get(f) or "").strip() and not (target.get(f) or "").strip():
                    target[f] = old_row[f]
            if not (target.get("school") or "").strip() and (old_row.get("school") or "").strip():
                target["school"] = old_row["school"]
        rows.pop(old_key, None)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted(rows.values(), key=lambda r: canonical_player_name(r.get("player_name", ""))):
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    return {
        "players_total": len(rows),
        "players_created": created_players,
        "fields_updated": updated_fields,
        "metric_updates": metric_updates,
    }
