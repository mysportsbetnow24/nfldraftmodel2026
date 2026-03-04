from __future__ import annotations

import csv
import datetime as dt
import html
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple



ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = ROOT / "data" / "sources"

DEFAULT_BASELINES_PATH = SOURCES_DIR / "mockdraftable_position_baselines.csv"
FALLBACK_BASELINES_PATH = SOURCES_DIR / "mockdraftable_position_baselines_2026-02-25.csv"

BASE_URL = "https://www.mockdraftable.com"
POSITION_PAGE = BASE_URL + "/positions?position={position_code}"

DEFAULT_POSITION_MAP = {
    "QB": "QB",
    "RB": "RB",
    "WR": "WR",
    "TE": "TE",
    "OT": "OT",
    "IOL": "IOL",
    "EDGE": "EDGE",
    "DT": "DT",
    "LB": "LB",
    "CB": "CB",
    "S": "S",
}

_METRIC_KEY_MAP = {
    "height": "height",
    "weight": "weight",
    "wingspan": "wingspan",
    "arm length": "arm",
    "hand size": "hand",
    "10 yard split": "ten_split",
    "20 yard split": "twenty_split",
    "40 yard dash": "forty",
    "bench press": "bench",
    "vertical jump": "vertical",
    "broad jump": "broad",
    "3 cone drill": "three_cone",
    "20 yard shuttle": "shuttle",
}

_FRACTIONS = {
    "¼": 0.25,
    "½": 0.50,
    "¾": 0.75,
    "⅐": 1 / 7,
    "⅑": 1 / 9,
    "⅒": 0.10,
    "⅓": 1 / 3,
    "⅔": 2 / 3,
    "⅕": 0.20,
    "⅖": 0.40,
    "⅗": 0.60,
    "⅘": 0.80,
    "⅙": 1 / 6,
    "⅚": 5 / 6,
    "⅛": 0.125,
    "⅜": 0.375,
    "⅝": 0.625,
    "⅞": 0.875,
}

# Guardrail ranges keep malformed scraped baselines (e.g. height_mean=854) from poisoning athletic scores.
_METRIC_SANITY_BOUNDS = {
    "height": (64.0, 84.0, 0.1, 8.0),
    "weight": (150.0, 390.0, 0.1, 80.0),
    "wingspan": (68.0, 96.0, 0.1, 12.0),
    "arm": (28.0, 40.0, 0.05, 4.0),
    "hand": (7.0, 13.0, 0.01, 3.0),
    "ten_split": (1.35, 2.40, 0.01, 0.70),
    "twenty_split": (2.20, 3.70, 0.01, 0.80),
    "forty": (4.20, 6.20, 0.01, 0.80),
    "bench": (5.0, 55.0, 0.01, 15.0),
    "vertical": (20.0, 50.0, 0.01, 10.0),
    "broad": (80.0, 150.0, 0.01, 30.0),
    "three_cone": (6.20, 9.00, 0.01, 1.20),
    "shuttle": (3.70, 5.80, 0.01, 1.20),
}


def _metric_stats_are_sane(metric: str, mean: float, std: float) -> bool:
    bounds = _METRIC_SANITY_BOUNDS.get(metric)
    if not bounds:
        return std > 0.0
    mean_lo, mean_hi, std_lo, std_hi = bounds
    return mean_lo <= mean <= mean_hi and std_lo <= std <= std_hi


def _clean_text(value: str) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _parse_mixed_number(value: str) -> float | None:
    txt = (value or "").strip()
    if not txt:
        return None

    txt = txt.replace(",", "")

    # replace unicode fractions with +decimal form
    for frac, dec in _FRACTIONS.items():
        if frac in txt:
            txt = txt.replace(frac, f" {dec}")

    txt = re.sub(r"\s+", " ", txt).strip()

    # plain float
    try:
        return float(txt)
    except ValueError:
        pass

    # sum tokens if mixed number
    parts = txt.split(" ")
    try:
        vals = [float(p) for p in parts if p]
        if vals:
            return sum(vals)
    except ValueError:
        pass

    # fraction like 1/8
    m = re.match(r"^(\d+)\/(\d+)$", txt)
    if m:
        num = float(m.group(1))
        den = float(m.group(2))
        if den != 0:
            return num / den

    return None


def _extract_rows_from_html(page_html: str) -> List[Tuple[str, str, str, str]]:
    rows_out: List[Tuple[str, str, str, str]] = []

    table_blocks = re.findall(r"<table[^>]*>(.*?)</table>", page_html, flags=re.IGNORECASE | re.DOTALL)
    for block in table_blocks:
        tr_blocks = re.findall(r"<tr[^>]*>(.*?)</tr>", block, flags=re.IGNORECASE | re.DOTALL)
        for tr in tr_blocks:
            cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, flags=re.IGNORECASE | re.DOTALL)
            if len(cells) < 4:
                continue
            metric = _clean_text(cells[0])
            mean = _clean_text(cells[1])
            std = _clean_text(cells[2])
            count = _clean_text(cells[3])
            rows_out.append((metric, mean, std, count))

    return rows_out


def fetch_position_aggregate(position_code: str, timeout: int = 20) -> dict:
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError("requests is required for live MockDraftable pulls. Install requirements first.") from exc

    url = POSITION_PAGE.format(position_code=position_code)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    rows = _extract_rows_from_html(resp.text)

    metrics: Dict[str, dict] = {}
    for metric_name, mean_raw, std_raw, count_raw in rows:
        key = _METRIC_KEY_MAP.get(metric_name.lower().strip())
        if key is None:
            continue

        mean = _parse_mixed_number(mean_raw)
        std = _parse_mixed_number(std_raw)

        try:
            count = int(float(count_raw.replace(",", "")))
        except ValueError:
            count = None

        if mean is None or std is None or count is None:
            continue

        metrics[key] = {"mean": round(mean, 3), "std": round(std, 3), "count": count}

    if len(metrics) < 6:
        raise RuntimeError(f"Could not parse enough aggregate metrics for position '{position_code}' from {url}")

    return {
        "source_position": position_code,
        "source_url": url,
        "pulled_on": dt.date.today().isoformat(),
        "metrics": metrics,
    }


def pull_position_baselines(position_map: Dict[str, str] | None = None) -> List[dict]:
    position_map = position_map or DEFAULT_POSITION_MAP
    out = []
    for model_pos, source_pos in position_map.items():
        payload = fetch_position_aggregate(source_pos)
        row = {
            "model_position": model_pos,
            "source_position": payload["source_position"],
            "source_url": payload["source_url"],
            "pulled_on": payload["pulled_on"],
        }

        for metric in [
            "height",
            "weight",
            "wingspan",
            "arm",
            "hand",
            "ten_split",
            "twenty_split",
            "forty",
            "bench",
            "vertical",
            "broad",
            "three_cone",
            "shuttle",
        ]:
            metric_payload = payload["metrics"].get(metric, {})
            row[f"{metric}_mean"] = metric_payload.get("mean", "")
            row[f"{metric}_std"] = metric_payload.get("std", "")
            row[f"{metric}_count"] = metric_payload.get("count", "")

        out.append(row)

    return out


def write_position_baselines(rows: List[dict], path: Path = DEFAULT_BASELINES_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _load_baseline_file(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    out: Dict[str, dict] = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            pos = (row.get("model_position") or "").strip().upper()
            if not pos:
                continue

            metrics = {}
            for metric in [
                "height",
                "weight",
                "wingspan",
                "arm",
                "hand",
                "ten_split",
                "twenty_split",
                "forty",
                "bench",
                "vertical",
                "broad",
                "three_cone",
                "shuttle",
            ]:
                mean_txt = row.get(f"{metric}_mean", "").strip()
                std_txt = row.get(f"{metric}_std", "").strip()
                if not mean_txt or not std_txt:
                    continue
                try:
                    mean = float(mean_txt)
                    std = float(std_txt)
                except ValueError:
                    continue
                if not _metric_stats_are_sane(metric, mean, std):
                    continue
                metrics[metric] = {
                    "mean": mean,
                    "std": std,
                    "count": int(float(row.get(f"{metric}_count", 0) or 0)),
                }

            out[pos] = {
                "source_position": row.get("source_position", ""),
                "source_url": row.get("source_url", ""),
                "pulled_on": row.get("pulled_on", ""),
                "metrics": metrics,
            }
    return out


def load_mockdraftable_baselines(path: Path | None = None) -> Dict[str, dict]:
    if path is None:
        if DEFAULT_BASELINES_PATH.exists():
            path = DEFAULT_BASELINES_PATH
        else:
            path = FALLBACK_BASELINES_PATH

    primary = _load_baseline_file(path)
    if path != DEFAULT_BASELINES_PATH or not FALLBACK_BASELINES_PATH.exists():
        return primary

    # If the latest scrape is malformed, merge in the known-good fallback by position.
    fallback = _load_baseline_file(FALLBACK_BASELINES_PATH)
    if not fallback:
        return primary

    merged: Dict[str, dict] = {}
    for pos in set(primary.keys()) | set(fallback.keys()):
        primary_row = primary.get(pos)
        fallback_row = fallback.get(pos)
        primary_count = len((primary_row or {}).get("metrics", {}))
        fallback_count = len((fallback_row or {}).get("metrics", {}))

        if primary_row and primary_count >= 6 and primary_count >= fallback_count:
            merged[pos] = primary_row
        elif fallback_row:
            merged[pos] = fallback_row
        elif primary_row:
            merged[pos] = primary_row

    return merged
