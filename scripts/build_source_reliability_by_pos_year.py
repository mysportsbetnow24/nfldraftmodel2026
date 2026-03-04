#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.rankings_loader import canonical_player_name, normalize_pos  # noqa: E402


BASE_SOURCE_PATH = ROOT / "data" / "sources" / "manual" / "source_reliability_weights_2026.csv"
HISTORICAL_PATH = ROOT / "data" / "sources" / "manual" / "historical_draft_outcomes_2016_2025.csv"
PANEL_PATH = ROOT / "data" / "sources" / "manual" / "historical_source_rank_panel_2016_2025.csv"
PANEL_TEMPLATE_PATH = ROOT / "data" / "sources" / "manual" / "historical_source_rank_panel_2016_2025_template.csv"
JOIN_OUTCOMES_PATH = ROOT / "data" / "processed" / "historical_labels_leagify_2015_2023.csv"
OUT_PATH = ROOT / "data" / "sources" / "manual" / "source_reliability_by_pos_year_2016_2025.csv"
METRICS_PATH = ROOT / "data" / "outputs" / "source_position_trust_metrics_2016_2025.csv"
PANEL_JOINED_PATH = ROOT / "data" / "outputs" / "historical_source_rank_panel_joined_2016_2025.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "source_reliability_by_pos_year_report_2026-02-28.md"

MODEL_POSITIONS = ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _to_int(value) -> int | None:
    f = _to_float(value)
    if f is None:
        return None
    return int(round(f))


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    seen = set(fieldnames)
    for row in rows[1:]:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _weighted_mean(rows: list[dict], key: str, weight_key: str = "sample_weight") -> float | None:
    num = 0.0
    den = 0.0
    for row in rows:
        v = _to_float(row.get(key))
        if v is None:
            continue
        w = _to_float(row.get(weight_key))
        if w is None:
            w = 1.0
        num += float(v) * float(w)
        den += float(w)
    if den <= 0:
        return None
    return num / den


def _average_ranks(values: list[float]) -> list[float]:
    pairs = sorted([(v, i) for i, v in enumerate(values)], key=lambda x: x[0])
    out = [0.0] * len(values)
    i = 0
    while i < len(pairs):
        j = i
        while j + 1 < len(pairs) and pairs[j + 1][0] == pairs[i][0]:
            j += 1
        avg_rank = (i + j + 2) / 2.0
        for k in range(i, j + 1):
            out[pairs[k][1]] = avg_rank
        i = j + 1
    return out


def _pearson_corr(x: list[float], y: list[float]) -> float | None:
    n = min(len(x), len(y))
    if n < 2:
        return None
    mx = sum(x[:n]) / n
    my = sum(y[:n]) / n
    num = 0.0
    denx = 0.0
    deny = 0.0
    for i in range(n):
        dx = x[i] - mx
        dy = y[i] - my
        num += dx * dy
        denx += dx * dx
        deny += dy * dy
    if denx <= 0 or deny <= 0:
        return None
    return num / ((denx * deny) ** 0.5)


def _spearman_corr(x: list[float], y: list[float]) -> float | None:
    if len(x) < 3 or len(y) < 3:
        return None
    xr = _average_ranks(x)
    yr = _average_ranks(y)
    return _pearson_corr(xr, yr)


def _metric_bundle(rows: list[dict]) -> dict:
    n = len(rows)
    if n == 0:
        return {
            "n": 0,
            "pick_slot_mae": 80.0,
            "top32_hit_rate": 0.50,
            "top100_hit_rate": 0.50,
            "rank_corr": 0.0,
            "success_rate": 0.50,
        }

    abs_err = []
    x_rank = []
    y_pick = []
    top32_num = 0.0
    top32_den = 0.0
    top100_num = 0.0
    top100_den = 0.0
    success_num = 0.0
    success_den = 0.0
    for row in rows:
        sr = _to_float(row.get("source_rank"))
        pick = _to_float(row.get("overall_pick"))
        if sr is None or pick is None:
            continue
        w = _to_float(row.get("sample_weight"))
        if w is None:
            w = 1.0
        abs_err.append(abs(float(sr) - float(pick)))
        x_rank.append(float(sr))
        y_pick.append(float(pick))
        if sr <= 32:
            top32_den += float(w)
            if pick <= 32:
                top32_num += float(w)
        if sr <= 100:
            top100_den += float(w)
            if pick <= 100:
                top100_num += float(w)
        success = _to_float(row.get("success_label"))
        if success is not None:
            success_num += float(success) * float(w)
            success_den += float(w)

    mae = sum(abs_err) / len(abs_err) if abs_err else 80.0
    top32 = (top32_num / top32_den) if top32_den > 0 else 0.50
    top100 = (top100_num / top100_den) if top100_den > 0 else 0.50
    corr = _spearman_corr(x_rank, y_pick)
    if corr is None:
        corr = 0.0
    success_rate = (success_num / success_den) if success_den > 0 else 0.50
    return {
        "n": n,
        "pick_slot_mae": float(mae),
        "top32_hit_rate": float(_clamp(top32, 0.0, 1.0)),
        "top100_hit_rate": float(_clamp(top100, 0.0, 1.0)),
        "rank_corr": float(_clamp(corr, -1.0, 1.0)),
        "success_rate": float(_clamp(success_rate, 0.0, 1.0)),
    }


def _blend_metric(cell: float, prior: float, n: int, k: float = 46.0) -> float:
    w = _clamp(float(n) / float(n + k), 0.0, 1.0)
    return (w * cell) + ((1.0 - w) * prior)


def _bundle_quality(bundle: dict) -> dict:
    mae = float(bundle.get("pick_slot_mae", 80.0))
    top32 = float(bundle.get("top32_hit_rate", 0.5))
    top100 = float(bundle.get("top100_hit_rate", 0.5))
    corr = float(bundle.get("rank_corr", 0.0))
    success = float(bundle.get("success_rate", 0.5))
    mae_score = _clamp(1.0 - (mae / 140.0), 0.0, 1.0)
    corr_score = _clamp((corr + 1.0) / 2.0, 0.0, 1.0)
    quality = (
        0.30 * mae_score
        + 0.20 * top32
        + 0.20 * top100
        + 0.15 * corr_score
        + 0.15 * success
    )
    return {
        "mae_score": round(mae_score, 4),
        "corr_score": round(corr_score, 4),
        "quality_score": round(_clamp(quality, 0.0, 1.0), 4),
    }


def _weighted_success(rows: list[dict]) -> tuple[float, float]:
    num = 0.0
    den = 0.0
    for row in rows:
        y = _to_float(row.get("success_label"))
        if y is None:
            continue
        w = _to_float(row.get("sample_weight"))
        if w is None:
            w = 1.0
        num += float(y) * float(w)
        den += float(w)
    return num, den


def _pos_year_quality_map(hist_rows: list[dict], *, min_year: int, max_year: int) -> dict[tuple[int, str], dict]:
    overall_num, overall_den = _weighted_success(hist_rows)
    overall_sr = (overall_num / overall_den) if overall_den > 0 else 0.5

    grouped: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for row in hist_rows:
        yr = int(_to_float(row.get("draft_year")) or 0)
        if yr < min_year or yr > max_year:
            continue
        pos = normalize_pos(str(row.get("position", "")).strip())
        if not pos:
            continue
        grouped[(yr, pos)].append(row)

    out: dict[tuple[int, str], dict] = {}
    for (yr, pos), rows in grouped.items():
        num, den = _weighted_success(rows)
        if den <= 0:
            continue
        sr = num / den
        n = len(rows)
        shrink = _clamp(float(n) / float(n + 40), 0.10, 0.85)
        sr_shrunk = ((1.0 - shrink) * overall_sr) + (shrink * sr)
        quality_mult = _clamp(sr_shrunk / max(0.01, overall_sr), 0.82, 1.18)
        out[(yr, pos)] = {
            "hist_success_rate_raw": round(sr, 4),
            "hist_success_rate_shrunk": round(sr_shrunk, 4),
            "hist_quality_multiplier": round(quality_mult, 4),
            "hist_row_count": n,
        }

    positions = sorted({pos for (_y, pos) in out.keys()})
    for pos in positions:
        available = sorted([y for (y, p) in out.keys() if p == pos])
        if not available:
            continue
        for yr in range(min_year, max_year + 1):
            key = (yr, pos)
            if key in out:
                continue
            nearest = min(available, key=lambda y: abs(y - yr))
            src = dict(out[(nearest, pos)])
            src["hist_row_count"] = max(10, int(src.get("hist_row_count", 0)))
            src["imputed"] = 1
            src["imputed_from_year"] = nearest
            out[key] = src

    return out


def _ensure_panel_template(path: Path) -> None:
    if path.exists():
        return
    sample = [
        {
            "source": "kiper_rank",
            "draft_year": 2023,
            "player_name": "Sample Player",
            "position": "EDGE",
            "source_rank": 18,
        }
    ]
    _write_csv(path, sample)


def _load_join_outcomes(path: Path, *, min_year: int, max_year: int) -> tuple[dict, dict]:
    rows = _read_csv(path)
    by_name_pos: dict[tuple[int, str, str], dict] = {}
    by_name_only: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for row in rows:
        year = _to_int(row.get("draft_year"))
        if year is None or year < min_year or year > max_year:
            continue
        name = canonical_player_name(row.get("player_name", ""))
        pos = normalize_pos(row.get("position", ""))
        pick = _to_int(row.get("overall_pick"))
        rnd = _to_int(row.get("draft_round")) or _to_int(row.get("round"))
        success = _to_float(row.get("success_label_3yr"))
        if success is None:
            success = _to_float(row.get("success_label"))
        sample_w = _to_float(row.get("sample_weight"))
        if sample_w is None:
            sample_w = _to_float(row.get("censor_weight"))
        if sample_w is None:
            sample_w = 1.0
        if not name or not pos or pick is None:
            continue
        payload = {
            "draft_year": year,
            "player_name": row.get("player_name", ""),
            "position": pos,
            "overall_pick": int(pick),
            "round": int(rnd) if rnd is not None else "",
            "success_label": round(_clamp(float(success if success is not None else 0.5), 0.0, 1.0), 4),
            "sample_weight": round(max(0.01, float(sample_w)), 4),
        }
        by_name_pos[(year, name, pos)] = payload
        by_name_only[(year, name)].append(payload)
    return by_name_pos, by_name_only


def _build_panel_join(
    panel_rows: list[dict],
    outcomes_by_name_pos: dict,
    outcomes_by_name_only: dict,
    *,
    min_year: int,
    max_year: int,
) -> tuple[list[dict], int]:
    joined = []
    unmatched = 0
    for row in panel_rows:
        source = str(row.get("source", "")).strip()
        year = _to_int(row.get("draft_year"))
        player_name = str(row.get("player_name", "")).strip()
        pos = normalize_pos(row.get("position", ""))
        source_rank = _to_float(row.get("source_rank"))
        if not source or year is None or year < min_year or year > max_year or not player_name or source_rank is None:
            continue
        name_key = canonical_player_name(player_name)

        outcome = outcomes_by_name_pos.get((year, name_key, pos))
        if outcome is None:
            cands = outcomes_by_name_only.get((year, name_key), [])
            if len(cands) == 1:
                outcome = cands[0]
        if outcome is None:
            unmatched += 1
            continue

        joined.append(
            {
                "source": source,
                "draft_year": int(year),
                "player_name": player_name,
                "position": pos,
                "source_rank": round(float(source_rank), 4),
                "overall_pick": outcome["overall_pick"],
                "round": outcome["round"],
                "success_label": outcome["success_label"],
                "sample_weight": outcome["sample_weight"],
            }
        )
    return joined, unmatched


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build source reliability by position/year using historical source-rank panel + outcomes with hierarchical shrinkage."
    )
    p.add_argument("--base", type=Path, default=BASE_SOURCE_PATH)
    p.add_argument("--historical", type=Path, default=HISTORICAL_PATH)
    p.add_argument("--panel", type=Path, default=PANEL_PATH)
    p.add_argument("--join-outcomes", type=Path, default=JOIN_OUTCOMES_PATH)
    p.add_argument("--out", type=Path, default=OUT_PATH)
    p.add_argument("--metrics-out", type=Path, default=METRICS_PATH)
    p.add_argument("--panel-joined-out", type=Path, default=PANEL_JOINED_PATH)
    p.add_argument("--report", type=Path, default=REPORT_PATH)
    p.add_argument("--panel-template-out", type=Path, default=PANEL_TEMPLATE_PATH)
    p.add_argument("--min-year", type=int, default=2016)
    p.add_argument("--max-year", type=int, default=2025)
    p.add_argument("--lookback-years", type=int, default=4)
    args = p.parse_args()

    if not args.base.exists():
        raise FileNotFoundError(f"Missing base source reliability file: {args.base}")
    if not args.historical.exists():
        raise FileNotFoundError(f"Missing historical outcomes file: {args.historical}")
    if not args.join_outcomes.exists():
        raise FileNotFoundError(f"Missing join outcomes file: {args.join_outcomes}")

    _ensure_panel_template(args.panel_template_out)

    base_rows = _read_csv(args.base)
    hist_rows = _read_csv(args.historical)
    hist_rows = [
        r
        for r in hist_rows
        if args.min_year <= int(_to_float(r.get("draft_year")) or 0) <= args.max_year
    ]
    pos_year_quality = _pos_year_quality_map(hist_rows, min_year=args.min_year, max_year=args.max_year)

    panel_rows = _read_csv(args.panel)
    outcomes_by_name_pos, outcomes_by_name_only = _load_join_outcomes(
        args.join_outcomes,
        min_year=args.min_year,
        max_year=args.max_year,
    )
    panel_joined, unmatched = _build_panel_join(
        panel_rows,
        outcomes_by_name_pos,
        outcomes_by_name_only,
        min_year=args.min_year,
        max_year=args.max_year,
    )
    panel_joined.sort(key=lambda r: (r["source"], r["draft_year"], r["position"], r["source_rank"]))
    _write_csv(args.panel_joined_out, panel_joined)

    base_map: dict[str, dict] = {}
    for row in base_rows:
        source = str(row.get("source", "")).strip()
        if not source:
            continue
        hit = _to_float(row.get("hit_rate"))
        stab = _to_float(row.get("stability"))
        sample = _to_int(row.get("sample_size"))
        base_map[source] = {
            "hit_rate": _clamp(float(hit if hit is not None else 0.56), 0.35, 0.85),
            "stability": _clamp(float(stab if stab is not None else 0.66), 0.30, 0.95),
            "sample_size": max(1, int(sample if sample is not None else 300)),
        }
    sources = sorted(base_map.keys())

    panel_by_year = defaultdict(list)
    for row in panel_joined:
        panel_by_year[int(row["draft_year"])].append(row)

    out_rows: list[dict] = []
    metrics_rows: list[dict] = []
    for year in range(args.min_year, args.max_year + 1):
        window_start = max(args.min_year, year - args.lookback_years + 1)
        window = []
        for y in range(window_start, year + 1):
            window.extend(panel_by_year.get(y, []))

        overall_bundle = _metric_bundle(window)
        src_bundles = {s: _metric_bundle([r for r in window if r["source"] == s]) for s in sources}
        pos_bundles = {
            pos: _metric_bundle([r for r in window if normalize_pos(r["position"]) == pos]) for pos in MODEL_POSITIONS
        }

        for source in sources:
            base = base_map[source]
            for pos in MODEL_POSITIONS:
                cell_rows = [r for r in window if r["source"] == source and normalize_pos(r["position"]) == pos]
                cell = _metric_bundle(cell_rows)
                n = int(cell["n"])

                src = src_bundles[source]
                posg = pos_bundles[pos]
                src_n = int(src.get("n", 0))
                pos_n = int(posg.get("n", 0))
                src_w = _clamp(src_n / float(src_n + 48), 0.0, 1.0)
                pos_w = _clamp(pos_n / float(pos_n + 48), 0.0, 1.0)

                prior = {}
                for metric in ["pick_slot_mae", "top32_hit_rate", "top100_hit_rate", "rank_corr", "success_rate"]:
                    src_prior = (src_w * float(src[metric])) + ((1.0 - src_w) * float(overall_bundle[metric]))
                    pos_prior = (pos_w * float(posg[metric])) + ((1.0 - pos_w) * float(overall_bundle[metric]))
                    prior[metric] = (0.60 * src_prior) + (0.40 * pos_prior)

                shrunk = {
                    "pick_slot_mae": _blend_metric(float(cell["pick_slot_mae"]), float(prior["pick_slot_mae"]), n),
                    "top32_hit_rate": _blend_metric(float(cell["top32_hit_rate"]), float(prior["top32_hit_rate"]), n),
                    "top100_hit_rate": _blend_metric(float(cell["top100_hit_rate"]), float(prior["top100_hit_rate"]), n),
                    "rank_corr": _blend_metric(float(cell["rank_corr"]), float(prior["rank_corr"]), n),
                    "success_rate": _blend_metric(float(cell["success_rate"]), float(prior["success_rate"]), n),
                }
                q = _bundle_quality(shrunk)
                sample_conf = _clamp(n / float(n + 90), 0.0, 1.0)
                stability_signal = (
                    0.55 * q["corr_score"]
                    + 0.25 * (1.0 - abs(float(shrunk["top32_hit_rate"]) - float(shrunk["top100_hit_rate"])))
                    + 0.20 * sample_conf
                )
                quality_mult = 1.0
                py = pos_year_quality.get((year, pos), {})
                if py:
                    quality_mult = float(py.get("hist_quality_multiplier", 1.0) or 1.0)

                # Convert shrunk panel metrics into bounded weights used by prior blend.
                hit_rate = _clamp((0.40 + (0.42 * q["quality_score"])) * quality_mult, 0.35, 0.85)
                # Blend stability with base to avoid violent year-to-year swings.
                stab_panel = _clamp(0.32 + (0.58 * stability_signal), 0.30, 0.95)
                stab_shrink = _clamp(n / float(n + 140), 0.0, 1.0)
                stability = ((1.0 - stab_shrink) * float(base["stability"])) + (stab_shrink * stab_panel)
                stability = _clamp(stability, 0.30, 0.95)

                effective_sample = int(round((0.35 * float(base["sample_size"])) + (0.65 * float(max(0, n)))))
                if n == 0:
                    effective_sample = max(20, int(round(base["sample_size"] * 0.30)))
                effective_sample = max(20, min(int(base["sample_size"]), effective_sample))

                out_rows.append(
                    {
                        "source": source,
                        "position": pos,
                        "draft_year": year,
                        "hit_rate": round(float(hit_rate), 4),
                        "stability": round(float(stability), 4),
                        "sample_size": int(effective_sample),
                        "pick_slot_mae": round(float(shrunk["pick_slot_mae"]), 4),
                        "top32_hit_rate": round(float(shrunk["top32_hit_rate"]), 4),
                        "top100_hit_rate": round(float(shrunk["top100_hit_rate"]), 4),
                        "rank_corr": round(float(shrunk["rank_corr"]), 4),
                        "success_rate": round(float(shrunk["success_rate"]), 4),
                        "quality_score": q["quality_score"],
                        "hist_success_rate_raw": py.get("hist_success_rate_raw", ""),
                        "hist_success_rate_shrunk": py.get("hist_success_rate_shrunk", ""),
                        "hist_quality_multiplier": round(float(quality_mult), 4),
                        "hist_row_count": py.get("hist_row_count", ""),
                        "panel_cell_count": int(n),
                        "window_start_year": int(window_start),
                        "window_end_year": int(year),
                        "method": "hierarchical_source_position_rank_panel_shrinkage",
                    }
                )

                metrics_rows.append(
                    {
                        "source": source,
                        "position": pos,
                        "draft_year": year,
                        "window_start_year": int(window_start),
                        "window_end_year": int(year),
                        "cell_count": int(n),
                        "source_count": int(src_n),
                        "position_count": int(pos_n),
                        "overall_count": int(overall_bundle.get("n", 0)),
                        "pick_slot_mae_raw": round(float(cell["pick_slot_mae"]), 4),
                        "pick_slot_mae_shrunk": round(float(shrunk["pick_slot_mae"]), 4),
                        "top32_hit_rate_raw": round(float(cell["top32_hit_rate"]), 4),
                        "top32_hit_rate_shrunk": round(float(shrunk["top32_hit_rate"]), 4),
                        "top100_hit_rate_raw": round(float(cell["top100_hit_rate"]), 4),
                        "top100_hit_rate_shrunk": round(float(shrunk["top100_hit_rate"]), 4),
                        "rank_corr_raw": round(float(cell["rank_corr"]), 4),
                        "rank_corr_shrunk": round(float(shrunk["rank_corr"]), 4),
                        "success_rate_raw": round(float(cell["success_rate"]), 4),
                        "success_rate_shrunk": round(float(shrunk["success_rate"]), 4),
                        "quality_score": q["quality_score"],
                    }
                )

    out_rows.sort(key=lambda r: (r["source"], int(r["draft_year"]), r["position"]))
    metrics_rows.sort(key=lambda r: (r["source"], int(r["draft_year"]), r["position"]))
    _write_csv(args.out, out_rows)
    _write_csv(args.metrics_out, metrics_rows)

    panel_years = sorted({int(r["draft_year"]) for r in panel_joined}) if panel_joined else []
    report_lines = [
        "# Source Reliability By Position/Year Report",
        "",
        "## Inputs",
        f"- Base source table: `{args.base}`",
        f"- Historical outcomes (pos-year quality): `{args.historical}`",
        f"- Historical source-rank panel: `{args.panel}`",
        f"- Panel join outcomes: `{args.join_outcomes}`",
        "",
        "## Outputs",
        f"- Reliability table: `{args.out}`",
        f"- Panel metrics table: `{args.metrics_out}`",
        f"- Joined panel (for QA): `{args.panel_joined_out}`",
        "",
        "## Coverage",
        f"- Year window: **{args.min_year}-{args.max_year}**",
        f"- Base sources: **{len(base_rows)}**",
        f"- Panel rows loaded: **{len(panel_rows)}**",
        f"- Panel rows joined to outcomes: **{len(panel_joined)}**",
        f"- Panel rows unmatched: **{unmatched}**",
        f"- Panel draft years present: **{panel_years if panel_years else 'none'}**",
        f"- Reliability rows written: **{len(out_rows)}**",
        "",
        "## Metrics Used",
        "- pick-slot MAE",
        "- top-32 hit rate",
        "- top-100 hit rate",
        "- rank correlation (Spearman)",
        "- success label rate",
        "",
        "## Hierarchical Shrinkage",
        "- Cell metrics shrink toward a blended prior: `0.60 * source-global + 0.40 * position-global`.",
        "- Source-global and position-global priors each shrink toward overall-global using support.",
        "- Final `hit_rate` / `stability` are bounded and blended with base source defaults.",
        "",
        "## Notes",
        "- `build_big_board.py` already consumes this file for prior-blend multipliers by source+position+year.",
        "- If panel coverage is sparse, this process remains conservative via shrinkage and base fallback.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(report_lines))

    print(f"Wrote: {args.out}")
    print(f"Rows: {len(out_rows)}")
    print(f"Panel loaded: {len(panel_rows)}")
    print(f"Panel joined: {len(panel_joined)} (unmatched={unmatched})")
    print(f"Metrics: {args.metrics_out}")
    print(f"Panel QA join: {args.panel_joined_out}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
