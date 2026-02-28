#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.athletic_profile_loader import evaluate_athletic_profile, load_historical_athletic_context
from src.ingest.rankings_loader import normalize_pos

INPUT_PATH = ROOT / "data" / "sources" / "external" / "nflverse" / "combine.parquet"
LEGACY_PATH = ROOT / "data" / "outputs" / "team_athletic_thresholds_2026_inferred.csv"
OUT_TEAM_PATH = ROOT / "data" / "outputs" / "team_athletic_thresholds_2026_inferred.csv"
OUT_POS_PATH = ROOT / "data" / "outputs" / "team_athletic_thresholds_2026_by_position.csv"
OUT_DIGEST_PATH = ROOT / "data" / "outputs" / "team_athletic_thresholds_2026_digest.md"
OUT_POS_DIGEST_PATH = ROOT / "data" / "outputs" / "team_athletic_thresholds_2026_by_position_digest.md"

POSITION_BUCKET = {
    "QB": "premium",
    "OT": "premium",
    "EDGE": "premium",
    "CB": "premium",
    "WR": "mid",
    "S": "mid",
    "DT": "mid",
    "LB": "mid",
    "IOL": "low",
    "TE": "low",
    "RB": "low",
}

TIER_TO_THRESHOLDS = {
    "A+ Athlete-heavy": (8.3, 7.8, 7.3),
    "A Athlete-lean": (8.0, 7.5, 7.0),
    "B Balanced-athletic": (7.7, 7.2, 6.8),
    "C Balanced": (7.4, 7.0, 6.6),
    "D Film/production-tolerant": (7.1, 6.7, 6.3),
}

POS_MAP = {
    "T": "OT",
    "LT": "OT",
    "RT": "OT",
    "OT": "OT",
    "G": "IOL",
    "OG": "IOL",
    "C": "IOL",
    "OL": "IOL",
    "DE": "EDGE",
    "OLB": "EDGE",
    "EDGE": "EDGE",
    "ED": "EDGE",
    "DT": "DT",
    "NT": "DT",
    "IDL": "DT",
    "ILB": "LB",
    "MLB": "LB",
    "LB": "LB",
    "CB": "CB",
    "FS": "S",
    "SS": "S",
    "S": "S",
    "DB": "S",
    "QB": "QB",
    "RB": "RB",
    "HB": "RB",
    "FB": "RB",
    "WR": "WR",
    "TE": "TE",
}

TEAM_ABBR_MAP = {
    "ARIZONA CARDINALS": "ARI",
    "ATLANTA FALCONS": "ATL",
    "BALTIMORE RAVENS": "BAL",
    "BUFFALO BILLS": "BUF",
    "CAROLINA PANTHERS": "CAR",
    "CHICAGO BEARS": "CHI",
    "CINCINNATI BENGALS": "CIN",
    "CLEVELAND BROWNS": "CLE",
    "DALLAS COWBOYS": "DAL",
    "DENVER BRONCOS": "DEN",
    "DETROIT LIONS": "DET",
    "GREEN BAY PACKERS": "GB",
    "HOUSTON TEXANS": "HOU",
    "INDIANAPOLIS COLTS": "IND",
    "JACKSONVILLE JAGUARS": "JAX",
    "KANSAS CITY CHIEFS": "KC",
    "LAS VEGAS RAIDERS": "LV",
    "LOS ANGELES CHARGERS": "LAC",
    "LOS ANGELES RAMS": "LAR",
    "MIAMI DOLPHINS": "MIA",
    "MINNESOTA VIKINGS": "MIN",
    "NEW ENGLAND PATRIOTS": "NE",
    "NEW ORLEANS SAINTS": "NO",
    "NEW YORK GIANTS": "NYG",
    "NEW YORK JETS": "NYJ",
    "PHILADELPHIA EAGLES": "PHI",
    "PITTSBURGH STEELERS": "PIT",
    "SAN FRANCISCO 49ERS": "SF",
    "SEATTLE SEAHAWKS": "SEA",
    "TAMPA BAY BUCCANEERS": "TB",
    "TENNESSEE TITANS": "TEN",
    "WASHINGTON COMMANDERS": "WAS",
    "WASHINGTON FOOTBALL TEAM": "WAS",
    "WASHINGTON REDSKINS": "WAS",
}

DEFAULT_EXPLICIT_RB_SOURCE = {
    "ARI": "yes",
    "ATL": "yes",
    "BAL": "no",
    "BUF": "yes",
    "CAR": "yes",
    "CHI": "yes",
    "CIN": "yes",
    "CLE": "no",
    "DAL": "yes",
    "DEN": "yes",
    "DET": "yes",
    "GB": "yes",
    "HOU": "no",
    "IND": "no",
    "JAX": "no",
    "KC": "yes",
    "LAC": "yes",
    "LAR": "yes",
    "LV": "yes",
    "MIA": "no",
    "MIN": "yes",
    "NE": "yes",
    "NO": "yes",
    "NYG": "yes",
    "NYJ": "yes",
    "PHI": "yes",
    "PIT": "no",
    "SEA": "yes",
    "SF": "yes",
    "TB": "yes",
    "TEN": "no",
    "WAS": "yes",
}


def _to_float(value) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _height_to_inches(raw: str) -> float | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    if "-" in txt:
        left, right = txt.split("-", 1)
        try:
            return float((int(left) * 12) + int(right))
        except ValueError:
            return None
    try:
        return float(txt)
    except ValueError:
        return None


def _canonical_pos(raw: str) -> str:
    pos = str(raw or "").strip().upper()
    mapped = POS_MAP.get(pos, "")
    if mapped:
        return mapped
    normalized = normalize_pos(pos)
    if normalized in POSITION_BUCKET:
        return normalized
    return ""


def _team_abbr(raw: str) -> str:
    team = str(raw or "").strip().upper()
    if not team:
        return ""
    if team in TEAM_ABBR_MAP:
        return TEAM_ABBR_MAP[team]
    # already an abbreviation
    if len(team) in {2, 3, 4} and " " not in team:
        return team
    return ""


def _tier_from_percentile(percentile_rank: float) -> str:
    # Relative tiering by team distribution over the chosen year window.
    if percentile_rank >= 0.90:
        return "A+ Athlete-heavy"
    if percentile_rank >= 0.70:
        return "A Athlete-lean"
    if percentile_rank >= 0.45:
        return "B Balanced-athletic"
    if percentile_rank >= 0.25:
        return "C Balanced"
    return "D Film/production-tolerant"


def _read_rb_source_flags(path: Path) -> dict[str, str]:
    flags: dict[str, str] = dict(DEFAULT_EXPLICIT_RB_SOURCE)
    if not path.exists():
        return flags
    with path.open() as f:
        for row in csv.DictReader(f):
            team = str(row.get("team", "")).strip()
            if not team:
                continue
            val = str(row.get("explicit_public_rb_threshold_source_found", "")).strip().lower()
            if val == "yes":
                flags[team] = "yes"
            elif val == "no" and team not in flags:
                flags[team] = "no"
    return flags


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _compute_pos_threshold(*, team_bucket_threshold: float, observed_median: float | None, sample_n: int) -> tuple[float, float]:
    # More samples -> more weight on observed team/position behavior.
    weight = max(0.0, min(1.0, (sample_n - 1) / 7.0))
    if observed_median is None:
        return round(team_bucket_threshold, 2), round(weight, 3)
    blended = ((1.0 - weight) * team_bucket_threshold) + (weight * observed_median)
    # Keep thresholds from drifting too far from team baseline.
    lo = team_bucket_threshold - 0.8
    hi = team_bucket_threshold + 0.8
    clamped = max(lo, min(hi, blended))
    return round(clamped, 2), round(weight, 3)


def _clamp_threshold(v: float) -> float:
    return round(max(5.5, min(9.5, float(v))), 2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build team athletic thresholds (2021-2025) with position breakdown.")
    parser.add_argument("--input", type=Path, default=INPUT_PATH)
    parser.add_argument("--legacy-flags", type=Path, default=LEGACY_PATH)
    parser.add_argument("--out-team", type=Path, default=OUT_TEAM_PATH)
    parser.add_argument("--out-position", type=Path, default=OUT_POS_PATH)
    parser.add_argument("--out-digest", type=Path, default=OUT_DIGEST_PATH)
    parser.add_argument("--out-position-digest", type=Path, default=OUT_POS_DIGEST_PATH)
    parser.add_argument("--year-start", type=int, default=2021)
    parser.add_argument("--year-end", type=int, default=2025)
    parser.add_argument("--premium-bump", type=float, default=0.0, help="Additive bump applied to premium thresholds.")
    parser.add_argument("--mid-bump", type=float, default=0.0, help="Additive bump applied to mid thresholds.")
    parser.add_argument("--low-bump", type=float, default=0.0, help="Additive bump applied to low thresholds.")
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Missing input: {args.input}")

    pack = load_historical_athletic_context()
    df = pd.read_parquet(args.input)
    needed_cols = {
        "season",
        "draft_team",
        "draft_round",
        "player_name",
        "pos",
        "ht",
        "wt",
        "forty",
        "bench",
        "vertical",
        "broad_jump",
        "cone",
        "shuttle",
    }
    for col in needed_cols:
        if col not in df.columns:
            raise RuntimeError(f"Input missing required column: {col}")

    subset = df[list(needed_cols)].copy()
    subset["season"] = pd.to_numeric(subset["season"], errors="coerce")
    subset["draft_round"] = pd.to_numeric(subset["draft_round"], errors="coerce")
    subset = subset[
        (subset["season"] >= args.year_start)
        & (subset["season"] <= args.year_end)
        & subset["draft_team"].notna()
        & subset["draft_round"].notna()
        & (subset["draft_round"] >= 1)
        & (subset["draft_round"] <= 7)
    ].copy()

    rows: list[dict] = []
    for rec in subset.to_dict(orient="records"):
        team = _team_abbr(rec.get("draft_team", ""))
        pos = _canonical_pos(rec.get("pos", ""))
        if not team or pos not in POSITION_BUCKET:
            continue

        metrics = {
            "height_in": _height_to_inches(rec.get("ht")),
            "weight_lb": _to_float(rec.get("wt")),
            "arm_in": None,
            "hand_in": None,
            "forty": _to_float(rec.get("forty")),
            "ten_split": None,
            "vertical": _to_float(rec.get("vertical")),
            "broad": _to_float(rec.get("broad_jump")),
            "three_cone": _to_float(rec.get("cone")),
            "shuttle": _to_float(rec.get("shuttle")),
            "bench": _to_float(rec.get("bench")),
            "wingspan_in": None,
        }

        profile = evaluate_athletic_profile(position=pos, current_metrics=metrics, pack=pack)
        score = _to_float(profile.get("athletic_profile_score"))
        if score is None:
            continue

        rows.append(
            {
                "team": team,
                "year": int(float(rec["season"])),
                "player_name": str(rec.get("player_name", "")).strip(),
                "position": pos,
                "athletic_proxy_ras": round(float(score) / 10.0, 3),
            }
        )

    if not rows:
        raise RuntimeError("No rows produced for threshold build. Check input coverage and year window.")

    calc = pd.DataFrame(rows)
    rb_flags = _read_rb_source_flags(args.legacy_flags)

    team_year = (
        calc.groupby(["team", "year"], as_index=False)["athletic_proxy_ras"]
        .mean()
        .rename(columns={"athletic_proxy_ras": "ras_year_avg"})
    )
    team_avg = (
        calc.groupby("team", as_index=False)["athletic_proxy_ras"]
        .mean()
        .rename(columns={"athletic_proxy_ras": "ras_5yr_avg"})
    )
    team_avg = team_avg.sort_values("ras_5yr_avg").reset_index(drop=True)
    if len(team_avg) > 1:
        team_avg["team_percentile_rank"] = team_avg["ras_5yr_avg"].rank(method="average", pct=True)
    else:
        team_avg["team_percentile_rank"] = 1.0
    team_2025 = team_year[team_year["year"] == args.year_end][["team", "ras_year_avg"]].rename(
        columns={"ras_year_avg": "ras_2025"}
    )

    team_rows: list[dict] = []
    for rec in team_avg.to_dict(orient="records"):
        team = str(rec["team"])
        avg_5 = float(rec["ras_5yr_avg"])
        tier = _tier_from_percentile(float(rec.get("team_percentile_rank", 0.5)))
        thresholds = TIER_TO_THRESHOLDS[tier]
        premium_thr = _clamp_threshold(float(thresholds[0]) + float(args.premium_bump))
        mid_thr = _clamp_threshold(float(thresholds[1]) + float(args.mid_bump))
        low_thr = _clamp_threshold(float(thresholds[2]) + float(args.low_bump))
        one_year = team_2025[team_2025["team"] == team]
        ras_2025 = float(one_year.iloc[0]["ras_2025"]) if len(one_year) else None

        team_rows.append(
            {
                "team": team,
                "ras_2025": round(ras_2025, 3) if ras_2025 is not None else "",
                "ras_5yr_avg": round(avg_5, 3),
                "ras_2021_2025_tier": tier,
                # Back-compat alias for existing consumers.
                "ras_2021_2024_tier": tier,
                "suggested_ras_threshold_premium_pos_qb_ot_edge_cb": premium_thr,
                "suggested_ras_threshold_mid_value_pos_wr_s_dt_lb_s": mid_thr,
                "suggested_ras_threshold_low_value_pos_iol_te_rb": low_thr,
                "explicit_public_rb_threshold_source_found": rb_flags.get(team, "no"),
                "threshold_year_start": args.year_start,
                "threshold_year_end": args.year_end,
                "team_percentile_rank_2021_2025": round(float(rec.get("team_percentile_rank", 0.0)), 4),
                "premium_bump_applied": round(float(args.premium_bump), 3),
                "mid_bump_applied": round(float(args.mid_bump), 3),
                "low_bump_applied": round(float(args.low_bump), 3),
            }
        )
    team_rows.sort(key=lambda x: x["team"])

    # Position-specific thresholds by team.
    pos_rows: list[dict] = []
    team_lookup = {r["team"]: r for r in team_rows}
    grouped = calc.groupby(["team", "position"])
    for (team, pos), g in grouped:
        team_row = team_lookup.get(team)
        if not team_row:
            continue
        bucket = POSITION_BUCKET.get(pos, "mid")
        if bucket == "premium":
            bucket_threshold = float(team_row["suggested_ras_threshold_premium_pos_qb_ot_edge_cb"])
        elif bucket == "low":
            bucket_threshold = float(team_row["suggested_ras_threshold_low_value_pos_iol_te_rb"])
        else:
            bucket_threshold = float(team_row["suggested_ras_threshold_mid_value_pos_wr_s_dt_lb_s"])

        sample_n = int(len(g))
        pos_avg = float(g["athletic_proxy_ras"].mean())
        pos_q50 = float(g["athletic_proxy_ras"].median())
        pos_threshold, conf_w = _compute_pos_threshold(
            team_bucket_threshold=bucket_threshold,
            observed_median=pos_q50,
            sample_n=sample_n,
        )
        pos_rows.append(
            {
                "team": team,
                "position": pos,
                "position_bucket": bucket,
                "sample_n_2021_2025": sample_n,
                "ras_proxy_avg_2021_2025": round(pos_avg, 3),
                "ras_proxy_q50_2021_2025": round(pos_q50, 3),
                "team_bucket_threshold_ras": round(bucket_threshold, 2),
                "team_position_threshold_ras": round(pos_threshold, 2),
                "position_threshold_confidence_weight": conf_w,
                "threshold_year_start": args.year_start,
                "threshold_year_end": args.year_end,
            }
        )
    pos_rows.sort(key=lambda x: (x["team"], x["position"]))

    _write_csv(args.out_team, team_rows)
    _write_csv(args.out_position, pos_rows)

    digest_lines = [
        "# Team Athletic Thresholds (2026, Historical + Public Evidence)",
        "",
        "Interpretation:",
        "- Premium positions: QB/OT/EDGE/CB",
        "- Mid-value positions: WR/S/DT/LB",
        "- Low-value positions: IOL/TE/RB",
        "- `explicit_rb_source=yes` means a public team-specific RB archetype threshold article was found (The 33rd Team).",
        f"- Otherwise thresholds are inferred from {args.year_start}-{args.year_end} team draft athletic trends.",
        f"- Bumps applied: premium `{args.premium_bump:+.2f}`, mid `{args.mid_bump:+.2f}`, low `{args.low_bump:+.2f}`.",
        "",
        "| Team | 5Y Avg RAS Proxy | Tier | Premium Min RAS | Mid Min RAS | Low Min RAS | explicit_rb_source |",
        "|---|---:|---|---:|---:|---:|---|",
    ]
    for row in team_rows:
        digest_lines.append(
            f"| {row['team']} | {row['ras_5yr_avg']} | {row['ras_2021_2025_tier']} | "
            f"{row['suggested_ras_threshold_premium_pos_qb_ot_edge_cb']} | "
            f"{row['suggested_ras_threshold_mid_value_pos_wr_s_dt_lb_s']} | "
            f"{row['suggested_ras_threshold_low_value_pos_iol_te_rb']} | "
            f"{row['explicit_public_rb_threshold_source_found']} |"
        )
    args.out_digest.write_text("\n".join(digest_lines))

    pos_digest_lines = [
        "# Team Athletic Thresholds By Position (2021-2025)",
        "",
        f"- Source window: `{args.year_start}-{args.year_end}`",
        f"- Rows: `{len(pos_rows)}`",
        "- `team_position_threshold_ras` is a blended value:",
        "- blend(team bucket threshold, team-position observed median), weighted by sample size.",
        "",
        "| Team | Pos | Bucket | N | Pos Avg | Pos Median | Bucket Base | Position Threshold | Confidence W |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in pos_rows:
        pos_digest_lines.append(
            f"| {row['team']} | {row['position']} | {row['position_bucket']} | "
            f"{row['sample_n_2021_2025']} | {row['ras_proxy_avg_2021_2025']} | "
            f"{row['ras_proxy_q50_2021_2025']} | {row['team_bucket_threshold_ras']} | "
            f"{row['team_position_threshold_ras']} | {row['position_threshold_confidence_weight']} |"
        )
    args.out_position_digest.write_text("\n".join(pos_digest_lines))

    print(f"Wrote team thresholds: {args.out_team}")
    print(f"Wrote position thresholds: {args.out_position}")
    print(f"Wrote digest: {args.out_digest}")
    print(f"Wrote position digest: {args.out_position_digest}")
    print(f"Rows used ({args.year_start}-{args.year_end}): {len(calc)}")


if __name__ == "__main__":
    main()
