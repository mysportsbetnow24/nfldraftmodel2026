#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IN_PATH = ROOT / "data" / "processed" / "leagify_historical_outcomes_2015_2023.csv"
OUT_PATH = ROOT / "data" / "processed" / "historical_labels_leagify_2015_2023.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "leagify_historical_labels_report_2026-02-28.md"


YEAR_WEIGHT = {
    2015: 1.00,
    2016: 1.00,
    2017: 1.00,
    2018: 1.00,
    2019: 0.85,
    2020: 0.70,
    2021: 0.50,
    2022: 0.30,
    2023: 0.15,
}


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


def _read_rows(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _starter_seasons_proxy(starts: float, games: float) -> int:
    # Conservative proxy from starts + games.
    starts_from_games = games / 16.0
    estimate = max(starts / 10.0, starts_from_games * 0.45)
    return int(max(0, min(10, round(estimate))))


def main() -> None:
    parser = argparse.ArgumentParser(description="Create leakage-safe historical labels from Leagify outcomes.")
    parser.add_argument("--input", type=Path, default=IN_PATH)
    parser.add_argument("--output", type=Path, default=OUT_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Missing input: {args.input}")

    rows = _read_rows(args.input)
    out: list[dict] = []
    for row in rows:
        draft_year = _to_int(row.get("draft_year"))
        if draft_year is None:
            continue
        wav = float(_to_float(row.get("wav")) or 0.0)
        games = float(_to_float(row.get("games")) or 0.0)
        starts = float(_to_float(row.get("starts")) or 0.0)
        ap1 = float(_to_float(row.get("ap1")) or 0.0)
        pb = float(_to_float(row.get("pb")) or 0.0)
        years_in_career = int(_to_float(row.get("years_in_career")) or 0)
        to_year = _to_int(row.get("to_year"))

        success_label_3yr = 1 if (wav >= 10.0 or games >= 32.0 or (ap1 + pb) >= 1.0) else 0
        starter_label_3yr = 1 if (starts >= 2.0 or games >= 40.0) else 0
        ceiling_label = 1 if (ap1 >= 1.0 or pb >= 2.0) else 0

        second_contract_proxy = 0
        if years_in_career >= 5:
            second_contract_proxy = 1
        elif to_year is not None and to_year >= (draft_year + 4):
            second_contract_proxy = 1

        starter_seasons = _starter_seasons_proxy(starts=starts, games=games)
        weight = YEAR_WEIGHT.get(draft_year, 0.20 if draft_year >= 2024 else 1.0)

        out.append(
            {
                **row,
                "success_label_3yr": success_label_3yr,
                "starter_label_3yr": starter_label_3yr,
                "ceiling_label": ceiling_label,
                "starter_seasons_proxy": starter_seasons,
                "second_contract_proxy": second_contract_proxy,
                "censor_weight": round(float(weight), 2),
            }
        )

    out.sort(key=lambda r: (int(r.get("draft_year", 0) or 0), int(float(r.get("overall_pick", 0) or 0))))
    _write_csv(args.output, out)

    n = len(out)
    weighted_n = round(sum(float(r.get("censor_weight", 0.0) or 0.0) for r in out), 2)
    if n > 0:
        success_rate = sum(int(float(r.get("success_label_3yr", 0) or 0)) for r in out) / n
        starter_rate = sum(int(float(r.get("starter_label_3yr", 0) or 0)) for r in out) / n
        ceiling_rate = sum(int(float(r.get("ceiling_label", 0) or 0)) for r in out) / n
    else:
        success_rate = starter_rate = ceiling_rate = 0.0

    lines = [
        "# Leagify Historical Labels Report",
        "",
        f"- Input: `{args.input}`",
        f"- Output: `{args.output}`",
        f"- Rows: **{n}**",
        f"- Weighted rows (censor): **{weighted_n}**",
        "",
        "## Label Rates (Unweighted)",
        "",
        f"- success_label_3yr: **{success_rate:.3f}**",
        f"- starter_label_3yr: **{starter_rate:.3f}**",
        f"- ceiling_label: **{ceiling_rate:.3f}**",
        "",
        "## Censor Weight Map",
        "",
        "| Draft Year | Weight |",
        "|---:|---:|",
    ]
    for year in sorted(YEAR_WEIGHT):
        lines.append(f"| {year} | {YEAR_WEIGHT[year]:.2f} |")

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Rows: {n}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
