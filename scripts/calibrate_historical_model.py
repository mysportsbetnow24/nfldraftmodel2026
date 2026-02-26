#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.modeling.calibration import (
    DEFAULT_CALIBRATION_PATH,
    DEFAULT_HISTORICAL_PATH,
    build_config,
    load_historical_rows,
    save_calibration_outputs,
    year_based_backtest,
)


OUT_REPORT = ROOT / "data" / "outputs" / "historical_calibration_report_2016_2025.md"
OUT_BACKTEST_CSV = ROOT / "data" / "outputs" / "historical_calibration_backtest_2016_2025.csv"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Calibrate draft model from 2016-2025 historical outcomes")
    p.add_argument("--input", type=str, default=str(DEFAULT_HISTORICAL_PATH))
    p.add_argument("--output", type=str, default=str(DEFAULT_CALIBRATION_PATH))
    p.add_argument("--min-year", type=int, default=2016)
    p.add_argument("--max-year", type=int, default=2025)
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input)
    out_path = Path(args.output)

    rows = load_historical_rows(path=in_path, min_year=args.min_year, max_year=args.max_year)
    if not rows:
        raise RuntimeError("No historical rows loaded. Provide a valid historical outcomes CSV.")

    cfg = build_config(rows)
    save_calibration_outputs(rows, cfg, output_path=out_path)
    backtest_rows = year_based_backtest(rows)

    if backtest_rows:
        OUT_BACKTEST_CSV.parent.mkdir(parents=True, exist_ok=True)
        with OUT_BACKTEST_CSV.open("w", newline="") as f:
            import csv

            writer = csv.DictWriter(f, fieldnames=list(backtest_rows[0].keys()))
            writer.writeheader()
            writer.writerows(backtest_rows)

    report = [
        "# Historical Calibration Report (2016-2025)",
        "",
        f"- Sample size: **{cfg.sample_size}**",
        f"- Data source: **{cfg.data_source}**",
        f"- Year window: **{args.min_year}-{args.max_year}**",
        f"- Logistic intercept: `{cfg.intercept}`",
        f"- Logistic slope: `{cfg.slope}`",
        "",
        "## Position Additives",
        "",
        "| Position | Additive |",
        "|---|---:|",
    ]

    for pos, delta in sorted(cfg.position_additive.items()):
        report.append(f"| {pos} | {delta:+.4f} |")

    report.extend(["", "## Year-Based Backtest", "", "| Holdout Year | Train Rows | Test Rows | Brier | Accuracy | Avg Pred | Obs Rate |", "|---:|---:|---:|---:|---:|---:|---:|"])
    if backtest_rows:
        for row in backtest_rows:
            report.append(
                f"| {row['holdout_year']} | {row['train_rows']} | {row['test_rows']} | "
                f"{row['brier_score']} | {row['accuracy']} | {row['avg_predicted_success']} | "
                f"{row['observed_success_rate']} |"
            )
    else:
        report.append("| n/a | 0 | 0 | n/a | n/a | n/a | n/a |")

    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.write_text("\n".join(report))

    print(f"Rows loaded: {len(rows)}")
    print(f"Calibration config: {out_path}")
    print(f"Report: {OUT_REPORT}")
    if backtest_rows:
        print(f"Backtest CSV: {OUT_BACKTEST_CSV}")


if __name__ == "__main__":
    main()
