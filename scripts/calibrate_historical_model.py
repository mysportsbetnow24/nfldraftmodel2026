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
)


OUT_REPORT = ROOT / "data" / "outputs" / "historical_calibration_report_2016_2025.md"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Calibrate draft model from 2016-2025 historical outcomes")
    p.add_argument("--input", type=str, default=str(DEFAULT_HISTORICAL_PATH))
    p.add_argument("--output", type=str, default=str(DEFAULT_CALIBRATION_PATH))
    p.add_argument("--no-seed", action="store_true", help="Do not auto-generate synthetic seed if input file missing")
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input)
    out_path = Path(args.output)

    rows = load_historical_rows(path=in_path, generate_seed_if_missing=not args.no_seed)
    if not rows:
        raise RuntimeError("No historical rows loaded. Provide a valid historical outcomes CSV.")

    cfg = build_config(rows)
    save_calibration_outputs(rows, cfg, output_path=out_path)

    report = [
        "# Historical Calibration Report (2016-2025)",
        "",
        f"- Sample size: **{cfg.sample_size}**",
        f"- Data source: **{cfg.data_source}**",
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

    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.write_text("\n".join(report))

    print(f"Rows loaded: {len(rows)}")
    print(f"Calibration config: {out_path}")
    print(f"Report: {OUT_REPORT}")


if __name__ == "__main__":
    main()
