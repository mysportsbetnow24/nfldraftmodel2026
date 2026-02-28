#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
IN_PATH = ROOT / "data" / "sources" / "external" / "nflverse" / "combine.parquet"
OUT_PATH = ROOT / "data" / "sources" / "external" / "nflverse" / "nflverse_combine_standardized.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "nflverse_combine_baseline_build_report_2026-02-28.md"


def _to_float(v) -> float | None:
    if v is None:
        return None
    txt = str(v).strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _height_to_inches(ht: str) -> float | None:
    txt = str(ht or "").strip()
    if not txt:
        return None
    if "-" in txt:
        parts = txt.split("-", 1)
        try:
            return float(int(parts[0]) * 12 + int(parts[1]))
        except ValueError:
            return None
    try:
        return float(txt)
    except ValueError:
        return None


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Convert nflverse combine parquet to athletic baseline schema.")
    p.add_argument("--input", type=Path, default=IN_PATH)
    p.add_argument("--output", type=Path, default=OUT_PATH)
    p.add_argument("--report", type=Path, default=REPORT_PATH)
    args = p.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Missing input: {args.input}")

    df = pl.read_parquet(args.input)
    rows: list[dict] = []
    for row in df.iter_rows(named=True):
        season = int(_to_float(row.get("season")) or 0)
        if season <= 0:
            continue
        player = str(row.get("player_name", "")).strip()
        pos = str(row.get("pos", "")).strip().upper()
        if not player or not pos:
            continue

        picktotal = _to_float(row.get("draft_ovr"))
        pickround = _to_float(row.get("draft_round"))
        payload = {
            "year": season,
            "name": player,
            "position": pos,
            "pickround": pickround if pickround is not None else "",
            "picktotal": picktotal if picktotal is not None else "",
            "heightinchestotal": _height_to_inches(row.get("ht")),
            "weight": _to_float(row.get("wt")),
            "arms": "",
            "hands": "",
            "fortyyd": _to_float(row.get("forty")),
            "tenyd": "",
            "vertical": _to_float(row.get("vertical")),
            "broad": _to_float(row.get("broad_jump")),
            "threecone": _to_float(row.get("cone")),
            "twentyss": _to_float(row.get("shuttle")),
            "bench": _to_float(row.get("bench")),
            "source": "nflverse_combine",
        }
        rows.append(payload)

    rows.sort(key=lambda r: (int(r["year"]), str(r["position"]), str(r["name"])))
    _write_csv(args.output, rows)

    lines = [
        "# NFLverse Combine Baseline Build Report",
        "",
        f"- Input: `{args.input}`",
        f"- Output: `{args.output}`",
        f"- Input rows: `{df.height}`",
        f"- Output rows: `{len(rows)}`",
        "",
        "## Notes",
        "",
        "- Converted to the same metric column names expected by `athletic_profile_loader.py`.",
        "- Missing arms/hands/10-split in nflverse combine are left blank and handled by coverage penalties.",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines))

    print(f"Wrote: {args.output}")
    print(f"Rows: {len(rows)}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
