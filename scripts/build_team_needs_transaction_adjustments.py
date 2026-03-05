#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CBS_TXN_PATH = ROOT / "data" / "processed" / "cbs_nfl_transactions_2026.csv"
OVERRIDE_PATH = ROOT / "data" / "sources" / "manual" / "transactions_overrides_2026.csv"
OUT_PATH = ROOT / "data" / "sources" / "team_needs_transaction_adjustments_2026.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "team_needs_transaction_adjustments_report_2026.md"

POS_MAP = {
    "QB": "QB",
    "RB": "RB",
    "WR": "WR",
    "TE": "TE",
    "OT": "OT",
    "T": "OT",
    "G": "IOL",
    "OG": "IOL",
    "C": "IOL",
    "OL": "OT",
    "DE": "EDGE",
    "EDGE": "EDGE",
    "DT": "DT",
    "NT": "DT",
    "DL": "DT",
    "LB": "LB",
    "ILB": "LB",
    "OLB": "LB",
    "CB": "CB",
    "DB": "CB",
    "S": "S",
    "FS": "S",
    "SS": "S",
    "SAF": "S",
}


def _norm_pos(pos: str) -> str:
    key = str(pos or "").strip().upper()
    return POS_MAP.get(key, "")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _safe_date(value: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(str(value).strip(), "%B %d, %Y").date()
    except Exception:
        try:
            return dt.date.fromisoformat(str(value).strip())
        except Exception:
            return None


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _is_truthy(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _is_confirmed_status(status: str) -> bool:
    text = str(status or "").strip().lower()
    if not text:
        return True
    return text in {"confirmed", "official", "signed", "re-signed", "released", "waived", "traded", "activated"}


def _event_to_effects(weight: float) -> dict:
    w = abs(float(weight))
    if weight > 0:
        return {
            "depth_delta": 0.09 * w,
            "future_need_1y_delta": 0.08 * w,
            "future_need_2y_delta": 0.04 * w,
            "starter_quality_delta": -0.07 * w,
            "roster_count_delta": -0.60 * w,
        }
    if weight < 0:
        return {
            "depth_delta": -0.075 * w,
            "future_need_1y_delta": -0.055 * w,
            "future_need_2y_delta": -0.03 * w,
            "starter_quality_delta": 0.06 * w,
            "roster_count_delta": 0.50 * w,
        }
    return {
        "depth_delta": 0.0,
        "future_need_1y_delta": 0.0,
        "future_need_2y_delta": 0.0,
        "starter_quality_delta": 0.0,
        "roster_count_delta": 0.0,
    }


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def build_adjustments(window_days: int = 7) -> list[dict]:
    today = dt.date.today()
    min_date = today - dt.timedelta(days=max(1, int(window_days)))

    cbs_rows = _read_csv(CBS_TXN_PATH)
    ov_rows = _read_csv(OVERRIDE_PATH)

    events: list[dict] = []

    # CBS events map directly by team/direction-weight row.
    for row in cbs_rows:
        pos = _norm_pos(row.get("position", ""))
        if not pos:
            continue
        event_date = _safe_date(row.get("event_date", ""))
        if event_date is None or event_date < min_date:
            continue
        team = str(row.get("team", "")).strip().upper()
        if not team:
            continue
        weight = _safe_float(row.get("impact_weight"), 0.0)
        if abs(weight) < 0.001:
            continue
        events.append(
            {
                "event_date": event_date,
                "team": team,
                "position": pos,
                "weight": weight,
                "transaction_status": "confirmed",
                "player_name": str(row.get("player_name", "")).strip(),
                "action_text": str(row.get("action_text", "")).strip(),
                "source_url": str(row.get("source_url", "")).strip(),
            }
        )

    # Manual overrides capture both sides of important trades where CBS only shows the destination.
    for row in ov_rows:
        pos = _norm_pos(row.get("position", ""))
        if not pos:
            continue
        event_date = _safe_date(row.get("event_date", ""))
        if event_date is None or event_date < min_date:
            continue
        from_team = str(row.get("from_team", "")).strip().upper()
        to_team = str(row.get("to_team", "")).strip().upper()
        base_weight = abs(_safe_float(row.get("impact_weight"), 1.0))
        tx_status = str(row.get("transaction_status", "")).strip().lower() or "confirmed"
        apply_to_needs_raw = row.get("apply_to_team_needs", "")
        apply_to_needs = _is_truthy(apply_to_needs_raw) if str(apply_to_needs_raw).strip() else _is_confirmed_status(tx_status)
        if not apply_to_needs:
            continue
        if from_team:
            events.append(
                {
                    "event_date": event_date,
                    "team": from_team,
                    "position": pos,
                    "weight": base_weight,
                    "transaction_status": tx_status,
                    "player_name": str(row.get("player_name", "")).strip(),
                    "action_text": str(row.get("action_text", "trade_out")).strip(),
                    "source_url": str(row.get("source_url", "")).strip(),
                }
            )
        if to_team:
            events.append(
                {
                    "event_date": event_date,
                    "team": to_team,
                    "position": pos,
                    "weight": -base_weight,
                    "transaction_status": tx_status,
                    "player_name": str(row.get("player_name", "")).strip(),
                    "action_text": str(row.get("action_text", "trade_in")).strip(),
                    "source_url": str(row.get("source_url", "")).strip(),
                }
            )

    grouped: dict[tuple[str, str], dict] = defaultdict(
        lambda: {
            "depth_delta": 0.0,
            "future_need_1y_delta": 0.0,
            "future_need_2y_delta": 0.0,
            "starter_quality_delta": 0.0,
            "roster_count_delta": 0.0,
            "transaction_count": 0,
            "event_summary": [],
            "source_url": "",
        }
    )

    for ev in sorted(events, key=lambda x: (x["event_date"], x["team"], x["position"])):
        key = (ev["team"], ev["position"])
        node = grouped[key]
        effects = _event_to_effects(ev["weight"])
        for k, v in effects.items():
            node[k] += float(v)
        node["transaction_count"] += 1
        if len(node["event_summary"]) < 8:
            status_txt = str(ev.get("transaction_status", "")).strip().lower() or "confirmed"
            tag = f"{ev['event_date'].isoformat()} {status_txt} {ev['player_name']} ({ev['action_text']})"
            node["event_summary"].append(tag)
        if not node["source_url"]:
            node["source_url"] = ev["source_url"]

    out_rows: list[dict] = []
    for (team, pos), node in sorted(grouped.items()):
        out_rows.append(
            {
                "team": team,
                "position": pos,
                "depth_delta": round(_clamp(node["depth_delta"], -0.35, 0.35), 4),
                "future_need_1y_delta": round(_clamp(node["future_need_1y_delta"], -0.30, 0.30), 4),
                "future_need_2y_delta": round(_clamp(node["future_need_2y_delta"], -0.20, 0.20), 4),
                "starter_quality_delta": round(_clamp(node["starter_quality_delta"], -0.30, 0.30), 4),
                "roster_count_delta": round(_clamp(node["roster_count_delta"], -2.5, 2.5), 3),
                "transaction_count": int(node["transaction_count"]),
                "event_summary": " | ".join(node["event_summary"]),
                "window_days": int(window_days),
                "source_url": node["source_url"] or "https://www.cbssports.com/nfl/transactions/",
            }
        )
    return out_rows


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "team",
        "position",
        "depth_delta",
        "future_need_1y_delta",
        "future_need_2y_delta",
        "starter_quality_delta",
        "roster_count_delta",
        "transaction_count",
        "event_summary",
        "window_days",
        "source_url",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_report(path: Path, rows: list[dict], window_days: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Team Needs Transaction Adjustments",
        "",
        f"- generated_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- window_days: `{window_days}`",
        f"- rows: `{len(rows)}`",
        "",
        "| Team | Pos | Txn Count | Depth Δ | Future1Y Δ | StarterQ Δ |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in sorted(rows, key=lambda r: int(r["transaction_count"]), reverse=True)[:50]:
        lines.append(
            f"| {row['team']} | {row['position']} | {row['transaction_count']} | "
            f"{row['depth_delta']} | {row['future_need_1y_delta']} | {row['starter_quality_delta']} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    rows = build_adjustments(window_days=7)
    _write_csv(OUT_PATH, rows)
    _write_report(REPORT_PATH, rows, window_days=7)
    print(f"Adjustment rows: {len(rows)}")
    print(f"Wrote: {OUT_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
