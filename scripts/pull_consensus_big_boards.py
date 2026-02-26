#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import re
import ssl
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


TANKATHON_URL = "https://www.tankathon.com/nfl/big_board"
NFLMOCK_URL = "https://www.nflmockdraftdatabase.com/big-boards/2026/consensus-big-board-2026"
ATHLETIC_URL = "https://www.nytimes.com/athletic/7052286/2026/02/18/nfl-draft-2026-consensus-big-board-arvell-reese/"
RINGER_URL = "https://www.theringer.com/nfl-draft/2026/big-board"
ANALYST_SEED_URL = "https://www.espn.com/nfl/draft2026/"
EXTERNAL_BOARD_URL = "manual://nfl-draft-bigboard-scout-mode-2026-02-25.csv"

PROCESSED_PATH = ROOT / "data" / "processed" / "consensus_big_boards_2026.csv"
REPORT_PATH = ROOT / "data" / "outputs" / "consensus_big_boards_pull_report.md"
MANUAL_NFLMOCK_PATH = ROOT / "data" / "sources" / "manual" / "nflmock_consensus_2026.csv"
MANUAL_ATHLETIC_PATH = ROOT / "data" / "sources" / "manual" / "athletic_consensus_2026.csv"
MANUAL_RINGER_PATH = ROOT / "data" / "sources" / "manual" / "ringer_big_board_2026.csv"
ANALYST_SEED_PATH = ROOT / "data" / "sources" / "analyst_rankings_seed.csv"
EXTERNAL_BOARD_PATH = ROOT / "data" / "sources" / "manual" / "nfl-draft-bigboard-scout-mode-2026-02-25.csv"


POS_RE = r"(?:QB|RB|WR|TE|OT|IOL|EDGE|DL|DT|DE|LB|CB|S|DB|C|G|T)"
TANKATHON_PLAYER_RE = re.compile(rf"^(?P<name>.+?)\s+(?P<pos>{POS_RE})\s*\|\s*(?P<school>.+)$")
NFLMOCK_PLAYER_RE = re.compile(rf"^(?P<name>.+?)\s+(?P<pos>{POS_RE})\s+(?P<school>.+)$")
RINGER_PLAYER_RE = re.compile(rf"(?P<name>[^,]+),\s*(?P<pos>{POS_RE}),\s*(?P<school>.+)$", re.I)
TANKATHON_ROW_RE = re.compile(
    r'<div class="mock-row[^"]*".*?<div class="mock-row-pick-number">\s*(?P<rank>\d{1,3})\s*</div>'
    r'.*?<div class="mock-row-name">\s*(?P<name>[^<]+?)\s*</div>'
    r'.*?<div class="mock-row-school-position">\s*(?P<pos>[A-Za-z/]+)\s*\|\s*(?P<school>[^<]+?)\s*</div>',
    re.I | re.S,
)
RINGER_CARD_RE = re.compile(
    r'aria-label="Open player card for (?P<name>[^"]+) on the big board"'
    r'.{0,8000}?font-gt-america-expanded[^>]*>\s*(?P<rank>\d{1,3})\s*</span>'
    r'.{0,5000}?text-foreground[^>]*>\s*(?P<pos_full>[^,<]+),\s*(?P<school>[^<]+)\s*</div>',
    re.I | re.S,
)

POS_FULL_TO_ABBR = {
    "quarterback": "QB",
    "running back": "RB",
    "wide receiver": "WR",
    "tight end": "TE",
    "offensive tackle": "OT",
    "offensive guard": "IOL",
    "offensive center": "IOL",
    "center": "IOL",
    "guard": "IOL",
    "offensive lineman": "IOL",
    "edge": "EDGE",
    "edge rusher": "EDGE",
    "defensive end": "EDGE",
    "defensive tackle": "DT",
    "interior defensive line": "DT",
    "linebacker": "LB",
    "cornerback": "CB",
    "safety": "S",
}


def _normalize_pos(value: str) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    low = txt.lower()
    if low in POS_FULL_TO_ABBR:
        return POS_FULL_TO_ABBR[low]

    raw = txt.upper().replace(".", "").strip()
    raw = {"ED": "EDGE", "DE": "EDGE", "DL": "DT", "DB": "CB", "C": "IOL", "G": "IOL", "T": "OT"}.get(raw, raw)
    if re.fullmatch(POS_RE, raw):
        return raw
    return raw


def _fetch(url: str) -> str:
    req = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        },
    )
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, timeout=45, context=ssl_ctx) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _to_text_lines(page_html: str) -> list[str]:
    cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", page_html)
    cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = re.sub(r"(?s)<[^>]+>", "\n", cleaned)
    cleaned = html.unescape(cleaned)
    out = []
    for raw in cleaned.splitlines():
        line = re.sub(r"\\s+", " ", raw).strip()
        if line:
            out.append(line)
    return out


def _rank_nearby(lines: list[str], idx: int, back: int = 8) -> int | None:
    for off in range(1, back + 1):
        j = idx - off
        if j < 0:
            break
        token = lines[j]
        if token.isdigit():
            val = int(token)
            if 1 <= val <= 500:
                return val
    return None


def _dedupe_best(rows: list[dict]) -> list[dict]:
    best: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (str(row.get("source", "")).strip(), str(row.get("player_name", "")).strip().lower())
        cur = best.get(key)
        if cur is None or int(row["consensus_rank"]) < int(cur["consensus_rank"]):
            best[key] = row
    out = list(best.values())
    out.sort(key=lambda r: (r["source"], int(r["consensus_rank"])))
    return out


def parse_tankathon(page_html: str, snapshot_date: str) -> list[dict]:
    rows: list[dict] = []

    # Primary parser: extract directly from row HTML.
    for m in TANKATHON_ROW_RE.finditer(page_html):
        rank = int(m.group("rank"))
        if not (1 <= rank <= 500):
            continue
        pos = _normalize_pos(m.group("pos"))
        rows.append(
            {
                "source": "Tankathon_2026",
                "snapshot_date": snapshot_date,
                "consensus_rank": rank,
                "player_name": m.group("name").strip(),
                "school": m.group("school").strip(),
                "position": pos,
                "source_url": TANKATHON_URL,
            }
        )

    if rows:
        return _dedupe_best(rows)

    # Fallback parser: flattened text lines.
    lines = _to_text_lines(page_html)
    for idx, line in enumerate(lines):
        m = TANKATHON_PLAYER_RE.match(line)
        if not m:
            continue
        rank = _rank_nearby(lines, idx, back=8)
        if rank is None:
            continue
        rows.append(
            {
                "source": "Tankathon_2026",
                "snapshot_date": snapshot_date,
                "consensus_rank": rank,
                "player_name": m.group("name").strip(),
                "school": m.group("school").strip(),
                "position": _normalize_pos(m.group("pos")),
                "source_url": TANKATHON_URL,
            }
        )
    return _dedupe_best(rows)


def parse_nflmock(page_html: str, snapshot_date: str) -> list[dict]:
    # Primary parser: parse embedded data-react JSON payload.
    rows: list[dict] = []
    m = re.search(r'data-react-class="big_boards/Consensus"\s+data-react-props="([^"]+)"', page_html)
    if m:
        try:
            payload = json.loads(html.unescape(m.group(1)))
            selections = payload.get("mock", {}).get("selections", [])
            for sel in selections:
                rank = int(sel.get("pick") or 0)
                if not (1 <= rank <= 500):
                    continue
                player = sel.get("player") or {}
                name = str(player.get("name") or "").strip()
                if not name:
                    continue
                pos = _normalize_pos(str(player.get("position") or ""))
                school = str((player.get("college") or {}).get("name") or "").strip()
                rows.append(
                    {
                        "source": "NFLMockDraftDatabase_2026",
                        "snapshot_date": snapshot_date,
                        "consensus_rank": rank,
                        "player_name": name,
                        "school": school,
                        "position": pos,
                        "source_url": NFLMOCK_URL,
                    }
                )
        except (json.JSONDecodeError, TypeError, ValueError):
            rows = []

    if rows:
        return _dedupe_best(rows)

    # Fallback parser: flattened text lines.
    lines = _to_text_lines(page_html)
    for idx, line in enumerate(lines):
        m = NFLMOCK_PLAYER_RE.match(line)
        if not m:
            continue
        rank = _rank_nearby(lines, idx, back=6)
        if rank is None:
            continue
        rows.append(
            {
                "source": "NFLMockDraftDatabase_2026",
                "snapshot_date": snapshot_date,
                "consensus_rank": rank,
                "player_name": m.group("name").strip(),
                "school": m.group("school").strip(),
                "position": _normalize_pos(m.group("pos")),
                "source_url": NFLMOCK_URL,
            }
        )
    return _dedupe_best(rows)


def parse_athletic(page_html: str, snapshot_date: str) -> list[dict]:
    """
    Best-effort parser for Athletic consensus pages.
    The Athletic is often paywalled, so this may return 0 rows; manual CSV fallback is primary.
    """
    lines = _to_text_lines(page_html)
    rows: list[dict] = []
    for idx, line in enumerate(lines):
        m = NFLMOCK_PLAYER_RE.match(line)
        if not m:
            continue
        rank = _rank_nearby(lines, idx, back=8)
        if rank is None:
            continue
        rows.append(
            {
                "source": "Athletic_Consensus_2026",
                "snapshot_date": snapshot_date,
                "consensus_rank": rank,
                "player_name": m.group("name").strip(),
                "school": m.group("school").strip(),
                "position": m.group("pos").strip().upper(),
                "source_url": ATHLETIC_URL,
            }
        )
    return _dedupe_best(rows)


def parse_ringer(page_html: str, snapshot_date: str) -> list[dict]:
    # Primary parser: parse player card DOM blocks.
    rows: list[dict] = []
    for m in RINGER_CARD_RE.finditer(page_html):
        rank = int(m.group("rank"))
        if not (1 <= rank <= 500):
            continue
        rows.append(
            {
                "source": "Ringer_NFL_Draft_Guide_2026",
                "snapshot_date": snapshot_date,
                "consensus_rank": rank,
                "player_name": m.group("name").strip(),
                "school": m.group("school").strip(),
                "position": _normalize_pos(m.group("pos_full")),
                "source_url": RINGER_URL,
            }
        )
    if rows:
        return _dedupe_best(rows)

    # Fallback parser: flattened text lines.
    lines = _to_text_lines(page_html)
    rows = []
    pending_rank: int | None = None
    for raw in lines:
        line = re.sub(r"^Image\s+(for\s+)?", "", raw, flags=re.I).strip()
        rank = int(line) if line.isdigit() else None
        if rank is not None and 1 <= rank <= 400:
            pending_rank = rank
            continue
        m = RINGER_PLAYER_RE.search(line)
        if not m or pending_rank is None:
            continue
        rows.append(
            {
                "source": "Ringer_NFL_Draft_Guide_2026",
                "snapshot_date": snapshot_date,
                "consensus_rank": pending_rank,
                "player_name": m.group("name").strip(),
                "school": m.group("school").strip(),
                "position": _normalize_pos(m.group("pos")),
                "source_url": RINGER_URL,
            }
        )
        pending_rank = None
    return _dedupe_best(rows)


def load_nflmock_manual_csv(path: Path, snapshot_date: str) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(float(str(row.get("consensus_rank", row.get("rank", ""))).strip()))
            except (TypeError, ValueError):
                continue
            if rank <= 0:
                continue
            player = str(row.get("player_name", row.get("player", ""))).strip()
            if not player:
                continue
            rows.append(
                {
                    "source": "NFLMockDraftDatabase_2026",
                    "snapshot_date": snapshot_date,
                    "consensus_rank": rank,
                    "player_name": player,
                    "school": str(row.get("school", "")).strip(),
                    "position": str(row.get("position", row.get("pos", ""))).strip().upper(),
                    "source_url": NFLMOCK_URL,
                }
            )
    return _dedupe_best(rows)


def load_athletic_manual_csv(path: Path, snapshot_date: str) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(float(str(row.get("consensus_rank", row.get("rank", ""))).strip()))
            except (TypeError, ValueError):
                continue
            if rank <= 0:
                continue
            player = str(row.get("player_name", row.get("player", ""))).strip()
            if not player:
                continue
            rows.append(
                {
                    "source": "Athletic_Consensus_2026",
                    "snapshot_date": snapshot_date,
                    "consensus_rank": rank,
                    "player_name": player,
                    "school": str(row.get("school", "")).strip(),
                    "position": str(row.get("position", row.get("pos", ""))).strip().upper(),
                    "source_url": ATHLETIC_URL,
                }
            )
    return _dedupe_best(rows)


def load_ringer_manual_csv(path: Path, snapshot_date: str) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(float(str(row.get("consensus_rank", row.get("rank", row.get("source_rank", "")))).strip()))
            except (TypeError, ValueError):
                continue
            if rank <= 0:
                continue
            player = str(row.get("player_name", row.get("player", ""))).strip()
            if not player:
                continue
            rows.append(
                {
                    "source": "Ringer_NFL_Draft_Guide_2026",
                    "snapshot_date": str(row.get("snapshot_date", "")).strip() or snapshot_date,
                    "consensus_rank": rank,
                    "player_name": player,
                    "school": str(row.get("school", "")).strip(),
                    "position": str(row.get("position", row.get("pos", ""))).strip().upper(),
                    "source_url": str(row.get("source_url", "")).strip() or RINGER_URL,
                }
            )
    return _dedupe_best(rows)


def load_analyst_seed_rows(path: Path, snapshot_date: str) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(float(str(row.get("source_rank", "")).strip()))
            except (TypeError, ValueError):
                continue
            if rank <= 0:
                continue
            player = str(row.get("player_name", "")).strip()
            if not player:
                continue
            source = str(row.get("source", "")).strip() or "AnalystSeed_2026"
            rows.append(
                {
                    "source": source,
                    "snapshot_date": snapshot_date,
                    "consensus_rank": rank,
                    "player_name": player,
                    "school": str(row.get("school", "")).strip(),
                    "position": str(row.get("position", "")).strip().upper(),
                    "source_url": ANALYST_SEED_URL,
                }
            )
    return _dedupe_best(rows)


def load_external_board_rows(path: Path, snapshot_date: str) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(float(str(row.get("Rank", "")).strip()))
            except (TypeError, ValueError):
                continue
            if rank <= 0:
                continue
            player = str(row.get("Player", "")).strip()
            if not player:
                continue
            rows.append(
                {
                    "source": "ExternalScoutBoard_2026",
                    "snapshot_date": snapshot_date,
                    "consensus_rank": rank,
                    "player_name": player,
                    "school": str(row.get("School", "")).strip(),
                    "position": str(row.get("Pos", "")).strip().upper(),
                    "source_url": EXTERNAL_BOARD_URL,
                }
            )
    return _dedupe_best(rows)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["source", "snapshot_date", "consensus_rank", "player_name", "school", "position", "source_url"]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_report(
    path: Path,
    rows: list[dict],
    tank_rows: list[dict],
    nflmock_rows: list[dict],
    athletic_rows: list[dict],
    ringer_rows: list[dict],
    analyst_rows: list[dict],
    external_rows: list[dict],
    warnings: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top = sorted(rows, key=lambda r: int(r["consensus_rank"]))[:25]
    lines = [
        "# Consensus Big Boards Pull Report",
        "",
        f"- pulled_at_utc: `{dt.datetime.now(dt.UTC).isoformat()}`",
        f"- Tankathon rows: `{len(tank_rows)}`",
        f"- NFLMock rows: `{len(nflmock_rows)}`",
        f"- Athletic rows: `{len(athletic_rows)}`",
        f"- Ringer rows: `{len(ringer_rows)}`",
        f"- Analyst seed rows: `{len(analyst_rows)}`",
        f"- External board rows: `{len(external_rows)}`",
        f"- Combined rows: `{len(rows)}`",
        "",
        "## Warnings",
    ]
    if warnings:
        for w in warnings:
            lines.append(f"- {w}")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Top 25 (By Rank Within Source)",
            "",
            "| Source | Rank | Player | Pos | School |",
            "|---|---:|---|---|---|",
        ]
    )
    for r in top:
        lines.append(
            f"| {r.get('source','')} | {r.get('consensus_rank','')} | {r.get('player_name','')} | "
            f"{r.get('position','')} | {r.get('school','')} |"
        )
    path.write_text("\n".join(lines))


def main() -> None:
    p = argparse.ArgumentParser(description="Pull 2026 consensus big boards from Tankathon and NFLMockDB.")
    p.add_argument("--skip-fetch", action="store_true", help="Do not fetch web pages; only use manual CSV fallback.")
    p.add_argument("--manual-nflmock-csv", default=str(MANUAL_NFLMOCK_PATH), help="Manual NFLMock CSV fallback path.")
    p.add_argument("--manual-athletic-csv", default=str(MANUAL_ATHLETIC_PATH), help="Manual Athletic CSV fallback path.")
    p.add_argument("--manual-ringer-csv", default=str(MANUAL_RINGER_PATH), help="Manual Ringer CSV fallback path.")
    p.add_argument(
        "--no-local-seeds",
        action="store_true",
        help="Disable local analyst + external-board consensus sources.",
    )
    p.add_argument("--out", default=str(PROCESSED_PATH), help="Output CSV path.")
    args = p.parse_args()

    snapshot_date = dt.date.today().isoformat()
    warnings: list[str] = []

    tank_rows: list[dict] = []
    nflmock_rows: list[dict] = []
    athletic_rows: list[dict] = []
    ringer_rows: list[dict] = []
    analyst_rows: list[dict] = []
    external_rows: list[dict] = []

    if not args.skip_fetch:
        try:
            tank_html = _fetch(TANKATHON_URL)
            tank_rows = parse_tankathon(tank_html, snapshot_date=snapshot_date)
            if not tank_rows:
                warnings.append("Tankathon fetched but parser returned 0 rows.")
        except Exception as exc:
            warnings.append(f"Tankathon fetch failed: {exc}")

        try:
            nflmock_html = _fetch(NFLMOCK_URL)
            nflmock_rows = parse_nflmock(nflmock_html, snapshot_date=snapshot_date)
            if not nflmock_rows:
                warnings.append("NFLMock fetched but parser returned 0 rows (likely JS/anti-bot page).")
        except Exception as exc:
            warnings.append(f"NFLMock fetch failed: {exc}")

        try:
            athletic_html = _fetch(ATHLETIC_URL)
            athletic_rows = parse_athletic(athletic_html, snapshot_date=snapshot_date)
            if not athletic_rows:
                warnings.append("Athletic fetched but parser returned 0 rows (paywall/format likely).")
        except Exception as exc:
            warnings.append(f"Athletic fetch failed: {exc}")

        try:
            ringer_html = _fetch(RINGER_URL)
            ringer_rows = parse_ringer(ringer_html, snapshot_date=snapshot_date)
            if not ringer_rows:
                warnings.append("Ringer fetched but parser returned 0 rows.")
        except Exception as exc:
            warnings.append(f"Ringer fetch failed: {exc}")

    if not nflmock_rows:
        manual_rows = load_nflmock_manual_csv(Path(args.manual_nflmock_csv), snapshot_date=snapshot_date)
        if manual_rows:
            nflmock_rows = manual_rows
        else:
            warnings.append("NFLMock manual CSV fallback not found/empty.")

    if not athletic_rows:
        manual_rows = load_athletic_manual_csv(Path(args.manual_athletic_csv), snapshot_date=snapshot_date)
        if manual_rows:
            athletic_rows = manual_rows
        else:
            warnings.append("Athletic manual CSV fallback not found/empty.")

    if not ringer_rows:
        manual_rows = load_ringer_manual_csv(Path(args.manual_ringer_csv), snapshot_date=snapshot_date)
        if manual_rows:
            ringer_rows = manual_rows
        else:
            warnings.append("Ringer manual CSV fallback not found/empty.")

    if not args.no_local_seeds:
        analyst_rows = load_analyst_seed_rows(ANALYST_SEED_PATH, snapshot_date=snapshot_date)
        external_rows = load_external_board_rows(EXTERNAL_BOARD_PATH, snapshot_date=snapshot_date)
        if not analyst_rows:
            warnings.append("Analyst seed rows not found/empty.")
        if not external_rows:
            warnings.append("External board rows not found/empty.")

    all_rows = _dedupe_best(tank_rows + nflmock_rows + athletic_rows + ringer_rows + analyst_rows + external_rows)
    out_path = Path(args.out)
    if not all_rows and out_path.exists():
        warnings.append("No rows pulled; existing consensus CSV preserved.")
        # keep current file contents and use them for report preview
        with out_path.open() as f:
            all_rows = list(csv.DictReader(f))
    else:
        _write_csv(out_path, all_rows)

    _write_report(
        REPORT_PATH,
        rows=all_rows,
        tank_rows=tank_rows,
        nflmock_rows=nflmock_rows,
        athletic_rows=athletic_rows,
        ringer_rows=ringer_rows,
        analyst_rows=analyst_rows,
        external_rows=external_rows,
        warnings=warnings,
    )

    print(f"Tankathon rows: {len(tank_rows)}")
    print(f"NFLMock rows: {len(nflmock_rows)}")
    print(f"Athletic rows: {len(athletic_rows)}")
    print(f"Ringer rows: {len(ringer_rows)}")
    print(f"Analyst seed rows: {len(analyst_rows)}")
    print(f"External board rows: {len(external_rows)}")
    print(f"Combined rows: {len(all_rows)}")
    print(f"Wrote: {args.out}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
