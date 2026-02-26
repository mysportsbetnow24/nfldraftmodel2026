from __future__ import annotations

import csv
import html
import re
from pathlib import Path
from typing import Dict, List, Tuple


DEFAULT_URL = "https://www.drafttek.com/NFL-Trade-Value-Chart.asp"
DEFAULT_RAW_HTML_PATH = Path("data/sources/external/drafttek_trade_value_chart_2026.html")
DEFAULT_FULL_ORDER_PATH = Path("data/sources/draft_order_2026_full.csv")
DEFAULT_TRADE_ROWS_PATH = Path("data/sources/draft_pick_trades_2026.csv")
DEFAULT_ROUND1_PATH = Path("data/sources/draft_order_2026_round1.csv")


_MAIN_TABLE_RE = re.compile(
    r"<table[^>]*width\s*=\s*['\"]?600['\"]?[^>]*>(.*?)</table>",
    flags=re.IGNORECASE | re.DOTALL,
)
_PICK_CELL_RE = re.compile(
    r"<td\s+class\s*=\s*[\"']TradeValueData[AB][\"']>(.*?)<FONT\s+id\s*=\s*[\"']ConsolidatedTradeColor[\"']>(.*?)</font></td>",
    flags=re.IGNORECASE | re.DOTALL,
)

_DETAILS_TABLE_RE = re.compile(
    r"<table[^>]*width\s*=\s*['\"]?620['\"]?[^>]*>(.*?)</table>",
    flags=re.IGNORECASE | re.DOTALL,
)
_DETAILS_ROW_RE = re.compile(
    r"<tr>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*</tr>",
    flags=re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_TEAM_RE = re.compile(r"\b[A-Z]{2,3}\b")


def fetch_drafttek_trade_value_html(url: str = DEFAULT_URL, timeout: int = 20) -> str:
    try:
        import requests
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("requests is required for live Drafttek pulls. Install requirements first.") from exc

    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _clean_text(raw: str) -> str:
    text = html.unescape(raw or "")
    text = _TAG_RE.sub(" ", text)
    text = _WS_RE.sub(" ", text)
    return text.strip()


def _parse_pick_number(raw: str) -> int | None:
    txt = _clean_text(raw)
    m = re.search(r"\d+", txt)
    if not m:
        return None
    return int(m.group(0))


def _extract_main_table_cells(page_html: str) -> List[Tuple[int, int, str]]:
    block = ""
    for candidate in _MAIN_TABLE_RE.findall(page_html):
        if "RD1" in candidate and "ConsolidatedTradeColor" in candidate:
            block = candidate
            break
    if not block:
        return []

    cells = _PICK_CELL_RE.findall(block)
    out: List[Tuple[int, int, str]] = []
    for idx, (pick_raw, team_raw) in enumerate(cells):
        round_no = (idx % 7) + 1
        pick_no = _parse_pick_number(pick_raw)
        team = _clean_text(team_raw).upper()
        if pick_no is None or not team:
            continue
        out.append((round_no, pick_no, team))
    return out


def _extract_trade_details(page_html: str) -> Dict[Tuple[int, int], str]:
    block = ""
    for candidate in _DETAILS_TABLE_RE.findall(page_html):
        if "Traded Pick Details" in candidate:
            block = candidate
            break
    if not block:
        return {}

    out: Dict[Tuple[int, int], str] = {}
    for round_raw, pick_raw, detail_raw in _DETAILS_ROW_RE.findall(block):
        round_txt = _clean_text(round_raw)
        pick_txt = _clean_text(pick_raw)
        detail_txt = _clean_text(detail_raw)
        if not (round_txt.isdigit() and pick_txt.isdigit()):
            continue
        out[(int(round_txt), int(pick_txt))] = detail_txt
    return out


def _parse_trade_route(detail: str) -> Tuple[str | None, str | None, List[str]]:
    if not detail:
        return None, None, []
    if "proj. compensatory pick" in detail.lower():
        return None, None, []

    head = detail.split("(", 1)[0].upper()
    teams = _TEAM_RE.findall(head)
    if len(teams) < 2:
        if teams:
            return teams[0], teams[0], teams
        return None, None, []
    return teams[0], teams[-1], teams


def parse_drafttek_order(page_html: str, source_url: str = DEFAULT_URL) -> Tuple[List[dict], List[dict], List[dict]]:
    cells = _extract_main_table_cells(page_html)
    if not cells:
        return [], [], []

    details = _extract_trade_details(page_html)

    by_round: Dict[int, List[Tuple[int, str]]] = {}
    for round_no, overall_pick, current_team in cells:
        by_round.setdefault(round_no, []).append((overall_pick, current_team))

    full_rows: List[dict] = []
    for round_no in sorted(by_round):
        round_picks = sorted(by_round[round_no], key=lambda x: x[0])
        for idx, (overall_pick, current_team) in enumerate(round_picks, start=1):
            detail = details.get((round_no, overall_pick), "")
            detail_current, original_from_detail, route_teams = _parse_trade_route(detail)
            route_team_set = set(route_teams)

            detail_matches_owner = (
                not detail_current
                or detail_current == current_team
                or (current_team in route_team_set)
            )
            original_team = current_team
            if detail_matches_owner and original_from_detail:
                original_team = original_from_detail.upper()

            full_rows.append(
                {
                    "round": round_no,
                    "pick_in_round": idx,
                    "overall_pick": overall_pick,
                    "current_team": current_team.upper(),
                    "original_team": original_team,
                    "acquired_via": detail,
                    "source_url": source_url,
                }
            )

    trade_rows: List[dict] = []
    for row in full_rows:
        note = (row.get("acquired_via") or "").strip()
        if not note:
            continue
        if "proj. compensatory pick" in note.lower():
            continue
        original = (row.get("original_team") or "").strip()
        current = (row.get("current_team") or "").strip()
        if not original or not current:
            continue
        if original == current:
            continue
        trade_rows.append(
            {
                "round": row["round"],
                "pick_in_round": row["pick_in_round"],
                "overall_pick": row["overall_pick"],
                "original_team": original,
                "current_team": current,
                "trade_note": note,
                "source_url": row["source_url"],
            }
        )

    round1_rows = [
        {"pick": row["pick_in_round"], "team": row["current_team"]}
        for row in full_rows
        if int(row["round"]) == 1
    ]
    return full_rows, trade_rows, round1_rows


def write_csv(rows: List[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
