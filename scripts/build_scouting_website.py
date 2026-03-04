#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "data" / "outputs"
SITE_DIR = OUTPUTS / "site"

BOARD_PATH = OUTPUTS / "big_board_2026.csv"
RANK_VS_CONSENSUS_PATH = OUTPUTS / "big_board_2026_rank_vs_consensus.csv"
ROUND1_PATH = OUTPUTS / "mock_2026_round1.csv"
ROUND7_PATH = OUTPUTS / "mock_2026_7round.csv"
REPORTS_INDEX_PATH = OUTPUTS / "reports_index.html"
CARD_TEMPLATE_PATH = OUTPUTS / "scouting_card_template.html"
PLAYER_REPORTS_DIR = OUTPUTS / "player_reports_html"


def _to_int(value: str | None, default: int = 999999) -> int:
    try:
        return int(float(str(value or "").strip()))
    except (TypeError, ValueError):
        return default


def _to_float(value: str | None, default: float = 0.0) -> float:
    try:
        return float(str(value or "").strip())
    except (TypeError, ValueError):
        return default


def _require(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _load_top_board_rows(path: Path, max_rows: int) -> list[dict]:
    with path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    rows.sort(
        key=lambda r: (
            _to_int(r.get("consensus_rank")),
            -_to_float(r.get("final_grade")),
            r.get("player_name", ""),
        )
    )
    return rows[:max_rows]


def _build_home_html(top_rows: list[dict]) -> str:
    lines = []
    for row in top_rows:
        rank = _to_int(row.get("consensus_rank"), 999999)
        name = html.escape(str(row.get("player_name", "")))
        pos = html.escape(str(row.get("position", "")))
        school = html.escape(str(row.get("school", "")))
        grade = html.escape(str(row.get("final_grade", "")))
        round_value = html.escape(str(row.get("round_value", "")))
        consensus_mean = html.escape(str(row.get("consensus_board_mean_rank", "")))
        search_blob = html.escape(f"{name} {pos} {school}".lower())
        lines.append(
            "<tr data-search='{}'>"
            "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>"
            "</tr>".format(
                search_blob,
                rank,
                name,
                pos,
                school,
                grade,
                round_value,
                consensus_mean,
            )
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>NF Draft Hub 2026</title>
  <style>
    :root {{
      --bg: #f1f5fb;
      --paper: #ffffff;
      --ink: #0d1117;
      --line: #d6dde8;
      --accent: #0d4f6b;
      --muted: #596377;
      --shadow: 0 24px 48px rgba(7, 17, 35, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 6% -4%, rgba(13,79,107,0.14), transparent 30%),
        radial-gradient(circle at 98% 0%, rgba(31,78,47,0.16), transparent 26%),
        repeating-linear-gradient(135deg, rgba(16,96,56,0.05) 0, rgba(16,96,56,0.05) 8px, rgba(255,255,255,0) 8px, rgba(255,255,255,0) 18px),
        linear-gradient(180deg, #f4f7fb 0%, #edf4ee 100%);
      padding: 1.1rem;
    }}
    .shell {{
      max-width: 1240px;
      margin: 0 auto;
      border: 1px solid var(--line);
      border-radius: 20px;
      overflow: hidden;
      background: var(--paper);
      box-shadow: var(--shadow);
    }}
    .hero {{
      border-bottom: 1px solid var(--line);
      padding: 1.1rem 1.25rem;
      background:
        radial-gradient(circle at 0% 0%, rgba(10,79,107,0.18), transparent 30%),
        linear-gradient(100deg, #f5f9fd, #f4f8f2);
    }}
    .hero h1 {{
      margin: 0;
      letter-spacing: -0.02em;
      font-size: 2.2rem;
      line-height: 1.05;
    }}
    .hero p {{ margin: 0.45rem 0 0; color: var(--muted); }}
    .links {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin-top: 0.75rem;
    }}
    .links a {{
      text-decoration: none;
      border: 1px solid #c6d1de;
      border-radius: 11px;
      background: #f8fafb;
      color: var(--ink);
      padding: 0.5rem 0.75rem;
      font-weight: 650;
      font-size: 0.92rem;
    }}
    .links a.primary {{
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 0.75rem;
      padding: 0.95rem 1rem 1rem;
    }}
    .panel {{
      border: 1px solid #d5dde8;
      border-radius: 14px;
      padding: 0.8rem;
      background: #fff;
    }}
    .panel h2 {{
      margin: 0 0 0.5rem;
      font-size: 1.1rem;
      letter-spacing: 0.01em;
    }}
    .hint {{
      margin: 0 0 0.5rem;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    #search {{
      width: 100%;
      border: 1px solid #c7d1df;
      border-radius: 11px;
      padding: 0.6rem;
      font-size: 0.94rem;
      margin: 0 0 0.6rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 0.88rem;
    }}
    th, td {{
      border: 1px solid #d5dde8;
      padding: 0.35rem 0.4rem;
      text-align: left;
      vertical-align: top;
      word-wrap: break-word;
    }}
    th {{
      background: #edf3f9;
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.03em;
      color: #16344a;
    }}
    @media (max-width: 780px) {{
      body {{ padding: 0.55rem; }}
      .hero h1 {{ font-size: 1.55rem; }}
      th, td {{ font-size: 0.8rem; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <h1>NFL Draft Hub 2026</h1>
      <p>Model board, mock drafts, and scouting cards in a clean, fast public format.</p>
      <div class="links">
        <a class="primary" href="reports_index.html">Open Scouting Cards</a>
        <a href="2026-nfl-draft-big-board.html">Big Board Page</a>
        <a href="data_rank_vs_consensus.html">Rank vs Consensus Page</a>
        <a href="2026-nfl-mock-draft-round-1.html">Round 1 Mock Page</a>
        <a href="2026-nfl-7-round-mock-draft.html">7-Round Mock Page</a>
      </div>
    </section>

    <section class="grid">
      <article class="panel">
        <h2>Top Board Snapshot</h2>
        <input id="search" placeholder="Search player, school, or position..." />
        <table>
          <thead>
            <tr>
              <th>Rank</th>
              <th>Player</th>
              <th>Pos</th>
              <th>School</th>
              <th>Grade</th>
              <th>Round</th>
              <th>Consensus Mean</th>
            </tr>
          </thead>
          <tbody id="boardBody">
            {''.join(lines)}
          </tbody>
        </table>
      </article>
    </section>
  </main>

  <script>
    (function () {{
      const input = document.getElementById('search');
      const body = document.getElementById('boardBody');
      if (!input || !body) return;
      input.addEventListener('input', () => {{
        const q = input.value.trim().toLowerCase();
        body.querySelectorAll('tr').forEach((tr) => {{
          const hay = (tr.getAttribute('data-search') || '');
          tr.style.display = hay.includes(q) ? '' : 'none';
        }});
      }});
    }})();
  </script>
</body>
</html>
"""


def _deploy_readme(site_dir: Path) -> None:
    txt = """Scouting Website Bundle
======================

This folder is deploy-ready static content.

Recommended host:
1) Cloudflare Pages (fastest)
2) Netlify
3) GitHub Pages

Deploy:
- Set publish directory to this folder.
- No build command needed.

Update cycle:
1) python3 scripts/build_big_board.py
2) python3 scripts/run_mock_draft.py
3) python3 scripts/generate_player_reports.py
4) python3 scripts/build_scouting_website.py
"""
    (site_dir / "DEPLOY_README.txt").write_text(txt, encoding="utf-8")


def build_site(max_rows: int) -> None:
    _require(BOARD_PATH)
    _require(ROUND1_PATH)
    _require(ROUND7_PATH)
    _require(REPORTS_INDEX_PATH)
    _require(CARD_TEMPLATE_PATH)
    _require(PLAYER_REPORTS_DIR)

    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_DIR.mkdir(parents=True, exist_ok=True)

    _copy_file(REPORTS_INDEX_PATH, SITE_DIR / "reports_index.html")
    _copy_file(CARD_TEMPLATE_PATH, SITE_DIR / "scouting_card_template.html")
    _copy_file(BOARD_PATH, SITE_DIR / "big_board_2026.csv")
    _copy_file(ROUND1_PATH, SITE_DIR / "mock_2026_round1.csv")
    _copy_file(ROUND7_PATH, SITE_DIR / "mock_2026_7round.csv")
    if RANK_VS_CONSENSUS_PATH.exists():
        _copy_file(RANK_VS_CONSENSUS_PATH, SITE_DIR / "big_board_2026_rank_vs_consensus.csv")
    _copy_tree(PLAYER_REPORTS_DIR, SITE_DIR / "player_reports_html")

    top_rows = _load_top_board_rows(BOARD_PATH, max_rows=max_rows)
    (SITE_DIR / "index.html").write_text(_build_home_html(top_rows), encoding="utf-8")
    _deploy_readme(SITE_DIR)

    print(f"Wrote website bundle: {SITE_DIR}")
    print(f"Homepage: {SITE_DIR / 'index.html'}")
    print(f"Reports index: {SITE_DIR / 'reports_index.html'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deployable static scouting website bundle.")
    parser.add_argument("--max-rows", type=int, default=120, help="Top board rows to render on homepage.")
    args = parser.parse_args()
    build_site(max_rows=max(20, min(400, args.max_rows)))


if __name__ == "__main__":
    main()
