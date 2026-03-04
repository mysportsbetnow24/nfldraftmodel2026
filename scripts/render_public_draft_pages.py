#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import html
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "data" / "outputs"

BOARD_CSV = OUTPUTS / "big_board_2026.csv"
ROUND1_CSV = OUTPUTS / "mock_2026_round1.csv"
ROUND7_CSV = OUTPUTS / "mock_2026_7round.csv"

TARGET_DIRS = [ROOT / "docs", OUTPUTS / "site"]

BIG_BOARD_SLUG = "2026-nfl-draft-big-board.html"
ROUND1_SLUG = "2026-nfl-mock-draft-round-1.html"
ROUND7_SLUG = "2026-nfl-7-round-mock-draft.html"
LEGACY_ROUND7 = "data_mock_7round.html"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _to_int(value: str | None, default: int = 0) -> int:
    try:
        return int(float(str(value or "").strip()))
    except (TypeError, ValueError):
        return default


def _to_float(value: str | None, default: float = 0.0) -> float:
    try:
        return float(str(value or "").strip())
    except (TypeError, ValueError):
        return default


def _esc(value: str | None) -> str:
    return html.escape(str(value or ""))


def _build_big_board_rows(rows: list[dict[str, str]]) -> str:
    def key(row: dict[str, str]) -> tuple[float, float, str]:
        rank_sort = _to_float(row.get("rank_sort_score"), 0.0)
        grade = _to_float(row.get("final_grade"), 0.0)
        return (rank_sort, grade, row.get("player_name", ""))

    ordered = sorted(rows, key=key, reverse=True)

    out = []
    for idx, row in enumerate(ordered, start=1):
        name = _esc(row.get("player_name"))
        pos = _esc(row.get("position"))
        school = _esc(row.get("school"))
        grade = _esc(row.get("final_grade"))
        round_value = _esc(row.get("round_value"))
        consensus_mean = _esc(row.get("consensus_board_mean_rank"))
        consensus_std = _esc(row.get("consensus_board_rank_std"))
        search_blob = _esc(f"{row.get('player_name','')} {row.get('position','')} {row.get('school','')}".lower())
        out.append(
            f"<tr data-search='{search_blob}'><td>{idx}</td><td>{name}</td><td>{pos}</td><td>{school}</td>"
            f"<td>{grade}</td><td>{round_value}</td><td>{consensus_mean}</td><td>{consensus_std}</td></tr>"
        )

    return "".join(out)


def _build_round1_rows(rows: list[dict[str, str]]) -> str:
    ordered = sorted(rows, key=lambda r: _to_int(r.get("overall_pick"), 999))
    out = []
    for row in ordered:
        out.append(
            "<tr>"
            f"<td>{_esc(row.get('overall_pick'))}</td>"
            f"<td>{_esc(row.get('team'))}</td>"
            f"<td>{_esc(row.get('player_name'))}</td>"
            f"<td>{_esc(row.get('position'))}</td>"
            f"<td>{_esc(row.get('school'))}</td>"
            f"<td>{_esc(row.get('final_grade'))}</td>"
            f"<td>{_esc(row.get('round_value'))}</td>"
            "</tr>"
        )
    return "".join(out)


def _build_round7_rows(rows: list[dict[str, str]]) -> str:
    ordered = sorted(rows, key=lambda r: (_to_int(r.get("round"), 99), _to_int(r.get("pick"), 99)))
    out = []
    for row in ordered:
        team = row.get("team", "")
        out.append(
            "<tr "
            f"data-team='{_esc(team)}' "
            f"data-round='{_esc(row.get('round'))}' "
            f"data-pick='{_esc(row.get('pick'))}' "
            f"data-overall='{_esc(row.get('overall_pick'))}' "
            f"data-player='{_esc(row.get('player_name'))}' "
            f"data-pos='{_esc(row.get('position'))}' "
            f"data-school='{_esc(row.get('school'))}'"
            ">"
            f"<td>{_esc(row.get('round'))}</td>"
            f"<td>{_esc(row.get('pick'))}</td>"
            f"<td>{_esc(row.get('overall_pick'))}</td>"
            f"<td>{_esc(team)}</td>"
            f"<td>{_esc(row.get('player_name'))}</td>"
            f"<td>{_esc(row.get('position'))}</td>"
            f"<td>{_esc(row.get('school'))}</td>"
            f"<td>{_esc(row.get('final_grade'))}</td>"
            f"<td>{_esc(row.get('round_value'))}</td>"
            "</tr>"
        )
    return "".join(out)


def _shell_styles() -> str:
    return """
    :root {
      --bg: #f1f5fb;
      --paper: #ffffff;
      --ink: #0d1117;
      --line: #d6dde8;
      --muted: #596377;
      --accent: #0d4f6b;
      --accent-2: #1f4e2f;
      --shadow: 0 24px 48px rgba(7, 17, 35, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 1rem;
      font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 6% -4%, rgba(13,79,107,0.14), transparent 30%),
        radial-gradient(circle at 98% 0%, rgba(31,78,47,0.16), transparent 26%),
        repeating-linear-gradient(135deg, rgba(16,96,56,0.05) 0, rgba(16,96,56,0.05) 8px, rgba(255,255,255,0) 8px, rgba(255,255,255,0) 18px),
        linear-gradient(180deg, #f4f7fb 0%, #edf4ee 100%);
    }
    .card {
      max-width: 1220px;
      margin: 0 auto;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 1.1rem;
      box-shadow: var(--shadow);
    }
    h1 { margin: 0 0 0.42rem; font-size: 2.2rem; letter-spacing: -0.02em; line-height: 1.05; }
    p { color: var(--muted); line-height: 1.6; max-width: 95ch; }
    .controls { display: flex; gap: 0.6rem; align-items: center; flex-wrap: wrap; margin: 0.8rem 0; }
    .controls input, .controls select {
      border: 1px solid #c7d1df;
      border-radius: 11px;
      padding: 0.55rem 0.66rem;
      font-size: 0.92rem;
      min-width: 220px;
      background: #fbfdff;
    }
    .btn {
      display: inline-block;
      padding: 0.5rem 0.82rem;
      border: 1px solid #c6d1de;
      border-radius: 11px;
      text-decoration: none;
      color: var(--ink);
      background: #f8fafb;
      font-weight: 650;
    }
    .hint { font-size: 0.9rem; color: var(--muted); margin: 0.35rem 0; }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 0.88rem; }
    th, td { border: 1px solid #d5dde8; padding: 0.35rem 0.42rem; text-align: left; vertical-align: top; word-break: break-word; }
    th { background: #edf3f9; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; color: #16344a; }
    #teamSummary { margin-top: 1rem; border: 1px solid var(--line); border-radius: 14px; padding: 0.8rem; background: #fafcfd; }
    #teamSummary h2 { margin: 0 0 0.55rem; font-size: 1rem; }
    #teamSummary ul { margin: 0; padding-left: 1.15rem; line-height: 1.45; }
    @media (max-width: 860px) {
      body { padding: 0.55rem; }
      th, td { font-size: 0.8rem; }
      .controls input, .controls select { min-width: 100%; }
    }
    """


def _big_board_page(rows_html: str, total: int, built_at: str) -> str:
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>2026 NFL Draft Big Board | Scouting Grade</title>
  <meta name=\"description\" content=\"2026 NFL Draft Big Board with model rankings, grades, and consensus context from Scouting Grade.\" />
  <link rel=\"canonical\" href=\"https://www.scoutinggrade.com/{BIG_BOARD_SLUG}\" />
  <style>{_shell_styles()}</style>
</head>
<body>
  <main class=\"card\">
    <h1>2026 NFL Draft Big Board</h1>
    <p>Model-first ranking board built from scouting, athletic, production, and consensus context. This page is public read-only.</p>
    <p class=\"hint\">Rows: {total} | Last update: {built_at}</p>
    <div class=\"controls\">
      <a class=\"btn\" href=\"index.html\">Back To Hub</a>
      <input id=\"boardSearch\" placeholder=\"Search player, school, or position...\" />
    </div>
    <table>
      <thead>
        <tr>
          <th>Model Rank</th><th>Player</th><th>Pos</th><th>School</th><th>Grade</th><th>Projection</th><th>Consensus Mean</th><th>Consensus Std</th>
        </tr>
      </thead>
      <tbody id=\"boardBody\">{rows_html}</tbody>
    </table>
  </main>
  <script>
    (function () {{
      const input = document.getElementById('boardSearch');
      const rows = Array.from(document.querySelectorAll('#boardBody tr'));
      if (!input || !rows.length) return;
      input.addEventListener('input', () => {{
        const q = input.value.trim().toLowerCase();
        rows.forEach((tr) => {{
          const blob = (tr.getAttribute('data-search') || '');
          tr.style.display = blob.includes(q) ? '' : 'none';
        }});
      }});
    }})();
  </script>
</body>
</html>
"""


def _round1_page(rows_html: str, total: int, built_at: str) -> str:
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>2026 NFL Mock Draft Round 1 | Scouting Grade</title>
  <meta name=\"description\" content=\"2026 NFL Mock Draft Round 1 with projected picks, player grades, and round projections from Scouting Grade.\" />
  <link rel=\"canonical\" href=\"https://www.scoutinggrade.com/{ROUND1_SLUG}\" />
  <style>{_shell_styles()}</style>
</head>
<body>
  <main class=\"card\">
    <h1>2026 NFL Mock Draft (Round 1)</h1>
    <p>Latest first-round projection from the Scouting Grade model. This is the current public snapshot and updates with each model cycle.</p>
    <p class=\"hint\">Picks: {total} | Last update: {built_at}</p>
    <div class=\"controls\"><a class=\"btn\" href=\"index.html\">Back To Hub</a></div>
    <table>
      <thead>
        <tr>
          <th>Pick</th><th>Team</th><th>Player</th><th>Pos</th><th>School</th><th>Grade</th><th>Projection</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </main>
</body>
</html>
"""


def _round7_page(rows_html: str, total: int, built_at: str) -> str:
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>2026 7-Round NFL Mock Draft | Scouting Grade</title>
  <meta name=\"description\" content=\"2026 7-round NFL mock draft with team-by-team filtering and full draft class view from Scouting Grade.\" />
  <link rel=\"canonical\" href=\"https://www.scoutinggrade.com/{ROUND7_SLUG}\" />
  <style>{_shell_styles()}</style>
</head>
<body>
  <main class=\"card\">
    <h1>2026 7-Round NFL Mock Draft</h1>
    <p>Full seven-round projection. Use the team filter to review one franchise's complete draft class in one view.</p>
    <p class=\"hint\">Picks: {total} | Last update: {built_at}</p>
    <div class=\"controls\">
      <a class=\"btn\" href=\"index.html\">Back To Hub</a>
      <label for=\"teamFilter\">Team:</label>
      <select id=\"teamFilter\"><option value=\"\">All Teams</option></select>
      <span id=\"rowCount\" class=\"hint\"></span>
    </div>

    <table>
      <thead>
        <tr>
          <th>Round</th><th>RND Pick</th><th>Overall</th><th>Team</th><th>Player</th><th>Pos</th><th>School</th><th>Grade</th><th>Projection</th>
        </tr>
      </thead>
      <tbody id=\"round7Body\">{rows_html}</tbody>
    </table>

    <section id=\"teamSummary\">
      <h2>Team Draft Class View</h2>
      <div id=\"teamSummaryText\" class=\"hint\">Select a team above to view its complete mock draft class.</div>
      <ul id=\"teamSummaryList\"></ul>
    </section>
  </main>

  <script>
    (function () {{
      const select = document.getElementById('teamFilter');
      const rows = Array.from(document.querySelectorAll('#round7Body tr'));
      const rowCount = document.getElementById('rowCount');
      const summaryText = document.getElementById('teamSummaryText');
      const summaryList = document.getElementById('teamSummaryList');
      if (!select || !rows.length) return;

      const teams = Array.from(new Set(rows.map((r) => r.getAttribute('data-team') || '').filter(Boolean))).sort();
      teams.forEach((team) => {{
        const opt = document.createElement('option');
        opt.value = team;
        opt.textContent = team;
        select.appendChild(opt);
      }});

      function applyFilter() {{
        const team = select.value;
        let visible = 0;
        const picks = [];
        rows.forEach((row) => {{
          const rowTeam = row.getAttribute('data-team') || '';
          const show = !team || rowTeam === team;
          row.style.display = show ? '' : 'none';
          if (show) {{
            visible += 1;
            if (team) {{
              picks.push({{
                round: row.getAttribute('data-round') || '',
                pick: row.getAttribute('data-pick') || '',
                overall: row.getAttribute('data-overall') || '',
                player: row.getAttribute('data-player') || '',
                pos: row.getAttribute('data-pos') || '',
                school: row.getAttribute('data-school') || ''
              }});
            }}
          }}
        }});
        rowCount.textContent = team ? `${{team}} picks shown: ${{visible}}` : `All picks shown: ${{visible}}`;

        summaryList.innerHTML = '';
        if (!team) {{
          summaryText.textContent = 'Select a team above to view its complete mock draft class.';
          return;
        }}

        if (!picks.length) {{
          summaryText.textContent = `${{team}} has no picks in this current mock output.`;
          return;
        }}

        summaryText.textContent = `${{team}} full draft class (${{picks.length}} picks):`;
        picks.forEach((p) => {{
          const li = document.createElement('li');
          li.textContent = 'Round ' + p.round + ' Pick ' + p.pick + ' (Overall ' + p.overall + '): ' + p.player + ' (' + p.pos + ') - ' + p.school;
          summaryList.appendChild(li);
        }});
      }}

      select.addEventListener('change', applyFilter);
      applyFilter();
    }})();
  </script>
</body>
</html>
"""


def _legacy_redirect() -> str:
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta http-equiv=\"refresh\" content=\"0; url=/{ROUND7_SLUG}\" />
  <meta name=\"robots\" content=\"noindex\" />
  <title>Redirecting...</title>
  <link rel=\"canonical\" href=\"https://www.scoutinggrade.com/{ROUND7_SLUG}\" />
</head>
<body>
  <p>Redirecting to <a href=\"/{ROUND7_SLUG}\">2026 7-Round NFL Mock Draft</a>...</p>
</body>
</html>
"""


def render() -> None:
    if not BOARD_CSV.exists() or not ROUND1_CSV.exists() or not ROUND7_CSV.exists():
        raise FileNotFoundError("One or more required CSV files are missing in data/outputs.")

    board_rows = _read_csv(BOARD_CSV)
    round1_rows = _read_csv(ROUND1_CSV)
    round7_rows = _read_csv(ROUND7_CSV)
    built_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    big_board_html = _big_board_page(_build_big_board_rows(board_rows), len(board_rows), built_at)
    round1_html = _round1_page(_build_round1_rows(round1_rows), len(round1_rows), built_at)
    round7_html = _round7_page(_build_round7_rows(round7_rows), len(round7_rows), built_at)
    redirect_html = _legacy_redirect()

    for dest in TARGET_DIRS:
        dest.mkdir(parents=True, exist_ok=True)
        (dest / BIG_BOARD_SLUG).write_text(big_board_html, encoding="utf-8")
        (dest / ROUND1_SLUG).write_text(round1_html, encoding="utf-8")
        (dest / ROUND7_SLUG).write_text(round7_html, encoding="utf-8")
        (dest / LEGACY_ROUND7).write_text(redirect_html, encoding="utf-8")

    print("Rendered public data pages:")
    for dest in TARGET_DIRS:
        print(f"- {dest / BIG_BOARD_SLUG}")
        print(f"- {dest / ROUND1_SLUG}")
        print(f"- {dest / ROUND7_SLUG}")
        print(f"- {dest / LEGACY_ROUND7} (redirect)")


if __name__ == "__main__":
    render()
