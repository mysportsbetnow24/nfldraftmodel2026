from __future__ import annotations

import csv
import html
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BOARD_PATH = ROOT / "data" / "processed" / "big_board_2026.csv"
OUT_DIR = ROOT / "data" / "outputs" / "player_reports_html"


def _player_card(row: dict) -> str:
    name = html.escape(row["player_name"])
    school = html.escape(row["school"])
    pos = html.escape(row["position"])
    note = html.escape(row["scouting_notes"])
    team_fit = html.escape(row["best_team_fit"])
    comp = html.escape(row["historical_comp"])
    ras_score = row.get("ras_estimate", "")
    ras_tier = html.escape(row.get("ras_tier", ""))
    ras_pct = row.get("ras_percentile", "")
    ras_comp_1 = html.escape(row.get("ras_historical_comp_1", ""))
    ras_comp_2 = html.escape(row.get("ras_historical_comp_2", ""))
    headshot = html.escape(row.get("headshot_url", ""))

    img_tag = f'<img src="{headshot}" alt="{name}" class="headshot" />' if headshot else '<div class="headshot placeholder">No headshot yet</div>'

    return f"""
<article class=\"report\">
  {img_tag}
  <h1>{name}</h1>
  <p class=\"meta\">{pos} | {school} | Grade {row['final_grade']} ({row['round_value']})</p>
  <p><strong>Best Team Fit:</strong> {team_fit}</p>
  <p><strong>Historical Comp:</strong> {comp}</p>
  <p><strong>RAS (Estimated):</strong> {ras_score}/10 ({ras_tier}, {ras_pct} percentile)</p>
  <p><strong>RAS Historical Bucket Comps:</strong> {ras_comp_1} | {ras_comp_2}</p>
  <p><strong>Core Stat:</strong> {html.escape(row['core_stat_name'])} = {row['core_stat_value']}</p>
  <p>{note}</p>
</article>
""".strip()


def render_reports() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with BOARD_PATH.open() as f:
        rows = list(csv.DictReader(f))

    css = """
body { font-family: 'Georgia', serif; margin: 0; padding: 2rem; background: linear-gradient(120deg,#f6f4ec,#e2ecf5); color: #111; }
.report { max-width: 720px; margin: 0 auto 2rem; background: #fff; border: 1px solid #d7d7d7; border-radius: 12px; padding: 1.25rem; box-shadow: 0 6px 18px rgba(0,0,0,0.08); }
.meta { color: #444; font-size: 0.95rem; }
.headshot { width: 128px; height: 128px; object-fit: cover; border-radius: 10px; background: #eee; }
.placeholder { display: flex; align-items: center; justify-content: center; color: #666; border: 1px dashed #999; }
""".strip()

    index_cards = []
    for row in rows[:120]:
        player_html = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>{html.escape(row['player_name'])} - 2026 Report</title>
  <style>{css}</style>
</head>
<body>
{_player_card(row)}
</body>
</html>
""".strip()
        slug = row["player_name"].lower().replace(" ", "-").replace("'", "")
        out_path = OUT_DIR / f"{slug}.html"
        out_path.write_text(player_html)
        index_cards.append(f"<li><a href=\"player_reports_html/{out_path.name}\">{html.escape(row['consensus_rank'])}. {html.escape(row['player_name'])} ({html.escape(row['position'])})</a></li>")

    index_html = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>2026 Big Board Reports</title>
  <style>{css}\nul {{line-height: 1.7;}}</style>
</head>
<body>
  <section class=\"report\">
    <h1>2026 NFL Draft Big Board Reports</h1>
    <ul>
      {''.join(index_cards)}
    </ul>
  </section>
</body>
</html>
""".strip()
    (ROOT / "data" / "outputs" / "reports_index.html").write_text(index_html)


if __name__ == "__main__":
    render_reports()
