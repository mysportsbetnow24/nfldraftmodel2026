Scouting Website Bundle
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
