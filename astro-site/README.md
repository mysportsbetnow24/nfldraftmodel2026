# Scouting Grade Astro Site

Astro starter for `scoutinggrade.com` that reads model outputs from this repo.

## Local Run

1. `cd astro-site`
2. `npm install`
3. `npm run dev`

`predev` runs `python3 ../scripts/export_astro_site_data.py` automatically to refresh JSON from:

- `data/outputs/big_board_2026.csv`
- `data/outputs/mock_2026_round1.csv`
- `data/outputs/mock_2026_7round.csv`

## Build

1. `cd astro-site`
2. `npm run build`

Output goes to `astro-site/dist`.

## Cloudflare Pages

- Framework preset: `Astro`
- Build command: `npm run build`
- Build output directory: `astro-site/dist`
- Root directory: `astro-site`

Set env var in Cloudflare Pages:

- `PYTHONUNBUFFERED=1`

If Python is not available in your Pages build image, move `sync-data` to your local pipeline and commit `astro-site/src/data/*.json`.
