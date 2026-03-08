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

## Regular Refresh Workflow

Run the full daily update pipeline from `astro-site`:

```bash
npm run refresh:workflow
```

That workflow runs, in order:

1. consensus pull + big-board rebuild
2. ESPN depth chart pull
3. team-needs context rebuild
4. CBS transactions pull
5. team-needs transaction adjustment rebuild
6. mock rebuild
7. Astro data export
8. site build

Useful direct flags from repo root:

```bash
python3 scripts/refresh_update_workflow.py --skip-consensus-fetch
python3 scripts/refresh_update_workflow.py --skip-depth-charts-fetch
python3 scripts/refresh_update_workflow.py --skip-team-needs-context
python3 scripts/refresh_update_workflow.py --skip-transactions-fetch
python3 scripts/refresh_update_workflow.py --skip-mocks
python3 scripts/refresh_update_workflow.py --skip-site-build
```

## Cloudflare Pages

- Framework preset: `Astro`
- Build command: `npm run build`
- Build output directory: `astro-site/dist`
- Root directory: `astro-site`

Set env var in Cloudflare Pages:

- `PYTHONUNBUFFERED=1`

If Python is not available in your Pages build image, move `sync-data` to your local pipeline and commit `astro-site/src/data/*.json`.
