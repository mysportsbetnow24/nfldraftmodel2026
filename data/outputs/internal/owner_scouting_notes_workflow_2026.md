# Owner Scouting Notes Workflow 2026

This is the private editing path for public scouting-card copy.

## Source of truth

- Public owner overrides live in [data/sources/manual/owner_scouting_notes_2026.csv](/Users/nickholz/NFL%20Draft%20Model%201/data/sources/manual/owner_scouting_notes_2026.csv)
- A starter template lives in [data/sources/manual/owner_scouting_notes_2026_template.csv](/Users/nickholz/NFL%20Draft%20Model%201/data/sources/manual/owner_scouting_notes_2026_template.csv)

## Supported public override fields

- `public_report_summary`
- `public_why_he_wins`
- `public_primary_concerns`
- `public_film_notes`
- `public_role_projection`
- `seo_description`

These override the generated public player-page copy when a matching `slug` is present.

## Private-only field

- `private_owner_notes`

This is stored in the CSV for owner workflow only. It is not exported to the public site.

## Matching key

- Use `slug` as the primary key.
- Keep `player_name` filled for human QA, but `slug` is what the exporter matches on.

## Publish workflow

1. Add or update rows in `owner_scouting_notes_2026.csv`.
2. Rebuild the board if model-driven scouting text changed:
   - `ALLOW_SINGLE_YEAR_PRODUCTION_KNN=1 PYTHONPATH=. python3 scripts/build_big_board.py`
3. Export site data:
   - `PYTHONPATH=. python3 scripts/export_astro_site_data.py`
4. Build Astro:
   - `cd astro-site && npm run build`
5. Commit only the note CSV and generated site data if the change is owner-note-only.
6. Push to `main` for Cloudflare deployment.

## Guardrails

- Do not put private notes, sourcing notes, or methodology notes into the public override columns.
- Keep SEO descriptions concise and player-specific.
- Keep `Primary Concerns` player-specific; do not reintroduce generic template warnings.

## Current architecture limit

This site is static. There is no secure live browser editor yet.

If true on-site editing is needed later, the correct next step is:
- Cloudflare Access + private admin route
- server-side note storage
- authenticated write workflow

Do not add browser-side editable public fields without backend auth.
