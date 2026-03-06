# SEO Route Checklist (2026-03-05)

## Scope
- `https://scoutinggrade.com/2026-nfl-draft-big-board`
- `https://scoutinggrade.com/2026-nfl-mock-draft-round-1`
- `https://scoutinggrade.com/2026-nfl-7-round-mock-draft`
- `https://scoutinggrade.com/nfl-team-needs-2026`
- `https://scoutinggrade.com/players/<slug>`

## Global SEO Baseline
- [x] Canonical tag enabled from `BaseLayout`.
- [x] Open Graph (`og:title`, `og:description`, `og:url`, `og:image`) on every page.
- [x] Twitter card tags on every page.
- [x] `robots` meta set to `index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1`.
- [x] Optional route keywords support added.
- [x] Route-level extra schema support added.

## Route-by-Route Implementation

## 1) Big Board
Path: `/2026-nfl-draft-big-board`
- [x] Title tuned for intent: rankings + big board.
- [x] Description tuned for SERP clarity.
- [x] `CollectionPage` schema present.
- [x] `ItemList` schema added for top 100 ranked prospects.

## 2) Round 1 Mock
Path: `/2026-nfl-mock-draft-round-1`
- [x] Title tuned for "Round 1 picks" query intent.
- [x] Description tuned for pick-by-pick summary.
- [x] `CollectionPage` schema present.
- [x] `ItemList` schema added for 32 first-round picks.

## 3) 7-Round Mock
Path: `/2026-nfl-7-round-mock-draft`
- [x] Title tuned for full-class query intent.
- [x] Description tuned for full-order + team-filter intent.
- [x] `CollectionPage` schema present.
- [x] `ItemList` schema added for full pick list.

## 4) Team Needs
Path: `/nfl-team-needs-2026`
- [x] Title tuned for "by team" query intent.
- [x] Description tuned for weakness tiers + free-agency context.
- [x] `CollectionPage` schema present.
- [x] `ItemList` schema added for team top-need outputs.

## 5) Player Pages
Path: `/players/<slug>`
- [x] Title tuned for player + scouting report intent.
- [x] Description includes player, position, grade/projection context.
- [x] `ProfilePage` schema retained.
- [x] `SportsPerson` `mainEntity` added with player properties.
- [x] `BreadcrumbList` schema added.

## Refresh Checklist (after each data rebuild)
1. Run `python3 scripts/export_astro_site_data.py`.
2. Run `cd astro-site && npm run build`.
3. Spot-check head tags on the five routes above.
4. Validate one player page has:
   - `ProfilePage`
   - `SportsPerson`
   - `BreadcrumbList`
5. Re-submit sitemap only if route structure changes.

