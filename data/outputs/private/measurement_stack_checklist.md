# Scouting Grade Private Measurement Checklist

Use this internally. Do not publish this page to the website.

## 1) Bing Webmaster Tools
- Add property: `https://scoutinggrade.com/`
- Verify ownership using DNS TXT record.
- Submit sitemap: `https://scoutinggrade.com/sitemap.xml`
- Monitor index coverage weekly.

## 2) Cloudflare Web Analytics (Free)
- Cloudflare Dashboard -> Analytics & Logs -> Web Analytics.
- Enable on `scoutinggrade.com`.
- Confirm pageview + top paths are populating.
- Keep this as baseline traffic source (lightweight, no extra JS bloat).

## 3) Ahrefs or Semrush
- Track target keywords:
  - `2026 nfl draft big board`
  - `2026 nfl mock draft`
  - `2026 7-round mock draft`
  - `nfl team needs 2026`
  - `nfl player comparison`
- Track weekly rank movement and backlink growth.
- Track competitor overlap and gap terms.

## 4) Screaming Frog Crawl Audit
- Crawl root domain weekly.
- Fix:
  - Broken links
  - Missing titles/descriptions
  - Duplicate canonicals
  - Orphan pages
- Validate sitemap URLs against crawled URLs.

## 5) PostHog or Plausible (Behavior)
- If minimal setup is priority: Plausible.
- If funnel + event depth is priority: PostHog.
- Initial events:
  - `open_big_board`
  - `apply_position_filter`
  - `open_player_page`
  - `compare_player_select`
  - `open_mock_round1`
  - `open_mock_7round`
- Review weekly:
  - Top engaged pages
  - Filter usage rate
  - Player-page CTR from big board

## 6) Public Trust + Freshness Check
- Ensure `Last updated (UTC)` appears in footer after each pipeline run.
- Ensure weekly updates page has current risers/fallers.
- Ensure QA flags render for low-evidence profiles.

## 7) Performance Guardrails
- Keep JS page-scoped only (no global heavy bundles).
- Keep third-party scripts minimal; only install if measurable ROI.
- Use SVG or WebP for social/preview images.
- Re-run Lighthouse after major UI updates.

## 8) Weekly Operating Cadence
- Run data pipeline.
- Export Astro JSON.
- Deploy.
- Verify:
  - `/sitemap.xml`
  - `/robots.txt`
  - Big board filters
  - Player compare interactions
- Log notable changes in weekly updates.
