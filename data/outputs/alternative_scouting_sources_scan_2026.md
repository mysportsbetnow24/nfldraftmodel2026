# Alternative 2026 Scouting Sources Scan

Generated: 2026-02-27

## Tier 1 (Use Next)

1. **CBS Sports 2026 Big Board**
- URL: https://www.cbssports.com/nfl/draft/news/2026-nfl-draft-top-prospects-big-board-rankings/
- Why: National desk, stable updates, includes ranking + rationale blurbs.
- Parseability: High.
- Model use: rank anchor + text traits + risk language.
- Suggested blend: rank 8-12%, text trait 4-6%, risk penalty only.

2. **NFL.com (Bucky Brooks Top 5 by Position)**
- URL: https://www.nfl.com/news/bucky-brooks-top-five-2026-nfl-draft-prospects-by-position-1-0-jordyn-tyson-leads-wr-rankings
- Why: Official league analyst, good positional context.
- Parseability: Medium.
- Model use: positional priors + qualitative language (avoid global rank overweight due per-position list).
- Suggested blend: text trait 3-5%, risk penalty only, optional very light rank signal (<=3%).

3. **NFL.com (Daniel Jeremiah Top 50)**
- URL: https://www.nfl.com/news/daniel-jeremiah-s-top-50-2026-nfl-draft-prospect-rankings-2-0
- Why: One of the strongest public analyst boards.
- Parseability: High (already integrated).
- Model use: rank anchor + strong language signal.
- Suggested blend: keep current moderate anchor.

## Tier 2 (Strong, but constraints)

4. **ESPN Big Boards (Reid/Kiper/Yates/Miller)**
- Example URL: https://www.espn.com/nfl/draft2026/story/_/id/46573669/2026-nfl-draft-rankings-mel-kiper-big-board-top-prospects-players-positions
- Why: High analyst quality.
- Parseability: Medium (partial paywall risk).
- Model use: rank and language features where accessible.
- Suggested blend: similar to DJ/CBS where available.

5. **The Athletic (Dane Brugler / Consensus board)**
- Example URL: https://www.nytimes.com/athletic/7052286/2026/02/18/nfl-draft-2026-consensus-big-board-arvell-reese/
- Why: Elite quality and strong consensus signal.
- Parseability: Low direct (paywall), high with manual CSV exports.
- Model use: consensus sanity anchor and calibration check.
- Suggested blend: 6-10% consensus anchor if legally sourced.

6. **PFF Big Board Builder / Draft Board**
- Example URL: https://www.pff.com/news/draft-2026-nfl-draft-big-board-builder-is-live
- Why: premium grades + WAA + scouting notes.
- Parseability: Low without subscription access.
- Model use: premium production/grade layer if licensed.
- Suggested blend: moderate if licensed, otherwise metadata only.

## Tier 3 (Use with caution)

7. **Yahoo (Nate Tice big board articles)**
- Example URL: https://sports.yahoo.com/nfl/article/2026-nfl-draft-midseason-big-board-several-qbs-to-like-but-sec-and-big-ten-defenders-are-top-prospects-at-this-point-170801577.html
- Why: good analysis quality, but article structure can change.
- Parseability: Medium.
- Model use: language layer more than hard rank.

8. **Sporting News big board pages**
- Example URL: https://www.sportingnews.com/us/nfl/news/nfl-draft-prospects-2026-big-board-top-50-player-rankings/a63de6826f784f1a236bbde0
- Why: broad list + positions, useful as external rank source.
- Parseability: Medium.
- Model use: extra consensus/rank coverage.

9. **FantasyPros consensus draft board pages**
- Example URL: https://www.fantasypros.com/2026/02/2026-nfl-draft-big-board-expert-prospect-rankings-mendoza/
- Why: aggregated consensus with mean/stddev.
- Parseability: High.
- Model use: great consensus stability feature (`avg`, `stddev`) not scouting text.

## Recommendation

- Next ingestion priority: **NFL.com Bucky + FantasyPros consensus**.
- Keep CBS/DJ/ESPN as your primary qualitative core.
- Treat paywalled sources as manual/licensed enrichment.
