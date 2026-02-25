# Sources And Use Cases (2026 Cycle)

## Primary public sources

1. CRAN Task View: Sports Analytics (`https://cran.r-project.org/view=SportsAnalytics`)
- Use case: package discovery for reproducible NFL/CFB workflows in R.
- What to pull: modeling libraries, time-series tools, optimization packages, simulation packages.

2. `nflverse` docs (`https://nflverse.nflverse.com`)
- Use case: NFL play-by-play, rosters, depth charts, schedules, injuries, weekly data.
- What to pull: team tendency features, need calibration, scheme tags, usage trends.

3. `nflreadr` docs (`https://nflreadr.nflverse.com`)
- Use case: direct loading of nflverse data in R.
- What to pull: draft history, PBP, player stats, roster snapshots.

4. `cfbfastr`/SportsDataverse CFB docs (`https://cfbfastr.sportsdataverse.org`)
- Use case: college play-by-play and player/team-level advanced features.
- What to pull: EPA/play, success rate, pressure/sack context, target share, route depth proxies, explosive rate.

5. DraftTek 2026 Big Board (`https://www.drafttek.com/2026-NFL-Draft-Big-Board/Top-NFL-Draft-Prospects-2026-Page-1.asp`)
- Use case: large candidate pool seed (top 300+).
- What to pull: player name, school, position, size, class year, baseline rank.

6. NFL Mock Draft Database 2026 consensus (`https://www.nflmockdraftdatabase.com/big-boards/2026/consensus-big-board-2026`)
- Use case: cross-source consensus signal and rank volatility.
- What to pull: consensus rank and source count.

7. NFL.com Draft order tracker (`https://www.nfl.com/news/2026-nfl-draft-order-for-all-seven-rounds`) and NFL Operations (`https://operations.nfl.com`)
- Use case: official pick order, comp picks, and traded-pick ownership.

8. Odds API (`https://the-odds-api.com`) (requires key)
- Use case: market-implied probabilities for #1 pick, first position drafted, team-to-draft-player props.
- Note: odds pulls are approval-gated per your instruction.

## Analyst ranking feeds

1. Daniel Jeremiah (NFL.com Top 50 / position rankings)
- Public and ingestable with scraper adapter.

2. PFF draft board (`https://www.pff.com/draft/big-board`)
- Public summary often available; deeper details may require subscription.

3. Dane Brugler (The Athletic) and Todd McShay feeds
- Usually subscription-gated; supported through manual CSV import template.

4. NFL Stock Exchange / media boards and mocks
- Use as another external rank input with source weighting.

## Recommended use cases by module

- `Prospect grading`: merge production + athletic + size + film/trait tags.
- `Team fit`: use team offensive/defensive tendencies + coordinator tree + personnel usage.
- `Mock simulation`: blend board value, positional demand, roster needs, draft capital, and trade pressure.
- `Historical comps`: nearest-neighbor by position archetype + size/athletic/production profile.
- `Publishing`: player profile cards + board pages + downloadable CSV/JSON.

## Suggested additional data

- Shrine Bowl / Senior Bowl / East-West Shrine participation and practice reports.
- Combine and Pro Day verified testing (laser/hand-timed flags).
- Injury history timeline with missed games and surgery types.
- Character/background confidence tags (scout-entered only).
- NIL transfer history and age-at-draft features.
