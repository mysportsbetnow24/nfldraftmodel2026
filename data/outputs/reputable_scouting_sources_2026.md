# Reputable 2026 Scouting Sources (Shortlist)

| Source | Credibility | Access | Pull Feasibility | Best Use In Model | URL |
|---|---|---|---|---|---|
| CBS Sports Top 100 Big Board | High (national desk) | Public | High (stable HTML headings + blurbs) | Rank anchor + scouting text traits + risk language | https://www.cbssports.com/nfl/draft/news/2026-nfl-draft-top-prospects-big-board-rankings/ |
| NFL.com Daniel Jeremiah Top 50 | High (league media) | Public | Already integrated | Rank anchor + report language | https://www.nfl.com/news/daniel-jeremiah-s-top-50-2026-nfl-draft-prospect-rankings-2-0 |
| NFL.com Bucky Brooks Top 5 by Position | High (league media) | Public | Medium (position-list parsing) | Positional priors + tie-break notes | https://www.nfl.com/news/bucky-brooks-2026-nfl-draft-top-5-prospects-by-position |
| NFL.com Prospect Tracker (2026) | High (official league page) | Public | Medium (table/card extraction) | Identity normalization + board completeness + pipeline QA | https://www.nfl.com/draft/tracker/prospects/ALL/all-colleges/all-statuses/2026?page=1 |
| ESPN Jordan Reid Big Board | High (analyst) | Partial paywall | Already integrated from public-accessible pulls | Consensus anchor + language style signal | https://www.espn.com/nfl/draft2026/story/_/id/47027232/2026-nfl-draft-rankings-jordan-reid-top-prospects-players-positions |
| PFF Draft Big Board | High (grading org) | Paywall for full data | Low without subscription export | If licensed: premium rank + role/value tags | https://www.pff.com/draft/big-board |
| The Athletic Consensus Big Board | High (aggregator quality) | Paywall | Low direct scrape, medium with manual captures | Consensus sanity anchor only | https://www.nytimes.com/athletic/7052286/2026/02/18/nfl-draft-2026-consensus-big-board-arvell-reese/ |

## Current Recommendation

1. Keep CBS + NFL.com (DJ + Bucky + tracker) as core public scouting stack.
2. Keep ESPN/DJ/CBS as the primary reputable text/rank anchors.
3. Treat paywalled sources (PFF/Athletic) as optional licensed/manual-input layers.
