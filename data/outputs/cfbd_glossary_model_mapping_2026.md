# CFBD Glossary -> Model Mapping (2026)

## Pull status (this run)
- Pulled: `data/sources/cfbd/team_advanced_stats_2025.json`
- Pulled: `data/sources/cfbd/team_ppa_2025.json`
- Pulled: `data/sources/cfbd/advanced_game_stats_2025.json`
- Existing already in project: `player_season_stats_2025.json`, `player_ppa_2025.json`, `fbs_teams_2025.json`
- CFBD usage after pull: 10 / 1000 calls (2026-02)

## Separation policy (locked)
- Team metrics are **context-only** and do **not** enter player `final_grade`.
- Player `final_grade` may only use player-level features from `cfb_production_2025.csv` / `cfb_production_loader.py`.
- Team-level CFBD files are for:
  - team-needs engine
  - scheme/environment context
  - weekly opponent-adjusted notes

## Glossary metric mapping
| Glossary metric | Found in CFBD pulls | Player grade input? | Team/context use? | Practical use in your stack |
|---|---|---:|---:|---|
| Success Rate | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Team efficiency trend; offense/defense stability for fit context |
| Explosiveness | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Identify vertical/big-play environments for role projection |
| PPA (Predicted Points Added) | `team_advanced_stats`, `team_ppa`, `advanced_game_stats`, `player_ppa` | Yes (player only) | Yes | Player PPA for production score; team PPA for context only |
| EP / EPA | Indirect via CFBD PPA framework | Yes (player proxies only) | Yes | Keep EPA/PPA at player-level for grade; team EPA/PPA for environment |
| Havoc | `team_advanced_stats` (`havoc.total/frontSeven/db`) | No | Yes | OL/DL need pressure and secondary disruption context |
| Front Seven Havoc | `team_advanced_stats` (`havoc.frontSeven`) | No | Yes | Front-7 disruption profile for EDGE/DT/LB landing fit |
| DB Havoc | `team_advanced_stats` (`havoc.db`) | No | Yes | Coverage-disruption environment for CB/S fit |
| Line Yards | `team_advanced_stats`, `advanced_game_stats` | No | Yes | OL run-block ecosystem; RB projection context |
| Second Level Yards | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Run-game structure quality (fit context for RB/OL) |
| Open Field Yards | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Explosive rushing environment quality |
| Stuff Rate | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Trench resistance/creation context |
| Power Success | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Short-yardage identity (IOL/TE/RB context) |
| Passing Downs | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Team performance in obvious pass situations |
| Standard Downs | `team_advanced_stats`, `advanced_game_stats` | No | Yes | Early-down execution profile |
| Scoring Opportunities | `team_advanced_stats` (`totalOpportunies`) | No | Yes | Red-zone opportunity context |
| Points Per Opportunity | `team_advanced_stats` | No | Yes | Red-zone conversion quality by team |
| Field Position | `team_advanced_stats` (`fieldPosition.*`) | No | Yes | Hidden context in production inflation/deflation |
| Usage | Not direct in team files; inferred in player stats workloads | Yes (player only) | Optional | Player involvement can remain player-only workload signal |
| Excitement Index | Not in pulled endpoints | No | Optional | Can be derived later from win-probability swings per game |
| Postgame Win Probability | Not in these pulled endpoints | No | Optional | Add via games + win-prob API if desired |
| Garbage Time | Not a direct field | No | Optional | Derive from score/time rules for stat de-noising |
| SRS | Not direct in these files | No | Optional | Add external SRS feed for opponent/schedule normalization |

## Recommended immediate usage (safe)
1. Keep current player production block as-is (`cfb_production_loader.py`) for grading.
2. Add a **separate** team-context table built from `team_advanced_stats` + `team_ppa`:
   - `team_off_success_rate`, `team_def_success_rate`
   - `team_off_explosiveness`, `team_def_explosiveness`
   - `team_havoc_created`, `team_havoc_allowed`
   - `team_off_passing_down_success`, `team_def_passing_down_success_allowed`
3. Use those only in:
   - `best_team_fit`
   - mock draft team-need pressure
   - narrative scouting card context

## Do-not-cross rule
- Any column prefixed `team_` or sourced from team-level CFBD endpoints must be excluded from:
  - `production_score`
  - `formula_production_component`
  - `final_grade`

