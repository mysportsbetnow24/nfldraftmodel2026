# Model Formulas

## 1. Composite player grade

For each player `p` at position `pos`:

`grade_p = w_trait*trait_score + w_prod*production_score + w_ath*athletic_score + w_size*size_score + w_context*context_score - risk_penalty`

Where weights are position-specific (see `config/position_weights.yml`).

## 2. Floor and ceiling

- `floor_grade = grade_p - volatility_penalty`
- `ceiling_grade = grade_p + upside_bonus`

Volatility drivers:
- low sample size
- role ambiguity
- age/injury flags
- inconsistent film vs production alignment

## 3. Round value mapping

- 92-100: Round 1
- 88-91.9: Round 1-2
- 84-87.9: Round 2-3
- 80-83.9: Round 3-4
- 76-79.9: Round 4-5
- 72-75.9: Round 5-6
- 68-71.9: Round 6-7
- <68: UDFA

## 4. Team fit score

`team_fit = 0.50*need_score + 0.25*scheme_score + 0.15*roster_timeline_score + 0.10*gm_tendency_score`

- `need_score`: based on depth chart age, contract status, and prior-year efficiency.
- `scheme_score`: fit against team front/coverage/offense tendency profile.
- `roster_timeline_score`: immediate role availability.
- `gm_tendency_score`: historical preference for size/athletic profile and position value.

## 5. Draft pick score

At pick `k` by team `t` for player `p`:

`pick_score = 0.55*board_value + 0.30*team_fit + 0.10*positional_run_pressure + 0.05*scarcity_bonus`

- `board_value = 100 - normalized_rank`
- `positional_run_pressure`: higher when several teams behind share same urgent need.
- `scarcity_bonus`: bump for thin position tiers.

## 6. Trade-down signal

Team on clock gets a trade-down flag when:
- no top-need player in current tier, and
- at least 2 similarly graded players expected to be available 4-10 picks later, and
- a QB/OT/EDGE scarcity signal exists for teams behind.

## 7. Historical comps

Use nearest-neighbor distance on standardized profile:

`distance = sum_i(alpha_i * abs(feature_i_player - feature_i_comp))`

Feature groups:
- size
- athletic testing
- production efficiency
- film trait buckets

Comp confidence tiers:
- `A`: distance <= 0.35
- `B`: 0.36 - 0.55
- `C`: >0.55

## 8. Relative Athletic Score (RAS) proxy

Until verified combine/pro-day testing is ingested, this model uses an estimated 0-10 RAS-like score:

`ras_estimate = 0.72*athletic_component + 0.23*size_component + 0.05*consensus_component`

- `athletic_component`: normalized from current athletic proxy.
- `size_component`: distance from position size targets.
- `consensus_component`: small stabilizer from seed rank.

RAS tiers:
- `elite`: >= 9.0
- `great`: 8.0-8.99
- `good`: 7.0-7.99
- `average`: 6.0-6.99
- `below_average`: < 6.0

Each tier maps to historical athlete buckets by position for quick contextual comps.
