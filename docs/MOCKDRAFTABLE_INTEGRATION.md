# MockDraftable Integration (2026)

## Verdict

Use it, but as an **athletic normalization and comp layer**, not as a primary ranking source.

Best use cases:
- Position-specific combine baselines (mean/std/count).
- Z-score and percentile normalization by position.
- Athletic style comps against historical archetypes.

Not good for:
- Independent board ranking authority.
- Production projection by itself.
- Team-fit / scheme logic by itself.

## Data pulled now

Saved baseline file:
- `data/sources/mockdraftable_position_baselines_2026-02-25.csv`

This includes position aggregates for:
- QB, RB, WR, TE, OT, IOL, EDGE, DT, LB, CB, S

Metrics captured (when available):
- Height, Weight, Wingspan, Arm, Hand
- 10 split, 20 split, 40
- Vertical, Broad
- Shuttle, 3-cone
- Bench

Each metric has:
- mean
- std
- sample count

## Refresh pipeline

Live pull script:

```bash
cd /Users/nickholz/nfldraftmodel2026
python3 scripts/pull_mockdraftable_data.py --execute
```

Dry run (no calls):

```bash
python3 scripts/pull_mockdraftable_data.py
```

Output:
- `data/sources/mockdraftable_position_baselines.csv`

## How to use in your model

1. Athletic percentile features
- For each metric:
  - `z = (player_value - position_mean) / position_std`
  - For timed drills (10 split, 40, shuttle, 3-cone), invert sign.
- Convert to percentile for interpretability.

2. Composite athletic index
- Build components:
  - speed: 10 split + 40
  - explosion: vertical + broad + bench
  - agility: shuttle + 3-cone
  - size: height + weight + arm + hand
- Weighted position-specific composite (calibrate later on historical outcomes).

3. RAS support
- Use as a fallback comparator when official RAS is missing.
- Keep official RAS as source of truth when available.

4. Comp engine
- Find nearest neighbors by position in normalized metric space.
- Output comp confidence by distance bucket.

## What to avoid

- Do not overweight MockDraftable vs film and production.
- Do not use post-draft labels as pre-draft features.
- Do not treat all position groups as identical distributions.

## Recommended weights (initial)

If official combine is available:
- athletic_score contribution can include 20-35% of normalized MockDraftable metric signal.

If official combine is missing:
- keep current athletic proxy,
- add only a light stabilizer (<=10%) from MockDraftable position baseline assumptions.

## Source links

- Home: https://www.mockdraftable.com/
- Position pages: `https://www.mockdraftable.com/positions?position=<POS>`

## Compliance notes

- Confirm website Terms/robots policy before large-scale automation.
- Respect rate limits and crawl intervals.
- Keep pulls incremental and cache results locally.
