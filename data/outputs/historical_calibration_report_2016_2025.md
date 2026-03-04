# Historical Calibration Report (2016-2025)

- Sample size: **2051**
- Data source: **leagify_2015_2023**
- Year window: **2016-2025**
- Actual years loaded: **2016-2023**
- Logistic intercept: `-8.994193`
- Logistic slope: `0.125449`

## Position Additives

| Position | Additive |
|---|---:|
| CB | +0.0217 |
| DL | -0.1200 |
| DT | +0.0797 |
| EDGE | +0.0109 |
| IOL | -0.0657 |
| K | +0.0000 |
| LB | +0.1104 |
| LS | +0.0000 |
| OT | -0.0289 |
| P | +0.0000 |
| QB | -0.1200 |
| RB | +0.0506 |
| S | +0.0717 |
| TE | +0.0109 |
| WR | -0.0410 |

## Pick Slot Calibration

- Pick intercept: `-16.0`
- Pick slope: `7.15`
- Objective optimized on: pick-slot MAE, top-32 hit rate, and QB/OT/EDGE/CB position MAE.

### Position Slot Additives

| Position | Slot Additive |
|---|---:|
| CB | -1.672 |
| DL | +3.137 |
| DT | -1.441 |
| EDGE | -1.599 |
| IOL | -0.116 |
| LB | +1.278 |
| OT | -1.955 |
| QB | +0.846 |
| RB | +2.371 |
| S | -0.092 |
| TE | -0.940 |
| WR | +0.268 |

### In-Sample Pick Metrics

- pick_slot_mae: **18.831**
- top32_hit_rate: **0.3702**
- pos_mae_avg_qb_ot_edge_cb: **18.233**
- objective: **0.50755**

## Year-Based Backtest

| Holdout Year | Train Rows | Test Rows | Brier | Accuracy | Avg Pred | Obs Rate |
|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 253 | 253 | 0.1971 | 0.6877 | 0.6687 | 0.6364 |
| 2018 | 506 | 256 | 0.1648 | 0.7578 | 0.6577 | 0.7227 |
| 2019 | 762 | 254 | 0.1752 | 0.7323 | 0.6849 | 0.6457 |
| 2020 | 1016 | 255 | 0.2125 | 0.6706 | 0.6768 | 0.5843 |
| 2021 | 1271 | 259 | 0.3108 | 0.5212 | 0.6635 | 0.3436 |
| 2022 | 1530 | 262 | 0.435 | 0.3321 | 0.6312 | 0.0115 |
| 2023 | 1792 | 259 | 0.563 | 0.1699 | 0.7249 | 0.0 |

## Year-Based Pick Slot Backtest

| Holdout Year | Train Rows | Test Rows | Pick MAE | Top-32 Hit | Pos MAE (QB/OT/EDGE/CB) | Objective |
|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 253 | 253 | 18.007 | 0.3438 | 18.755 | 0.50544 |
| 2018 | 506 | 256 | 18.054 | 0.3438 | 17.928 | 0.50194 |
| 2019 | 762 | 254 | 18.191 | 0.3125 | 17.862 | 0.51131 |
| 2020 | 1016 | 255 | 18.373 | 0.3438 | 17.48 | 0.50409 |
| 2021 | 1271 | 259 | 18.541 | 0.3125 | 17.526 | 0.51445 |
| 2022 | 1530 | 262 | 18.512 | 0.3125 | 19.357 | 0.5232 |
| 2023 | 1792 | 259 | 37.199 | 1.0 | 33.57 | 0.67934 |