# Top-50 Tuning Queue

- source: `/Users/nickholz/nfldraftmodel2026/data/processed/big_board_2026.csv`
- rows: `50`

## Highest-Leverage Tuning Targets

| Queue | Rank | Player | Pos | Priority | Market Gap | Lane | Recommended Actions | Current Fit |
|---:|---:|---|---|---:|---:|---|---|---|
| 1 | 47 | Tyreak Sapp | EDGE | 9.560 | +51.00 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | CAR |
| 2 | 26 | Dani Dennis-Sutton | EDGE | 9.209 | +63.00 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | NO |
| 3 | 42 | Brian Parker II | OT | 9.021 | +57.00 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | HOU |
| 4 | 31 | Skyler Bell | WR | 8.431 | +48.00 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | TEN |
| 5 | 44 | Sam Roush | TE | 8.350 | +57.00 | film + disagreement | add film traits; add age data; audit model-vs-market gap | CHI |
| 6 | 36 | Rueben Bain Jr. | EDGE | 8.181 | -24.00 | film + disagreement | fill testing; add film traits; verify production context; add age data; audit model-vs-market gap | DAL |
| 7 | 30 | Fernando Mendoza | QB | 7.830 | -28.42 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | LV |
| 8 | 37 | Daylen Everette | CB | 7.503 | +35.00 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | BAL |
| 9 | 46 | Ty Simpson | QB | 6.884 | -13.60 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | CLE |
| 10 | 32 | Peter Woods | DT | 6.686 | -13.75 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | TEN |
| 11 | 41 | KC Concepcion | WR | 6.654 | -11.00 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | ARI |
| 12 | 15 | Blake Miller | OT | 6.420 | +25.40 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | ARI |
| 13 | 16 | Kayden McDonald | DT | 6.372 | +12.83 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | NE |
| 14 | 34 | Akheem Mesidor | EDGE | 6.357 | +1.57 | testing + film | fill testing; add film traits; verify production context; add age data | DEN |
| 15 | 2 | Kadyn Proctor | OT | 6.284 | +19.10 | film + disagreement | fill testing; add film traits; verify production context; add age data; audit model-vs-market gap | WAS |
| 16 | 22 | Omar Cooper Jr. | WR | 6.260 | +18.40 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | TEN |
| 17 | 45 | Christen Miller | DT | 6.149 | +4.60 | testing + film | fill testing; add film traits; add age data | KC |
| 18 | 21 | Carnell Tate | WR | 6.095 | -12.38 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | MIA |
| 19 | 9 | Francis Mauigoa | OT | 6.016 | -2.00 | testing + film | fill testing; add film traits; verify production context; add age data | NYJ |
| 20 | 5 | Emmanuel McNeil-Warren | S | 5.994 | +22.60 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | NYJ |
| 21 | 6 | Caleb Lomu | OT | 5.916 | +13.75 | film + disagreement | fill testing; add film traits; verify production context; add age data; audit model-vs-market gap | BAL |
| 22 | 49 | Josiah Trotter | LB | 5.770 | +4.00 | testing + film | fill testing; add film traits; add age data | LV |
| 23 | 3 | Monroe Freeling | OT | 5.754 | +19.33 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | NE |
| 24 | 17 | Emmanuel Pregnon | IOL | 5.738 | +16.50 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | BAL |
| 25 | 40 | Avieon Terrell | CB | 5.732 | -12.30 | film + disagreement | fill testing; add film traits; add age data; audit model-vs-market gap | TEN |

## Lane Definitions

- `testing + film`: player needs both verified athletic coverage and structured film traits before the rank should harden.
- `film + disagreement`: player is thin on film coverage and also far from market consensus; strongest candidate for focused review.
- `film-first`: athletic and production coverage are usable, but the profile still lacks structured scouting traits.
- `testing-first`: most important next step is verified combine/pro-day coverage.
- `disagreement review`: coverage is serviceable, but the model is taking a strong stand versus the market.

Full CSV: `/Users/nickholz/nfldraftmodel2026/data/outputs/top50_tuning_queue_2026.csv`