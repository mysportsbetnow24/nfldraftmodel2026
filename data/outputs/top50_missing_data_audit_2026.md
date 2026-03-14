# Top-50 Missing Data Audit

- source: `/Users/nickholz/nfldraftmodel2026/data/processed/big_board_2026.csv`
- rows: `50`

## Highest-Priority Cleanup Targets

| Rank | Player | Pos | School | Priority | Flags | Athletic Coverage | Film Coverage | Prod Reliability |
|---:|---|---|---|---:|---|---:|---:|---:|
| 9 | Francis Mauigoa | OT | Miami (FL) | 5.486 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing; production_signal_light | 0.182 | 0.000 | 0.000 |
| 34 | Akheem Mesidor | EDGE | Miami (FL) | 5.486 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing; production_signal_light | 0.182 | 0.000 | 0.000 |
| 36 | Rueben Bain Jr. | EDGE | Miami (FL) Hurricanes | 5.486 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing; production_signal_light | 0.182 | 0.000 | 0.000 |
| 41 | KC Concepcion | WR | Texas A&M Aggies | 4.924 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.750 |
| 11 | Mansoor Delane | CB | LSU | 4.909 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.770 |
| 14 | Makai Lemon | WR | USC Trojans | 4.886 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.800 |
| 8 | Caleb Downs | S | Ohio State | 4.871 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.820 |
| 16 | Kayden McDonald | DT | Ohio State Buckeyes | 4.871 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.820 |
| 30 | Fernando Mendoza | QB | Indiana | 4.871 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.820 |
| 32 | Peter Woods | DT | Clemson | 4.871 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.820 |
| 45 | Christen Miller | DT | Georgia Bulldogs | 4.871 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.820 |
| 46 | Ty Simpson | QB | Alabama Crimson Tide | 4.871 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.820 |
| 27 | C.J. Allen | LB | Georgia | 4.841 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.182 | 0.000 | 0.860 |
| 7 | Jordyn Tyson | WR | Arizona State | 4.555 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.273 | 0.000 | 0.800 |
| 21 | Carnell Tate | WR | Ohio State | 4.555 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.273 | 0.000 | 0.800 |
| 4 | Jeremiyah Love | RB | Notre Dame | 4.540 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.273 | 0.000 | 0.820 |
| 47 | Tyreak Sapp | EDGE | Florida | 4.540 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.273 | 0.000 | 0.820 |
| 2 | Kadyn Proctor | OT | Alabama | 4.491 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing; production_signal_light | 0.455 | 0.000 | 0.000 |
| 6 | Caleb Lomu | OT | Utah | 4.491 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing; production_signal_light | 0.455 | 0.000 | 0.000 |
| 12 | Arvell Reese | LB | Ohio State | 4.480 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.273 | 0.000 | 0.900 |
| 49 | Josiah Trotter | LB | Missouri Tigers | 4.480 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.273 | 0.000 | 0.900 |
| 18 | Olaivavega Ioane | IOL | Penn State | 4.260 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.364 | 0.000 | 0.750 |
| 22 | Omar Cooper Jr. | WR | Indiana | 4.223 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.364 | 0.000 | 0.800 |
| 25 | Denzel Boston | WR | Washington | 4.223 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.364 | 0.000 | 0.800 |
| 13 | Keldric Faulk | EDGE | Auburn | 4.208 | athletic_coverage_thin; many_athletic_blanks; film_traits_missing | 0.364 | 0.000 | 0.820 |

## Notes

- `athletic_coverage_thin`: fewer than half of the tracked athletic fields are populated.
- `many_athletic_blanks`: six or more athletic measurement fields are still empty.
- `film_traits_missing`: structured film-trait coverage is effectively missing.
- `production_signal_light`: current production reliability is below the strong-confidence band.

Full CSV: `/Users/nickholz/nfldraftmodel2026/data/outputs/top50_missing_data_audit_2026.csv`