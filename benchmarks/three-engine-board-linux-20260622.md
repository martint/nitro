# Three-engine board (2026-06-22) — Nitro w/ new Parquet reader

Nitro(new reader)+Trino re-run this session, taskset -c 0, JMH @Fork1/wi3/i5. Velox REUSED from
20260621 (unchanged C++ reference). Ratio>1 = Nitro slower than that engine.

## Geomeans of per-query ratios

| suite | #N∩V | N/V geo | N/V med | #N∩T | N/T geo | N/T med | TPC-DS Nitro old→new |
|---|---|---|---|---|---|---|---|
| TPC-DS | 98 | 1.65x | 1.67x | 99 | 0.30x | 0.33x | 1.095x (99q) |
| TPC-H | 21 | 1.81x | 1.66x | 21 | 0.88x | 0.87x | 1.107x (21q) |
| ClickBench | 31 | 0.93x | 0.93x | 35 | 0.65x | 0.83x | 1.128x (39q) |

## TPC-DS: Nitro new-vs-old (reader impact), biggest movers

| q | old ms | new ms | new/old |
|---|---|---|---|
| 3 | 129 | 75 | 0.58x |
| 22 | 1947 | 1348 | 0.69x |
| 39 | 2138 | 1492 | 0.70x |
| 55 | 150 | 121 | 0.80x |
| 14 | 6537 | 5351 | 0.82x |
| 42 | 147 | 122 | 0.82x |
| 72 | 4271 | 3609 | 0.85x |
| 66 | 489 | 414 | 0.85x |
| ... | | | |
| 83 | 92 | 150 | 1.63x |
| 76 | 675 | 1154 | 1.71x |
| 45 | 200 | 354 | 1.77x |
| 91 | 115 | 212 | 1.84x |
| 41 | 8 | 20 | 2.37x |

TPC-DS total: old sum=153772ms  new sum=156049ms

## Clean same-day N/T: queries where Nitro is SLOWER than Trino (>1.05x)

### TPC-DS: 2 queries Nitro-slower
  q82: nitro=4286 trino=3825 (1.12x)
  q37: nitro=4075 trino=3747 (1.09x)
### TPC-H: 5 queries Nitro-slower
  q10: nitro=7044 trino=2940 (2.40x)
  q6: nitro=1189 trino=900 (1.32x)
  q19: nitro=2823 trino=2329 (1.21x)
  q15: nitro=2179 trino=1832 (1.19x)
  q14: nitro=1941 trino=1697 (1.14x)
### ClickBench: 7 queries Nitro-slower
  q2: nitro=602 trino=343 (1.75x)
  q8: nitro=617 trino=379 (1.63x)
  q20: nitro=632 trino=472 (1.34x)
  q13: nitro=4141 trino=3597 (1.15x)
  q32: nitro=4422 trino=3980 (1.11x)
  q15: nitro=4092 trino=3842 (1.07x)
  q31: nitro=3803 trino=3588 (1.06x)

## Caveats (important)
- **Velox numbers reused from 20260621** (unchanged C++). The box drifted ~10% slower between then and
  now: TPC-H and ClickBench Nitro (reader UNCHANGED for those suites) are 1.11x / 1.13x slower today than
  20260621. So **N/V here is inflated ~10% by machine drift, not regression** — treat N/V as approximate.
  The clean same-day signal is **N/T** (Nitro and Trino both re-run this session).
- Nitro TPC-DS ran with the new reader: `-Dnitro.parquet.useNitroReader=true -Dnitro.parquet.nitroSkip=true`.
- TPC-DS Nitro old->new geomean 1.095x is mostly the ~10% machine drift; net reader effect ≈ neutral with
  targeted scan wins (q3 0.58x, q22 0.69x, q39 0.70x, q14 0.82x, q72 0.85x) and noise on tiny queries.
- New reader-internal laggard surfaced: **q76 nitroSkip 1013ms vs Trino-reader 767ms = 1.32x** (clean idle).
  Does NOT make Nitro lose to the Trino *tree* (N/T for q76 < 1). Candidate for the next reader pass.
