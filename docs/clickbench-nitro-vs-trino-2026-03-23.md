# ClickBench Nitro vs Trino Benchmark Snapshot

Date: 2026-03-24

This snapshot compares the Nitro ClickBench harness and the separate hand-built Trino 479 ClickBench harness on the real parquet `hits` dataset at `/Users/martin/tmp/clickbench`.

Method:
- JMH forked runs (`-f 1`) for both harnesses
- `-wi 0 -i 3 -r 1ms -w 1ms`
- Nitro full pass at `-Xmx16g`
- Trino full pass at `-Xmx16g` with `-Dnitro.clickbench.trino.queryMaxMemoryGigabytes=8`
- Both harnesses used `-Dnitro.trino.scan.maxBatchRows=10000`

Caveats:
- These are still directional engineering numbers, not publication-grade benchmark results.
- The forked `-f 1 -i 3` shape is materially more stable than the earlier same-VM one-shot runs, but the confidence intervals are still wide on some queries because the iterations are intentionally short.
- `Q21-Q24` were later rerun in a focused post-fix pass after changing the Trino harness from a bespoke `strpos` lowering to faithful SQL `LIKE` / `NOT LIKE`. The main full-suite headline below still comes from the last whole-suite run; the targeted `Q21-Q24` numbers below supersede those four rows only.

Summary:
- Nitro faster on 29 / 44 queries
- Trino faster on 15 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.52x`
- End-to-end full-suite wall clock:
  Nitro `924.20s` (`15m 24.20s`)
  Trino `1878.68s` (`31m 18.68s`)
  Trino took about `2.03x` as long as Nitro overall

Notable movement in this refresh:
- This refresh switches to the steadier forked baseline, so several near-ties moved relative to the earlier same-VM snapshot.
- `Q5` remains on the Nitro side under the forked runs (`2780.168 ms/op` vs Trino `3612.037 ms/op`).
- `Q30` also lands decisively on the Nitro side in the forked run (`8768.431 ms/op` vs Trino `29580.280 ms/op`).
- The main remaining Trino wins are now concentrated in `Q21/Q22`, several string/order near-ties (`Q25-Q28`), and a handful of grouped queries (`Q9`, `Q11`, `Q12`, `Q15`, `Q16`, `Q19`, `Q32`).

Targeted `Q21-Q24` post-fix refresh:
- Method: same forked shape (`-f 1 -wi 0 -i 3 -w 1ms -r 1ms`) on the real parquet `hits` data
- This rerun switched the Trino harness to faithful `LIKE` / `NOT LIKE` expressions instead of the earlier `strpos` lowering
- Focused wall clock:
  Nitro `142.16s` (`2m 22.16s`)
  Trino `273.39s` (`4m 33.39s`)
  Trino took about `1.92x` as long across `Q21-Q24`
- Updated outcomes for those four queries:
  - `Q21`: Nitro `11301.649 ms/op`, Trino `14408.644 ms/op`
  - `Q22`: Nitro `9911.027 ms/op`, Trino `8400.803 ms/op`
  - `Q23`: Nitro `12640.583 ms/op`, Trino `20372.006 ms/op`
  - `Q24`: Nitro `13057.308 ms/op`, Trino `45711.398 ms/op`
- Main takeaway: the earlier Trino advantage on `Q21` was an artifact of the non-canonical `strpos` lowering. With faithful `LIKE`, Nitro is faster on `Q21`, `Q23`, and `Q24`, and only `Q22` remains on the Trino side in this family.

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 6982.823 | 36092.080 | 5.17 | Nitro |
| Q1 | 127.903 | 388.604 | 3.04 | Nitro |
| Q2 | 443.777 | 534.153 | 1.20 | Nitro |
| Q3 | 773.959 | 1371.200 | 1.77 | Nitro |
| Q4 | 703.286 | 606.067 | 0.86 | Trino |
| Q5 | 2780.168 | 3612.037 | 1.30 | Nitro |
| Q6 | 6459.207 | 7771.508 | 1.20 | Nitro |
| Q7 | 703.559 | 534.487 | 0.76 | Trino |
| Q8 | 459.816 | 521.405 | 1.13 | Nitro |
| Q9 | 8298.814 | 7887.137 | 0.95 | Trino |
| Q10 | 9922.950 | 10147.615 | 1.02 | Nitro |
| Q11 | 1827.231 | 1415.871 | 0.77 | Trino |
| Q12 | 2015.342 | 1688.791 | 0.84 | Trino |
| Q13 | 5456.540 | 6252.828 | 1.15 | Nitro |
| Q14 | 8302.567 | 11227.541 | 1.35 | Nitro |
| Q15 | 6724.248 | 6274.031 | 0.93 | Trino |
| Q16 | 4272.656 | 4077.531 | 0.95 | Trino |
| Q17 | 15757.122 | 16129.625 | 1.02 | Nitro |
| Q18 | 13314.900 | 14831.865 | 1.11 | Nitro |
| Q19 | 22884.723 | 22661.311 | 0.99 | Trino |
| Q20 | 590.804 | 594.154 | 1.01 | Nitro |
| Q21 | 11301.649 | 14408.644 | 1.28 | Nitro |
| Q22 | 9911.027 | 8400.803 | 0.85 | Trino |
| Q23 | 12640.583 | 20372.006 | 1.61 | Nitro |
| Q24 | 13057.308 | 45711.398 | 3.50 | Nitro |
| Q25 | 2735.058 | 2660.568 | 0.97 | Trino |
| Q26 | 2760.080 | 2419.228 | 0.88 | Trino |
| Q27 | 2776.774 | 2668.373 | 0.96 | Trino |
| Q28 | 10653.674 | 10135.586 | 0.95 | Trino |
| Q29 | 28215.930 | 186324.703 | 6.60 | Nitro |
| Q30 | 8768.431 | 29580.280 | 3.37 | Nitro |
| Q31 | 4689.819 | 5248.066 | 1.12 | Nitro |
| Q32 | 5848.092 | 5600.837 | 0.96 | Trino |
| Q33 | 23129.801 | 24454.002 | 1.06 | Nitro |
| Q34 | 22395.394 | 29231.639 | 1.31 | Nitro |
| Q35 | 23058.926 | 31645.996 | 1.37 | Nitro |
| Q36 | 5677.634 | 11685.647 | 2.06 | Nitro |
| Q37 | 987.828 | 6060.612 | 6.14 | Nitro |
| Q38 | 914.819 | 6349.352 | 6.94 | Nitro |
| Q39 | 862.057 | 5724.804 | 6.64 | Nitro |
| Q40 | 1311.725 | 10166.418 | 7.75 | Nitro |
| Q41 | 1115.553 | 2031.502 | 1.82 | Nitro |
| Q42 | 1084.067 | 1985.264 | 1.83 | Nitro |
| Q43 | 903.862 | 1827.496 | 2.02 | Nitro |

Queries where Trino was faster in the last whole-suite run:
- Q4
- Q7
- Q9
- Q11
- Q12
- Q15
- Q16
- Q19
- Q22
- Q25
- Q26
- Q27
- Q28
- Q32
