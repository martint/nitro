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
- `Q21-Q24` were later rerun in a focused post-fix pass after changing the Trino harness from a bespoke `strpos` lowering to faithful SQL `LIKE` / `NOT LIKE`.
- The summary and table below reflect the current best-known per-query numbers, combining the last whole-suite forked run with the newer focused `Q21-Q24` reruns.
- The overall benchmark total below is derived by summing the per-query average times for all 44 queries, not by using the suite wall-clock measurement.

Summary:
- Nitro faster on 31 / 44 queries
- Trino faster on 13 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.59x`
- End-to-end total from summed per-query times:
  Nitro `305.56s` (`5m 05.56s`)
  Trino `622.20s` (`10m 22.20s`)
  Trino took about `2.04x` as long as Nitro overall by this aggregate measure

Notable movement in this refresh:
- The forked full-suite baseline still anchors most rows, but the later focused `Q21-Q24` reruns now replace those four entries in the summary.
- `Q22` flipped to Nitro after the `contains_utf8` Vector API rewrite (`8431.885 ms/op` vs Trino `9819.377 ms/op`).
- The whole `Q21-Q24` family is now on the Nitro side.
- The remaining Trino wins are concentrated in a smaller set of near-ties and grouped queries: `Q4`, `Q7`, `Q9`, `Q11`, `Q12`, `Q15`, `Q16`, `Q19`, `Q25-Q28`, and `Q32`.

Targeted `Q21-Q24` post-fix refresh:
- Method: same forked shape (`-f 1 -wi 0 -i 3 -w 1ms -r 1ms`) on the real parquet `hits` data
- This rerun switched the Trino harness to faithful `LIKE` / `NOT LIKE` expressions instead of the earlier `strpos` lowering
- Nitro also includes the later `contains_utf8` Vector API rewrite that removed the old hot-path mask allocation churn
- Focused wall clock:
  Nitro `118.23s` (`1m 58.23s`)
  Trino `283.52s` (`4m 43.52s`)
  Trino took about `2.40x` as long across `Q21-Q24`
- Updated outcomes for those four queries:
  - `Q21`: Nitro `8503.677 ms/op`, Trino `15249.959 ms/op`
  - `Q22`: Nitro `8431.885 ms/op`, Trino `9819.377 ms/op`
  - `Q23`: Nitro `11527.449 ms/op`, Trino `20027.606 ms/op`
  - `Q24`: Nitro `10403.475 ms/op`, Trino `46682.787 ms/op`
- Main takeaway: the earlier Trino advantage on `Q21` was an artifact of the non-canonical `strpos` lowering, and the remaining `Q22` gap disappeared once Nitro’s `contains_utf8` hot loop stopped allocating Vector API mask temporaries. With the faithful `LIKE` harness and the rewritten kernel, Nitro is now faster on all four queries in this family.

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
| Q21 | 8503.677 | 15249.959 | 1.79 | Nitro |
| Q22 | 8431.885 | 9819.377 | 1.16 | Nitro |
| Q23 | 11527.449 | 20027.606 | 1.74 | Nitro |
| Q24 | 10403.475 | 46682.787 | 4.49 | Nitro |
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

Queries where Trino is faster in the current best-known per-query mix:
- Q4
- Q7
- Q9
- Q11
- Q12
- Q15
- Q16
- Q19
- Q25
- Q26
- Q27
- Q28
- Q32
