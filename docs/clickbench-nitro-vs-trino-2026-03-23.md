# ClickBench Nitro vs Trino Benchmark Snapshot

Date: 2026-03-25

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
- The summary and table below reflect the current best-known per-query numbers, combining the newest Nitro full-suite forked rerun, the existing forked Trino full-suite baseline, and the newer focused faithful-`LIKE` Trino reruns for `Q21-Q24`.
- The overall benchmark total below is derived by summing the per-query average times for all 44 queries, not by using the suite wall-clock measurement.

Summary:
- Nitro faster on 44 / 44 queries
- Trino faster on 0 / 44 queries
- Geometric mean of `Trino / Nitro`: about `2.22x`
- End-to-end total from summed per-query times:
  Nitro `268.25s` (`4m 28.25s`)
  Trino `622.20s` (`10m 22.20s`)
  Trino took about `2.32x` as long as Nitro overall by this aggregate measure

Notable movement in this refresh:
- Nitro was rerun end to end after the recent `TopN` ordering work, and the summed per-query total improved from `272.69s` to `268.25s`.
- The latest full Nitro run moved the remaining ordered-string holdouts in the right direction: `Q25` improved from `2758.597 ms/op` to `2646.803 ms/op`, `Q26` from `2524.199 ms/op` to `2222.973 ms/op`, and `Q27` from `2889.928 ms/op` to `2630.661 ms/op`.
- `Q7` also stayed firmly on the Nitro side after the fused global integer `min/max` path, landing at `180.683 ms/op` against the Trino baseline of `534.487 ms/op`.
- With those changes in place, there are no remaining Trino wins in the current best-known per-query mix. The closest near-ties are now `Q25` and `Q27`, but Nitro is still slightly ahead on both.

Targeted `Q21-Q24` post-fix refresh:
- Method: same forked shape (`-f 1 -wi 0 -i 3 -w 1ms -r 1ms`) on the real parquet `hits` data
- This rerun switched the Trino harness to faithful `LIKE` / `NOT LIKE` expressions instead of the earlier `strpos` lowering
- Nitro also includes the later `contains_utf8` Vector API rewrite that removed the old hot-path mask allocation churn
- Focused wall clock:
  Nitro `118.23s` (`1m 58.23s`)
  Trino `283.52s` (`4m 43.52s`)
  Trino took about `2.40x` as long across `Q21-Q24`
- Historical focused outcomes for those four queries:
  - `Q21`: Nitro `8503.677 ms/op`, Trino `15249.959 ms/op`
  - `Q22`: Nitro `8431.885 ms/op`, Trino `9819.377 ms/op`
  - `Q23`: Nitro `11527.449 ms/op`, Trino `20027.606 ms/op`
  - `Q24`: Nitro `10403.475 ms/op`, Trino `46682.787 ms/op`
- Main takeaway: the earlier Trino advantage on `Q21` was an artifact of the non-canonical `strpos` lowering, and the remaining `Q22` gap disappeared once Nitro’s `contains_utf8` hot loop stopped allocating Vector API mask temporaries. With the faithful `LIKE` harness and the rewritten kernel, Nitro is now faster on all four queries in this family.

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 6303.188 | 36092.080 | 5.73 | Nitro |
| Q1 | 33.979 | 388.604 | 11.44 | Nitro |
| Q2 | 191.270 | 534.153 | 2.79 | Nitro |
| Q3 | 523.585 | 1371.200 | 2.62 | Nitro |
| Q4 | 390.578 | 606.067 | 1.55 | Nitro |
| Q5 | 2448.137 | 3612.037 | 1.48 | Nitro |
| Q6 | 5654.378 | 7771.508 | 1.37 | Nitro |
| Q7 | 180.683 | 534.487 | 2.96 | Nitro |
| Q8 | 195.197 | 521.405 | 2.67 | Nitro |
| Q9 | 7257.483 | 7887.137 | 1.09 | Nitro |
| Q10 | 8787.209 | 10147.615 | 1.15 | Nitro |
| Q11 | 1207.570 | 1415.871 | 1.17 | Nitro |
| Q12 | 1411.721 | 1688.791 | 1.20 | Nitro |
| Q13 | 4743.216 | 6252.828 | 1.32 | Nitro |
| Q14 | 7402.496 | 11227.541 | 1.52 | Nitro |
| Q15 | 5547.745 | 6274.031 | 1.13 | Nitro |
| Q16 | 3394.017 | 4077.531 | 1.20 | Nitro |
| Q17 | 13285.312 | 16129.625 | 1.21 | Nitro |
| Q18 | 12545.658 | 14831.865 | 1.18 | Nitro |
| Q19 | 20030.133 | 22661.311 | 1.13 | Nitro |
| Q20 | 334.543 | 594.154 | 1.78 | Nitro |
| Q21 | 7874.495 | 15249.959 | 1.94 | Nitro |
| Q22 | 7596.159 | 9819.377 | 1.29 | Nitro |
| Q23 | 10345.257 | 20027.606 | 1.94 | Nitro |
| Q24 | 9282.484 | 46682.787 | 5.03 | Nitro |
| Q25 | 2646.803 | 2660.568 | 1.01 | Nitro |
| Q26 | 2222.973 | 2419.228 | 1.09 | Nitro |
| Q27 | 2630.661 | 2668.373 | 1.01 | Nitro |
| Q28 | 9295.595 | 10135.586 | 1.09 | Nitro |
| Q29 | 27117.626 | 186324.703 | 6.87 | Nitro |
| Q30 | 8179.079 | 29580.280 | 3.62 | Nitro |
| Q31 | 4310.405 | 5248.066 | 1.22 | Nitro |
| Q32 | 5079.382 | 5600.837 | 1.10 | Nitro |
| Q33 | 19710.718 | 24454.002 | 1.24 | Nitro |
| Q34 | 20017.862 | 29231.639 | 1.46 | Nitro |
| Q35 | 21584.118 | 31645.996 | 1.47 | Nitro |
| Q36 | 4825.592 | 11685.647 | 2.42 | Nitro |
| Q37 | 517.220 | 6060.612 | 11.72 | Nitro |
| Q38 | 468.466 | 6349.352 | 13.55 | Nitro |
| Q39 | 373.442 | 5724.804 | 15.33 | Nitro |
| Q40 | 726.804 | 10166.418 | 13.99 | Nitro |
| Q41 | 565.799 | 2031.502 | 3.59 | Nitro |
| Q42 | 558.139 | 1985.264 | 3.56 | Nitro |
| Q43 | 449.055 | 1827.496 | 4.07 | Nitro |

Queries where Trino is faster in the current best-known per-query mix:
- None
