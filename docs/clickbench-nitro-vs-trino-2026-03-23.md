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
- Nitro faster on 40 / 44 queries
- Trino faster on 4 / 44 queries
- Geometric mean of `Trino / Nitro`: about `2.13x`
- End-to-end total from summed per-query times:
  Nitro `272.69s` (`4m 32.69s`)
  Trino `619.31s` (`10m 19.31s`)
  Trino took about `2.27x` as long as Nitro overall by this aggregate measure

Notable movement in this refresh:
- Nitro was rerun end to end after fixing the `Q33` grouped-state pooling regression; `Q33` no longer OOMs and improved from `23129.801 ms/op` to `21488.560 ms/op`.
- The new Nitro rerun also moved a broad set of queries in the right direction, including `Q21-Q24`, `Q29`, `Q30`, `Q31`, `Q32`, `Q36`, and the distinct-heavy queries.
- `Q11`, `Q12`, `Q15`, `Q16`, `Q19`, `Q28`, and `Q32` all flipped to Nitro relative to the older forked comparison.
- Only four near-ties remain on the Trino side in the current best-known mix: `Q7`, `Q25`, `Q26`, and `Q27`.

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
| Q0 | 6501.028 | 36092.080 | 5.55 | Nitro |
| Q1 | 34.717 | 388.604 | 11.19 | Nitro |
| Q2 | 190.829 | 534.153 | 2.80 | Nitro |
| Q3 | 507.219 | 1371.200 | 2.70 | Nitro |
| Q4 | 362.553 | 606.067 | 1.67 | Nitro |
| Q5 | 2320.763 | 3612.037 | 1.56 | Nitro |
| Q6 | 5504.464 | 7771.508 | 1.41 | Nitro |
| Q7 | 615.442 | 534.487 | 0.87 | Trino |
| Q8 | 184.739 | 521.405 | 2.82 | Nitro |
| Q9 | 7121.719 | 7887.137 | 1.11 | Nitro |
| Q10 | 9835.632 | 10147.615 | 1.03 | Nitro |
| Q11 | 1263.558 | 1415.871 | 1.12 | Nitro |
| Q12 | 1532.811 | 1688.791 | 1.10 | Nitro |
| Q13 | 5114.597 | 6252.828 | 1.22 | Nitro |
| Q14 | 8052.762 | 11227.541 | 1.39 | Nitro |
| Q15 | 5822.771 | 6274.031 | 1.08 | Nitro |
| Q16 | 3737.844 | 4077.531 | 1.09 | Nitro |
| Q17 | 14075.188 | 16129.625 | 1.15 | Nitro |
| Q18 | 13100.601 | 14831.865 | 1.13 | Nitro |
| Q19 | 20657.253 | 22661.311 | 1.10 | Nitro |
| Q20 | 328.007 | 594.154 | 1.81 | Nitro |
| Q21 | 7893.450 | 14408.644 | 1.83 | Nitro |
| Q22 | 7715.648 | 8400.803 | 1.09 | Nitro |
| Q23 | 10274.878 | 20372.006 | 1.98 | Nitro |
| Q24 | 9370.791 | 45711.398 | 4.88 | Nitro |
| Q25 | 2758.597 | 2660.568 | 0.96 | Trino |
| Q26 | 2524.199 | 2419.228 | 0.96 | Trino |
| Q27 | 2889.928 | 2668.373 | 0.92 | Trino |
| Q28 | 9167.015 | 10135.586 | 1.11 | Nitro |
| Q29 | 25733.999 | 186324.703 | 7.24 | Nitro |
| Q30 | 8047.568 | 29580.280 | 3.68 | Nitro |
| Q31 | 4306.866 | 5248.066 | 1.22 | Nitro |
| Q32 | 5267.045 | 5600.837 | 1.06 | Nitro |
| Q33 | 21488.560 | 24454.002 | 1.14 | Nitro |
| Q34 | 20148.270 | 29231.639 | 1.45 | Nitro |
| Q35 | 19838.456 | 31645.996 | 1.60 | Nitro |
| Q36 | 4865.338 | 11685.647 | 2.40 | Nitro |
| Q37 | 494.473 | 6060.612 | 12.26 | Nitro |
| Q38 | 442.971 | 6349.352 | 14.33 | Nitro |
| Q39 | 322.178 | 5724.804 | 17.77 | Nitro |
| Q40 | 743.821 | 10166.418 | 13.67 | Nitro |
| Q41 | 553.177 | 2031.502 | 3.67 | Nitro |
| Q42 | 552.225 | 1985.264 | 3.60 | Nitro |
| Q43 | 421.176 | 1827.496 | 4.34 | Nitro |

Queries where Trino is faster in the current best-known per-query mix:
- Q7
- Q25
- Q26
- Q27
