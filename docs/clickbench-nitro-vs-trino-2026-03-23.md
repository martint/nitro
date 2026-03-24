# ClickBench Nitro vs Trino Benchmark Snapshot

Date: 2026-03-23

This snapshot compares the Nitro ClickBench harness and the separate hand-built Trino 479 ClickBench harness on the real parquet `hits` dataset at `/Users/martin/tmp/clickbench`.

Method:
- JMH same-VM runs (`-f 0`) for both harnesses
- `-wi 0 -i 1 -r 1ms -w 1ms`
- Nitro full pass at `-Xmx8g`, with `Q29` and `Q33` rerun at `-Xmx16g`
- Trino full pass at `-Xmx16g` with `-Dnitro.clickbench.trino.queryMaxMemoryGigabytes=8`

Caveats:
- These are directional engineering numbers, not publication-grade benchmark results.
- Same-VM JMH runs are useful for consistent side-by-side comparison in this environment, but they are not as clean as forked runs.

Summary:
- Nitro faster on 34 / 44 queries
- Trino faster on 10 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.72x`

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 8399.749 | 45479.558 | 5.41 | Nitro |
| Q1 | 257.760 | 2801.305 | 10.87 | Nitro |
| Q2 | 1742.525 | 3994.064 | 2.29 | Nitro |
| Q3 | 1263.502 | 5843.483 | 4.62 | Nitro |
| Q4 | 737.584 | 3386.833 | 4.59 | Nitro |
| Q5 | 6548.051 | 8073.746 | 1.23 | Nitro |
| Q6 | 8194.145 | 12487.015 | 1.52 | Nitro |
| Q7 | 622.083 | 3479.045 | 5.59 | Nitro |
| Q8 | 1427.948 | 4277.684 | 3.00 | Nitro |
| Q9 | 12843.264 | 15311.996 | 1.19 | Nitro |
| Q10 | 15135.161 | 17590.474 | 1.16 | Nitro |
| Q11 | 6857.850 | 7832.176 | 1.14 | Nitro |
| Q12 | 7185.276 | 7912.132 | 1.10 | Nitro |
| Q13 | 10579.413 | 11045.585 | 1.04 | Nitro |
| Q14 | 13900.829 | 18414.066 | 1.32 | Nitro |
| Q15 | 10975.422 | 11553.444 | 1.05 | Nitro |
| Q16 | 7541.121 | 7254.589 | 0.96 | Trino |
| Q17 | 15138.320 | 19004.376 | 1.26 | Nitro |
| Q18 | 14137.505 | 17292.988 | 1.22 | Nitro |
| Q19 | 26576.189 | 29834.232 | 1.12 | Nitro |
| Q20 | 1391.609 | 3078.938 | 2.21 | Nitro |
| Q21 | 15235.368 | 12660.336 | 0.83 | Trino |
| Q22 | 15639.835 | 11311.877 | 0.72 | Trino |
| Q23 | 18029.859 | 19335.975 | 1.07 | Nitro |
| Q24 | 21030.559 | 49813.842 | 2.37 | Nitro |
| Q25 | 7469.489 | 7505.273 | 1.00 | Nitro |
| Q26 | 6884.150 | 6486.725 | 0.94 | Trino |
| Q27 | 7539.343 | 7523.393 | 1.00 | Trino |
| Q28 | 19262.401 | 15451.916 | 0.80 | Trino |
| Q29 | 42457.026 | 191353.505 | 4.51 | Nitro |
| Q30 | 96131.398 | 50171.095 | 0.52 | Trino |
| Q31 | 14225.279 | 11680.622 | 0.82 | Trino |
| Q32 | 13196.394 | 12157.007 | 0.92 | Trino |
| Q33 | 25851.656 | 42797.099 | 1.66 | Nitro |
| Q34 | 23842.047 | 32189.978 | 1.35 | Nitro |
| Q35 | 23575.714 | 36064.647 | 1.53 | Nitro |
| Q36 | 23397.840 | 17953.210 | 0.77 | Trino |
| Q37 | 2978.667 | 9911.645 | 3.33 | Nitro |
| Q38 | 2634.638 | 10569.239 | 4.01 | Nitro |
| Q39 | 2837.884 | 10682.341 | 3.76 | Nitro |
| Q40 | 3664.164 | 16077.144 | 4.39 | Nitro |
| Q41 | 2537.918 | 7568.878 | 2.98 | Nitro |
| Q42 | 2757.285 | 7485.537 | 2.71 | Nitro |
| Q43 | 2809.024 | 7196.346 | 2.56 | Nitro |

Queries where Trino is currently faster:
- Q16
- Q21
- Q22
- Q26
- Q27
- Q28
- Q30
- Q31
- Q32
- Q36
