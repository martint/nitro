# ClickBench Nitro vs Trino Benchmark Snapshot

Date: 2026-03-24

This snapshot compares the Nitro ClickBench harness and the separate hand-built Trino 479 ClickBench harness on the real parquet `hits` dataset at `/Users/martin/tmp/clickbench`.

Method:
- JMH same-VM runs (`-f 0`) for both harnesses
- `-wi 0 -i 1 -r 1ms -w 1ms`
- Nitro full pass at `-Xmx16g`
- Trino full pass at `-Xmx16g` with `-Dnitro.clickbench.trino.queryMaxMemoryGigabytes=8`
- Both harnesses used `-Dnitro.trino.scan.maxBatchRows=10000`

Caveats:
- These are directional engineering numbers, not publication-grade benchmark results.
- Same-VM JMH runs are useful for consistent side-by-side comparison in this environment, but they are not as clean as forked runs.

Summary:
- Nitro faster on 27 / 44 queries
- Trino faster on 17 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.37x`

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 8121.269 | 35331.031 | 4.35 | Nitro |
| Q1 | 52.369 | 166.328 | 3.18 | Nitro |
| Q2 | 641.125 | 320.538 | 0.50 | Trino |
| Q3 | 602.828 | 1116.597 | 1.85 | Nitro |
| Q4 | 469.122 | 474.574 | 1.01 | Nitro |
| Q5 | 5858.225 | 3226.827 | 0.55 | Trino |
| Q6 | 7333.447 | 8047.126 | 1.10 | Nitro |
| Q7 | 281.328 | 840.866 | 2.99 | Nitro |
| Q8 | 280.809 | 362.480 | 1.29 | Nitro |
| Q9 | 8268.428 | 8933.311 | 1.08 | Nitro |
| Q10 | 16971.153 | 9473.874 | 0.56 | Trino |
| Q11 | 1963.150 | 1157.048 | 0.59 | Trino |
| Q12 | 1761.827 | 1445.465 | 0.82 | Trino |
| Q13 | 5097.511 | 5601.021 | 1.10 | Nitro |
| Q14 | 8191.371 | 10563.462 | 1.29 | Nitro |
| Q15 | 5581.377 | 5665.325 | 1.02 | Nitro |
| Q16 | 6770.120 | 3621.907 | 0.53 | Trino |
| Q17 | 12721.667 | 15228.843 | 1.20 | Nitro |
| Q18 | 11867.831 | 14510.627 | 1.22 | Nitro |
| Q19 | 21157.298 | 23578.074 | 1.11 | Nitro |
| Q20 | 762.844 | 391.806 | 0.51 | Trino |
| Q21 | 11658.964 | 9527.923 | 0.82 | Trino |
| Q22 | 7732.060 | 7315.060 | 0.95 | Trino |
| Q23 | 10283.812 | 14389.155 | 1.40 | Nitro |
| Q24 | 12807.723 | 37822.213 | 2.95 | Nitro |
| Q25 | 3033.070 | 2325.208 | 0.77 | Trino |
| Q26 | 2714.079 | 2114.384 | 0.78 | Trino |
| Q27 | 3021.528 | 2446.438 | 0.81 | Trino |
| Q28 | 13899.184 | 9631.248 | 0.69 | Trino |
| Q29 | 32219.947 | 179999.126 | 5.59 | Nitro |
| Q30 | 34406.326 | 29137.934 | 0.85 | Trino |
| Q31 | 7368.464 | 5119.410 | 0.69 | Trino |
| Q32 | 6760.783 | 5959.674 | 0.88 | Trino |
| Q33 | 29716.237 | 32822.015 | 1.10 | Nitro |
| Q34 | 24561.257 | 25779.075 | 1.05 | Nitro |
| Q35 | 23060.893 | 27145.866 | 1.18 | Nitro |
| Q36 | 15697.184 | 10746.249 | 0.68 | Trino |
| Q37 | 854.179 | 5609.478 | 6.57 | Nitro |
| Q38 | 629.757 | 6298.602 | 10.00 | Nitro |
| Q39 | 476.542 | 5249.992 | 11.02 | Nitro |
| Q40 | 1086.696 | 9958.553 | 9.16 | Nitro |
| Q41 | 836.624 | 1875.963 | 2.24 | Nitro |
| Q42 | 826.251 | 1803.682 | 2.18 | Nitro |
| Q43 | 706.327 | 1517.778 | 2.15 | Nitro |

Queries where Trino is currently faster:
- Q2
- Q5
- Q10
- Q11
- Q12
- Q16
- Q20
- Q21
- Q22
- Q25
- Q26
- Q27
- Q28
- Q30
- Q31
- Q32
- Q36
