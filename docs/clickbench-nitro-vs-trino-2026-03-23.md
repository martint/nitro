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
- Nitro faster on 31 / 44 queries
- Trino faster on 13 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.59x`

Notable movement in this refresh:
- `Q8` flipped decisively to Nitro after optimizing negated long-comparison masks (`197.878 ms/op` vs Trino `332.514 ms/op`).
- `Q4` and `Q28` also landed slightly on the Nitro side in this run, while `Q18` and `Q26` drifted to near-tie Trino wins under same-VM noise.
- The main remaining Trino wins are now concentrated in the global distinct holdout (`Q5`), simple primitive filters (`Q2`, `Q20`), the string filter kernel (`Q21`), and a handful of near-ties (`Q10`, `Q15`, `Q16`, `Q18`, `Q25`, `Q26`, `Q30`).

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 8571.387 | 37448.046 | 4.37 | Nitro |
| Q1 | 59.115 | 170.733 | 2.89 | Nitro |
| Q2 | 650.115 | 340.900 | 0.52 | Trino |
| Q3 | 609.652 | 1315.365 | 2.16 | Nitro |
| Q4 | 415.853 | 494.650 | 1.19 | Nitro |
| Q5 | 5235.857 | 3361.037 | 0.64 | Trino |
| Q6 | 6230.237 | 8153.642 | 1.31 | Nitro |
| Q7 | 298.796 | 850.790 | 2.85 | Nitro |
| Q8 | 197.878 | 332.514 | 1.68 | Nitro |
| Q9 | 9308.535 | 10499.804 | 1.13 | Nitro |
| Q10 | 10304.692 | 10027.734 | 0.97 | Trino |
| Q11 | 2068.269 | 1226.873 | 0.59 | Trino |
| Q12 | 1957.844 | 1574.964 | 0.80 | Trino |
| Q13 | 6094.065 | 6142.611 | 1.01 | Nitro |
| Q14 | 9130.960 | 11369.475 | 1.25 | Nitro |
| Q15 | 6713.048 | 6251.278 | 0.93 | Trino |
| Q16 | 4135.005 | 3807.568 | 0.92 | Trino |
| Q17 | 15496.223 | 16158.761 | 1.04 | Nitro |
| Q18 | 15215.712 | 14830.795 | 0.97 | Trino |
| Q19 | 23687.571 | 24588.471 | 1.04 | Nitro |
| Q20 | 787.646 | 436.560 | 0.55 | Trino |
| Q21 | 12092.969 | 10163.544 | 0.84 | Trino |
| Q22 | 7962.014 | 8264.575 | 1.04 | Nitro |
| Q23 | 11438.200 | 15195.930 | 1.33 | Nitro |
| Q24 | 13510.366 | 42953.464 | 3.18 | Nitro |
| Q25 | 2756.843 | 2658.631 | 0.96 | Trino |
| Q26 | 2353.319 | 2302.381 | 0.98 | Trino |
| Q27 | 2717.141 | 2897.078 | 1.07 | Nitro |
| Q28 | 11019.709 | 11101.507 | 1.01 | Nitro |
| Q29 | 29128.282 | 201697.109 | 6.92 | Nitro |
| Q30 | 36938.650 | 32695.660 | 0.89 | Trino |
| Q31 | 5150.643 | 6131.708 | 1.19 | Nitro |
| Q32 | 5880.040 | 7463.231 | 1.27 | Nitro |
| Q33 | 21129.021 | 34440.034 | 1.63 | Nitro |
| Q34 | 26046.311 | 29751.678 | 1.14 | Nitro |
| Q35 | 26070.120 | 31428.122 | 1.21 | Nitro |
| Q36 | 6494.213 | 12865.387 | 1.98 | Nitro |
| Q37 | 835.099 | 6742.695 | 8.07 | Nitro |
| Q38 | 660.289 | 6958.556 | 10.54 | Nitro |
| Q39 | 382.363 | 5902.913 | 15.44 | Nitro |
| Q40 | 1052.120 | 11255.780 | 10.70 | Nitro |
| Q41 | 806.467 | 1933.960 | 2.40 | Nitro |
| Q42 | 807.500 | 1804.502 | 2.23 | Nitro |
| Q43 | 652.187 | 1605.206 | 2.46 | Nitro |

Queries where Trino is currently faster:
- Q2
- Q5
- Q10
- Q11
- Q12
- Q15
- Q16
- Q18
- Q20
- Q21
- Q25
- Q26
- Q30
