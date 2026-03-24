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
- Nitro faster on 30 / 44 queries
- Trino faster on 14 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.51x`

Notable movement in this refresh:
- `Q10` is now nearly closed after sharing grouped distinct selection across accumulators (`10191.729 ms/op` vs Trino `10027.734 ms/op`).
- `Q9` remains on the Nitro side, and `Q22`, `Q26`, `Q27`, and `Q34` also ended up Nitro-faster in this run.
- The main remaining Trino wins are now concentrated in the simple primitive filter bucket (`Q2`, `Q4`, `Q8`, `Q20`), the global distinct holdout (`Q5`), and a few near-ties (`Q10`, `Q15`, `Q16`, `Q25`, `Q28`, `Q30`).

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 7681.660 | 37448.046 | 4.87 | Nitro |
| Q1 | 53.644 | 170.733 | 3.18 | Nitro |
| Q2 | 1066.698 | 340.900 | 0.32 | Trino |
| Q3 | 613.815 | 1315.365 | 2.14 | Nitro |
| Q4 | 511.019 | 494.650 | 0.97 | Trino |
| Q5 | 6328.889 | 3361.037 | 0.53 | Trino |
| Q6 | 6808.483 | 8153.642 | 1.20 | Nitro |
| Q7 | 325.640 | 850.790 | 2.61 | Nitro |
| Q8 | 838.072 | 332.514 | 0.40 | Trino |
| Q9 | 9316.535 | 10499.804 | 1.13 | Nitro |
| Q10 | 10191.729 | 10027.734 | 0.98 | Trino |
| Q11 | 1971.468 | 1226.873 | 0.62 | Trino |
| Q12 | 1847.562 | 1574.964 | 0.85 | Trino |
| Q13 | 6013.284 | 6142.611 | 1.02 | Nitro |
| Q14 | 9099.235 | 11369.475 | 1.25 | Nitro |
| Q15 | 6335.910 | 6251.278 | 0.99 | Trino |
| Q16 | 3850.435 | 3807.568 | 0.99 | Trino |
| Q17 | 13086.967 | 16158.761 | 1.23 | Nitro |
| Q18 | 12392.182 | 14830.795 | 1.20 | Nitro |
| Q19 | 21934.815 | 24588.471 | 1.12 | Nitro |
| Q20 | 747.696 | 436.560 | 0.58 | Trino |
| Q21 | 12135.885 | 10163.544 | 0.84 | Trino |
| Q22 | 7839.942 | 8264.575 | 1.05 | Nitro |
| Q23 | 10752.036 | 15195.930 | 1.41 | Nitro |
| Q24 | 13147.324 | 42953.464 | 3.27 | Nitro |
| Q25 | 2708.006 | 2658.631 | 0.98 | Trino |
| Q26 | 2280.872 | 2302.381 | 1.01 | Nitro |
| Q27 | 2666.641 | 2897.078 | 1.09 | Nitro |
| Q28 | 11406.885 | 11101.507 | 0.97 | Trino |
| Q29 | 33757.585 | 201697.109 | 5.97 | Nitro |
| Q30 | 37124.702 | 32695.660 | 0.88 | Trino |
| Q31 | 4902.866 | 6131.708 | 1.25 | Nitro |
| Q32 | 5753.314 | 7463.231 | 1.30 | Nitro |
| Q33 | 21110.736 | 34440.034 | 1.63 | Nitro |
| Q34 | 27034.509 | 29751.678 | 1.10 | Nitro |
| Q35 | 26452.700 | 31428.122 | 1.19 | Nitro |
| Q36 | 6567.016 | 12865.387 | 1.96 | Nitro |
| Q37 | 841.814 | 6742.695 | 8.01 | Nitro |
| Q38 | 657.773 | 6958.556 | 10.58 | Nitro |
| Q39 | 604.744 | 5902.913 | 9.76 | Nitro |
| Q40 | 1045.581 | 11255.780 | 10.77 | Nitro |
| Q41 | 853.740 | 1933.960 | 2.27 | Nitro |
| Q42 | 881.987 | 1804.502 | 2.05 | Nitro |
| Q43 | 709.886 | 1605.206 | 2.26 | Nitro |

Queries where Trino is currently faster:
- Q2
- Q4
- Q5
- Q8
- Q10
- Q11
- Q12
- Q15
- Q16
- Q20
- Q21
- Q25
- Q28
- Q30
