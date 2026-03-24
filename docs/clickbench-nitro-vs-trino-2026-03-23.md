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
- Nitro faster on 35 / 44 queries
- Trino faster on 9 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.64x`

Notable movement in this refresh:
- `Q5` flipped decisively to Nitro after adding the primitive single-column distinct fast path (`2579.532 ms/op` vs Trino `3361.037 ms/op`).
- `Q15`, `Q18`, and `Q26` also landed on the Nitro side in this run, shrinking the remaining Trino-faster set to mostly near-ties plus the simple-filter and string-kernel holdouts.
- The main remaining Trino wins are now concentrated in `Q2`, `Q20`, `Q21`, and a smaller set of near-ties (`Q10`, `Q11`, `Q12`, `Q16`, `Q25`, `Q30`).

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 7965.608 | 37448.046 | 4.70 | Nitro |
| Q1 | 55.665 | 170.733 | 3.07 | Nitro |
| Q2 | 632.449 | 340.900 | 0.54 | Trino |
| Q3 | 579.930 | 1315.365 | 2.27 | Nitro |
| Q4 | 473.467 | 494.650 | 1.04 | Nitro |
| Q5 | 2579.532 | 3361.037 | 1.30 | Nitro |
| Q6 | 6460.841 | 8153.642 | 1.26 | Nitro |
| Q7 | 304.349 | 850.790 | 2.80 | Nitro |
| Q8 | 200.500 | 332.514 | 1.66 | Nitro |
| Q9 | 9948.163 | 10499.804 | 1.06 | Nitro |
| Q10 | 10447.224 | 10027.734 | 0.96 | Trino |
| Q11 | 2054.205 | 1226.873 | 0.60 | Trino |
| Q12 | 1866.079 | 1574.964 | 0.84 | Trino |
| Q13 | 5473.176 | 6142.611 | 1.12 | Nitro |
| Q14 | 8093.980 | 11369.475 | 1.40 | Nitro |
| Q15 | 5832.827 | 6251.278 | 1.07 | Nitro |
| Q16 | 3920.319 | 3807.568 | 0.97 | Trino |
| Q17 | 13907.179 | 16158.761 | 1.16 | Nitro |
| Q18 | 13168.296 | 14830.795 | 1.13 | Nitro |
| Q19 | 21985.167 | 24588.471 | 1.12 | Nitro |
| Q20 | 1070.402 | 436.560 | 0.41 | Trino |
| Q21 | 11554.384 | 10163.544 | 0.88 | Trino |
| Q22 | 7655.839 | 8264.575 | 1.08 | Nitro |
| Q23 | 10755.984 | 15195.930 | 1.41 | Nitro |
| Q24 | 12886.335 | 42953.464 | 3.33 | Nitro |
| Q25 | 2660.771 | 2658.631 | 1.00 | Trino |
| Q26 | 2298.511 | 2302.381 | 1.00 | Nitro |
| Q27 | 2645.263 | 2897.078 | 1.10 | Nitro |
| Q28 | 10692.660 | 11101.507 | 1.04 | Nitro |
| Q29 | 30234.854 | 201697.109 | 6.67 | Nitro |
| Q30 | 34385.574 | 32695.660 | 0.95 | Trino |
| Q31 | 5128.611 | 6131.708 | 1.20 | Nitro |
| Q32 | 5482.278 | 7463.231 | 1.36 | Nitro |
| Q33 | 20730.033 | 34440.033 | 1.66 | Nitro |
| Q34 | 25693.650 | 29751.678 | 1.16 | Nitro |
| Q35 | 24770.686 | 31428.122 | 1.27 | Nitro |
| Q36 | 6329.294 | 12865.387 | 2.03 | Nitro |
| Q37 | 837.146 | 6742.695 | 8.05 | Nitro |
| Q38 | 690.756 | 6958.556 | 10.07 | Nitro |
| Q39 | 372.416 | 5902.913 | 15.85 | Nitro |
| Q40 | 1024.795 | 11255.780 | 10.98 | Nitro |
| Q41 | 841.752 | 1933.960 | 2.30 | Nitro |
| Q42 | 806.614 | 1804.502 | 2.24 | Nitro |
| Q43 | 683.342 | 1605.206 | 2.35 | Nitro |

Queries where Trino is currently faster:
- Q2
- Q10
- Q11
- Q12
- Q16
- Q20
- Q21
- Q25
- Q30
