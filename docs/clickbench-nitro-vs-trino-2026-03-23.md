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
- Nitro faster on 24 / 44 queries
- Trino faster on 20 / 44 queries
- Geometric mean of `Trino / Nitro`: about `1.33x`

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 7142.337 | 40832.840 | 5.72 | Nitro |
| Q1 | 55.180 | 228.502 | 4.14 | Nitro |
| Q2 | 1083.073 | 315.585 | 0.29 | Trino |
| Q3 | 594.597 | 1159.940 | 1.95 | Nitro |
| Q4 | 431.454 | 440.243 | 1.02 | Nitro |
| Q5 | 5617.528 | 2954.958 | 0.53 | Trino |
| Q6 | 7502.498 | 8242.355 | 1.10 | Nitro |
| Q7 | 294.016 | 843.650 | 2.87 | Nitro |
| Q8 | 882.415 | 299.134 | 0.34 | Trino |
| Q9 | 10092.652 | 9250.892 | 0.92 | Trino |
| Q10 | 15200.173 | 10223.514 | 0.67 | Trino |
| Q11 | 1771.503 | 1282.541 | 0.72 | Trino |
| Q12 | 1763.489 | 1460.595 | 0.83 | Trino |
| Q13 | 5317.292 | 5692.917 | 1.07 | Nitro |
| Q14 | 8429.442 | 11139.992 | 1.32 | Nitro |
| Q15 | 5562.105 | 5782.757 | 1.04 | Nitro |
| Q16 | 6080.542 | 3709.264 | 0.61 | Trino |
| Q17 | 13464.739 | 15953.214 | 1.18 | Nitro |
| Q18 | 12751.348 | 16304.008 | 1.28 | Nitro |
| Q19 | 21753.931 | 23665.855 | 1.09 | Nitro |
| Q20 | 872.656 | 409.095 | 0.47 | Trino |
| Q21 | 11302.209 | 9259.913 | 0.82 | Trino |
| Q22 | 8016.050 | 7401.385 | 0.92 | Trino |
| Q23 | 10574.809 | 14650.584 | 1.39 | Nitro |
| Q24 | 12593.433 | 40491.565 | 3.22 | Nitro |
| Q25 | 2663.378 | 2543.892 | 0.96 | Trino |
| Q26 | 2340.205 | 2210.558 | 0.94 | Trino |
| Q27 | 2656.242 | 2465.093 | 0.93 | Trino |
| Q28 | 13578.636 | 10171.697 | 0.75 | Trino |
| Q29 | 32062.414 | 186006.944 | 5.80 | Nitro |
| Q30 | 36461.641 | 32195.574 | 0.88 | Trino |
| Q31 | 7549.694 | 5441.117 | 0.72 | Trino |
| Q32 | 6214.929 | 6096.571 | 0.98 | Trino |
| Q33 | 27567.155 | 32595.256 | 1.18 | Nitro |
| Q34 | 26362.058 | 27642.648 | 1.05 | Nitro |
| Q35 | 28810.234 | 28397.238 | 0.99 | Trino |
| Q36 | 17934.843 | 11679.527 | 0.65 | Trino |
| Q37 | 857.791 | 5921.152 | 6.90 | Nitro |
| Q38 | 695.819 | 6283.221 | 9.03 | Nitro |
| Q39 | 1033.098 | 5398.055 | 5.23 | Nitro |
| Q40 | 1066.670 | 9986.543 | 9.36 | Nitro |
| Q41 | 927.954 | 1788.176 | 1.93 | Nitro |
| Q42 | 871.612 | 1700.413 | 1.95 | Nitro |
| Q43 | 785.671 | 1482.333 | 1.89 | Nitro |

Queries where Trino is currently faster:
- Q2
- Q5
- Q8
- Q9
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
- Q35
- Q36
