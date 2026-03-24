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

Notable movement in this refresh:
- `Q31`, `Q32`, and `Q36` flipped to Nitro after the fixed-width grouped-key specialization pass.
- A few near-ties moved the other way under same-VM noise, especially `Q4`, `Q13`, `Q15`, and `Q34`.

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 10146.545 | 40832.840 | 4.02 | Nitro |
| Q1 | 95.660 | 228.502 | 2.39 | Nitro |
| Q2 | 821.394 | 315.585 | 0.38 | Trino |
| Q3 | 599.817 | 1159.940 | 1.93 | Nitro |
| Q4 | 491.810 | 440.243 | 0.90 | Trino |
| Q5 | 6718.176 | 2954.958 | 0.44 | Trino |
| Q6 | 7103.876 | 8242.355 | 1.16 | Nitro |
| Q7 | 292.072 | 843.650 | 2.89 | Nitro |
| Q8 | 634.477 | 299.134 | 0.47 | Trino |
| Q9 | 9771.818 | 9250.892 | 0.95 | Trino |
| Q10 | 14987.369 | 10223.514 | 0.68 | Trino |
| Q11 | 2041.651 | 1282.541 | 0.63 | Trino |
| Q12 | 1863.168 | 1460.595 | 0.78 | Trino |
| Q13 | 5703.337 | 5692.917 | 1.00 | Trino |
| Q14 | 9258.626 | 11139.992 | 1.20 | Nitro |
| Q15 | 6097.537 | 5782.757 | 0.95 | Trino |
| Q16 | 3817.287 | 3709.264 | 0.97 | Trino |
| Q17 | 14199.152 | 15953.214 | 1.12 | Nitro |
| Q18 | 13437.909 | 16304.008 | 1.21 | Nitro |
| Q19 | 21850.334 | 23665.855 | 1.08 | Nitro |
| Q20 | 717.714 | 409.095 | 0.57 | Trino |
| Q21 | 14266.228 | 9259.913 | 0.65 | Trino |
| Q22 | 9720.258 | 7401.385 | 0.76 | Trino |
| Q23 | 13482.014 | 14650.584 | 1.09 | Nitro |
| Q24 | 15449.337 | 40491.565 | 2.62 | Nitro |
| Q25 | 3384.005 | 2543.892 | 0.75 | Trino |
| Q26 | 2539.367 | 2210.558 | 0.87 | Trino |
| Q27 | 2949.480 | 2465.093 | 0.84 | Trino |
| Q28 | 13419.497 | 10171.697 | 0.76 | Trino |
| Q29 | 32787.966 | 186006.944 | 5.67 | Nitro |
| Q30 | 37877.879 | 32195.574 | 0.85 | Trino |
| Q31 | 5226.695 | 5441.117 | 1.04 | Nitro |
| Q32 | 5895.141 | 6096.571 | 1.03 | Nitro |
| Q33 | 22076.857 | 32595.256 | 1.48 | Nitro |
| Q34 | 27868.011 | 27642.648 | 0.99 | Trino |
| Q35 | 27408.925 | 28397.238 | 1.04 | Nitro |
| Q36 | 6883.540 | 11679.527 | 1.70 | Nitro |
| Q37 | 924.271 | 5921.152 | 6.41 | Nitro |
| Q38 | 682.003 | 6283.221 | 9.21 | Nitro |
| Q39 | 879.458 | 5398.055 | 6.14 | Nitro |
| Q40 | 1042.458 | 9986.543 | 9.58 | Nitro |
| Q41 | 866.297 | 1788.176 | 2.06 | Nitro |
| Q42 | 862.865 | 1700.413 | 1.97 | Nitro |
| Q43 | 718.243 | 1482.333 | 2.06 | Nitro |

Queries where Trino is currently faster:
- Q2
- Q4
- Q5
- Q8
- Q9
- Q10
- Q11
- Q12
- Q13
- Q15
- Q16
- Q20
- Q21
- Q22
- Q25
- Q26
- Q27
- Q28
- Q30
- Q34
