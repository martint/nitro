# ClickBench Nitro vs Trino Benchmark Snapshot

Date: 2026-03-23

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
- Geometric mean of `Trino / Nitro`: about `1.30x`

| Query | Nitro ms/op | Trino ms/op | Trino/Nitro | Faster |
|---|---:|---:|---:|---|
| Q0 | 7190.229 | 35659.396 | 4.96 | Nitro |
| Q1 | 62.881 | 160.063 | 2.55 | Nitro |
| Q2 | 1071.201 | 315.223 | 0.29 | Trino |
| Q3 | 660.019 | 1121.769 | 1.70 | Nitro |
| Q4 | 487.361 | 441.789 | 0.91 | Trino |
| Q5 | 6799.004 | 3063.289 | 0.45 | Trino |
| Q6 | 6434.271 | 7874.231 | 1.22 | Nitro |
| Q7 | 310.142 | 824.394 | 2.66 | Nitro |
| Q8 | 775.332 | 300.615 | 0.39 | Trino |
| Q9 | 11639.472 | 10034.038 | 0.86 | Trino |
| Q10 | 14025.458 | 9776.432 | 0.70 | Trino |
| Q11 | 1977.954 | 1156.640 | 0.58 | Trino |
| Q12 | 2011.439 | 1471.749 | 0.73 | Trino |
| Q13 | 5407.585 | 5628.336 | 1.04 | Nitro |
| Q14 | 8139.377 | 10831.057 | 1.33 | Nitro |
| Q15 | 5723.041 | 5840.850 | 1.02 | Nitro |
| Q16 | 7078.828 | 3774.747 | 0.53 | Trino |
| Q17 | 12476.036 | 15832.537 | 1.27 | Nitro |
| Q18 | 11130.224 | 14351.103 | 1.29 | Nitro |
| Q19 | 22018.091 | 22766.308 | 1.03 | Nitro |
| Q20 | 892.057 | 395.647 | 0.44 | Trino |
| Q21 | 11676.288 | 9366.666 | 0.80 | Trino |
| Q22 | 7716.444 | 7492.771 | 0.97 | Trino |
| Q23 | 10301.522 | 14537.171 | 1.41 | Nitro |
| Q24 | 12758.348 | 38518.911 | 3.02 | Nitro |
| Q25 | 2999.132 | 2482.955 | 0.83 | Trino |
| Q26 | 2746.753 | 2154.948 | 0.78 | Trino |
| Q27 | 3089.068 | 2523.310 | 0.82 | Trino |
| Q28 | 13912.748 | 9977.960 | 0.72 | Trino |
| Q29 | 29757.545 | 180229.448 | 6.06 | Nitro |
| Q30 | 34534.101 | 30014.968 | 0.87 | Trino |
| Q31 | 7093.540 | 5235.659 | 0.74 | Trino |
| Q32 | 6134.193 | 6055.258 | 0.99 | Trino |
| Q33 | 25764.256 | 31229.948 | 1.21 | Nitro |
| Q34 | 23924.726 | 27361.157 | 1.14 | Nitro |
| Q35 | 23049.193 | 28670.095 | 1.24 | Nitro |
| Q36 | 16299.752 | 12025.318 | 0.74 | Trino |
| Q37 | 847.259 | 5725.498 | 6.76 | Nitro |
| Q38 | 625.815 | 6335.655 | 10.12 | Nitro |
| Q39 | 948.373 | 5438.790 | 5.73 | Nitro |
| Q40 | 1027.925 | 9887.925 | 9.62 | Nitro |
| Q41 | 819.974 | 1782.005 | 2.17 | Nitro |
| Q42 | 845.865 | 1696.719 | 2.01 | Nitro |
| Q43 | 692.784 | 1465.166 | 2.11 | Nitro |

Queries where Trino is currently faster:
- Q2
- Q4
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
- Q36
