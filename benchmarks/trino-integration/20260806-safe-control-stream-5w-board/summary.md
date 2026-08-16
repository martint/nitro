# Warmed Nitro/Trino board — 2026-08-06

Configuration: SF10 Parquet inputs, both engines in one runner, five warmups and three measurements per query,
12 GiB JVM heap, ClickBench query memory capped at 8 GiB. Operator and plan-node CPU, query/heap peak memory, and
native-source counts are retained in the suite logs.

| Suite | Queries | Nitro/Trino wall geomean | Prior wall | Nitro/Trino CPU geomean | Prior CPU |
|---|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.721x | 0.734x | 0.678x | 0.668x |
| TPC-DS (raw 5W) | 103 | 0.603x | 0.595x | 0.414x | 0.402x |
| TPC-DS (adequately warmed controls) | 103 | 0.590x | 0.595x | 0.401x | 0.402x |
| ClickBench (raw 5W) | 43 | 0.897x | 0.882x | 0.700x | 0.701x |
| ClickBench (q30 adequately warmed) | 43 | 0.880x | 0.882x | 0.685x | 0.701x |

Maximum measured query peak memory:

| Suite | Nitro | Trino |
|---|---:|---:|
| TPC-H | q18, 1,075.8 MiB | q09, 1,271.3 MiB |
| TPC-DS | q67, 2,879.9 MiB | q23a, 1,772.6 MiB |
| ClickBench | q33, 5,921.9 MiB | q33, 4,945.5 MiB |

The raw TPC-DS CPU regressions clear under adequately warmed controls: q20 is 0.105x, q22 is 0.692x, q23a is
0.971x, and q23b is 0.909x. q67 remains at practical CPU parity (0.96--1.05x across controls) while retaining a
roughly 1.12--1.16x wall-time gap. ClickBench q19 and q40 likewise have controlled CPU ratios near 1.01x. TPC-H
has no query above 1.0x CPU or wall. The corrected all-suite geomeans are 0.671x wall and 0.493x CPU over 168 queries.

Five warmups are insufficient for ClickBench q30 when both engines share the full-suite JVM. With q01--q29 history
preserved and q30 increased to 15 warmups and three measurements, q30 is 1.191 s wall / 8.138 CPU-s for Nitro versus
2.956 s / 21.325 CPU-s for Trino (0.403x wall / 0.382x CPU). Substituting that adequately warmed result changes the
ClickBench geomeans to 0.880x wall and 0.685x CPU. The raw five-warmup row is retained so the correction is explicit.
