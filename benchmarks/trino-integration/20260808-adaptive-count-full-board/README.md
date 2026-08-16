# Adaptive-count post-change full board

This is the complete 168-variant board after the adaptive compact COUNT/AVG-count state change.  Every query passed
exact result comparison.  Measurements used the native Nitro Parquet reader, five warmups, three alternating
measurements, a fresh 12 GiB JVM per suite, an 8 GiB query-memory limit, thread-allocation accounting, and sampled
query peak memory.

| suite | queries | wall geomean | CPU geomean | wall wins | CPU wins | allocation geomean |
|---|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.732 | 0.685 | 21 | 22 | 0.178 |
| TPC-DS | 103 | 0.585 | 0.405 | 93 | 101 | 0.182 |
| ClickBench | 43 | 0.864 | 0.689 | 32 | 41 | 0.314 |

Weighted wall/CPU ratios are 0.668/0.551 for TPC-DS and 0.759/0.764 for ClickBench.  Maximum sampled query peaks are
985 MiB Nitro versus 1,073 MiB Trino on TPC-H q09, 1,901 MiB Nitro on TPC-DS q23a versus Trino's suite maximum of
1,708 MiB on q23b, and 4,205 MiB Nitro versus 4,694 MiB Trino on ClickBench q33.  The count-state change therefore
moves Nitro's largest ClickBench peak below Trino's while keeping aggregate CPU stable.

The raw suite-order CPU candidates have already been bounded by fresh warmed controls: TPC-DS q20 reverses when
isolated and q23a measures 0.941x CPU; ClickBench q19 is near parity and q40 clears.  The isolated q21 artifact in this
directory is the pre-bound-LIKE baseline (0.928x CPU) and motivated the subsequent function-kernel correction under
`../20260808-bound-like-contains/`.

`tpch.xml`, `tpcds.xml`, and `clickbench.xml` contain the complete measurements and operator/native metrics.  No JFR,
heap dump, or Kata artifact was created.
