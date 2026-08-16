# Post-native-layout warmed Nitro/Trino board — 2026-08-06

Configuration: SF10 Parquet inputs, both engines in one runner, five warmups and three measurements per query,
12 GiB JVM heap, and an 8 GiB ClickBench query-memory cap. The raw suite logs retain operator CPU, thread
allocation, query/heap peak memory, native-source counts, and boundary metrics.

| Suite | Queries | Nitro/Trino wall geomean | Prior board | Nitro/Trino CPU geomean | Prior board | Wall total | CPU total |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.734x | 0.721x | 0.676x | 0.678x | 0.714x | 0.720x |
| TPC-DS | 103 | 0.607x | 0.603x | 0.410x | 0.414x | 0.711x | 0.577x |
| ClickBench | 43 | 0.872x | 0.897x | 0.668x | 0.700x | 0.751x | 0.757x |
| All suites | 168 | 0.683x | 0.671x corrected | 0.496x | 0.493x corrected | — | — |

The new full board is effectively CPU-neutral versus the prior board overall. ClickBench improved materially;
TPC-H wall and TPC-DS wall moved slightly against Nitro while their CPU ratios remained within roughly one point.
The prior all-suite comparison used adequately warmed controls for known history-sensitive rows, while the new
all-suite row above is the uncorrected five-warmup sweep.

Maximum measured query peak memory:

| Suite | Nitro | Trino |
|---|---:|---:|
| TPC-H | q18, 1,017.5 MiB | q09, 1,066.9 MiB |
| TPC-DS | q23a, 2,188.1 MiB | q23a, 1,809.2 MiB |
| ClickBench | q33, 5,921.9 MiB | q33, 4,765.2 MiB |

Raw CPU regressions and warmed controls:

| Suite | Query | CPU ratio | Wall ratio | Notes |
|---|---|---:|---:|---|
| TPC-DS | q20 | 1.233x | 0.921x | Fifteen-warmup analyzed control confirms 1.162x CPU, but the sample remains only 74/86 ms. |
| TPC-DS | q22 | 1.219x | 1.038x | Fifteen-warmup analyzed control confirms 1.183x CPU. The earlier 0.692x result omitted `ANALYZE` and used a different plan. |
| TPC-DS | q23a | 1.097x | 1.178x | Fifteen-warmup analyzed control clears this at 0.960x CPU and 1.034x wall. |
| ClickBench | q19 | 1.056x | 0.806x | CPU near parity; prior controlled result was about 1.01x. |
| ClickBench | q40 | 1.030x | 1.086x | Small 438/451 ms CPU sample. |
| ClickBench | q05 | 1.019x | 1.168x | CPU parity; heap history affects wall time. |

TPC-H has no CPU regression. Its two wall regressions, q12 at 1.057x and q16 at 1.014x, retain CPU wins of
0.824x and 0.918x respectively. Most other wall-only regressions are likewise scheduling, concurrency, heap-history,
or short-query effects rather than excess operator CPU. ClickBench q43 is the clearest example: 2.012x wall on a
33--67 ms query despite 0.854x CPU.

The first TPC-DS pass stopped at q72 on a one-off result mismatch. The mismatch did not reproduce with q71→q72,
q67→q72, or the exact five-warmup/three-measurement q67 precondition. The resumed q72 passed at 0.238x wall and
0.197x CPU. q72 is therefore retained from the successful resumed q71--q99 run; no production change was made for
the transient failure.

The q20--q23a control is in `tpcds-q20-q23a-15w-controls.log`. A follow-up q22 direct-native-handoff candidate
removes one complete 26.52-million-row Nitro→Page→Nitro round-trip. Its five-warmup control reports 0.817x wall,
1.063x CPU, and 0.624x allocation; this matches the current SQL-shaped operator benchmark's 1.075x CPU ratio.
