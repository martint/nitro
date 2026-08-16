# SQL-shape operator benchmark board

Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy
single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,
and the optimizer-selected join and ranking shapes.

CPU values are milliseconds per measured query invocation.

| suite | engine | queries | query-stages | aggregation | exchange_output | fused_scan_aggregation | join | other | ranking_sort_window | scan_filter_project |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tpch | nitro | 22 | 158 | 3406.5 | 2134.6 | 13624.8 | 6514.9 | 178.9 | 33.8 | 21704.3 |
| tpch | trino | 22 | 158 | 13565.1 | 2837.0 | 0.0 | 15258.2 | 223.3 | 26.5 | 32507.2 |
| tpcds | nitro | 103 | 1341 | 60620.8 | 16827.0 | 25434.9 | 35617.1 | 1632.1 | 2627.6 | 48604.5 |
| tpcds | trino | 103 | 1341 | 108556.7 | 19177.7 | 0.0 | 101092.4 | 2014.3 | 5480.5 | 118749.2 |
| clickbench | nitro | 43 | 119 | 69994.6 | 14634.8 | 120813.0 | 0.0 | 3.0 | 116.3 | 42056.0 |
| clickbench | trino | 43 | 119 | 157005.3 | 13837.7 | 0.0 | 0.0 | 0.0 | 4779.7 | 196201.0 |

## End-to-end operator CPU reconciliation

| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |
|---|---:|---:|---:|---:|---:|
| tpch | 47597.9 | 64417.4 | 0.739 | 99.898% | 99.926% |
| tpcds | 191364.0 | 355070.9 | 0.539 | 99.846% | 99.970% |
| clickbench | 247617.7 | 371823.7 | 0.666 | 99.855% | 99.975% |

## Largest single-sweep Nitro CPU regressions

These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;
a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.

| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |
|---|---|---:|---:|---:|---:|
| tpcds | q22 | 19202.7 | 16428.3 | 1.169 | 2774.4 |
| clickbench | q07 | 1893.9 | 425.3 | 4.453 | 1468.6 |
| clickbench | q19 | 25114.5 | 23909.8 | 1.050 | 1204.7 |
| tpcds | q23a | 20296.1 | 19742.1 | 1.028 | 554.0 |
| clickbench | q40 | 477.8 | 437.4 | 1.092 | 40.4 |
| tpcds | q20 | 91.7 | 74.0 | 1.239 | 17.7 |

The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,
physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured
before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.
