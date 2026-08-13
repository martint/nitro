# SQL-shape operator benchmark board

Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy
single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,
and the optimizer-selected join and ranking shapes.

CPU values are milliseconds per measured query invocation.

| suite | engine | queries | query-stages | aggregation | exchange_output | fused_scan_aggregation | join | other | ranking_sort_window | scan_filter_project |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tpch | nitro | 22 | 158 | 4215.8 | 2058.6 | 11897.0 | 6803.8 | 187.3 | 29.0 | 21103.2 |
| tpch | trino | 22 | 158 | 13923.7 | 3006.3 | 0.0 | 15128.9 | 220.2 | 27.9 | 32417.7 |
| tpcds | nitro | 103 | 1341 | 63465.1 | 15178.4 | 23008.6 | 28722.7 | 1691.6 | 2267.3 | 46957.5 |
| tpcds | trino | 103 | 1341 | 107411.3 | 19270.5 | 0.0 | 102204.9 | 2115.7 | 5576.1 | 117019.7 |
| clickbench | nitro | 43 | 119 | 64486.4 | 14860.1 | 110905.6 | 0.0 | 0.9 | 52.9 | 41270.8 |
| clickbench | trino | 43 | 119 | 156617.0 | 13888.4 | 0.0 | 0.0 | 0.0 | 4754.6 | 196363.4 |

## Operator-family CPU comparison

| suite | family | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | CPU difference (ms) |
|---|---|---:|---:|---:|---:|
| tpch | aggregation | 4215.8 | 13923.7 | 0.303 | -9707.9 |
| tpch | exchange_output | 2058.6 | 3006.3 | 0.685 | -947.8 |
| tpch | fused_scan_aggregation | 11897.0 | 0.0 | n/a | 11897.0 |
| tpch | join | 6803.8 | 15128.9 | 0.450 | -8325.2 |
| tpch | other | 187.3 | 220.2 | 0.850 | -32.9 |
| tpch | ranking_sort_window | 29.0 | 27.9 | 1.037 | 1.0 |
| tpch | scan_filter_project | 21103.2 | 32417.7 | 0.651 | -11314.5 |
| tpcds | aggregation | 63465.1 | 107411.3 | 0.591 | -43946.2 |
| tpcds | exchange_output | 15178.4 | 19270.5 | 0.788 | -4092.0 |
| tpcds | fused_scan_aggregation | 23008.6 | 0.0 | n/a | 23008.6 |
| tpcds | join | 28722.7 | 102204.9 | 0.281 | -73482.2 |
| tpcds | other | 1691.6 | 2115.7 | 0.800 | -424.1 |
| tpcds | ranking_sort_window | 2267.3 | 5576.1 | 0.407 | -3308.8 |
| tpcds | scan_filter_project | 46957.5 | 117019.7 | 0.401 | -70062.2 |
| clickbench | aggregation | 64486.4 | 156617.0 | 0.412 | -92130.6 |
| clickbench | exchange_output | 14860.1 | 13888.4 | 1.070 | 971.7 |
| clickbench | fused_scan_aggregation | 110905.6 | 0.0 | n/a | 110905.6 |
| clickbench | join | 0.0 | 0.0 | n/a | 0.0 |
| clickbench | other | 0.9 | 0.0 | n/a | 0.9 |
| clickbench | ranking_sort_window | 52.9 | 4754.6 | 0.011 | -4701.7 |
| clickbench | scan_filter_project | 41270.8 | 196363.4 | 0.210 | -155092.6 |

## End-to-end operator CPU reconciliation

| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |
|---|---:|---:|---:|---:|---:|
| tpch | 46294.5 | 64724.8 | 0.715 | 99.923% | 99.935% |
| tpcds | 181291.3 | 353598.2 | 0.513 | 99.865% | 99.970% |
| clickbench | 231576.6 | 371623.3 | 0.623 | 99.847% | 99.965% |

## Largest single-sweep Nitro CPU regressions

These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;
a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.

| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |
|---|---|---:|---:|---:|---:|

The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,
physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured
before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.
