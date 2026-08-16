# SQL-shape operator benchmark board

Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy
single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,
and the optimizer-selected join and ranking shapes.

CPU values are milliseconds per measured query invocation.

| suite | engine | queries | query-stages | aggregation | exchange_output | fused_scan_aggregation | join | other | ranking_sort_window | scan_filter_project |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tpch | nitro | 22 | 158 | 3428.0 | 2311.1 | 13880.5 | 6722.0 | 181.8 | 32.7 | 21687.7 |
| tpch | trino | 22 | 158 | 14762.7 | 3015.4 | 0.0 | 15433.0 | 226.0 | 25.9 | 32376.8 |
| tpcds | nitro | 103 | 1341 | 63966.2 | 13736.9 | 27444.8 | 35989.3 | 1628.7 | 2636.3 | 48963.4 |
| tpcds | trino | 103 | 1341 | 106081.7 | 19397.0 | 0.0 | 97237.6 | 2013.4 | 5645.8 | 114609.2 |
| clickbench | nitro | 43 | 119 | 71980.5 | 14533.9 | 118387.7 | 0.0 | 3.1 | 117.8 | 42570.8 |
| clickbench | trino | 43 | 119 | 156626.9 | 13783.6 | 0.0 | 0.0 | 0.0 | 4728.9 | 196231.8 |

## Operator-family CPU comparison

| suite | family | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | CPU difference (ms) |
|---|---|---:|---:|---:|---:|
| tpch | aggregation | 3428.0 | 14762.7 | 0.232 | -11334.7 |
| tpch | exchange_output | 2311.1 | 3015.4 | 0.766 | -704.3 |
| tpch | fused_scan_aggregation | 13880.5 | 0.0 | n/a | 13880.5 |
| tpch | join | 6722.0 | 15433.0 | 0.436 | -8711.0 |
| tpch | other | 181.8 | 226.0 | 0.805 | -44.2 |
| tpch | ranking_sort_window | 32.7 | 25.9 | 1.261 | 6.8 |
| tpch | scan_filter_project | 21687.7 | 32376.8 | 0.670 | -10689.0 |
| tpcds | aggregation | 63966.2 | 106081.7 | 0.603 | -42115.5 |
| tpcds | exchange_output | 13736.9 | 19397.0 | 0.708 | -5660.1 |
| tpcds | fused_scan_aggregation | 27444.8 | 0.0 | n/a | 27444.8 |
| tpcds | join | 35989.3 | 97237.6 | 0.370 | -61248.3 |
| tpcds | other | 1628.7 | 2013.4 | 0.809 | -384.7 |
| tpcds | ranking_sort_window | 2636.3 | 5645.8 | 0.467 | -3009.4 |
| tpcds | scan_filter_project | 48963.4 | 114609.2 | 0.427 | -65645.8 |
| clickbench | aggregation | 71980.5 | 156626.9 | 0.460 | -84646.4 |
| clickbench | exchange_output | 14533.9 | 13783.6 | 1.054 | 750.2 |
| clickbench | fused_scan_aggregation | 118387.7 | 0.0 | n/a | 118387.7 |
| clickbench | join | 0.0 | 0.0 | n/a | 0.0 |
| clickbench | other | 3.1 | 0.0 | n/a | 3.1 |
| clickbench | ranking_sort_window | 117.8 | 4728.9 | 0.025 | -4611.2 |
| clickbench | scan_filter_project | 42570.8 | 196231.8 | 0.217 | -153661.0 |

## End-to-end operator CPU reconciliation

| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |
|---|---:|---:|---:|---:|---:|
| tpch | 48243.9 | 65839.8 | 0.733 | 99.925% | 99.931% |
| tpcds | 194365.6 | 344984.7 | 0.563 | 99.850% | 99.959% |
| clickbench | 247593.7 | 371371.2 | 0.667 | 99.871% | 99.980% |

## Largest single-sweep Nitro CPU regressions

These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;
a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.

| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |
|---|---|---:|---:|---:|---:|
| tpcds | q23a | 21860.2 | 19649.4 | 1.113 | 2210.8 |
| clickbench | q19 | 24970.4 | 23725.2 | 1.052 | 1245.3 |
| clickbench | q40 | 489.2 | 440.9 | 1.110 | 48.3 |
| tpcds | q20 | 89.9 | 75.3 | 1.194 | 14.6 |

The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,
physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured
before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.
