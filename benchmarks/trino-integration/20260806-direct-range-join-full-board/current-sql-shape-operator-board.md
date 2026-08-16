# SQL-shape operator benchmark board

Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy
single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,
and the optimizer-selected join and ranking shapes.

CPU values are milliseconds per measured query invocation.

| suite | engine | queries | query-stages | aggregation | exchange_output | fused_scan_aggregation | join | other | ranking_sort_window | scan_filter_project |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tpch | nitro | 22 | unavailable | 3211.4 | 1578.5 | 13682.3 | 6366.5 | 178.4 | 32.3 | 21981.6 |
| tpch | trino | 22 | unavailable | 14732.1 | 3131.8 | 0.0 | 15302.8 | 225.0 | 27.4 | 32368.5 |
| tpcds | nitro | 103 | unavailable | 65171.5 | 8864.2 | 25711.4 | 41589.1 | 1672.9 | 2503.3 | 47505.7 |
| tpcds | trino | 103 | unavailable | 109809.3 | 19046.4 | 0.0 | 100393.2 | 2095.8 | 5581.2 | 117837.8 |
| clickbench | nitro | 43 | unavailable | 70761.5 | 13729.5 | 148579.9 | 0.0 | 1.9 | 95.9 | 53947.4 |
| clickbench | trino | 43 | unavailable | 155667.3 | 13337.7 | 0.0 | 0.0 | 0.0 | 4716.5 | 195203.0 |

## Operator-family CPU comparison

| suite | family | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | CPU difference (ms) |
|---|---|---:|---:|---:|---:|
| tpch | aggregation | 3211.4 | 14732.1 | 0.218 | -11520.7 |
| tpch | exchange_output | 1578.5 | 3131.8 | 0.504 | -1553.3 |
| tpch | fused_scan_aggregation | 13682.3 | 0.0 | n/a | 13682.3 |
| tpch | join | 6366.5 | 15302.8 | 0.416 | -8936.3 |
| tpch | other | 178.4 | 225.0 | 0.793 | -46.6 |
| tpch | ranking_sort_window | 32.3 | 27.4 | 1.178 | 4.9 |
| tpch | scan_filter_project | 21981.6 | 32368.5 | 0.679 | -10386.9 |
| tpcds | aggregation | 65171.5 | 109809.3 | 0.593 | -44637.7 |
| tpcds | exchange_output | 8864.2 | 19046.4 | 0.465 | -10182.2 |
| tpcds | fused_scan_aggregation | 25711.4 | 0.0 | n/a | 25711.4 |
| tpcds | join | 41589.1 | 100393.2 | 0.414 | -58804.1 |
| tpcds | other | 1672.9 | 2095.8 | 0.798 | -422.9 |
| tpcds | ranking_sort_window | 2503.3 | 5581.2 | 0.449 | -3077.9 |
| tpcds | scan_filter_project | 47505.7 | 117837.8 | 0.403 | -70332.1 |
| clickbench | aggregation | 70761.5 | 155667.3 | 0.455 | -84905.8 |
| clickbench | exchange_output | 13729.5 | 13337.7 | 1.029 | 391.8 |
| clickbench | fused_scan_aggregation | 148579.9 | 0.0 | n/a | 148579.9 |
| clickbench | join | 0.0 | 0.0 | n/a | 0.0 |
| clickbench | other | 1.9 | 0.0 | n/a | 1.9 |
| clickbench | ranking_sort_window | 95.9 | 4716.5 | 0.020 | -4620.6 |
| clickbench | scan_filter_project | 53947.4 | 195203.0 | 0.276 | -141255.5 |

## End-to-end operator CPU reconciliation

| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |
|---|---:|---:|---:|---:|---:|
| tpch | 47031.0 | 65787.7 | 0.715 | 99.914% | 99.924% |
| tpcds | 193018.2 | 354763.8 | 0.544 | 99.875% | 99.968% |
| clickbench | 287116.1 | 368924.5 | 0.778 | 99.907% | 99.968% |

## Largest single-sweep Nitro CPU regressions

These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;
a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.

| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |
|---|---|---:|---:|---:|---:|
| tpcds | q22 | 19637.4 | 16461.3 | 1.193 | 3176.1 |
| clickbench | q19 | 25788.6 | 23988.8 | 1.075 | 1799.8 |
| clickbench | q14 | 7479.9 | 7424.3 | 1.007 | 55.6 |
| clickbench | q12 | 1178.0 | 1132.6 | 1.040 | 45.4 |
| clickbench | q40 | 455.3 | 436.9 | 1.042 | 18.4 |
| tpcds | q20 | 87.6 | 73.3 | 1.195 | 14.3 |

The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,
physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured
before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.
