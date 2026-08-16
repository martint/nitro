# SQL-shape operator benchmark board

Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy
single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,
and the optimizer-selected join and ranking shapes.

CPU values are milliseconds per measured query invocation.

| suite | engine | queries | query-stages | aggregation | exchange_output | fused_scan_aggregation | join | other | ranking_sort_window | scan_filter_project |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tpch | nitro | 22 | 158 | 4205.5 | 1985.7 | 11910.0 | 6888.7 | 189.1 | 31.0 | 20980.3 |
| tpch | trino | 22 | 158 | 14827.4 | 3118.4 | 0.0 | 15453.9 | 223.3 | 28.1 | 32444.9 |
| tpcds | nitro | 103 | 1341 | 63276.2 | 15034.4 | 22885.8 | 28772.4 | 1655.0 | 2267.1 | 46752.4 |
| tpcds | trino | 103 | 1341 | 106604.2 | 19061.7 | 0.0 | 98688.0 | 2048.1 | 5622.2 | 115080.8 |
| clickbench | nitro | 43 | 119 | 68160.7 | 14899.1 | 108941.6 | 0.0 | 0.9 | 58.3 | 41298.5 |
| clickbench | trino | 43 | 119 | 157114.7 | 13704.4 | 0.0 | 0.0 | 0.0 | 4795.6 | 197128.6 |

## Operator-family CPU comparison

| suite | family | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | CPU difference (ms) |
|---|---|---:|---:|---:|---:|
| tpch | aggregation | 4205.5 | 14827.4 | 0.284 | -10621.9 |
| tpch | exchange_output | 1985.7 | 3118.4 | 0.637 | -1132.7 |
| tpch | fused_scan_aggregation | 11910.0 | 0.0 | n/a | 11910.0 |
| tpch | join | 6888.7 | 15453.9 | 0.446 | -8565.2 |
| tpch | other | 189.1 | 223.3 | 0.847 | -34.3 |
| tpch | ranking_sort_window | 31.0 | 28.1 | 1.103 | 2.9 |
| tpch | scan_filter_project | 20980.3 | 32444.9 | 0.647 | -11464.6 |
| tpcds | aggregation | 63276.2 | 106604.2 | 0.594 | -43328.0 |
| tpcds | exchange_output | 15034.4 | 19061.7 | 0.789 | -4027.3 |
| tpcds | fused_scan_aggregation | 22885.8 | 0.0 | n/a | 22885.8 |
| tpcds | join | 28772.4 | 98688.0 | 0.292 | -69915.7 |
| tpcds | other | 1655.0 | 2048.1 | 0.808 | -393.1 |
| tpcds | ranking_sort_window | 2267.1 | 5622.2 | 0.403 | -3355.2 |
| tpcds | scan_filter_project | 46752.4 | 115080.8 | 0.406 | -68328.4 |
| clickbench | aggregation | 68160.7 | 157114.7 | 0.434 | -88954.0 |
| clickbench | exchange_output | 14899.1 | 13704.4 | 1.087 | 1194.7 |
| clickbench | fused_scan_aggregation | 108941.6 | 0.0 | n/a | 108941.6 |
| clickbench | join | 0.0 | 0.0 | n/a | 0.0 |
| clickbench | other | 0.9 | 0.0 | n/a | 0.9 |
| clickbench | ranking_sort_window | 58.3 | 4795.6 | 0.012 | -4737.3 |
| clickbench | scan_filter_project | 41298.5 | 197128.6 | 0.210 | -155830.0 |

## End-to-end operator CPU reconciliation

| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |
|---|---:|---:|---:|---:|---:|
| tpch | 46190.3 | 66095.9 | 0.699 | 99.877% | 99.923% |
| tpcds | 180643.2 | 347105.1 | 0.520 | 99.862% | 99.959% |
| clickbench | 233359.0 | 372743.2 | 0.626 | 99.849% | 99.971% |

## Largest single-sweep Nitro CPU regressions

These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;
a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.

| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |
|---|---|---:|---:|---:|---:|

The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,
physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured
before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.
