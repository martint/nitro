# SQL-shape operator benchmark board

Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy
single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,
and the optimizer-selected join and ranking shapes.

CPU values are milliseconds per measured query invocation.

| suite | engine | queries | query-stages | aggregation | exchange_output | fused_scan_aggregation | join | other | ranking_sort_window | scan_filter_project |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tpch | nitro | 22 | 158 | 3906.2 | 2115.0 | 12088.5 | 6853.6 | 190.3 | 30.9 | 21291.1 |
| tpch | trino | 22 | 158 | 14642.3 | 3097.5 | 0.0 | 15115.9 | 225.2 | 34.7 | 32610.2 |
| tpcds | nitro | 103 | 1341 | 61722.6 | 14907.4 | 22752.1 | 28519.2 | 1601.2 | 2234.1 | 46077.0 |
| tpcds | trino | 103 | 1341 | 105031.5 | 18283.5 | 0.0 | 95341.9 | 1981.0 | 5602.6 | 110612.7 |
| clickbench | nitro | 43 | 119 | 64597.7 | 14400.4 | 109748.0 | 0.0 | 0.8 | 52.8 | 41496.4 |
| clickbench | trino | 43 | 119 | 157511.4 | 13457.8 | 0.0 | 0.0 | 0.0 | 4734.6 | 194392.0 |

## Operator-family CPU comparison

| suite | family | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | CPU difference (ms) |
|---|---|---:|---:|---:|---:|
| tpch | aggregation | 3906.2 | 14642.3 | 0.267 | -10736.1 |
| tpch | exchange_output | 2115.0 | 3097.5 | 0.683 | -982.5 |
| tpch | fused_scan_aggregation | 12088.5 | 0.0 | n/a | 12088.5 |
| tpch | join | 6853.6 | 15115.9 | 0.453 | -8262.4 |
| tpch | other | 190.3 | 225.2 | 0.845 | -34.8 |
| tpch | ranking_sort_window | 30.9 | 34.7 | 0.890 | -3.8 |
| tpch | scan_filter_project | 21291.1 | 32610.2 | 0.653 | -11319.1 |
| tpcds | aggregation | 61722.6 | 105031.5 | 0.588 | -43308.9 |
| tpcds | exchange_output | 14907.4 | 18283.5 | 0.815 | -3376.1 |
| tpcds | fused_scan_aggregation | 22752.1 | 0.0 | n/a | 22752.1 |
| tpcds | join | 28519.2 | 95341.9 | 0.299 | -66822.6 |
| tpcds | other | 1601.2 | 1981.0 | 0.808 | -379.7 |
| tpcds | ranking_sort_window | 2234.1 | 5602.6 | 0.399 | -3368.4 |
| tpcds | scan_filter_project | 46077.0 | 110612.7 | 0.417 | -64535.7 |
| clickbench | aggregation | 64597.7 | 157511.4 | 0.410 | -92913.7 |
| clickbench | exchange_output | 14400.4 | 13457.8 | 1.070 | 942.6 |
| clickbench | fused_scan_aggregation | 109748.0 | 0.0 | n/a | 109748.0 |
| clickbench | join | 0.0 | 0.0 | n/a | 0.0 |
| clickbench | other | 0.8 | 0.0 | n/a | 0.8 |
| clickbench | ranking_sort_window | 52.8 | 4734.6 | 0.011 | -4681.8 |
| clickbench | scan_filter_project | 41496.4 | 194392.0 | 0.213 | -152895.7 |

## End-to-end operator CPU reconciliation

| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |
|---|---:|---:|---:|---:|---:|
| tpch | 46475.5 | 65725.7 | 0.707 | 99.890% | 99.913% |
| tpcds | 177813.7 | 336853.1 | 0.528 | 99.880% | 99.971% |
| clickbench | 230296.0 | 370095.9 | 0.622 | 99.828% | 99.970% |

## Largest single-sweep Nitro CPU regressions

These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;
a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.

| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |
|---|---|---:|---:|---:|---:|
| clickbench | q40 | 494.8 | 432.5 | 1.144 | 62.4 |

The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,
physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured
before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.
