# SQL-shape operator benchmark board

Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy
single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,
and the optimizer-selected join and ranking shapes.

CPU values are milliseconds per measured query invocation.

| suite | engine | queries | query-stages | aggregation | exchange_output | fused_scan_aggregation | join | other | ranking_sort_window | scan_filter_project |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tpch | nitro | 22 | 157 | 3621.4 | 1764.2 | 11231.5 | 8077.8 | 264.1 | 41.5 | 24594.2 |
| tpch | trino | 22 | 158 | 14919.4 | 3400.6 | 0.0 | 15197.4 | 236.9 | 49.5 | 32133.8 |
| tpcds | nitro | 103 | 1332 | 56045.0 | 9425.7 | 23191.0 | 46874.8 | 1549.9 | 4709.9 | 54861.2 |
| tpcds | trino | 103 | 1341 | 106966.2 | 19704.0 | 0.0 | 98635.6 | 2242.2 | 5665.1 | 117600.7 |
| clickbench | nitro | 43 | 115 | 75808.7 | 14771.1 | 143427.1 | 0.0 | 5.2 | 159.2 | 56992.5 |
| clickbench | trino | 43 | 119 | 154500.9 | 13639.3 | 0.0 | 0.0 | 0.0 | 4721.8 | 195678.3 |

## End-to-end operator CPU reconciliation

| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |
|---|---:|---:|---:|---:|---:|
| tpch | 49594.6 | 65937.5 | 0.752 | 99.901% | 99.934% |
| tpcds | 196657.5 | 350813.8 | 0.561 | 99.809% | 99.939% |
| clickbench | 291163.7 | 368540.2 | 0.790 | 99.893% | 99.975% |

## Largest single-sweep Nitro CPU regressions

These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;
a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.

| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |
|---|---|---:|---:|---:|---:|
| clickbench | q30 | 30953.8 | 21205.8 | 1.460 | 9748.0 |
| tpcds | q67 | 28332.2 | 21641.1 | 1.309 | 6691.1 |
| tpcds | q22 | 21025.8 | 17005.8 | 1.236 | 4020.0 |
| clickbench | q19 | 24405.3 | 23639.4 | 1.032 | 765.9 |
| tpch | q21 | 7845.1 | 7181.5 | 1.092 | 663.7 |
| tpcds | q87 | 2466.4 | 1998.0 | 1.234 | 468.5 |
| tpcds | q38 | 2510.2 | 2126.6 | 1.180 | 383.5 |
| tpcds | q11 | 5206.7 | 4993.8 | 1.043 | 212.9 |
| tpcds | q02 | 797.4 | 596.2 | 1.337 | 201.1 |
| tpcds | q47 | 12319.6 | 12146.3 | 1.014 | 173.4 |
| tpcds | q15 | 389.7 | 220.8 | 1.765 | 168.9 |
| clickbench | q40 | 546.9 | 432.9 | 1.263 | 114.0 |
| tpcds | q04 | 10412.2 | 10331.8 | 1.008 | 80.4 |
| tpch | q03 | 3180.5 | 3103.8 | 1.025 | 76.8 |
| tpcds | q78 | 9589.2 | 9528.6 | 1.006 | 60.6 |

The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,
physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured
before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.
