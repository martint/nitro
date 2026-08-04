# Post-native-single warmed performance board

This board compares the Trino and Nitro backends over the native SF10 Parquet data. Every qualified row uses two complete warmup executions per engine followed by three alternating measured executions. Allocation and operator metrics are enabled. Runs with fewer warmups are activation or correctness evidence only and are not included here.

The suite logs are long-lived-JVM breadth sweeps. Their evolving heap, compilation, worker, and scheduler state makes individual outliers less reliable than the fresh-JVM controls below.

| Suite | Queries | Wall geomean | CPU geomean | Total wall | Total CPU | CPU wins | Allocation total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.721x | 0.697x | 0.721x | 0.744x | 19/22 | 0.330x |
| TPC-DS | 103 | 0.597x | 0.418x | 0.707x | 0.598x | 96/103 | 0.291x |
| ClickBench | 43 | 0.891x | 0.711x | 0.795x | 0.846x | 39/43 | 0.413x |
| Overall | 168 | 0.678x | 0.512x | 0.749x | 0.727x | 154/168 | 0.339x |

Allocation totals exclude rows with invalid negative thread-allocation deltas: one TPC-H row, thirteen TPC-DS rows, and five ClickBench rows.

## Fresh-JVM controls

The conspicuous long-sweep regressions were rerun one query per fresh JVM with the same two-warmup/three-measurement protocol:

| Query | Wall p50 | CPU mean | Allocation | Classification |
| --- | ---: | ---: | ---: | --- |
| TPC-DS q15 | 0.408x | 0.145x | 0.075x | Long-process artifact |
| TPC-DS q22 | 0.843x | 0.687x | 0.324x | Long-process artifact |
| ClickBench q19 | 0.795x | 1.001x | 0.718x | CPU parity; wall/allocation win |
| ClickBench q30 | 0.391x | 0.394x | 0.084x | Long-process artifact |

The fresh controls take precedence over their corresponding suite rows for code decisions. In particular, they provide no evidence for changing the q15 aggregation, q22 join, or q30 execution kernels.

`tpch-no-allocation-control.log` is retained only as a warmed control without allocation instrumentation; `tpch.log` is the qualified TPC-H board artifact.
