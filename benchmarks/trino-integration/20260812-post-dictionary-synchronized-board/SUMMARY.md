# Synchronized Nitro/Trino board — 2026-08-12

All ratios are Nitro / Trino; lower is better. Each suite ran uninterrupted in one JVM, with three warmups and three measured iterations per query. The test JVM used a 12 GiB heap and the query-memory limit was 8 GiB. Geometric means give each query equal multiplicative weight; weighted ratios sum the underlying medians.

| Suite | Queries | Wall geo | Wall weighted | Wall wins | CPU geo | CPU weighted | CPU wins | Operator CPU geo | Allocation weighted |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H SF10 Parquet | 22 | 0.703 | 0.717 | 22/22 | 0.688 | 0.743 | 22/22 | 0.688 | 0.366 |
| TPC-DS SF10 Parquet | 103 | 0.566 | 0.665 | 95/103 | 0.377 | 0.528 | 103/103 | 0.375 | 0.304 |
| ClickBench | 43 | 0.766 | 0.686 | 34/43 | 0.568 | 0.630 | 43/43 | 0.564 | 0.270 |
| All queries | 168 | 0.629 | 0.678 | 151/168 | 0.453 | 0.594 | 168/168 | 0.451 | 0.295 |

The operator-CPU sum independently reconciles with query CPU: 0.593 weighted operator CPU versus 0.594 reported query CPU. Nitro uses less CPU on every query in this capture.

Dictionary-selective Parquet row groups are now rejected by bounded dictionary inspection when min/max metadata alone overlaps the predicate. The focused source audit measured zero TPC-DS physical-position overreads after this change. In the synchronized board, q20 moved from the prior sole CPU loss to 0.885 query CPU and 0.869 operator CPU.

Nine queries have invalid process-wide thread-allocation deltas and are excluded from paired allocation summaries. Of the 159 valid pairs, Nitro wins 156. Query-peak weighted ratios are 0.524 for TPC-H, 0.739 for TPC-DS, 0.937 for ClickBench, and 0.826 globally.

The material remaining wall regressions all have lower Nitro CPU. They are dominated by short-query/scheduling floors in ClickBench and CPU/wall disconnects such as TPC-DS q20 (1.224 wall, 0.885 CPU), q83 (1.089, 0.910), and ClickBench q16 (1.085, 0.926). These remain attribution targets; they are not operator-CPU regressions.

`board.csv` contains the full query board. `operator-cpu-all.csv` contains independently summed operator CPU. The raw suite logs and deterministic `generate_board.py` script are alongside them.
