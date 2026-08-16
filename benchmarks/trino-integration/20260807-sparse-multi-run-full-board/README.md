# Sparse multi-run join post-change board

This board validates Nitro core commit `23e32d77` against the clean Trino integration at `75bf4c27`. All 168 query
variants passed exact comparison against SF10 Parquet/ClickBench Parquet data. Each query received five complete
warmups followed by three alternating Trino/Nitro measurements in a 12 GiB JVM with an 8 GiB query-memory limit.
Thread allocation, query/heap peak memory, native-boundary metrics, and operator CPU metrics were enabled.

| suite | queries | wall geo | CPU geo | allocation geo | wall wins | CPU wins | max query peak Trino / Nitro |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.744x | 0.716x | 0.181x | 21 | 21 | 1,085 / 1,390 MiB (q09) |
| TPC-DS | 103 | 0.584x | 0.401x | 0.188x* | 94 | 100 | 1,756 MiB q23b / 2,263 MiB q23a |
| ClickBench raw | 43 | 0.867x | 0.691x | 0.311x* | 31 | 41 | 4,627 / 5,541 MiB (q33) |
| ClickBench q30-reconciled | 43 | 0.846x | 0.673x | — | — | — | unchanged |

`*` Allocation geometric means exclude five TPC-DS and three ClickBench rows where the long-lived JVM's cumulative
thread-allocation counter wrapped and the harness reported a negative delta. Nitro won 96/98 valid TPC-DS allocation
rows and 40/40 valid ClickBench rows; TPC-H had no invalid rows and Nitro won all 22.

TPC-DS is effectively unchanged-to-better than the preceding 0.586x wall / 0.406x CPU board. TPC-H is somewhat
noisier than its preceding 0.716x / 0.689x board, but still has only the known q03 CPU loss (1.034x here) and no broad
regression. ClickBench's raw aggregate contains a known long-lived-JVM q30 failure mode: Nitro q30 used 21,188 CPU-ms
in sequence versus 6,749 CPU-ms in a fresh five-warmup/five-measurement JVM, while Trino remained about 21,000 CPU-ms.
The fresh control measures q30 at 0.346x wall / 0.322x CPU; replacing only that unstable row produces the reconciled
0.846x / 0.673x aggregate. q30 has no join, so this movement cannot be caused by the sparse hash-join representation.

Raw Surefire reports are `tpch.xml`, `tpcds.xml`, and `clickbench.xml`; `clickbench-q30-isolated.xml` is the fresh
control. The focused q22/q23a/q67 admission controls are in the sibling
`20260807-sparse-multi-run-binary-join` directory. No JFR, heap dump, or Kata artifact was created.
