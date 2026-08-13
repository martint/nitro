# Synchronized Nitro/Trino board — 2026-08-13

All ratios are Nitro / Trino; lower is better. Each suite ran uninterrupted in one JVM with three warmups and three measured iterations per query, a 12 GiB heap, and an 8 GiB query-memory limit. Geometric means weight queries equally; weighted ratios divide sums of the underlying medians.

| Suite | Queries | Wall geo | Wall weighted | Wall wins | CPU geo | CPU weighted | CPU wins | Allocation geo | Allocation weighted | Query-peak weighted | Heap-peak weighted |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H SF10 Parquet | 22 | 0.711 | 0.722 | 22/22 | 0.696 | 0.725 | 22/22 | 0.193 | 0.334 | 0.676 | 0.818 |
| TPC-DS SF10 Parquet | 103 | 0.559 | 0.639 | 97/103 | 0.380 | 0.535 | 102/103 | 0.180 | 0.275 | 0.691 | 0.873 |
| ClickBench | 43 | 0.762 | 0.694 | 34/43 | 0.574 | 0.638 | 42/43 | 0.254 | 0.297 | 0.977 | 0.952 |
| All queries | 168 | 0.625 | 0.674 | 153/168 | 0.457 | 0.600 | 166/168 | 0.198 | 0.290 | 0.842 | 0.885 |

The independently summed operator CPU geomean is 0.456 and its weighted ratio is 0.599. It wins on 165 of 168 queries and closely reconciles with independently reported query CPU.

The only query-CPU regressions are ClickBench q40 at 1.076 and TPC-DS q02 at 1.044. The largest wall regressions are ClickBench q07, q43, q41, q42, and q01; each uses less Nitro CPU, so they remain critical-path and fixed-cost investigations rather than evidence of excess aggregate computation.

Semantic operator-family attribution shows scan/filter/project/aggregation at 0.637 CPU (220.4 CPU-seconds saved), join/build at 0.326 (76.2 seconds saved), sort/TopN/window at 0.222 (8.1 seconds saved), and exchange/output at 0.902 (3.5 seconds saved). ClickBench exchange/output remains the one material suite-level boundary regression at 1.136.

`board.csv` is the complete per-query board. `query-board.csv`, `operator-cpu-all.csv`, `operator-cpu.csv`, `operator-family.csv`, and `summary.csv` are derived presentation inputs. Raw suite logs are retained alongside them.
