# Post-layout synchronized benchmark board

This is the provenance-clean performance board after fixed-record layout admission and bounded control lookahead were accepted. All 168 query variants passed exact result and type comparison: 22 TPC-H queries, 103 TPC-DS variants, and 43 ClickBench queries.

Each suite ran uninterrupted in a fresh JDK 26 test JVM. Both engines executed the same SQL against the same SF10/ClickBench Parquet data, alternating within one fixture. Every query received three complete warmups and three measured iterations. The JVM heap was 12 GiB, the query-memory limit was 8 GiB, and the fixture recorded wall time, query CPU, per-operator CPU, thread allocation, retained query peak, and JVM RSS peak. Ratios are Nitro / Trino; lower is better. Geometric means give each query equal multiplicative weight, while weighted ratios divide summed per-query medians.

| Suite | Queries | Wall geo | CPU geo | CPU weighted | Allocation weighted | CPU wins | Wall wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.702x | 0.672x | 0.718x | 0.351x | 22 | 22 |
| TPC-DS | 103 | 0.550x | 0.369x | 0.513x | 0.286x | 103 | 99 |
| ClickBench | 43 | 0.749x | 0.566x | 0.623x | 0.309x | 43 | 34 |
| Overall | 168 | 0.614x | 0.445x | 0.582x | 0.302x | 168 | 155 |

Summed operator CPU independently reconciles with query CPU: its geometric mean is 0.443x and its weighted ratio is 0.581x (459.350 versus 790.434 CPU-seconds). Operator CPU accounts for at least 99.8% of reported query CPU in every suite. Nitro wins operator CPU on all 168 variants.

Four thread-allocation observations were unavailable because the JVM live-thread counter delta crossed its signed range: TPC-DS q22 Nitro, TPC-DS q82 Trino, ClickBench q24 Nitro, and ClickBench q33 Trino. They are excluded rather than imputed. Nitro wins 161 of the 164 valid allocation pairs.

The q02 row is a three-sample outlier rather than a kernel regression. A subsequent plan-matched 10-warmup, 20-measurement confirmation reports 0.625x query CPU, 0.626x operator CPU, and 0.901x wall. It is kept separate instead of being spliced into these internally synchronized aggregates. The remaining wall losses all retain lower Nitro CPU and are treated as critical-path or fixed-cost questions.

Derived data:

- `summary.csv`: suite and global ratios.
- `query-board.csv`: one normalized row per query variant.
- `operator-cpu-all.csv`: per-query operator CPU totals.
- `sql-shape-operator-board.md`: semantic operator-family reconciliation.
- `generate_board.py`: deterministic derivation from the three suite logs.
