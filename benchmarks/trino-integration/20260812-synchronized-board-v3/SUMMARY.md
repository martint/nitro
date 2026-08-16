# Synchronized Nitro/Trino board — 2026-08-12, sparse-dictionary fix

All ratios are Nitro / Trino; lower is better. Each suite ran uninterrupted in one JVM, with three warmups and three measured iterations per query. The JVM used a 12 GiB heap and an 8 GiB query-memory limit. Headline geometric means give each query equal weight; weighted ratios sum the underlying medians.

## Board

| Suite | Queries | Wall geometric mean | Wall weighted | Wall wins | CPU geometric mean | CPU weighted | CPU wins | Allocation geometric mean | Allocation weighted | Query-peak weighted | Heap-peak weighted |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H SF10 Parquet | 22 | 0.741 | 0.737 | 21/22 | 0.713 | 0.751 | 22/22 | 0.198 | 0.349 | 0.611 | 0.811 |
| ClickBench | 43 | 0.752 | 0.678 | 35/43 | 0.561 | 0.635 | 43/43 | 0.260 | 0.322 | 0.988 | 0.973 |
| TPC-DS SF10 Parquet | 103 | 0.564 | 0.652 | 95/103 | 0.395 | 0.552 | 102/103 | 0.175 | 0.260 | 0.729 | 0.831 |
| All queries | 168 | 0.629 | 0.671 | 151/168 | 0.467 | 0.608 | 167/168 | 0.196 | 0.294 | 0.852 | 0.862 |

Allocation geometric means exclude rows where the process-wide thread-allocation delta was unavailable. Query-peak geometric means similarly omit zero-denominator rows; weighted ratios above include the recorded totals.

## Corrected state-sensitive regression

TPC-DS q35 caused q36's grouping input to carry 10,000 dictionary ids over a 102,000-row join-build base. Mixed-composite admission eagerly interned every base value, making aggregation work scale with unreferenced build rows. Grouping now keeps sparse dictionary fields lazy and interns only referenced entries; dense fields retain eager compact-composite admission.

| Query | Previous synchronized wall / CPU | Current synchronized wall / CPU |
|---|---:|---:|
| TPC-DS q35 | 0.717 / 0.491 | 0.728 / 0.493 |
| TPC-DS q36 | 2.182 / 2.606 | 0.527 / 0.484 |

The focused q35→q36 run measured q36 at 0.628 wall / 0.472 CPU, with grouping reduced from roughly 4 seconds to 0.30–0.39 seconds. The full synchronized continuation corroborates the correction.

## Remaining laggards

TPC-DS q20 is the only CPU regression: 0.777 wall / 1.039 CPU. Its absolute median CPU difference is 3 ms, down from 1.173 CPU on the previous synchronized board; the already identified pipeline-source path remains the next CPU investigation.

All wall regressions have lower Nitro CPU. Material cases are ClickBench q16 (1.191 wall / 0.926 CPU), TPC-DS q02 (1.188 / 0.969), and TPC-DS q74 (1.076 / 0.639). Short-query wall floors are ClickBench q01, q41, q42, and q43. These should be investigated through critical-path scheduling and blocking attribution, not by changing otherwise faster operators.

The complete per-query board is in `board.csv`; raw suite logs and the board-generation script are alongside it.
