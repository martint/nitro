# Synchronized Nitro/Trino board — 2026-08-12

All ratios are Nitro / Trino; lower is better. Headline ratios use per-query medians from one uninterrupted run per suite. Each query ran paired on the same JVM with 3 warmups and 3 measured iterations. The JVM used an adaptive 128 MiB initial / 12 GiB maximum heap and an 8 GiB query-memory limit.

## Board

| Suite | Queries | Wall geometric mean | Wall weighted | Wall wins | CPU geometric mean | CPU weighted | CPU wins | Allocation geometric mean | Allocation weighted | Query-peak weighted | Heap-peak weighted |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H SF10 Parquet | 22 | 0.709 | 0.698 | 21/22 | 0.678 | 0.731 | 21/22 | 0.203 | 0.352 | 0.513 | 0.807 |
| ClickBench | 43 | 0.749 | 0.671 | 35/43 | 0.572 | 0.641 | 43/43 | 0.265 | 0.354 | 0.952 | 0.921 |
| TPC-DS SF10 Parquet | 103 | 0.563 | 0.644 | 95/103 | 0.391 | 0.549 | 99/103 | 0.185 | 0.302 | 0.748 | 0.828 |
| All queries | 168 | 0.624 | 0.660 | 151/168 | 0.463 | 0.608 | 163/168 | 0.205 | 0.328 | 0.832 | 0.848 |

Four allocation rows are unavailable because the process-wide thread-allocation delta became negative when worker threads retired: ClickBench q24 and TPC-DS q22, q39a, and q82. They are excluded only from allocation aggregates. Query-peak geometric means omit two zero-denominator rows; weighted query-peak ratios include every row.

## Regressions and laggards

Stable or actionable CPU regressions:

| Suite/query | Wall ratio | CPU ratio | Interpretation |
|---|---:|---:|---|
| TPC-DS q57 | 1.358 | 1.445 | Largest stable full-sweep regression. An isolated JVM instead measured 0.530 wall / 0.412 CPU, indicating a strong cross-query JVM-state or compilation interaction. |
| TPC-DS q20 | 0.846 | 1.173 | Wall remains faster, but CPU is repeatably higher. A preheated 10-sample run measured 0.926 wall / 1.108 CPU. |
| TPC-H q03 | 0.838 | 1.040 | Small CPU regression with a wall win; lower priority than q20/q57. |

State-sensitive anomalies requiring a controlled reproduction before changing engine code:

| TPC-DS query | Full-sweep wall / CPU | Isolated 10-sample wall / CPU |
|---|---:|---:|
| q36 | 2.182 / 2.606 | 0.575 / 0.427 |
| q86 | 4.848 / 5.435 | 1.445 / 0.969 |

The tight samples within each run rule out ordinary per-iteration noise, while the reversal between run shapes rules out treating the full-sweep values as stable operator regressions. The leading hypothesis is generated-code/JIT or code-cache state across a long suite, not GC pause time: no large pause coincided with either measured row, and query CPU itself changed sharply.

Wall-only regressions are mostly short-query floors: ClickBench q01, q03, q04, q07, q41, q42, q43; TPC-H q12; and TPC-DS q02, q30, q65, q67, q85. Nitro CPU is lower on every one of these. The material long-query wall cases are TPC-DS q65 (1.053 wall / 0.614 CPU), q67 (1.084 / 0.901), and ClickBench q16 (1.066 / 0.883).

## Focused attribution

TPC-DS q16, 5 warmups and 10 measurements:

- 0.449 wall and 0.345 CPU; allocation median 2.17 GiB versus 8.05 GiB; query peak 299 MiB versus 853 MiB.
- Nitro's largest mean operator CPU consumers were pipeline source (197 ms), hash build (160 ms), and partitioned output (153 ms). Trino's were partitioned output (624 ms), hash build (466 ms), and scan/filter/project (379 ms).
- The synchronized suite row was even better at 0.406 wall / 0.167 CPU. Attribution is therefore useful for composition, but not a replacement for the synchronized headline row.

TPC-DS q20, equal 500 ms dynamic-filter wait, 5 warmups and 10 measurements:

- The cold isolated result was 0.941 wall / 1.540 CPU; two outlier iterations distorted means but not medians.
- A suite-preheated 10-sample rerun measured 0.926 wall / 1.108 CPU, confirming a smaller real CPU regression.
- In that preheated run, Nitro pipeline-source CPU was 51.9 ms versus Trino scan/filter/project at 32.0 ms. Secondary differences were aggregation 12.2 versus 11.4 ms, window 5.4 versus 3.5 ms, and partitioned output 3.7 versus 2.6 ms. The source path is the primary opportunity.

Wall-attribution runs (5 warmups and 5 measurements):

| Query | Wall ratio | CPU ratio | Longest observed critical pipeline, Trino → Nitro |
|---|---:|---:|---:|
| q24a | 0.359 | 0.250 | 1,313 → 514 ms |
| q24b | 0.315 | 0.249 | 1,314 → 361 ms |
| q09 | 0.749 | 0.635 | 778 → 510 ms |
| q74 | 0.495 | 0.501 | 2,745 → 1,297 ms |
| q57 | 0.530 | 0.412 | 3,947 → 1,765 ms |
| q11 | 0.496 | 0.529 | 4,603 → 1,838 ms |

For q24a/b, q74, and q11, wall time tracks the shortened critical pipeline closely. q09 retains more wall than its CPU reduction because the longest Nitro pipeline falls less than aggregate CPU. q57 reverses under the synchronized suite state, so its isolated critical-path result is not representative and the JVM-state interaction is the next investigation target.

## Next investigations

1. Reproduce q57 after a minimal prefix/bisection of preceding TPC-DS queries, then capture compilation/deoptimization and code-cache telemetry without JFR.
2. Do the same prefix bisection for q36 and q86; determine whether a preceding query, generated-class churn, or compilation tier transition causes the stable within-run slowdown.
3. Optimize q20's Nitro pipeline-source path only after attributing its 20 ms preheated CPU excess to scan decode, mask construction, dynamic-filter checks, or vector materialization.
4. Attribute q65/q67 wall critical paths because their CPU reductions are not reaching wall time.

The complete per-query board is in `board.csv`; raw logs remain alongside it.
