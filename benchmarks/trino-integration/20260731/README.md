# Trino/Nitro full-suite baseline — 2026-07-31

## Scope and method

- Trino integration parent: `ddc606aa95e0 Run benchmark suites fully through Nitro`.
- Nitro integration dependency: `f0e329c4 Reuse dense aggregate fallback materialization`.
- Persisted Parquet inputs: TPC-H SF10, TPC-DS SF10, and the existing ClickBench data set.
- JDK: Temurin 26.0.1; mvnd 1.0.6 with `-Dmaven.gitcommitid.skip=true`.
- Host visible to the container: AMD Ryzen 7 9800X3D, 8 cores/8 threads, one NUMA node.
- Each engine was run in its own fresh JVM for bounded query ranges.
- Broad sweep: one warmup and three measurements per query; medians are reported.
- Heap: 16 GB. Query memory: 8 GB for TPC-H and 4 GB for TPC-DS/ClickBench.
- Tables were analyzed before measurement. Spilling was not enabled.
- Heap-dump generation was disabled and no JFR artifacts were created.
- Native Parquet-source counts were checked for Nitro. All 168 Nitro query variants had positive native-source counts.

Two visibly noisy broad-sweep comparisons were repeated with both engines interleaved in one JVM, three warmups, and ten measurements:

| Query | Trino wall p50 | Nitro wall p50 | Wall ratio | CPU ratio |
|---|---:|---:|---:|---:|
| TPC-DS q42 | 565.888 ms | 128.360 ms | 0.227x | 0.188x |
| ClickBench q31 | 1109.732 ms | 1235.912 ms | 1.114x | 1.193x |

The aggregate results below use those confirmations in place of the noisy broad rows.

## Suite summary

Ratios are Nitro divided by Trino; lower is better. “Sum ratio” divides the sum of per-query medians and therefore weights expensive queries more heavily. The geometric mean weights queries equally.

| Suite | Queries | Wall sum | CPU sum | Wall geomean | CPU geomean | Nitro wall wins | Nitro CPU wins | Wall regressions >5% | CPU regressions >5% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 1.816x | 1.757x | 1.256x | 1.264x | 8 | 6 | 14 | 15 |
| TPC-DS | 103 | 1.100x | 0.901x | 0.707x | 0.539x | 69 | 81 | 30 | 20 |
| ClickBench | 43 | 1.129x | 1.151x | 1.253x | 1.217x | 15 | 19 | 24 | 21 |

TPC-DS demonstrates that Nitro's operator advantages do survive integration for many query shapes: 81 of 103 variants use less CPU, and aggregate CPU is about 10% lower. However, a small set of expensive regressions raises aggregate wall time above Trino. TPC-H and ClickBench currently regress in both aggregate wall and CPU.

## Highest-impact regressions

This ranking prioritizes absolute CPU and elapsed-time cost, not tiny-query ratios.

| Query | Wall ratio | CPU ratio | Excess wall p50 | Excess CPU p50 | Initial classification |
|---|---:|---:|---:|---:|---|
| ClickBench q30 | 3.711x | 4.028x | 8.42 s | 64.11 s | kernel/work amplification |
| TPC-DS q22 | 3.288x | 2.732x | 12.61 s | 32.33 s | kernel/work amplification |
| TPC-DS q67 | 2.610x | 2.046x | 8.59 s | 22.27 s | kernel/work amplification |
| TPC-DS q51 | 4.611x | 3.415x | 11.11 s | 20.59 s | kernel/work amplification |
| TPC-H q09 | 2.852x | 2.430x | 8.77 s | 18.59 s | kernel/work amplification |
| TPC-H q17 | 1.628x | 2.755x | 1.34 s | 12.97 s | kernel/work amplification |
| TPC-DS q47 | 2.326x | 1.642x | 4.49 s | 11.43 s | mixed CPU and pipeline loss |
| TPC-H q18 | 2.190x | 2.849x | 1.43 s | 9.37 s | kernel/work amplification |
| TPC-H q13 | 3.270x | 2.596x | 4.28 s | 7.50 s | kernel/work amplification |
| TPC-H q08 | 2.311x | 3.065x | 1.24 s | 6.99 s | kernel/work amplification |

Important elapsed/CPU mismatches should be investigated separately:

- TPC-DS q08: 4.656x wall but 1.787x CPU.
- TPC-DS q09: 2.697x wall while Nitro uses only 0.538x CPU.
- ClickBench q06: 3.465x wall with 1.022x CPU.
- ClickBench q24: 2.020x wall with 1.130x CPU.
- ClickBench q05: 2.114x wall while Nitro uses 0.856x CPU.

These are unlikely to be explained primarily by function adaptation. Their ratios point first to pipeline shape, driver concurrency, blocking/synchronization, or insufficient parallel work admission.

## Comparison with operator measurements

The earlier JDK 26 operator sweep has exact query IDs for all 22 TPC-H queries, 95 of the 103 TPC-DS variants, and all 43 measured ClickBench queries: 160 exact matches total. TPC-DS q14a/b, q23a/b, q24a/b, and q39a/b deliberately have no exact match to the operator sweep's unsuffixed IDs.

The end-to-end results are far from the operator ratios for many queries. Examples:

| Query | End-to-end wall ratio | Operator duration ratio |
|---|---:|---:|
| TPC-DS q08 | 4.656x | 0.032x |
| TPC-DS q51 | 4.611x | 0.260x |
| TPC-DS q84 | 5.677x | 0.397x |
| ClickBench q30 | 3.711x | 0.277x |
| TPC-H q09 | 2.852x | 0.402x |

The operator benchmark and Trino integration are not identical workloads or execution envelopes, so their absolute times are not compared. The Nitro/Trino ratios are still useful diagnostics: reversals this large mean integration-level work, scheduling, materialization, or a different physical shape is overwhelming the isolated operator advantage.

## Artifacts

- `tpch.csv`: raw broad TPC-H rows.
- `tpcds.csv`: raw broad TPC-DS rows.
- `clickbench.csv`: raw broad ClickBench rows.
- `confirmations.csv`: ten-measurement reruns for noisy comparisons.
- `ratios.csv`: broad end-to-end ratios joined with exact-query operator duration and cycle ratios.

## Investigation order

1. Profile and compare plans/work counters for ClickBench q30, TPC-DS q22/q51/q67, and TPC-H q09.
2. Diagnose elapsed/CPU divergence for TPC-DS q08/q09 and ClickBench q05/q06/q24.
3. Check join/grouping cardinality, input/output positions, driver concurrency, and materialization boundaries before changing kernels.
4. Re-run the affected query and its closest operator benchmark after each fix.

## ClickBench q30 follow-up

The first investigation found that the isolated operator workload did not preserve the SQL expression's logical
integer arithmetic. It benchmarked already-widened, unchecked long additions, while the Trino plan performs checked
`INTEGER + INTEGER` and only then widens the result for `sum`. The integration adapter also recognized only flat
`I64Vector` inputs for its column/constant fast path, but the native Parquet reader correctly produces the compact
`I32Vector` representation for this column. The resulting generic null/error loop accounted for the original
regression.

Trino integration commit `f076a2dd` accepts either legal flat representation, specializes compact checked arithmetic
without per-position carrier conversion, and retains lazy overflow-error materialization. The focused correctness
suite passed 51 tests. A one-warmup, three-measurement Nitro run after the final specialization reported 1.471 s wall
and 8.200 CPU-s. Against the interleaved Trino reference above (3.239 s and 20.837 CPU-s), this is approximately 0.45x
wall and 0.39x CPU. Thus q30 changed from the baseline's 3.71x wall / 4.03x CPU regression to a clear Nitro win.

The residual difference from the isolated 0.277x duration ratio is now explained rather than an unidentified engine
loss: the isolated Nitro expression uses unchecked generated arithmetic, while the SQL expression requires checked
overflow semantics. Nitro's current projection-code SPI can describe value and null programs but cannot describe
error-producing operations. A future reusable extension should let registry-owned function providers contribute
generated error-aware projections without teaching the engine about Trino functions or arithmetic semantics.

## TPC-DS q22 follow-up

The q22 regression came primarily from an integration boundary between `GroupId` and partial aggregation. Trino
materialized all five grouping-set expansions as Blocks, and the aggregation adapter immediately converted those
Blocks back into Nitro vectors. Nitro commit `f88c68fb` first made dense grouping-set value expansion share immutable
dictionary views. Commit `9a55c87e` then made a GroupId pipeline distinguish temporary exhaustion of an externally
scheduled `BatchFeedOperator` from permanent source completion. The full Nitro suite passed 1,565 tests with no
failures or errors and 566 skips.

Trino integration commit `91809c72` fuses GroupId into the Nitro aggregation pipeline. It also accounts logical input
bytes for every pipeline output batch and prevents adaptive partial aggregation from passing expanded grouping-set
rows directly to the exchange. Without those controls, q22 emitted 14--48 million partial rows depending on driver
timing and could exceed the 4 GB query-memory limit.

Nitro's composite string grouping state needs slightly more than Trino's default 16 MB partial-aggregation buffer to
retain q22's complete local group domain. Controlled runs at 20, 24, 32, and 64 MB all emitted exactly 407,521
aggregation rows, matching Trino; 16 MB caused repeated flushes and work amplification. Trino commit `fd50465f`
therefore adds an immutable, composition-owned `TrinoNitroAggregationExecutionPolicy` and uses its default 2x memory
scale when adapting Trino's partial-aggregation limit. This is an engine-layout capacity adapter, not a query-specific
setting, and periodic adaptive flushes continue to bound each operator instance.

The final correctness-checked, interleaved run used one warmup and three measurements:

| Engine | Wall p50 | CPU p50 | Aggregation output rows |
|---|---:|---:|---:|
| Trino | 6.145 s | 16.505 CPU-s | 407,521 |
| Nitro | 7.953 s | 16.649 CPU-s | 407,521 |

The final Nitro/Trino ratios are 1.294x wall and 1.009x CPU, down from the baseline's 3.288x wall and 2.732x CPU.
Aggregation CPU is now close to parity (10.26--10.40 CPU-s for Nitro versus 9.95--10.03 CPU-s for Trino). The
remaining elapsed-time gap, despite equal total CPU, points to driver scheduling, pipeline concurrency, and the
scan/join envelope rather than function adaptation or aggregation work amplification.

This slice exposed two reusable abstraction requirements. A host-fed Nitro pipeline needs an explicit distinction
between temporary input exhaustion and terminal completion, and an expanding native pipeline must report logical
output work to host-owned adaptive controls instead of charging only its first output batch.

## TPC-DS q51 follow-up

The q51 regression was concentrated in cumulative decimal windows. The generic registered window adapter originally
materialized a one-row aggregation result and copied it for every output position, and it updated state through a
one-row mask for every input position. Nitro commit `09caf819` adds optional, function-owned position update and
direct result-copy capabilities to the classloader-neutral aggregation SPI. Trino commit `d9fddf81` implements those
capabilities for decimal sum and extrema without exposing decimal semantics to the Nitro engine. This reduced a
Nitro-only q51 run from approximately 13.70 s wall / 28.72 CPU-s to 8.26 s wall / 18.58 CPU-s.

The remaining difference was an execution-shape mismatch with the isolated operator benchmark. That benchmark gave
the window operator one dense page and therefore used Nitro's primitive radix sort. Trino feeds a window through
many native pages, for which Nitro retained packed page/position references but used a comparison merge sort. Nitro
commit `41ccaf31` extends the existing stable radix algorithm to those packed references for general flat integer
partition and ordering keys. It preserves signed, nullable, ascending, descending, and stable ordering semantics and
does not compact or duplicate the buffered pages. The full Nitro suite passed 1,566 tests with no failures or errors
and 566 skips.

A correctness-checked, interleaved run with no warmup and two measurements reported:

| Engine | Wall p50 | CPU p50 | Native sources |
|---|---:|---:|---:|
| Trino | 3.250 s | 7.561 CPU-s | 0 |
| Nitro | 4.272 s | 9.758 CPU-s | 19 |

The final ratios are 1.315x wall and 1.291x p50 CPU (1.273x mean CPU), down from the baseline's 4.611x wall and
3.415x CPU. Across the two measured executions, Nitro's three window nodes used about 1.77, 0.72, and 2.53 CPU-s,
versus Trino's 1.42, 0.70, and 2.13 CPU-s. The isolated operator advantage still does not transfer literally because
the integrated workload retains multi-page indirection and decimal SQL semantics, but the large reversal is now
accounted for rather than being caused by function adaptation or an avoidable comparison-sort path.

## TPC-H q21 native adaptive-partial accounting

The source-fused partial aggregation received zero input bytes from native
`BatchSource` batches. Trino's adaptive controller is byte-sampled, so it never
disabled a near-identity partial aggregation and Nitro repeatedly hashed 1.58
million rows per flush. The Trino boundary now supplies a conservative logical
size from the projected flat row layout, using connector completed-byte deltas
when those are larger. This accounting does not inspect query, table, column,
function, or cardinality identities.

The exact q21 audit passes with 100 rows, 65 native source creations, and zero
fallbacks. One warmup and three measurements improved Nitro from 2,656.731 ms
wall / 9,846 ms query CPU to 2,392.646 ms / 8,231 ms. The fused scan/partial
aggregation fell from about 2.94 CPU-s to 0.86--0.88 CPU-s, now below Trino's
roughly 0.95 CPU-s scan-plus-aggregation stage. Current Trino q21 remains
1,901.301 ms / 6,750 ms; the remaining gap is concentrated in the subsequent
60-million-row join.

## TPC-H q21 multi-batch join follow-up

The remaining q21 join cost had two general causes. Its replicated build retains
multiple batches, so the single-long residual predicate (`<>` or bitwise overlap)
fell through the generic registry-function dispatch for every candidate. Nitro
now resolves primitive long access once per retained batch and caches the probe
value once per row while retaining the same null and residual-filter semantics.

The build also contained 7.31 million unique nonnegative long keys in a bounded
60-million-value physical domain. The existing bounded direct-payload admission
was applicable, but its default minimum expected-row count was 10 million per
driver. Lowering that immutable construction-policy threshold to 5 million
admits the same bounded representation from observed size and domain alone; it
does not inspect query, table, column, or function identity.

The exact q21 audit returns 100 rows with all 65 native-source attempts accepted.
With two warmups and five measurements, Nitro reports 1,897.935 ms median wall
and 7,434 ms median query CPU, versus the current Trino reference of 1,901.301 ms
and 6,750 ms. Nitro therefore reaches wall-time parity while retaining a 10.1%
CPU gap. The same general build admission improves q09 from 1,288.133 ms / 5,864
CPU-ms to 1,055.557 ms / 4,742 CPU-ms, now 28.8% faster in wall time and 30.8%
lower in CPU than Trino's 1,481.877 ms / 6,854 CPU-ms reference.

The complete 22-query Nitro TPC-H screen passes, and the full Nitro suite passes
1,574 tests with no failures or errors and 566 skips.

## TPC-DS q67 current re-baseline

Lowering the general bounded-payload join threshold exposed a latent blocking
operator contract violation: logically equivalent integer fields can arrive as
`I32Vector` or `I64Vector` in different retained batches. Top-N ranking selected
rows across those batches and attempted to append the second representation into
the first representation's output vector. It now normalizes integer values to
the physical prototype captured from the first input batch, with checked
narrowing when that prototype is compact.

The interleaved exact-result run passes after the fix. With one warmup and three
measurements, Trino reports 5,429.688 ms / 21,650 CPU-ms and Nitro reports
7,411.696 ms / 26,322 CPU-ms. The current 1.365x wall and 1.216x CPU ratios are
substantially improved from the original board's 2.610x and 2.046x, but q67
remains an active regression.

Plan-node accounting attributes most remaining excess CPU to the two decimal
aggregation stages (about 3.5 CPU-s combined), Top-N ranking (about 1.2 CPU-s),
and the scan/join envelope. The historical standalone fixture uses unchecked
`I64` sales arithmetic, whereas SQL uses checked decimal multiplication and sum;
its 0.58x duration ratio is therefore not a literal kernel target for this plan.
The full Nitro suite passes 1,575 tests with no failures or errors and 566 skips.
