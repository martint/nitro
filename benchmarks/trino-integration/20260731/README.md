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

The distributed plan also exposes a capacity-policy mismatch hidden by that
fixture. SQL q67 performs `PARTIAL` decimal aggregation after nine-way `GroupId`,
exchanges varbinary state, and then performs `FINAL` aggregation. The standalone
fixture performs one unbounded `SINGLE` bigint aggregation. Nitro's ordinary flat
grouping policy speculates 64 observed high-cardinality batches ahead, which is
appropriate for the unbounded fixture but makes each memory-bounded partial table
expand aggressively early in its lifecycle.

Nitro commit `86167de7` adds immutable grouping-policy views that retain the same
owner-scoped pool identities. Trino commit `24d47532` gives adaptive partial
aggregation a four-batch view while leaving ordinary and final grouping at the
64-batch default. This is selected from the physical `PARTIAL` lifecycle, not a
query, table, function, or grouping-key identity. The historical grouping cohort
remained within roughly 1.4% of the 64-batch control, q39 was neutral, and q57
improved 3.4%.

The correctness-checked interleaved q67 rerun reports Trino at 5,789.327 ms /
21,412 CPU-ms and Nitro at 8,123.128 ms / 24,270 CPU-ms: 1.403x wall and 1.133x
CPU. The CPU ratio improves from 1.216x to 1.133x. Nitro's full suite passes 1,576
tests with no failures or errors and 566 skips; the complete Trino Nitro core
cohort passes 163 tests.

Long-decimal ordering exposed a separate provider-boundary cost. The structural
kernel SPI previously required operators to read an `Int128` carrier for both
sides of every comparison. Nitro commit `212d530c` adds optional provider-owned
direct vector identity, hash, and comparison handles while keeping operators
logical-type agnostic. Trino commit `0f2fba71` supplies allocation-free two-limb
identity and ordering for long decimals; Top-N itself is unchanged.

The focused operator test proves that ordering uses the direct handles without
calling the carrier reader, Nitro's full suite passes 1,577 tests with 566 skips,
and Trino's Nitro core cohort passes 164 tests. With a 4 GB query cap to avoid
q67's unstable 2 GB boundary, the correctness-checked one-warmup/three-measurement
run reports Trino at 5,764.279 ms / 21,798 CPU-ms and Nitro at 7,837.931 ms /
23,500 CPU-ms: 1.360x wall and 1.078x CPU. Cold plan-node accounting shows the
large ranking node's add-input CPU falling from about 553 ms to 454 ms, versus
about 347 ms for Trino. Its roughly one-second Nitro get-output phase remains;
the carrier materialization hypothesis therefore explains only part of the
ranking gap, and the next investigation must follow the exact ranking lifecycle.

Nitro commit `e8f56e02` removes the ranking loader's object-per-row representation. A row is now a packed
`(pageIndex, position)` long, partition buckets use primitive lists, and partition lookup reuses one mutable probe
while retaining an immutable key only for each observed partition. This removes one `RowReference` and one transient
`PartitionKey` from each of q67's 5.79 million ranking input rows without changing the ranking algorithm or teaching
it about decimals, Trino, or q67. The full Nitro suite passes 1,578 tests with 566 skips, and the Trino Nitro core
cohort passes all 164 tests.

The correctness-checked one-warmup/three-measurement run at the stable 4 GB query cap reports Trino at 5,704.555 ms
p50 wall / 21,425 CPU-ms and Nitro at 7,404.460 ms / 22,538 CPU-ms. The resulting ratios are 1.298x wall and 1.052x
p50 CPU (1.019x mean CPU), improving from 1.360x / 1.078x before packed references. Cold node accounting reduced
the large Nitro ranking node from about 1.48 CPU-s to about 1.04 CPU-s.

An uncommitted streaming-candidate prototype was also tested and discarded. Its corrected O(log N) admission
retained only qualifying rows and required ties, but the same interleaved protocol produced 7,601.718 ms / 22,830
CPU-ms for Nitro versus 5,607.225 ms / 21,698 CPU-ms for Trino: 1.356x wall and 1.052x CPU. Moving ranking work into
host ingestion did not reduce total CPU and worsened the critical path, so the committed implementation remains the
packed blocking algorithm. The residual wall gap with near-parity CPU should be investigated at the distributed
scheduling and pipeline envelope before another ranking rewrite.

## Current TPC-H re-baseline

The original TPC-H regression ranking became stale after the shared scan, grouping, join, decimal, and ranking
changes above. A fresh separate-JVM sweep of all 22 queries on 2026-08-01 used one warmup and three measurements per
engine, the same SF10 Parquet data, a 16 GB heap, and an 8 GB query-memory cap. All Nitro source attempts were
accepted and every query processed the same number of logical input positions as Trino.

Nitro's sum-of-medians ratio is now 0.828x wall and 0.786x CPU; the equal-query geometric means are 0.822x wall and
0.726x CPU. Nitro uses less CPU on 19 of 22 queries. The only material remaining CPU regression is q21 at 1.082x
(7.800 versus 7.210 CPU-s), already localized above to the 60-million-row join. q12 and q13 are effectively CPU
parity at 1.023x and 1.008x, respectively.

In particular, q17 no longer reproduces its original 1.628x wall / 2.755x CPU regression. The full-board run reports
0.591 s / 2.982 CPU-s for Nitro versus 1.408 s / 6.045 CPU-s for Trino (0.420x wall / 0.493x CPU). A preceding
isolated run produced the same conclusion at 0.388x wall / 0.471x CPU. No q17 production change is justified by the
current evidence.

## ClickBench q24 sparse wide-payload follow-up

The q24 plan scans and filters 105 columns before a partial Top-N. Its URL predicate retains only a few rows per
10,000-row source batch, but the Parquet late-materialization policy rejected fragmented numeric skip-decode whenever
a scan had more than six columns or more than eight live numeric payload columns. That forced full numeric payload
decode even though the physical survivor mask was sparse. A live thread dump also found primitive pool misses
allocating and zeroing arrays while holding the pool monitor.

Nitro commit `c3ad4086` moves primitive array allocation on a pool miss outside the synchronized lookup while
preserving bucket, ownership, FIFO, and capacity semantics. Commit `252e440b` raises the connector-owned fragmented
numeric width bounds to 128 while retaining the existing six-percent survivor guard, numeric/dictionary proof, and
immutable construction policy. The full Nitro suite passes 1,578 tests with no failures or errors and 566 skips.

The no-override, correctness-checked q24 confirmation with one warmup and three interleaved measurements reports Nitro
at 6,784.437 ms / 28,022 CPU-ms and Trino at 5,421.274 ms / 34,510 CPU-ms. The resulting 1.251x wall / 0.812x CPU
ratios substantially improve the original 2.020x / 1.130x result. Nitro now uses about 19% less CPU, so the remaining
wall gap is a critical-path and source-parallelism issue rather than function adaptation or excess aggregate work.

`clickbench-post-wide-skip.csv` records a complete 43-query same-JVM screen after this change. It is useful for finding
current candidates but is not a replacement for isolated confirmations: q30 followed the allocation-heavy q29 and
reported 4,440.911 ms / 32,189 CPU-ms in the screen, while an immediately following fresh-JVM run with the same
defaults reported 1,417.113 ms / 8,640 CPU-ms. The q30 plan has one physical scan column and does not qualify for the
new fragmented-width path. No operator change is justified by the noisy broad row.

The screen also makes the next connector mismatch explicit. ClickBench q37--43 read about 100 million logical
positions through the Nitro source, while the Trino Parquet path reports about 1.4 million physical input positions
after predicate/statistics pruning. Their large ratio reversals therefore compare different scan work. Static filter
domains need an explicit optimizer-to-connector SPI path so Nitro Parquet can prune row groups without teaching the
connector about SQL expressions or benchmark queries.

## ClickBench q37--43 static-domain follow-up

Nitro commit `cfd0912e` gives connector-neutral long domains a conservative row-group overlap capability and lets the
Parquet connector reject disjoint INT32/INT64 row groups. Trino commit `5da7da15` passes the Hive table handle's compact
effective predicate into each native Nitro source independently of the completed dynamic-filter snapshot. This is a
host/connector boundary change: neither the execution engine nor an operator recognizes SQL expressions, Hive,
ClickBench, tables, or columns.

The first end-to-end pass closed the physical-work mismatch for mixed-payload q37--40. It also showed that all-numeric
q41--43 still took the row-level filter-window path without consulting row-group metadata. Nitro commit `7dd2a8e8`
now applies row-group rejection before those windows and aligns each accepted window to a row-group boundary. The
variable window sizes exposed and fixed a stale nullable-payload scratch reference when a later row group was larger
than the first. Focused Parquet tests pass 83/83, including explicit pruning and growing-scratch regressions; Nitro's
full suite passes 1,582 tests with no failures or errors and 566 skips.

All q37--43 queries now report the same 1,406,798 input positions for Nitro and Trino. Selected controlled results are:
q37 uses less Nitro CPU (287 versus 347 CPU-ms) despite 1.514x wall time; q41 is approximately CPU parity; q42 remains
1.737x CPU; and q43 reports 262.271 ms / 178 CPU-ms for Nitro versus 176.837 ms / 151 CPU-ms for Trino (1.483x wall,
1.179x CPU). q39 remains the dominant outlier at roughly 13.9x CPU in the short screen. Because scan work is now equal,
q39 and then q42 are valid downstream profiling targets. No downstream operator change is justified until their exact
plan-node CPU and lifecycle are compared with the corresponding operator fixtures.

## ClickBench q39 broad-domain compilation follow-up

The q39 operator attribution exonerated grouping and Top-N: nearly all excess CPU was charged to the split-scoped
native source. A frame-pointer perf profile then identified the integration boundary rather than Parquet decoding:
`SortedRangeSet.tryExpandRanges` and its iterator machinery dominated samples, while Nitro page decode was about
0.1%. The `IsLink <> 0` static predicate has two broad integral ranges. The Trino adapter attempted to enumerate up to
one million values before declining the domain, independently for each of 165 splits.

Trino commit `8e0ec290` computes integral range cardinality from bounds before asking Trino to enumerate it. Small
domains retain the same dense-bitset/hash-set compilation; broad domains immediately use the existing exact Trino
domain fallback. This is a general host-boundary fix and does not alter Nitro scan, aggregation, or Top-N operators.
The focused adapter tests pass 7/7.

The controlled q39 run with one warmup and ten interleaved measurements reports Nitro at 146.263 ms / 195 CPU-ms and
Trino at 95.246 ms / 182 CPU-ms: 1.536x wall, 1.071x CPU by median and 0.999x CPU by mean. Before the fix, Nitro used
roughly 2.3--2.7 CPU-seconds per execution and was 13--14x behind. q39 is therefore no longer a CPU regression; its
remaining wall difference should be treated as split/scheduling critical-path work, not an operator-kernel target.

## ClickBench q42 split-metadata follow-up

The post-q39 controlled q42 baseline still reported 171.216 ms / 238 CPU-ms for Nitro versus 83.363 ms / 138
CPU-ms for Trino (2.054x wall, 1.725x CPU). Operator attribution showed 165 split-scoped native sources plus
distributed partial/final aggregation and three Top-N plan levels. This is not the single-source,
single-aggregation, single-TopN topology used by the operator fixture.

A frame-pointer profile found repeated Parquet Thrift footer decoding as the largest identifiable source cost:
`TCompactProtocol.readByte`, column metadata, varint, statistics, and schema decoding together exceeded the direct
page-decoder samples. Each split source mapped and reparsed its physical file even when sibling splits and later
queries had already parsed the same immutable file version.

Nitro commit `4016f2a5` adds a bounded, connector-instance-owned metadata cache to `NitroParquetScanResources`.
Identity includes normalized path, file size, and modification time. Concurrent splits for one version share a
single future; misses for different files load concurrently; the immutable maximum-entry policy is supplied during
resource construction. Mappings and decoder state remain source-owned. Focused tests pass 85/85 and the full Nitro
suite passes 1,584 tests with no failures or errors and 566 skips.

With one warmup and ten interleaved measurements after the change, q42 reports 143.677 ms / 173 CPU-ms for Nitro and
84.319 ms / 146 CPU-ms for Trino. Median ratios improve to 1.704x wall and 1.185x CPU; mean CPU is 0.961x. The cache
therefore removes most of the SQL-only CPU reversal without changing aggregation or TopN implementations. The
remaining wall gap and small median CPU excess require a fresh board/ranking before another production target.

## ClickBench sweep source-close follow-up

The first post-cache broad sweep completed q01--q18, then q19 reached the test's 1 GB per-query memory limit. Query
failure can close the same connector source concurrently from cancellation and driver-teardown paths. Although the
Parquet source already ignored sequential duplicate closes, its boolean guard did not make concurrent close
idempotent; both callers could release the same allocator lease.

Nitro commit `f3160f3f` serializes the short source-close transition and extends the shared-arena lifecycle test with
two concurrent close callers. Focused Parquet tests pass 83/83 and the full Nitro suite passes 1,584 tests with no
failures or errors and 566 skips. This is a general connector lifecycle correction, not a benchmark or operator
optimization. The performance sweep resumes at q19 with the established 4 GB ClickBench no-spill envelope.

## ClickBench boolean-negation lowering

The post-cache board's largest equal-input CPU reversals clustered around SQL `<>` predicates. Trino represents
`a <> b` as a resolved `$not($operator$equal(a, b))` call. The Nitro boundary previously lowered both calls as
ordinary scalar assignments, so filtering materialized the equality Boolean vector and then negated it. Trino commit
`eb4a5318` records the host function's semantic negation as a Nitro `NotMask`; the engine remains function-neutral
and can select the registry-provided UTF-8 literal mask directly, including the false outcome.

The controlled q26 confirmation with one warmup and five interleaved measurements reports Nitro at 329.175 ms /
1,547 CPU-ms and Trino at 368.625 ms / 2,100 CPU-ms: 0.893x wall and 0.737x CPU. The preceding stale-artifact run
was 1.482x wall / 1.714x CPU. The same lowering changes q11, q12, q25, q27, q28, and q30 from CPU regressions to CPU
wins. Later confirmations report q13/q14/q15 at 0.871x/0.815x/0.887x median CPU and q31/q32 at 0.792x/0.850x,
removing the remaining stale `<>` board regressions. `clickbench-mask-regexp-targeted.csv` contains the one-warmup,
three-measurement follow-up rows.

## ClickBench q29 bound-regexp adaptation

The q29 SQL boundary bound Trino's constant Joni pattern but still invoked `JoniRegexpFunctions.regexpReplace` for
every value. The operator fixture instead selected Nitro's exact, newline-correct host-extraction implementation and
preserved dictionary encoding. Trino commit `d57d64b9` recognizes the same constant pattern and replacement at the
function-adaptation boundary and binds `ExtractHostUtf8`; dynamic replacements retain the generic Joni path and its
error behavior. Edge cases cover final and embedded line feeds, empty authorities, absent paths, and nulls.

With one warmup and three interleaved measurements, q29 improves from 8,276.731 ms / 55,045 CPU-ms to 2,910.821 ms /
18,429 CPU-ms for Nitro, while Trino reports 8,521.009 ms / 53,670 CPU-ms. The resulting 0.342x wall / 0.343x CPU
ratio is a 3x SQL-engine win. It does not reproduce the old operator ratio of roughly 0.11x because the real Trino SQL
path is itself about 2.8x faster than the hand-built Trino operator fixture (about 54 CPU-s rather than 150 s). Nitro
SQL's 18.4 CPU-s is close to the Nitro fixture's 16.1 s absolute duration, so the remaining ratio difference is a
comparison-fixture mismatch rather than a missing Nitro operator speedup.

## Current ClickBench re-baseline

Overlaying the controlled boolean-mask and q29 regexp confirmations on the complete post-metadata-cache board gives
Nitro 40 CPU wins and 28 wall-time wins across 43 queries. The sum of per-query medians is 276.920 CPU-s for Nitro
versus 369.412 CPU-s for Trino, a 0.750x CPU ratio; the corresponding wall-time ratio is 0.808x. This replaces the
original board's 1.151x aggregate CPU regression. The remaining rows at or above CPU parity are q19 at 1.015x, q40
at 1.119x, and q42 at 1.306x.

Those three ratios do not identify a missing Nitro operator speedup. Q19 is within ordinary broad-screen noise. For
q40, Nitro SQL uses about 0.49 CPU-s, already below the historical Nitro operator fixture's roughly 0.64 s, while
real Trino SQL uses about 0.44 CPU-s instead of the Trino fixture's roughly 11.44 s. For q42, Nitro SQL uses about
0.16--0.17 CPU-s, below the historical Nitro fixture's roughly 0.23 s, while real Trino SQL uses about 0.12--0.15
CPU-s instead of the Trino fixture's roughly 1.19 s. The controlled ten-measurement q42 confirmation has a 1.185x
median CPU ratio but a 0.961x mean CPU ratio. In both cases Nitro SQL has reproduced or exceeded the absolute Nitro
operator result; it is the hand-built Trino comparison fixture that does not reproduce Trino SQL's optimized path.

Accordingly, no q40/q42 operator rewrite is justified by this board. The remaining ClickBench work is critical-path
and split-scheduling analysis for wall-time gaps, plus isolated confirmation of any future CPU candidate. Production
changes should continue to target measured SQL integration boundaries rather than attempting to force ratios toward
an inapplicable Trino fixture baseline.

## TPC-DS q01/q44 encoded-input correctness follow-up

A fresh current TPC-DS ranking pass exposed two representation contracts that the isolated fixtures did not cover.
Q01 fed a dictionary over Nitro's two-limb long-decimal struct into the Trino-owned decimal sum implementation. Trino
commit `85485fd1` makes long-decimal sum, average, extrema, and any-value providers project struct fields through
Nitro's encoding-preserving `VectorAccess` API instead of casting the outer vector. The 169-test Trino Nitro core
cohort passes, and q01 is exact at 0.676x Trino CPU in a cold one-shot check.

Q44's `ss_addr_sk IS NULL` static domain carried an empty non-null long-value set plus `includesNull = true`. The
Parquet filter-window path consumed only the optional `LongDomain` value capability and therefore treated the domain
as empty, eliminating the scalar-average input. Nitro commit `e0090591` leaves null-inclusive typed domains as
residuals until the connector has an explicitly null-aware encoded-domain protocol; ordinary non-null domains retain
their existing row-level and row-group pruning. The focused Parquet cohort passes 102 tests, the full Nitro suite
passes 1,585 tests with 566 skips, and q44 is exact at 0.514x Trino CPU in a cold one-shot check.

Both fixes are boundary adaptations. Neither changes aggregation, ranking, join, or scan-decoding kernels.
