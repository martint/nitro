# Adaptive source-batch SQL board

This refresh uses JDK 26, the native Parquet datasets, a 12 GiB test heap, one warmup, three measured iterations,
and the committed adaptive 10K-to-40K Nitro Parquet host-boundary policy. The retained Trino rows from the controlled
current board are the denominator. TPC-DS and ClickBench ran Nitro-only in fresh JVMs so their long sequential sweeps
did not also retain a baseline engine in the same heap.

| Suite | Queries | Wall geomean Nitro/Trino | CPU geomean Nitro/Trino | CPU wins | Total CPU Nitro/Trino |
|---|---:|---:|---:|---:|---:|
| TPC-DS, controlled | 103 | 0.496 | 0.365 | 103 | 0.526 |
| ClickBench, controlled | 43 | 0.700 | 0.567 | 43 | 0.699 |
| TPC-H, controlled | 22 | 0.742 | 0.700 | 20 | 0.740 |
| All suites | 168 | 0.571 | 0.445 | 166 | 0.621 |

The TPC-DS sweep initially exposed a correctness defect at the interaction between adaptive batch sizing and deferred
filtered payload. Commit `42123803` confines deferred payload to the current public batch rather than the policy's
eventual capacity. The accepted sweep is entirely after that fix and passes all 103 query variants.

The ratio CSVs apply only established run-order controls. TPC-DS q04 and q22 use the accepted paired controls in the
dynamic-filter board; q67 uses the paired 20,680/21,051 CPU-ms control. The raw sequential measurements for those
queries remain in `tpcds-nitro.log`. ClickBench q30 again suffered its known late-sweep heap-history failure
(45,991 CPU-ms); a fresh two-warmup/five-measurement process reports 8,098 CPU-ms and 1,346 ms median wall, which is
the value used in `clickbench-controlled-ratios.csv`.

Fresh paired controls clear the material residual screen values too. TPC-DS q15 is 0.166x CPU and q78 is 0.738x;
ClickBench q19 is 0.974x CPU and 0.773x median wall. Their logs are retained beside the board. The only remaining
screen rows also clear: TPC-DS q87 is 0.674x CPU/0.803x wall and ClickBench q14 is 0.958x CPU/0.838x wall. Thus every
controlled TPC-DS and ClickBench query is a CPU win. Only the two small TPC-H CPU differences below remain, both with
material wall wins. No JFR, heap dump, or other profiling artifact was created.

Fresh paired two-warmup/five-measurement controls also clear the larger TPC-H screen values. Q03 is 3,148 versus
3,029 mean CPU-ms (1.039x) while Nitro is faster on median wall (650 versus 719 ms). Q16 is 740 versus 707 mean
CPU-ms (1.047x) while Nitro is again faster on median wall (346 versus 457 ms). Both are small CPU differences and
strong wall wins; neither justifies changing the already-faster standalone operators. The paired logs are retained
as `tpch-q03-paired.log` and `tpch-q16-paired.log`.

## Current operator attribution

Fresh paired one-warmup/one-measurement sweeps print every operator's add-input, get-output, and finish CPU. The
operator totals cover 99.8% or more of query-reported CPU in every suite and engine, so there is no material
unattributed SQL-framework CPU bucket. These runs are attribution screens, not replacements for the controlled
performance rows above: TPC-DS spans two fresh processes and ClickBench q30 has the known late-sweep heap-history
distortion.

| Suite | Nitro query CPU ms | Nitro operator CPU ms | Trino query CPU ms | Trino operator CPU ms | Raw CPU ratio |
|---|---:|---:|---:|---:|---:|
| TPC-H, 22 queries | 49,793 | 49,736 | 65,302 | 65,251 | 0.762 |
| TPC-DS, 103 variants | 216,440 | 216,095 | 348,043 | 347,891 | 0.622 |
| ClickBench, 43 queries | 311,834 | 311,553 | 369,234 | 369,165 | 0.845 |
| ClickBench excluding distorted q30 | 265,656 | 265,390 | 348,488 | 348,421 | 0.762 |

TPC-H reproduces the frozen SQL-shaped operator ratio of 0.752x directly. TPC-DS's raw 0.622x attribution screen
is close to the 0.561x SQL-shaped operator ratio, while the controlled SQL board is better at 0.526x. Its leading
families are 48,434 ms Nitro pipeline source plus 7,206 ms page processing versus 109,787 ms Trino scan/filter,
and 40,449 ms Nitro hash join versus 91,968 ms Trino lookup join. Aggregation is the principal dilution: 97,989 ms
across Nitro aggregation families versus 107,328 ms across Trino aggregation families.

ClickBench q30 alone consumed 46,178 Nitro CPU-ms in the sequential attribution run versus its accepted fresh
8,098 ms control. Excluding q30 gives 0.762x CPU, which corroborates the 0.790x SQL-shaped operator ratio; applying
the fresh q30 Nitro result and its retained Trino control gives a 0.731x total before the other established paired
overlays, while the final controlled board is 0.699x. The largest raw families are 241,339 ms across Nitro
aggregation operators versus 156,228 ms across Trino aggregation operators, offset by 55,971 ms Nitro pipeline
source versus 194,355 ms Trino scan/table-scan work and 164 ms versus 4,732 ms TopN. `AggregationSource` fuses the
scan, filter, and partial aggregation, so its CPU is not comparable to Trino aggregation alone. Per-query fused
compute (Nitro source/page/aggregation versus Trino scan/filter/aggregation) wins every raw row except q19 and q40;
both are small screens already cleared by controlled measurements. There is no demonstrated broad aggregation,
scan, or TopN regression to optimize.

The first long-lived TPC-DS attribution attempt produced a one-row q72 top-100 mismatch after q01-q71. An isolated
q72 run, q71-q72, q67-q72, an exact q01-q72 history retry, and twenty consecutive paired q72 parity repetitions in
one shared engine lifetime all passed; the history retry reported 3,948 Nitro versus 22,366 Trino CPU-ms. The single
event is retained as `tpcds-q72-nonreproduced-mismatch.log`, with all controls and the 20x stress log beside it. It is
not causal evidence for a production change, but remains a required stress check in future full paired sweeps.

## Synchronized hardware counters

Each engine ran in a separate fresh 12 GiB JVM with one warmup and one measured invocation per query. The harness
wrote its PID after the first query's warmup and waited until `perf stat` attached. Therefore setup and ANALYZE are
excluded, while the counter interval includes the measured first query and both warmup and measured invocations of
the remaining queries. Query CPU below includes measured invocations only. Events were multiplexed at comparable
rates within each pair. ClickBench commit `1bb90b26` adds the same synchronization seam already used by TPC-H and
TPC-DS; it changes only the benchmark harness. No JFR or heap-dump artifact was created.

| Metric Nitro/Trino | TPC-H | TPC-DS | ClickBench |
|---|---:|---:|---:|
| measured query CPU | 0.780 | 0.622 | 0.830 |
| task-clock | 0.886 | 0.744 | 0.950 |
| cycles | 0.888 | 0.746 | 0.950 |
| instructions | 1.034 | 0.762 | 0.925 |
| branches | 1.066 | 0.786 | 1.006 |
| branch misses | 1.104 | 0.727 | 0.897 |
| cache references | 0.853 | 0.766 | 0.979 |
| cache misses | 0.745 | 0.622 | 0.918 |
| L1 data loads | 1.068 | 0.763 | 0.938 |
| L1 data-load misses | 0.786 | 0.699 | 0.943 |
| dTLB loads | 0.747 | 0.842 | 1.081 |
| dTLB-load misses | 0.654 | 0.778 | 0.794 |

TPC-H's only counter regressions are more instructions/branches and 1.104x branch misses; higher IPC still yields
0.888x cycles, and every measured cache/TLB miss class improves materially. TPC-DS improves every counter class.
ClickBench's counter window is diluted by subsequent warmups and the q30 heap-history effect, but it still reduces
instructions and every miss class; its only material locality tradeoff is 1.081x dTLB loads with just 0.794x dTLB
misses. These results corroborate the operator attribution: current residual work is long-lived-query robustness,
not a general Nitro kernel, aggregation, or scan-locality regression.
