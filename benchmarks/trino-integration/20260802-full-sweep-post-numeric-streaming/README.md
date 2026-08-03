# Nitro/Trino full SQL sweep after nullable numeric streaming

Date: 2026-08-02

This is a matched, end-to-end SF10 sweep over Parquet data. Each engine ran in a
fresh 12 GiB JVM with one warmup and one measured invocation per query, a 4 GiB
query-memory limit, JDK 26, and `task_concurrency=1`. ClickBench contains q01-q43,
TPC-H q01-q22, and TPC-DS all 103 standard query variants. All 168 queries
completed on both engines; every Nitro query reported native Nitro sources.

`perf stat` was attached only after the benchmark CSV header, so ClickBench's
wide `ANALYZE` setup is excluded. Counter totals include warmups and query-phase
framework/JVM work. Query CPU and operator metrics come from Trino query stats
for the measured invocation. The raw logs are the source of record.

## Query board

Ratios are Nitro / Trino; lower is better.

| suite | queries | Nitro wall wins | wall sum ratio | wall geometric ratio | Nitro CPU wins | CPU sum ratio | CPU geometric ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 18 | 0.776 | 0.791 | 20 | 0.763 | 0.719 |
| TPC-DS | 103 | 82 | 0.775 | 0.656 | 94 | 0.567 | 0.432 |
| ClickBench | 43 | 26 | 0.885 | 0.996 | 39 | 0.801 | 0.705 |
| Overall | 168 | 126 | 0.824 | 0.748 | 153 | 0.693 | 0.524 |

Overall measured-query totals are 152.901 s Nitro versus 185.498 s Trino wall,
and 547.014 CPU-s Nitro versus 789.025 CPU-s Trino.

## Query-phase hardware counters

| suite | task-clock | cycles | instructions | branches | branch misses | cache references | cache misses |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 0.899 | 0.901 | 1.051 | 1.082 | 1.159 | 0.876 | 0.799 |
| TPC-DS | 0.712 | 0.713 | 0.776 | 0.811 | 0.732 | 0.723 | 0.606 |
| ClickBench | 0.930 | 0.930 | 0.927 | 1.079 | 0.977 | 0.990 | 0.860 |
| Overall | 0.795 | 0.797 | 0.846 | 0.910 | 0.854 | 0.810 | 0.683 |

Overall raw totals:

| event | Nitro | Trino |
|---|---:|---:|
| task-clock (ms) | 1,377,333.79 | 1,731,938.03 |
| cycles | 7,086,267,941,244 | 8,892,798,410,296 |
| instructions | 20,141,596,025,324 | 23,819,221,521,200 |
| branches | 4,045,166,232,032 | 4,446,724,695,220 |
| branch misses | 31,948,137,650 | 37,399,348,891 |
| cache references | 452,931,700,315 | 559,181,057,184 |
| cache misses | 62,478,978,065 | 91,535,022,997 |

## Operator-level CPU attribution

The logs contain one `operator_cpu` row per operator class and query, including
driver count, add-input/get-output/finish CPU, input/output positions and bytes,
and blocked time. Largest aggregate CPU families are below (milliseconds).
These are attribution totals, not mutually comparable one-for-one classes: a
Nitro source operator can fuse scan, filter/project, and partial aggregation.

| suite | Trino operator family | CPU ms | Nitro operator family | CPU ms |
|---|---|---:|---|---:|
| TPC-H | ScanFilterAndProject | 31,000.8 | PipelineSource | 24,216.8 |
| TPC-H | HashAggregation | 14,489.1 | AggregationSource + Aggregation | 15,294.1 |
| TPC-H | LookupJoin | 13,219.3 | HashJoin | 6,171.1 |
| TPC-H | HashBuilder | 2,270.7 | HashBuild | 1,936.9 |
| TPC-DS | ScanFilterAndProject | 110,609.7 | PipelineSource + PageProcessor | 54,431.7 |
| TPC-DS | HashAggregation | 104,599.7 | AggregationSource + Aggregation | 81,874.6 |
| TPC-DS | LookupJoin | 96,754.8 | HashJoin | 40,728.1 |
| TPC-DS | Window | 5,066.1 | Window | 2,951.2 |
| ClickBench | ScanFilterAndProject | 183,778.1 | PipelineSource + PageProcessor | 57,056.3 |
| ClickBench | HashAggregation + Aggregation | 155,495.7 | AggregationSource + Aggregation | 224,883.6 |
| ClickBench | TopN | 4,923.4 | TopN | 165.4 |

ClickBench's aggregation total includes the q30 long-lived-JVM distortion.
Fresh-JVM q30 previously measured 8.107 Nitro CPU-s and 1.334 s wall, versus
33.878 CPU-s and 4.722 s wall in the 43-query Nitro sequence. The broad sweep is
therefore a board/ranking instrument, not sufficient evidence for changing an
operator implementation.

## Reconciliation against the frozen operator benchmark

The frozen pre-architecture operator harness reported Nitro/Trino geometric
speedups of 1.739x TPC-H, 5.094x TPC-DS, and 2.580x ClickBench. Current SQL CPU
geometric speedups are 1.391x, 2.314x, and 1.418x respectively. The gap is real,
but does not by itself identify slower kernels: SQL retains distributed
partial/final stages, Page exchange boundaries, different parallelism, and in
some cases a different physical plan from the old fixture. Controlled prior
reruns already removed several broad-sweep reversals without operator changes.

Fresh-JVM and interleaved controls completed after the broad sweep:

1. TPC-H q12 reproduced at 1.054x query CPU. Its fused Nitro source CPU is
   slightly lower than Trino scan+join; the small remainder is downstream
   boundary/lifecycle CPU. q21 is a documented physical-plan mismatch.
2. TPC-DS q22's five-measurement interleaved control is a Nitro CPU win:
   0.822x median and 0.821x mean, with 1.076x wall. Separate-JVM startup/JIT
   asymmetry had falsely shown 1.053x CPU.
3. TPC-DS q67 reproduces at 1.070x median and 1.063x mean CPU, with 1.321x
   wall. The remaining CPU is localized to distributed aggregation and ranking;
   scan and join are near parity or better.
4. ClickBench q19/q40/q42 and q30 remain board checks. q42's prior ten-measurement mean was already
   0.961x CPU; q30 is known sequence pressure.
5. Wall-only outliers such as ClickBench q06 and TPC-DS q09 should be evaluated
   as parallelism/critical-path issues when CPU is already lower.

## Raw artifacts

- `*-queryphase-instrumented.log`: per-query timing, query CPU, native-source
  counts, and operator metrics.
- `*-queryphase.perf.csv`: query-phase hardware counter totals.
