# Structural SQL profiling

This investigation used the current clean Nitro `2a38b423` and Trino integration `8d6085f6`, JDK 26, a 12 GiB
test JVM, an 8 GiB per-query limit, the native Nitro Parquet reader, and complete query warmups before every measured
window. It retained JFR recordings because this investigation explicitly requested JFR; it created no heap dumps.

## Conclusions

1. **The largest actionable integration gap is fused scan+aggregation egress.**
   `TrinoNitroPipelineSourceOperator` implements the native exchange-output contract, while
   `TrinoNitroAggregationSourceOperator` does not. ClickBench q35 therefore materializes partial aggregation output
   through `TrinoNitroAggregationOperator.toPage()` before `PartitionedOutputOperator`, even though the downstream
   exchange is Nitro-capable. Exact allocation stacks show the wide URL payload passing through a Nitro
   `BinaryVector`, a Trino `VariableWidthBlock`, partition appenders, and finally remote serialization. The right
   structural repair is to give the fused aggregation source the same native handoff as the stateless fused source,
   preferably by consolidating the two source paths rather than adding another one-off bridge.

2. **Humongous allocation, not insufficient heap, explains a material wall/CPU disconnect.**
   In the ten-measurement q35 JFR window Nitro performed 84 old collections, including 27 full compactions; Trino
   performed 29 old collections and no full compactions. Nitro recorded 120 GC causes attributed to humongous
   allocation versus 18 for Trino. Stop-the-world pauses totalled 6.42 s versus 4.07 s. Nitro nevertheless allocated
   only 38.8 GiB/query versus Trino's 69.2 GiB/query and remained faster, so this is a representation/lifetime problem,
   not a general allocation-volume regression.

3. **Adaptive partial output defeats Nitro's ordinary output batch bound.**
   The default aggregation output limit is 16,384 rows, but both Trino aggregation integration paths pass
   `Integer.MAX_VALUE` when adaptive partial aggregation is present, and `GroupedAggregationSession.flush()` also
   emits an entire generation through `finishInput()`. Exact q35 events include Nitro output byte arrays as large as
   65.3 MiB. A correct repair needs a drainable, bounded sequence of batches for a flushed generation, retaining that
   generation until all chunks have crossed the boundary. Merely lowering a process property cannot affect this path.

4. **Variable-width flat grouping is the largest remaining Nitro kernel cost, but it is still a net win.**
   q19 and q35 spend most sampled Nitro CPU in `FlatGroupingTable` probe/equality, `FlatKeyLayout` binary
   hash/write/equality, arena retention, and rehash. Nitro uses more instructions but achieves materially higher IPC,
   fewer branch misses, lower allocation, and lower wall time. The next kernel opportunities are fewer growth/rehash
   generations from a planner-provided cardinality hint and exact constant/RLE lane handling; neither justifies a
   query-specific implementation.

5. **The apparent TPC-DS q57 wall regression was suite-history noise.**
   A fresh paired JFR run measured Nitro/Trino at 0.519x wall, 0.426x CPU, 0.533x allocation, and 0.250x peak query
   memory. Nitro's join/gather paths are cheaper. Its blocked-time counters are predominantly downstream backpressure,
   not CPU lost inside the join, and cannot be added to infer critical-path delay.

6. **ClickBench q40 is a small scheduling/utilization edge, not a microarchitectural collapse.**
   The earlier 500-measurement steady capture remains 1.132x wall and 1.035x CPU. Nitro averages 4.02 active CPUs
   versus 4.23 for Trino, with essentially equal IPC and a better branch-miss rate. No operator rewrite is supported by
   that evidence.

## Controlled profiles

| query | Nitro wall | Trino wall | wall ratio | CPU ratio | allocation ratio | principal finding |
|---|---:|---:|---:|---:|---:|---|
| ClickBench q19 | 5,339.5 ms | 6,427.7 ms | 0.831x | 1.033x | 0.693x | same flat variable-width grouping shape; extra Nitro hash/record work, but lower wall/allocation |
| ClickBench q35 | 5,305.5 ms | 6,930.7 ms | 0.766x | 0.837x | 0.561x | fused aggregation-source egress copies and humongous/full-GC pressure |
| TPC-DS q57 | 1,918.4 ms | 3,692.8 ms | 0.519x | 0.426x | 0.533x | raw board wall loss does not reproduce; Nitro join remains substantially cheaper |

The adjacent q34/q35 control isolates the redundant literal grouping lane. Adding `1` to the otherwise identical URL
grouping shape raises Nitro CPU by 10.8% and Trino CPU by 4.1%, a 6.7 percentage-point Nitro excess. The current flat
layout has an execution-stable one-run binary specialization but no corresponding one-run long specialization, so the
long accessor still reaches `RleVector.runIndexFromHint`. This is real but lower priority than removing the egress
materializations. Any retained fix must remain exact when a later batch changes the constant.

## Exact q35 allocation evidence

An extra three-measurement recording enabled exact `ObjectAllocationOutsideTLAB` events only after five warmups. The
largest allocations of at least 512 KiB were:

| allocation path | total | events | largest |
|---|---:|---:|---:|
| Nitro `BinaryVector.allocateStorage` | 24,105.6 MiB | 825 | 65.3 MiB |
| Trino partition `VariableWidthBlockBuilder.ensureFreeSpace` | 19,696.3 MiB | 1,305 | 23.6 MiB |
| remote page serializer write buffers | 11,852.6 MiB | 6,861 | 2.4 MiB |
| remote page reader buffers | 11,653.5 MiB | 8,203 | 2.0 MiB |
| Nitro-to-Trino binary `flatBlock` | 10,927.1 MiB | 617 | 31.3 MiB |
| remote variable-width block decode | 6,361.1 MiB | 4,809 | 1.9 MiB |
| Nitro flat grouping variable-width arena | 2,581.0 MiB | 2,581 | 1.0 MiB |

The first, second, and fifth rows are avoidable representation stages. The serializer/read buffers remain until the
remote protocol itself becomes vector-native or can serialize Nitro vectors directly at the explicit boundary.

## Hardware counters

The q35 counters cover exactly five post-warmup measurements. Nitro is 0.763x elapsed and 0.898x whole-process task
clock, but executes 1.228x as many instructions. It compensates with 2.27 IPC versus 1.66, a 1.05% versus 1.98%
branch-miss rate, and 0.873x absolute cache misses. Nitro incurs 219,645 page faults versus 8,907 (24.7x), consistent
with repeatedly allocating and touching humongous output arrays. Its average process utilization is 6.78 versus 5.76
CPUs. Query-attributed CPU is 24.656 versus 29.424 CPU-s/query; whole-process task clock is 35.35 versus 39.37
CPU-s/query, exposing about 0.75 CPU-s/query more non-query JVM work on Nitro, principally GC and allocation support.

The q19 attach windows are not equal-length, so absolute counters are not compared. Normalized by task clock, Nitro
retires about 14.68 billion instructions/CPU-s versus 12.41 for Trino. Combined with measured query CPU this implies
roughly 14% more instructions/query, consistent with JFR's flat-key hash/write/equality and rehash attribution.

## Priority order

1. Add native exchange output to fused scan+aggregation and consolidate it with the existing terminal-based source
   path; gate with q34/q35 plus a broad activation/correctness sweep.
2. Make adaptive partial generations drain through bounded output batches without ending their ownership lifetime
   early. Size by retained bytes as well as rows so wide binary output stays below a humongous-allocation horizon.
3. Add a direct Nitro-vector remote serialization boundary, or an ownership-safe zero-copy variable-width block
   adapter, to eliminate the Nitro-to-Block copy and partition-builder copy.
4. Add exact one-run long access at the layout batch boundary, then evaluate whether optimizer-proven constant
   grouping lanes can be omitted and reconstructed without putting a physical rewrite in the execution engine.
5. Pass trustworthy cardinality/capacity estimates from the optimizer to flat grouping construction to reduce
   rehash/growth. Do not infer query identities or sample values as correctness assumptions.

No production source was changed in this diagnostic slice. Both JJ worktrees remain clean.
