# Grouped native adaptive-flush full board

This board qualifies Nitro `0ac8f52a` (`Model ClickBench aggregation driver topology`) with Trino integration
`95c572f4` (`Preserve native adaptive aggregation flushes`) and `bd131814` (`Limit intact adaptive flushes to grouped
sources`). All suites ran sequentially in separate JVMs with JDK 26, five warmups, three measured executions, exact
result comparison, a 12 GiB heap, an 8 GiB query-memory limit, thread-allocation measurement, sampled peak memory,
and plan-node/operator metrics. Heap-dump generation was explicitly disabled. All 103 TPC-DS variants, 22 TPC-H
queries, and 43 ClickBench queries passed.

| Suite | Queries | Wall geo | CPU geo | Weighted wall | Weighted CPU | Allocation geo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-DS | 103 | 0.5759 | 0.3995 | 0.6885 | 0.5791 | 0.1950 |
| TPC-H | 22 | 0.7150 | 0.6874 | 0.7026 | 0.7412 | 0.1777 |
| ClickBench | 43 | 0.7618 | 0.5771 | 0.6809 | 0.6349 | 0.2545 |

Across all 168 equal-weight query variants, Nitro measures **0.6364x wall, 0.4713x CPU, and 0.2060x valid
allocation**. The preceding accepted board was 0.6490x / 0.4887x / 0.2182x. Workload-weighted wall improves from
0.6902x to 0.6862x and weighted CPU improves from 0.6343x to 0.6189x.

| Suite | Preceding wall/CPU geo | Current wall/CPU geo | Preceding weighted wall/CPU | Current weighted wall/CPU |
| --- | --- | --- | --- | --- |
| TPC-DS | 0.5863 / 0.4131 | 0.5759 / 0.3995 | 0.7047 / 0.5871 | 0.6885 / 0.5791 |
| TPC-H | 0.7405 / 0.6970 | 0.7150 / 0.6874 | 0.7249 / 0.7385 | 0.7026 / 0.7412 |
| ClickBench | 0.7737 / 0.6096 | 0.7618 / 0.5771 | 0.6688 / 0.6602 | 0.6809 / 0.6349 |

The motivating ClickBench q31 now measures 810.6 ms / 3,986 CPU-ms for Nitro versus 1,109.9 ms / 5,403 CPU-ms
for Trino: 0.730x wall and 0.738x CPU. The preceding board was 0.850x / 1.016x. q32 remains stable at 0.909x wall /
0.849x CPU, and the high-cardinality q33 remains favorable at 0.855x / 0.845x. Preserving each memory-bounded grouped
adaptive flush therefore removes the dense slicing/state-retention penalty without changing the operator kernel.

Applying the intact-output policy to ungrouped native source aggregation initially exposed a ClickBench q07
regression. Global aggregation produces only one row per source driver and cannot benefit from intact grouped flushes.
Restricting the policy to grouped sources restores the full-sequence q07 result to 115.7 ms / 384 CPU-ms versus
Trino's 87.4 ms / 422 CPU-ms. A separate five-warmup/five-measurement control measured 130.7 ms / 224 CPU-ms versus
125.4 ms / 432 CPU-ms. The focused q31 control remained favorable at 0.719x wall / 0.762x CPU.

The leading material CPU laggards are now TPC-H q21 (1.190x CPU, 0.959x wall), TPC-H q16 (1.090x / 1.041x),
TPC-DS q23a (1.070x / 1.141x), and ClickBench q40 (1.056x / 1.082x). TPC-DS q20 is 1.280x CPU but only 96 versus
75 CPU-ms and is not a high-impact target. TPC-DS q65, q67, and several similar rows use less CPU but have poorer
wall ratios, leaving parallelism, scheduling, and backpressure as distinct follow-up work from kernel efficiency.

For queries where both sampled query peaks exceed 1 MiB, peak-memory geomean / byte-weighted ratios are 0.6793x /
0.8166x for TPC-DS, 0.3012x / 0.6525x for TPC-H, and 0.7350x / 0.9494x for ClickBench. Maximum sampled Nitro query
peaks are 2,059.2 MiB, 1,235.7 MiB, and 4,205.4 MiB respectively, below the configured query limit.

Thread-retirement accounting produced impossible negative allocation deltas for four TPC-DS rows and two ClickBench
rows. Those raw records are preserved and excluded from allocation geomeans. No JFR recording, heap dump, or Kata
artifact was created by this qualification sweep.
