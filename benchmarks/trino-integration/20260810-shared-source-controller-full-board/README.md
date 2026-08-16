# Shared source adaptive-controller full board

This board qualifies the Trino integration change that preserves one adaptive partial-aggregation controller across
the source operators created by a single operator factory. Nitro remains at `0ac8f52a` (`Model ClickBench aggregation
driver topology`), and the tested Trino parent is `bd131814` (`Limit intact adaptive flushes to grouped sources`). All
suites ran sequentially in separate JVMs with JDK 26, five warmups, three measured executions, exact result comparison,
a 12 GiB heap, an 8 GiB query-memory limit, thread-allocation measurement, sampled peak memory, and plan-node/operator
metrics. Heap-dump generation was explicitly disabled. All 103 TPC-DS variants, 22 TPC-H queries, and 43 ClickBench
queries passed.

| Suite | Queries | Wall geo | CPU geo | Weighted wall | Weighted CPU | Allocation geo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-DS | 103 | 0.5850 | 0.4044 | 0.6948 | 0.5790 | 0.1936 |
| TPC-H | 22 | 0.7202 | 0.6794 | 0.6992 | 0.7110 | 0.1751 |
| ClickBench | 43 | 0.7570 | 0.5862 | 0.6641 | 0.6366 | 0.2647 |

Across all 168 equal-weight query variants, Nitro measures **0.6421x wall, 0.4760x CPU, and 0.2070x valid
allocation**. Workload-weighted wall and CPU are 0.6811x and 0.6175x. The preceding accepted board was 0.6364x /
0.4713x / 0.2060x, with workload-weighted wall and CPU of 0.6862x / 0.6189x. The small geomean movement is not a
material suite regression: Nitro weighted CPU improves slightly, and most changed ratios are caused by short Trino
baselines moving between independent full-suite JVMs.

The motivating TPC-H q21 result improves materially and absolutely. Nitro now measures 1,673.1 ms p50 wall,
6,934.0 mean CPU-ms, and 11,118.2 MiB p50 allocation versus Trino at 1,959.9 ms, 7,241.3 CPU-ms, and 20,440.6 MiB:
0.854x wall, 0.958x CPU, and 0.544x allocation. On the preceding board it was 0.959x wall and 1.190x CPU, with Nitro
using 8,489 CPU-ms. Sharing the controller therefore removes about 1.56 CPU-seconds of repeated high-cardinality
sampling from the same Nitro plan without changing the grouping implementation.

The lifecycle distinction matches Trino. A `PartialAggregationController` belongs to an operator factory and is shared
by the drivers that factory creates. It becomes independent only when the factory is duplicated. Nitro's native-source
output wrapper previously allocated a new controller for every source operator, so q21's 19 source drivers each paid
the admission phase even though the ordinary and native-output objects differed only in output presentation policy.
The wrapper now shares the factory controller; the existing explicit `duplicate()` path continues to create independent
state. A focused lifecycle test covers both properties, and all 251 `TestTrinoNitro*` tests pass.

TPC-H q16 is now the only material TPC-H CPU ratio above parity at 1.071x, although the absolute gap is just 50
CPU-ms. TPC-DS q78 is the largest remaining material CPU target at 1.068x and about 682 excess CPU-ms; q23a is
1.030x with about 594 excess CPU-ms. ClickBench q40 is 1.086x but only 38 excess CPU-ms. ClickBench q43's 1.779x
wall ratio is a 27 ms scheduling-scale gap while Nitro uses 0.834x CPU and 0.347x allocation.

For queries where both sampled query peaks exceed 1 MiB, peak-memory geomean / byte-weighted ratios are 0.6299x /
0.8042x for TPC-DS, 0.2110x / 0.4340x for TPC-H, and 0.8052x / 0.9689x for ClickBench. Maximum sampled Nitro query
peaks are 2,085.4 MiB, 834.9 MiB, and 4,205.4 MiB respectively. Three invalid negative TPC-DS allocation deltas and
one invalid ClickBench delta from thread-retirement accounting are preserved in the raw logs and excluded from the
allocation geomeans. No JFR recording, heap dump, or Kata artifact was created.
