# Adaptive compact count state

COUNT and AVG-count state now starts as unsigned-byte 4,096-position chunks and promotes only an overflowing chunk to
full-width longs. This is an exact, function-owned representation choice; it has no query, table, column, or Trino type
knowledge. The allocator binds dynamic retained-byte accounting whenever it allocates, adopts, or reuses such a
vector, so adapters do not need implementation-specific lifecycle calls.

The five-warmup/three-measurement ClickBench q33 A/B used the native Parquet input, fresh 12 GiB JVMs, an 8 GiB query
limit, exact result comparison, thread-allocation accounting, and sampled query peak memory.

| revision | p50 wall ms | mean CPU ms | allocation MiB | query peak MiB |
|---|---:|---:|---:|---:|
| compact candidate | 8,428.889 | 17,510.000 | 25,893.928 | 4,205.407 |
| adjacent parent | 8,376.926 | 17,696.667 | 30,456.885 | 5,540.547 |
| candidate / parent | 1.006 | 0.989 | 0.850 | 0.759 |

The final same-JVM backend comparison confirms the candidate remains an end-to-end win on q33: Nitro measured
8,197.156 ms p50 wall / 17,403.333 mean CPU-ms / 29,270.704 MiB allocated / 4,205.408 MiB query peak, versus
Trino at 8,981.576 / 19,273.333 / 28,382.757 / 4,834.116. The Nitro/Trino wall and CPU ratios were 0.913 and
0.903 respectively. The allocation samples are thread-local and therefore do not include all worker allocation;
query peak is the reliable retained-memory comparison here.

The final warmed SF10 TPC-H q01 accounting guardrail used five warmups and three alternating measurements. Nitro was
403.928 ms p50 wall / 2,197.000 mean CPU-ms / 302.064 MiB allocated; Trino was 469.399 / 2,732.333 / 7,569.896.
Results were exact and allocator teardown was clean. The XML preserves the complete command and measurements.

A final five-warmup/three-measurement TPC-DS q23a control also passed exact comparison. Nitro measured 5,790.987 ms
p50 wall / 26,430 mean CPU-ms / 46,875.627 MiB allocated / 2,513.912 MiB sampled query peak, versus Trino at
6,037.403 / 28,086 / 71,981.750 / 2,489.069. The former q23a wall and CPU lag therefore does not reproduce on the
committed DATE plus compact-count stack; sampled query peak is now within 1% in this paired process.

The complete Nitro suite passes 1,683 tests with zero failures and 567 skipped. The Trino Nitro cohort passes 242
tests. No JFR, heap dump, or Kata artifact was created.
