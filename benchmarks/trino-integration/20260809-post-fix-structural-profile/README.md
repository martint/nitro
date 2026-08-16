# Post-fix structural profile: ClickBench q35

This directory contains the synchronized post-warmup JFR, perf-stat, plan, and
query-harness evidence used to investigate ClickBench q35 after native fused
aggregation egress and bounded adaptive output were in place.

The profile covers five measured Nitro executions after five warmups. Median
query wall time was 5,047.552 ms, median query CPU was 25.270 s, median thread
allocation was 43.462 GiB, median sampled query peak was 3.742 GiB, and median
sampled heap peak was 10.429 GiB. Perf recorded 172.215 CPU-seconds over 26.036
elapsed seconds (6.61 average CPUs), 884.1 billion cycles, 1.955 trillion
instructions, 2.21 IPC, a 1.21% branch-miss rate, and a 9.74% cache-miss rate.
JFR recorded 1.91 seconds of GC pauses across 85 pauses.

The leading Java execution samples were `FlatGroupingTable.identicalKey`
(19.72%), Parquet `decodeNextDataPage` (13.50%), `binaryHash` (9.38%),
`binaryEquals` (4.09%), `FlatKeyLayout.identicalField` (3.10%), and
`FlatGroupingTable.rehash` (2.72%). The profile therefore still identifies
variable-width flat grouping as the dominant kernel, but does not by itself
justify larger eager tables: earlier controlled lookahead/capacity experiments
reduced rehashing while regressing cache and TLB behavior.

The distributed plan exposed a more important structural issue. Q35 grouped on
`[URL, gid]`, where `gid` was the projected literal integer `1`, and used the
same redundant key for repartitioning. A Nitro-gated pre-exchange physical
optimizer now removes projected literal keys when at least one real grouping
key remains. It reconstructs the literal at the latest directly adjacent safe
point; for q35 that is above partial TopN, so the literal is materialized only
for TopN survivors rather than for the complete global group stream.

The final five-warmup/five-measurement acceptance recorded 4,442.284 ms median
wall, 22.621 s median CPU, and 41.450 GiB median allocation. Against the
same-state pre-rewrite control (4,898.578 ms, 25.740 s, 43.449 GiB), this is a
9.3% wall reduction, a 12.1% CPU reduction, and a 4.6% allocation reduction.
Q35 is consequently within 0.6% of q34 wall and matches q34's CPU/allocation
shape, as the operator-level model predicts.

Two controls were rejected. A one-run RLE long accessor specialization was
neutral to negative in paired q34/q35 measurements and was removed. Rebuilding
the literal immediately above aggregation reduced CPU but increased allocation
to 46.069 GiB by materializing it for every group; moving reconstruction above
TopN removed that cost. The rejected and accepted timing logs are retained in
the sibling `20260809-one-run-long-access` directory.

The subsequent full-compute audit found that a physical plan node is not always
evidence of host compute. TPCH q02 and TPC-DS q01 retain equality-free inner
`JoinNode` shells because Trino uses them to wire build/probe driver
dependencies, while the local planner installs Nitro's nested-loop session for
the actual operation. The audit now admits this exact shell only when the
per-query Nitro nested-loop counter proves that its runtime operator was
created. With that correction, exact result and full-compute coverage pass all
22 TPC-H queries and all 103 TPC-DS variants. The real-Parquet ClickBench suite
also passes all 43 queries with 165/165 native Nitro sources and zero native
scan rejection per query.
