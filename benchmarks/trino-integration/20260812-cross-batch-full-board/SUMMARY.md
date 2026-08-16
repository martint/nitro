# Aborted cross-batch-admission continuation

This run tested the 16,384-row initial-cardinality observation candidate under the representative analyzed TPC-DS
plan. TPCH completed all 22 queries. TPC-DS was intentionally stopped after q51 once the decisive q47 result rejected
the candidate. ClickBench was not run. This directory is not a synchronized board.

The protocol was three warmups and three measurements, paired engines, a 12 GiB JVM, an 8 GiB query-memory limit,
exact result comparison, allocation and peak-memory accounting, and per-plan-node operator CPU. Heap-dump generation
was disabled and no JFR was recorded.

TPC-DS q47 remained row-preserving at all three relevant Nitro partial aggregations: each processed and emitted about
6.599 million positions. It measured 2,259.7 ms wall and 11,431 CPU-ms for Nitro versus 2,177.0 ms and 13,000 CPU-ms
for Trino, or 1.038x wall / 0.879x CPU. The earlier isolated candidate result came from an invocation that omitted the
sweep setup's `ANALYZE`, produced a materially different plan, and is not valid board evidence.

The policy change was reverted. Preserve these logs as falsifying evidence; do not merge their partial results into
the current presentation board.
