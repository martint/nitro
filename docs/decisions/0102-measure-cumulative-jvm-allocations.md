# ADR-0102: Measure cumulative JVM allocations across thread lifetimes

- **Status:** accepted
- **Date:** 2026-09-12
- **Spec:** §18
- **Related:** ADR-0087 (suite JVM reuse), ADR-0088 (serial query bundles)

## Context

The benchmark harness estimated total allocation by summing lifetime allocation counters for currently live threads
and subtracting two such sums. A thread exiting between snapshots removes its entire lifetime allocation from the
second sum. This can undercount the current query, subtract allocation from earlier queries, or produce a negative
delta. A reproducer allocates 8 MiB in a thread, joins it, and observes only 66,128 bytes with the old method.

This is an implementation defect against the existing total-allocated-bytes contract, not a change in which engine
workload is being measured. The query CPU, latency, and peak-reservation metrics come from query statistics and do not
use this allocation counter.

## Decision

Use the JVM's cumulative total-thread-allocated-byte counter, including terminated threads, across the same serial
query execution interval. Enable allocation accounting before measurement. Missing cumulative accounting or a
decreasing total invalidates the measurement instead of producing a zero or negative allocation result.

The JDK 26 harness uses `ThreadMXBean.getTotalThreadAllocatedBytes()` and labels metadata with
`jvm-total-thread-allocated-bytes-v1`. A thread-termination regression test protects the lifetime contract. Allocation
continues to cover heap allocation throughout the benchmark JVM, including coordinator, workers, and client result
materialization within the execution interval. It is not operator-only allocation and does not measure native or
device allocations. Metadata is emitted even when profiling is disabled.

## Consequences

- Historical live-thread-sum allocation comparisons are not accepted as total-allocation evidence. They cannot be
  repaired from the existing CSVs because terminated-thread totals were not retained.
- Remeasure both sides with the corrected counter before publishing allocation comparisons. This does not invalidate
  independently recorded CPU or latency evidence or require pretending the underlying engine binaries changed.
- Reuse of a JVM across queries requires cumulative lifetime-safe counters, not snapshots of currently present owners.
- JFR or allocation sampling remains useful for attribution, but sampled events do not replace total byte counters.

## Alternatives considered

**Keep summing live threads and clamp negative deltas.** Rejected: many undercounts remain positive.

**Track every thread's last observed value in the harness.** Rejected: allocation after the last sample and before
termination is still lost. The JVM already owns the required cumulative accounting.

**Treat low samples as statistical outliers.** Rejected: filtering the symptom cannot recover missing allocations or
prove unbiased engine comparisons.
