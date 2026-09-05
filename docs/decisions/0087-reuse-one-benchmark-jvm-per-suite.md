# ADR-0087: Reuse one benchmark JVM per suite

- **Status:** accepted
- **Date:** 2026-09-06
- **Spec:** §18
- **Amends:** [ADR-0028](0028-separate-routine-and-stress-benchmarks.md)
- **Evidence:** `2026-09-06-adaptive-duration-protocol-correction.md`; partial Cork W5/M1 sizing preflight

## Context

The remote campaign wrapper launched a fresh parent and forked benchmark JVM for every query even though the benchmark
harness natively accepts an ordered query list and executes it serially through one `DistributedQueryRunner`. In the
first 62 Engine Coverage points of the corrected W5/M1 sizing preflight, process and runner setup contributed 2,395 of
7,068 wall-seconds, or 38.6 seconds per query and 34% of elapsed time. The repeated setup dominated many short points
and made the complete inventory operationally impractical.

Fresh processes contained retained state and made each query an independent restart boundary, but they did not isolate
the host file cache and they hid cross-query lifecycle leaks that a long-lived engine must tolerate. Per-query adaptive
warmup already establishes query-specific steady state. Process teardown is therefore an unnecessarily expensive way
to obtain isolation.

## Decision

Routine and sizing campaigns use one benchmark JVM and one distributed query runner per suite and engine. Queries run
serially in a fixed manifest order with query concurrency one. Subject and control use the same order and suite
boundaries.

Warmup convergence, retained measurements, result validation, EXPLAIN evidence, capability evidence, and diagnostics
remain query-specific. After every completed query, the harness publishes a durable checkpoint containing that
query's timing and convergence rows. A failed suite invocation may resume in a new suite JVM at the first
uncheckpointed query; completed checkpoints are not remeasured.

Query close must release query reservations and execution-owned resources. Shared JIT state, immutable metadata,
bounded allocator high-water state, and the host file cache are part of the warmed suite condition. Campaign gates
detect cumulative retained-memory growth and state leakage rather than masking them with process exit. Focused
diagnostics may still select an explicitly labelled isolated-query JVM.

## Consequences

- Engine and JVM startup is amortized across a suite while every query retains independent samples and attribution.
- Historical fresh-JVM and suite-JVM measurements are different protocols and are not merged.
- Checkpoint publication and artifact materialization become part of campaign validity.
- Query-order effects must be fixed, recorded, and matched; order-rotation diagnostics may quantify them separately.
- A crash loses only the active query, although the resumed remainder necessarily uses a replacement suite JVM.

## Alternatives considered

**Keep one JVM per query.** Strong process isolation, but measured setup consumed one third of Engine Coverage sizing
time and obscured lifecycle leaks.

**Use one JVM for all four suites.** Avoids three more startups but couples distinct workload/catalog bootstraps and
provides little benefit compared with eliminating hundreds of per-query startups.

**Run queries concurrently.** Improves throughput while contaminating latency, CPU, cache, and memory evidence; query
concurrency remains one.
