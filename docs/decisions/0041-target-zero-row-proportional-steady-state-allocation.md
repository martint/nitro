# ADR-0041: Target zero row-proportional allocation in steady-state streaming execution

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established before 2026-07 and sharpened 2026-08-20)
- **Spec:** §11, §18
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0019](0019-account-retained-state-to-the-host.md)
- **Historical evidence:** `design-principles.md`; `2026-08-20-steady-state-allocation-audit.md`

## Context

Java query engines can spend substantial CPU allocating and collecting per-page, per-position, and temporary buffers.
Nitro's allocator and explicit batch lifetimes make a stronger target possible for streaming queries, while grouping,
sort, variable-size output, and state growth still require essential allocation.

## Decision

After bounded warmup, streaming scan/filter/project/aggregate execution allocates no storage proportional to processed
row count. Vectors, masks, scratch, mappings, and reusable output come from allocator pools. Essential state growth and
final/variable-size output remain allowed and separately attributed.

## Consequences

- Allocation rate is a primary benchmark metric and a correctness-like regression signal.
- Every bare hot-path array/object allocation requires justification.
- Pools need bounded retention, exact ownership, and memory-pressure visibility.
- “Allocation-free” never permits hidden untracked buffers or excessive permanent capacity.

## Alternatives considered

**Rely on generational GC for short-lived objects.** Simpler code, with CPU and memory bandwidth proportional to rows.

**Preallocate worst-case storage permanently.** Avoids allocation and harms co-tenancy and memory limits.

**Measure only retained peak memory.** Misses large allocation/GC costs with a low ending footprint.
