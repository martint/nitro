# ADR-0019: Account retained Nitro state to host memory reservations

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-30)
- **Spec:** §11, §15
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md)
- **Historical evidence:** reconciliation entries for host-visible retained execution state and allocator-context diagnostics

## Context

Allocator counters made Nitro memory visible to benchmarks but did not constrain it through the host resource system.
Grouping tables, join builds, sort/window buffers, pools, and aggregate state could therefore consume memory the query
manager did not reserve.

## Decision

Every live or retained Nitro allocation is charged through an allocator context connected to the host memory
reservation. Shared storage has one physical charge and attributable logical consumers. Reuse pools remain charged
while retaining bytes and release reservations when storage is discarded.

## Consequences

- Query limits and co-tenancy policies see Nitro memory.
- Allocation, live bytes, retained pool bytes, and peak bytes remain distinct diagnostics.
- Sharing requires careful non-duplicative accounting.
- Host callbacks stay outside allocation hot loops through bounded/context-level updates where possible.

## Alternatives considered

**Report metrics without reservations.** Observable but not enforceable under contention.

**Charge only JVM heap estimates at query end.** Too late for control and unable to assign shared state.

**Disable pooling to simplify accounting.** Removes useful steady-state reuse rather than representing it correctly.
