# ADR-0030: Design spilling behind engine-level memory arbitration

- **Status:** proposed
- **Date:** 2026-09-03 (retrospective proposal from 2026-08-17)
- **Spec:** §19
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0004](0004-keep-cooperative-pull-scheduler-neutral.md)
- **Historical evidence:** `2026-08-17-batch-pull-execution-and-spilling.md`

## Context

Having each operator decide independently when to spill cannot account for query priority, global memory pressure, the
relative reclaim cost of operators, or deadlock. Reusing host Page/Block spill would also force Nitro state through a
representation it does not use internally.

## Decision

Proposed: a host-neutral engine arbiter chooses the query/operator victim from hierarchical memory accounting and
priority. Operators expose reclaimer capabilities describing reclaimable bytes, cost, progress, and safe points, then
choose which internal partitions, runs, or buffers to externalize. Spill formats remain vector/algorithm-specific and
I/O is asynchronous through injected services.

## Consequences

- Policy remains centralized while algorithm knowledge remains in operators.
- Aggregation, join, sort, window, TopN, and exchange need distinct external algorithms.
- Reclaim must cooperate with scheduling, cancellation, and allocation deadlock prevention.
- This is not implemented and does not authorize dependence on Trino spill machinery.

## Alternatives considered

**Each operator spills at its own threshold.** Locally simple and globally unable to select the best victim.

**The engine serializes arbitrary operator state.** Centralized but cannot choose algorithmically meaningful partitions
or resumable formats.

**Reuse host Page/Block spill.** Available, but imports host representation and its practical limitations into Nitro.
