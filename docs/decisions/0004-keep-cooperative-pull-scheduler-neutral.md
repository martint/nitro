# ADR-0004: Keep cooperative pull independent of the host scheduler

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective)
- **Spec:** §10
- **Depends on:** [ADR-0003](0003-compose-indivisible-pull-islands.md)
- **Historical evidence:** `2026-08-17-batch-pull-execution-and-spilling.md`; reconciliation entries for 2026-08-27–28

## Context

Batch pull naturally keeps an operator graph on one call stack. A thread-per-driver scheduler could park that stack,
but the current integration uses the existing time-sharing executor, which may need work to unwind and resume. Earlier
experiments risked making virtual threads and host-executor replacement part of Nitro's contract.

## Decision

Nitro receives an execution-control dependency and checkpoints only at bounded, restart-safe points. Operator state is
committed before a checkpoint and resumes after unwind without duplicate or lost work. The current integration retains
the host time-sharing scheduler and platform threads. A future stack-preserving scheduler is another embedding of the
same contract, not a prerequisite.

## Consequences

- Nitro pull operators work with the current production scheduler.
- State machines distinguish progress, temporary depletion, and end-of-input.
- Generated kernels need bounded chunk checkpoints.
- The host scheduler can evolve independently without another Nitro operator API.

## Alternatives considered

**Require thread-per-driver virtual threads.** This couples adoption to broad scheduler replacement outside current
scope.

**Expose “made progress but no output” throughout the API.** This leaks one scheduler protocol into every operator.

**Keep a push graph for time sharing.** This creates two execution models and prevents a native pull graph.
