# ADR-0044: Limit host changes to execution components Nitro replaces and their adapters

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; reaffirmed 2026-08-27)
- **Spec:** §1, §2, §10, §15
- **Depends on:** [ADR-0004](0004-keep-cooperative-pull-scheduler-neutral.md), [ADR-0024](0024-confine-page-block-adaptation-to-boundaries.md)
- **Historical evidence:** reconciliation entry “preserve scheduler compatibility while completing pull islands”

## Context

Performance integration can be made easier by changing host scheduling, planning, connector semantics, or unrelated
operators. Those changes confound attribution, increase product risk, and turn Nitro adoption into a broad engine fork.
An earlier thread-per-driver experiment crossed this boundary and was explicitly restored.

## Decision

Cork/Trino changes are confined to planning and adapting Nitro islands and the core execution components those islands
replace. Existing host scheduling, resource management, connector semantics, and unrelated operators remain intact
unless a separate explicit decision expands scope.

## Consequences

- Benchmarks isolate Nitro execution rather than unrelated host modifications.
- Incremental rollout and upstream review have a smaller blast radius.
- Some ideal future integrations require visible compatibility adapters for now.
- A broader host change needs its own rationale and cannot arrive disguised as Nitro plumbing.

## Alternatives considered

**Co-design the host around Nitro immediately.** May reach a cleaner endpoint faster and creates an inseparable,
high-risk rewrite.

**Treat any benchmark improvement as in scope.** Optimizes the result rather than the execution architecture under
evaluation.

**Avoid all host changes.** Impossible: island planning, registries, memory accounting, I/O, and boundaries require
explicit adapters.
