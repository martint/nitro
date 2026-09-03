# ADR-0015: Keep aggregate state and intermediate semantics with function providers

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-26)
- **Spec:** §8, §12
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md)
- **Historical evidence:** reconciliation entries for explicit aggregation programs, provider-owned state, and portable boundaries

## Context

Early aggregation state vectors and accumulator classes in core encoded knowledge of SUM, AVG, extrema, distinct, and
particular intermediate layouts. That made operators responsible for functions and made host-defined aggregations
difficult to integrate without reimplementation.

## Decision

The registered aggregation provider owns state semantics, update/merge/result capabilities, intermediate format, null
behavior, ordering requirements, and serialization. Nitro supplies generic grouping, allocator-owned storage,
selection, generated update plumbing, and physical input views. Function-specific state classes live with their
implementation, not in core Nitro SPI.

## Consequences

- Trino and future providers can supply exact aggregate semantics.
- Portable boundary state and live allocator-owned state are explicitly different representations.
- Generated grouped updates invoke opaque provider state rather than a builtin state class.
- Some common physical state layouts require reusable provider-neutral storage capabilities.

## Alternatives considered

**Builtin aggregate state in core.** Efficient for a closed function set and incompatible with a dynamic registry.

**Serialize state after every batch.** Portable but allocation-heavy and hostile to incremental execution.

**Let each operator special-case known accumulators.** Recreates function knowledge in every driver.
