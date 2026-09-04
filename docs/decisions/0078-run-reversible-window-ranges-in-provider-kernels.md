# ADR-0078: Run reversible window ranges in provider kernels

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §8, §12.1, §17
- **Depends on:** [ADR-0013](0013-use-layered-function-calling-conventions.md), [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0076](0076-bind-reversible-window-aggregation-state.md), [ADR-0077](0077-materialize-selected-window-results-by-forward-batch-range.md)
- **Evidence:** Engine Coverage q241 attribution after reversible state and bounded result materialization

## Context

Reversible single-position updates and bounded output planes remove frame replay and partition-sized results, but q241
still enters generic aggregation adaptation, result-copy, and allocator admission once per output row. Multiple window
functions over the same positional frame also resolve identical bounds independently. The engine cannot combine these
operations without learning provider state, null, inverse, and result semantics.

## Decision

An aggregation provider may bind an optional classloader-neutral reversible window range kernel. Nitro binds one
forward contiguous output range through a physical frame cursor, prepares the complete destination before entering the
provider loop, and invokes the kernel once. A bound cursor can drive the provider directly or expose an exact compact
affine ROWS descriptor containing only partition size and constant preceding/following distances. The descriptor has no
logical type or function identity. The provider owns state reset, add/remove, null, inverse, empty-frame, and result
semantics and reports completed positions, additions, removals, and results through returned work counters.

Window frames may bind a batch cursor. Functions whose frames expose equal traversal identities share one cursor or
descriptor for an output batch. A cursor may expose affine ROWS bounds only when both offsets are proven single-run,
non-null, error-free, and non-negative constants; it retains exact per-row bounds for dynamic or invalid offset shapes
and across incompatible retained pages. Missing kernels retain the established single-position reversible or replay
paths.

## Consequences

- Selected capable aggregates avoid per-row aggregation dispatch, result copying, and allocator admission.
- Common affine ROWS frames avoid both frame arrays and generic bound resolution inside provider loops.
- Frame traversal is bound once for sibling functions with exactly the same physical frame contract.
- Providers must keep kernel state consistent across output batches and reset it at every partition boundary.
- Diagnostics make kernel admission and exact delta work observable without function-specific engine logic.
- The SPI grows by optional interfaces; existing providers remain source- and behavior-compatible.

## Alternatives considered

**Generate a count-specific engine loop.** Rejected because it moves function, null, and result semantics into Nitro.

**Fuse every function sharing a frame into one provider object.** Rejected because independently registered providers
need not share a classloader, state representation, or lifecycle.

**Retain per-row result callbacks after sharing bounds.** Rejected because q241 attribution identifies that calling and
allocation surface, not only frame resolution, as the remaining repeated work.
