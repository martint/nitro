# ADR-0076: Bind reversible window aggregation state through providers

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §8, §12.1
- **Depends on:** [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)

## Context

Bounded aggregate windows currently reinitialize provider state and replay every frame. Adjacent ROWS, GROUPS, and
RANGE frames commonly overlap almost completely, so replay turns a fixed-width sliding frame into work proportional
to frame width for every output row. The engine cannot infer whether an aggregate has an exact inverse, how nulls
affect that inverse, or which auxiliary state is required.

## Decision

An aggregation provider may bind optional reversible single-position accumulators to physical input batches. Each
binding adds positions entering a monotonically advancing frame and removes positions leaving it while updating the
same provider state. A removed position must previously have been added to that state. Providers own exact state,
null, underflow, and inverse semantics.

The capability is optional. If it is absent or the frame moves backwards, window execution reinitializes provider
state, invalidates physical bindings, and replays the complete frame before publishing the result. The operator never
recognizes an aggregate function or logical type.

## Consequences

- Sliding count and other exactly reversible aggregates can update in work proportional to frame delta.
- Providers may add auxiliary allocator-owned state needed to preserve empty/null semantics.
- Non-reversible and numerically unsafe aggregates retain exact replay behavior.
- Physical bindings must be refreshed when retained window positions cross source batches.

## Alternatives considered

**Teach the window operator count and sum inverses.** Rejected because function and null semantics belong to providers.

**Require every aggregate to remove input.** Rejected because extrema, approximate state, and floating-point
aggregates may not have an exact or acceptable inverse.

**Precompute all window results in a function-specific host operator.** Rejected because it decomposes Nitro islands
and loses shared vector ownership.
