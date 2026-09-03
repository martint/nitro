# ADR-0016: Lower function-local ordering to a physical input contract

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-09-01)
- **Spec:** §8, §12
- **Depends on:** [ADR-0015](0015-keep-aggregate-state-with-function-providers.md)
- **Historical evidence:** reconciliation entry “function-local ordering lowers to a physical input contract”

## Context

Ordered aggregates and window functions need rows in a defined order, but recognizing `array_agg` or another function
inside an operator violates registry ownership. Keeping sort logic entirely inside every function duplicates the
engine's physical sorting machinery.

## Decision

The host planner lowers a resolved invocation's ordering requirement into an engine-neutral physical ordering
contract. Nitro creates the ordered selected view, then invokes the ordinary provider program. Functions with the same
ordering may share the view; incompatible orderings remain separate requirements.

## Consequences

- Ordering semantics remain provider/planner-owned while sorting machinery remains reusable.
- Input retention and sort memory are charged with the aggregate/window operation.
- Admission is conservative when several functions request incompatible orderings.
- The operator never branches on function identity.

## Alternatives considered

**Recognize ordered aggregate names in the operator.** Direct but function-specific.

**Require every provider to implement sorting.** Preserves ownership but duplicates complex engine machinery.

**Ignore ordering for partial stages.** Incorrect unless the provider explicitly proves an order-insensitive merge.
