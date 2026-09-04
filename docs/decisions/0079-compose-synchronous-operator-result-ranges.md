# ADR-0079: Compose synchronous operator result ranges without batch transport

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §9, §11, §12.1, §17
- **Depends on:** [ADR-0003](0003-compose-indivisible-pull-islands.md), [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md), [ADR-0077](0077-materialize-selected-window-results-by-forward-batch-range.md), [ADR-0078](0078-run-reversible-window-ranges-in-provider-kernels.md)
- **Evidence:** Engine Coverage q241 W5/M5 direct-range comparison

## Context

Provider-owned range kernels and bounded window results leave a `Batch`, `Output`, dense `Mask`, and generic borrow
path between a forward-only window result and its downstream global aggregation. The two operators already execute in
one pull graph, so those objects express no scheduling, selection, or ownership transition for this shape. Recognizing
`count` or `sum` in the engine would remove more work but transfer function and null semantics out of registries.

## Decision

Nitro defines an optional synchronous dense range source/sink capability between operators. Negotiation is immutable
and occurs before source advancement. A successful source call drains all remaining ranges; each range exposes
zero-based borrowed stream lanes only until the sink call returns. The producer releases its range on success,
cancellation, or failure, while graph close remains authoritative for retained state.

`WindowOperator` admits the capability only when every public output is a selected forward-range function result.
Pass-through projections may remap channels without materialization. Global aggregation admits only schema-compatible
programs whose units need neither filters nor DISTINCT selection. All other shapes retain ordinary batch pull without
partial advancement. Providers continue to own window result production and aggregate interpretation; the ABI carries
physical streams and row count, not function identity.

## Consequences

- Eligible window-to-global-aggregation graphs avoid `Batch`, `Output`, and `Mask` construction and borrowing per
  range while preserving independently registered providers.
- Result vectors remain provider-owned range storage. Removing those vectors requires a separate carrier-level
  provider-to-provider convention with evidence broad enough to justify the larger SPI.
- Computed projections, filtering, grouping, DISTINCT, legacy window functions, and incompatible programs fall back
  before either side mutates state.
- A sink failure can leave provider state advanced, as ordinary batch consumption can; closing the graph releases all
  retained state and the current generated range.

## Alternatives considered

**Recognize `count(*) + sum(window count)` in the engine.** Rejected because query and function identity cannot own a
physical execution path.

**Add a primitive long callback directly between the two providers.** Deferred because it couples result-carrier and
downstream update conventions across independently loaded providers. The measured batch-wrapper removal is modest;
the broader ABI needs separate cross-function evidence.

**Fuse arbitrary adjacent operators.** Rejected as an unnecessarily broad contract. Selection, row identity,
restartability, and ownership differ by operator; the dense synchronous range is the smallest exact capability.
