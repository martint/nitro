# ADR-0080: Compose provider primitive result ranges

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §9, §12.1, §17
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0057](0057-describe-aggregation-inputs-by-primitive-carrier.md), [ADR-0078](0078-run-reversible-window-ranges-in-provider-kernels.md), [ADR-0079](0079-compose-synchronous-operator-result-ranges.md)
- **Evidence:** Engine Coverage q241 one-count W5/M5 and window-stage attribution after ADR-0079

## Context

ADR-0079 removes synchronous transport wrappers but deliberately leaves provider-produced result vectors between a
forward window kernel and a downstream aggregation provider. q241 still spends material CPU allocating and filling
I64 result batches and traversing those values through the generic aggregate input protocol. Recognizing window count,
downstream sum, or downstream count in the engine would remove that work but violate registry ownership. Coupling the
providers by implementation class or method-handle identity would also fail across independent provider classloaders.

## Decision

Nitro adds an optional classloader-neutral primitive range contribution convention. A producer declares each output's
primitive carrier and exact null convention independently of function identity. A downstream global aggregation unit
declares the input channel and exact contribution shape it accepts, then binds a provider-owned consumer over its
opaque state only after the engine proves that every unit in the program is compatible.

The initial carriers are primitive `long` and cardinality. Long output distinguishes never-null from explicitly
nullable contributions. Cardinality is separate because aggregates that observe only row count must not force value
production or traversal. A kernel may deliver one output to several compatible consumers and may deliver several
outputs in one range. Negotiation occurs before the window loads or either provider advances semantic state.

Any mismatch, filter, DISTINCT stage, grouping, absent provider binding, or unsupported carrier declines the complete
primitive path and preserves ADR-0079 vector ranges or ordinary batch pull. Cancellation propagates from the consumer;
ordinary graph close remains responsible for retained provider state. The engine matches structural descriptors only
and never compares function identity, provider classes, or target handles.

## Consequences

- Compatible forward window and global aggregation providers avoid result vectors, masks, stream accessors, and the
  generic aggregation row traversal.
- Providers retain state, arithmetic, empty-frame, null, inverse, and final-result semantics.
- The SPI gains a small extensible physical ABI whose carrier set must grow deliberately as evidence supports it.
- Atomic fallback requires a side-effect-free declaration separate from the later state-bound consumer.
- Cardinality is range-level work, while primitive values remain one contribution per logical result position.

## Alternatives considered

**Recognize count and sum in the engine.** Rejected because function identity and SQL semantics belong to providers.

**Pass I64 arrays without vectors.** This removes wrappers but still materializes and traverses the complete result
range, preserving most of the attributed work.

**Reuse method-handle identity from grouped updates.** Rejected because independently loaded providers cannot share
implementation identity, and matching target identity would confuse physical compatibility with function identity.

**Fuse arbitrary provider state objects.** Rejected because it leaks provider classes across classloaders and makes
ownership, cancellation, and fallback impossible to validate structurally.
