# ADR-0086: Prove raw key identity through type providers

- **Status:** accepted
- **Date:** 2026-09-05
- **Spec:** §8, §11, §12
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md), [ADR-0036](0036-share-physical-key-contracts-across-consumers.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)
- **Evidence:** TPC-DS q67 regression analysis

## Context

A logical type binding can publish exact vector hash, identity, and comparison operations for consumers that need
them. Grouping interpreted their presence as evidence that the generic physical key kernel was inapplicable. That is
not necessarily true: for many provider types, raw equality of every admitted physical value is already the exact
logical key identity, while the richer operations exist for a different consumer or calling convention.

The false exclusion routed ordinary integral, character, and binary grouping keys through row-wise semantic method
handles. TPC-DS q67 amplified the cost across grouping sets and millions of partial groups. Its structural decimal
result path also rebuilt immutable stream maps and accessors for every copied or compared output position, violating
the steady-state allocation contract independently of key-table selection.

## Decision

Add an explicit provider-owned capability proving that raw equality over every admitted non-null physical vector is
the logical key identity. Generic key consumers may prefer their physical kernel when the proof is present, even if
the provider also publishes richer semantic operations. They must use those semantic operations when the proof is
absent. The engine never infers the proof from a carrier class, vector class, or logical type identity.

Keep structural output reuse and single-value provider operations allocation-free. Reuse immutable stream tuples when
their backing vectors are unchanged, validate their fixed streams without map construction, and do not instantiate a
bound vector accessor inside a provider entry point invoked once per comparison.

## Consequences

- Providers can publish multiple exact capabilities without a richer one disabling a cheaper, stronger proof.
- Normalized, canonicalized, unordered, or otherwise non-raw key semantics remain on exact semantic kernels.
- The same proof is reusable by grouping, joins, and distinct execution.
- Hot structural result copying and comparison no longer allocate metadata proportional to output positions.
- Capability tests must cover both admitted and deliberately rejected logical types.

## Alternatives considered

**Infer raw identity from carrier or vector classes.** Rejected because one physical carrier can represent logical
types with different identity semantics.

**Omit richer semantic operations for raw-compatible types.** Rejected because ordering, functions, and other
consumers still need them.

**Special-case the q67 key types or decimal aggregate.** Rejected because query, type, and function recognition do
not belong in engine admission.
