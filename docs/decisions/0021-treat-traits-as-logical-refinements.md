# ADR-0021: Treat physical traits as refinements of registered logical types

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-27)
- **Spec:** §5, §14
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md)
- **Historical evidence:** reconciliation entries “REAL remains a Trino semantic binding” and “physical BYTE_ARRAY does not imply text”

## Context

Physical carriers alone cannot distinguish BIGINT from temporal/decimal values or VARCHAR from VARBINARY. At the same
time, runtime facts such as ASCII-only or valid UTF-8 can enable cheaper kernels for an already-known logical type.

## Decision

Bind logical type first through the registry. Attach producer-proven traits that refine facts about values of that
logical type. A trait never assigns logical meaning to a vector and is invalidated when a transformation no longer
preserves its proof.

## Consequences

- The same physical storage safely supports many logical types.
- ASCII, UTF-8, range, cardinality, ordering, and null-shape optimizations remain available.
- Sources and functions must state which traits they establish or preserve.
- Type semantics never depend on the current vector implementation.

## Alternatives considered

**Tag physical vectors with logical type names.** Couples storage to a type system and duplicates registry identity.

**Infer text from binary storage.** Confuses VARCHAR/CHAR/VARBINARY and can corrupt comparison or decoding semantics.

**Expose no traits.** Correct but discards facts already proven by readers or producers.
