# ADR-0011: Generate specializations from physical capabilities and layouts

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-26)
- **Spec:** §7, §8, §12, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md)
- **Historical evidence:** reconciliation entries for generated grouping, flat records, scalar targets, and provider updates

## Context

Handwritten paths for specific type/arity combinations improved individual cases but created a combinatorial set of
interfaces and operators. Fully generic per-position dispatch, on the other hand, left vector kind, function, hash, and
comparison decisions in hot loops.

## Decision

Resolve logical behavior through registries, resolve physical shape outside the row loop, and use the Java Classfile
API to generate tight kernels where specialization is justified. Generated shapes are derived from capability
interfaces, carriers, layouts, masks, and encodings—not SQL type names, function names, tables, or fixed arities.

## Consequences

- Arbitrary composite expressions and key arities can share one generation framework.
- Providers need small classloader-neutral targets and metadata rather than generated-source ownership.
- Generated code, cache lifetime, linkage, and fallback require conformance tests.
- Handwritten kernels remain valid provider implementations when evidence shows generation cannot match them.

## Alternatives considered

**One Java interface/class per carrier signature.** Direct and optimizable, but grows combinatorially.

**Method-handle invocation from instance fields inside loops.** General, but frequently prevents constant linkage and
inlining.

**One fully polymorphic loop.** Small code surface, but repeats dispatch at the hottest level.
