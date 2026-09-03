# ADR-0036: Share physical key and table contracts across grouping, joins, and distinct

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-25 and extended through 2026-08)
- **Spec:** §12, §13, §16
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md)
- **Historical evidence:** design-principles entries for reusable key-table contracts and generated probes shared with distinct

## Context

Grouping, hash join, and distinct repeatedly need the same physical operations: resolve accessors, hash, write a stable
key, compare exactly, probe slots, and materialize output. Separate type/arity-specific implementations diverged and
forced each consumer to rediscover the same representation.

## Decision

Define provider-neutral physical key layouts and batch/table contracts reusable by grouping, joins, and distinct.
Generate hashing, probing, writing, and equality together from the resolved lane layout. Each consumer retains its own
semantics and lifecycle; the shared table never recognizes a query, function, or logical SQL type.

## Consequences

- A new physical lane or arbitrary-arity layout benefits multiple operators.
- Exact stored-key comparison remains authoritative after hash/identity shortcuts.
- Consumer-specific output, multiplicity, null, and match semantics remain outside the shared table.
- Changes require cross-consumer correctness and performance coverage.

## Alternatives considered

**Separate optimized tables per operator.** Easier local tuning, with duplicate code and inconsistent behavior.

**One semantic hash abstraction for everything.** Over-generalizes consumer behavior and risks virtual dispatch in hot
loops.

**Keep fixed pair/triple classes.** Fast for selected shapes and not sustainable for arbitrary arity.
