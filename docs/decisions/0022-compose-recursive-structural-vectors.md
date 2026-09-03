# ADR-0022: Compose nested values as recursive vectors and materialize them in one pass

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-26–09-01)
- **Spec:** §5, §12, §14
- **Historical evidence:** reconciliation entries for nested leaf projection, native ARRAY/UNNEST, and one-pass structural materialization

## Context

Hard-coded nested decoders and per-value host Blocks do not scale across ARRAY, MAP, ROW, nesting depth, encodings, or
new leaf types. Repeatedly appending mixed dictionary/RLE/flat segments can also make recursive output quadratic.

## Decision

Represent ARRAY by offsets and child vectors, MAP by offsets plus key/value vectors, and ROW by aligned child fields.
Resolve leaf semantics through type bindings and compose mappings recursively. For retained mixed segments, normalize
their mappings, size every recursive level once, and populate each level in one pass.

## Consequences

- Nested processing remains vectorized and encoding-aware.
- Parent nulls, offsets, and child coordinate systems require explicit validation.
- Leaf projection can avoid decoding unused siblings or constructing a parent.
- Structural providers can add new logical types without adding SQL-type branches to decoder core.

## Alternatives considered

**One decoder per complete logical nested type.** Straightforward initially and combinatorial with nesting.

**Materialize host Blocks.** Reuses host semantics but imports Page/Block and per-value adaptation into Nitro.

**Append segments incrementally.** Simple API but repeatedly rebuilds recursive offsets/children and can become
quadratic.
