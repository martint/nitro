# ADR-0095: Generate unordered repeated key layouts

- **Status:** accepted
- **Date:** 2026-09-08
- **Spec:** §5, §13, §16, §19
- **Amends:** [ADR-0093](0093-generate-recursive-product-key-layouts.md) and
  [ADR-0094](0094-generate-ordered-repeated-key-layouts.md)
- **Depends on:** [ADR-0090](0090-reject-row-wise-semantic-key-tables.md),
  [ADR-0092](0092-compose-persistent-key-layouts-in-one-generated-table.md),
  [ADR-0094](0094-generate-ordered-repeated-key-layouts.md)
- **Evidence:** Engine Coverage q69

## Context

ADR-0094 intentionally excludes maps from ordered repetition because physical entry order is not logical map
identity. After q68 gained ordered array coverage, q69 remains outside Nitro islands at its `MAP(BIGINT,VARCHAR)`
grouping and join.

The former recursive semantic kernel compared every left entry against right entries until it found a matching key.
That path is exact but invokes child kernels inside nested row loops and has quadratic worst-case equality work. Sorting
host objects or invoking provider comparisons would reintroduce allocation and dynamic dispatch. Hashing entries in an
order-independent way without canonical storage still leaves collision equality with the same search problem.

## Decision

A provider may prove that one logical key is an **unordered repeated multiset** of aligned child-output tuples. Entry
multiplicity is part of identity. Map providers use one key output and one value output; Nitro does not infer map
semantics from that arity or from `MapVector`.

Resolution uses the same recursive child algebra as ordered repetition: direct identities, canonical lanes, finite
products, and repeated children. Generated code encodes every entry tuple into exact provider-neutral physical bytes.
It sorts reusable primitive entry references by those bytes and writes the entries in canonical order to persistent
variable-width storage. Hashing combines the canonical entries, and collision equality compares the canonical bytes.
No host objects, per-entry virtual or method-handle dispatch, or logical-type branches occur in the hot path.

Scratch offsets and ordering indexes are allocator-owned and reused across rows and batches. Canonicalization must not
allocate one object or array per entry or per map. Unsupported children, inconsistent aligned outputs, oversized
single records, or unsupported wrappers fail before table mutation.

## Consequences

- Maps and other provider-defined unordered repeated identities can use grouping, DISTINCT, hash join, and membership
  without making physical entry order semantic.
- The same canonical-entry machinery supports any number and combination of recursively supported child outputs; it
  does not create a map table or key/value carrier matrix.
- Duplicate identical entries remain distinguishable by multiplicity even if a particular provider normally enforces
  unique keys.
- Sorting adds `O(entries log entries)` work on first insertion. Encoded-domain reuse remains important for repeated
  map domains, and performance evidence must compare the complete query rather than only canonicalization.

## Rejected alternatives

**Publish maps as ordered repetition.** Incorrect when equal maps use different physical entry orders.

**Use an order-independent hash and scan entries for equality.** Exact but quadratic in the collision path and
retains per-entry child dispatch.

**Sort provider objects or call provider comparison.** Adds allocation and requires ordering semantics that equality
identity does not otherwise need.

**Add a map-specific hash table.** Encodes one logical type and child arity in every persistent-key consumer.
