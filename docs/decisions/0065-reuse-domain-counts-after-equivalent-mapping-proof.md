# ADR-0065: Reuse domain counts after an equivalent-mapping proof

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §5, §12.1
- **Depends on:** [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0064](0064-count-independent-encoded-domains.md)

## Context

Two independently owned dictionary vectors may contain the exact same row-to-domain ID sequence. Identity-based
alignment cannot recognize that fact. Counting selected IDs remains correct, but it performs dependent histogram
updates for every selected row even though the mask already carries the exact domain histogram.

Equal domain cardinality is insufficient: independently encoded values can assign different IDs or map rows in a
different order. Any broader reuse therefore needs an exact proof.

## Decision

A caller that benefits from existing domain counts may request an O(rows) equivalence proof between a mask's retained
dictionary mapping and an independently owned dictionary mapping. Exact equality of mapping length and every ID
permits reuse of the mask's domain frequencies and selected-domain bits. Storage identity and ownership remain
separate; the proof does not make either mapping transferable through the other.

The identity-aligned lookup remains available for callers that require O(1) admission. The equivalent-mapping lookup
is explicit because it exchanges one sequential comparison pass for avoiding more expensive selected-row work.

## Consequences

- Independent columns written with the same ID layout can share already-computed mask frequencies safely.
- The proof is linear in logical rows, but is cache-friendly and does not perform dependent histogram writes.
- Differently ordered or differently assigned mappings use ADR-0064's exact recount path.
- Mapping ownership and mask provenance are unchanged.

## Alternatives considered

**Treat matching cardinalities as equivalent.** Incorrect because IDs are local to each mapping.

**Canonicalize every source dictionary mapping.** Adds coordination, retention, and ownership costs to all reads even
when no consumer needs cross-column domain reuse.

**Always recount selected IDs.** Correct but measurably leaves CPU and allocation efficiency on the table for common
co-encoded columns.
