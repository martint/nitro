# ADR-0128: Clear selected errors in reused scalar outputs

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §6, §8, §11, §18
- **Related:** ADR-0041, rejected ADR-0121/0123/0126, ADR-0127

## Evidence

Allocation profiling of the binding-reuse candidate exposed boxed positions in scalar error propagation. Primitive
iteration alone failed local CPU/allocation qualification and is not retained as a conditional-copy or null-loop
optimization. That audit exposed two independent correctness defects: dense masks smaller than an oversized output
left selected stale errors untouched; propagating presence-only errors after marking the output all-false left its
cached summary incorrect. Both have failing regression tests on the prior implementation.

## Decision and correlation

Clear selected error presence and diagnostics through the vector's own mutation API. Use bulk clearing for a dense
prefix and exact primitive traversal for sparse selections. Do not equate the mask's logical extent with the pooled
vector's capacity. Preserve unselected diagnostics and allocate no diagnostic storage merely to clear bits.
Fresh scratch is already zeroed by the ErrorVector constructor or allocator reuse. Clear explicitly only when an
existing output was supplied, including when its contents were copied into a grown vector; do not zero fresh
scratch twice.

The scalar framework returns immediately after clearing when there are no upstream failures to propagate. Otherwise
it copies only selected failures, using summary-invalidating mutation for both diagnostic and presence-only errors.
Keep every existing mapped-error buffer, failure convention, null rule and allocator ownership contract. No function
identity, cache, physical-admission heuristic or enabling flag enters this correction.

This conforms to ADR-0041 and corrects violations of the selected-row/error contracts. It does not revive rejected
scratch removal (ADR-0121/0123). The initial traversal-only trial is recorded in the dated investigation, not retained
as a second production path. Performance qualification is independent of the demonstrated correctness defects.

## Qualification

Retain the two red-proven correctness repairs. Dense-prefix stale diagnostics and presence-only summary
invalidation fail on the prior implementation and pass after the correction. Full Nitro and focused host tests,
q242/q243 exact/native diagnostics, macro guards and the combined 411-query remote functional gate pass.
Large between-fork CPU/allocation variation prevents attributing the later recovery stack's total benefit to this
change. The rejected scratch-removal and traversal-only variants are not revived by accepting the correctness fix.

Cover dense prefixes in oversized vectors, sparse and empty selections, summary invalidation, presence-only errors,
preserved inactive diagnostics, stale selected-error clearing, upstream/target failures and successive demands.
Run the complete Nitro suite and host error/fusion tests. Qualify uninstrumented q242/q243 and macro guards with
frozen controls. Other boxed traversal sites remain audit targets; globally allocation-free execution is not claimed.
