# ADR-0115: Bind single-match hash dispatch per batch

- **Status:** rejected; production candidate removed
- **Date:** 2026-09-13
- **Spec:** §5, §13, §16, §18
- **Depends on:** [ADR-0114](0114-reduce-only-the-first-empty-join-tag.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

The first-empty SIMD substitution did not improve q15. A diagnostic compilation/inlining capture on accepted
code shows the q15 matching loop failing to inline `singleRef`: it was already compiled into a large method.
That method checks direct-build, direct-lookup and hash representations for each row. Probe representation is
stable throughout the batch, unlike insertion where a row can change the representation (ADR-0113).

## Evaluated proposal

After ordinary finalization and dense-sequence admission, select the hash-only single-match loop once per batch
when neither direct representation is active. Bind physical long and null accessors once, preserve logical
selections and null short-circuiting, and call the narrower exact hash lookup without repeated representation
checks. All admitted physical encodings use the existing accessor contract. Keep direct paths, hash function,
table layout/load factor, sparse membership and row references intact. Add no pool, threshold, switch, function
semantics, query recognition, planner or scheduler change.

This conforms to immutable probe-state and batch specialization contracts. It does not generalize the assumption
of representation stability to build insertion, which still observes transitions within a batch.

## Validation

Run the full correctness suite, then frozen parent/candidate/parent q15/q20 and q09/q21 measurements. Check whether
the narrowed lookup actually inlines, and reject a neutral or regressing candidate. Extend focused selected/null/
encoded probe coverage if accepted. Do not attribute this candidate to the already-running frozen remote board.

## Outcome

The candidate passes 2,160 tests and achieves the intended C2 inlining of the narrower lookup. Initial W30/M9
q15/q20 CPU medians are 217/213/215 ms and 116/115/116 ms in parent/candidate/parent order, with overlap.
A longer W100/M30 check removes the apparent q15 advantage: both the first parent and candidate measure
203 ms CPU with overlapping ranges; the repeat parent is 204 ms. q20 retains a small lower median at
112/110/112 ms, but its ranges also overlap and broader performance qualification was not established.
Improved inlining alone does not establish the targeted q15 improvement.

Restore the original implementation, retaining only this decision and external evidence. The longer warmup also
improves the accepted implementation, so compare Cork with the same longer warmup before assigning that change
to an engine regression. Do not alter or splice measurements into the currently frozen board protocol.
