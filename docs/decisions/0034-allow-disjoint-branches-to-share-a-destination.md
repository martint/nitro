# ADR-0034: Allow conditional branches to populate one destination only through explicit ownership

- **Status:** proposed
- **Date:** 2026-09-03 (retrospective proposal from 2026-08-19)
- **Spec:** §6, §8, §11
- **Depends on:** [ADR-0014](0014-stream-function-results-into-owned-vectors.md)
- **Historical evidence:** `2026-08-19-in-place-conditional-branch-output.md`

## Context

Flat conditional fallback can evaluate each branch into a temporary vector and then copy disjoint positions into a
merge result. Both branches could instead write the same output if they preserve positions outside their masks, but
memoization, aliases, nested outputs, null/error streams, and provider refusal make implicit reuse unsafe.

## Decision

Proposed: the evaluator may offer one exclusively owned destination to disjoint branch calls. A provider can accept or
reject it per stream. Rejected streams use exact masked copy fallback. Encoded-domain merge remains preferred, and no
memoized or independently observable result aliases the shared destination.

## Consequences

- Eligible flat branches avoid temporary vectors and selected copies.
- VALUES, NULLS, and ERRORS require independent destination/reuse accounting.
- Finish, abort, and fallback need one unambiguous release authority.
- The proposal needs an Engine Coverage family before admission.

## Alternatives considered

**Always materialize then merge.** Current exact behavior with extra allocation and copying.

**Always pass the final vector as reusable output.** Fast and unsafe for aliases, memoized results, and rejecting
providers.

**Adopt the first branch result as destination.** Works only when newly owned, writable, full-length, and unobserved.
