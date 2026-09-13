# ADR-0109: Encode constant initial average counts

- **Status:** accepted; remote performance qualification pending
- **Date:** 2026-09-13
- **Spec:** §11, §12.1, §18
- **Depends on:** [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

The average provider's initial intermediate constructs one sum and one count per selected input position.
The count is one for non-null inputs, yet the implementation materializes and writes an entire long vector.
Allocation profiling of ClickBench q31 identifies this provider among the largest row-proportional allocation
sites. The count provider already represents its constant initial state using the allocator's single-run RLE API.

## Decision

When the existing encoding-aware null capability proves all input values non-null, represent initial average
counts as a single run of one. Keep sums independently owned and computed with the existing logical conversion.
Nullable input keeps its current flat zero/one count representation. Compact and position-preserving outputs
retain their existing selected-position semantics; values outside the output mask remain unobservable.

This conforms to provider-owned function semantics and encoded intermediate consumption, and closes an allocation
gap. No aggregate-specific knowledge enters the evaluator or operators. No feature flag, borrowed-input lifetime,
planner, scheduler, or intermediate field-order change is introduced.

## Validation

Exercise empty, dense and sparse inputs; flat and encoded null evidence; nullable fallback; integral, REAL and
DOUBLE conversions; downstream intermediate consumption; and allocator closure. Compare warmed SQL CPU and
allocation against the exact parent, then check neighboring average workloads. Reject the candidate if the
extra encoding increases end-to-end cost enough to negate its allocation benefit.

The provider and encoding tests pass; 601 tests in the broader Nitro integration package pass outside four
pre-existing failing classes. The exact parent reproduces all 12 failures and 40 errors in those classes; this is
not a claim that the full integration package passes. The detailed note retains the parent/candidate test logs.

Allocation reductions repeat across isolated and neighboring workloads. CPU was initially mixed by runtime
context, so the candidate was not accepted from its first point result. The final local W30/M9 q31/q32 pair on
the same repaired key-layout engine gives q31 CPU 5.062 versus 5.039 seconds (overlapping ranges), allocation
14.095 versus 14.681 GB (separated ranges); q32 CPU 17.130 versus 17.960 seconds and allocation 48.636 versus
53.134 GB (both separated ranges). Latency and peak-reservation ranges overlap. Source positions and bytes match.
No remote or full-board improvement is claimed. The final paired report is
`average-count-bound-null-comparison-20260913.txt` under the evidence directory.
