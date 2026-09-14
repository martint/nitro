# ADR-0152: Bound partition remapping by admitted reuse

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §11, §15, §18
- **Related:** ADR-0108, ADR-0150; rejected ADR-0149

## Context and correlation

Partition dictionary remapping currently reserves space for as many entries as there are output positions, capped
by the copy limit, then rejects the result if its reuse is too low. A distinct count above the already configured
reuse limit can never recover as the remaining positions are visited: the total selected count is fixed.

This fills the existing principle that remapping storage is bounded by the admitted destination domain. It changes
neither the immutable policy nor its accepted representations, output ownership, routing, or buffering contract.

## Proposal

Bound the temporary distinct-entry capacity by the smaller of the copy limit and floor(output count / minimum reuse).
Return the existing flat fallback immediately when that bound is zero. Both lookup implementations already stop at
their distinct-entry capacity. Keep the final policy check and all selected-position copying semantics unchanged.

This is not cumulative dictionary admission. Do not expand its scope into pooling escaping IDs or changing host
exchange buffering. Those have different lifetimes and require separate evidence.

## Qualification

Test identical output values and representation admission at exact and non-divisible reuse limits, zero copy limit,
very large minimum reuse, sparse selections, and both array/hash lookup paths. A scratch-state test must prove that
rejected remapping stops at the bound while the fallback still copies every selected value.

Measure complete partition copying with admitted and rejected domains, then warmed SQL against the accepted parent.
Require full correctness and no material guard regression before accepting. Retain rejected evidence and remove the
candidate if it does not qualify; no opt-in switch is introduced.

## Outcome

Accept the bounded speculative work. Full tests pass: 2,194, zero failures/errors, 394 skipped. Both lookup forms
retain exact output and admission at the tested boundaries. Two-fork component measurements reduce rejected-domain
time by about 18–29% and reduce allocation across all eight points. The longer q31 parent/candidate/parent guard
places the candidate between its controls, with overlapping ranges; no q31 SQL speedup or regression is established.

The paired runtime including the separately qualified pool-reuse change passes all 409 native-execution/result
checks on September 14. Each change has independent component and SQL brackets; the combined functional gate does
not attribute performance to either change. The accepted measured board predates both and is not relabeled.

Evidence: `2026-09-14-partition-remapping-bound-qualification.md`, its retained component/SQL results, and
`results/partition-pools-functional-20260914` under the Nitro notes directory.
