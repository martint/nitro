# ADR-0124: Preserve provider-proven bound-call failure conventions

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §8, §16, §18
- **Related:** [ADR-0120](0120-specialize-provider-owned-primitive-literals.md), [ADR-0051](0051-contain-mapped-failures-during-values-evaluation.md)

## Context

The integral function provider already proves that a constant nonzero remainder divisor cannot cause a row failure,
and that division by a constant other than zero or minus one cannot fail. Its native batch loop consumes that proof.
After literal binding, the generated adapter retains only the general host function's fallible declaration. It may
therefore execute independent error evaluation and allocate error-containment storage that this exact call does not
need. This is a calling-contract gap, not permission for the evaluator to interpret arithmetic semantics.

## Decision and correlation

Allow the selected provider to declare a more precise failure convention for an exact constant-bound call. Share
the existing integral proof between its batch implementation and call binding. The generic host adapter accepts
that declaration without inspecting function names, operations, types, or literal values for semantic meaning.
Keep the same target, null convention, determinism, and constant/runtime argument ordering. The resolved identity
retains the bound values and distinguishes the failure convention so incompatible contracts cannot share a binding.

Only the explicit integral provider supplies this proof. Ordinary calls and unproved literal calls retain their
current failure convention. A nonzero literal on the left does not prove a variable divisor safe. No observations
of benchmark values, query names, or vector physical widths establish infallibility. Provider declarations apply
to every admissible non-null input of the bound logical signature. This extends the existing provider-owned model
without changing failure containment for calls that may fail or adding a feature switch.

## Qualification

Retain the provider-owned proof contract in the qualified recovery stack. Integral boundary/null/error tests and
exact scalar comparisons pass, followed by the combined 2,178-test Nitro suite, 45 focused host tests and 411-query
remote exact/native functional gate. This does not remove containment from fallible calls or imply that failure
metadata alone explains later SQL gains. Partial-application projection preservation and the subsequent pooling
repairs remain separately identified decisions; no feature switch or function interpretation enters the evaluator.

Test all integral logical types, both argument positions, zero and negative divisors, minimum-value overflow,
nulls, existing input diagnostics, masks and encodings. Compare the exact host scalar target's results. Preserve
all prior scalar/registry/decimal integration tests and full Nitro tests. Measure warmed uninstrumented CPU and
allocation against the literal-only implementation, plus macro neighbors. The frozen four-suite board remains
unchanged while the local candidate is evaluated. Remove the candidate if qualification fails.

## Alternatives

**Keep the general fallible declaration.** Correct and conservative, but drops an existing provider proof and may
repeat work. It remains the correct convention where no exact provider proof is available.

**Infer arithmetic safety in the evaluator.** Rejected: it imports function semantics into generic execution.

**Remove all unrequested error scratch.** Not this proposal; the broader revisions were rejected in ADR-0121/0123.
Those containment paths remain unchanged wherever a call can fail.
