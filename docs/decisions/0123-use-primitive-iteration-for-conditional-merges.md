# ADR-0123: Reject the conditional-merge traversal revisions after SQL qualification

- **Status:** rejected
- **Date:** 2026-09-13
- **Spec:** §6, §8, §16
- **Related:** [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md), [ADR-0121](0121-discard-unrequested-mapped-scalar-errors.md)

## Evidence and alternatives

An allocation profile localized 18–19 GB of excess allocation after scratch removal to Integer boxing through
PrimitiveIterator.OfInt.next in PlanEvaluator.copyVector. Three repairs were evaluated: explicit nextInt in the
copy loops; direct dense/sparse copying; and that direct copy plus primitive traversal throughout PlanEvaluator
and FrameworkManagedScalarFunction.

All preserved focused semantics. The last passed 2,164 Nitro tests and 164 Cork tests; its local q243 and macro CPU
guards overlapped the literal-only controls. None qualified the remote ordered q242/q243 CPU comparison.
q243 medians were 368.862 s, 339.026 s and 368.043 s respectively, versus 303.766 s for the initial literal-only control
and 282.797 s for its repeat. The direct-copy local allocation increase also disappeared under instrumentation,
so its later profiles cannot be claimed to locate that unprofiled increase.

## Decision

Remove these traversal revisions from the introducing mutable commit together with the scratch-removal change
that exposed the issue. Retain sparse-to-full conditional-demand and sparse diagnostic-preservation tests as
coverage for the provider-literal integration. Do not retain a feature flag or selectable experimental loop.

The allocation target remains unchanged. Enhanced-for over Iterable<Integer> is still a boxing risk; this decision
does not recommend boxed iteration as an optimization. It rejects the tested implementations as production fixes
until the CPU mechanism is understood and a replacement qualifies in the affected SQL context.

## Reconsideration

Use exact results, encoded/masked demand, allocator ownership and warmed uninstrumented CPU/allocation evidence.
Distinguish a query alone from the same query after a different physical shape in the same JVM. CPU samples and
compiler evidence must reproduce the observed state; do not infer a particular JIT decision from a moved stack.

Evidence remains outside the worktree in the scalar-literal investigation, local ordered runs and remote paired
profiles. These artifacts are not accepted benchmark-board replacements.
