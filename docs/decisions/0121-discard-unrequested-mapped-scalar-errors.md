# ADR-0121: Reject unrequested-error scratch removal after SQL qualification

- **Status:** rejected
- **Date:** 2026-09-13
- **Spec:** §6, §8, §11
- **Preserves:** [ADR-0051](0051-contain-mapped-failures-during-values-evaluation.md)
- **Related:** [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md), [ADR-0120](0120-specialize-provider-owned-primitive-literals.md), [ADR-0123](0123-use-primitive-iteration-for-conditional-merges.md)

## Proposal and evidence

A VALUES-only generated scalar call allocates temporary error storage to classify and contain mapped failures.
The proposed handler classified and discarded the mapped diagnostic directly when ERRORS was not requested,
preserving mapper rejection, strict nulls, input errors and the exact target-only protected region.
A preallocated-output unit test proved the extra vector unnecessary for those semantics.

The first SQL revision increased allocation through boxed conditional-copy traversal. Subsequent primitive-loop
repairs removed or moved the allocation symptom but did not qualify CPU. The broadest repair passed 2,164 Nitro
tests and local macro guards, yet remote q243 consumed 368.043 core-seconds versus 282.797 for the repeated
literal-only control (about 30% more CPU), with similar allocated bytes. The earlier copy-only revision also lost.
Instrumented and uninstrumented local runs did not consistently reproduce the same allocation state.

## Decision

Reject this production revision and remove its implementation at the introducing mutable commit. ADR0051's
temporary-storage containment mechanism remains current. Keep the new mapper/null/error semantic coverage but
do not retain an assertion that VALUES-only execution allocates no scratch.

This does not establish that temporary error storage is fundamentally necessary or that primitive traversal is
undesirable. It establishes that these implementations did not meet the measured end-to-end CPU requirement.
Frozen binaries and profiles remain diagnostic evidence, not a dormant production path or opt-in switch.

## Reconsideration

A future revision must preserve all independent-stream and failure-mapper contracts and demonstrate CPU and
allocation results across warmed JVM forks and query ordering. A local allocator counter or a profiled run alone
is insufficient. The literal-binding-only CPU improvement is separately qualified under ADR0120.

Evidence: `2026-09-13-scalar-literal-binding-investigation.md`, the primitive-family remote comparison and retained
R1–R5 run manifests, timings and profiles.
