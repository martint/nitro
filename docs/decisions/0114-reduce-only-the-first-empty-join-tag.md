# ADR-0114: Reduce only the first empty join tag

- **Status:** rejected; production candidate removed
- **Date:** 2026-09-13
- **Spec:** §13, §16, §18
- **Depends on:** [ADR-0112](0112-probe-grouped-join-tags-with-packed-word-masks.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

Fresh q15 local profiling reproduces a separated CPU regression against Cork and identifies join lookup mask
extraction among the driver costs. ADR-0112 already rejected a packed-word replacement as neutral. That decision
stands: the sampled leaf alone does not justify replacing the control representation.

## Evaluated proposal

Keep the existing sixteen-byte Vector API group, exact tag candidates, slot order, hash function, load factor,
allocation and admission. Where the empty-slot path needs only the first true lane, ask the vector mask for that
lane rather than materializing every bit and then finding the first one. The all-false sentinel continues probing
the next group. This conforms to the existing exact table contract and changes no policy or logical semantics.

## Validation

Run collision, absent/present, wrapped-group, growth and pooled-reuse tests and the full suite. Measure warmed
q15/q20 and neighboring q09/q21 parent/candidate/parent runs with frozen classes. Accept only with evidence of
benefit and no established breadth regression; otherwise restore the existing kernel and retain the conclusion.
Do not change the running remote recovery runtime.

## Outcome

The candidate passes the full 2,160-test suite, but warmed parent/candidate/parent SQL shows no target benefit.
q15 CPU medians are 213/215/216 ms and q20 116/116/116 ms, with overlapping ranges. The first neighboring
q21 comparison regresses from 6.441 to 6.553 core-seconds with separated ranges, but the repeat parent is
6.595 core-seconds with overlap, so this is not a consistently established regression. q09 is neutral at
5.722/5.709/5.708 core-seconds. The complete repeat reports are retained with the investigation evidence.

Restore the existing mask conversion. No switch or dormant alternate kernel remains. The original table's
collision, wrapped-group, absent-key, growth and pooled-reuse tests remain the correctness guards. A more specific
Vector API reduction is not necessarily a faster compiled loop; investigate complete batch lookup and inlining
before further isolated substitutions.
