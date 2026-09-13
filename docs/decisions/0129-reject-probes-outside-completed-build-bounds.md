# ADR-0129: Reject probes outside completed build bounds

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §13, §16, §18
- **Related:** ADR-0036, rejected ADR-0112/0119

## Context

The single-long join already records exact minimum and maximum physical keys. Its optional bitmap membership
helper currently discards this proof whenever the domain is too wide for a bitmap. The wide-domain q243 diagnostic
then hashes probe keys outside the complete build bounds; 90% of its input is constructed that way. Profiles show
substantial grouped-table probing cost, but do not establish that replacing the tag kernel is beneficial. Earlier
tag-kernel replacements failed qualification.

## Decision and correlation

Keep complete-build endpoints independently of optional bitmap allocation. Reject keys outside them before hash
lookup; in-range keys still require bitmap membership or authoritative exact lookup. Compare endpoints directly,
without overflow-prone subtraction. Before build and after release the helper remains unconstrained. Preserve
prepared-view sharing, duplicate rows, null exclusion, masks, exact match order and every existing representation.

This conforms to the physical exact-key and ownership contracts. It is a local membership proof over a completed
build, not logical SQL ordering, a new source dynamic filter, query recognition, or a scheduler/planner change.
No enabling flag or allocation is added. Bitmap admission and exported dynamic-filter behavior remain unchanged.

## Qualification

Retain completed-build endpoint proofs independently of optional bitmaps. A qualified two-fork component matrix
with correctly scoped per-execution allocator lifetime shows wide-key zero-match time 30.009 to 8.940 ms and
10%-match time 44.334 to 26.529 ms; all-match 150.632 to 150.707 ms and dense-domain points are neutral.
Allocation per operation is effectively equal. Boundary/release/exact-match tests, full Nitro tests and the
combined 411-query remote exact/native gate pass. SQL recovery includes other independently recorded fixes and
between-fork variability; these component results are not a claim of a universal SQL speedup.

Test wide positive/negative bounds, full-width endpoints, interior false positives, no bitmap storage, release,
and exact bitmap behavior. Run joins and the full Nitro suite. Compare q242/q243 and neighboring joins against
frozen controls, including predominantly in-range probes where the two comparisons could add cost. Do not infer
end-to-end gains solely from the fraction of impossible keys or from hash-kernel profile weights.
