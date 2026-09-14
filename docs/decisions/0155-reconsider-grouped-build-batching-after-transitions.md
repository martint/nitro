# ADR-0155: Reconsider grouped build batching after transitions

- **Status:** rejected; production candidate removed
- **Date:** 2026-09-14
- **Spec:** §11, §13, §16, §18
- **Related:** rejected ADR-0145; ADR-0018, ADR-0036, ADR-0105, ADR-0139

## New evidence and correlation

ADR-0145's entry-only admission missed grouped insertion reached after a sequence/direct fallback within the same
bulk call. A new measured-phase counter capture observes q13's six bulk calls all entering sequence mode but then
performing three million grouped lookups across two executions. q21 has no bulk calls entering ready grouped mode,
yet performs about 5.85 million grouped lookups. Dynamic-filter key collection is inactive in these runs; disabling
it or forcing a different table layout would not address the observed gap.

This revisits the rejected experiment with new admission evidence rather than changing its historical outcome.
It fills the existing bounded re-adaptation contract. Hashing, exact equality, table layout, growth timing, duplicate
FIFO order, masks/nulls, row references, filtering, ownership, and host scheduling remain unchanged.

## Proposal

For an already constructed grouped layout, reconsider insertion batching at bounded chunk boundaries while a build
can transition from direct/sequence state or grow to the admitted capacity. Once ready, stage independent initial
lookups in allocator-owned scratch and validate every hint against the current table and earlier insertions.
Growth, release, and reallocation invalidate hints by storage generation. Release scratch before publishing prepared
probe views. Immutable construction policy selects the chunk size and capacity floor; no enabling flag is added.

Keep the original generic addRow body unchanged. Do not move its shared bookkeeping into helpers merely to reuse it
from the hinted loop: that changed compilation shape in non-admitted workloads under ADR-0145. Non-grouped layouts
retain their original loops; transitional grouped layouts get a separate bounded loop.

## Required qualification

Test same-batch transitions, growth, stale hints, duplicate collisions and FIFO order, full-width keys, selected/null
inputs, both build reference mappings, scratch accounting/release, and prepared-build lifetime. Use behavior and
capability assertions, not package/class-name architecture tests.

Before interpreting timing, prove that the new path admits substantial work in the actual target queries. Then run
full tests and warmed frozen parent/candidate/parent q09/q13/q21 and neighboring guards. Retain no dormant production
experiment if complete SQL results fail to qualify. The remote accepted-runtime sweep remains frozen and cannot
qualify this later candidate.

Evidence: `2026-09-14-join-build-transition-admission.md` and its retained diagnostic sources, hashes, and counts.

## Qualification and decision

The full candidate passes 2,202 tests with zero failures/errors and 394 skips. Its original generic addRow method
is unchanged. A separate measured-phase counter capture confirms 2,982,400 hinted build rows in q13 and 5,817,856
in q21 across two executions, rather than zero in ADR-0145. q09 admits 648,192 rows; its physical table mix varies
between diagnostic JVMs. The admission gap is real and addressed by the candidate, but admission is not a CPU win.

Frozen parent / candidate / parent SQL qualification uses 40 warmups and 15 measurements per query, identical
distributed topology, heap, host/SPI binaries, query order, input positions, and bytes.

| Query | Parent A median CPU, seconds | Candidate | Parent C | Outcome |
| --- | ---: | ---: | ---: | --- |
| TPC-H q09 | 5.640 | 5.677 | 5.634 | No gain; ranges overlap |
| TPC-H q13 | 2.781 | 2.777 | 2.932 | Matches first parent; later parent is slower |
| TPC-H q21 | 6.399 | 6.425 | 6.425 | No gain; ranges overlap |

Latency ranges overlap for every comparison. Allocation ratios are approximately one. q13's candidate is faster
than only the second parent; identical parent binaries differ by about 5.4%. That is evidence of between-JVM
variation, not a repeatable benefit from batching. Do not explain that variation as a known JIT cause without a
separate capture, or retroactively attribute ADR-0145's entire penalty to its helper extraction.

Remove the new policy, scratch helper, table hint API, bulk loops, and trial tests. Keep no dormant implementation
or enabling flag. Accepted spec and production defaults remain unchanged. Preserve both the tested runtime and
candidate patch outside the repository. No new remote functional gate is needed for a removed candidate; the
already running accepted-runtime sweep remains frozen.

Evidence: `2026-09-14-grouped-build-readmission-qualification.md`,
`results/join-build-readmission-20260914`, and `results/join-build-readmission-counts-20260914`.
