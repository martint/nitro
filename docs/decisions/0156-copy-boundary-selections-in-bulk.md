# ADR-0156: Copy boundary selections in bulk

- **Status:** rejected; production candidate removed
- **Date:** 2026-09-15
- **Spec:** §6, §15, §16, §18
- **Related:** ADR-0012, ADR-0150
- **Evidence:** `~/notes/nitro/2026-09-15-matched-remote-join-attribution.md`

## Context

The matched q21 CPU profile attributes repeated work to checked mask-position access while the host boundary
constructs selected-position and dictionary-ID arrays. Mask position access checks bounds and resolves its current
representation for each row. A batch consumer already knows how many positions it needs and owns the complete
destination. Repeating that dispatch is unnecessary when copying a known selection.

## Proposed contract

Add an optional specialized operation to `Selection` that copies its positions in iteration order into caller-owned
array storage at a checked destination offset. Existing providers inherit a default loop. The Nitro mask adapter
selects dense generation or sparse/domain materialization once, then fills or bulk-copies the destination. No
borrowed backing array escapes; destination storage must not alias selection storage. Invalid bounds fail before
writing, and surrounding destination elements remain unchanged.

The explicit host boundary uses this operation when building selected-position mappings and composes dictionary
IDs in the same owned output array. Dictionary depth,
selection order, field ownership, output lifetime and encoding admission remain unchanged. There is no feature flag,
query recognition, additional cache, scheduler change or allocation domain.

This conforms to first-class selection and outside-loop physical dispatch. It extends the SPI additively rather than
making the generic host context inspect a concrete mask implementation. Acceptance requires semantic and SQL evidence.

## Validation and alternatives

Cover dense, empty, unordered sparse, excluded and dictionary-domain selections; generic providers; nonzero output
offsets; invalid bounds; independent ownership; nested dictionary composition and existing borrowed/owning boundaries.
Run focused and full tests, then warmed parent/candidate/parent SQL guards including q09/q21 and other mask-heavy
output queries. A neutral or regressing candidate is removed rather than shipped on the strength of fewer accessor
calls alone.

Keeping per-position access retains maximal simplicity but repeats checked representation dispatch. Exposing raw
mask arrays through generic `Selection` would couple consumers to representation and lifetime. A new destination-owned
remote accumulator is a separate deferred model change and is not part of this proposal.

## Qualification and decision

The implementation passes 2,202 Nitro tests (394 skipped), 160 focused host tests and 653 eligible host tests, with
zero failures/errors. Source-position counts match in every local and remote comparison. Allocation ranges overlap.
The local W40/M15 parent/candidate/parent bracket has no consistent whole-query gain; ClickBench q29's apparent
local improvement reverses in the first remote comparison and overlaps the repeated remote parent.

The remote W40/M15 bracket uses the same distributed topology, eight active processors, task concurrency eight,
32 GiB maximum heap, normal flags and frozen binaries. Median CPU seconds are:

| Query | Parent A | Candidate | Parent C |
|---|---:|---:|---:|
| TPC-H q09 | 17.160 | 16.985 | 16.835 |
| TPC-H q21 | 20.192 | 20.356 | 20.376 |
| ClickBench q29 | 0.637 | 0.672 | 0.665 |
| ClickBench q39 | 2.058 | 2.048 | 2.056 |

All candidate CPU ranges overlap parent C; all latency ranges overlap both parents. q09 latency medians are
3.000 / 3.190 / 2.994 seconds; q21 is 3.716 / 3.736 / 3.672. The candidate does not qualify a whole-query win.

The intended local mechanism is visible: output-scoped driver samples fall from 16.521% to 14.573%, and repeated
checked mask-access leaves disappear from that sampled scope. Remote q21 source-fragment CPU falls from
4.590 / 4.880 seconds in the parents to 4.070 in the candidate, with separate ranges. Other fragments offset the
saving: these observations do not establish why their cost changed, and do not turn a neutral query into a win.
No claim of eliminated necessary mask materialization follows from absent exclusive samples.

Remove the SPI addition, mask specialization, host consumers and trial tests. Keep the existing contracts and
defaults; retain no enabling flag or alternate production implementation. Preserve the tested snapshots, patches,
raw measurements and profile reports outside the repositories. A later proposal must demonstrate an end-to-end
benefit, not merely fewer checked accessor calls. The accepted full board is unchanged.

Evidence: `~/notes/nitro/2026-09-15-bulk-selection-qualification.md`, `results/bulk-selection-20260915`,
`results/bulk-selection-profile-20260915`, and the mirrored remote `bulk-selection-remote-20260915-*` arms.
