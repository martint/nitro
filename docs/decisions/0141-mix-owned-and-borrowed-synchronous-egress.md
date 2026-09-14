# ADR-0141: Mix owned and borrowed synchronous egress

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §5–6, §11, §15–18
- **Amends:** [ADR-0098](0098-borrow-remote-output-storage-through-synchronous-destination-append.md), all-channel admission only
- **Related:** ADR-0098, ADR-0106, rejected ADR-0140

## Context and contract amendment

ADR-0098 admits synchronous borrowing only when every channel has directly compatible physical storage. ADR-0140
confirmed that extending one encoding alone does not remove the obstruction: both tested SQL shapes still report
zero borrowed batches. A single short-width or semantically transformed column can force otherwise compatible
wide columns through a redundant owning copy before destination append.

Admission changes from all compatible to at least one compatible channel. Each compatible column
retains its registered adapter's exact physical proof and callback-scoped lifetime. Each incompatible column uses
ordinary owning conversion over the actual selection, never an all-position prefix. The temporary Page may contain
both kinds of storage, but the complete consumer remains explicitly synchronous and non-retaining. Retaining
consumers continue to receive fully owning Pages. No function semantics or source ownership proof is weakened.

This amends the all-channel admission detail of ADR-0098, while preserving its stronger consumer/lifetime
contract and ADR-0106's selected-extent bound. It does not change the scheduler, host partition assignment,
serialization format or output accounting. Existing registered adapters remain responsible for types; the boundary
does not recognize queries, functions or fixed column combinations. Borrowed callbacks use the host's existing
direct append path, which may consume encoded owning siblings differently from the ordinary buffered path.
The host implementation is unchanged, but selecting this path more often requires performance qualification.

## Qualification

Test mixed physical carriers, normalization and errors on selected rows, dense and sparse masks, retaining-consumer
rejection, and actual borrowing of only the compatible arrays. Existing destination tests must still prove that no
borrowed flat, dictionary or RLE base survives callback return. Measure activated queries, including q14/q39, and
guard increased flushing or encoding expansion. Compare CPU, allocation, latency, rows and bytes, not copies alone.
Accept only with broad correctness and warmed SQL evidence; otherwise remove the candidate. No feature flag.

The preceding frozen full board excludes this change. A new synchronized board measures the combined qualified
implementation without replacing individual cells in that preceding board.

## Local qualification

The first implementation lost buffered dictionary policy when copying incompatible siblings. A nine-query guard
exposed a q21 CPU and allocation regression. The corrected implementation shares the existing owning dictionary
context construction, including domain cache, mixed-encoding policy and externalization. Tests verify actual borrowed
array identity, owned dictionary retention, sparse selected extents and retaining-consumer rejection. The complete
eligible host family passes 641 tests; the four documented parent-failing legacy classes remain excluded unchanged.

In a 30-warmup/15-measurement parent/candidate/parent q21 run, CPU is 6.508/6.508/6.485 seconds with overlapping
ranges. Allocation falls 13.658/13.314/13.630 GB with separated ranges. A nine-point, three-suite breadth guard
finds separated CPU improvements for q22 and q67 against both controls, and no CPU loss separated from both.
A longer q29 guard gives 215/216/238 ms CPU: unchanged-control drift prevents a point-ratio speedup claim.

These are local same-engine qualification results, not replacements for the synchronized Cork comparison. The
combined exact-runtime remote gate passed all 409 queries, including strict native execution and exact results,
at 07:57 UTC. The accepted scope is the callback ownership change with preserved owning conversion policy; the
new full-board magnitude remains pending and is not inferred from local point results.
Evidence: `2026-09-14-mixed-egress-qualification.md` and its linked immutable campaign artifacts.
