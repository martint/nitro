# ADR-0160: Compact provisional direct build domains

- **Status:** rejected; removed after repeated remote q21 regression
- **Date:** 2026-09-15
- **Spec:** §§11, 13, 16, 18
- **Depends on:** ADR-0027, ADR-0113, ADR-0158

## Context

Exact observed build keys can span a large raw address range while containing invariant interior bits. Existing
compressed completed-table lookup does not reduce construction-map storage. Sampling cannot prove a future key
invariant; a full proof pass adds input work and requires separate scheduler-cooperation design.

## Evaluated proposal

For already-admitted direct builds, derive an optional reversible physical mapping from the existing selected,
non-null sample. Only power-of-two initial raw capacities admit compression; other custom capacities keep their
existing raw representation and growth behavior. Remove only invariant bits below the highest observed varying bit; leave higher bits variable so
an ordered prefix can grow without forcing a transition merely at its sampled maximum. Decline constant samples.
Check the invariant at every insertion. On a contradictory raw-representable key, widen only the contradicted
bits and remap existing state. Widening is monotonic; each compact remap must at least double storage capacity,
otherwise return permanently to raw direct storage. Never exceed the equivalent raw capacity. Existing out-of-range
keys still cause exact hash fallback. Probing never mutates the mapping.

Heads and dense duplicate metadata use one physical coordinate system. Sparse groups keep their identities;
sparse/dense admission uses the equivalent raw capacity to preserve its prior policy. All arrays remain pooled,
accounted and released. Exact publication reconstructs original keys and prepared probes share immutable build state.

This conforms to provisional admission and physical specialization contracts; it does not change logical identity,
raw-key admission, null/selection semantics, host planning or scheduling. No enabling flag is added.

## Costs and alternatives

Bit extraction and invariant checks add work to insertion and probing. A late escape requires remapping and temporary
coexistence of old/new arrays. Geometric growth bounds cumulative compact remapping work by the final capacity;
there can be at most one final raw transition. Preserving a raw map, or establishing a complete proof before
construction, are viable alternatives. Smaller storage alone is not enough to accept this proposal.

The initial local prototype abandoned all compression at the first contradiction. An activation capture showed
that one sampled false invariant in a q09 partition discarded other still-valid invariant bits. This proposed
revision retained that remaining compression while bounding transition cost. The earlier raw-fallback proposal and
its measurements remain recorded in the evidence note; neither version was accepted.

## Required qualification

Mechanism-sensitive tests must cover growth, duplicate references/counts, selections/nulls, invariant misses, late
escape, hash fallback, exact key publication, prepared sharing, pool reuse, and non-power-of-two custom capacities. Measure favorable, ordered-prefix
and uncompressible domains. Require warmed SQL qualification with repeated controls, neighboring workloads and
peak-memory evidence, followed by the full test suite. Reject and remove the implementation if it does not qualify.

Evidence and qualification status: `~/notes/nitro/2026-09-15-compact-build-domain-proposal.md`.

## Qualification

The capacity-guard revision passes 35 focused tests and the full suite of 2,205 tests (394 skipped), with no
failures or errors. The remote exact/native functional gate passes all 409 SQL identities. No correctness or
performance result from this candidate has replaced the accepted synchronized board.

Local W40/M15 repeated controls give q09 CPU of 5.439 / 4.994 / 5.473 seconds and latency of
1.357 / 1.209 / 1.365 seconds for parent/candidate/parent. Allocation and peak reservation also decrease; all four
candidate ranges separate favorably from both controls. No neighboring CPU or latency loss separates from both
controls in that run. The preceding compact-mapping revision also passed a 22-query TPC-H breadth bracket.
These observations do not substitute for the final revision's remote performance qualification.

## Outcome

Removed the implementation and its candidate-specific tests. The accepted implicit-singleton-tail repair remains
intact. No enabling flag or dormant compact-build strategy remains in production, and no candidate result replaces
a synchronized-board cell.

After removal, the full suite passes 2,199 tests (394 skipped), with no failures or errors. Rebuilt engine classes
match the accepted parent exactly (SHA256 `386f2491b64ddf4fe7ce72f582452cda7c254319922b92133cc7dc5436a3c3a0`).

Remote q09 improves in the first W40/M15 parent/candidate/parent bracket: CPU is 16.687 / 15.505 / 16.533 seconds,
allocation is 12.467 / 11.633 / 12.534 GB, and peak reservation is 1.657 / 1.030 / 1.670 GB. Those three candidate
ranges separate favorably from both controls. Latency improves centrally but overlaps the repeat control's range.

The q21 guard does not qualify. Its first candidate overlaps the first control, but a second candidate/control
extension confirms the loss: surrounding controls use 19.388 / 19.906 CPU-seconds versus 20.814 for the candidate,
with separated ranges (candidate/control 1.074 / 1.046). Latency is 3.495 / 3.603 seconds versus 3.816, overlapping
the final control's range. Allocation and source positions/bytes are unchanged. Local TPC-H breadth did not expose
this remote penalty. A favorable target and a passed correctness gate are insufficient to retain the candidate.

The precise compiler or raw-path cause remains unproved. A future proposal must isolate compact mapping from
uncompressed execution and qualify both; do not revive this implementation solely because it reduces map storage.
Evidence: `compact-build-r4-remote-20260915-vs-parent-{a,c}.txt`, the repeated five-query campaign
`compact-build-r4-repeat-20260915`, and `2026-09-15-compact-build-domain-proposal.md` in the notes archive.
