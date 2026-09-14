# ADR-0149: Admit build dictionaries from cumulative demand

- **Status:** rejected
- **Date:** 2026-09-14
- **Spec:** §5, §9, §13, §16, §18
- **Related:** ADR-0006, ADR-0012, ADR-0036, ADR-0132, ADR-0134, rejected ADR-0147

## Motivation and correlation

The single-batch non-retained join output path compares each output batch with the entire build length before
trying bounded binary-domain compaction. Repeated small outputs never satisfy that test even when their total
demand greatly exceeds the build. A join-instance-scoped diagnostic found that q22/q67 reject every observed
binary output call at this gate; cumulative demand would admit 10,956/10,980 and 2,172/2,220 calls respectively.
The earlier ADR-0147 changed a different, unexecuted multibatch path and remains rejected.

This is a physical amortization gap. Accumulating observed selected demand conforms to adaptive physical
specialization and explicit allocator ownership; it does not recognize a query, logical type or function.

## Evaluated change

Keep the existing immediate dense admission. For sparse non-retained binary output, maintain saturating selected
demand per build batch and output column within one join lifetime. Admit the existing bounded dictionary
constructor once cumulative demand reaches the same ratio. Preserve its exact-byte equality, cardinality bound,
negative cache, valid build extent and raw fallback. Do not modify retained/deferred/multibatch output, host
exchange admission, scheduling or plans, and introduce no opt-in flag.

The selected-demand test also exposed a missing notification: HashJoinOperator returns a Batch with a no-op
constraint callback, leaving its materialization mask dense after the returned batch is narrowed. Route that
callback to the existing producer constraint method, after Batch's invalidation ordering. This fills the §9 and
ADR-0134 producer-lifecycle contract rather than restoring duplicate consumer/operator notifications. It does not
advertise retained or constrained-reborrow capabilities that the join does not own. Existing retained/deferred
materialization algorithms remain unchanged, but now receive the intended current mask.

Counter storage belongs to the build allocator context and is explicitly cleared on acquisition. Compose the
newly admitted per-batch row mapping in the existing allocator-owned output mapping scope instead of allocating
a new int array. Preserve logical extents, borrowed lifetime, detach-on-take and null/error stream mappings.

## Qualification

Cover repeated small output batches, constrained demand, low-cardinality success and near-unique fallback,
independent build domains, duplicate matches, nullable payloads, valid extents within pooled capacity, borrowed
and taken output lifetime, and allocator closure. Run focused/full tests and frozen q22/q67 parent/candidate/
parent timing plus neighboring join guards. Confirm actual admission and downstream domain sizes separately
from timing. Smaller domains alone are insufficient: a high-cardinality sibling can still force Page flattening.
Remove the whole candidate if complete-query benefit does not qualify.

## Outcome

The implementation and its compaction test were removed. All 409 exact/native SQL cases and 2,192 Nitro tests
(394 skipped) passed, but local W20/M15 parent/candidate/parent q22 allocation increased 22–23%, CPU about 3%,
and peak memory rose from 0.21–0.24 GB to 1.52 GB. q67 improved CPU 5–9%; that does not qualify the q22 regression.

Paired allocation/CPU profiles showed a downstream interaction: compact dictionaries activate native destination
partitioning, adding remapping/copying and another host capture copy. Proposed ADR-0150 independently repairs the
partition's missing exclusive-ownership grant. A four-arm original/combined/ownership-only/original run shows
that repair removes the q22 peak blow-up, but combined allocation remains 11–12% above original and CPU benefit
is not consistent across the two controls. q67 CPU remains about 4% lower. Do not retain this admission rule behind
an experimental flag or a query-specific exception. Smaller domains alone do not justify the complete path.

The independently exposed hash-join batch notification defect must be repaired separately: a dedicated test shows
the old callback requests all 3 probe payload positions after only 1 is selected. Its fix is not evidence for accepting
cumulative dictionary compaction. Neither source fix is smuggled into this rejected experiment's commit.

Full evidence: `~/notes/nitro/2026-09-14-cumulative-build-domain-qualification.md` and
`~/notes/nitro/2026-09-14-native-partition-ownership-qualification.md`. Frozen candidate artifacts and profiles
remain available; no rejected timing replaces a synchronized-board cell.

Evidence and initial design: `~/notes/nitro/2026-09-14-cumulative-build-domain-admission-plan.md`.
