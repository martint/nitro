# ADR-0153: Share constructed partition-copy pools

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §11, §15, §18
- **Related:** ADR-0007, ADR-0041, ADR-0108, ADR-0150

## Context and correlation

Each partition copy creates a fresh allocator context with a fresh pool group. Once a partition is released, its
idle vectors remain in a group that no subsequent copy requests. Long-lived repeated partitioning therefore retains
and allocates storage proportional to the number of completed copies, rather than reusing a bounded idle working set.
The host exchange already carries an explicitly constructed boundary-copy group, but does not supply it to Nitro's
partition copier.

This is an implementation gap in SPEC §11's separation of attribution/ownership scopes from shared reuse domains.
It does not propose destination-owned accumulation, different partition assignments, new flushing behavior, a
process-static pool, or changes to the host scheduler. Sharing a pool never grants ownership of live vectors.

## Proposal

Give each partitioner a constructed pool group. Standalone construction owns an instance group; hosts may pass an
existing boundary group shared by successive copiers. Each output copy still creates its own ownership context, so
failure cleanup cannot release other copies. Only idle, released vectors may be reused; retained and asynchronous
leases continue to prevent reuse until their ownership ends.

Pass the existing host boundary-copy group at both host partition-copy construction sites. No host logic outside
Nitro integration changes. Preserve policy, representation, reservation charging, and explicit asynchronous transfer.

## Qualification

Prove physical buffer reuse and stable allocated/resident vector bytes across repeated completed copies. Hold an
asynchronous lease while other copies close and reuse storage, and prove that its values remain independent. Audit
error cleanup, separate allocation scopes, and variable-capacity/encoded vectors through the existing ownership tests.
Measure query-lifetime component copying and warmed SQL with allocation, CPU, latency, and peak memory. The current
remote sweep remains frozen on ADR-0150 + ADR-0151 and cannot qualify this later change.

## Outcome

Accept the constructed reuse group while preserving separate live ownership scopes. Full Nitro tests pass:
2,197, zero failures/errors, 394 skipped; 645 eligible host tests pass. Tests prove reuse after release, independent
outstanding asynchronous leases, and failure isolation. Two-fork component results reduce allocation in all local
and asynchronous release cases; SQL results do not justify attributing the entire q22 difference to this change.

The initial q67 CPU penalty did not reproduce either in a longer standalone reverse bracket or in a reverse bracket
preserving the original preceding q22 query. Identical candidate JVMs also varied, so within-JVM ranges alone do
not establish small-delta equivalence. q31/q39 guards show no established regression. The frozen paired runtime
passes all 409 exact-result/native-execution checks on September 14, separately from the accepted performance board.

Evidence: `2026-09-14-partition-pool-reuse-qualification.md`, the independent component and SQL brackets it names,
and `results/partition-pools-functional-20260914` under the Nitro notes directory. Primitive-array recycling gaps
discovered during this audit are separate work, not implicitly solved by pool-group reuse.
