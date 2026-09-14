# ADR-0150: Transfer exclusive native partition output ownership

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §9, §11, §15, §18
- **Related:** ADR-0007, ADR-0041, ADR-0108, rejected ADR-0149

## Context and correlation

NativeBatchPartitioner copies selected rows into independent allocator-owned output buffers, but returns ordinary
batches. Those batches do not grant asynchronous ownership transfer. The host exchange correctly declines to infer
ownership from allocator provenance and copies them again during capture. ADR-0149's dictionary experiment exposed
this path in q22: allocation profiles newly attribute substantial allocation to both partition copying and capture.

This is a missing producer capability, not a reason to weaken the host boundary check. It conforms to the existing
exclusive retained-batch contract, explicit ownership, and reservation handoff. It changes neither scheduling nor
partition assignment, dictionary admission, buffering architecture, logical semantics or function implementations.

## Decision

Publish independently copied partition results through the existing RetainedBatch lifetime. Acquire its vector-tree
lease before closing the original per-output owners. The lease may transfer asynchronously only when the existing
allocator exclusivity proof succeeds. Keep ordinary borrowing and closure correct, reject a foreign allocator's
transfer request without mutation, and preserve all streams and shared dictionary mappings.

## Qualification

Prove asynchronous detachment, independence from source mutation and subsequent partitions, shared mapping identity,
closure on another thread, and no live allocator bytes after all owners close. Existing partition tests must continue
to pass. Add a host capture test proving physical-vector identity survives capture, not only equal results.
Run focused/full Nitro and eligible host tests, then frozen paired SQL with allocation and CPU attribution.
Qualify independently against the accepted engine and against ADR-0149; do not hide the latter's regression by
reporting only its improvement over an already regressed candidate.

## Outcome

The standalone ownership change passes 2,191 Nitro tests (394 skipped). The final ownership plus independent
mask-notification repair passes 2,192 Nitro tests, 644 eligible host tests, and all 409 exact/native SQL cases across
the four suites. Four established parent-failing host classes remain excluded. Tests demonstrate vector identity,
zero additional Nitro buffer allocation at capture, rejected foreign ownership, and safe asynchronous release.

Local warmed q31 parent/candidate/parent measurements consume 5.036 / 4.778 / 5.030 CPU-seconds and allocate
14.027 / 13.382 / 14.026 GB, with separated ranges. Latency ranges overlap. q39 is neutral within observed variation;
q22 shows no consistent CPU effect across drifting fresh-JVM controls and has neutral standalone allocation.
The cumulative dictionary proposal remains rejected: fixing ownership does not qualify its remaining overhead.

This accepts the existing exclusive transfer contract at a missing producer, not a new exchange buffering policy.
The published board is unchanged. Evidence and frozen runtime identities are recorded in
`~/notes/nitro/2026-09-14-native-partition-ownership-qualification.md`.
