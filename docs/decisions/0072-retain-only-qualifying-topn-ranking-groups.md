# ADR-0072: Retain only qualifying Top-N ranking peer groups

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §9, §11, §16
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0011](0011-generate-specializations-from-physical-capabilities.md), [ADR-0019](0019-account-retained-state-to-the-host.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)
- **Evidence:** Engine Coverage q229 and the unpartitioned Top-N ranking allocation audit

## Context

The unpartitioned Top-N ranking operator selected a bounded result only after retaining every input batch. A query
whose rank limit was two consequently retained 100 million input rows across its distributed partial stages. The
selection algorithm was bounded, but its input lifetime was not: peak query memory reached 1.81 GB and whole-query
allocation reached 5.48 GB for a result of three rows.

SQL ranking modes require different retention rules. `ROW_NUMBER` needs at most the limit rows. `RANK` must retain
every row in a boundary peer group whose preceding-row count is below the limit. `DENSE_RANK` must retain the first
limit distinct peer groups. A boundary group can itself be large, so no correct implementation can promise storage
bounded only by the numeric limit.

## Decision

Unpartitioned Top-N ranking consumes input incrementally and retains only peer groups that can still satisfy the
requested ranking limit. New better groups evict worse retained groups immediately. Evicted slots are reused, and
payload is copied only for candidates that remain live at the end of the offered batch.

The state is generic over ordering types and arity. It uses provider-backed structural comparison and the shared
allocator-owned Top-N row storage; it does not recognize SQL types or functions. Both pull composition inside a Nitro
island and the externally offered-batch session use the same state.

Retained memory scales with the qualifying result, including semantically required ties, rather than with total input.
Partitioned ranking retains its existing implementation until an equivalent per-partition state is implemented and
validated.

## Consequences

- Small unpartitioned rank limits no longer turn a large input into input-sized retained state.
- `ROW_NUMBER`, `RANK`, and `DENSE_RANK` preserve their distinct peer semantics.
- Very large qualifying tie groups may still require large state because every tied output row is observable.
- Partitioned Top-N ranking remains a visible bounded-state coverage gap.
- The shared state keeps host-session and native pull paths behaviorally aligned.

## Alternatives considered

**Retain all input and select at finish.** Simple and correct, but makes a bounded result consume memory proportional
to input and caused the measured regression.

**Use an ordinary N-row heap for every ranking mode.** Correct for `ROW_NUMBER`, but loses boundary ties for `RANK`
and distinct peer groups for `DENSE_RANK`.

**Specialize for the benchmark's fixed-width key.** Faster to implement locally, but violates Nitro's generic
provider-owned type contract and leaves composite and structural ordering unresolved. ADR-0073 instead defines an
optional provider proof that any ordering consumer can use without recognizing the logical type.
