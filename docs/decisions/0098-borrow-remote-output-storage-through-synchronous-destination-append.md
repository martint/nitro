# ADR-0098: Borrow remote-output storage through synchronous destination append

- **Status:** accepted
- **Amended by:** [ADR-0141](0141-mix-owned-and-borrowed-synchronous-egress.md), which replaces all-channel admission
  with per-channel borrowed/owned conversion while preserving the non-retaining consumer contract below.
- **Date:** 2026-09-10
- **Spec:** §15, §17, §18
- **Supersedes:** [ADR-0033](0033-defer-destination-owned-exchange-accumulation.md)
- **Evidence:** direct-destination remote-output coverage and adaptive A/B campaign at Cork `0e344495` and Nitro
  `200f328f`, retained under `~/notes/nitro/results/direct-destination-remote-output-20260910/`

## Context

ADR-0033 deferred a Nitro-owned destination accumulator because the observed boundary cost was small and a new
asynchronously retained ownership domain would have had to reproduce the host's buffering, backpressure,
serialization, routing, and failure contracts. The existing Cork destination builders already own those contracts.
The remaining avoidable work is earlier: an eligible Nitro output batch was first copied into independently owned
host blocks, then read again while the host partitioner copied selected rows into its destination-owned builders.

A temporary borrowed host view can remove the first copy without introducing a persistent Nitro accumulator, but only
if the complete consumer path is synchronous and non-retaining. An earlier encoding-retaining experiment appeared to
return from the callback safely while a destination appender still held a transitive dictionary base. Sequential
queries then reused and changed that storage. The capability must therefore describe the destination consumer, not
only the immediate adapter call.

## Decision

The host remote-output adapter may borrow Nitro value storage only through an explicit callback whose consumer
contract excludes retention. The consumer copies every selected value into destination-owned storage or completes an
encoding-preserving serialization before returning. It then retains no source block, dictionary or RLE wrapper,
backing array, or companion stream. The Nitro source batch remains live through the callback and closes afterward.

Admission is structural and all-channel. Each registered type adapter must prove that the exact physical value vector
can be exposed with the host type's semantics. The initial capability covers flat 32-bit and 64-bit integral storage
whose host block width agrees, and flat binary storage. `CHAR` binary storage additionally proves that no value needs
trailing-space normalization. Unsupported vectors or any channel requiring semantic conversion select the existing
owning boundary adaptation.

Dense input exposes only its logical range. Sparse input may use a callback-scoped dictionary selection over the
borrowed flat base; the destination's direct append path maps the selection once and appends the underlying values
without retaining that temporary dictionary. A destination path that elects to preserve an encoding flushes it within
the same callback. Borrowing is not inferred from a normal appender call or from return timing.

Partition assignment, destination buffering, output accounting, replication, skew handling, backpressure,
serialization, encryption, and failure cleanup remain host-owned and unchanged. Nitro core does not acquire host
`Page` or `Block` dependencies. Boundary metrics distinguish borrowed and owning batches and retain conversion and
append timing.

## Consequences

- Ownership tests cover dense and sparse borrowed views, retaining-consumer rejection, direct destination appends,
  dictionary mapping reuse, `CHAR` normalization, and sequential query reuse.
- The complete TPC-H, TPC-DS, and ClickBench functional inventory passes: 22/22 TPC-H queries, 103/103 TPC-DS
  variants, and 43/43 ClickBench queries. The borrowed path activates on 38 queries (6, 31, and 1 respectively).
- An adaptive W5--15/M5--7, 3% A/B over all 38 activated queries has exact capability and physical-input parity.
  Against forced owning conversion, summed wall time improves 1.86%, summed CPU 2.15%, and allocation is effectively
  neutral at +0.51%. The CPU geometric mean improves 1.34%, with no query regressing CPU by 3% or more.
- TPC-H q21, the largest activation, improves from 1,691 to 1,623 ms wall, 6,793 to 6,707 ms CPU, and 13.849 to
  13.777 GB allocated. The subject borrows about 1,545 of 7,560 adapted batches per run.
- The earlier persistent native destination accumulator remains unimplemented. It still requires separate evidence
  and a complete asynchronous ownership and memory-accounting contract if reconsidered.

## Alternatives considered

**Keep the owning host-block conversion.** Safe and simple, but the broad matched cohort shows repeatable CPU and wall
cost with no compensating CPU regression.

**Let ordinary destination appenders receive borrowed blocks.** Rejected because some appenders retain encoded bases
across calls; callback return alone is not a lifetime proof.

**Transfer every eligible Nitro vector into the host page.** This replaces a bounded synchronous borrow with a new
cross-allocator ownership and accounting protocol and still does not guarantee that all transitive wrappers transfer.

**Implement the persistent Nitro destination accumulator proposed before ADR-0033.** It may remove more boundary work,
but it remains a substantially larger asynchronous contract and is unnecessary for the measured improvement here.
