# ADR-0033: Defer destination-owned exchange accumulation

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; decided 2026-08-13)
- **Spec:** §15, §19
- **Historical evidence:** reconciliation proposal “destination-owned exchange accumulation”

## Context

Letting each exchange destination accumulate selected Nitro positions across input batches could remove temporary
gather/Page/copy work. The measured opportunity was small, while the boundary must preserve authoritative partition
assignment, backpressure, replication, skew, serialization, encryption, memory accounting, and failure cleanup.

## Decision

Defer destination-owned accumulation until broader evidence shows enough absolute boundary cost to justify a new
retained exchange capability. Any future design remains an explicit host boundary, keeps destination assignment in the
host, and specifies ownership for every vector encoding and companion stream.

## Consequences

- Current exchange paths may retain one bounded gather/copy stage.
- Nitro does not import host Page abstractions to optimize the boundary prematurely.
- Future work needs representative queries and ownership/cancellation tests before implementation.

## Alternatives considered

**Implement the persistent destination buffer immediately.** Plausible savings and substantial correctness surface for
a measured fraction of a CPU-second.

**Move partition assignment into Nitro.** Avoids a boundary and changes host distribution semantics outside scope.

**Lend pooled producer buffers to the exchange.** Avoids copies and violates asynchronous retention safety.
