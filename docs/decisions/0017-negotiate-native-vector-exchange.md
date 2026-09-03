# ADR-0017: Negotiate native vectors and leases across exchanges

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-03–15)
- **Spec:** §9, §11, §15
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md)
- **Historical evidence:** reconciliation entries for asynchronous vector leases, native local exchange, and encoded remote transport

## Context

Materializing every partition or exchange batch as host Pages discarded Nitro encodings and ownership continuity.
Directly lending mutable or pool-owned buffers to asynchronous consumers, however, allowed reuse before the consumer
finished and made cancellation unsafe.

## Decision

An exchange boundary negotiates the physical representations it can retain. Compatible local paths transfer a
reference-counted Nitro lease; retaining or remote paths receive independently owned storage or a negotiated portable
encoding. The exchange owns that storage until all consumers release it. Borrowed batch memory never crosses an
asynchronous boundary.

## Consequences

- Dictionary/RLE mappings and native layouts can survive transport.
- Local transfer can avoid Page/Block and redundant copies.
- Remote serialization remains an explicit boundary and may compact or copy.
- Lease/reference and cancellation behavior become part of exchange correctness.

## Alternatives considered

**Always convert to Page.** Safe and compatible, but erases vector continuity and adds allocation.

**Borrow producer buffers asynchronously.** Minimal copying and unsafe under pool reuse or cancellation.

**Require one native representation everywhere.** Avoids negotiation but makes all consumers pay for the widest
capability.
