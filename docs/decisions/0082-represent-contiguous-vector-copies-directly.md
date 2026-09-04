# ADR-0082: Represent contiguous vector copies directly

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §5, §11
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)
- **Evidence:** Engine Coverage q241 allocation profile after ADR-0019 retained-storage accounting

## Context

Generic row materialization represents every complete source segment as an integer identity mapping before copying
it. A retained ten-million-position RLE input therefore allocates and fills a forty-megabyte logical-position array
even though the operation is a contiguous range and the physical input contains only a few runs. This discards useful
structure, consumes CPU, and violates the steady-state rule that working mappings come from bounded reusable storage.

Pooling the identity array would account for it but would preserve the unnecessary O(rows) construction and memory
traffic. Teaching the window operator about RLE or primitive values would avoid one case while putting physical-vector
knowledge in an operator and leaving the same defect in every other range-copy caller.

## Decision

The vector SPI represents a contiguous logical source range directly. The operation appends that range into an owned
destination under the existing allocator context. Its compatibility implementation copies positions individually
without allocating range metadata. Physical implementations may override it: flat fixed-width vectors use bulk
storage copies, regions translate their bounds, and RLE vectors traverse intersecting runs and broadcast each physical
run value into the corresponding destination range.

Callers that already know a source is contiguous use this operation rather than constructing an identity mapping.
Arbitrary gather operations retain the position-selection APIs. The range operation carries no logical type or
operator semantics.

## Consequences

- Dense materialization no longer requires one integer per logical source position.
- Encoded vectors preserve their compact traversal until the destination representation requires expansion.
- The operation is reusable by windows, buffering operators, nested reconstruction, and future source/output paths.
- Additional vector representations can add bulk range copying independently without changing callers.
- The compatibility implementation favors bounded allocation over speed; important physical forms require focused
  performance evidence for specialized overrides.

## Alternatives considered

**Borrow an identity array from the allocator.** Rejected because it accounts for storage but still writes and reads
O(rows) redundant indices.

**Special-case RLE in the window operator.** Rejected because operators must remain independent of concrete vector
representations and the same issue exists in generic materialization.

**Require every vector to implement the operation immediately.** Rejected because a correct allocation-free default
allows the SPI to evolve incrementally while hot physical forms receive efficient implementations.
