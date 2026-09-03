# ADR-0024: Confine Page and Block adaptation to explicit host boundaries

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-23)
- **Spec:** §2, §9, §15
- **Depends on:** [ADR-0003](0003-compose-indivisible-pull-islands.md), [ADR-0023](0023-put-vectors-and-allocation-in-the-connector-spi.md)
- **Historical evidence:** `2026-07-23-architecture-review-and-trino-integration-boundary.md`

## Context

Trino's Page/Block model is a valuable ecosystem boundary but does not express Nitro masks, stream demand, allocator
ownership, or all encoded-domain metadata. Allowing it between Nitro stages creates repeated conversion and makes the
host representation Nitro's object model.

## Decision

Page, Block, and ConnectorPageSource appear only in host adapters at explicit ingress, egress, exchange, or result
boundaries that have not yet become native. Core Nitro and edges within an island use Nitro contracts exclusively.

## Consequences

- Host interoperability remains available during incremental rollout.
- Every remaining adapter is visible integration debt with measurable cost.
- Adapters preserve logical length, laziness, encodings, null/error semantics, and ownership where possible.
- Moving a boundary requires a native replacement, not hiding the adapter.

## Alternatives considered

**Use Page/Block as Nitro's batch/vector API.** Minimizes integration code but prevents the ownership and mask model.

**Prohibit all adaptation immediately.** Architecturally clean and blocks incremental integration with exchanges and
unsupported sources.

**Adapt between every operator.** Easy host composition and negates native islands.
