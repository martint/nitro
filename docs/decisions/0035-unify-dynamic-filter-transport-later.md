# ADR-0035: Preserve a path to subsume Nitro dynamic-filter transport under the host

- **Status:** proposed
- **Date:** 2026-09-03 (retrospective proposal from 2026-08-17)
- **Spec:** §13
- **Depends on:** [ADR-0025](0025-use-one-logical-dynamic-filter-lifecycle.md)
- **Historical evidence:** reconciliation section “Deferred: reconcile Trino and Nitro dynamic-filter enforcement”

## Context

The current integration derives Nitro physical source constraints and host connector dynamic filters from one logical
build, but uses different plumbing to reach consumers. Maintaining both transports adds lifecycle and diagnostics even
when their semantics are aligned.

## Decision

Proposed: let host dynamic-filter transport carry the authoritative logical constraint and expose Nitro physical
capabilities as representations/consumers beneath it. Adopt only if host publication preserves prepared membership,
encoded-domain forms, source timing, bounded waiting, and exact residual enforcement.

## Consequences

- Successful unification removes redundant readiness and transport machinery.
- Nitro-specific physical forms may still exist without becoming a second semantic filter.
- Until the host contract can preserve these capabilities, current transport remains.

## Alternatives considered

**Delete Nitro source filtering now.** Loses encoded membership and measured native-reader pruning.

**Keep both mechanisms permanently.** Functional, but duplicates lifecycle, waiting, and observability.

**Move all dynamic filtering into Nitro.** Conflicts with host connector scheduling and ecosystem integration.
