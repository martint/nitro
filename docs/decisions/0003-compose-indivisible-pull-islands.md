# ADR-0003: Compose each Nitro island as one indivisible pull graph

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective)
- **Spec:** §3, §9, §15
- **Historical evidence:** `2026-07-24-design-reconciliation.md`, “make physical Nitro islands indivisible”

## Context

Representing adjacent Nitro stages as separate host operators inserted Page/Block adaptation, created separate vector
pooling domains, and forced copies or unsafe retention between stages. Allowing unsupported composition to split an
island hid coverage gaps and made “Nitro execution” include host compute in its middle.

## Decision

A maximal supported plan region is one composed Nitro batch-pull graph and one ownership domain. Adjacent Nitro stages
communicate only through Nitro interfaces. Planning composes the region or rejects it with a diagnostic; it never
decomposes an admitted island as fallback.

## Consequences

- Encodings, masks, vector identity, and pooling survive operator boundaries.
- Missing composition becomes an explicit correctness and coverage failure.
- Host adaptation remains at real ingress, egress, exchange, and result boundaries.
- Planner and corpus tests enforce the invariant across complete plans.

## Alternatives considered

**One host operator per Nitro operator.** Simple integration, but preserves the Page/Block and allocator seams Nitro is
intended to remove.

**Fuse only profitable combinations.** This makes correctness and ownership depend on performance heuristics.

**Fall back locally inside an island.** The result is neither a Nitro island nor an explicit host fallback and cannot be
measured or reasoned about cleanly.
