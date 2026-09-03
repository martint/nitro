# ADR-0025: Use one logical dynamic-filter lifecycle with multiple physical consumers

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-23)
- **Spec:** §13
- **Depends on:** [ADR-0018](0018-publish-immutable-task-scoped-builds.md)
- **Historical evidence:** reconciliation entries “let Trino own dynamic-filter readiness” and prepared-build source reuse

## Context

Nitro can apply prepared join membership directly while the host also transports dynamic filters to connectors.
Independent readiness, domains, or publication rules risk duplicate waiting, inconsistent pruning, and two authorities
for the same join constraint.

## Decision

One finalized build-derived constraint defines dynamic-filter semantics. The host owns readiness and scheduling policy
in the current integration. Nitro operators and source capabilities may consume physical forms derived from that same
constraint, while the exact join predicate remains the correctness backstop.

## Consequences

- Waiting is not multiplied per split or source.
- Row-group, dictionary, bloom, range, and exact membership forms can serve different physical boundaries.
- Diagnostics distinguish rows/bytes pruned, wait time, publication, and exact enforcement.
- Transport unification remains possible without changing semantic authority.

## Alternatives considered

**Independent Nitro and host filters.** Locally convenient and redundant, with inconsistent timing and domains.

**Use only the host's materialized domain.** Simple integration but can discard prepared physical membership and encoded
source capabilities.

**Treat source pruning as exact predicate replacement.** Incorrect for probabilistic or incomplete metadata filters.
