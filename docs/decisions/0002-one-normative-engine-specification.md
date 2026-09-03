# ADR-0002: Keep one normative specification for Nitro and its host integration

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** entire document
- **Depends on:** [ADR-0001](0001-record-architectural-decisions.md)

## Context

Architecture is spread across target contracts, evaluator documents, principles, reconciliation notes, and integration
campaign notes. Several describe an earlier implementation or explored future as current. Nitro and its host
integration need one vocabulary and one answer about island, scheduler, allocator, type, function, and connector
contracts.

## Decision

`docs/SPEC.md` is the normative architecture specification for Nitro and its host integration. Supporting documents may
explain implementation, evidence, and migration history, but a contradiction is resolved in favor of the specification
unless a new ADR changes it. The spec states the accepted target contract while known coverage gaps remain explicit.

## Consequences

- Engineers have one entry point for concepts and invariants.
- Spec changes must remain synchronized with architectural code changes.
- Older documents need deprecation labels and gradual migration rather than a risky one-time rewrite.
- Productization must distinguish “specified but incomplete” from “unsupported by design.”

## Alternatives considered

**Use design principles as the spec.** They mix contracts with thousands of empirical hot-loop lessons.

**Maintain separate Nitro and host-integration specs.** The boundary would be described twice and drift.

**Describe only current implementation.** Temporary gaps would become the architecture instead of visible work.
