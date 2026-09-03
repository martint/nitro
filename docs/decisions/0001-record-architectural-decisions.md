# ADR-0001: Record architectural decisions beside the Nitro specification

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §20

## Context

Nitro accumulated a large design-principles file and chronological reconciliation log. They preserve evidence, but
decisions, experiments, reversals, and current contracts are interleaved. Code shows the chosen interface; it does not
reliably show which alternative lost or which property the choice protects.

## Decision

Keep numbered decision records under `docs/decisions`, one decision per file. A consequential architecture change adds
its record and updates the specification in the same Nitro change. Records preserve context, decision, consequences,
and alternatives. Accepted records are immutable; later changes amend or supersede them with a new record.

## Consequences

- Current behavior becomes discoverable without reading campaign history.
- Writing down costs and losing alternatives becomes part of completing architecture work.
- Prior decisions require honest retrospective backfill.
- Cross-repository changes cite a central Nitro ADR rather than creating conflicting histories.

## Alternatives considered

**Continue using dated notes.** Excellent evidence, but the current decision depends on reading everything afterward.

**Put reasoning in the specification.** This makes the normative contract argue with itself.

**Use commit messages alone.** They are difficult to discover when a decision spans commits or repositories.
