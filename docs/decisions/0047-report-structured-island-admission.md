# ADR-0047: Report island admission and rejection through semantic categories

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-09-01)
- **Spec:** §9, §17, §19
- **Depends on:** [ADR-0029](0029-make-coverage-gaps-explicit.md), [ADR-0043](0043-test-capabilities-and-invariants-not-names.md)
- **Historical evidence:** `2026-09-01-pattern-recognition-and-table-function-operator-contracts.md`

## Context

A Boolean “not supported” or exception text cannot produce a completeness inventory or distinguish a missing operator,
type, function capability, semantic modifier, ordering, provider level, or boundary. Class-name-based assertions are
equally unstable.

## Decision

Planning returns a structured admission result with stable operator-family and semantic rejection categories. It names
accepted sources/boundaries and distinguishes unsupported operator, logical type, registry capability, modifier,
ordering/partitioning, native provider, and adaptation-only provider. Tests and coverage reports consume these semantic
identities.

## Consequences

- Productization gaps can be generated from resolved plans rather than handwritten lists.
- Diagnostics explain why an island stopped without exposing Java implementation names.
- New rejection classes must extend a stable machine-readable taxonomy.
- Admission reporting becomes part of planner/integration compatibility.

## Alternatives considered

**Boolean admission.** Sufficient for fallback and insufficient for coverage, debugging, or invariants.

**Free-form exception messages.** Human-readable and unstable for automation.

**Class/package-based categories.** Easy to derive and coupled to refactoring rather than semantics.
