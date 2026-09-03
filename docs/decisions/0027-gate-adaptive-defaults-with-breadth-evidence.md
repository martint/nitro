# ADR-0027: Gate adaptive defaults with breadth evidence and expose their decisions

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established progressively through 2026-07–08)
- **Spec:** §16–18
- **Historical evidence:** `design-principles.md`; reconciliation entries for admitted and rejected adaptive mechanisms

## Context

Most Nitro fast paths win only in regions defined by density, cardinality, encoding, locality, or retained-state cost.
A default tuned to one benchmark query can create cliffs elsewhere, while silent adaptation makes regressions hard to
attribute.

## Decision

Adaptive admission depends only on bounded observed physical facts and immutable policy. Strategy choice occurs outside
position loops, supports safe fallback and re-adaptation, resists oscillation, and reports admission reason, selected
strategy, transitions, and achieved reduction. A default changes only after correctness, focused A/B, adverse-shape,
and broad-suite evidence.

## Consequences

- Physical heuristics remain query/type/function neutral.
- Parameterized response curves and boundary probes are required as coverage grows.
- Rejected experiments are removed rather than retained as dormant flags.
- Conservative fallback may leave performance unrealized until sufficient evidence exists.

## Alternatives considered

**One universal strategy.** Predictable and predictably wrong as density or encoding changes.

**Query-specific heuristics.** Strong benchmark results with no reusable architecture.

**Keep every candidate behind a property.** Preserves experimentation by shipping multiple unowned production paths.
