# ADR-0040: Use one normalized, mask-aware expression IR and evaluator

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-24)
- **Spec:** §6–8
- **Depends on:** [ADR-0008](0008-use-first-class-masks-and-streams.md), [ADR-0010](0010-keep-relational-rewrites-in-the-optimizer.md)
- **Historical evidence:** `docs/architecture-target-contracts.md`; architecture-review sections on conditional structure and projection lowering

## Context

Scalar functions, filter predicates, projections, conditionals, and operator-specific expressions once used overlapping
evaluation mechanisms. Implicit IF/COALESCE behavior hid branch masks, and eager intermediate vectors prevented reuse
across projected outputs.

## Decision

Lower scalar computation to one typed expression DAG with explicit calls, inputs, literals, references, copies,
merges, structural access, masks, and requested streams. Normalize special forms before execution. One evaluator owns
lazy demand, within-batch memoization, physical dispatch, generated composition, nulls, and errors.

## Consequences

- Filter/project/conditional expressions share semantics and optimization infrastructure.
- Repeated subexpressions can reuse results within a batch and generation.
- Normalization and IR validation become correctness boundaries.
- Operators orchestrate expression programs rather than implement scalar evaluators.

## Alternatives considered

**Separate filter and projection engines.** Familiar and duplicates dispatch, null, mask, and code generation.

**Represent special forms as ordinary eager function calls.** Hides conditional evaluation and error isolation.

**Generate directly from host expression trees.** Can be fast but binds Nitro to one planner IR and bypasses its
registry-neutral contract.
