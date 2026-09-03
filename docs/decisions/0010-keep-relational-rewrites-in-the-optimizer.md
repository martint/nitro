# ADR-0010: Keep relational rewrites in the optimizer

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-23)
- **Spec:** §7, §9
- **Historical evidence:** `2026-07-23-architecture-review-and-trino-integration-boundary.md`

## Context

Several early performance paths recognized a function combination, grouping shape, or benchmark plan inside an
operator and changed the operation being executed. This made the engine depend on SQL identities and produced physical
plans that could differ from the host plan being compared.

## Decision

Relational and expression rewrites that change plan structure belong to the optimizer. Nitro executes the physical
program it is given. Operators and the evaluator may select an implementation from resolved capabilities and observed
physical shape, but do not discover a new logical plan or recognize a query/function combination.

## Consequences

- Plan identity remains inspectable and comparable with the host engine.
- Useful fusion requires an optimizer-authored program or a provider capability with identical semantics.
- Operators remain reusable as new functions and types are registered.
- Some opportunities wait for planner integration instead of being patched into a hot loop.

## Alternatives considered

**Recognize profitable combinations in operators.** Fast to prototype, but couples execution to function identity and
silently changes plan shape.

**Perform no physical selection in execution.** Too restrictive: choosing dense versus sparse loops or a generated key
layout does not change relational semantics and depends on runtime facts unavailable to the optimizer.
