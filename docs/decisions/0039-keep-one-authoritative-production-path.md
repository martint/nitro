# ADR-0039: Keep one authoritative production path after a decision is settled

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established throughout architecture cleanup)
- **Spec:** §2, §4, §16, §19
- **Depends on:** [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Historical evidence:** `2026-09-02-dead-code-and-generated-adapter-audit.md`

## Context

Prototype implementations, benchmark-only operators, and property-selected alternatives accumulated in production
packages. Even when unreachable from the current planner, they obscured the authoritative path, duplicated semantics,
and allowed later work to revive a rejected design accidentally.

## Decision

Once evidence settles an implementation or default, remove the losing production path, its ambient switch, and tests
that exist only to preserve it. Keep useful experimental fixtures under tests or tools. A remaining alternative must be
an actively admitted capability selected by general physical policy, not dormant historical code.

## Consequences

- Production code describes what can actually execute.
- Reversing a removed choice requires an explicit ADR and implementation rather than flipping a stale property.
- Benchmark controls can remain outside the engine for reproducibility.
- Cleanup must prove registry/planner reachability before deletion.

## Alternatives considered

**Keep alternatives disabled.** Convenient for experiments and permanently expands review, test, and semantic surface.

**Keep every property as an emergency escape hatch.** Makes decisions provisional forever and production behavior
process-global.

**Delete all rejected experimental evidence.** Simplifies the tree but loses useful controls; test/tool isolation is
the better boundary.
