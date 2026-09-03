# ADR-0005: Inject all execution dependencies and policies at construction

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective)
- **Spec:** §4

## Context

Static pools, registries, system-property switches, and class-load-time policies made behavior process-wide, hard to
attribute, difficult to isolate across classloaders, and impossible to reason about per query or task.

## Decision

Every execution service and immutable policy is supplied during construction. Static or system-property factories may
translate configuration only at the composition root. Operators and data structures retain instance-owned policy and
do not perform ambient lookup.

## Consequences

- Query/task isolation and deterministic testing improve.
- Composition roots must construct complete dependency graphs.
- Sharing is explicit through injected task/island resources.
- Policy additions require API work rather than a convenient global toggle.

## Alternatives considered

**Static defaults with overrides.** The default remains ambient and becomes the path production silently uses.

**Thread-local services.** They hide lifetime and fail under scheduler migration.

**System properties read by operators.** Useful experimentally, but retain two production behaviors and process-global
policy.
