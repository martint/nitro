# ADR-0014: Stream function results into allocator-owned vectors

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-25 and extended 2026-09-03)
- **Spec:** §8, §11
- **Depends on:** [ADR-0013](0013-use-layered-function-calling-conventions.md)
- **Historical evidence:** `2026-08-25-direct-registry-function-output.md`; reconciliation entry for provider-owned reference-result writers

## Context

Adapters that first collected scalar results into host arrays and then built Nitro vectors doubled writes, retained
host objects, and defeated proposed-output reuse. Variable-width and structural results require logical-type knowledge
that Nitro core must not acquire.

## Decision

Function calls write directly into a caller-proposed or allocator-created Nitro destination. Primitive adapters use
typed output vectors. Reference-carrier adapters invoke a result writer supplied by the logical type provider, which
copies or recursively materializes each result immediately. Accepted proposals are writer-owned until finish or abort.

## Consequences

- Intermediate host-result arrays disappear.
- Provider writers must define proposal acceptance, finish, abort, and retained-state rules.
- Writers may retain sizing history but not execution resources or host values.
- VALUES-only calls do not allocate companion streams that were not requested.

## Alternatives considered

**Collect then convert.** Simple, but adds allocation, copying, and host-object retention proportional to rows.

**Teach Nitro about Slice/Block.** Avoids a provider hook but violates classloader and logical-type boundaries.

**Require the caller to allocate every exact result shape.** Impossible for many variable-width and recursive outputs
without executing the function first.
