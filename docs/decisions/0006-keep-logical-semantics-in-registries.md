# ADR-0006: Keep logical types and function semantics in dynamic registries

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective)
- **Spec:** §4, §5, §7, §8, §12, §13
- **Historical evidence:** `2026-07-23-architecture-review-and-trino-integration-boundary.md`; `2026-08-18-registry-owned-function-optimization.md`

## Context

Nitro must execute semantics supplied by a host without coupling operators or evaluators to Trino classes, specific
functions, SQL types, arities, or provider classloaders. Physical carriers are shared by semantically different logical
types, so deriving semantics from vectors is incorrect.

## Decision

Logical semantics come from dynamically supplied, classloader-neutral type and function bindings. Operators and the
evaluator consume general batch, scalar-target, state, writer, hashing, comparison, and physical-kernel capabilities.
They do not recognize function or type identities. Generated adapters implement those conventions without duplicating
provider semantics.

## Consequences

- Trino-defined, parametric, and connector-defined semantics can participate.
- Registry composition and capability binding carry more responsibility.
- Fast paths are reusable physical capabilities rather than engine special cases.
- Missing bindings fail at planning/binding instead of selecting a hidden builtin substitute.

## Alternatives considered

**Reimplement common functions in core.** This duplicates semantics and grows permanent function-specific knowledge.

**Infer semantics from JVM/vector carriers.** Incorrect for shared carriers such as long and binary.

**Call every host scalar row by row.** General but forfeits encoding-aware processing, generation, and direct output.
