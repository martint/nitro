# ADR-0037: Link generated scalar targets as class constants

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-09-02)
- **Spec:** §8, §16
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md), [ADR-0013](0013-use-layered-function-calling-conventions.md)
- **Historical evidence:** reconciliation entry for registry scalar targets and generated expression fusion

## Context

A method handle stored in each adapter instance remained opaque in cheap inner loops and measured several times slower
than a direct call. The registry must still supply implementations dynamically and retain classloader ownership, so a
source-level `invokestatic` reference is not generally available at Nitro compile time.

## Decision

Generate a class whose constant call site resolves the exact registry-supplied method handle once, then invoke it with
the exact descriptor in the loop. Linkage is immutable for that generated class/shape. Generation can compose several
eligible targets into one loop without exposing function identities to Nitro.

## Consequences

- C2 can treat the target as stable and inline through `invokeExact` when normal JVM conditions permit.
- Cache/class lifecycle must follow provider classloader lifetime.
- Dynamic rebinding creates a new generated class rather than mutating a hot instance field.
- Microbenchmarks validate cheap targets because expensive functions can hide linkage overhead.

## Alternatives considered

**Store MethodHandle in an instance field.** Straightforward and measurably blocks optimization for cheap kernels.

**Generate direct symbolic calls to provider classes.** Optimizable but violates classloader neutrality and is not
possible for arbitrary handles.

**Interpret a handle graph per row.** Fully dynamic and too expensive in the hot loop.
