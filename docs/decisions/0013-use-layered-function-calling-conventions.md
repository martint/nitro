# ADR-0013: Use layered Nitro-native function calling conventions

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-20)
- **Spec:** §8, §12
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0008](0008-use-first-class-masks-and-streams.md)
- **Historical evidence:** `2026-08-20-layered-function-calling-conventions.md`

## Context

One maximally general vector API burdens simple implementations, while many signature-specific interfaces create a
combinatorial surface. Nitro also needs host scalar functions, vector-aware implementations, parametric functions,
aggregations, and windows to share null, error, mask, encoding, allocation, and ownership rules.

## Decision

Define one general batch contract over requested streams, inputs, mask, proposed outputs, and allocator context. Define
convenience profiles by fixing or framework-managing dimensions of that contract. Generated scalar loops are ordinary
implementations of these conventions, not a separate function mechanism.

## Consequences

- Simple providers can opt into dense typed profiles while advanced providers control encodings and allocation.
- Convention selection occurs per resolved invocation and physical input shape.
- Null/error propagation can be framework-managed only when the provider explicitly supplies the required semantic
  proof.
- Aggregates and windows use related state/update/result profiles rather than unrelated operator hooks.

## Alternatives considered

**Only a general vector call.** Expressive but unnecessarily difficult for ordinary functions.

**An interface per JVM signature.** Easy locally and unbounded globally.

**Treat host scalar adaptation as exceptional.** This leaves two semantic paths and makes generated loops impossible to
reason about as function implementations.
