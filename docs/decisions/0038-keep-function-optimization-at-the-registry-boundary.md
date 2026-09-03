# ADR-0038: Keep function-specific optimization at the registry boundary

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-18)
- **Spec:** §7, §8, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md)
- **Historical evidence:** `2026-08-18-registry-owned-function-optimization.md`

## Context

Optimizations for LIKE, regular expressions, URL extraction, casts, or other known functions can be valuable, but
placing their semantics in the evaluator makes Nitro a second function catalog. Conversely, insisting that every
function use only a scalar target prevents vector/encoding-aware implementations.

## Decision

The resolved registry binding may provide a specialized batch implementation or general physical kernel for the whole
function. Nitro supplies masks, encodings, generated harnesses, output storage, and dispatch. It never substitutes a
different implementation by recognizing a function name or expression pattern.

## Consequences

- Function authors can add vector, dictionary, RLE, SIMD, or stateful implementations without editing the engine.
- Nitro-generated scalar adapters remain the universal compatibility floor.
- A host implementation can delegate to existing scalar semantics while later adding a batch convention.
- Query-specific rewrites remain prohibited even when they mimic a common function.

## Alternatives considered

**Builtin Nitro replacements for popular functions.** Can be fast and risks semantic drift and engine coupling.

**Optimize only the call harness.** Safe and leaves function-internal vector opportunities inaccessible.

**Require every host function to be rewritten for Nitro.** Maximizes control and makes adoption impractical.
