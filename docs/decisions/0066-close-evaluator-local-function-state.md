# ADR-0066: Close evaluator-local function state with its evaluator

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §8, §11
- **Depends on:** [ADR-0005](0005-inject-all-execution-dependencies.md), [ADR-0013](0013-use-layered-function-calling-conventions.md)

## Context

Registry bindings are immutable, while some function implementations need mutable, reusable execution state. The
primitive execution context already scopes that state to one evaluator, but it did not have a lifecycle hook. State
which owns a nested evaluator, generated resource, or other closeable object would therefore outlive the evaluator's
last batch unless each caller invented a separate cleanup path.

## Decision

The primitive execution context owns all state created through its state registry. When the containing evaluator
closes, the context closes every state object which implements `AutoCloseable`, then drops all state references.
Resolved function bindings remain immutable and never own this mutable state.

## Consequences

- Batch-reusable function state has one deterministic lifetime.
- Nested expression evaluators and future provider resources can use the ordinary function context without static
  registries or operator-specific cleanup hooks.
- A state object which owns a resource must implement `AutoCloseable`; non-closeable reusable value objects require no
  additional ceremony.
- Cleanup failures are reported after every closeable state has been given a close attempt.

## Alternatives considered

**Let each function register an operator close callback.** This couples function state to operators and duplicates
lifecycle wiring.

**Store mutable state in the resolved binding.** This violates immutability, thread confinement, and classloader-safe
registry ownership.

**Recreate state for every batch.** This avoids cleanup but defeats steady-state reuse and allocation goals.
