# ADR-0067: Bind higher-order functions as provider-owned nested plans

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §7, §8
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0008](0008-use-first-class-masks-and-streams.md), [ADR-0013](0013-use-layered-function-calling-conventions.md), [ADR-0022](0022-compose-recursive-structural-vectors.md), [ADR-0040](0040-use-one-normalized-mask-aware-expression-ir.md), [ADR-0066](0066-close-evaluator-local-function-state.md)

## Context

A higher-order call contains executable expressions rather than only runtime values. Passing a host lambda object to a
row loop would hide masks, encodings, error streams, and allocator ownership. Adding `transform`, `filter`, or other
function names to the evaluator would instead move registry-owned semantics into the engine.

A vertical prototype demonstrated array transformation, array filtering, captured parent-row values, conditional
error isolation, and recursive array results through one mechanism. This supplies the evidence required to choose a
general binding model.

## Decision

A higher-order function provider identifies container arguments, captures, and nested expressions while binding a
resolved host call. The host compiler lowers every nested expression through the ordinary Nitro expression compiler,
binding lambda parameters to an explicit nested-domain input schema. The provider receives the resulting immutable
plans and creates a registry-bound batch function that owns the higher-order result semantics.

At execution, that function derives a nested mask from the active parent mask, maps captures to nested positions,
evaluates the nested plan, and constructs or reduces the result using allocator-owned storage. Its nested evaluator is
mutable execution-local state owned and closed by the containing evaluator. No core evaluator operation or operator
recognizes a higher-order function identity.

## Consequences

- Nested expressions retain ordinary conditional evaluation, error streams, computation reuse, registry resolution,
  and encoded-vector behavior.
- Inactive, null, and errored parent rows are absent from the nested mask and cannot produce observable failures.
- Providers can define transformation, selection, reduction, or reconstruction semantics without new evaluator cases.
- Captures require an explicit parent-to-entry mapping; independently encoded captures are not assumed to share the
  container's mapping.
- Container-specific result construction remains provider code until repeated evidence justifies a smaller reusable
  nested-domain framework capability.
- Every newly supported higher-order shape requires recursive type, null/error, sparse-mask, and Parquet-backed engine
  coverage.

## Alternatives considered

**Represent lambdas as host objects passed to scalar functions.** This forfeits vector-domain evaluation and obscures
ownership and failure boundaries.

**Add higher-order operations to the core evaluation IR.** This makes a function-neutral evaluator responsible for
container and function semantics.

**Reimplement each lambda body inside its function adapter.** This duplicates the expression engine and cannot reuse
its mask, encoding, or generated-kernel machinery.

**Introduce a universal nested-domain runtime abstraction immediately.** The proven functions share mapping mechanics
but differ in result semantics. Keeping that code provider-owned preserves a clear boundary until broader evidence
identifies a genuinely reusable contract.
