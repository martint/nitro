# ADR-0071: Bind specialization constants before generated scalar adaptation

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §8, §15, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0013](0013-use-layered-function-calling-conventions.md), [ADR-0014](0014-stream-function-results-into-owned-vectors.md), [ADR-0037](0037-link-generated-scalar-targets-as-constants.md), [ADR-0052](0052-gate-scalar-adaptation-on-carrier-materialization.md)
- **Evidence:** Engine Coverage q227 and the generic `json_extract_scalar(JSON, JsonPath)` admission analysis

## Context

Some host scalar functions specialize a runtime implementation from a constant argument whose carrier is meaningful
only to the host. A compiled JSON path is one example, but the architectural shape also covers compiled patterns and
other provider-owned specialization objects. Treating such a carrier as a Nitro vector type would expose host
semantics to the engine and pay a per-row argument that the exact target has already specialized.

The same class of host functions may return a null reference for non-null arguments. Nitro's generated reference
result path previously assumed that every invoked target produced a value acceptable to the provider-owned writer.
Collecting nullable host results before conversion would restore row-proportional allocation and a second write pass.

## Decision

A call-aware provider binding may consume an exact, non-null immutable constant whose logical type is not admitted as
a Nitro runtime type. The binding obtains the exact host target, binds each such constant before generated adaptation,
and exposes only the remaining Nitro-vector arguments in the runtime signature. The bound constant's semantic value
participates in the resolved function identity so two differently specialized calls cannot alias in one evaluator.
Unsupported non-constant arguments reject admission.

Generated strict scalar adapters support nullable reference results. The generated loop writes a null return into the
ordinary NULLS stream and does not pass it to the provider-owned result writer. NULLS-only demand invokes the target
and discards the unpublished value storage. Nullable primitive results remain unsupported until a boxed or explicit
null-result convention is defined.

Exact catalog bindings retain precedence over the generic call-aware fallback. Neither Nitro core nor its evaluator
recognizes a concrete host function, specialization carrier, or logical result type.

## Consequences

- Parametric host scalars can use Nitro's generated masked batch loop without making planning-only carriers into
  runtime vectors.
- Dynamic specialization remains provider-owned and constant-linked, so the generated row loop has no per-row
  specialization dispatch.
- Nullable reference results retain direct allocator-owned output and first-class null propagation.
- Providers must construct collision-free identities for differently bound constants.
- Nullable primitive-return functions still reject generic adaptation rather than silently boxing every row.

## Alternatives considered

**Register every specialization carrier as a Nitro type.** This adds meaningless runtime vectors and couples Nitro to
host-only planning objects.

**Recognize JSON paths, patterns, or individual functions in the evaluator.** This violates registry ownership and
does not generalize to independently loaded providers.

**Collect nullable reference results in a host object array.** This preserves semantics but adds allocation and a
second output pass proportional to the number of rows.

**Pass null to the provider result writer.** This conflates the NULLS stream with value materialization and forces
every writer to interpret a carrier sentinel.
