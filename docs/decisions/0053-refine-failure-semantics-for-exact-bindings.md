# ADR-0053: Refine failure semantics for exact registry bindings

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §8, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0037](0037-link-generated-scalar-targets-as-constants.md), [ADR-0038](0038-keep-function-optimization-at-the-registry-boundary.md), [ADR-0052](0052-gate-scalar-adaptation-on-carrier-materialization.md)

## Context

Host function catalogs may conservatively classify a function family as fallible even when one selected physical
implementation cannot fail for any value admitted by its bound signature. Treating that family-level declaration as
the final property of every binding prevents otherwise eligible adjacent scalar calls from sharing one generated
loop. Short-decimal subtraction followed by absolute value consequently materialized an intermediate vector even
though both exact implementations use primitive carriers and are infallible.

The registry owns the selected implementation and already publishes its calling-convention capabilities. Nitro must
not infer a stronger property from a function name, query data, or the surrounding expression.

## Decision

A registry binding may refine conservative host failure metadata when it owns the exact selected implementation and
can prove that implementation infallible for the complete bound input domain. The refined semantics travel with the
immutable resolved call and apply equally to its batch and scalar capabilities.

A provider-owned batch implementation may expose its exact scalar target as an additive capability. The generic
compiler may compose adjacent eligible targets into one loop using only resolved capabilities and semantics. It does
not recognize the function or reproduce its value semantics.

Operations whose exact implementations can fail remain fallible. They are not admitted to an infallible fused slice.

## Consequences

- Infallible primitive-carrier calls can avoid intermediate vectors while retaining their authoritative batch
  implementation as the general fallback.
- Conservative family-level metadata does not permanently disable safe composition for exact implementations.
- Correctness depends on a proof over the entire bound domain, so bindings need direct tests for both refined and
  retained failure classifications.
- Adding a scalar capability does not guarantee admission; carrier materialization and other physical eligibility
  rules still apply.

## Alternatives considered

**Always preserve host family-level failure metadata.** Safe but prevents composition even when the selected
implementation is demonstrably infallible.

**Let the evaluator recognize known infallible functions.** This duplicates function semantics in the engine and
violates registry ownership.

**Infer infallibility from sampled or constrained query data.** This is not valid for the complete function domain and
would make correctness depend on optimization assumptions.
