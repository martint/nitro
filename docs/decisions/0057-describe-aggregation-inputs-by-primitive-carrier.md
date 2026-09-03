# ADR-0057: Describe generated aggregation inputs by primitive carrier

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §8, §12.1, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0055](0055-link-generated-aggregation-targets-as-constants.md), [ADR-0056](0056-allow-generated-aggregation-updates-to-consume-tuples.md)

## Context

The first tuple convention distinguished a long input contribution from a double input contribution with separate
descriptor classes. Extending that pattern to Boolean and future primitive carriers would grow a parallel descriptor
hierarchy. It would also encourage names such as I64 to be read as logical types even though the same JVM `long`
carrier represents BIGINT, short DECIMAL, REAL bits, dates, intervals, and other provider-defined semantics.

The generated loop needs the carrier for two purposes only: bind the corresponding primitive vector storage and emit
the exact invocation descriptor. Neither task requires knowledge of the logical type or function.

## Decision

One input-contribution descriptor carries an explicit `PrimitiveContributionCarrier`. The initial carrier set is
`LONG`, `DOUBLE`, and `BOOLEAN`, named after their JVM invocation types rather than Nitro or SQL types. Convenience
factories may select a carrier, but they lower to the same descriptor.

The physical binder validates that the runtime vector supplies storage compatible with the declared carrier. The
generated kernel specializes its array cast, load instruction, and constant-linked invocation descriptor from that
carrier. A tuple may mix carriers in any order. Adding a future primitive carrier extends the carrier enumeration and
the two generic lowering switches; it does not add function-specific interfaces or tuple shapes.

Constants remain long-carried until a provider demonstrates a need for another constant carrier. This is a current
capability boundary, not a claim that constants are logically BIGINT values.

## Consequences

- Boolean and mixed Boolean/numeric aggregate updates use the same generated mechanism as existing numeric updates.
- JVM representation and logical meaning remain separate; the registry provider owns interpretation.
- The API grows linearly with admitted primitive carriers rather than combinatorially with carrier tuples.
- Physical compatibility is checked once while binding a batch; the row loop performs typed primitive loads only.
- The convention remains classloader-neutral and creates no per-row carrier objects.

## Alternatives considered

**Add `BooleanInputValue` beside `InputValue` and `DoubleInputValue`.** Small initially, but repeats the descriptor
hierarchy problem for every future carrier and obscures that these are JVM calling-convention choices.

**Represent every primitive as long bits.** Uniform descriptors, but forces conversions inside providers, prevents
exact Boolean and double call sites, and weakens the JVM's type information at the generated boundary.

**Pass boxed objects or an argument array.** Handles arbitrary carriers but reintroduces allocation and indirect loads
in the dominant row loop.
