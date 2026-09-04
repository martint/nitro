# ADR-0070: Make unordered-value placement explicit in ordering capabilities

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §5, §8
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0062](0062-expose-composed-type-identity-to-functions.md)
- **Evidence:** Engine Coverage q225 and the `array_max` admission analysis

## Context

Nitro exposed one composed ordering for a bound logical type. That was sufficient for ascending sort and
`array_min`, whose Trino contracts use unordered-last comparison. It is not sufficient for `array_max`, which uses
unordered-first comparison. The distinction is observable for floating-point NaN and recursively for null or
unordered values inside arrays and rows.

Reversing comparison results does not preserve the requested semantics: it reverses all ordered values as well as
unordered placement. Detecting NaN or a concrete logical type in a function would move registry-owned semantics into
provider-independent code.

## Decision

Ordering capabilities name unordered-first or unordered-last placement. A type provider may supply distinct carrier
or direct-vector comparisons for each convention. Nitro's structural kernel factory composes the requested convention
through nested arrays and rows, including the matching nested-null placement, and rejects a missing convention rather
than silently substituting another one.

The existing no-argument comparison binding remains the unordered-last convention for compatibility. New consumers
whose semantics require unordered-first must request it explicitly.

## Consequences

- Generic extrema, ordering, and future registry functions preserve exact floating-point and nested semantics without
  recognizing logical types.
- Providers for types without unordered values may intentionally publish the same implementation for both
  conventions.
- A type that supplies only one convention is admitted only for consumers that request that convention.
- The type capability surface grows by an ordering dimension, but the distinction is explicit at plan binding rather
  than paid for through a row-loop branch.

## Alternatives considered

**Use one total order for every consumer.** Incorrect for functions whose host contract requests the other unordered
placement.

**Reverse the existing comparison.** Reverses ordered values too and therefore does not implement maximum selection.

**Special-case floating-point values in each function.** Violates registry ownership and fails to compose for nested
values or future provider-defined unordered domains.

**Invoke host Block comparison per element.** Preserves semantics but reintroduces host materialization and dispatch
inside Nitro islands.
