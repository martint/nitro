# ADR-0073: Bind exact normalized ordering keys through the type provider

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §5, §8, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0011](0011-generate-specializations-from-physical-capabilities.md), [ADR-0070](0070-make-unordered-placement-explicit.md)
- **Evidence:** Engine Coverage q229 and its CPU profile

## Context

An unpartitioned Top-N ranking with a two-row limit still compares every input row against its retained boundary.
Provider-bound structural comparison preserves exact logical semantics, but an opaque method-handle comparison plus
repeated vector and stream dispatch cost 4.07 CPU-seconds over 100 million rows after the retained state itself had
been bounded. Inferring signed or floating-point order from an I64, I32, or F64 carrier would be faster but
incorrect: the same carrier can represent logical domains with different comparison semantics.

Some logical domains have a complete non-null ordering that a provider can normalize losslessly into 64 bits. That
fact is stronger than fixed-width storage or raw identity and therefore needs its own explicit capability.

## Decision

A type binding may expose an order-key binder. For a supported physical vector, the binder resolves access once per
batch and returns a position-to-key operation. Unsigned comparison of two keys must agree with the provider's exact
logical comparison, and logical peers must produce identical keys. Null placement remains outside the key.

The capability is optional. Consumers use provider-backed structural comparison whenever the logical domain does not
fit losslessly in a 64-bit order key, the current physical representation is unsupported, or the batch contains
nullable ordering values that the specialized consumer does not handle. Operators never infer the capability from a
logical type name, carrier class, or vector class.

The unpartitioned Top-N ranking state uses the bound key for its single-key, null-free admission loop and retains the
ordinary structural path for all other shapes.

## Consequences

- Hot ordering loops can resolve representation access once per batch without abandoning registry-owned semantics.
- Providers can opt in independently for each logical domain and admitted vector representation.
- Composite, variable-width, nullable, and wider-than-64-bit orderings retain the general comparison path.
- The same capability can be evaluated later for TopN, Sort, ordered merge, and Window without adding type-specific
  operator branches.

## Alternatives considered

**Infer ordering from the physical vector class.** Fast but wrong for carriers shared by BIGINT, decimal, temporal,
floating-point, and provider-defined logical types.

**Keep invoking the opaque comparison per row.** Correct and general, but the q229 profile showed linkage and
representation dispatch dominating a boundary comparison repeated 100 million times.

**Recognize BIGINT in the ranking operator.** Solves one query while violating registry ownership and creating a
type-specific operator path.
