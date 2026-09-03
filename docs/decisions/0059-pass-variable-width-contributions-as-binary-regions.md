# ADR-0059: Pass variable-width generated aggregation contributions as binary regions

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §12.1
- **Depends on:** [ADR-0055](0055-link-generated-aggregation-targets-as-constants.md), [ADR-0057](0057-describe-aggregation-inputs-by-primitive-carrier.md), [ADR-0058](0058-select-structural-components-in-generated-aggregation-updates.md)

## Context

Some provider-owned aggregation inputs and portable intermediate states are variable-width byte sequences. Passing a
new slice object per logical row would add allocation and obscure the flat physical layout. Restricting generated
updates to one JVM primitive per contribution instead leaves these states on the ordinary per-position provider path.
Adding function- or logical-type-specific decoding to the grouping operator would violate registry ownership.

## Decision

A generated aggregation contribution declares an allocation-free physical carrier. Fixed-width carriers expand to
one JVM parameter. The binary-region carrier expands to the exact parameter sequence
`(byte[] data, int offset, int length)` while remaining one logical contribution.

The physical binder unwraps supported dictionary and region mappings once per batch and exposes the flat binary data
and offset arrays. Generated code maps the logical position, loads adjacent offsets, and invokes the provider's exact
constant-linked target. It does not allocate a slice object, copy bytes, decode a provider state, or assign meaning to
the bytes. Null handling continues to guard the complete contribution before invocation.

The carrier model is therefore named for physical contributions rather than primitives. A carrier may expand to a
fixed JVM parameter tuple when that tuple is the allocation-free representation of one value.

## Consequences

- Providers can generate exact updates over VARCHAR, VARBINARY, and serialized intermediate states without row-sized
  wrapper allocation or byte copying.
- Dictionary and region mappings remain visible to the physical binder and generated loop.
- Provider method handles stay exact and constant-linked; the provider owns parsing and update semantics.
- Every carrier has a fixed JVM parameter shape, so target validation and generated-kernel caching remain structural.
- Encoded-domain reduction for binary values still requires the same alignment and repeated-update proofs as other
  carriers.

## Alternatives considered

**Pass a slice object.** Simple provider API, but allocates or requires a mutable cursor contract and adds another
indirection to the hot loop.

**Pass `BinaryVector` and a logical position.** Avoids allocation, but moves encoding and region access back into each
provider invocation and couples providers to per-row vector dispatch.

**Decode known serialized states in the engine.** Can yield primitive targets, but makes operators understand
function-specific intermediate formats.

**Always normalize intermediate states to fixed-width structural vectors.** Useful within an island when a provider
offers it, but not a general replacement for portable variable-width states crossing host or exchange boundaries.
