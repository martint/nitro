# ADR-0052: Gate scalar adaptation on carrier materialization

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §8, §11, §16
- **Depends on:** [ADR-0013](0013-use-layered-function-calling-conventions.md), [ADR-0037](0037-link-generated-scalar-targets-as-constants.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)

## Context

The generic scalar adapter can invoke an exact registry target for a reference carrier through a provider-owned
`(Vector, position) -> carrier` reader. That proves semantic interoperability, but not an efficient physical calling
convention. Trino long decimals are stored by Nitro as two primitive limbs while Trino's scalar ABI accepts an
`Int128` object. Constructing that carrier for every row allocated about 8.08 GB and used 3.014 CPU-seconds over 250
million rows. The existing two-limb batch implementation used 46 MB and 1.623 CPU-seconds on the same query.

Linking the reader and target as one constant method-handle graph did not enable scalar replacement: allocation and
CPU remained unchanged. The limitation is therefore an observable property of this carrier boundary, not a reason
for Nitro to recognize decimal semantics.

## Decision

Generated scalar admission requires both an exact semantic target and an allocation-appropriate carrier path.
Providers must not expose a reference value reader solely to make a composite physical value eligible when doing so
materializes one host object per logical row.

Such functions retain a provider-owned physical batch implementation over their native vector children. Primitive
or already-efficient reference carriers continue to use the generic generated adapter. A future structural calling
convention may replace the batch implementation only after end-to-end evidence shows that it preserves semantics,
physical encodings, and the steady-state allocation target.

## Consequences

- Short DECIMAL-to-DOUBLE uses the generic generated scalar adapter; long DECIMAL-to-DOUBLE retains a two-limb batch
  implementation.
- Logical function semantics remain registry-owned, while the provider selects the efficient physical convention.
- Coverage includes both decimal representations so a broad scalar-adapter migration cannot silently reintroduce
  per-row carrier allocation.
- Not every exact stack-carrier function is automatically a good generated-adapter candidate.

## Alternatives considered

**Admit every exact reference carrier.** This is semantically general but violated the allocation and CPU objectives
for composite carriers.

**Teach the generator about `Int128` or decimals.** This would couple Nitro to a host type and function family.

**Rely on HotSpot scalar replacement.** The measured constant-call-site composition did not eliminate the objects;
eligibility cannot depend on an optimization the production shape does not receive.
