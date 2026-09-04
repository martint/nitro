# ADR-0083: Match generated-kernel physical shapes exactly

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §12, §16
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)
- **Evidence:** repeated TPC-DS q14a execution with changing aggregation input layouts

## Context

Generated aggregation kernels specialize loads, row mappings, offsets, carriers, and null handling to the bound
physical vectors. The grouping operator summarized these properties with a folded 32-bit value and reused its
current kernel when that value was unchanged. Distinct physical layouts can produce the same folded value. A repeated
q14a execution exposed such a collision: code generated for a mapped input was invoked with a later binding whose
optional mapping array was empty.

Always resolving through the exact generator cache would restore correctness, but constructing its composite cache
key on every batch would add steady-state allocation. Treating a wider hash as proof would only make the correctness
failure less likely.

## Decision

Reusable generated bindings expose an exact structural shape descriptor and an allocation-free comparison with the
currently bound vectors. The descriptor includes every property that changes generated code. The operator compares
the current bindings with its captured descriptor on every batch. On a mismatch it asks the generator for the exact
kernel and captures a new immutable descriptor; otherwise it reuses the current kernel directly.

Execution-strategy flags that are independent of vector bindings use a collision-free bit set. Hashes may remain as
cache indexes, but equality of complete structural keys is authoritative for both cache lookup and current-kernel
reuse.

## Consequences

- A generated kernel cannot run against a merely hash-colliding physical layout.
- Stable batches compare compact primitive descriptors without allocating objects or arrays.
- Shape transitions allocate one bounded descriptor and perform one exact generator-cache lookup.
- Adding a generated load or representation property requires adding it to the structural descriptor and its tests.

## Alternatives considered

**Resolve through the generator on every batch.** Correct, but creates composite keys and collection wrappers in a
steady-state path.

**Use a 64-bit or stronger hash.** Reduces collision probability but still treats probability as a correctness
contract.

**Regenerate unconditionally.** Correct but discards caching and adds class-generation or lookup overhead unrelated
to actual physical transitions.
