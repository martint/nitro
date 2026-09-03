# ADR-0062: Expose composed type identity to registry-bound functions

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §5, §8, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0022](0022-compose-recursive-structural-vectors.md), [ADR-0036](0036-share-physical-key-contracts-across-consumers.md)
- **Historical evidence:** Engine Coverage q202 map-subscript slice

## Context

Registry-owned structural functions sometimes need a logical type operation over values already held in Nitro
vectors. Map subscript, for example, must compare a row-varying lookup key with map entries according to the key
type's exact identity semantics. Adapting every map to a host carrier introduces row-proportional object construction,
while reimplementing equality in each function duplicates type semantics and does not scale to structural keys.

Nitro already composes provider-supplied leaf identity recursively for grouping, joins, distinct, comparison, and
null-safe equality. That composed operation was package-private, so registry-bound functions could not reuse it.

## Decision

Expose a classloader-neutral `BoundTypeIdentity` operation from the structural kernel factory. It compares positions
in any vector representations admitted by one `TypeBinding`, with explicit null streams. Leaf semantics remain in the
type provider; recursive physical traversal remains in Nitro; the consuming operation's semantics remain in its
function provider.

The contract exposes capability, not logical identity. A provider may still select a narrower physical path when the
binding proves raw identity, such as sign-extended long equality. Consumers must retain composed identity as the
semantic path for every admitted key type.

## Consequences

- Structural lookup and future registry functions reuse the same exact identity as grouping, joins, and distinct.
- No host `Block`, `SqlMap`, or row-local carrier is required inside a Nitro island.
- Adding a key type extends lookup through its type binding rather than through a function-specific type switch.
- The public operation is deliberately identity-only; hashing, storage, and table policy are not imposed on functions
  that only scan a bounded structural value.

## Alternatives considered

**Invoke the host map function per row.** Semantically direct, but constructs composite host carriers and crosses the
Page/Block boundary inside the island.

**Implement key equality in the map function.** Fast for a few primitive types and creates an unsustainable duplicate
type catalog.

**Expose the complete internal structural-key kernel.** More capability than lookup needs and unnecessarily couples
function implementations to hashing and table policy.
