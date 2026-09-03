# ADR-0063: Expose composed key binding to stateful registry functions

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §5, §8, §11
- **Depends on:** [ADR-0036](0036-share-physical-key-contracts-across-consumers.md), [ADR-0062](0062-expose-composed-type-identity-to-functions.md)
- **Historical evidence:** map-construction aggregation breadth slice

## Context

Some registry-owned stateful functions retain a set of logical keys across batches. Identity alone permits a linear
scan, but an efficient general implementation also needs the registered hash. Reimplementing hashes in each function
would duplicate type semantics; adapting keys to host flat records would cross the vector boundary and discard
structural encodings.

Nitro already composes provider-supplied hash and identity for grouping, joins, and distinct. The composed binder was
internal to operators even though its contract is useful to stateful function providers.

## Decision

Expose a classloader-neutral `TypeKeyBinder` from the structural kernel factory. A binder can bind independently
owned vectors; each resulting `BoundTypeKey` hashes positions and compares them with positions in another vector
bound by the same binder.

The contract supplies logical key semantics only. A consuming provider owns its table layout, allocation, growth,
payload retention, ordering, duplicate policy, and intermediate representation. Null participation remains a
consumer-level semantic decision and is not implicit in the bound key.

## Consequences

- Stateful functions can build indexes across allocator-owned vector segments without host carriers.
- Structural and future provider-defined types enter the same implementation through their type binding.
- Operators and registry functions share exact hash/identity semantics without sharing a table implementation.
- Binding is performed once per vector segment rather than rediscovered for each position.

## Alternatives considered

**Expose an engine hash table.** This would impose duplicate, grouping, payload, and growth policies on function
providers.

**Use identity with a linear scan.** Correct but creates a cardinality-dependent performance cliff.

**Require provider functions to supply another key implementation.** Duplicates the semantics already registered on
the logical type and risks disagreement with grouping, joins, and distinct.
