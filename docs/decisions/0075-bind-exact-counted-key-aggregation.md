# ADR-0075: Bind exact counted-key aggregation through type identity

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §8, §11, §12, §16
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0060](0060-generate-aligned-encoded-domain-aggregation.md), [ADR-0063](0063-expose-composed-key-binding-to-stateful-functions.md)
- **Evidence:** Engine Coverage q201/q231 and generic histogram state tests

## Context

The first exact histogram provider stored keys in a primitive `long` array and was consequently bound only for
`BIGINT`. The aggregation semantics are not specific to that carrier: the function counts values using the input
logical type's equality and hash semantics, emits `MAP(K, BIGINT)`, and must preserve a portable intermediate across
partial and final aggregation.

Adding one state and binding per physical or logical key type would duplicate table, grouping, merge, and
materialization code. Teaching the aggregation operator about histogram or supported types would instead move
function and logical-type semantics into the engine.

## Decision

An exact counted-key aggregation binds the input type's composed key capability at function resolution. Its
provider-owned state uses that opaque capability to hash and compare positions across allocator-owned retained vector
segments. Group ids, hashes, counts, segment ids, source positions, table slots, and temporary position lists use
allocator-owned primitive vectors; retained keys use the registered type's vector construction and copy contracts.

Raw logical-row input, exact dictionary frequencies, and aligned grouped-domain multiplicities update the same state.
Encoded-domain paths are admitted only with the mapping and null-domain proofs required by ADR-0060. The provider,
not the engine, declares that applying a count is equivalent to repeated updates.

Partial and final stages exchange the ordinary portable `MAP(K, BIGINT)` value. Merge binds key identity over the
map's recursive key vector and materialization reconstructs keys through the registered vector factory. The operator
and evaluator do not recognize the function, key type, carrier, or map layout.

## Consequences

- One implementation covers fixed-width, variable-width, and structural keys supported by the type registry.
- Exact logical hashing and equality remain provider-owned even when several logical types share a physical carrier.
- State may retain several compact owned key segments rather than one primitive key array; segment count follows the
  batches that introduce new distinct keys, not input row count.
- Carrier-specific state remains permissible only as a measured provider-internal tier behind the same generic
  binding, never as an operator or evaluator special case.
- Unsupported key construction or identity fails function admission explicitly.

## Alternatives considered

**Add one implementation per carrier or logical type.** This preserves simple primitive tables but creates a growing
matrix of duplicate state and binding code and confuses physical carriers with logical identity.

**Materialize host objects as retained keys.** Generic but allocation-heavy, hostile to recursive values, and outside
the Nitro allocator's ownership and accounting model.

**Teach the aggregation operator about histogram.** This violates dynamic provider ownership and makes engine
behavior depend on a specific function.
