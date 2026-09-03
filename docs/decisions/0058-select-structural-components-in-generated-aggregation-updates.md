# ADR-0058: Let providers select structural components for generated aggregation updates

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §12.1
- **Depends on:** [ADR-0022](0022-compose-recursive-structural-vectors.md), [ADR-0056](0056-allow-generated-aggregation-updates-to-consume-tuples.md), [ADR-0057](0057-describe-aggregation-inputs-by-primitive-carrier.md)

## Context

Some logical values have a provider-owned structural physical representation. A two-limb integer is one example, but
the same issue applies to timestamps with auxiliary components and future composite carriers. The generated grouped
update convention could consume multiple primitive arguments, but every contribution previously selected only the
top-level VALUES vector. Supporting each structural representation in the engine would couple operators to logical
types and create one lowering path per representation.

## Decision

An input contribution may carry an immutable path of named structural fields in addition to its function argument and
primitive carrier. The empty path retains direct-value behavior. The registry provider declares both the path and the
exact update target, so it owns the relationship between its logical value and physical components.

Before binding a generated batch, the grouping operator recursively projects the path with the general structural
vector API. That API preserves outer dictionary, RLE, and region mappings. The primitive binder then applies its
existing representation checks and the generated row loop remains unchanged. The engine does not recognize field
names, logical types, arities, or function identities.

Logical argument nullness continues to guard the whole contribution tuple. A structural component is a physical part
of that argument rather than an independently nullable SQL field. If a selected component contains an additional
nontrivial null stream, the exact path is declined and execution falls back to the provider's general implementation.

## Consequences

- One provider can expose any fixed-width structural value as an arbitrary tuple of primitive contributions.
- Multiple contributions may select different paths from the same logical argument without materializing carrier
  objects or row tuples.
- Direct, dictionary, RLE, and region shapes retain their encodings when structural projection supports them.
- Nested paths work without extending the operator for each nesting depth.
- Invalid layouts or independently nullable components fail exact-path admission rather than weakening semantics.

## Alternatives considered

**Add long-decimal or timestamp cases to the grouping operator.** Direct initially, but violates registry ownership of
logical semantics and scales with every composite physical representation.

**Require providers to materialize one flat primitive input per component.** Keeps the grouping loop simple but adds
an avoidable projection, storage, and ownership boundary before aggregation.

**Pass the structural vector to the update target.** General, but moves representation dispatch and virtual access
back into the per-row function call and prevents an exact primitive invocation descriptor.
