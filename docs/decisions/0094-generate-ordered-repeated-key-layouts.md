# ADR-0094: Generate ordered repeated key layouts

- **Status:** amended by [ADR-0095](0095-generate-unordered-repeated-key-layouts.md)
- **Date:** 2026-09-08
- **Spec:** §5, §13, §16, §19
- **Amends:** [ADR-0093](0093-generate-recursive-product-key-layouts.md)
- **Depends on:** [ADR-0089](0089-generate-fixed-width-key-kernels-from-provider-layouts.md),
  [ADR-0090](0090-reject-row-wise-semantic-key-tables.md),
  [ADR-0091](0091-generate-canonical-fixed-width-key-projections.md),
  [ADR-0092](0092-compose-persistent-key-layouts-in-one-generated-table.md),
  [ADR-0093](0093-generate-recursive-product-key-layouts.md)
- **Evidence:** Engine Coverage q68 and q137;
  `~/notes/nitro/p2/2026-08-27-engine-coverage-q68-q69-structural-keys/`;
  `TestFlatGroupingTable.testGeneratedOrderedRepeatedProductGroupingPreservesProductAndLeafNullBoundaries`

## Context

ADR-0093 deliberately left arrays and maps outside persistent key tables because a finite product cannot describe a
variable number of child identities. After removal of row-wise semantic key tables, Engine Coverage q68 therefore
rejects grouping and joining on `ARRAY(BIGINT)` even though the repeated-vector SPI exposes exact parent offsets and
aligned child streams.

Restoring the former recursive semantic kernel would restore correctness but violate ADR-0090: its element loop
dispatches through nested kernel objects and representation accessors for every element. Adding an array table would
instead encode a logical type and one child arity in the engine, and would repeat the mistake for nested arrays,
products, projected identities, and future provider-defined repeated structures.

Ordered repetition and unordered collection identity are different capabilities. Array equality is determined by
length and the identity of each child position in order. SQL map equality is independent of physical entry order, so
admitting a map through the ordered capability would make a storage convention part of logical semantics.

## Decision

A type provider may prove that one logical key is an **ordered repetition** of one or more aligned child identities.
Each repeated output is identified by its physical output ordinal and authoritative child binding. The children may
use direct physical identity, canonical fixed-width lanes, finite products, or ordered repetition recursively.

Persistent-key resolution composes repetition into the same provider-neutral descriptor used by direct, canonical,
and product fields. Batch binding resolves outer dictionary, region, and RLE mappings, parent offsets, aligned child
streams, nested mappings, null streams, primitive arrays, binary regions, and projection targets once. Generated
hash, record-write, and collision-equality methods loop directly over the resolved ranges. They contain no row
objects, per-element virtual or method-handle dispatch, `VectorAccess` calls, logical-type tests, or generator chosen
by child carrier combination.

The persistent record stores an exact generated physical identity for the repeated field in variable-width table
storage. Grouping retains the first logical representative whenever that identity is not sufficient to reconstruct
the provider value. Dictionary- and RLE-domain reuse remains available only after the same exact mapping and lifetime
proofs required by ADR-0093.

This capability is ordered. Providers must not publish it for maps or any identity whose equality ignores physical
child order. Such types remain explicit coverage gaps until they publish a distinct semantic layout with its own
generated implementation.

## Consequences

- Arrays and provider-defined ordered repeated structures can participate in grouping, DISTINCT, hash join, and
  membership through one generated persistent table.
- Nested arrays and arrays of products or canonical identities extend the descriptor rather than adding named table
  or generator classes.
- Unsupported child layouts, mismatched repeated-output counts, independently misaligned child streams, unsupported
  wrappers, or unordered semantics fail admission before table mutation.
- Repetition can increase record-arena traffic; encoded-domain amortization and representative retention must remain
  independently measured, and are not reasons to weaken the identity proof.
- Maps remain unsupported by this decision.

## Rejected alternatives

**Restore recursive semantic key kernels.** Exact, but retains the row-wise dispatch and allocation cliff rejected by
ADR-0090.

**Add an array-specific grouping or join table.** It handles one logical spelling and creates a combinatorial family
for nested child layouts and consumers.

**Treat every `RepeatedVector` as ordered identity.** Physical repetition is not a semantic proof; maps share that
carrier while their identity is unordered.

**Canonicalize repeated values into host objects or a temporary vector before probing.** This adds row-proportional
allocation and an extra materialization pass while hiding the physical mappings the generated loop needs.
