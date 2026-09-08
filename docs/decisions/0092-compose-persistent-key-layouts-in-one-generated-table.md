# ADR-0092: Compose persistent key layouts in one generated table

- **Status:** accepted
- **Date:** 2026-09-08
- **Spec:** §5, §8, §12, §13, §16, §19
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md),
  [ADR-0036](0036-share-physical-key-contracts-across-consumers.md),
  [ADR-0083](0083-match-generated-kernel-shapes-exactly.md),
  [ADR-0086](0086-prove-raw-key-identity-through-type-providers.md),
  [ADR-0089](0089-generate-fixed-width-key-kernels-from-provider-layouts.md),
  [ADR-0090](0090-reject-row-wise-semantic-key-tables.md),
  [ADR-0091](0091-generate-canonical-fixed-width-key-projections.md)
- **Amends:** [ADR-0091](0091-generate-canonical-fixed-width-key-projections.md)
- **Evidence:** strict TPC-H q10 rejection in
  `/home/ubuntu/nitro-bench/results/functional-adr0091-from-q42-20260908T0958Z/results/tpch/run.log` and mixed-layout
  grouping coverage in `TestFlatGroupingTable.testGeneratedProjectedFlatLayoutComposesVariableWidthAndCanonicalFields`

## Context

ADR-0086 admits fields whose provider proves raw physical identity. ADR-0091 admits logical fields whose identity is
an ordered tuple of provider-projected canonical primitive lanes. A persistent SQL key can contain both capabilities.
TPC-H q10 has direct variable-width fields and a canonical floating-point field in one grouping key. Neither the
homogeneous flat table nor the homogeneous fixed-width table can represent that complete identity alone.

Choosing a table or generator from the logical type of each field would create a class for every type combination.
Choosing among hand-written pair, triple, long/binary, or projected/direct variants would create the same
combinatorial matrix at the physical level. Running canonical fields through row-wise semantic handles inside the
flat table would restore the bridge rejected by ADR-0090. Pre-materializing projected vectors would add a
row-proportional pass and temporary storage.

## Decision

Nitro resolves one provider-neutral persistent-key layout for an arbitrary ordered sequence of logical fields. A
field contributes either its provider-proven direct flat representation, including variable-width storage, or one or
more provider-declared canonical fixed-width lanes. The resolved descriptor records field order, input channels,
primitive sources, wrapper mappings, projection targets, storage geometry, and representative-retention needs. It
does not record or switch on logical type identity.

One flat table owns hash control, fixed and variable-width records, value interning, growth, group payload, and output
for the complete key. A hidden layout subclass is generated from the resolved descriptor. Its hash, record-write,
and equality methods load projected primitive sources from concrete arrays and invoke projection targets through
constant-linked call sites. Direct fields use the established flat record machinery. Projection interfaces,
`MethodHandle.invoke`, `VectorAccess` accessors, logical-type switches, and scratch canonical vectors do not appear
in the per-row path.

Canonical fields store only canonical identity in the table and retain the first logical input representative for
grouped output. Direct fields continue to materialize output from their flat records. Dictionary, region, and RLE
mappings are resolved per batch without changing the persistent layout or table class.

The generator is parameterized by the complete structural descriptor and cached by structural shape. Adding another
supported field representation extends descriptor resolution, binding, and code emission once; it does not add a
generator for every combination containing that representation. A field without a provider proof or without a
supported generated/storage representation rejects persistent-key admission before mutation. It never selects a
semantic object table or another correctness-first bridge.

## Consequences

- Mixed direct, variable-width, raw primitive, structural-source, and canonical-projected keys share one persistent
  table and one layout compiler.
- Logical types, queries, and field combinations do not select implementation classes.
- Generated code remains monomorphic for each exact physical shape without requiring source-level shape classes.
- Representative retention is proportional to new groups and only applies to logical fields whose canonical storage
  cannot reproduce the original representative.
- Consumers adopt the composed descriptor independently. A consumer that has not implemented a compatible generated
  table continues to reject the layout; it does not fall back to row-wise semantics.

## Alternatives considered

**Add a double-plus-varchar grouping table.** This would fix TPC-H q10 but immediately require variants for different
field orders, arities, canonical types, and direct storage kinds.

**Wrap the fixed-width table beside the flat table.** Two independent tables cannot atomically assign identity for
one composite key and would duplicate probe state, collision resolution, and group payload coordination.

**Pre-project canonical columns and use the ordinary flat table.** This keeps table code simple but adds a complete
projection pass and row-sized storage before every key consumer.

**Interpret a list of field accessors.** This accepts arbitrary descriptors but introduces virtual dispatch and
repeated shape checks in hash, write, and equality loops. The resolved descriptor should generate those loops once.
