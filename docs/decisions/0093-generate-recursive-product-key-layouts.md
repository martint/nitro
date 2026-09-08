# ADR-0093: Generate recursive product key layouts

- **Status:** accepted
- **Date:** 2026-09-08
- **Spec:** §5, §8, §12, §13, §16, §19
- **Depends on:** [ADR-0022](0022-compose-recursive-structural-vectors.md),
  [ADR-0036](0036-share-physical-key-contracts-across-consumers.md),
  [ADR-0089](0089-generate-fixed-width-key-kernels-from-provider-layouts.md),
  [ADR-0090](0090-reject-row-wise-semantic-key-tables.md),
  [ADR-0091](0091-generate-canonical-fixed-width-key-projections.md),
  [ADR-0092](0092-compose-persistent-key-layouts-in-one-generated-table.md)
- **Evidence:** strict Engine Coverage q63 rejection in
  `/home/ubuntu/nitro-bench/results/functional-adr0092-logical-physical-from-q50-20260908T1109Z/results/engine-coverage/run.log`

## Context

ADR-0092 composes direct flat fields and canonical fixed-width lanes when each top-level logical key publishes one of
those physical identities. A finite structural product can contain both representations recursively. Engine Coverage
q63 groups `ROW(TIMESTAMP(9), VARCHAR)`: the timestamp contributes two primitive lanes and the character field
contributes variable-width storage. The complete row also owns a null boundary distinct from a non-null row whose
children are all null.

Restoring the removed structural object table would make coverage depend on row-wise provider dispatch and retained
semantic objects. Adding a generator for row, timestamp-plus-varchar, fixed-plus-variable, or each carrier ordering
would reproduce the combinatorial matrix rejected by ADR-0092. Treating structural children as independent SQL keys
without retaining every enclosing null boundary would be incorrect.

## Decision

A type provider may declare that a logical key is the ordered product of independently nullable child identities.
Each child supplies a structural field path and its authoritative `TypeBinding`; a child may recursively publish
another product. The engine resolves the finite tree into one provider-neutral persistent-key descriptor containing
product-presence boundaries, leaf paths, direct storage, canonical lanes and projections, wrapper mappings, null
sources, and representative-retention requirements.

One layout compiler generates hashing, record writes, and collision equality for the complete descriptor. It binds
concrete primitive and binary storage, mappings, and every nullable ancestor once per batch. Generated row loops load
those sources directly and skip descendants under a null product boundary. They do not invoke structural semantic
objects, generic field accessors, provider virtual methods, or row-wise method handles.

The descriptor and compiler are independent of logical type identity, field names' meaning, field count, nesting
depth, leaf carrier sequence, and query. Adding a new finite product shape requires no new table or generator class.
Grouping, DISTINCT, hash join, and membership share the resolved contract while retaining their consumer-specific
payload and null policies. A recursive value whose identity cannot be expressed as a finite product of supported leaf
identities rejects admission; arrays and maps therefore remain explicit gaps rather than entering a semantic bridge.

An encoded probe domain does not lose its amortization merely because the resolved layout requires batch binding. A
join binds the complete generated layout over an exactly aligned physical domain and performs the authoritative table
lookup once per domain position. The domain may be a shared dictionary mapping, optionally combined with single-run
RLE constants, or the one-entry domain of an all-RLE key. The logical-row loop maps the cached result through the
physical mapping. Nested product null rejection is evaluated while the domain layout is bound, so a cached rejected
position never requires access to cleared binding state. Reuse across batches additionally requires immutable domain
content with matching vector identities and content generations; otherwise the domain results are recomputed for the
batch. Unaligned fields, non-empty top-level null streams, large or weakly reused domains, and unsupported encodings
retain the ordinary generated probe path rather than weakening the mapping proof.

## Consequences

- Fixed-width, canonical, variable-width, and nested product fields compose in one generated persistent table.
- Every structural null boundary remains part of identity, including the distinction between a null product and a
  non-null product containing only null children.
- Providers explicitly authorize product composition; Nitro does not infer row semantics from a vector class.
- Generated shapes grow with the physical descriptor, not with a source-level matrix of logical or carrier cases.
- Product-valued grouped output retains one logical representative per new group until direct reconstruction has a
  separately proven implementation.
- Generated product joins retain encoded-domain probe amortization when every field proves one aligned mapping;
  batch binding is not a reason to perform the same authoritative lookup once per logical row.
- Variable-length collections remain unsupported persistent keys until they receive an efficient physical layout.

## Alternatives considered

**Restore the semantic structural table only for encoded domains.** Low-cardinality dictionaries can bound semantic
calls in favorable batches, but flat or changing batches would reintroduce the performance cliff and a second
production table contract.

**Flatten rows in the Trino adapter.** This could expose q63 as several top-level columns, but it would move key
semantics into the host boundary, duplicate null composition there, and fail to benefit other providers or consumers.

**Add binary leaves to `FixedWidthKeyLayout`.** That contract deliberately describes canonical fixed-width lanes and
their projection ABI. Overloading it with nullable recursive products would obscure product presence and
variable-width storage rather than describe them explicitly.
