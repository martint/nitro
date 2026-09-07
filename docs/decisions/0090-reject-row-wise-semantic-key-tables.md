# ADR-0090: Reject row-wise semantic key tables

- **Status:** accepted
- **Date:** 2026-09-08
- **Spec:** §2, §8, §12, §13, §16, §19
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md),
  [ADR-0029](0029-make-coverage-gaps-explicit.md),
  [ADR-0036](0036-share-physical-key-contracts-across-consumers.md),
  [ADR-0039](0039-keep-one-authoritative-production-path.md)
- **Amends:** [ADR-0086](0086-prove-raw-key-identity-through-type-providers.md),
  [ADR-0089](0089-generate-fixed-width-key-kernels-from-provider-layouts.md)
- **Evidence:** `~/notes/nitro/p1/2026-09-08-engine-coverage-q85-canonical-long-key-prototype/README.md`;
  TPC-DS q67 evidence summarized in ADR-0086

## Context

Grouping, distinct, hash join, and semi-join membership retained exact object-table implementations for logical keys
without a provider-proven physical identity. Those implementations bound provider semantic hashing and equality, but
invoked them for individual rows and retained row-shaped key objects. They preserved correctness while violating the
generated calling convention and created a performance cliff inside an already admitted Nitro island.

TPC-DS q67 showed the broader class of failure: routing physical keys through row-wise semantic handles raised warmed
CPU from about 42.7 seconds to about 106 seconds and allocation from about 17.9 GB to 331 GB. Engine Coverage q85 then
isolated a structural long-decimal key. The retained semantic path took 35.714 seconds per q85x8 point; the production
provider-layout implementation takes 23.186 seconds, 0.649x wall and 0.648x CPU. Exactness alone is therefore not a
sufficient admission criterion for persistent key tables.

ADR-0086 required semantic key operations when raw identity was absent. ADR-0089 narrowed that rule only after a
fixed-width layout had already been selected, while explicitly retaining semantic execution for undeclared types.
That policy conflicts with the project rule that unsupported execution is a visible coverage gap rather than a slow
bridge inside an admitted island.

## Decision

Persistent Nitro key consumers require a provider-neutral physical key implementation. A key is eligible when every
logical component can use an existing direct physical table or the provider declares an exact generated layout that
the complete observed key shape can bind. Grouping, distinct, hash join, and semi-join membership must reject at the
planning or physical-binding boundary when satisfying the key would require row-wise semantic hashing/equality or
retained row-key objects.

The engine removes the structural object-table implementations and does not retain them as disabled alternatives.
Provider semantic identity and comparison remain valid for scalar functions, ordering, and other operations that do
not use those implementations as a persistent key-table bridge.

A missing provider layout, unsupported carrier or wrapper, nullable selected lane, excessive lane count, or mixed
key that cannot form one generated layout is a functionality gap. The host integration should reject it during
island admission when plan-time capabilities suffice; otherwise the engine rejects it at first physical binding with
a diagnostic. It must not continue through a semantic table.

## Consequences

- Performance cliffs from exact but row-wise structural key tables are removed from admitted Nitro execution.
- Arrays, maps, nullable records, floating-point identities, timestamps, and other undeclared key shapes lose Nitro
  grouping/join/distinct/membership coverage until they receive an efficient general implementation.
- Coverage inventories become temporarily smaller and accurately identify provider/layout work that remains.
- One provider layout continues to benefit grouping, distinct, hash join, and membership without type-specific
  operator code.
- General scalar structural equality and ordering are unchanged.

## Alternatives considered

**Retain the semantic tables as a correctness fallback.** This maximizes apparent coverage, but repeats the measured
performance cliff and makes an admitted Nitro query unpredictably worse than the host path.

**Choose the semantic table only below a row-count threshold.** This can bound one execution's cost but makes query
coverage data-dependent and preserves the losing implementation. A small-key implementation may be added later only
as an independently evidenced physical capability, not as a correctness escape hatch.

**Special-case known slow logical types.** This would fix long decimal and leave the same failure for every future
provider type. Admission must follow provider-neutral physical capabilities.
