# ADR-0097: Admit exact DISTINCT value IDs by observed reuse

- **Status:** accepted
- **Date:** 2026-09-09
- **Spec:** §12, §16, §18, §19
- **Depends on:** [ADR-0086](0086-prove-raw-key-identity-through-type-providers.md),
  [ADR-0090](0090-reject-row-wise-semantic-key-tables.md),
  [ADR-0096](0096-normalize-mixed-flat-distinct-keys.md)
- **Amends:** [ADR-0096](0096-normalize-mixed-flat-distinct-keys.md)
- **Evidence:** ClickBench q13 production-gate failure at Nitro `a911d085`; q11/q13 diagnostic at Nitro working-copy
  `fda8f4539ce2` and Cork `a727bfa5`, retained under
  `~/notes/nitro/remote-benchmark-results/in-progress/q11-q13-distinct-admission-diagnostic-20260909T0412Z`

## Context

ADR-0096 normalized every eligible mixed LONG/BINARY DISTINCT layout into the generated fixed-width table. It reused
the flat grouping interner, whose 65,536-value ceiling is an adaptive cost cutoff: an ordinary flat record can retain
exact bytes after the cutoff. The normalized table has no direct-byte record tier after mutation, so the same cutoff
became an unintended functionality limit. The production ClickBench sweep passed q00--q12 and then failed q13 when
`SearchPhrase` crossed it.

Raising the interner ceiling for every admitted layout restored q13 correctness, but retained a query-wide copy of
its high-cardinality phrases. q13 then allocated 14.25 GB and peaked at 1.79 GB, roughly 10% above its established
direct flat path, without a material wall-time gain. q11 benefits because `MobilePhoneModel` is reused; q13 does not
because its initial `SearchPhrase` domain is nearly unique. This distinction is physical and observable without
recognizing either query or logical type.

## Decision

Generated normalized DISTINCT requires a bounded initial sample of every binary source to demonstrate reuse. A field
is admitted when the percentage of distinct sampled physical values does not exceed the flat layout's configured
binary-reuse threshold. Sampling uses the same wrapper-aware binary hash and bounded evenly spaced positions as the
existing physical layout admission. Logical type identity, column name, query identity, and field combination do not
participate.

A high-cardinality initial domain selects the already-authorized direct flat physical table before any persistent
table mutation. This is initial physical-policy selection, not a runtime fallback or semantic object bridge.

Once normalized DISTINCT is selected, its query-stable interner is exact over the complete positive I32 ID domain.
The adaptive grouping value-ID ceiling does not apply. Later batches may grow beyond the sampled cardinality or the
grouping cutoff without changing tables or losing exactness. True I32-domain exhaustion remains an unsupported
transition and fails before the generated table consumes the affected batch.

## Consequences

- A 65,537-distinct-value regression test proves that an admitted normalized layout can grow beyond the grouping
  cutoff without fallback.
- A high-cardinality initial-domain test proves that admission declines before mutation.
- In the ordered q11/q13 diagnostic, q11 retained the generated layout and measured 734 ms wall, 3,617 ms CPU, and
  2.893 GB allocated. q13 selected the direct flat layout and measured 3,034 ms wall, 19,949 ms CPU, 13.192 GB
  allocated, and 1.585 GB peak memory.
- Against the established production q13 row, the refined policy was +0.9% wall, -3.5% CPU, about +2.0% allocation,
  and -2.3% peak memory. The indiscriminate exact-interner prototype's roughly 10% allocation/peak penalty is not
  retained.
- The sample is a cost admission only. Exactness never depends on its prediction because every admitted layout owns
  the full I32 ID domain.

## Alternatives considered

**Apply the 65,536 grouping cutoff and fail on overflow.** Rejected because a heuristic cost threshold is not a
physical identity limit and made an accepted benchmark query unsupported.

**Promote to the direct flat table after overflow.** Rejected because persistent table identity cannot change after
mutation without an explicit generated migration, and a silent slow fallback violates ADR-0090.

**Use exact full-domain interning for every binary shape.** Correct and simple, but the q13 diagnostic increased
allocation and peak memory by roughly 10% when the binary values had little reuse.

**Recognize `SearchPhrase`, q13, or a named logical type.** Rejected because admission is a physical-layout policy.
