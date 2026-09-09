# ADR-0096: Normalize mixed flat DISTINCT keys into generated fixed-width layouts

- **Status:** accepted
- **Date:** 2026-09-09
- **Spec:** §5, §12, §16, §18, §19
- **Depends on:** [ADR-0089](0089-generate-fixed-width-key-kernels-from-provider-layouts.md),
  [ADR-0090](0090-reject-row-wise-semantic-key-tables.md),
  [ADR-0091](0091-generate-canonical-fixed-width-key-projections.md),
  [ADR-0092](0092-compose-persistent-key-layouts-in-one-generated-table.md)
- **Evidence:** ClickBench q10/q11 ordered A-B-A at Nitro `6f5f81a2` and Cork `a727bfa5`, retained under
  `~/notes/nitro/remote-benchmark-results/in-progress/q10-q11-normalized-fixed-width-{a1,b,a2}-6f5f81a2-20260909T032110Z`

## Context

ClickBench q11 spends its dominant Nitro excess in source-side DISTINCT over a mixed flat key. The established flat
table keeps exact variable-width values and can reuse direct composite state, but its scalar hash/probe path remained
about 1.34x the frozen Cork CPU control. Prioritizing the existing tiled generated driver saved a little CPU while
worsening latency. A prototype that staged two packed `long[]` lanes demonstrated a larger win, but violated the
contract against pre-materialized canonical key vectors and added one complete gather/write pass.

The generated fixed-width table already binds concrete primitive arrays and wrapper mappings once per batch, emits
constant-linked reversible projections, and keeps table lanes in locals through hash and probe. Mixed flat DISTINCT
needs an exact bridge into that existing physical contract without interpreting logical types, adding a query-shaped
table, or restoring a row-wise semantic fallback.

## Decision

DISTINCT normalizes an eligible mixed flat key into one generated fixed-width table by default. Every specified
direct field must carry its provider's explicit proof that raw physical equality is logical key identity; Nitro does
not infer that proof from a carrier, vector, or flat-handler kind. Every admitted direct I64 carrier remains a
full-width lane. Direct I32 carriers and exact query-stable I32 value IDs for admitted binary fields are physical
sources; adjacent I32 sources may be packed bijectively into one I64 table lane by a constant-linked projection. The
planner accepts arbitrary admitted LONG/BINARY field counts and orderings whose resulting lane count fits the shared
generated table. Logical type identity, field meaning, and query identity do not select the implementation.

The binary interner owns exact query-lifetime value identity across flat and dictionary generations. It materializes
only its reusable per-field position-to-ID source, not packed canonical table lanes. Batch binding resolves the
concrete primitive sources and wrapper mappings once; generated hash, probe, and insert code loads them directly and
keeps packed lanes in locals. Ordinary DISTINCT filters null-containing rows before binding.

Admission is exact and irreversible after table mutation. Carrier changes, unsupported wrappers, exhausted binary-ID
domains, or excessive generated lane counts reject before the table is mutated. They do not promote into the old flat
table, an object table, or a row-wise provider bridge. Shapes that cannot select this table initially retain another
already-authorized direct/generated physical implementation or remain explicit unsupported coverage.

## Consequences

- ClickBench q11's two enabled placements averaged 0.883x wall and 0.868x CPU versus the disabled placement. The q10
  guard averaged 0.946x wall and 0.949x CPU. Both performed identical physical input/output work: bytes and
  positions matched.
- Allocation increased 4.3% on q11 and 5.5% on q10. This is an accepted measured cost of the current reusable binary-ID
  sources, not evidence for restoring staged packed lanes; reducing it remains an implementation optimization.
- The generated table and projection cache grow by physical carrier/layout shape rather than by logical type or key
  combination.
- The property `nitro.distinct.normalizedFixedWidth=false` remains a diagnostic opt-out for controlled comparisons,
  not a semantic fallback.

## Alternatives considered

**Retain the scalar flat table.** It is exact and allocates less, but the ordered A-B-A measured 13.2% more q11 CPU
and 11.7% more wall time than the generated layout.

**Stage packed `long[]` lanes before probing.** The prototype established the target representation, but added a full
materialization pass and row-proportional scratch contrary to ADR-0091.

**Specialize the q11 three-field shape.** It could preserve the immediate win with less planner work, but would add a
query/arity/carrier combination beside the shared physical-layout generator.

**Invoke generic value accessors or projection handles from the probe loop.** This avoids source preparation but
retains the megamorphic row-wise bridge rejected by ADR-0089 through ADR-0091.
