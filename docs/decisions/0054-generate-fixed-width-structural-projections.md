# ADR-0054: Generate fixed-width structural projections without carrier-specific engine types

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §5, §8, §16
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md), [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0022](0022-compose-recursive-structural-vectors.md), [ADR-0052](0052-gate-scalar-adaptation-on-carrier-materialization.md)

## Context

Some logical values use several fixed-width vector children while the host scalar ABI uses one reference object. Long
decimal arithmetic is one example: reconstructing a host `Int128` for each row defeated scalar adaptation, while a
provider-owned two-limb loop could not compose with the following function and therefore materialized an intermediate
vector. Adding a signed-128 carrier to Nitro would solve that case but would start a carrier/type/arity hierarchy in
the engine.

The first generated two-limb experiment exposed boxed mask iteration in the downstream flat decimal accumulator and
raised q186 allocation to about 4.04 GB. A later generated class shape happened to let HotSpot scalar-replace that
boxing, but a mechanically equivalent generalized shape made it return. This was not a stable optimization contract.

Separately, generated projection handled only top-level dictionary inputs even though the evaluator already knew how
to peel row-aligned structural children sharing one mapping. Multi-component generation must not introduce a second,
weaker encoded-domain path.

## Decision

The provider-facing physical projection builder supports a fixed-width `STRUCT` value composed from named `I64`,
`F64`, or `BOOLEAN` components at arbitrary arity. Providers construct and read those components and own their names,
layout meaning, logical semantics, guards, and null behavior. The generated engine keeps components in primitive
locals across adjacent program steps and constructs an allocator-owned `StructVector` only for a published result.

Nitro does not define carrier-specific structural types such as signed-128 or decimal. A provider may decline any
layout and retain its ordinary batch implementation.

Physical-program projection uses the evaluator's existing dictionary-peeling operation. That operation recognizes a
row-aligned structure through children with one proven mapping, evaluates the generated program over the shared
domain, and restores the mapping and frequency metadata on every output stream.

## Consequences

- Multi-component functions can participate in expression-slice fusion without per-row host objects or one interface
  per carrier signature.
- One mechanism handles two limbs, timestamps with zones, intervals, UUID-like layouts, and future fixed-width records;
  providers decide which of those meanings apply.
- Variable-width or recursively nested components remain on the general batch convention until evidence justifies a
  broader generated contract.
- Structural dictionary preservation is tested with mixed primitive components, not a decimal-specific class or name.
- Generated and interpreted evaluation share one encoded-domain correctness proof and ownership implementation.

On Engine Coverage q186's flat structural input, long-decimal subtraction followed by absolute value and sum uses
1.749 CPU-seconds and 1.269 seconds latency after warmup, versus 2.419 and 1.744 seconds for the object-free two-pass
batch path. Allocation falls from about 84 MB to 48.1 MB. The unchanged Cork control uses 3.238 CPU-seconds and 2.342
seconds latency. A separate mixed-component structural dictionary test covers encoded-domain preservation.

## Alternatives considered

**Add one physical value type per composite carrier.** Direct, but couples the engine to specific widths and creates
the same combinatorial API problem as carrier-signature interfaces.

**Keep provider-owned batch loops only.** Avoids host objects but cannot keep components in locals across adjacent
function calls.

**Depend on HotSpot scalar replacement for host carriers or mask iterators.** Observed behavior changed with unrelated
generated-class shape. Row-proportional allocation must be removed structurally, not assumed away by one compilation.

**Implement separate structural dictionary handling in the projection operator.** Duplicates mask, companion-stream,
frequency, and ownership proofs already owned by the evaluator.
