# ADR-0085: Preserve RLE through row concatenation

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §5, §12.1
- **Depends on:** [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md), [ADR-0084](0084-prove-constant-window-bounds-across-sources.md)
- **Evidence:** Engine Coverage q241 frame-bound allocation trace

## Context

Blocking operators may normalize several retained pages into one physical row domain for sorting and repeated access.
The generic RLE row-materialization implementation delegated every logical run position to the underlying flat vector.
A typed constant represented as one run per input page consequently became a flat value for every row. Consumers lost
the constant domain and repeated work and storage at logical cardinality.

Special-casing constant frame offsets in the window operator would leave the same encoding loss in every other
blocking row assembler. Coalescing equal values requires logical equality owned by a type provider and is unnecessary
to preserve the physical RLE domain.

## Decision

When every row segment being concatenated is RLE, the RLE vector implementation concatenates their run-count arrays
and materializes only their physical run-value domains. The resulting owned RLE vector preserves every run exactly;
it does not infer semantic equality or merge adjacent runs. Mixed representations retain the existing general dense
fallback.

Consumers may inspect the compact run domain using their already-bound logical semantics. A positional-frame adapter,
for example, can prove that all long-carried bound runs contain the same valid offset without scanning logical rows.
Physical null/error proofs likewise inspect the complete Boolean run-value domain; preserving several all-false runs
must not turn a known null-free stream back into a logical-row accessor.

## Consequences

- Row assembly allocates and copies in proportion to physical runs rather than logical rows for all-RLE inputs.
- Constant and low-run-count domains remain visible to downstream operators.
- Adjacent equal runs remain separate unless a type-owned capability later proves that merging is worthwhile.
- Run counts and the materialized physical value domain are allocator-owned and accounted.
- Mixed inputs remain correct through dense materialization.

On Engine Coverage q241 (five warmups and five measurements), preserving the frame-bound RLE domains and their
all-false NULLS/ERRORS proof reduced CPU from 0.329 to 0.277 seconds, execution latency from 0.285 to 0.222 seconds,
and allocation from 1.224 GB to 1.043 GB. An intermediate implementation that preserved RLE but recognized only a
single all-false run regressed CPU to 5.738 seconds; the multi-run null proof is therefore part of the composition
contract rather than an optional consumer optimization.

## Alternatives considered

**Flatten every RLE input.** Simple, but discards encoded-domain execution and creates row-proportional storage.

**Merge adjacent equal runs generically.** Rejected because physical carriers do not define logical equality.

**Recognize constant window columns in the operator.** Rejected because it makes an operator compensate for a general
vector-composition defect.
