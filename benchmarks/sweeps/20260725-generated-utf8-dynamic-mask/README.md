# Generated dynamic UTF-8 equality masks

Date: 2026-07-25

This slice completes the mask migration for `eq_utf8`. Its registry capability
now authors `utf8Equal(argument(0), argument(1))` plus
`isNull(0) OR isNull(1)` for every two-argument physical shape, not only an
input/literal call. `ProjectionMaskCompiler` lowers that program to an opaque
engine executable. `EqualUtf8` no longer implements
`MaskEvaluablePrimitiveFunction` and owns no true/false mask hooks.

The Classfile API backend emits raw-buffer mask loops for flat/flat,
dictionary/dictionary, and both mixed one-level dictionary shapes. It fuses the
provider-authored null expression into those loops. Nested/RLE-compatible
shapes use a general binary-region fallback; errors remain generic structural
exclusions applied by the evaluator. No evaluator branch names `eq_utf8`,
UTF-8, a fixed arity, or a provider class.

Mutable kernel state is owned by the constructed compiled program. No static
pool, cache, or service was added.

## Correctness

TPC-DS q24 exposed a pre-existing allocator bug: constructing an
"uninitialized sparse" mask with `selectedCount == size` violated `Mask`'s
dense-selection invariant. `Allocator.allocateUninitializedSparseMask` now
returns an all mask for that legitimate case and preserves pooling/accounting.
The exact parent fails q24 with `allSelected must match selectedCount`; the
candidate completes it.

The full suite passes 1,356 tests with zero failures/errors and 566 skipped.
No JFR artifact was produced.

## Qualification

Candidate and exact parent `f8cfbc98` used JDK 26, one unpinned JMH thread,
`-Xmx12g`, 10x1s warmup, 5x1s measurement, three forks, allocation, and the
fixed eight counters in each benchmark invocation. The parent was rebuilt from
a clean `target/` and run in reverse order.

The exact parent cannot provide a q24 duration/counter row because it fails.
The candidate's valid three-fork q24 row is 772.283 ms, 146.787 MB/op, 14.305B
instructions, and 4.229B cycles. No parent ratio is published.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q19 | +0.04% | -0.01% | +0.42% | +1.19% | +2.92% / +0.43% | -2.29% / +5.00% | +2.74% / +0.74% |
| TPC-DS q46 | -0.60% | +0.02% | -0.18% | +0.27% | +2.64% / +0.86% | -0.50% / +9.43% | +0.34% / -0.03% |
| TPC-DS q64 | +1.03% | -0.00% | +0.45% | +1.45% | -0.54% / +1.03% | +3.61% / -0.17% | +1.17% / +0.10% |
| TPC-DS q68 | +0.38% | +0.09% | -0.20% | -0.87% | -3.47% / -0.42% | -1.40% / -6.71% | -0.15% / -0.43% |

For the four comparable queries, candidate/parent duration is +0.21%
geometrically and +0.62% by sum (3,240.274 ms versus 3,220.182 ms). This is
retained as an architectural migration and correctness repair, not as a
performance-board promotion. The frozen 165-query board is unchanged.

## Rejected variants and next target

- A generated scalar byte comparator behind a Java row loop regressed all four
  comparable durations geometrically by 0.64%.
- Replacing that byte loop with a direct `Arrays.mismatch` call helped but left
  per-row mode/interface dispatch.
- Moving mask iteration and encoding mapping into four generated monomorphic
  entry points reduced work but remained adverse until the provider-authored
  null expression was fused.
- TPC-DS q64 uses value dictionaries of depths 4 and 3 and dictionary-encoded
  null streams, so it still takes the general selected-row fallback.
- Composing whole-batch nested mappings/null arrays was rejected: q64 regressed
  to +4.32% duration and +5.02% cycles versus parent.

The next general optimization is a generated selected-position nested
dictionary traversal that follows the actual dictionary depths without
materializing full mappings. It must be driven by runtime encoding shape, not
query/table/column identity.

Primary evidence:

- `candidate-generated-fused-nulls-3fork.json`
- `parent-reverse-3fork.json`
- `parent-q24-failure.log`

