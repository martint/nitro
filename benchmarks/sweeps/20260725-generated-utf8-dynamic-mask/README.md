# Generated dynamic UTF-8 equality masks

Date: 2026-07-25

This slice completes the mask migration for `eq_utf8`. Its registry capability
now authors `utf8Equal(argument(0), argument(1))` plus
`isNull(0) OR isNull(1)` for every two-argument physical shape, not only an
input/literal call. `ProjectionMaskCompiler` lowers that program to an opaque
engine executable. `EqualUtf8` no longer implements
`MaskEvaluablePrimitiveFunction` and owns no true/false mask hooks.

The Classfile API backend emits raw-buffer mask loops for flat/flat,
dictionary/dictionary, both mixed one-level dictionary shapes, and selected
nested-dictionary pairs. It fuses the provider-authored null expression into
those loops. Other compatible shapes use a general binary-region fallback;
errors remain generic structural exclusions applied by the evaluator. No
evaluator branch names `eq_utf8`, UTF-8, a fixed arity, or a provider class.

The stateless generated physical template is owned by the
construction-injected engine compiler resource and is shared across
short-lived compiled programs. Program bindings and all mutable state remain
instance-owned. No static pool, cache, or service was added.

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

## Rejected variants and retained lifetime repair

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
- A generated selected-position traversal was also rejected. Against its
  immediately following restored-current control it was +9.54% duration,
  +5.17% instructions, +22.88% cycles, +15.26% L1D misses, +6.31% L1D loads,
  +7.45% branch misses, and +4.36% branches; allocation was effectively flat.
  The candidate/control dTLB readings occupied incompatible page-placement
  regimes (40.586M/120.866M versus 1.020M/31.726M misses/loads) and are retained
  but not interpreted as a source effect.
- The selected traversal performed less logical mapping work, but it lived in
  a fresh hidden class owned by each compiled program. The operator harness
  reconstructs the graph per invocation, while the old general Java accessor
  loop shares compilation across those instances. Generated-code lifetime and
  compilation amortization therefore precede another nested-loop attempt.
- Sharing the existing kernel through a construction-injected compiler resource
  fixed that lifetime but did not qualify alone. Its three-fork cohort was
  +0.55% geometrically and +1.75% by summed duration, with q64 at +3.37%
  duration, +1.46% instructions, and +2.52% cycles.
- A post-qualification cleanup that added a recurring terminal-state check to
  every compilation and hoisted nested depths was rejected. Q64 moved to
  +1.30% duration, +0.88% instructions, and +1.26% cycles; cohort geometric
  duration remained -0.16%, but summed duration became +0.48%. The exact
  qualified hot source was restored. The enclosing
  `OperatorCodeGenerationResources` close boundary remains terminal and is
  directly tested.

The retained implementation combines the two necessary pieces: one stateless
generated template per explicit `EngineResources` compiler lifetime, plus
selected-position traversal driven only by runtime dictionary depth and null
encoding. The generated class accumulates one compilation profile across
short-lived evaluators, while per-program bindings remain isolated.

Against exact parent `0bd7f402` in the candidate-then-parent three-fork pair:

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| q19 | -1.18% | -0.42% | -0.10% | -0.93% | -0.35% / +0.25% | -0.48% / -0.79% | -0.45% / -0.16% |
| q24 | -0.01% | +0.12% | -0.18% | -0.28% | -0.25% / -0.35% | -0.67% / -4.55% | -1.60% / -0.20% |
| q46 | -0.18% | -0.03% | -0.13% | +0.14% | +0.07% / +0.24% | +1.23% / -10.90% | -0.88% / +0.00% |
| q64 | -0.86% | -0.01% | -0.24% | -2.19% | -2.08% / -0.89% | +22.40% / +53.30% | -1.44% / -0.38% |
| q68 | -0.11% | +0.01% | -0.13% | -0.17% | +0.26% / +0.01% | -0.84% / +1.46% | -1.13% / -0.15% |

Geometric duration improves 0.47%; summed duration improves 0.54%
(3896.695 versus 3917.681 ms). Q64's relative TLB increase remains below
Velox in absolute terms: 0.689M/8.707M misses/loads versus Velox's published
0.934M/33.765M.

Direct q64 operator and Trino-SQL parity, compiled-q64 parity, the focused
architecture gate, and the complete 1,358-test gate pass with zero
failures/errors and 566 expected skips. No JFR artifact was produced. The
frozen board remains the pre-architecture baseline and is not refreshed from
this focused cohort.

Primary evidence:

- `candidate-generated-fused-nulls-3fork.json`
- `parent-reverse-3fork.json`
- `parent-q24-failure.log`
- `candidate-q64-selected-nested-screen.json`
- `current-q64-reverse-screen.json`
- `compiler-owned-kernel-candidate-3fork.json`
- `compiler-owned-kernel-parent-reverse-3fork.json`
- `compiler-owned-nested-candidate-3fork.json`
- `compiler-owned-nested-final-candidate-3fork.json` (rejected cleanup)
