# Provider-derived UTF-8 equality — rejected

Date: 2026-07-25

This attempted to move `eq_utf8` mask execution from
`MaskEvaluablePrimitiveFunction` to its existing provider-authored
`utf8Equal` projection IR. `ProjectionMaskCompiler` admitted the physical
shape and returned an opaque backend supporting allocating and in-place
true/false masks. Structural errors remained evaluator-owned.

The attempt also moved `Utf8BinaryDispatch` from the builtin-function package
to `jit`, allowing ordinary scalar execution and the compiled backend to share
the existing flat/dictionary/RLE kernels without a JIT-to-function dependency.
A focused test found and repaired a semantic fallback hole: when the
dictionary allocating specialization declined a flat shape, the compiled
backend had to copy the domain and invoke its general in-place kernel rather
than materialize a scalar result that omitted argument errors.

Focused compiler, registry, architecture, scalar, and nullable/error evaluator
tests passed. Performance did not.

All comparisons use JDK 26, one unpinned JMH thread, a 12 GB heap, 10x1s
warmup, 5x1s measurement, allocation, and the fixed eight counters in one
invocation. The exact parent is `3fa5127b`.

Final three-fork candidate versus reverse parent:

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H q03 | -0.25% | +0.01% | +0.62% | +0.06% | +0.55% / +0.75% | -0.27% / +2.49% | +1.12% / +0.46% |
| TPC-H q16 | +1.45% | -0.01% | +1.87% | +1.53% | +0.20% / +2.16% | +0.28% / -0.69% | +0.19% / +1.63% |

The two-query geometric duration delta is +0.60%. Q16 is adverse across
duration and general work counters, so the implementation is fully reverted.
The one-fork q03/q16/q21 screen is retained only as secondary evidence; it was
too noisy to override the three-fork rejection.

No function-name/type branch was added to the evaluator. The next attempt
should not relocate the entire UTF runtime or wrap its old dispatch surface.
It should compile provider `utf8Equal` IR directly to an engine-owned
Classfile-API mask kernel and bind only classloader-neutral constants/arguments.
That isolates generated mask shape from unrelated UTF scalar class layout and
removes the extra fallback/dispatch layers rather than preserving them.

Primary artifacts:

- `candidate-q03-q16-3fork.json`
- `parent-q03-q16-reverse-3fork.json`
- `candidate-screen.json`
- `parent-reverse-screen.json`

The published board is unchanged.
