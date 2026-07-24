# Rejected mask input-shape capability

Date: 2026-07-24

This rejected experiment tried to move the remaining declarative mask input
requirements out of `is_null_i32`/`is_null_i64`. A classloader-neutral
`MaskInputShapeProvider` declared argument count, required physical components,
and whether absent companion components must be materialized. The already
separate `IsNullDirectMaskOptimization` supplied both direct-alias and input-
shape metadata.

The abstraction itself was clean, but the implementation resolved the
capability and translated its component enum to evaluator stream sets inside
`PlanEvaluator.resolveMaskPrimitiveInvocation`, once per evaluated batch.
That failed the performance gate. The code was reverted completely.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the fixed eight-event `perfnorm` bundle in the same invocation.
The exact parent is `1bbcdd6d`.

The initial q30/q40/q81 three-fork candidate versus reverse parent was mixed:

| Query | Duration | Allocation | Instructions | Cycles | L1D loads | Branches |
|---|---:|---:|---:|---:|---:|---:|
| q30 | +1.06% | -0.24% | +0.19% | +1.64% | -0.36% | +0.14% |
| q40 | +0.36% | -0.10% | -0.47% | -0.32% | +0.12% | -0.40% |
| q81 | -0.88% | -0.98% | -1.35% | -1.92% | -1.39% | -0.96% |

Because q30 was the adverse direct-filter case, it was escalated to five fresh
forks. That rejected the implementation despite effectively flat wall time:

| Metric | Candidate vs parent |
|---|---:|
| duration | +0.17% |
| allocation | +2.01% |
| instructions | +1.06% |
| cycles | +1.32% |
| L1D misses | +1.54% |
| L1D loads | +1.36% |
| dTLB misses | +3.27% |
| dTLB loads | +13.69% |
| branch misses | +2.58% |
| branches | +0.77% |

The next design must resolve and bind mask access shape once while constructing
the evaluator (or lower it into an engine-owned executable plan). It must not
perform provider lookup, enum decoding, or semantic validation in the batch
path. This result does not justify retaining access-shape methods on scalar
execution objects permanently; it identifies the binding/lifetime requirement
for their replacement.

Artifacts:

- `candidate-q30-q40-q81-3fork.json`
- `parent-q30-q40-q81-reverse-3fork.json`
- `candidate-q30-5fork.json`
- `parent-q30-reverse-5fork.json`

No source from the experiment is retained, and the published board is
unchanged.
