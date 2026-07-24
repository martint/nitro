# Provider-derived I64 masks

Date: 2026-07-24

`eq(I64,I64)` and `lt(I64,I64)` no longer implement
`MaskEvaluablePrimitiveFunction`. Their dynamically registered optimization
providers expose the same projection IR through `MaskCodeProvider`.
`ProjectionMaskCompiler` admits the physical I64 comparison shapes and returns
an opaque compiled mask program. `PlanEvaluator` sees only dynamic arguments,
required streams, structural exclusions, and that opaque program; it contains
no function identity, I64 operation, or fixed-arity branch.

The complete old mask surface migrated: outcome classification, allocating
true/false masks, and in-place true/false masks. Nulls remain inputs to the
physical comparison backend. Errors are structural exclusions resolved after
values and nulls, then promoted to the outcome error mask. This ordering is
both semantically complete and important for source behavior: an eager
VALUES/NULLS/ERRORS tuple caused q21 to resolve absent error components on
every invocation.

Encoding-specific pair traversal now lives in the physical data layer
(`I64BinaryDispatch`), not in a builtin function package. Both ordinary scalar
execution and the compiled-mask backend use that representation API. The
evaluator remains independent of physical integer encodings.

All comparative measurements use JDK 26, one unpinned JMH thread, a 12 GB
maximum heap, 10x1s warmup, 5x1s measurement, allocation, and the fixed eight
hardware counters in the same invocation. The exact parent is `3523c4a7`.

Final three-fork candidate versus reverse-order parent:

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H q12 | +0.76% | -0.33% | +0.14% | +0.68% | +0.15% / +1.31% | -0.50% / +0.21% | -0.35% / -0.03% |
| TPC-H q21 | +0.45% | -0.22% | +0.48% | +1.08% | +0.71% / -0.50% | -9.53% / -3.00% | +0.43% / +1.50% |

Q21 wall time is multimodal in both revisions. Candidate iterations span
3.738--5.173 seconds and parent iterations span 3.734--5.227 seconds, so the
small mean difference is not treated as a stable duration regression. The
small adverse work counters remain explicit architectural costs.

The diagnostic sequence established the main avoidable cost:

- eager error inputs: 63.125B instructions and 22.609B cycles in the initial
  three-fork q21 candidate;
- restoring encoding-specialized traversal alone: 62.856B instructions and
  23.215B cycles in one diagnostic fork;
- deferring absent error components: 62.004B instructions and 21.578B cycles
  in one diagnostic fork.

The final structurally correct shape lands at 62.383B instructions and 22.021B
cycles versus the parent's 62.086B and 21.785B.

Q04 is not comparative evidence. Both exact parent and candidate fail during
the first warmup with `released more resident memory than allocated` while
closing a filter/semi-join pipeline. The failure predates this slice and is
retained in both logs for the next allocator-accounting repair.

Primary artifacts:

- `candidate-final-q12-q21-3fork.json`
- `parent-q04-q12-q21-reverse-3fork.json`
- `diagnostic-specialized-q21-1fork.json`
- `diagnostic-no-eager-errors-q21-1fork.json`

This is an architecture qualification. The published 165-query performance
board is unchanged.
