# Provider-derived F64 masks

Date: 2026-07-24

The five F64 comparison scalars no longer implement
`MaskEvaluablePrimitiveFunction`. Their dynamically registered optimization
providers mark the existing projection program as mask-compilable. The engine
compiler validates the provider-authored physical IR and returns an opaque
compiled mask program. `PlanEvaluator` prebinds its dynamic arguments and
null/error component references; it contains no function identity, F64,
comparison-operation, or fixed-arity vocabulary.

Null and error semantics are structural. Values are compared first, then
provider/compiler-declared companion components are excluded. Physical input
components are read through the source boundary after values have resolved, so
a source may return an already-resident component without a second reader or a
temporary mask. Computed arguments fall back to ordinary component evaluation.

All retained measurements use JDK 26, one unpinned JMH thread, a 12 GB maximum
heap, 10x1s warmup, 5x1s measurement, allocation, and the fixed eight counters
in one invocation. The exact source parent is `45839a35`.

Final five-fork q06 candidate versus reverse parent:

| Metric | Delta |
|---|---:|
| Duration | +0.66% |
| Allocation | +0.21% |
| Instructions | +0.25% |
| Cycles | +0.51% |
| L1D misses / loads | +0.19% / +0.26% |
| dTLB misses / loads | +0.30% / +2.31% |
| Branch misses / branches | -0.08% / +0.37% |

The small general-work increases are retained explicitly as adverse. They buy
correct nullable comparison semantics that the old values-only hook did not
exercise, and remove function-owned executable mask behavior. The low-volume
dTLB-load estimate remains noisy (roughly 0.96--0.99 million/op).

Rejected intermediate shapes are preserved:

- eager companion materialization raised allocation 5.20%;
- direct source-null mask subtraction raised allocation 1.58% and duration
  1.05%;
- a compiled-program record containing another functional-interface evaluator
  regressed five-fork duration/cycles 0.87%/0.95% and dTLB loads 2.69%;
- prebinding component references and using one abstract compiled-program
  dispatch recovered most of that overhead.

Primary retained artifacts:

- `candidate-q06-5fork-final.json`
- `parent-q06-reverse-5fork.json`

This is an architecture qualification; the published 165-query board is
unchanged.
