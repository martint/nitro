# Structural null-mask alias

Date: 2026-07-24

Null tests no longer implement `MaskEvaluablePrimitiveFunction`.
`DirectMaskInputProvider` already declares their complete mask semantics: the
boolean result is exactly the argument's physical null component. The
evaluator now applies that alias to both physical inputs and internal variable
producers. Flat Boolean components compact the caller-owned mask in place;
other representations use the generic Boolean mask classifier.

The typed scalar classes retain only ordinary materialized-value execution and
registry metadata. The obsolete `IsNullMaskSupport` executable helper and its
static diagnostic property are removed. The evaluator contains no null-test
function name, logical type, or arity knowledge.

All measurements use JDK 26, one unpinned JMH thread, 12 GB heap,
10x1s warmup, 5x1s measurement, allocation, and the fixed eight counters in
one invocation. The exact source parent is `9c1c2bf1`.

Three-fork candidate versus reverse parent:

| Query | Duration | Allocation | Instructions | Cycles | L1D loads | Branches |
|---|---:|---:|---:|---:|---:|---:|
| q30 | -0.25% | +1.35% | -0.43% | -1.15% | -0.59% | -0.31% |
| q76 | -0.21% | -0.50% | +0.01% | -0.11% | -0.96% | -0.10% |
| q81 | -0.71% | -3.11% | -0.38% | -0.08% | -0.26% | -0.34% |

Q30 allocation was escalated to five fresh forks. Its allocation modes remain
wide and overlapping; the candidate mean is +1.06%. Duration is +0.23%, while
every work counter improves: instructions -0.34%, cycles -0.53%, L1D
misses/loads -1.02%/-0.34%, dTLB misses/loads -3.00%/-5.12%, branch
misses -1.74%, and branches -0.29%. Alongside q76/q81 allocation improvements,
the cohort qualifies, with q30 allocation retained explicitly as adverse.

Artifacts:

- `candidate-q30-q76-q81-3fork.json`
- `parent-q30-q76-q81-reverse-3fork.json`
- `candidate-q30-5fork.json`
- `parent-q30-reverse-5fork.json`

This is an architecture qualification; the published board is unchanged.
