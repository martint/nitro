# Prebound direct-mask capability

Date: 2026-07-24

`PlanEvaluator` now resolves `DirectMaskInputProvider` metadata once during
construction and stores an immutable `Variable -> Reference` binding. The
batch path no longer repeats assignment lookup, capability scan, arity/index
validation, component decoding, or `Reference` construction.

The binding remains general and classloader-neutral. Providers publish only
argument/component metadata; the evaluator owns its IR reference, and only a
physical input can be delegated to the source mask resolver. No static cache
or service is introduced.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the fixed eight-event counter bundle in the same invocation.
The exact source parent is `1bbcdd6d`.

The initial three-fork q30/q40/q81 candidate versus reverse parent:

| Query | Duration | Allocation | Instructions | Cycles | L1D loads | Branches |
|---|---:|---:|---:|---:|---:|---:|
| q30 | -0.47% | -2.95% | +0.18% | -0.87% | +0.15% | +0.38% |
| q40 | -0.16% | -0.00% | +0.34% | +0.59% | -0.06% | +0.73% |
| q81 | -0.41% | -1.01% | +0.24% | +1.16% | +0.77% | -0.72% |

Q40 is a projection-only control and does not consume the direct binding.
Q81's adverse three-fork cycle estimate was escalated to five fresh forks:

| Metric | Candidate vs parent |
|---|---:|
| duration | -2.60% |
| allocation | +0.62% |
| instructions | -0.31% |
| cycles | -1.19% |
| L1D misses | -0.34% |
| L1D loads | -0.53% |
| dTLB misses | -24.74% |
| dTLB loads | -1.23% |
| branch misses | +1.06% |
| branches | +0.38% |

The retained result establishes the correct capability lifetime: immutable
metadata is decoded at evaluator construction, while execution consumes an
engine-owned binding. Small adverse allocation/branch estimates in the q81
escalation remain explicit.

Artifacts:

- `candidate-q30-q40-q81-3fork.json`
- `parent-q30-q40-q81-reverse-3fork.json`
- `candidate-q81-5fork.json`
- `parent-q81-reverse-5fork.json`

This is an architecture qualification; the published board is unchanged.
