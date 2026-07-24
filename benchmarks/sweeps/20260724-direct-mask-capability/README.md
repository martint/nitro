# Direct mask input capability

Date: 2026-07-24

This moves the structural `is_null` alias ("the result is argument 0's null
component") out of `MaskEvaluablePrimitiveFunction` and into immutable
resolved-function capability metadata. The capability is classloader-neutral:
it contains only argument indexes/count and a shared-SPI physical component
enum. It does not expose evaluator IR, vectors, mutable masks, streams, or an
execution context.

`PlanEvaluator` now obtains the alias from the `Call`'s capability bundle.
That works for both the legacy registry and a fully resolved call and removes
the former execution-time function-name lookup plus implementation-interface
test. The richer representation-aware mask kernels are intentionally unchanged;
their current mutable engine protocol is not suitable for promotion to the
function SPI.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the fixed eight-event `perfnorm` bundle in the same invocation.
The exact parent is `3005e3dd`.

The q30/q40/q81 three-fork candidate versus reverse exact-parent control is:

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q30 | +0.02% | +0.09% | -0.95% | -1.41% | -1.83% | -1.31% | -5.54% | -15.67% | -3.26% | -1.16% |
| q40 | +0.36% | +0.21% | -0.13% | +0.56% | -1.01% | +0.82% | +3.84% | -6.94% | +1.14% | +0.03% |
| q81 | -1.48% | -1.04% | -1.67% | -2.75% | -2.36% | -2.66% | -13.42% | -18.67% | -3.71% | -1.67% |

Q30 and q81 exercise direct non-null filtering. Q40 is a projection-only
negative control: it registers the additional capability but does not consult
it in filter control flow. Its small mixed movements bound the incidental
registry/class-shape noise.

Q97 was also captured twice around an exact-parent reverse leg. It remains
highly multimodal (individual iterations span roughly 1.08--2.20 seconds).
The first candidate leg improved every aggregate metric, while the reverse
candidate leg regressed duration/high-volume counters but retained lower
allocation and dTLB work. Those files are preserved as instability evidence
and are not used to claim a q97 speedup.

Artifacts:

- `candidate-q30-q40-q81-3fork.json`
- `parent-q30-q40-q81-reverse-3fork.json`
- `candidate-q97-3fork.json`
- `parent-q97-reverse-3fork.json`
- `candidate-q97-reverse-3fork.json`

This is an architecture qualification, not a published-board refresh.
