# Explicit engine resources: focused performance control

This directory validates the architectural conversion from ambient
`PrimitiveArrayPool.shared()` singletons to explicitly constructed and injected
`EngineResources`. It is a focused same-host control, not a replacement for the
published 165-query board.

## Method

- JDK `/opt/java/openjdk` (26)
- Nitro operator harnesses, unpinned, one JMH thread and one fork
- 12 GiB heap with transparent huge pages
- 5 × 1 s warmup and 5 × 1 s measurement
- `gc` and Linux `perfnorm` profilers
- counters captured in the same invocation: instructions, cycles, L1D
  misses/loads, dTLB misses/loads, branch misses, and branches
- candidate first, then a freshly built parent in reverse order

## Results

Positive deltas mean the explicit-resource candidate used more than the
reverse-order parent.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ClickBench q43 | -0.99% | -0.05% | +1.03% | -0.38% | +0.68% | +0.66% | -12.09% | -24.61% | -2.73% | +0.83% |
| TPC-DS q64 | +0.49% | +0.19% | +0.45% | +0.57% | +1.32% | -0.12% | +1.23% | +1.29% | +0.79% | +0.51% |
| TPC-H q01 | -0.24% | +1.38% | -0.69% | -0.30% | -0.76% | -1.45% | -2.58% | +1.35% | -1.03% | +0.06% |

The ordinary work counters remain within about 1.5%, duration is flat, and the
larger dTLB movements reverse across placements. There is no evidence of a
performance regression from making the pool lifetime explicit.

Each JSON file is the machine-readable JMH result. Its matching log records the
complete invocation, environment, profiler availability, and raw trial output.
