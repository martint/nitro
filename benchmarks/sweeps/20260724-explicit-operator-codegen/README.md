# Explicit operator code-generation resources

This focused control validates moving the fused grouped-aggregation kernel cache
and generated-class identity from process-global static state into explicitly
constructed `OperatorCodeGenerationResources`. It does not replace the
published 165-query board.

## Method

- JDK `/opt/java/openjdk` (26)
- Nitro operator harnesses, unpinned, one JMH thread and one fork
- 12 GiB heap with transparent huge pages
- 5 × 1 s warmup and 5 × 1 s measurement
- allocation and all eight publication hardware counters captured together
- candidate first, then exact parent `8f49ea77` from a sparse jj workspace

Positive deltas mean the candidate used more than the reverse-order parent.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q39 | -3.56% | -0.02% | -2.38% | -5.25% | -5.81% | -1.61% | +1.41% | -8.42% | -4.94% | -1.57% |
| TPC-DS q17 | +0.34% | +0.00% | -0.40% | -0.02% | +0.63% | +0.06% | -12.55% | -1.79% | +0.37% | -0.45% |

The q17 guard is neutral on duration, allocation, and ordinary work. Q39 is
favorable across duration and ordinary counters. Single-fork dTLB movements are
treated as placement effects rather than promotion evidence. There is no
measured regression.

Each JSON file is machine-readable; the matching log contains the complete
environment and raw trial output.
