# Process-global output diagnostics removal

This focused control validates removal of the disabled, process-global
`OutputDebug` counters and their calls from the `Output` hot path. It is not a
replacement for the published 165-query board.

## Method

- JDK `/opt/java/openjdk` (26)
- Nitro operator harnesses, unpinned, one JMH thread and one fork
- 12 GiB heap with transparent huge pages
- 5 × 1 s warmup and 5 × 1 s measurement
- `gc` and Linux `perfnorm` profilers
- instructions, cycles, L1D misses/loads, dTLB misses/loads, branch misses, and
  branches captured in the same invocation
- candidate first, then the exact parent `c9abb497` from an isolated jj
  workspace

Positive deltas mean the candidate used more than the reverse-order parent.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ClickBench q43 | -4.59% | -11.69% | -5.53% | -4.12% | -1.50% | -4.63% | -2.04% | +9.09% | +2.74% | -5.32% |
| TPC-DS q64 | -4.83% | +0.22% | -0.53% | -0.87% | +0.44% | +0.12% | -34.86% | -51.90% | +0.67% | -0.76% |
| TPC-H q01 | +0.14% | -1.03% | -0.04% | +0.05% | +0.51% | -0.08% | -2.22% | +0.28% | -0.69% | +0.02% |

TPC-H q01 is the stable neutral guard. The larger q43/q64 wall and translation
movements are favorable but exceed what their ordinary work counters alone
explain, so they are treated as placement/run-order effects rather than a
promotable speedup. There is no evidence of a regression.

Each JSON file is the machine-readable result; the matching log contains the
complete environment and raw trial output.
