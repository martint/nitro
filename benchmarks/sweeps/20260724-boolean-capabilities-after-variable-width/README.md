# Boolean projection capabilities after variable-width completion

Date: 2026-07-24

This retries moving `and`, `or`, and `not` projection lowering from scalar
implementations into separately constructed registry capabilities after the
generic generated backend gained variable-width output support. The exact
parent is `c98675c9`.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the fixed eight-event `perfnorm` bundle.

The migration is retained. Completing variable-width generation removed q40's
former 6.44% allocation regression and the large partial-class-shape
instability. The affected cohort is:

| query | duration | allocation | instructions | cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | branch misses | branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q21 (1 fork) | +0.36% | +0.01% | -0.50% | -0.30% | -2.04% | -1.45% | -2.46% | -5.95% | -1.39% | -0.47% |
| TPC-DS q54 (3 forks) | -0.13% | +0.11% | -0.00% | +0.20% | +1.34% | -0.53% | +1.24% | +4.31% | +1.51% | +0.66% |
| TPC-DS q67 (3 forks) | -4.11% | -0.00% | -3.73% | -2.66% | +0.05% | -7.61% | +5.58% | -4.65% | +1.50% | -2.80% |
| ClickBench q40 (3 forks) | -1.90% | -0.61% | +0.50% | -0.51% | -0.52% | -0.03% | +0.07% | -2.14% | -0.48% | +0.31% |

Q54 reversed direction between adjacent one-fork pairs, so only its escalated
three-fork result is used. Q67 remains compiler-mode sensitive: the exact
parent entered a high-work mode in one fork, while the candidate's three forks
were narrower. The result is retained as evidence for the complete
architecture boundary, not described as a semantic boolean optimization.
Small adverse counters are published rather than filtered.

Artifacts:

- `candidate-screen.json`, `parent-reverse-screen.json`
- `candidate-q40-q67-3fork.json`, `parent-reverse-q40-q67-3fork.json`
- `candidate-q21-q54.json`, `parent-reverse-q21-q54.json`
- `candidate-q54-3fork.json`, `parent-reverse-q54-3fork.json`

This is an architecture qualification, so no published board row changes.
