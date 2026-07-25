# Explicit grouping-state pooling resources

Date: 2026-07-25

`GroupingState` previously owned a process-static primitive-pool family token
and resolved its pooling policy at class load. `GroupingStateResources` now
owns both. It is constructed by `EngineResources` and passed to every grouping
state. The direct-table density threshold is derived from the injected policy
in the state constructor, preserving the coupled admission behavior.

## Qualification

Candidate and exact parent `66bf7d6b` used JDK 26, one unpinned JMH thread,
`-Xmx12g`, THP, 10x1s warmup, 5x1s measurement, three forks, allocation, and
all eight hardware counters in each invocation.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q64 | -1.16% | -0.09% | -0.27% | -0.68% | +0.28% / +0.15% | -11.28% / -13.41% | -0.55% / -0.39% |
| TPC-H q11 | +1.17% | +0.02% | -0.58% | +0.86% | -0.31% / -0.83% | +2.23% / -4.16% | -1.17% / -1.11% |

Geometric-mean duration is neutral at -0.0025%; summed duration improves
0.99%. Q11's wall/cycle movement conflicts with its lower retired and memory
work and is treated as noise, not a regression or improvement. Allocation
signatures confirm the zeroed-family reuse regime is unchanged.

Real-SF10 q11 operator/compiled references and q64 Nitro operator, Trino
operator, Trino SQL, and compiled parity pass. The complete JDK 26 gate passes
1,368 tests with zero failures/errors and 566 skips. No JFR artifacts were
created. The frozen pre-architecture board is unchanged.

Primary evidence:

- `candidate-3fork.json`
- `parent-reverse-3fork.json`
