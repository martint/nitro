# Explicit hash-join compatibility ownership

Date: 2026-07-25

The retained slice moves hash join's cross-operator allocator compatibility
token and enablement policy into `HashJoinOperatorResources`, constructed by
`EngineResources`. Each join still owns a distinct local pool group and two
independent build/result lease contexts. The allocator still admits sharing
only when at least three compatible local groups register in the current
execution; a two-join plan remains isolated.

## Qualification

Candidate and exact parent `18d4c047` used JDK 26, one unpinned JMH thread,
`-Xmx12g`, THP, 10x1s warmup, 5x1s measurement, three forks, allocation, and
all eight hardware counters in each invocation.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q64 | -1.28% | +0.23% | -1.25% | -0.52% | -0.83% / -0.58% | -22.86% / -35.96% | -1.89% / -1.21% |
| TPC-H q18 | -0.99% | -0.10% | -0.76% | -1.49% | -0.73% / -0.50% | -2.27% / -2.85% | +0.39% / -0.95% |

Geometric-mean duration improves 1.13%; summed duration improves 1.14%.
Q18 retains its known noisy GC/translation distribution, so no performance
improvement is credited to an identity-ownership refactor. Crucially, q64
retains the shared-pool allocation regime and q18 retains the two-join isolated
regime.

Real-SF10 q18 operator/compiled references pass. Q64 Nitro operator, Trino
operator, Trino SQL, and compiled parity pass using the explicit
`/root/notes/trino` source root. The complete JDK 26 gate passes 1,365 tests
with zero failures/errors and 566 skips. No JFR artifacts were created, and
the frozen pre-architecture board is unchanged.

Primary evidence:

- `candidate-3fork.json`
- `parent-reverse-3fork.json`
