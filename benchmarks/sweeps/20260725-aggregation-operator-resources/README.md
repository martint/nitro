# Explicit global-aggregation compatibility ownership

Date: 2026-07-25

`AggregationOperator` previously used one process-static `Object` as the
allocator pool group for all global aggregates. The object retained no storage,
but its identity decided which independently owned aggregate contexts could
recycle compatible buffers after release.

The retained slice introduces `AggregationOperatorResources`.
`EngineResources` receives one constructed instance, and each aggregate creates
its independent `Allocator.Context` with the resource owner's compatibility
identity. Closing or constraining a nested aggregate still cannot release an
outer aggregate's live state. Separate engine owners no longer share an
implicit process domain.

## Qualification

Candidate and exact parent `6cf759f4` used JDK 26, one unpinned JMH thread,
`-Xmx12g`, THP, 10x1s warmup, 5x1s measurement, three forks, allocation, and
the fixed eight hardware counters in each invocation. The complete candidate
ran before a freshly built exact-parent control.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H q11 | +0.08% | -0.03% | -0.44% | -1.00% | -3.19% / -1.08% | -1.69% / -15.06% | -4.01% / -0.61% |
| TPC-H q15 | +0.04% | +0.00% | +0.33% | +0.39% | +0.90% / +0.48% | +0.82% / +7.97% | +2.18% / +0.10% |

Geometric-mean duration is +0.058%; summed duration is +0.046%
(1367.400 versus 1366.778 ms). The dTLB-load percentages come from low
absolute event counts and move in opposite directions. There is no stable
performance regression or credited improvement.

Real-SF10 operator and compiled q11/q15 results match the independent-engine
canonical references. Focused ownership/operator tests pass, followed by the
complete JDK 26 gate: 1,362 tests, zero failures/errors, 566 skips. No JFR
artifacts were created. The frozen performance board remains the
pre-architecture baseline and is not refreshed by this focused ownership
cohort.

Primary evidence:

- `candidate-3fork.json`
- `parent-reverse-3fork.json`
