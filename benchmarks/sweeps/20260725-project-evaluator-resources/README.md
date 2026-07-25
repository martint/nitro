# Explicit project-evaluator compatibility ownership

Date: 2026-07-25

`ProjectOperator` previously used one process-static `Object` as the allocator
compatibility group for evaluator buffers. The group does not own storage, but
its identity controls which closed evaluator generations may recycle compatible
buffers. It is therefore an ownership-bearing resource, not an immutable
constant.

The retained slice introduces `ProjectOperatorResources`. `EngineResources`
receives one constructed instance and `ProjectOperator` obtains the
compatibility group through that owner. Separate engine owners have distinct
domains; evaluators under one owner retain the established compatible-recycling
behavior. Per-generation `Allocator.Context` ownership and live-buffer
isolation are unchanged. No static pool, service, cache, or identity token was
added.

## Qualification

Candidate and exact parent `b4bcd5c4` used JDK 26, one unpinned JMH thread,
`-Xmx12g`, THP, 10x1s warmup, 5x1s measurement, three forks, allocation, and
the fixed eight hardware counters in each invocation. The complete candidate
ran before a sparse-workspace exact-parent control.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q24 | -0.05% | -0.24% | +0.30% | -0.25% | -0.68% / +0.23% | +0.77% / +0.33% | -0.02% / +0.10% |
| TPC-DS q82 | -2.18% | -0.06% | -0.68% | -2.75% | -2.25% / -0.16% | -16.73% / -34.52% | -0.80% / -0.57% |

The q82 screen initially appeared 12.5% faster. That magnitude was treated as
compilation-mode evidence, not credited to the ownership change; the retained
numbers are the complete three-fork pair above.

Focused projection/operator/evaluator tests and architecture ownership tests
pass. The frozen performance board remains the pre-architecture baseline and
is not refreshed by this focused ownership cohort.

Primary evidence:

- `candidate-3fork.json`
- `parent-reverse-3fork.json`
- `candidate-screen.json`
- `parent-screen.json`
