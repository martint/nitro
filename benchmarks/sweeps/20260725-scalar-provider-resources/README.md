# Scalar-provider allocation-context ownership

Date: 2026-07-25

The built-in scalar providers previously stored their allocator contexts in
process-static fields. That made dynamically registered providers share an
implicit allocation identity across registries, engine owners, and
classloader lifetimes. Every provider now owns its contexts as instance state.
`RegexpReplaceUtf8` also constructs and owns its specialized
`ExtractHostUtf8` helper and publishes both contexts through the provider
contract.

An architecture ratchet rejects static allocator contexts in the built-in
provider package. Runtime ownership tests prove that separate provider
instances publish disjoint contexts and that a composed provider publishes its
helper's context.

## Qualification

Candidate and exact parent `e0af7eda` used JDK 26, one unpinned JMH thread,
`-Xmx12g`, THP, 10x1s warmup, 5x1s measurement, three forks, allocation, and
all eight hardware counters in each invocation.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| ClickBench q29 | -0.16% | -0.04% | -0.78% | -1.11% | -0.99% / -2.08% | -26.38% / -26.05% | +0.32% / -0.40% |
| ClickBench q43 | -0.36% | +0.92% | -2.78% | -1.86% | +0.70% / -2.39% | -0.81% / -14.28% | -2.77% / -1.95% |

Geometric-mean duration improves 0.26%; summed duration improves 0.16%.
These are treated as neutral rather than credited improvements.

Q29 exposed a pre-existing fork-dependent allocation mode: both candidate and
parent ran approximately 2.38 GB/op, 1.10 GB/op, and 2.38 GB/op in the same
fork order while duration remained stable. Q43 also varied by roughly 10%
between forks. The aggregate allocation deltas therefore are not interpreted
as effects of this slice. The raw fork distributions are retained in the JSON
artifacts for follow-up; no favorable mode was selected.

Fixture, real-data, and compiled-reference q29/q43 parity pass along with
focused architecture, registry, primitive-function, regexp, and evaluator
tests. The complete JDK 26 gate passes 1,371 tests with zero failures/errors
and 566 skips. No JFR artifacts were created. The frozen pre-architecture
performance board is unchanged.

Primary evidence:

- `candidate-3fork.json`
- `parent-reverse-3fork.json`
- `focused-correctness.log`
- `full-gate.log`
