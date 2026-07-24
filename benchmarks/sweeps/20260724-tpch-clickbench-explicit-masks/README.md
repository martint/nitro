# TPC-H and ClickBench explicit filter-mask qualification

Date: 2026-07-24

This slice removes the test-only logical-call compatibility adapter from the
TPC-H and ClickBench operator harnesses. Filter conjunction, disjunction, and
negation are carried as `AndMask`, `OrMask`, and `NotMask`; ordinary boolean
values that are materialized by a projection remain dynamically registered
scalar calls.

Qualification compares this candidate with its exact adjacent parent
`7fe325d4` in reverse order. Both builds use JDK 26, one unpinned JMH thread, a
12 GB heap, transparent huge pages, 10x1s warmup, 5x1s measurement, one fork,
steady-state allocation, and the same eight-event `perfnorm` bundle:
instructions, cycles, L1D misses/loads, dTLB misses/loads, branch misses, and
branches. The operator harnesses and SQL-equivalent query shapes are unchanged.

The first candidate screen exposed a correctness bug in TPC-H q19's harness
builder. `projectQuery19BranchMask` treated the last assignment as the complete
predicate. Once redundant logical assignments disappeared, that happened to be
only the last leaf comparison. `candidate-screen.json` is diagnostic evidence
of that invalid candidate and must not be used as a performance result.

A correct structural `Merge(predicate, bit, zero)` experiment removed the
positional dependency, but `candidate-q19-structural-merge.json` measured
2166.038 ms and 6.303 GB/op. The generic merge materialization path is not
competitive for this shape and was rejected.

The qualified implementation gives `FilterSpec` two explicit representations:

- `predicate`, an `AndMask`/`OrMask`/`NotMask` tree used for control flow;
- `materializedValue`, a registry-resolved scalar reference used only when a
  downstream expression needs the boolean as data.

There is no inference from an assignment's position and no logical-function
knowledge in an operator, evaluator, or production resolver.

Positive deltas below mean the candidate used more than the exact parent.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H q13 | +0.97% | -0.06% | +0.51% | +1.46% | +0.61% | +0.20% | +40.11% | +5.73% | +0.39% | +0.42% |
| TPC-H q19 | +0.35% | +0.19% | +0.26% | +0.29% | -0.44% | +0.35% | -2.77% | -3.92% | +0.59% | +0.16% |
| ClickBench q43 | +2.12% | +3.76% | +1.69% | +1.01% | +0.32% | +1.03% | -4.33% | -24.25% | -5.02% | +1.78% |

The q13 dTLB-miss estimates are 266K ±1.400M for the candidate and
190K ±311K for the parent, so the percentage is not meaningful. Q43 is also
fork-sensitive: candidate duration is 121.404 ±2.103 ms and parent duration is
118.887 ±1.796 ms, while parent allocation includes an isolated 89.2 MB/op
fork between two 101 MB/op forks. The confidence intervals overlap and stable
high-volume counters remain within about 0–2%. Q19's escalated result is
effectively neutral across duration, allocation, instructions, cycles, and
branches.

The full JDK 26 gate passes 1,329 tests with zero failures/errors and 566
environment-dependent skips. No JFR artifacts were created. The published
performance board is unchanged.
