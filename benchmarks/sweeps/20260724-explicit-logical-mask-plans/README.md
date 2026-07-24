# Explicit logical mask-plan qualification

Date: 2026-07-24

This slice removes `and`/`or`/`not` function-name recovery from production
`MaskExpressionResolver`. Logical predicate structure now arrives as
`EvaluationPlan.maskPlans` or as a direct `AndMask`, `OrMask`, or `NotMask`.
An unannotated call remains an ordinary dynamically resolved function even if
its name happens to be `"or"`.

The three legacy benchmark builders still construct some logical calls by
string. A test-only `LegacyLogicalMaskAdapter` preserves their established
operator shape outside production Nitro. New harness code must construct
structural masks directly. TPC-DS q39's covariance predicate was migrated
fully: its redundant boolean `and` assignments are gone and its filter carries
the direct three-term `AndMask`.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the same eight-event bundle: instructions, cycles, L1D
misses/loads, dTLB misses/loads, branch misses, and branches. Harness SQL and
operator semantics are unchanged.

The first candidate screen intentionally ran before the legacy planners were
adapted. It exposed q39's dependency on implicit name recovery: duration
regressed 10.53% and allocation 25.45%. That result is a diagnostic rejected
shape, not a candidate for publication.

After the test-only adapter and direct q39 mask:

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ClickBench q43 | -2.06% | -12.36% | -4.73% | -3.19% | -1.20% | -5.06% | -3.49% | -33.59% | -8.01% | -4.47% |
| TPC-DS q39 | -2.95% | +0.59% | -1.05% | +1.68% | +2.64% | -1.56% | -1.63% | +0.34% | -0.49% | -0.74% |

TPC-H q01's one-fork duration signal was escalated. The adjacent three-fork
candidate/reverse-parent comparison is +0.59% duration, -0.25% allocation,
-0.51% instructions, +0.45% cycles, -0.79% L1D misses, -0.51% L1D loads,
-0.04% dTLB misses, +1.58% dTLB loads, -0.73% branch misses, and -0.01%
branches: neutral within fork variance, with the stable high-volume counters
at parity or better.

`candidate-q39-explicit` and `candidate-q39-direct-mask` were accidentally
executed before recompiling modified test sources and therefore contain the
same stale pre-migration class shape. They are retained as tooling evidence but
must not be used for comparison. `candidate-q39-direct-mask-built` is the first
compiled direct-mask run.

The full JDK 26 gate passes 1,328 tests with zero failures/errors and 566
environment-dependent skips. The published board is unchanged.
