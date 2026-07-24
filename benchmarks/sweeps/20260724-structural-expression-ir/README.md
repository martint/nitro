# Structural conditional/coalesce IR qualification

Date: 2026-07-24

This slice replaces name-recognizing `"if"` and `"coalesce"` normalization
with explicit `Conditional` and `Coalesce` structural operations. Their fixed
fields encode arity; they normalize into existing masks, copies, and merges
before execution. Dynamically registered functions such as `if_i64` remain
ordinary calls and are not recognized by the engine.

All controls use JDK 26, one unpinned JMH thread, a 12 GB heap, transparent
huge pages, 10x1s warmup, 5x1s measurement, steady-state allocation, and the
same eight-event bundle: instructions, cycles, L1D misses/loads, dTLB
misses/loads, branch misses, and branches. Harnesses and query shapes are
unchanged.

The adjacent one-fork candidate/reverse-parent screen covers TPC-H q01,
TPC-DS q39, and ClickBench q43. Q39 is favorable on duration, instructions,
cycles, both L1 metrics, and dTLB misses; allocation moves +0.63% and the noisy
dTLB-load/branch-miss estimates move +5.39%/+3.39%.

The escalated three-fork q01/q43 comparison records:

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ClickBench q43 | -1.36% | -4.13% | -1.83% | -2.12% | -0.36% | -2.21% | -4.68% | -2.87% | -6.32% | -1.69% |
| TPC-H q01 | -1.77% | +0.00% | -0.18% | -1.68% | +0.24% | -0.44% | +4.01% | +3.41% | +0.64% | -0.13% |

The q01 dTLB estimates have very large three-fork error bars (candidate
207.5K ±107.6K misses and 2.118M ±1.274M loads; parent 199.6K ±58.5K and
2.048M ±504K), so those small deltas are not evidence of a regression. The
duration and stable high-volume counters qualify the slice.

The full JDK 26 gate passes 1,327 tests with zero failures/errors and 566
environment-dependent skips. The published board is unchanged.

Remaining debt: `MaskExpressionResolver` still recognizes logical
`and`/`or`/`not` call names when expanding a referenced predicate. That must
move to explicit structural mask metadata in a separate slice.
