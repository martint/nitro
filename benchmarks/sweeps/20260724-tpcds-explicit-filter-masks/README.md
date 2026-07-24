# TPC-DS explicit filter-mask qualification

Date: 2026-07-24

This slice migrates the remaining TPC-DS operator harness filters to explicit
`AndMask`, `OrMask`, and `NotMask` control-flow structure and deletes the
test-only `LegacyLogicalMaskAdapter`. Registry-resolved logical scalar calls
remain only where a materialized boolean value is part of the evaluation plan.
Filter composition carries a separately named materialized reference and
never infers it from assignment order.

Qualification compares this candidate with its exact adjacent parent
`4a032c8f` in reverse order. Both builds use JDK 26, one unpinned JMH thread, a
12 GB heap, transparent huge pages, 10x1s warmup, 5x1s measurement, one fork,
steady-state allocation, and the same eight-event `perfnorm` bundle:
instructions, cycles, L1D misses/loads, dTLB misses/loads, branch misses, and
branches. The operator harnesses and SQL-equivalent query shapes are unchanged.

Positive deltas below mean the candidate used more than the exact parent.
Q04, q54, and q64 use the adjacent one-fork screen. Q21 and q45 use the
escalated adjacent three-fork comparison.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q04 | -1.95% | -0.04% | -1.79% | -3.43% | -4.31% | -2.38% | -5.60% | -16.31% | -1.66% | -0.95% |
| TPC-DS q21 | -0.07% | -0.01% | -0.26% | -0.16% | -0.87% | -0.49% | +0.53% | -1.11% | -3.38% | -0.10% |
| TPC-DS q45 | +0.65% | -0.14% | +0.43% | +0.59% | +0.37% | +1.00% | +1.95% | -2.18% | -0.54% | +0.08% |
| TPC-DS q54 | -0.66% | -0.04% | -0.78% | -0.29% | -2.20% | +0.27% | -2.06% | -15.83% | +2.09% | +0.06% |
| TPC-DS q64 | -5.33% | -0.31% | -1.48% | -4.15% | -6.15% | -0.73% | -15.81% | +3.01% | -2.89% | -1.34% |

The first q45 screen had adverse 2–7% cycle/cache/branch estimates despite a
duration win. At three forks those movements collapse to 0–2%, allocation
improves, and duration is 66.129 ±0.186 ms versus 65.703 ±0.482 ms. Q21 is
neutral-to-better on every stable high-volume metric. Q04 and q64 improve
broadly, including 1.95% and 5.33% duration wins.

The full JDK 26 gate passes 1,329 tests with zero failures/errors and 566
environment-dependent skips. No JFR artifacts were created. This is an
architecture control; the published performance board is unchanged.
