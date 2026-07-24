# Core integration ports validation

This directory is a targeted regression screen for the first architecture-cleanup
slice. It does **not** replace or mix any row on the published 165-query board.
The authoritative pre-change board remains the tagged
`pre-architecture-baseline-jdk26-20260723` capture.

All JMH captures used JDK 26 at `/opt/java/openjdk`, one unpinned JMH thread,
one fork, five 1-second warmup and five 1-second measurement iterations,
`-Xmx12g`, normalized allocation, and the complete standard hardware-counter
set.

## Qualified controls

Relative to the published pre-architecture baseline:

| Query | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ClickBench q43 | -2.86% | +1.20% | +0.26% | -2.38% | -0.73% | +0.68% | -10.82% | -13.36% | +0.35% | +0.76% |
| TPC-DS q64 | +0.53% | -0.41% | +0.33% | +0.51% | +0.73% | +0.39% | +8.35% | -1.58% | +0.61% | -0.03% |

The q43 result is `nitro-q43-resolved-in-call.json`; the q64 result is
`nitro-q64-resolved-in-call.json`.

TPC-H q01 in the initial representative capture was +1.14% duration,
+0.52% allocation, -0.87% instructions, +0.80% cycles, with every measured
cache, TLB, and branch counter lower. Its legacy evaluator bytecode was not
changed by the final representation.

## Rejected intermediate representation

The initial `BoundCall` operation expanded the evaluator's hottest sealed
type-switch. The first q64 capture entered a different TLB regime and the first
q43 controls consistently raised retired work and allocation. A reverse-order
q43 run from the exact tagged source (`tagged-baseline-q43-reverse.json`) proved
that source shape mattered on the same host.

The retained representation stores the complete resolved binding as data in the
existing `Call` node. `PlanEvaluator` installs those bindings into the dynamic
primitive registry before execution. `javap` confirms the legacy
`evaluateVariable` bytecode, switch order, offsets, and length are identical to
the tagged source.

`trino-q64-control.json` is an unchanged-engine infrastructure control. It
showed +105.73% dTLB misses and +62.44% dTLB loads versus its published
baseline, confirming that TLB placement was unstable during the first q64
captures; its duration moved +1.93%.

Other JSON/log pairs retain the unqualified and rejected screens for audit.
