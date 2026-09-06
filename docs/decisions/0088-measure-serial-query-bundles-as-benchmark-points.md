# ADR-0088: Measure serial query bundles as benchmark points

- **Status:** accepted
- **Date:** 2026-09-07
- **Spec:** §18
- **Amends:** [ADR-0087](0087-reuse-one-benchmark-jvm-per-suite.md)
- **Related:** [ADR-0028](0028-separate-routine-and-stress-benchmarks.md)
- **Evidence:** `2026-09-07-engine-coverage-sizing-proposal.md`

## Context

The complete Cork W0/M1 Engine Coverage duration inventory found 144 of 241 queries below the 25-second routine
minimum. Enlarging each source independently would multiply data variants, storage, and campaign work while adding no
new semantic or physical coverage. Removing the queries would lose coverage, and executing them independently would
leave fixed adaptive-sampling and checkpoint overhead dominant.

ADR-0087 requires query-specific warmup, measurement, validation, plans, capabilities, diagnostics, and checkpoints.
A harness-level serial bundle preserves the SQL and evidence of every member, but makes the bundle rather than one SQL
query the adaptive and checkpoint unit. That is a material change to the methodology and must be explicit.

## Decision

A workload manifest declares ordered **benchmark points**. A point is either one query or an ordered serial bundle of
existing queries. The point is the sizing, warmup-convergence, measurement-convergence, profiling, timing, and durable
checkpoint identity. Subject and control use the same point manifest, member order, suite order, data, and runner
boundaries.

One iteration of a bundle executes every member serially in the same suite runner. The iteration measurement sums
member latency, execution time, CPU time, allocation, physical input bytes, and physical input positions, and reports
the maximum member peak memory. Convergence is evaluated only on those aggregate iteration measurements. A member
failure fails the point and identifies the member; a partially executed bundle is never checkpointed or retained as a
measurement.

Every member keeps its original SQL and independent result validation, EXPLAIN evidence, Nitro capability inventory,
and execution diagnostics. Bundle construction does not fuse SQL, share results, change query predicates, or make
member-specific engine policy. Individual member identities remain selectable for labelled diagnostics and result
recording, but they are not additional routine manifest points.

The manifest is the single source of truth for Java execution and remote campaign tooling. Checkpoints record the
point identity and ordered member list. Resume begins at the first uncheckpointed point in a replacement suite JVM.
TPC-H, TPC-DS, and ClickBench remain unchanged one-query points.

## Consequences

- Short coverage queries retain their correctness and physical-plan evidence while amortizing adaptive sampling and
  checkpoint overhead into 25--45-second routine points.
- Point-level timing rows cannot be compared directly with historical member-level timing rows; member evidence still
  permits correctness and plan comparison.
- An aggregate bundle latency is the sum of serial member query latencies, while aggregate peak memory is a maximum,
  not a sum.
- The launcher and auditor must validate the point manifest, ordered members, aggregate rows, and member artifacts.
- Changing bundle membership, order, or data scale changes benchmark identity and invalidates prior duration
  inventories and subject/control measurements.

## Alternatives considered

**Fuse member SQL into one statement.** This can change plans, scheduling, result shapes, and operator dominance and
would no longer preserve the existing coverage queries.

**Scale every short query upward.** This requires many more source variants and repeats work without improving
semantic coverage; some queries are dominated by fixed planning or result costs rather than source scale.

**Keep every short query as an independent adaptive point.** This preserves old identities but leaves hundreds of
undersized invocations and makes the routine campaign unnecessarily long.

**Drop undersized queries.** This improves throughput by removing coverage and is therefore unacceptable.
