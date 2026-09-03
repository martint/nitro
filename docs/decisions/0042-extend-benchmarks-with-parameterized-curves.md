# ADR-0042: Extend macro benchmarks with attributed parameterized response curves

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; specified 2026-08-21)
- **Spec:** §18, §19
- **Depends on:** [ADR-0026](0026-require-identical-plan-and-work-for-benchmarks.md), [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Historical evidence:** `2026-08-21-benchmark-suite-evolution.md`

## Context

TPC-H, TPC-DS, ClickBench, and fixed Engine Coverage queries each sample one selectivity, cardinality, encoding, skew,
layout, and concurrency point. Query totals cannot reveal whether a source win hides an aggregation regression, and
fixed points cannot locate where adaptive strategies cross over or thrash.

## Decision

Retain macro suites as regression guards and extend Engine Coverage with deterministic parameterized data/query
families, per-operator attribution, response curves, crossover/cliff detection, mid-stream shifts, oscillation, and
decision-boundary probes. The harness remains applicable to Nitro, Trino, Cork, and other engines through common SQL,
data, plan validity, and metric contracts.

## Consequences

- Adaptive mechanisms are judged over their input space, not fortunate defaults.
- Dataset generation records seeds, distributions, physical Parquet layout, engine revisions, and hardware.
- Unattributable points, plan divergence, spill, or material blocking fail validation instead of entering curves.
- Building the generator and metrics pipeline is substantial benchmark infrastructure work.

## Alternatives considered

**Add more fixed public suites only.** Broadens workload coverage but still samples each mechanism at arbitrary points.

**Use JMH only.** Excellent kernel isolation and unable to validate integrated plans, sources, ownership, or adaptivity.

**Rely on query-level totals.** Operationally simple and cannot identify which subsystem moved.
