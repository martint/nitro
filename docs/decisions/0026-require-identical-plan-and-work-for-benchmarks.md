# ADR-0026: Require identical plan shape and physical work for engine comparisons

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-08-02)
- **Spec:** §17, §18
- **Historical evidence:** reconciliation entries for SQL-shaped operator replacement, join orientation, and physical-work parity

## Context

Earlier operator fixtures sometimes followed a logical SQL result or a Velox-inspired shape instead of the optimized
host plan. Reversed joins, missing stages, cached intermediate results, and different source positions produced large
ratios that did not describe execution-engine improvement.

## Decision

Subject and control use the same SQL-derived optimized topology, join orientation, stage boundaries, liveness, input
files, and execution configuration. Capture per-stage rows, bytes, positions, and operator CPU. A point with unexplained
plan or work divergence is invalid rather than a performance result.

## Consequences

- Operator and SQL measurements describe the same workload shape.
- Legitimate dynamic-filter pruning differences are reported explicitly rather than normalized away.
- Harnesses cannot drain/replay intermediates unless both engines contain the same barrier.
- Some attractive historical numbers are retired when their comparison shape is disproved.

## Alternatives considered

**Compare logically equivalent results.** Necessary for correctness and insufficient for performance attribution.

**Choose the best plan per engine.** Useful product comparison, but cannot isolate execution implementation.

**Force a Velox-style plan.** Answers a different question unless Velox and Trino plans are already identical.
