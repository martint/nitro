# ADR-0028: Separate routine performance coverage from stress coverage

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-09-02)
- **Spec:** §18
- **Historical evidence:** `2026-08-26-remote-benchmark-runbook.md`

## Context

Some Engine Coverage points grew to several minutes per execution. They made a full synchronized campaign take more
than a day, delayed failure discovery, and encouraged fewer repetitions, while often testing scale rather than a new
semantic or physical capability.

## Decision

Routine operator-dominant points preferably take 15–45 seconds on the slower engine and normally stay below one minute.
Long capacity, skew, memory-pressure, and scale-limit points live in an explicitly selected stress tier. A one-run
control preflight publishes runtime inventory before a campaign.

## Consequences

- Full routine sweeps remain operationally useful and statistically repeatable.
- Right-sizing preserves logical domains, selectivity, encoding, plan shape, and operator dominance.
- Scale changes invalidate prior measurements for that point.
- Stress behavior remains covered without taxing every iteration cycle.

## Alternatives considered

**Keep all points at maximum scale.** Operator-dominant but too slow to function as a regression loop.

**Apply an outer LIMIT.** Shortens output, often without reducing the operator work under test and may change plans.

**Use only microbenchmarks.** Fast but misses integrated ownership, planning, source, and lifecycle behavior.
