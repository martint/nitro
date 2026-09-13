# ADR-0118: Count host-visible source output once

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §16, §18
- **Related:** [ADR-0026](0026-require-identical-plan-and-work-for-benchmarks.md)

## Context

The new sequence-based join probes report 2.4 billion Nitro source output positions for 1.2 billion actual rows.
Exact query results and the following exchange agree with 1.2 billion rows. Both the Nitro leaf table-function
source and VALUES adapter call OperatorContext.recordOutput before returning a Page; Driver.recordGetOutput then
accounts for that same page again. The ordinary host source relies on the driver alone.

## Decision

Let the unchanged host driver own host-visible Page output accounting. Remove explicit recordOutput calls in these
two source adapters. Preserve independent Nitro boundary diagnostics. This conforms to the existing host boundary
and attribution contracts; it changes no rows, function evaluation, scheduling, planning, or source reads.

Tests must run the actual driver around both sources, compare emitted rows/bytes with operator output statistics
and downstream input statistics, and cover multi-batch sequence output. Historical double-counted source output
metrics are not evidence of twice the execution work; do not rewrite historical artifacts to imply new measurement.

## Validation

Both new real-driver tests fail on the parent:2 VALUES rows report4, and20,000 sequence rows report40,000.
After removing the duplicate calls,18 focused tests pass: the new row/byte checks, terminal-boundary tests, and
the two planner sequence integration tests. Sequence output spans three batches and both tests verify values as
well as source-output/downstream-input counters.

The broader97-test local planner class is not a clean gate: it has9 failures and40 errors on both parent and
candidate, with identical failing method identities. Most errors reject its non-native TPC-H connector fixture;
the same tests also contain stale lowering assertions. This existing coverage debt is not repaired by adding a
connector fallback. Retain the parent comparison and do not describe the full class as passing.

Evidence under the notes root: `source-output-accounting-red-r2-tests-20260913.log`,
`source-output-accounting-parent-planner-tests-20260913.log`, `source-output-accounting-green-tests-20260913.log`,
and `source-output-accounting-focused-final-tests-20260913.log`.
