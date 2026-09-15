# ADR-0130: Close component benchmark allocator domains

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §16, §18
- **Related:** ADR-0041, proposed ADR-0129

## Evidence

The zero-match wide-key join component ran out of its 4 GB heap after the bounds candidate made each invocation
much faster. Its thread-scoped allocator outlived hundreds of newly constructed operators. Operator close returns
vectors to allocator-owned context pools; it does not destroy a driver/query's entire allocation domain. The slower
parent completed fewer invocations in the same timed iterations and did not reach this accumulation limit.
This invalidates that component pair; missing JMH points must not be treated as successful measurements.

## Proposal and correlation

The filtered-long and two composite-join component benchmarks create and close an allocator for each complete join
execution. Engine resources and immutable input data remain trial-owned. Allocator construction/closure is included
in these complete-execution measurements, consistently in parent and candidate. The RLE-output benchmark already
creates an invocation allocator; add teardown for it and its explicitly owned engine resources.

This corrects harness use of the existing ownership contract; production allocator pooling and host lifetimes do
not change. Historical component measurements using the previous lifetime are not silently relabeled. Re-run both
engines with the same corrected benchmark classes and retain the failed pair and class hashes as evidence.

## Validation

Run the full Nitro suite and the complete two-fork 0/10/100%-match, dense/wide-key matrix at the same 4 GB heap.
Require all parameter points and no fork failures. Then repeat SQL qualification independently; component gains
are not substitutes for query CPU, allocation and latency evidence.

## Acceptance evidence

The full 2172-test Nitro suite passes with zero failures/errors and 394 skips. Both corrected benchmark arms complete
all six parameter points, two forks each, with the same 4 GB heap; neither zero-match fork fails. The complete matrix
and class digests are retained under completed-build-bounds-jmh-r2-20260913. The original incomplete pair remains
invalid. This accepts the harness lifetime correction, not the independently proposed bounds optimization.
