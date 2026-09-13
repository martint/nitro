# ADR-0134: Constrain selected batches through their lifecycle once

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §6, §9, §11, §18
- **Related:** ADR-0007, ADR-0041, ADR-0132

## Context

Several selecting consumers constrain both the source operator and its current batch. Filter and filtering semi-join
also invoke a BatchState callback which constrains that batch again. The operator-level call can discard a projected
producer's intermediates before the batch has established the lifetime proof needed to recycle them. Limit repeats
this pair in its forwarding lifecycle, so fixing only initial selection leaves downstream propagation affected.

## Proposal and correlation

After consuming input needed to derive a selection, notify the owned source batch once through its lifecycle.
That lifecycle invalidates exposed output generations before invoking producer state changes. Preserve the separate
operator-level API and its conservative behavior for callers without a batch lifetime guarantee. This conforms to
the explicit ownership, mask propagation, and scratch-reuse contracts; it changes no relational semantics, scheduler,
function implementation, or enabling policy.

Apply the correction to Filter, Limit, MarkDistinct, filtering SemiJoin, and limited PartitionedRowNumber initial
selection, and Limit's forwarding lifecycle. NestedLoopJoin's retained outer lease is not included without its own
proof. Do not claim every operator-level call is redundant.

## Validation

The source-notification test fails on the prior implementation, and the Limit-over-Project test reproduces one
additional 4 KiB intermediate allocation per post-warmup constraint. Both pass after the correction, including
unchanged-distinct passthrough and transfer rejection. Full Nitro tests pass (2,178 tests, zero failures/errors,
394 skipped), as do 45 focused host tests and the 411-query remote exact/native functional gate.

A paired local TPC-DS screen shows overlapping CPU ranges for q15/q20/q72 and a 1.65% reduction for q22.
Source-less join diagnostics show between-fork variation, including an initial local q243 slowdown which reverses
in independent CPU-profile and hardware-counter pairs. Those plans show none of the five changed operator types;
their work counters match. The instrumented profiles cannot explain the earlier unprofiled slowdown. Retain that
contrary evidence rather than claiming a causal SQL speedup from the lifecycle correction. The synchronized board
qualifies overall performance separately from the demonstrated ownership/allocation repair.

Use a producer with independent batch and operator notifications to assert one batch notification per initial and
forwarded selection, correct active rows and values. Exercise Limit over a two-call projection across repeated
dense/sparse constraints, proving constant post-warmup intermediate allocation and retained transfer rejection.
Run the existing ownership, filter, semi-join, limit, ranking, evaluator and full Nitro tests, then focused host tests
and SQL qualification. Keep functional evidence separate from performance claims.
