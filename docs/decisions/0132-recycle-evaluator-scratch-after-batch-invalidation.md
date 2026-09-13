# ADR-0132: Recycle evaluator scratch after batch invalidation

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §6, §9, §11, §13, §18
- **Related:** accepted ADR-0007, ADR-0041

## Evidence

The residual-join diagnostic allocates about 84 GB in generated scalar contexts even in its lower-allocation forks.
The earlier source audit identified ProjectOperator's discard-only evaluator reset during constraints. A focused
test now proves a reusable intermediate allocates another 4 KiB on each constraint after warmup, although Batch has
already invalidated every exposed output before invoking its constrainer. This is distinct from row-position boxing.

## Decision and correlation

Use Batch's existing ordered invalidation contract to recycle remaining evaluator scratch when resetting a projected
batch. Clear derived mask and output caches before re-evaluation. Keep the conservative discard-only reset for the
operator-level entry point, which does not invalidate exposed output generations first. Retain transfer rejection and
the existing output-recycling policy; do not add an experiment flag or weaken borrow lifetimes.

After the hash join consumes its probe batch, constrain that Batch rather than bypassing its output lifecycle through
the operator. Reborrow payload through the constrained batch. Exact matching, residual filtering, masks and join
semantics remain unchanged; no planner or scheduler changes. This fills an implementation gap in the accepted ownership
and steady-state allocation contracts. It does not claim all remaining scalar allocation is explained by this path.

## Qualification

Require stable tracked intermediate allocation across repeated dense/sparse batch constraints, exact values, and
rejection of constraints after output transfer. Test join reborrow, encoded payloads, aliases, retained outputs,
unmatched rows and batch close. Run the full Nitro suite and host integration checks, then paired q242/q243 and macro
CPU/allocation guards with frozen binaries. Remove the candidate if correctness or SQL qualification fails.

## Evidence during qualification

The test stops the reproduced 4 KiB-per-constraint growth. Full Nitro (2,174 tests) and 45 focused host tests pass.
Local q242/q243 CPU ratios are 0.8640/0.7901 and allocation ratios 0.3159/0.3394. A fresh combined fork also wins.
Remote q242/q243 CPU ratios are 0.9088/0.9204, allocation ratios 0.2365/0.2973, with separated ranges and exact
results. Generated-scalar context allocation in local q242 falls from about 84 GB to 0.47 GB. Macro qualification
completed on all 12 selected macro/bundle guards with exact/native checks passing. Most CPU ranges overlap;
TPCH q21 is 0.5% higher in this fork with separated ranges, whereas its allocation ranges overlap. Record that
small unresolved timing movement for the synchronized sweep rather than calling every guard a CPU win.
These diagnostics are not a replacement benchmark board.

Recycling increases some retained working sets: remote q242 peak rises from 1.127 to 1.187 GB (1.0530×).
Report that co-tenancy cost alongside CPU/allocation savings; do not relabel allocated bytes as retained memory.
No retention-budget default is changed. Other operators still have duplicate operator/batch constraints and require
their own lifecycle audit; this decision does not assert that all constrained execution is now allocation-free.
