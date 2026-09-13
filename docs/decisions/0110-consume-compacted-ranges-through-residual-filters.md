# ADR-0110: Consume compacted ranges through residual filters

- **Status:** accepted; remote performance qualification pending
- **Date:** 2026-09-13
- **Spec:** §7, §12, §16, §18
- **Depends on:** [ADR-0011](0011-generate-specializations-from-physical-capabilities.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

An admitted compacted join index already exposes insertion-ordered row ranges and removes proven misses from
a batch of probes. Unfiltered inner joins consume that representation directly. Filtered inner joins instead
construct list views, inspect every probe's list size, and dispatch through a list for each residual candidate.
Remote q21 profiles identify this dispatch and miss processing as worthwhile targets; previous copying of
matching references to scratch alone did not improve CPU.

## Decision

Allow inner joins with residual filters or first-match output to consume the same admitted row-range capability.
Expose direct reference lookup in its existing ordinal space and keep ordered payloads in that space. Retire
proven misses during batch lookup; evaluate every remaining required residual predicate in source match order.
Retain list-based matching for indexes without the capability and retain existing outer-join handling.

This extends consumption of a proven physical capability, not logical join semantics or query-specific shape
recognition. It adds no feature switch, changes no source pruning, and does not modify scheduling, join order,
type binding, output ownership, or allocator retention.

## Validation

Cover all-miss chunks followed by matches, nullable and sparse probes, duplicate chains spanning output batches,
first accepted match after rejected candidates, residual nulls, outer-join preservation, and multiple build
batches. Verify reference and ordered-payload coordinate agreement. Run focused and full tests, then exact warmed
parent/candidate SQL comparisons and neighboring join shapes. Accept only with repeatable end-to-end benefit.

The full suite passes 2,153 tests, zero failures/errors, 394 skipped. Exact local W20/M7 q21
parent/candidate/parent CPU medians are 7.018 / 6.477 / 7.018 seconds, with separated ranges. A five-query
q09/q13/q17/q20/q21 sequence repeats the q21 benefit (0.9335 CPU ratio, separated ranges) without an
established neighboring CPU or latency regression. Source positions and bytes match. Latency ranges overlap;
no latency win or remote improvement is claimed from these local results. The expanded microbenchmark varies
match rate and key-domain spacing: dense keys use the direct index, while wide keys exercise hashed duplicate
compaction. The initial dense-only comparison is essentially neutral and does not qualify the changed capability.
Wide-domain component measurements remain pending. Evidence is retained in the dated investigation note.
