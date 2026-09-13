# ADR-0131: Qualify primitive evaluator traversal with error containment

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §6, §8, §11, §16, §18
- **Related:** accepted ADR-0041, rejected ADR-0123, proposed ADR-0128/0129

## Evidence and reconsideration

Fresh SQL forks exhibit large allocation bands despite identical logical work and allocator counters. The completed
build-bounds candidate without binding reuse also enters the higher band: remote q242 allocates 171.926 GB versus
133.546 GB for its earlier no-cache control. Binding reuse therefore is not established as the sole cause. Allocation
profiles identify boxed mask positions in evaluator copies, but instrumentation changes the observed allocation state
and does not explain every unprofiled difference.

ADR-0123 rejected broad traversal changes paired with removal of mapped-error scratch. ADR-0128 also records a failed
narrow traversal-only trial. Those results remain valid. This proposal independently tests primitive position traversal
throughout PlanEvaluator while preserving every current error buffer and the selected-error correctness correction.
It does not reinstate scratch removal or claim that primitive traversal alone is already performance-qualified.

## Decision

Use the Mask iterator's primitive nextInt contract in existing position loops. Keep loop bodies, selected positions,
evaluation order, null and error propagation, encoding admission, allocation scopes and function capabilities unchanged.
There is no alternate execution flag. This conforms to the selected-row contract and row-proportional allocation goal;
it is an empirical reconsideration of a previously unsuccessful implementation approach, not a changed semantic rule.

## Qualification

Retain dense/sparse and successive-demand tests, including diagnostic preservation beyond cached Integer positions.
Run the full Nitro suite. Freeze the candidate separately and compare q242/q243 against the no-cache completed-bounds
runtime locally and remotely, with exact results and native-plan checks. Require macro guards and inspect fresh-fork
variation before attributing timing deltas. Reject and remove the production edit if it does not qualify; keep evidence
and this decision record. Do not mix these diagnostics into the frozen headline board.

## Acceptance evidence and limits

The full 2,173-test Nitro suite passes, including selected diagnostic preservation. Remote q242/q243 CPU ratios
against the completed-bounds control are 0.7829/0.6848; allocation ratios are 0.7757/0.7519. All selected remote
macro/native-result guards complete. Most macro CPU ranges overlap; small separated movements occur in both directions.

The first fresh local pair does not establish a standalone win: q242 ranges overlap, and q243 is 5.4% higher CPU
with overlapping allocation ranges. Keep that contrary evidence. After ADR-0132's independent lifecycle correction,
removing primitive traversal adds about 19 GB allocation per diagnostic query and increases CPU by 5.9%/8.8%;
a fresh combined fork also improves both CPU and allocation. This supports retaining primitive traversal in the
qualified recovery stack, not claiming a uniform speedup from one earlier fork. The old rejected scratch-removal
experiments remain rejected. Further profiling locates remaining boxing in delegated vector masked copies, which
this evaluator-only change does not alter. No globally allocation-free claim or new headline board is implied.
