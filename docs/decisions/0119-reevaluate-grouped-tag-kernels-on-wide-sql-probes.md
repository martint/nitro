# ADR-0119: Reevaluate grouped tag kernels on wide SQL probes

- **Status:** rejected; the remote target query regressed
- **Date:** 2026-09-13
- **Spec:** §12, §13, §18
- **Related:** [ADR-0112](0112-probe-grouped-join-tags-with-packed-word-masks.md)

## New evidence

ADR0112 rejected packed-word tag matching after neutral component and q09 results on the local machine. The new
Engine Coverage q243 exercises 1.2 billion probes against three million sparse build keys. On the remote benchmark
CPU, 20.3% of sampled Nitro driver CPU is in Byte128Mask.toLong, with another3.6% in vector loads. The dense q242
control does not exhibit the same cost. This is a new workload/hardware qualification context, not a reason to erase
the earlier negative result or an assertion that an intrinsic's samples equal its replacement's savings.

## Proposal and correlation

Reevaluate the same two-word tag kernel, retaining table geometry, tags, key verification, duplicate handling and
growth unchanged. Conforms to ADR0036's shared exact-key contract and ADR0011's physical specialization constraint.
Do not add an opt-in, query identity, function semantics, planner change, or scheduler change. Keep both outcomes
recorded; accept only new measured benefit without neighbor regressions, otherwise remove the candidate again.

## Gate

Retain the independent collision, sign, growth and dirty-reuse tests; run the full Nitro suite. Compare frozen
parent/candidate SQL on the remote host with matching warmups, heap, topology and inputs, including dense q242 and
wide q243. Preserve exact results and complete native-island checks. Repeat the local q09/q21 regression guards.
Frozen binary snapshots and hashes identify variants independently of ongoing local source development.

## Outcome

The first remote W5/M5 parent/candidate pair passes exact-result/native-execution checks, but q243 regresses:
CPU median384.915→435.923seconds (1.1325x), latency60.919→67.913seconds (1.1148x). Measured ranges are
separated for both. q242 improves CPU186.237→176.227seconds (0.9463x) and latency30.375→28.900seconds
(0.9514x); that does not compensate for the targeted wide-key regression. The final parent repeat continues as a
drift check, not a reason to keep an unsafe default until it completes.

Local q09 W30/M9 is neutral with overlap. The longer q21 W60/M15 parent/candidate/parent resolves the earlier
apparent local regression: CPU6.428/6.472/6.449seconds, latency1.534/1.515/1.545seconds, overlapping ranges.
Candidate full tests pass 2160 tests; the subsequent independent scalar benchmark/test raises the total to 2161.

Remove the prototype at its introducing mutable commit; retain tests and external frozen snapshots/evidence.
There is no new policy or opt-in. Profile samples identify investigation targets, not the benefit of a proposed
replacement. In particular, replacing a hot intrinsic can move work elsewhere or alter compiled code adversely.
The exact cause of this remote regression is not established by the timing result alone.

Evidence: `grouped-tag-remote-first-pair-20260913.txt`, remote `grouped-tag-sql-20260913-*` runs, and local
`grouped-tag-q21-long-parent-{a,c}-comparison-20260913.txt`, under the Nitro notes evidence root.
