# Adaptive-partial diagnostic controls

These controls investigate TPC-DS q47's three partial aggregations, where Trino contracts about 6.599 million rows to
1.068 million per pipeline while Nitro emits all 6.599 million rows. All valid runs enabled the performance-sweep
setup so the fixture executed `ANALYZE`, used three warmups and three measurements, a 12 GiB JVM, an 8 GiB query limit,
allocation and peak-memory accounting, and per-plan-node operator CPU.

The first no-prefix and 8x-horizon runs were made before the modified `trino-main` artifact was installed into the
local repository used by the standalone `trino-tests` module. They therefore ran the previous engine and are invalid
candidate measurements. They are retained only to document the harness failure.

After installing the exact core artifact, a diagnostic control forced aggregation on the planner-created adaptive
control. Q47 still emitted 6.599 million rows from each affected Nitro partial and measured 1.020x wall / 0.828x CPU.
This shows that the passthrough decision is governed by a different control instance propagated through a fused-plan
handoff, not by the visible planner-level instance changed by the control. The experiment was reverted.

The next investigation is controller identity and lifetime across fused source/join terminals, ordinary aggregation
factories, and exchange-side combiners. A decision based on source-local key order must not disable a distinct
physical stage that can contract keys across splits or partitions.
