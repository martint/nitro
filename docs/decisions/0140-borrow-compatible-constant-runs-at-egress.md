# ADR-0140: Borrow compatible constant runs at synchronous egress

- **Status:** rejected
- **Date:** 2026-09-14
- **Spec:** §5–6, §15–18
- **Related:** ADR-0007, ADR-0008, ADR-0098, ADR-0106

## Context and contract correlation

The synchronous non-retaining exchange path currently requires every output channel to expose compatible flat
storage. A single constant RLE channel therefore rejects a batch whose other large flat columns could be borrowed.
The owning path copies those columns before destination append. Current ClickBench q39 samples show boundary copy
work and an RLE companion column, but do not yet establish that changing admission improves complete execution.

The experiment extends the supported physical representations in ADR-0098, not its all-channel semantic proof or
lifetime contract. A single-run non-null input may expose its compatible one-value base through a temporary host
RLE block. Multi-run values, uncertain null shape, logical normalization and incompatible carriers retain owning
adaptation. The source remains live through the existing synchronous callback. No persistent accumulator, scheduler,
partitioning, serialization or host buffering change is proposed.

## Required evidence

Tests must verify dense/sparse selected values, actual base-array borrowing, rejection of mixed null and multi-run
inputs, normalization, retaining consumers and destination independence after source reuse. The destination must
not retain any transitive RLE base; callback return alone is not proof. Compare warmed q39 plus other affected query
shapes and allocation, row/byte counts and flush behavior. Immediate encoding flush or RLE expansion can outweigh
saved copies, so reject and remove the implementation if complete SQL evidence is neutral or regressing.

## Qualification and disposition

The candidate passes 642 host tests, including actual flat/RLE array borrowing, normalization, mixed-null rejection,
retaining consumers and destination independence after source mutation. The initial parent test fails as expected.

Local warmed q14/q39 parent/candidate/parent measurements are neutral with overlapping ranges. More importantly,
both parent and candidate report zero borrowed batches: other incompatible columns still reject the complete
batch. This does not establish that borrowing constants is intrinsically slower; it establishes that the narrow
extension does not remove the measured boundary obstruction. No SQL performance benefit justifies retaining it.

The implementation and its new tests are removed. Existing single-run conversion and ownership tests remain.
Evidence and patch are retained under the September 14 RLE-borrow qualification campaign. The frozen full board
never included the candidate. The current specification and ADR-0098 contract remain unchanged.
