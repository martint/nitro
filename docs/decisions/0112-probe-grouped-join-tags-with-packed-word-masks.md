# ADR-0112: Probe grouped join tags with packed-word masks

- **Status:** rejected; production implementation removed, regression tests retained
- **Date:** 2026-09-13
- **Spec:** §12, §13, §16, §18
- **Depends on:** [ADR-0036](0036-share-physical-key-contracts-across-consumers.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

The current q09 CPU capture still attributes appreciable work to grouped long-key join lookup and Vector API
mask extraction, across both construction and probing. Previous reduction-first and SWAR experiments in other
physical layouts have regressed. A hot intrinsic does not prove its replacement will improve the whole query.

The current table already uses fixed sixteen-byte control groups. Occupied tags always have their high bit set;
empty bytes are zero. Preserving that representation permits testing a different control-byte kernel without
changing table geometry, load factor, hash function, physical admission, duplicate handling, or growth.

## Evaluated proposal

Read each group as two little-endian words. Use byte-parallel zero detection after XOR with the repeated target tag
to identify key-verification candidates. This detection can admit a neighboring false positive through subtraction
borrow; exact key comparison remains authoritative. Derive empty-byte flags exactly from the occupancy high bits.
Visit candidates and empty slots in the same order as the previous sixteen-lane implementation.

Use only fixed intrinsic constants and existing allocator-owned storage. Add no policy switch, per-query heuristic,
logical-type knowledge, scratch buffers, planner change, or scheduling change. The proposal conforms to the existing
exact physical-key contract; prior negative results remain evidence against accepting an unmeasured replacement.

## Validation required

Check present and absent keys, collisions across both word halves and group boundaries, signed extreme keys,
duplicates, growth, and dirty pooled reuse against an independent reference. Run shared index and join tests and
the full suite. Compare exact local parent/candidate/parent SQL CPU, latency, allocation, and hardware counters,
plus neighboring join shapes. Remove the implementation if the complete cost does not improve. Remote qualification
must wait until the currently frozen recovery sweep finishes; do not alter that runtime.

## Outcome

The candidate passed 2,157 tests (zero failures/errors, 394 skipped), including the new collision, growth and reuse
coverage. A two-fork wide-domain component screen was neutral: 198.511 versus 196.092 ms/op with overlapping JMH
confidence intervals and unchanged allocation. Local q09 W30/M9 parent/candidate/parent CPU medians were
5.684 / 5.674 / 5.689 seconds; both comparisons have overlapping CPU, latency, allocation and peak ranges.

Retain the existing Vector API control kernel. The packed-word implementation is removed, not kept behind a switch.
Keep the independent reference and collision tests and the rejected patch as investigation evidence. A sampled
mask-conversion leaf did not establish that replacing its implementation would reduce end-to-end work. The current
remote recovery campaign never contained this candidate and its results must not be attributed to it.
