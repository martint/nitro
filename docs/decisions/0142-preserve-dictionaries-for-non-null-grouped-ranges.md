# ADR-0142: Preserve dictionaries for non-null grouped ranges

- **Status:** rejected; production candidate removed
- **Date:** 2026-09-14
- **Spec:** §5–6, §12, §16–18
- **Related:** ADR-0012, ADR-0036

## Context and correlation

Grouped binary output currently rejects dictionary representation for an entire field once any admitted group is
null. That protects the encoded-domain invariant: a dictionary VALUES stream and independently row-indexed nullable
NULLS stream must not be consumed as one physical domain. However, an individual output range can contain no null
groups even when another range does. Flattening such a range discards an already available repeated-value domain.

The bounded experiment proves null absence over the actual selected output range, alongside the existing exact
record-ID and dictionary-size checks. Ranges containing even one selected null still use the current flat output.
No nullable dictionary or shared ownership of sibling mappings is introduced. In particular, the replacement-only
`DictionaryVector.ownedMappingWithValues` contract is not used to publish independently owned sibling streams.

This refines the conservative empirical rule that grouped fields with any null stay flat, while preserving its
underlying value/null mapping invariant. Hashing, equality, group order, source plans, and dictionary admission
thresholds are unchanged. Temporary pooled mappings must be released on every rejected output attempt.

## Qualification

Test non-null slices of nullable fields, selected masks excluding and including null groups, all-null ranges,
out-of-range records, repeated rejection without allocation growth, and independently retained prior output.
Existing nullable full-range output must remain flat. Compare warmed grouping-heavy SQL and adverse mixed-null
output; abandon the extension if checking rejected ranges costs more than preserved domains save.

## Outcome

The candidate passes 2,191 tests with no failures or errors and 394 skips, including selected non-null ranges,
nullable/missing ranges, independently retained prior output, and repeated rejection without allocation growth
after pool warmup. The isolated six-query TPC-DS old/new/old cohort uses 15 warmups and 7 measurements per query.

q22 CPU medians are 14.077/14.486/14.466 seconds. The apparent increase against the leading control does not
repeat against the trailing control; dictionary counters are unchanged. q67 does expose about 10.3 million more
dictionary channel positions over seven measured runs, but CPU medians of 13.351/13.388/13.168 seconds overlap,
and the low-reuse exchange rejection counter increases. The other four queries show no repeatable CPU benefit.
Changing an upstream representation alone does not establish cheaper downstream consumption.

Remove the range admission and its dedicated tests. Keep the nullable-output contract unchanged; no dormant path,
enablement flag, or current-spec revision remains. A future proposal would need an end-to-end encoded-consumption
benefit, not just evidence that an intermediate dictionary survives. The independent missing temporary-mapping
release found during this work is a correctness cleanup, not evidence for accepting the representation change.

Evidence: `results/grouped-range-qualification-20260914`, `grouped-range-final-full-tests-r2-20260914.log`,
`grouped-range-q{22,67}-first-counter-comparison-20260914.txt`, and retained candidate patch in the notes archive.
Neither the frozen full board nor the separate host-boundary cohort includes this experiment.
