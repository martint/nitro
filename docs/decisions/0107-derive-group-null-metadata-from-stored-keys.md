# ADR-0107: Derive group null metadata from stored keys

- **Status:** rejected; production implementation removed, behavior coverage retained
- **Date:** 2026-09-13
- **Spec:** §12, §13, §16
- **Depends on:** [ADR-0036](0036-share-physical-key-contracts-across-consumers.md),
  [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

A flat grouping table records whether any stored group has a null field, so grouped outputs can omit all-false
null streams. Insertion separately decodes input null positions for this metadata even though writing the key
already resolves the same null semantics. Specialized layouts do not all share the base layout's batch-bound
accessors. A ClickBench q31 profile attributes substantial grouping work to repeated RLE null-position lookup
through this metadata path.

## Evaluated approach

After storing an inserted key, derive field-null presence from the table's existing stored-key null contract.
That contract handles both ordinary key records and normalized identities. Once a field is known to contain
a null, no further reads are necessary for its monotonic presence flag. Do not inspect logical types, function
names, query shapes, or concrete layout classes to select this behavior.

This fills a redundant-work gap within the existing authoritative-key contract. It does not change equality,
hashing, group identity, record representation, masks, or scheduler behavior.

## Validation before acceptance

Check null-free-to-nullable batch transitions, RLE/dictionary null streams, normalized and ordinary records,
and the shared table consumers. Run focused and full tests, an exact warmed parent/candidate/reverse comparison,
then neighboring workloads. Reject the candidate if its benefit is not reproducible or its breadth is unacceptable.

## Outcome

The first local candidate appeared 3–5% lower in CPU than its bracketing parents, but a repeated candidate
overlapped the parent range. The remote W30/M9 comparison was neutral: median CPU 13.031 versus 13.101 seconds,
ratio 0.9947, with overlapping ranges. Reusing the key writer's already-resolved chunk and offset in a second
implementation also measured neutral locally: 5.239 versus 5.241 seconds, ratio 0.9996, with overlapping ranges.
The complete candidate test suite passed (2,151 tests; zero failures/errors; 394 skipped), and neighboring
TPC-H q09/q13/q21 comparisons had overlapping CPU and latency ranges.

Remove both production variants rather than add an option or classify sampled work removal as a query win.
Retain the behavior assertions for ordinary/normalized record null metadata and encoded batch transitions.
The rejected patch and full measurements remain in the dated investigation artifacts outside the repository.
The existing input-derived metadata remains authoritative production behavior. This rejects these implementations
on current evidence, not every future way to eliminate repeated null work.

The remote isolated parent itself was materially faster than its earlier full-suite measurement. That difference
predates this candidate and must be investigated as warmup depth or suite-JVM context, not credited to this change.
