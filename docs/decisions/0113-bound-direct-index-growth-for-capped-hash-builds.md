# ADR-0113: Avoid provably unusable direct build state

- **Status:** accepted; locally validated, not included in the frozen remote recovery board
- **Date:** 2026-09-13
- **Spec:** §11, §12, §13, §16, §18
- **Depends on:** [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md), [ADR-0036](0036-share-physical-key-contracts-across-consumers.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

The wider-domain duplicate-join benchmark allocates about 1.63 GB per operation versus roughly 16 MB in its dense
domain. Allocation profiling identifies direct-index heads and duplicate metadata growing before conversion to
the hash representation. Input row counts, multiplicities, key widths and result checks are unchanged.

Both explicit direct-range admission and capped-hash admission can initialize the same direct index. Capping the
initial hash allocation is not evidence that the direct representation can hold the input. A capped buffered build
can expand sparse direct arrays before reaching an unrepresentable key already present in its first batch.

Applying the explicit/streaming density budget to all capped builds was evaluated and declined. It reduced sparse
component duration and allocation substantially but increased warmed q09 CPU by about3% in both a combined candidate
and an admission-only diagnostic. It rejected valid direct maps for existing inputs, including a roughly five-million
row partition with a first key near46million. The initial capped-build path intentionally tolerated a wider domain;
the narrower budget was not a behavior-preserving gap fix.

## Decision

Separate the initial hash-size decision from speculative direct construction. A bounded sample may reject direct
construction only when a selected, non-null key proves the representation cannot hold the build: a negative key or
one at or beyond the existing absolute ceiling. Keep hash capping itself and valid direct-domain admission unchanged.
No witness in the sample is not a proof of compatibility; ordinary insertion retains exact fallback for later keys.

Compressed key-only batch insertion must also honor its existing density budget and notice when an earlier row in
the batch has already converted to hash state. It must never continue through released direct arrays. Both insertion
paths use the same key-admission predicate and preserve duplicate counts/order, nulls, selections and ownership.
No new threshold, switch, query recognition or host-planning change is introduced.

## Validation required

Reproduce unnecessary pooled-array allocation with a small duplicate build containing a large gap and a sampled
unrepresentable key. Assert
exact residual-filter results, absent/null/negative-key behavior and a bounded primitive-array allocation budget.
Check dense domains and wider-domain component measurements, then exact warmed parent/candidate/parent SQL and
neighboring join shapes. Run focused and full tests. Keep the current remote recovery campaign frozen; this
candidate cannot be attributed to or inserted into that sweep.

The family audit also covers compressed key-only batch loops: selecting that loop at batch entry does not prove
that every later key remains inside the admitted range, or that a previous row did not already force hash fallback.
Test a duplicate prefix followed by large gaps, negative keys, valid small keys, selected-out positions and nulls in
the same later batch. Both ordinary and compressed insertion must respect the same budget and exact fallback.

## Results

The allocation regression reproduces 12,721,316 pooled-array bytes for sixteen build rows; the accepted change
passes the 1 MiB bound with identical residual-join results. A sufficiently long duplicate prefix also reproduces
a null dereference after within-batch fallback in the compressed default path. The corrected dense and selected-row
cases preserve exact multiplicity. The full suite passes 2,160 tests, zero failures/errors, 394 skipped.

Two-fork component measurements at 0%, 10% and 100% matching probes reduce wide-domain duration to
0.157×, 0.233× and 0.499×, and allocation to approximately 0.009–0.010×. Dense-domain duration remains
0.991–0.998× with overlapping ranges and unchanged allocation.

Thirty-warmup, nine-measurement parent/candidate/parent SQL checks find no established CPU regression:
q09 medians are 5.700/5.703/5.788 core-seconds, q21 6.400/6.389/6.512, and q72 4.098/4.111/4.147.
The candidate ranges overlap both controls. These standard-query checks establish breadth safety, not a SQL
speedup. New Engine Coverage dense/wide residual-join probes still require SQL execution and duration qualification.
They do not change the current frozen board inventory or establish an end-to-end improvement yet.
