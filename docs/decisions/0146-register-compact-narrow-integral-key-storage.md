# ADR-0146: Register compact narrow integral key storage

- **Status:** rejected
- **Date:** 2026-09-14
- **Spec:** §5, §7, §12, §13, §16, §18
- **Related:** ADR-0006, ADR-0011, ADR-0036

## Motivation and correlation

The host type provider publishes signed-four-byte flat-key storage for INTEGER and DATE, but not SMALLINT or
TINYINT. Generic flat tables therefore store those narrower logical domains in eight-byte fields. This is a
capability-binding omission, not a reason for engine operators to recognize more type names or infer semantics
from an I64 carrier. q39's composite grouping includes three such fields, making record footprint worth testing.

## Evaluated change

Publish the existing signed-int storage implementation for SMALLINT and TINYINT too. Its representable domain
contains both logical domains exactly. Reuse the already-supported four-byte reader/writer and hash/equality
contracts; do not add per-type interfaces, new physical kernels, sampled range heuristics, query rules or a flag.
Leave BIGINT and other long-carrier domains unchanged. This conforms to provider-owned semantics and shared
physical-key capabilities, filling a missing provider declaration. One-/two-byte storage is not part of this trial.

## Qualification and decision

Test exact logical endpoints, negative values, an unaligned record offset, neighboring bytes and signed-int
storage overflow rejection. Retain full engine and host integration checks. Compare frozen parent/candidate/parent
ClickBench q14/q39 plus nearby mixed-key and TPC-DS guards; never substitute point results into the active board.
Reject and remove the trial if the smaller records do not improve complete execution without material regressions.
Accepted specification and empirical principles change only after qualification.

The provider and its endpoint/offset guards passed 12 focused and 642 eligible host tests. A short frozen
parent/candidate/parent ClickBench q14/q39/q41 trial (40 warmups, 20 measurements) showed only overlapping small
q39 gains and worse q41 CPU. A longer independent trial (80 warmups, 60 measurements) did not reproduce a gain:
q39 CPU medians were 680, 704, and 685 ms for parent A, candidate, and parent C. The candidate is 3.6% and 2.8%
worse respectively; its range separates from parent A but overlaps parent C. q14 is neutral, and q41 varies across
the parent runs. No consistent allocation or latency benefit establishes a compensating improvement.

Reject the candidate and restore the provider and tests in full. Smaller field width alone does not prove a faster
compiled lookup/storage path, and this evidence does not identify which generated or adaptive path explains the
cost. No flag, alternative implementation, or accepted contract change remains. The narrower logical domains may
be revisited only with a new hypothesis and complete-path evidence, not by reporting the shorter trial alone.

Evidence: `~/notes/nitro/2026-09-14-narrow-integral-storage-qualification.md`,
`~/notes/nitro/results/narrow-integral-storage{,-long}-20260914`, and the retained rejected candidate patch.
