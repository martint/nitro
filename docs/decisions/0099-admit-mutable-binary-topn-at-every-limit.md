# ADR-0099: Admit mutable binary TopN winner storage at every positive limit

- **Status:** accepted
- **Date:** 2026-09-11
- **Spec:** §9
- **Depends on:** [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Evidence:** `~/notes/nitro/2026-09-11-short-limit-binary-topn-admission.md`

## Context

TopN already has a physical mutable-slot representation for error-free binary ordering lanes, but the standalone
policy admitted it only when the result limit was at least 512. Below that threshold, selection retained and replaced
independently allocated one-position vector and stream trees. The threshold preserved a conservative rollout; it did
not describe a cost boundary established by evidence.

ClickBench q25 orders roughly 100 million input positions by one binary lane with a limit of ten. The retained path
made wrapper, ownership, and allocation work proportional to winner replacement and left Nitro slower than Cork.
Affected-plan inventory found 26 macro queries with sub-512 all-binary ordering tuples. Paired adaptive cohorts across
single- and multi-lane CHAR/VARCHAR keys, nullable values, long values, and wide payloads found no reproducible CPU
regression from mutable slots. Material wins include q25 and TPC-DS q21; TopN-owned allocation falls throughout the
affected paths. A separate mixed fixed/binary q22 experiment regressed CPU by `1.044x` despite reducing allocation,
so the all-binary admission must remain distinct from the mixed-layout policy.

## Decision

Admit mutable binary winner storage for every positive TopN result limit when the complete ordering tuple consists of
error-free binary lanes. Keep fixed-width, hybrid, structural, and error-carrying admission policies unchanged.

The decision remains physical and instance-owned. It does not inspect SQL types, functions, tables, columns, query
identity, or benchmark identity. Selection stores replaceable binary values in allocator-accounted slots and
materializes compact variable-width vectors only after final ordering.

## Consequences

- Small-limit binary TopN no longer pays one-position vector-tree replacement costs.
- Existing null ordering, trait intersection, allocator accounting, pool bounds, and output materialization contracts
  remain unchanged.
- Mixed and structural tuples retain their independently evidenced thresholds; this decision does not generalize one
  representation's cost curve to another.
- The broad macro board remains the regression guard for interactions outside the inventoried affected cohort.

## Alternatives considered

**Retain the limit-512 threshold.** Rejected because limit cardinality did not predict the observed cost and preserved
the q25 regression.

**Recognize q25 or `SearchPhrase`.** Rejected because query and logical-column identity are not physical admission
facts.

**Admit all dense ordering representations at every limit.** Rejected because fixed-width, hybrid, structural, and
error-carrying representations have different cost and semantic boundaries.
