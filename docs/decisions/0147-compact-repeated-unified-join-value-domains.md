# ADR-0147: Compact repeated unified join value domains

- **Status:** rejected
- **Date:** 2026-09-14
- **Spec:** §5, §13, §16, §18
- **Related:** ADR-0006, ADR-0012, ADR-0036

## Motivation and correlation

The multi-batch join output path unifies binary build payloads into an immutable dictionary with one entry per
build row. This saves repeated payload copies but can leave many byte-identical entries. In a q22 diagnostic,
three 102,000-entry domains contained only 713, 100 and 11 distinct byte values; a fourth was almost unique.
The large physical domains fail downstream dictionary-reuse admission and are subsequently flattened.

This is a physical domain-compaction gap, not a reason to recognize queries, functions, column combinations or
logical types. Exact byte identity permits shared storage without changing values. Null and error streams remain
in their existing logical-row domain; payload compaction does not evaluate a function or interpret null contents.

## Proposed change

Extend the existing bounded build-dictionary construction to the already-admitted unified binary domain. Reuse
its exact byte interner and cardinality limit, preserving the original unified domain when the limit is exceeded.
Compose output IDs through an allocator-owned build-position mapping only for successfully compacted domains.
Keep the unmodified row-offset loop for non-compacted output. Release rejected temporary mappings and replaced
storage; retained mappings and compact values belong to the join's build allocation context.

Do not change remote exchange admission, host partitioning, query plans, or the existing output-reuse threshold.
Do not add an enabling flag. This conforms to the existing encoded-domain and registry contracts rather than
superseding a semantic decision.

## Qualification

Cover repeated values across build batches, near-unique fallback, changed payload bytes, nullable streams,
multiple probe batches, exact multiplicities and output order, retained output lifetime, and allocator closure.
Run the focused and full engine gates. Qualify frozen parent/candidate/parent q22/q67 plus neighboring join and
binary-output queries before any remote promotion. Measure actual domain sizes and boundary work separately
from uninstrumented timing. Remove the candidate completely if complete-query benefit does not qualify.

## Outcome

The candidate passed 183 focused and 2,191 full-reactor tests, with zero failures/errors and 394 skips. Frozen
W20/M15 parent/candidate/parent q22 CPU medians were 13.791/14.101/14.052 seconds; q67 medians were
12.740/12.781/12.686 seconds. There is no qualified complete-query gain. q22's candidate range separates from
the first parent but overlaps the second; parent drift prevents attributing all of the difference to code.

A subsequent entry-count diagnostic confirms that neither query invoked buildUnifiedBinaryColumn or the extracted
createBuildDictionary helper. The observed large dictionary bases did not identify that producer path. The
HashJoinOperator transformation was active in both diagnostic JVMs, and q67 exercised retained output wrapping.
The repeated-value opportunity remains real, but the changed path does not address these target queries.

Reject and remove the entire implementation and its test. Do not retain an unmeasured representation merely
because it is logically valid. A future experiment must identify the exact producer/admission path before editing
it, including retained/mapped build positions and demand constraints. Native exchange admission is unchanged.

Evidence: `~/notes/nitro/2026-09-14-unified-join-domain-qualification.md`,
`~/notes/nitro/results/unified-domain-20260914`, and `~/notes/nitro/results/unified-domain-admission-20260914`.
