# ADR-0154: Reject unreused ordered egress domains before compaction

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §11, §15, §18
- **Related:** ADR-0108, ADR-0141, ADR-0152

## Context and correlation

The host egress adapter compacts an oversized dictionary domain before applying its encoding policy. Compaction
sorts packed position/ID pairs and allocates temporary and output arrays even when the resulting dictionary is
rejected. A strictly increasing selected ID sequence proves that every selected ID is distinct without sorting.

An external, query-scoped counter capture observed 38,306,292 such positions out of 53,040,000 positions sent to
compaction over two q22 executions. All 4,001 corresponding compactions were followed by rejection. q67 did not
show this pattern. These instrumented counts are not performance measurements.

This fills SPEC §11/§15's computation-reuse and explicit-boundary efficiency contract. It preserves the empirical
rule that mapping analysis is shared across sibling value columns. It does not change dictionary admission,
partitioning, buffering, host scheduling, logical types, or ownership. It is independent of the pending partition
pool-reuse decision and does not alter the frozen runtime being measured remotely.

## Proposal

For an oversized domain in compact mode, recognize a strictly increasing selected ID sequence and apply the existing
policy to the exact selected distinct count before constructing the compact mapping. Cache the most recent mapping's
ordering proof within the existing batch-owned egress context. Do not cache the policy result or infer equality of
logical values from different IDs. A new selection receives a new context and proof scope.

Unproven mappings use the unchanged compactor. If a custom policy preserves a unique mapping, its normal downstream
compaction and output behavior remain available. Full-physical-domain mode retains its existing admission semantics.

## Qualification

Guard increasing IDs with gaps, duplicates, descending IDs, sparse selection, nested dictionaries, mixed nulls,
multiple sibling columns and changed selection contexts. Include custom policies that preserve unique domains and
policies that inspect exact counts. Semantic tests should pass on the parent too; they guard a performance-only
change. Component allocation and work-count evidence must prove the skipped work actually disappears.

Compare complete adapter output, not merely the ordering check, across favorable and unfavorable mappings. Run
warmed q22/q67 and ClickBench neighbors with frozen parent/candidate binaries, then full correctness qualification.
Retain no opt-in flag or unqualified experimental production path.

## Outcome

Accept the exact ordering proof without changing admission. Full Nitro tests pass (2,197, zero failures/errors,
394 skipped), as do 652 eligible host tests and all 409 remote exact-result/native-execution checks.

Warmed q22 parent/candidate/parent CPU medians are 13.635 / 13.206 / 13.963 seconds; allocation is
20.828 / 20.419 / 20.859 GB. Candidate CPU and allocation ranges are below both controls; source positions and
bytes are identical. Latency overlaps the first control. External work counts confirm that compaction positions
fall from 53.04 million to 12.49 million across two executions; q67 compaction is unchanged.

q67 and q39 guards overlap. The initial q31 CPU penalty did not reproduce in a longer reverse bracket:
candidate/parent/candidate CPU 4.796 / 4.796 / 4.783 seconds, with overlapping ranges. This does not prove every
small effect is zero. Increasing-mapping component elapsed-time ratios are 0.363–0.877 and allocation ratios are
0.376–0.505. A late-duplicate BIGINT mapping pays up to 2.9% extra component elapsed time for a failed proof;
accept this bounded cost explicitly rather than describing the change as universally faster.

The optional full-module error-prone build has the same first 100 diagnostics on parent and candidate and is not
claimed to pass. An uncapped targeted compilation of all three changed Java files using the captured configuration
passes. Its new reference-equality warning describes the intentional immutable-array identity cache; no check is
disabled. Existing full-module static cleanup remains separate work.

Evidence: `2026-09-14-ordered-egress-proof-qualification.md`, `results/ordered-egress-proof-20260914`,
`results/ordered-egress-q31-recheck-20260914`, and `results/ordered-egress-proof-functional-20260914` under Nitro notes.
The completed accepted performance board predates this change; do not relabel or replace its cells with diagnostics.
