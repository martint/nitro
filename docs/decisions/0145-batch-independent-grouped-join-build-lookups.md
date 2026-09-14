# ADR-0145: Batch independent grouped join build lookups

- **Status:** rejected; production candidate removed
- **Date:** 2026-09-14
- **Spec:** §11, §13, §16, §18
- **Related:** ADR-0018, ADR-0036, ADR-0105, ADR-0139; rejected ADR-0144

## Motivation and correlation

Current q09 lookup samples attribute more work to build insertion than ordinary single-reference probing. External
component tests separate initial tag access from insertion and improve complete grouped-table build time. The
smaller pooled case improves about 17%; larger pooled results have wider uncertainty. This justifies an integration
trial, not a SQL performance claim. The physical representation, hashing and equality remain unchanged.

## Proposed execution

Prepare grouped-table insertion hints in bounded batches, then validate them against current storage and earlier
inserts. An occupied hint requires exact key equality; collisions use the authoritative lookup. Growth, release
and reallocation invalidate hints through a storage generation. Do not change existing growth timing or duplicate
FIFO order. Direct/sequence builders and non-grouped tables retain their current paths.

Build scratch belongs to the mutable build owner and injected pool, is charged while live, and is released before
publishing a prepared build. Prepared probes do not share it. Immutable construction policy initially selects
256-row chunks and a 32,768-slot capacity floor, with no enabling property. Masks and nulls govern input staging;
row references retain the existing logical/physical-position contract in both build entry points.

This conforms to the spec's mutable-table batching, allocation and exact-key contracts. It extends physical
admission without changing SQL semantics, function/type knowledge, dynamic-filter publication, islands or scheduling.
The accepted spec is unchanged pending qualification.

## Required qualification

Cover collisions and duplicates within a batch, full-width keys, stale hints across growth and same-capacity reuse,
sparse/null selected inputs, both reference mappings, allocator accounting/release and prepared-build lifetime.
Run focused and full tests plus frozen parent/candidate/parent q09/q21 and neighboring SQL checks. Reject and remove
the production trial if component benefits do not survive the complete execution path.

Evidence: `2026-09-14-grouped-join-build-frontier.md` and its component sources/results.

## Qualification and decision

The integrated candidate passes 2,195 tests with zero failures/errors and 394 skips, including stale hints, growth,
pooled scratch and selected/null duplicate input. Frozen parent/candidate/parent SQL runs use 40 warmups and 15
measurements, with identical topology, data, heap and unaffected host/SPI binaries.

| Query | Parent A CPU, seconds | Candidate | Parent C | Decision |
| --- | ---: | ---: | ---: | --- |
| TPC-H q09 | 5.593 | 5.605 | 5.582 | No gain; ranges overlap |
| TPC-H q13 | 2.830 | 2.947 | 2.855 | 3.2–4.1% worse; ranges separate |
| TPC-H q21 | 6.428 | 6.530 | 6.347 | No gain; worse than the second control |

A separate instrumented execution confirms only 244,768 hinted build rows versus 15,381,653 scalar rows in q09.
The hint fallback count is 30,393. q13 and q21 perform no hinted lookups: their 1,500,000 and 10,232,378 observed
single-key build rows use existing paths. Moving shared insertion bookkeeping into helper methods therefore also
exposes non-admitted execution to compilation/code-shape changes. The experiment does not establish a benefit in
the paths responsible for these SQL regressions, and worsens a previously recovered query.

Remove the implementation, new policy and trial tests. Preserve the exact patch, frozen runtime and results outside
the repository; no enabling flag or dormant experiment remains. Accepted hash-table, build and probe contracts are
unchanged. A future attempt must first establish material admission on an actual target workload and protect the
non-admitted path, rather than extrapolating the fixed-capacity component result to complete SQL execution.

Evidence: `results/grouped-join-build-20260914`, `results/grouped-join-build-work-20260914`,
`grouped-join-build-rejected-candidate-20260914.patch`.
