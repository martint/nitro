# ADR-0158: Lower the bounded payload build admission floor

- **Status:** accepted; bounded CPU benefit with an explicit peak-memory cost
- **Date:** 2026-09-15
- **Spec:** §11, §13, §16–18
- **Related:** ADR-0027, ADR-0036, ADR-0113
- **Evidence:** `~/notes/nitro/2026-09-15-payload-range-admission.md`

## Context and correlation

A buffered payload build chooses its initial representation using immutable policy and bounded physical evidence.
The existing five-million-row floor admits a bounded nonnegative domain to capped/direct construction, while a
slightly smaller otherwise equivalent build starts with the presized hash table. Diagnostics found nearly equal
partitions at 5,000,553, 4,999,574 and 4,999,873 rows taking different paths over the same absolute key range.

This decision explicitly refines a physical default; it is not a behavior-preserving cleanup. It conforms to the
construction-owned policy, exact fallback and allocator contracts, and preserves ADR-0113's distinction between
an impossible representation and a valid but sparse direct domain. ADR-0027 requires adverse-shape and broader
qualification before acceptance. No previous accepted decision is silently rewritten.

## Decision

Lower only the existing bounded-payload expected-row floor from 5,000,000 to 4,000,000. Keep sample size,
distinctness admission, key-only behavior, the absolute direct-key ceiling, duplicate semantics, later exact
fallback, dynamic-filter publication, pooling and host scheduling unchanged. No query, table or logical-type
recognition and no new enabling property are introduced. The standalone property factory remains a composition
adapter; operators receive the immutable policy as before.

This moves an empirically investigated cost boundary; it does not remove all representation crossovers. The new
boundary requires coverage too. Tests exercise both sides of it, both sides of the former boundary, key-only
exclusion and out-of-range unique samples. Existing tests retain selected/null-key and within-batch fallback checks.

## Tradeoffs and qualification

The local default/lower-floor/default bracket reduces q09 CPU from 5.811 / 5.718 control seconds to 5.431 seconds;
ranges are separate, while neighboring q07/q13/q21 CPU and all latency ranges overlap. The orders build pipeline
accounts for part of the saving. Input rows and bytes match. Allocation and peak-memory central values increase,
although local ranges overlap.

The remote bracket is 17.920 / 16.268 / 17.322 CPU-seconds, with candidate ranges separate from both controls.
Peak reservation is 1.453 / 1.667 / 1.442 GB, also with separate ranges; allocation is 12.192 / 12.490 / 12.257 GB
with overlapping ranges. This is a real co-tenancy tradeoff, not a memory optimization.
These diagnostic cells do not replace the accepted board or establish a broad-suite improvement.

Focused tests pass 182 cases. The full suite passes 2,198 tests, zero failures/errors, 394 skipped. The frozen
candidate differs from the accepted runtime in only the policy class. Normal-flag qualification covers all 22
TPC-H queries plus TPC-DS q16/q72, ClickBench q39 and Engine Coverage b06, with matched parent/candidate/parent
histories. Every point passes in every arm. q09 CPU is 18.281 / 17.016 / 17.729 seconds with separate candidate
ranges, and peak reservation is 1.451 / 1.671 / 1.463 GB, also separate.

A longer normal-default candidate/parent/candidate repeat uses 40 warmups and 15 measurements. q09 CPU is
16.241 / 17.073 / 16.294 seconds: both candidates improve with separate ranges. Peak reservation is
1.665 / 1.445 / 1.658 GB. Allocation medians increase about 2.3%, with overlapping ranges. Latency improves in
central value but not with separate ranges in every bracket. Input rows and bytes match.

## Acceptance scope and unresolved neighboring variability

Retain the default because the targeted construction benefit repeats in every bracket and correctness/admission
boundaries pass, while explicitly accepting the larger direct-map footprint. This is not an all-metric or
all-query improvement. The default remains bounded by the existing physical domain ceiling and exact fallback;
there is no enabling flag or parallel dormant implementation.

Some neighboring CPU warnings are not reproducible across forks. In the broad cohort q22 is 2.096 / 2.172 / 2.117
seconds; in the reverse repeat it is 2.151 / 2.097 / 2.079 seconds. Reverse q21 is 21.542 / 20.611 / 20.934 seconds.
The first reverse candidates have separate higher ranges, while the second candidates overlap their control.
This does not prove those earlier losses were noise or that this default is their cause. Retain them as explicit
guards in the next synchronized cohort; q21/q22 are not declared fixed by this decision. Future consistent adverse
evidence requires revisiting this decision rather than selecting favorable forks.

The accepted performance board is unchanged until a synchronized run includes this code. The null-accessor
candidate in ADR-0159 is separately qualified and is not part of this decision's implementation or evidence.
