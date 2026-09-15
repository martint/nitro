# ADR-0162: Prove independent aggregate weight mappings

- **Status:** accepted; guard-query variability remains explicit
- **Date:** 2026-09-15
- **Spec:** §5, §12.1
- **Depends on:** [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0065](0065-reuse-domain-counts-after-equivalent-mapping-proof.md)

## Context

A weighted aggregate can receive values and weights in independently owned dictionaries. Their complete row-ID
sequences may be equal without sharing mapping identity. A q238 input probe found that the weighted percentile
provider rejected every batch, while most value batches had a sixteen-value domain. One inspected independent
weight mapping was exactly equivalent. Identity-only admission therefore warrants a measured broader proof.

## Decision

The percentile provider may use the existing exact row-mapping equivalence capability before combining dictionary
frequencies with per-domain weights. Identity remains the constant-time first check. Otherwise, equality of the
complete mapping length and every row ID is required. Matching domain cardinality or samples is not sufficient.
The provider retains its existing null, selection, percentile-parameter, multiplicity and portable-state contracts.
Incompatible mappings keep the logical-row implementation. No ownership is transferred or retagged by this proof.

This extends ADR-0065's explicit equivalence proof to companion weight alignment. Function semantics remain in the
registered aggregation implementation; the engine and evaluator acquire no knowledge of percentile or q238.

## Costs and qualification

The extra comparison is O(logical rows) when identity differs. It must avoid enough digest work to pay for itself.
All raw numeric loops retain their previous traversal. A simultaneous primitive-iterator rewrite did not establish
a consistent guard-query benefit and is not part of this decision. Removing it did not eliminate all between-JVM
variation; it must not be presented as a proven remedy for that variation.
Test independently equal and unequal mappings, selected masks, null weights and portable sketch output. Verify
actual admission with a separate diagnostic probe, then measure uninstrumented SQL with repeated controls and
coverage guards. Approximate sketches need the declared accuracy contract, not a universal insertion-order promise.

## Qualification and limits

Three independent distributed JVMs per version, each with 15 warmups and 10 measurements, compared the mapping-only
provider against unchanged controls in both execution orders. In the weighted low-cardinality coverage query,
median CPU across fork medians fell from 1.253 to 0.234 seconds and latency from 1.295 to 0.274 seconds. Allocation
fell from 77.832 to 75.850 MB. Source rows and bytes were identical in every measured execution. This is focused
qualification, not a synchronized macro-board result or a guarantee of allocation-free execution.

The other three percentile guards had overlapping fork-median ranges. Their central CPU ratios were 1.024, 1.018
and 1.076. The last guard's control range was 0.258–0.296 seconds and candidate range 0.291–0.316 seconds: this remains
a warning, not proof of no regression. The retained decision weighs a large repeatable reduction in digest work
against unresolved smaller guard variation; it does not claim universal speedup. Tiny peak reservations in these
queries are not meaningful evidence of process-memory or co-tenancy improvement.

Correctness tests cover independently equal and unequal mappings, null weights, dense and sparse masks, exact
total weight and portable sketch state. The 653-test eligible integration gate passes. A broader run has the same
12 failures and 40 errors on the unchanged parent and candidate; these existing planner/server test failures are
not waived as productization readiness.

## Alternatives

- Keep identity-only admission: correct, but can repeat expensive sketch transitions for every logical row.
- Canonicalize all source mappings: broader coordination and ownership change, unnecessary for this proof.
- Assume equal dictionary cardinalities imply alignment: incorrect for differently assigned IDs.
- Build a joint value/weight relation: potentially useful for genuinely unequal mappings, but separate work.
