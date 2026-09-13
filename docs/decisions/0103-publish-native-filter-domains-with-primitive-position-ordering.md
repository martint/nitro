# ADR-0103: Publish native filter domains with primitive position ordering

- **Status:** accepted
- **Adopted by:** [ADR-0104](0104-integrate-correlated-filtering-without-an-opt-in.md).
- **Date:** 2026-09-12
- **Spec:** §13, §17, §18
- **Related:** ADR-0027, ADR-0101

## Context

Exact host-filter publication boxes position indexes and comparator-sorts them. q17's construction counters show
identical physical spans and scalar cardinality with correlation enabled or disabled, while profiles identify scalar
publication as a dominant cost. Reducing publication overhead can recover CPU without weakening filters or changing
the host scheduler.

## Proposed decision

Allow exact-value publication to receive a caller-supplied Block value-set factory. The ordinary host builder retains its
existing behavior. The Nitro collector writes flat entries directly into an independently owned Block using the host's
registered FLAT-to-BLOCK_BUILDER convention, then stably orders primitive position indexes through the supplied host type
operators, then constructs the same discrete range representation through an explicit ordered-position API. No physical
carrier is assumed to define logical ordering, and no function/type/query-specific cases are introduced.
This removes the flat-to-boxed-list-to-Block round trip. Non-orderable types retain the existing exact-set representation.
The existing collector excludes nulls. The bound unordered-last comparator puts top-level NaNs at the end; publication
removes that suffix with the host's NaN predicate before constructing ranges. Nested NaNs are not top-level NaNs and
must retain their type semantics. Budgets, Bloom admission, partition completion, transport, and scalar retained-size
accounting remain unchanged.

The ordered-position API accepts positions selecting immutable non-null, non-NaN values in nondecreasing logical
order, validates position bounds/order/nulls, and removes duplicates. It does not retain the caller's mutable index
array or construct an intermediate dictionary only to unwrap it. It is a host-domain boundary capability, not an
alternate function implementation.

## Validation gate

Check semantic equivalence and retained representation across primitive, decimal, variable-width, and nested types;
ordinary host builder compatibility; native lifecycle; and full functional coverage. Benchmark q17/q72 against the
pinned pre-change candidate, retain feature-off comparisons, and check the complete multi-key construction surface
before accepting either this change or correlated-filter default enablement. A faster component does not close the
experiment. This fills a publication-adapter gap within existing semantic and policy contracts.
