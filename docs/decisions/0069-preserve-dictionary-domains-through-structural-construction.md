# ADR-0069: Preserve dictionary domains through structural construction

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §7, §8, §16
- **Depends on:** [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0022](0022-compose-recursive-structural-vectors.md), [ADR-0067](0067-bind-higher-order-functions-as-nested-plans.md), [ADR-0068](0068-recover-low-cardinality-nested-parent-domains.md)

## Context

The evaluator preserved a shared run mapping through generic row and array construction, but expanded a shared
dictionary mapping to logical-row width. A Parquet array could therefore retain a small recovered parent domain while
an adjacent `ARRAY[...]` expression over a dictionary input became flat. A higher-order function combining the two
then repeated nested orchestration and lambda evaluation for every logical row.

An end-to-end `zip_with` probe over 250 million rows made the asymmetry visible. The initial generic implementation
used 42.3 CPU-seconds and allocated 31.2 GB despite both inputs describing only sixteen parent values.

## Decision

Generic structural construction uses the evaluator's ordinary dictionary-peeling proof. When all row-varying child
streams are compatible with one mapping, the provider-owned constructor runs over the physical backing domain and
the result is wrapped in the original logical mapping. Known-empty NULLS and ERRORS streams are omitted from the
physical constructor inputs because they carry no row distinction.

The physical backing may be longer than the active invocation domain when the current batch references only a prefix
of a retained dictionary. Structural constructors receive the common backing length. Nested provider functions accept
such a repeated vector when it covers, rather than exactly equals, the invocation domain. Incompatible child lengths
or mappings retain the ordinary logical-row construction path.

## Consequences

- Structural IR remains generic over type and function identity; physical construction stays in `TypeBinding`.
- Constructed arrays and rows can participate in the same dictionary-domain execution as source vectors.
- Unused backing entries may be constructed, but work remains bounded by physical-domain size rather than logical-row
  count.
- The 250-million-row `zip_with` probe falls to 3.62 CPU-seconds and 1.31 GB allocated. Cork requires 9.95
  CPU-seconds and 73.38 GB for the same query.
- Cartesian-domain admission remains unchanged; this decision does not add an O(rows) proof to rejected calls.

## Alternatives considered

**Recognize `ARRAY[...]` or `zip_with` in the host integration.** Rejected because construction and higher-order
semantics remain provider-owned and the evaluator must not know function identities.

**Flatten both arrays before the call.** Correct but preserves the original row-proportional work and allocation.

**Admit correlated independent domains after scanning every row.** Potentially useful, but not required for this
case and capable of adding a full proof pass before a rejected logical-row execution. It requires separate response-
curve evidence before changing the adaptive default.
