# ADR-0068: Recover low-cardinality nested parent domains

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §5, §8, §14, §16
- **Depends on:** [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0022](0022-compose-recursive-structural-vectors.md), [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md), [ADR-0067](0067-bind-higher-order-functions-as-nested-plans.md)

## Context

Parquet can encode the leaves of an ARRAY or MAP over a tiny physical domain while the parent remains an ordinary
offset vector. Downstream higher-order and structural functions then see one parent per logical row and must rebuild
the equality already present in the child encodings. The reader previously recovered only the special case in which
every parent was identical.

An end-to-end `reduce` probe over 250 million rows and sixteen repeated array values exposed the consequence. A
correct wave evaluator initially consumed 83.4 CPU-seconds and allocated 81.4 GB. Removing accidental integer boxing
and preserving a one-entry initial-state domain reduced that to 16.0 CPU-seconds, but the engine still repeated parent
orchestration at logical-row cardinality.

## Decision

The native Parquet structural reader may recover a bounded outer dictionary for repeated ARRAY and MAP parents. It
hashes a representation-only signature and confirms every candidate collision using parent null state, repeated
boundaries, and each encoded child VALUES/NULLS/ERRORS identity. It does not call SQL equality or infer equality from
cardinality. Representatives, mappings, frequencies, and scratch are allocator-owned.

Admission reuses the immutable Parquet dictionary-domain limits: maximum entries and minimum rows per entry. If a
child is flat, the domain exceeds the bound, or the achieved reduction is insufficient, the reader returns the
ordinary recursive vector unchanged.

The evaluator's independent-domain path also treats a single-run RLE argument as a one-entry physical domain. This
allows a constant argument to participate with independent dictionary arguments instead of disabling domain
execution.

## Consequences

- Higher-order and other deterministic functions can execute once per proven parent shape without knowing how the
  shape was recovered.
- Nested-domain recovery is useful beyond any one function and remains independent of logical type semantics.
- The proof adds a bounded hash/equality pass for eligible encoded nested inputs; admission rejects domains whose
  reduction cannot amortize it.
- In a warmed paired run, the `reduce` probe executes in 2.394 CPU-seconds after the change, versus 6.427 CPU-seconds
  for Cork, while preserving the same 250-million-row input and result.
- Single-run constants no longer erase independent dictionary-domain opportunities.

## Alternatives considered

**Specialize `reduce` for the benchmark expression.** Rejected because function and query semantics belong to the
registry provider, not the reader or evaluator.

**Generate only a flat per-row reduction loop.** This avoids orchestration overhead but still repeats deterministic
work for every logical row and discards useful encoded identities.

**Infer a parent domain from matching cardinalities.** Rejected because independently encoded children can have the
same cardinality without representing the same logical partition.

**Keep only the all-constant special case.** Correct but too narrow: common low-cardinality nested columns contain a
small set of values rather than one value.
