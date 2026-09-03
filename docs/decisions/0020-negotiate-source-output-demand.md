# ADR-0020: Negotiate source output demand after selection

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established progressively through 2026-08)
- **Spec:** §6, §9, §14
- **Depends on:** [ADR-0008](0008-use-first-class-masks-and-streams.md)
- **Historical evidence:** reconciliation entries for lazy materialization, omitted predicate values, and source-fused demand propagation

## Context

Reading all projected columns before filters and joins determine survivors defeats the Parquet reader and evaluator's
ability to skip work. Declaring columns unused too early is also unsafe when a later consumer borrows one.

## Decision

Separate columns/streams needed to establish selection from payload demanded afterward. Operators propagate the
current batch mask toward the source, and consumers declare stream demand. The source may defer pages, value streams,
nested children, or output vectors until selected positions require them. A violated demand declaration fails clearly.

## Consequences

- Wide filtered queries can avoid decoding and allocating rejected payload.
- Cursor advancement and batch-generation ownership become strict correctness requirements.
- Predicate enforcement remains exact even when metadata skips portions of input.
- Demand metadata must propagate through fusion and expression boundaries.

## Alternatives considered

**Read every projected column eagerly.** Simple and predictably wastes I/O, decode, and allocation.

**Infer demand from the first consumer request.** Too late for planning and unsafe when later stages need another
stream.

**Physically remove residual predicates after metadata pruning.** Incorrect unless the source proves complete
enforcement.
