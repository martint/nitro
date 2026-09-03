# ADR-0012: Execute over encoded domains, not only encoded vectors

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established progressively through 2026-08)
- **Spec:** §3, §5–8, §12–16
- **Depends on:** [ADR-0008](0008-use-first-class-masks-and-streams.md)
- **Historical evidence:** `2026-08-23-synchronized-board-and-next-frontiers.md`; reconciliation entries for encoded frequencies and observed tuple relations

## Context

Preserving a dictionary wrapper avoids one copy but still wastes most of its value if every downstream function,
predicate, join, or aggregate iterates logical rows. Low-cardinality and repeated data can represent hundreds of
millions of rows with a small set of physical values and a mapping.

## Decision

When semantics, mask, companion streams, and downstream demand permit it, evaluate physical domain entries once and
carry the mapping, exact frequencies, or observed tuple relation to consumers. Fall back to logical positions when
alignment, determinism, errors, ordering, or representation evidence is insufficient.

## Consequences

- Work can scale with represented values rather than logical rows.
- VALUES, NULLS, ERRORS, masks, and frequencies require explicit domain-alignment proofs.
- Transformations preserve relevant domain metadata or deliberately invalidate it.
- Independent argument dictionaries may use their observed tuple relation instead of a Cartesian product.

## Alternatives considered

**Preserve encodings only for storage.** Saves memory/copies but leaves CPU proportional to logical rows.

**Always flatten before execution.** Simplifies kernels and forfeits repetition completely.

**Always execute the encoded domain.** Incorrect for row-dependent, nondeterministic, error-sensitive, or misaligned
inputs and can cost more for large domains.
