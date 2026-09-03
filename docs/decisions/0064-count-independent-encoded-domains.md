# ADR-0064: Count independent encoded domains under a mask

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §5, §6, §12.1
- **Depends on:** [ADR-0008](0008-use-first-class-masks-and-streams.md), [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)

## Context

A mask produced from one encoded column need not share the row-to-domain mapping of another encoded column consumed
later. Mapping identity therefore cannot prove that selected rows cover whole entries of the second domain. Falling
back to value-level processing is correct, but it repeats expensive function or state updates for every logical row
even when the independently encoded input has a tiny domain.

The exact selected frequency of the second domain can still be computed with one pass over integer ids. That work is
both cheaper than repeating arbitrary value semantics and reusable by any consumer that accepts weighted physical
values. The frequency scratch must remain allocator-owned rather than becoming a row-proportional heap allocation.

## Decision

A dictionary vector exposes a function-neutral operation that populates caller-owned scratch with the exact selected
frequency of each physical-domain entry under any same-length mask. The operation reuses aligned compact-domain
metadata and full-domain frequencies when their proofs apply; otherwise it counts the selected row ids exactly.

Consumers decide whether weighted domain processing is semantically valid. The vector and mask APIs only derive
physical multiplicities; they do not identify functions, infer idempotence, or apply aggregation semantics. Long-lived
consumers allocate and reuse the frequency scratch through their existing allocator context.

## Consequences

- An unrelated filter no longer forces a low-cardinality encoded aggregate or evaluator to repeat value-level work.
- The fallback cost is one integer-id pass, not zero work; aligned domains remain the preferred path.
- The contract supports arbitrary dictionary cardinality, unlike compact domain-selection bit sets.
- Providers that cannot apply multiplicities, including order-sensitive operations, retain logical-row processing.
- Reusable caller-owned scratch preserves the zero row-proportional steady-state allocation target.

## Alternatives considered

**Treat equal domain cardinality as alignment.** Incorrect: independent dictionaries can order values and map rows
differently.

**Attach the second domain to the mask by replacing its existing metadata.** A mask can serve multiple streams, and
mutating its proof to one consumer's mapping can invalidate another consumer's assumptions.

**Let every provider implement its own id-counting loop.** Correct but duplicates representation mechanics, obscures
the allocation contract, and makes optimized aligned-metadata reuse inconsistent.

**Always process selected logical rows.** Correct, but turns a compact encoded input into avoidable repeated function
and state work.
