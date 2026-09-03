# ADR-0045: Implement pattern recognition as an engine-owned restartable Nitro operator

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; decided and implemented 2026-09-01–02)
- **Spec:** §9, §10, §12.2
- **Depends on:** [ADR-0003](0003-compose-indivisible-pull-islands.md), [ADR-0006](0006-keep-logical-semantics-in-registries.md)
- **Historical evidence:** `2026-09-01-pattern-recognition-and-table-function-operator-contracts.md`

## Context

Trino pattern recognition combines a structural automaton, partitions/order, label definitions, navigation, measures,
match-local aggregation, skip behavior, and several output modes. Embedding its Page operator inside Nitro would split
an island and hide conversion cost, while teaching Nitro SQL labels/functions would violate registry neutrality.

## Decision

Nitro owns a structural pattern IR, compiler, restartable matcher, navigation/search/skip cursors, encoded row buffer,
and output operator. Host planning lowers resolved semantics into immutable ordinal-based descriptors. Definitions,
measures, and aggregates enter through registry-bound callbacks. Cooperative checkpoints occur between atomic semantic
calls with all progress committed.

## Consequences

- Pattern regions remain complete Nitro pull graphs with native vectors.
- Matcher state and cross-batch row retention are allocator-owned and explicitly releasable.
- The engine must support every output/navigation semantic before broad admission.
- Host labels, symbols, plan nodes, and function identities disappear at lowering.

## Alternatives considered

**Embed the Trino pattern operator.** Maximizes immediate coverage and violates island and Page/Block boundaries.

**Treat a pattern as a generated scalar expression.** Cannot represent partition-wide automaton state or multi-row
output.

**Implement common patterns as special operators.** Fast for examples and not a general SQL pattern contract.
