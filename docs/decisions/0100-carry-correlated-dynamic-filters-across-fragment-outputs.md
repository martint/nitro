# ADR-0100: Carry correlated dynamic filters across fragment outputs

- **Status:** amended
- **Amended by:** [ADR-0104](0104-integrate-correlated-filtering-without-an-opt-in.md) removes the opt-in and adopts bounded sparse transport; the original decision below is historical.
- **Date:** 2026-09-11
- **Spec:** §13, §15, §17, §19
- **Depends on:** [ADR-0025](0025-use-one-logical-dynamic-filter-lifecycle.md),
  [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Evidence:** focused transport, lineage, tuple-filter, and lifecycle tests in Cork `swzxxrlq`; adaptive q72 evidence
  under `~/notes/nitro/remote-benchmark-results/20260911T113317Z-q72-correlated-output-warmed-b8f117f3-20260911/`

## Context

A join with several equality criteria describes one build-key tuple. Independent per-column dynamic filters lose that
correlation. Each column can be non-selective while their conjunction admits only a small fraction of the probe rows.
TPC-DS q72 exposed the consequence across a fragment boundary: a Nitro producer emitted more than 17 million rows
because the host transported only the independent domains, while the downstream join accepted about 113 thousand
correlated tuples.

The exact join predicate remains the correctness authority, so a bounded tuple Bloom filter can safely preserve the
correlation as a necessary condition. The host planner must nevertheless identify the corresponding producer outputs
without guessing from names or from a query-specific plan shape. The metadata also crosses the coordinator-to-worker
task protocol and must survive retry, fragment reuse, projections, and exchanges.

## Decision

Represent a correlated dynamic filter as an ordered tuple of logical filter identities and types plus a bounded Bloom
filter over the registered tuple hash. Collect it from equality criteria of one join build. Keep the independent
column domains and exact join enforcement; Bloom collisions may retain rows but can never remove a matching row.

The host fragmenter traces the probe tuple to producer outputs through exact symbol lineage. Identity paths,
reference-only projections, exchange positional mappings, set-operation layouts, and nested Nitro plans are eligible.
A transformation whose lineage is not exact does not receive correlation metadata. Non-equality dynamic filters are
not included.

Attach the resulting ordered filter-ID/output-symbol groups to the producer fragment and serialize them through the
ordinary task-update protocol. Reused fragments merge distinct consumer groups. Retries may resend immutable
metadata or completed domains and must remain idempotent.

Partitioned build results union a correlation only when every completed partition supplies the same ordered tuple
shape. Intersections retain every independent necessary correlation. Missing, incompatible, or over-limit state
drops the correlation and preserves the ordinary independent domains.

An explicitly enabled Nitro remote-output consumer may apply an available immutable correlation before partitioning
or host adaptation. Admission is physical and general: every tuple output must be present with matching registered
types, retained state must be bounded and host-accounted, and selectivity sampling may stop applying an ineffective
filter. The production configuration is default-off until breadth validation establishes a safe default.

## Consequences

- Correlation is part of the one host dynamic-filter lifecycle from ADR-0025, not a second Nitro semantic authority.
- Fragment metadata and dynamic-filter payloads gain a version-compatible JSON field. Older payloads without the field
  retain their previous behavior.
- q72 producer output fell from 17,665,126 to 113,093 rows. Adaptive W10/M5 measurement reached 9.259 CPU-seconds,
  effectively restoring the historical 9.141-second band, with 32.3% less CPU and 61.8% less allocation than Cork.
- The capability remains opt-in while the full macro board identifies other activations and checks for regressions.

## Alternatives considered

**Intersect independent scalar domains at the producer.** Rejected because it cannot represent tuple correlation;
each scalar domain may cover nearly the entire input.

**Infer related filters in the worker from nearby descriptors.** Rejected because descriptor proximity and symbol
names are not lineage or join-semantics proofs.

**Publish an exact tuple set.** Rejected as the general transport because retained size and serialization can grow
with build cardinality. The exact join remains the bounded correctness backstop.

**Enable the capability by default from the q72 result.** Deferred because one favorable query does not establish
breadth safety under ADR-0027.
