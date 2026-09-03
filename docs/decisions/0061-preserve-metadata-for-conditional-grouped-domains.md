# ADR-0061: Preserve metadata for conditional grouped-domain admission

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §12.1
- **Depends on:** [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0060](0060-generate-aligned-encoded-domain-aggregation.md)

## Context

The raw grouped-domain calling convention is necessarily batch-conditional. An implementation can consume a physical
domain only when its value and varying null streams align exactly with the grouping mapping. Planning happens before
those vectors exist, but it must decide whether to ask the source and evaluator to retain domain frequencies.

Treating the batch-local capability check as the only declaration creates a circular dependency: planning discards
the metadata, so every runtime check must reject even though the original encoded input could have satisfied it.
Requesting counts for every aggregation would avoid that failure but impose row-proportional work on implementations
and ungrouped queries that cannot consume the metadata.

## Decision

Aggregation implementations expose a shape-independent declaration that they may support raw grouped-domain input.
Registered physical units preserve that declaration only for raw input. A host or source-demand planner uses it to
retain exact grouping-domain metadata when every participating unit is eligible.

The declaration is not execution admission. For every batch, the engine still asks the existing input-specific and
row-mapping-specific capability checks for all participating units before grouping mutates state. Any mismatch falls
back atomically to logical-row execution.

An exact row-mapping proof may use either established shared identity or complete equality of domain width and ID
sequence. Equal dictionary cardinality, values, or membership is not sufficient.

Deterministic row-aligned scalar functions propagate an exact domain-metadata demand to their inputs. This matches
the evaluator's function-neutral dictionary-domain execution: it evaluates the physical values and restores the
input mapping on the result. Non-deterministic functions do not propagate the demand because they must execute per
logical row.

## Consequences

- Conditional physical-domain paths receive the metadata needed to become admissible.
- Ungrouped aggregation and providers that do not declare the capability retain their ordinary, cheaper demands.
- Provider semantics remain opaque; planning knows only a physical calling-convention capability.
- Independently decoded but exactly aligned Parquet columns can share domain work after a linear equality proof.
- Shared mapping identity remains preferable because it avoids that proof cost.

## Alternatives considered

**Always request domain counts for grouped aggregation.** Correct but adds avoidable decode and metadata work for
programs that cannot consume a grouped domain.

**Infer capability from a function name or logical type.** Violates registry neutrality and would duplicate provider
knowledge in the engine or host integration.

**Require mapping object identity.** Cheap but rejects independently decoded columns even when their complete ID
sequences prove the same logical-to-physical mapping.

**Run the batch-local check during planning.** Impossible because physical vectors and their encodings do not exist
until execution.
