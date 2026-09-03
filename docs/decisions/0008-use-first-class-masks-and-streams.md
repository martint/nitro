# ADR-0008: Make masks and VALUES/NULLS/ERRORS first-class

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective)
- **Spec:** §3, §5, §6, §7, §8
- **Historical evidence:** `docs/architecture-target-contracts.md`; `2026-08-28-optional-function-output-streams.md`

## Context

Selection represented only by compacted rows forces payload work before survival is known. Nulls and errors embedded
implicitly in values make selective evaluation and conditional error isolation difficult. SQL conditions require
inactive branches not to execute.

## Decision

Masks are explicit throughout evaluation and operators. Expression results expose independently demandable VALUES,
NULLS, and ERRORS streams. Conditional evaluation derives branch masks before invocation, and constraints flow toward
lazy sources within their originating batch lifetime.

## Consequences

- Expensive expressions and columns can be skipped for rejected positions.
- SQL errors are observed only where selected.
- Consumers must declare stream demand accurately.
- Encoded-domain processing proves alignment for every requested companion stream.

## Alternatives considered

**Compact after each filter.** Simple loops, but copies data and loses upstream position identity.

**Embed null/error state in values.** Familiar APIs, but forces materialization and obscures independent demand.

**Evaluate all branches and merge.** Performs unnecessary work and violates conditional error semantics.
