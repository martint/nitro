# ADR-0029: Make coverage gaps explicit and add end-to-end probes with every capability

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established progressively through 2026-08)
- **Spec:** §2, §17, §19
- **Depends on:** [ADR-0003](0003-compose-indivisible-pull-islands.md)
- **Historical evidence:** Engine Coverage suite and capability inventories under the Nitro design knowledge base

## Context

Silent fallback makes unsupported functions, types, operators, and reader encodings look correct while hiding host
execution and performance cliffs. Public benchmark suites also omit important nested types, functions, encodings, and
adaptive boundaries.

## Decision

Planning/binding fails loudly when an admitted Nitro region lacks a required capability. Each added type, operator,
function convention, source encoding, or optimization receives focused conformance tests and an end-to-end Engine
Coverage query where meaningful. Public suites remain macro regression guards.

## Consequences

- Completeness is measurable rather than inferred from successful queries.
- Engine Coverage expands beyond TPC-H, TPC-DS, and ClickBench.
- Unsupported work may reduce short-term query coverage instead of quietly running in the host.
- Capability inventories become productization inputs.

## Alternatives considered

**Fall back inside an island.** Maximizes apparent coverage and destroys attribution and the island invariant.

**Test only public suites.** Stable and blind to large parts of the engine contract.

**Add unit tests only.** Necessary but insufficient for planner, source, ownership, and distributed composition.
