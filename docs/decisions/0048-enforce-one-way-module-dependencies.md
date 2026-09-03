# ADR-0048: Enforce one-way dependencies between SPI, engine, providers, compatibility, and tests

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-23–25)
- **Spec:** §4.1
- **Depends on:** [ADR-0006](0006-keep-logical-semantics-in-registries.md), [ADR-0023](0023-put-vectors-and-allocation-in-the-connector-spi.md)
- **Historical evidence:** `2026-07-23-architecture-review-and-trino-integration-boundary.md`

## Context

Early source, function, vector, benchmark, and engine code shared packages and upward dependencies. Connector and
function providers could see engine implementation, while reusable engine code could acquire Trino/test dependencies.
Package naming alone was insufficient to enforce a real classloader and dependency boundary.

## Decision

Keep a dependency-minimal SPI below the engine and independent providers. Engine and Parquet implementations depend on
the SPI, not each other. Connector loading depends only on the SPI. Host/legacy adaptation is quarantined in explicit
compatibility/integration modules, and tests sit above every production layer. No production dependency points into
tests or benchmark harnesses.

## Consequences

- Providers can load in isolated classloaders without engine implementation visibility.
- The SPI must contain only genuinely shared contracts, not convenient operator helpers.
- Compatibility debt remains visible as a module rather than leaking into core.
- Build/module dependency checks can enforce the boundary independently of package names.

## Alternatives considered

**One module with package conventions.** Easy development and no enforceable dependency/classloader boundary.

**Put Parquet inside the engine.** Simplifies construction and makes one connector format part of engine vocabulary.

**Expose engine services through the SPI.** Convenient for providers and creates upward coupling and an unstable SPI.
