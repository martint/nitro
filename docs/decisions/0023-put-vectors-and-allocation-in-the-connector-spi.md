# ADR-0023: Put Nitro vectors and allocator ownership in the connector SPI

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-25)
- **Spec:** §5, §11, §14
- **Historical evidence:** `2026-07-25-vector-connector-spi.md`

## Context

A connector that can return only row objects or host Pages forces adaptation before Nitro can exploit dictionary,
RLE, nested, lazy, or selected representations. Letting connectors allocate arbitrary buffers prevents ownership and
memory accounting.

## Decision

The Nitro connector SPI includes vector/column views, stream ownership operations, source selection, and a scoped
vector allocator. Connectors may produce native Nitro vectors and encodings without importing operators, evaluator IR,
engine resources, or host Page/Block types.

## Consequences

- Readers can decode directly into final engine storage.
- Connector vectors participate in the same borrow/transfer/release and accounting model.
- The SPI must stay small, stable, classloader-neutral, and free of engine services.
- Generic host batches enter through constructed type-specific importers at an explicit compatibility boundary.

## Alternatives considered

**Connector returns host Pages.** Broadly compatible and preserves adaptation overhead and host coupling.

**Connector returns rows.** Universally expressive and incompatible with vectorized/lazy execution.

**Expose complete engine internals to connectors.** Powerful but destroys isolation and makes the SPI unstable.
