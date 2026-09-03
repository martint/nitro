# ADR-0009: Decode Parquet natively over connector-authoritative ranged I/O

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective)
- **Spec:** §14, §15

## Context

Row-oriented `parquet-mr` fallback obscured missing coverage and introduced materialization incompatible with Nitro's
vector and mask model. Direct filesystem coupling would lose the host connector's object-store access, authentication,
retries, cancellation, and accounting.

## Decision

Nitro implements Parquet metadata, level, value, encoding, and structural decoding natively into Nitro vectors. It
reads through a connector-authoritative ranged-input capability. Unsupported types or encodings fail loudly. Decoder
composition is abstract rather than a SQL-type switch in nested reconstruction.

## Consequences

- Nitro preserves encodings and skips unneeded streams directly.
- Reader coverage is implemented explicitly and tested across physical layouts.
- The I/O boundary supports local files and object stores without Hadoop or `parquet-mr` record-reader coupling.
- Nitro may optimize range scheduling above the host but cannot bypass its security and lifecycle behavior.

## Alternatives considered

**Fall back to parquet-mr.** Broad coverage, but failures become silent performance cliffs and row materialization.

**Use Nitro-owned local-file I/O.** Fast for benchmarks, but not viable for object storage.

**Reuse host Page/Block readers internally.** Preserves connector features but makes host representation an engine
abstraction and reintroduces adaptation throughout scan.
