# ADR-0046: Use a Nitro-native batch SPI for table functions

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; decided and partially implemented 2026-09-01–02)
- **Spec:** §9, §12.3, §15, §19
- **Depends on:** [ADR-0023](0023-put-vectors-and-allocation-in-the-connector-spi.md), [ADR-0024](0024-confine-page-block-adaptation-to-boundaries.md)
- **Historical evidence:** `2026-09-01-pattern-recognition-and-table-function-operator-contracts.md`

## Context

Trino table-function processors exchange optional Pages and encode consumption, pass-through identity, partitioning,
ordering, markers, blocking, and completion in host-specific state. Calling that API inside Nitro requires Page
conversion for every batch and cannot expose Nitro encodings or allocator ownership.

## Decision

Define a classloader-neutral Nitro table-function SPI over batch views, masks, demand, consumption, allocator context,
owned outputs, blocking, and completion. Plan pass-through row identity and partition/order requirements explicitly.
Distinguish host compatibility, generated/materializing compatibility, and Nitro-native providers in admission and
diagnostics.

## Consequences

- Native providers participate directly in complete islands.
- Existing host functions can remain available outside an island or through visible compatibility boundaries.
- Multi-argument, marker, pass-through, partition, nested, and split/source coverage must be completed explicitly.
- Compatibility breadth is never mislabeled zero-adaptation execution.

## Alternatives considered

**Use Trino's Page processor as the Nitro SPI.** Broad compatibility and permanent host representation in the data plane.

**Require native rewrites before exposing any table function.** Clean but prevents incremental coverage.

**Hide Page conversion in a native-looking provider.** Makes performance and island identity misleading.
