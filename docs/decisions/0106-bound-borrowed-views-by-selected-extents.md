# ADR-0106: Bound borrowed views by selected extents

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §6, §15
- **Clarifies:** [ADR-0098](0098-borrow-remote-output-storage-through-synchronous-destination-append.md)
- **Depends on:** [ADR-0008](0008-use-first-class-masks-and-streams.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

A local repeated ClickBench q13 run failed while constructing a borrowed long block: its advertised position count
exceeded its backing array. The exact parent reproduced the same failure. A sparse output mask may retain its
original coordinate-domain width even when the materialized vector only needs to cover a selected prefix. The
remote-output adapter incorrectly used that original width to construct an all-position borrowed base view.
Incidental spare capacity in pooled arrays can hide the error, so passing a cold query does not establish safety.

## Decision

A temporary borrowed flat base for a sparse selection exposes the prefix ending immediately after the greatest
selected position, not the entire original mask domain. The selection mapping remains unchanged. Actual selected
values must still be present; this does not pad missing values, weaken bounds checks, or introduce a copying fallback.
The all-channel type capability and non-retaining synchronous callback required by ADR-0098 remain unchanged.

This repairs an implementation gap in the existing mask and borrowing contracts. It does not change host
partitioning, scheduling, buffering, serialization, or source-batch lifetime.

## Validation

The existing sparse borrowed-storage test now covers mask domain widths 3, 64, and 4096 over a three-value array.
The two larger widths reproduce the original exception before the fix and pass afterward, while asserting the
same backing array is borrowed and only selected values reach the consumer. All 51 focused exchange/serialization
boundary tests pass. Query-level repeated execution is retained with the investigation evidence.
