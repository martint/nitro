# ADR-0084: Prove constant window bounds across retained sources

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §12.1
- **Depends on:** [ADR-0008](0008-use-first-class-masks-and-streams.md), [ADR-0078](0078-run-reversible-window-ranges-in-provider-kernels.md)
- **Evidence:** Engine Coverage q241 frame-bound allocation profile

## Context

The reversible window range convention can represent constant preceding/following ROWS bounds with one compact
affine descriptor. The host adapter recognized the descriptor only when an entire partition referenced one retained
source batch. A ten-million-row partition assembled from many pages therefore allocated start and end arrays for
every output batch even when each page carried the same one-run constant offset.

Inspecting only the first page would be incorrect because a later page may contain a different value, null, or error.
Flattening the partition or constructing a logical-position mapping would discard encoded structure and add work
proportional to logical rows.

## Decision

A row-position index exposes the exclusive end of the contiguous logical run backed by the current physical source.
Its compatibility implementation advances through the run without allocating; indexes that already retain page
boundaries provide constant-time implementations.

A host positional-frame adapter may traverse those source runs and publish an affine descriptor only when every run
proves the same single-run, non-null, error-free, non-negative offset. Any dynamic, unequal, nullable, errored, or
unsupported physical shape retains exact per-position frame resolution. The contract contains no function, query,
column, or logical-type specialization.

## Consequences

- Constant bounds remain compact across retained page boundaries.
- The proof examines physical source runs without constructing row-position arrays.
- Existing row indexes remain compatible through the exact default traversal.
- Row indexes with known segment boundaries should override the default to avoid scanning logical positions.
- A later incompatible page cannot silently inherit an earlier page's bound.

## Alternatives considered

**Trust the first retained page.** Rejected because later pages can change the observable frame or contain errors.

**Treat every multi-page partition as dynamic.** Correct, but allocates and fills redundant frame planes for common
constant SQL bounds.

**Flatten frame-offset columns before binding.** Rejected because it copies data, erases encodings, and scales storage
with logical rows.
