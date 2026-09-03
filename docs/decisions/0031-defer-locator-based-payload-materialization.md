# ADR-0031: Defer locator-based payload materialization until remote reads are designed

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; decided 2026-08-13)
- **Spec:** §14, §19
- **Historical evidence:** reconciliation proposal “stable-locator payload replay”

## Context

Wide, selective TopN and similar operators could retain row locators and materialize payload only for winners. A local
mmap prototype is insufficient: production payload can come from object storage, files can be reopened, credentials
and retries remain connector-owned, and retained locators need snapshot/lifetime semantics.

## Decision

Do not add locator-based deferred payload materialization to the current model. Preserve it as a future connector/source
capability only after stable remote-capable locators, authorization lifetime, reread accounting, cache behavior, and
failure semantics are specified.

## Consequences

- Current work focuses on selection, encoded domains, decode avoidance, and existing vector lifetimes.
- Some filtered wide queries still materialize payload earlier than the theoretical minimum.
- A future design must work identically for local and object-store data rather than optimize only mmap benchmarks.

## Alternatives considered

**Implement local-file locators first.** Demonstrates potential but establishes an API that may not survive object
storage.

**Keep source batches pinned until TopN completes.** Avoids rereads and turns streaming input into query-sized retained
memory.

**Copy payload for all survivors.** Current conservative behavior; more work, but bounded and connector-independent.
