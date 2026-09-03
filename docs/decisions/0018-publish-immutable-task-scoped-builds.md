# ADR-0018: Publish immutable prepared join builds at task scope

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-30)
- **Spec:** §11, §13
- **Historical evidence:** reconciliation entries for immutable prepared hash/semi-join builds and prepared membership reuse

## Context

Parallel probe drivers repeatedly constructing equivalent build indexes wasted CPU and memory. Process-global build
caches violate query isolation, while mutable shared builds create publication and lifetime races.

## Decision

Finalize a build into an immutable task-owned capability and publish it to compatible probe drivers. The build exposes
only general membership, lookup, dynamic-filter, and retained-state contracts. Its storage is released when the task's
last lease closes.

## Consequences

- Build work and exact membership are shared once per task.
- Publication requires an explicit completion barrier and immutable post-publication state.
- Diagnostics and memory accounting attribute shared storage across consumers without double counting.
- Incompatible join semantics or physical key bindings require separate builds.

## Alternatives considered

**One build per driver.** Simple and predictably duplicates the dominant build work.

**A process-global prepared-build cache.** Reuses more broadly but leaks query data/lifetime and introduces ambient
state.

**Concurrent mutation by all builders.** Avoids a barrier but complicates exactness, resize, and cancellation.
