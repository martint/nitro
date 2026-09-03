# ADR-0007: Make allocator ownership, borrowing, and transfer explicit

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective)
- **Spec:** §3, §11, §15
- **Depends on:** [ADR-0003](0003-compose-indivisible-pull-islands.md)

## Context

Vector reuse works only with unambiguous lifetimes. Separate allocator domains per adjacent host operator caused copies,
while implicit aliasing caused retained foreign storage and double-release risk. Host memory accounting still needs
attribution finer than one island-wide pool.

## Decision

Nitro distinguishes owned vectors, scoped borrows, and ownership transfer. One island/task may share a storage owner
and pools while immutable allocator contexts retain separate policy, diagnostics, and host reservation charging. Every
resource has one release authority.

## Consequences

- Adjacent Nitro operators reuse storage without copying.
- Cancellation and parent/child close order require explicit tests.
- Allocator contexts are accounting/policy scopes, not isolated ownership universes.
- Pools remain bounded and observable; GC reachability is not lifecycle control.

## Alternatives considered

**One allocator per operator.** Easy attribution, but copies vector trees and destroys steady-state reuse.

**One undifferentiated query allocator.** Preserves reuse but loses operator attribution and host charging.

**Soft-reference pools.** Delegate policy to GC timing and do not solve ownership or double release.
