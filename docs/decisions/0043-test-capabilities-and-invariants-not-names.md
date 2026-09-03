# ADR-0043: Test architectural capabilities and invariants, not names

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; established 2026-07-23)
- **Spec:** §17
- **Historical evidence:** `2026-07-23-architecture-review-and-trino-integration-boundary.md`; architecture cleanup principles

## Context

Tests that forbid package prefixes or enumerate implementation class names can pass while architecture is violated
through a new name, and fail during harmless refactoring. The actual properties concern dependency direction,
construction, supported capabilities, island composition, ownership, and absence of host representations in core.

## Decision

Architecture tests assert observable contracts and dependency/capability boundaries. Representative and corpus-wide
tests prove island composition, registry resolution, allocator lifecycle, classloader neutrality, and explicit host
adaptation. Names may be checked only when the name itself is a public or module contract.

## Consequences

- Refactoring does not require updating brittle deny lists.
- Tests require purpose-built diagnostics and introspection at important boundaries.
- A violation is explained in architectural terms rather than as a string mismatch.
- Static dependency/module checks complement runtime behavior tests.

## Alternatives considered

**Ban package/class name patterns.** Cheap and disconnected from the behavior being protected.

**Rely on review only.** Flexible and too easy to regress across a large integration.

**Test only end results.** Proves SQL correctness but not that Nitro actually executed or preserved its boundaries.
