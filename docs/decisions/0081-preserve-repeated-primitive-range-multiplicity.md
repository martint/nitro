# ADR-0081: Preserve repeated primitive range multiplicity

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §9, §12.1, §17
- **Depends on:** [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0080](0080-compose-provider-primitive-result-ranges.md)
- **Evidence:** Engine Coverage q241 one-count W5/M5 and window-stage attribution after ADR-0080

## Context

ADR-0080 removes intermediate vectors from compatible window-to-aggregation composition, but initially invokes the
consumer once for every logical result. Bounded `ROWS` frames have a common physical shape: after short leading and
trailing edges, a large contiguous interior can have the same exact frame cardinality. Expanding that proven run into
millions of identical calls discards physical multiplicity already known by the producer.

The engine cannot infer whether applying one value repeatedly is equivalent to one weighted state transition. That
depends on provider-owned arithmetic, overflow, null, error, and ordering semantics. Recognizing count or sum in the
engine would violate the registry boundary.

## Decision

The primitive range consumer accepts an optional exact logical multiplicity with a value. Its default implementation
expands the run through the existing scalar contribution method, preserving every provider without additional
capabilities. A provider may override the method only when it can implement the same logical sequence exactly,
including overflow and failure behavior.

A producer may emit a repeated contribution only for a contiguous run whose identical physical value it proves. The
optimization neither assigns logical type nor function meaning to the primitive carrier. Fan-out and pass-through
composition preserve the multiplicity for every consumer; an unsupported consumer remains correct through default
expansion. Negative multiplicity is invalid and zero is an exact no-op.

## Consequences

- Long constant interiors can update compatible downstream state once rather than once per logical result.
- Providers retain all semantics of repeated application; the engine exposes only physical value and multiplicity.
- The convention generalizes beyond windows to any producer that proves an identical contiguous primitive run.
- Order-sensitive or non-associative consumers remain correct by inheriting scalar expansion.
- The physical ABI grows by one backward-compatible default method rather than by function-specific paths.

## Alternatives considered

**Recognize bounded count windows and sums in the engine.** Rejected because both identities and arithmetic semantics
belong to dynamically registered providers.

**Always multiply before updating downstream state.** Rejected because multiplication can change overflow, rounding,
ordering, and error behavior.

**Encode the run as an RLE vector.** Rejected for this composition because it restores the result allocation and
vector traversal that ADR-0080 intentionally removed. RLE remains appropriate at ordinary vector boundaries.
