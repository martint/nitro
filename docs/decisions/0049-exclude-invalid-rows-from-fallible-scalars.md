# ADR-0049: Exclude null and errored rows from fallible strict scalar targets

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §6, §8
- **Depends on:** [ADR-0008](0008-use-first-class-masks-and-streams.md), [ADR-0013](0013-use-layered-function-calling-conventions.md)

## Context

VALUES beneath a semantic null or input error are unspecified. Invoking an infallible strict scalar on such payloads
is harmless when no companion output is requested, but a fallible target can turn an otherwise hidden payload into an
observable exception. Avoiding the invocation requires companion-stream evidence even for a VALUES-only output.

## Decision

A framework-managed adapter for a fallible strict scalar requests each argument's NULLS and ERRORS streams whenever
VALUES are requested. The generated loop invokes the scalar target only for positions selected by the caller's mask
and absent from every input null/error stream. The adapter reuses invocation state to combine these conditions rather
than allocating a derived mask per batch.

Infallible strict scalar adapters retain the ordinary VALUES-only contract and do not acquire companion streams unless
their outputs require them.

## Consequences

- A fallible scalar cannot expose invalid payloads beneath a null or pre-existing error.
- Fallible VALUES-only calls may demand companion work that infallible calls avoid.
- Flat, dictionary, and RLE null-free fast paths remain available when the requested companions are absent or proven
  all false.
- Capturing failures raised by the target into a row-local ERRORS result remains a separate calling-convention layer.

## Alternatives considered

**Invoke the target and ignore its value.** This is safe only when the target cannot fail; an exception is observable.

**Catch target failures on invalid rows.** The adapter cannot distinguish an expected invalid-payload failure from
other failures without first reading the companion streams, and exception-driven control flow is unsuitable for a
batch loop.

**Always acquire companions for every strict scalar.** Correct but adds stream demand and disables proven null-free
fast paths for infallible functions that do not need the evidence.
