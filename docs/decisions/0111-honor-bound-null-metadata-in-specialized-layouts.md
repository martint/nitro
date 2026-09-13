# ADR-0111: Honor bound null metadata in specialized key layouts

- **Status:** accepted; remote performance qualification pending
- **Date:** 2026-09-13
- **Spec:** §12, §16
- **Depends on:** [ADR-0036](0036-share-physical-key-contracts-across-consumers.md)
- **Related:** rejected [ADR-0107](0107-derive-group-null-metadata-from-stored-keys.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

The shared layout's input-null capability promises to use the current batch binding when one exists, including
when the caller does not supply null vectors again. The existing pair specialization binds its own physical
accessors but inherits a metadata implementation that cannot see them. It repeats RLE decoding when supplied
null vectors, and reports false rather than the bound nullable value when the caller omits those vectors.
The q32 profile attributes substantial work to this metadata path, with large variation across runtime contexts.
The family audit also found that the general layout retained constant nullability flags after ending a binding,
so a subsequent unbound lookup could incorrectly reuse the previous batch's proof.

## Decision

Have the existing physical specialization answer input-null metadata from the same batch nullability proof and
accessors used to write and compare its keys. Outside a batch binding, preserve the general input-vector lookup.
Ending a batch invalidates the binding before a different encoding or null distribution is admitted.
Clear general-layout constant-null proofs at batch end and buffer release, alongside the existing accessor cleanup.
Keep the hot lookup unchanged: initialization also uses these proofs before all batch accessors are ready.

This repairs an existing capability contract without changing field arity admission, physical key layout,
logical type semantics, normalization, or stored-null representation. It does not reinstate ADR-0107's rejected
stored-record bookkeeping implementation. The other handwritten subclass, the projected layout, already supplies
its own bound ancestor-null lookup; its separate opportunity to hoist repeated ancestor reads is not changed here.

## Validation

Regression tests fail on the prior implementation for bound lookup without resupplying vectors and for stale
general-layout proofs after batch end. Coverage includes unbound fallback, all-false/all-true RLE, mixed RLE and
dictionary nulls, reset/rebind, buffer release, and stored grouping null metadata. The full suite passes 2,155 tests,
zero failures/errors, 394 skipped. No observed SQL-result failure is claimed from these internal-contract reproducers.

The specialized lookup's first unprofiled q31/q32 comparison has overlapping CPU ranges. A W30/M9 q30→q31 sequence
gives q31 CPU 5.124 versus 5.352 seconds with separated ranges, unchanged allocation and overlapping latency ranges;
q30 is neutral. An apparent allocation increase in the first q31 pair is not reproduced in either the subsequent
context pair or the matched allocation-profile pair. Those results precede the additional batch-end flag clearing.
The complete repair is undergoing combined SQL qualification; no remote or full-board improvement is claimed.
