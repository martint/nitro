# ADR-0157: Separate hash generation from probe-fusion admission

- **Status:** rejected; trial removed after SQL activation audit
- **Date:** 2026-09-15
- **Spec:** §12, §13, §16, §18
- **Related:** ADR-0011, ADR-0036
- **Evidence:** `~/notes/nitro/2026-09-15-decoupled-hash-activation.md`

## Context and correlation

The existing physical-domain rule keeps hashing and probing decoupled for reusable flat binary values. Its shared
kernel gate also rejects generated hash-only preparation. The latter still writes the ordinary batch hash buffer
and does not fuse table access, so generation and fusion are distinct decisions. Current CPU profiles attribute
substantial work to the generic field loop and null dispatch, but they do not prove this alternative faster.

The existing test explicitly expects hash-only rejection too. This proposal narrows that earlier admission rule;
it is not presented as a behavior-preserving cleanup. It preserves the empirical requirement to keep probing
decoupled for the admitted reusable domain and follows the capability-based generation contract in ADR-0011.

## Proposed decision

Retain the reusable-flat-domain rejection at all fused grouping and distinct entry points. Let hash-only preparation
use the existing Classfile-generated physical kernel when its other type, arity, row-count, null-shape and encoding
requirements hold. Existing exact hash values, discriminator choice, cached dictionary hashes, source accessors,
null semantics, table equality, position selection, pooling and ownership remain authoritative and unchanged.

There is no new kernel family, function implementation, enabling property, global cache or query recognition.
This is a bounded change to normal physical admission, not a blanket preference for fusion or a default-policy
threshold retuning. Generated equality and probing remain as before.

## Qualification

Compare generated hashes with the authoritative layout over reusable flat domains, null-free/mixed/all-null fields,
different key widths, batch transitions and selected positions. Assert that fused grouping and both distinct paths
still decline this domain without mutating output or the table. Keep high-cardinality and encoded-domain guards.
Run focused/full tests and warmed frozen parent/candidate/parent SQL across q22/q67 and neighboring grouping/join
shapes. Prove that the changed path actually executes before interpreting timings. Match input work and allocation;
use normal-flag SQL results for acceptance, not profiled sample shares. Remove a neutral or regressing candidate.

The accepted specification and defaults are not amended unless qualification supports accepting this refinement.

## Disposition

The candidate passed 177 focused tests and the full suite of 2,198 tests, with zero failures or errors and 394
skipped. The new tests demonstrate correct hash generation for a constructed reusable domain, but do not prove
that the changed path executes in SQL.

An untimed, separately instrumented execution of each target query counted every generated-kernel admission
return. q22 attempted hash-only preparation 8,740 times and accepted none. q67 attempted it six times and accepted
none; its 5,121 successful fused calls covered 48,503,790 rows through the existing path. Neither query admitted
the new hash-only path. The independent guards remain relevant: relaxing the reusable-domain gate alone does not
admit these inputs. First-use log messages were insufficient to establish this distinction.

Remove the production change and its trial-only tests. Keep the patch, frozen classes and counter evidence outside
the working copy. This rejects the trial as a remedy for the investigated regressions, not the general possibility
that a different workload benefits from decoupled generated hashing. No SQL speedup or slowdown was established;
another proposal would first need a representative activating workload and separate qualification. No accepted
contract, default or enabling flag changes.

The restored production tree passes the full suite: 2,197 tests, zero failures/errors, 394 skipped. The rejected
candidate's extra test case is absent; accepted behavior and its existing test expectations are restored together.
