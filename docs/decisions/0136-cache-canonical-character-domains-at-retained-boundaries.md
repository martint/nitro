# ADR-0136: Cache canonical character domains at retained boundaries

- **Status:** rejected; no production change
- **Date:** 2026-09-14
- **Spec:** §4–5, §16, §18
- **Related:** ADR-0012

## Context

The retained host boundary already copies recurring physical dictionaries into a bounded, memory-accounted cache.
It excludes CHAR even when the type provider proves that the physical bytes have no trailing ASCII spaces. Repeated
conversion then recreates host dictionary identity and payload despite an unchanged Nitro domain. This may prevent
host exchange coalescing from preserving the dictionary.

## Proposal and correlation

Admit BinaryVector-backed CHAR domains to the existing full-domain cache only when NO_TRAILING_SPACE proves their
canonical host representation. Keep the cache's interpretation, identity, generation, bounds and ownership rules.
Null views and mappings remain batch-specific. Padded or unproven values keep their existing normalization path.

This fills a provider-boundary capability gap within ADR-0012; it adds no logical semantics to the engine, no policy
flag, and no changes to host scheduling or buffering admission. The host block owns its copied arrays and never
retains allocator-recyclable storage. A generation change invalidates the old cached interpretation.

## Validation and disposition gate

Test recurring identity, owned storage, null overlays, mutation generations, cache cleanup, and padded values. Verify
exact SQL results and measured activation before attributing any query gain to this path. Qualify representative
grouping and exchange queries with frozen parent/candidate/parent runs. Do not claim that this alone explains all
encoding loss across exchanges; independent null-domain, dictionary identity and coalescing effects remain.

## Outcome

The focused boundary tests passed (176 tests), but six-query W15/M7 parent/candidate/parent qualification did not
establish a query CPU or allocation improvement. The repeated q22 control instead exposed an independent issue:
the same parent allocated about 25.1 GB rather than 20.8 GB, with buffered conversion time increasing about 1.8 s.
Other work counters did not explain that extra allocation. The retained conversion's per-value Slice wrappers are
the next investigation, not evidence for this dictionary-cache admission change.

Remove the candidate and its cache-specific tests. Preserve the patch, frozen runtime, and results under
`~/notes/nitro/canonical-char-domain-cache-rejected-20260914.patch` and
`~/notes/nitro/results/canonical-char-cache-qualification-20260914`. Do not change buffering thresholds or relax
generation/ownership proofs based on this outcome.
