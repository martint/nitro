# ADR-0135: Pack value interner fingerprints with stable IDs

- **Status:** rejected
- **Date:** 2026-09-14
- **Spec:** §12, §16, §18
- **Related:** ADR-0012, ADR-0036
- **Evidence:** `~/notes/nitro/2026-09-14-grouping-regression-attribution.md`

## Context

Grouped queries can repeatedly intern the same binary values from different flat input positions even when their
dictionary-entry caches work correctly across grouping sets. Local work counts exceed one hundred million lookups
for fewer than sixty thousand newly stored values across interner instances. Consecutive duplicate IDs are rare, so
a last-value cache would tax the common path without avoiding most lookups.

After a control-tag match, the existing interner loads an ID from its slot and then a hash from a separate per-ID
array before comparing bytes. Its private 64-bit slot hash is an injective multiplication of the supplied 32-bit
grouping hash; it contains no additional information.

## Proposal and correlation

Store the complete grouping hash and stable ID together in one 64-bit slot. Exact byte equality still resolves hash
collisions. Remove the redundant per-ID 64-bit hash array; the already-retained grouping hash can reconstruct slot
placement during growth. Preserve control tags, probing order, ID order, overflow behavior, and output values.

This conforms to the shared physical-key and encoded-domain contracts. It recognizes no logical type, function,
query, or column combination, changes no admission policy, and adds no enabling flag. It is distinct from the
previously rejected wide grouping-record-slot experiment: this smaller interner can remove a redundant array while
widening its slots. Allocation and SQL qualification remain required rather than assuming that distinction wins.

## Consequences and validation gates

- A matching slot supplies both fingerprint and ID without a dependent per-ID hash load.
- Slot growth allocates wider arrays, while per-ID growth no longer allocates the redundant hash array.
- Resizing, signed fingerprints, exact hash collisions, post-overflow lookup, and all shared consumers must remain
  correct. Existing and added collision/growth tests cover these contracts.
- Retain only after focused/full tests and paired warmed SQL qualification; remove a neutral or regressing candidate.

## Alternatives considered

**Keep split arrays.** Narrower slots, but another dependent load after each matching control tag.

**Last-value lookup cache.** Cheap for runs, but measured consecutive equality covers only 3–8% of these lookups.

**Relax dictionary-generation proofs.** Does not address these already-cacheable inputs and is unsafe for recycled
mutable storage.

**Preserve more encoding across exchange.** Potentially avoids work altogether, but has independent coalescing,
retained-memory, and ownership costs. This proposal neither selects nor prevents that investigation.

## Qualification and disposition

The focused and full suites passed (2,179 tests, no failures or errors, 394 skipped). Two-fork JMH results improved
lookup cost by roughly 2–7%, with the largest gain for prehashed lookup at 65,536 distinct values. That improvement
did not translate into a convincing SQL gain in the parent/candidate/parent sequence: ten queries, fifteen warmups
and seven measurements per query and arm. Most CPU ranges overlapped. TPC-DS q22 was neutral against the first
control but 2.2% slower than the repeat control, while the two controls themselves differed substantially.

The production layout is therefore unchanged. Retain the collision/growth test and the parameterized interner
benchmark as regression instruments, not the candidate implementation. The frozen candidate and complete evidence
remain outside the working copy in `~/notes/nitro/results/packed-interner-qualification-20260914` and its corresponding
runtime snapshot. Do not characterize this experiment as a query-performance improvement.
