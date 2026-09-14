# ADR-0143: Release rejected grouped dictionary mappings

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §11–12, §16–18
- **Related:** ADR-0036; discovered while evaluating rejected ADR-0142

## Contract and defect

A speculative physical representation owns its temporary allocations until it either publishes them with a result
or releases them on rejection. Dictionary output allocates a pooled row-ID mapping before checking that every
selected group has a usable interned value. Missing records or unavailable IDs currently reject the representation
without returning that mapping, retaining it until the enclosing allocator context closes.

Release the temporary mapping on each of those rejection paths, for both full and ranged output. Preserve all
representation admission, null behavior, grouping semantics, and successful-result ownership. This implements the
existing lifetime contract; it does not retain ADR-0142's rejected nullable-range admission or introduce a policy.

## Validation

Exercise rejected full and ranged mappings repeatedly. Permit initial pool allocation, then require cumulative
allocated bytes to stop growing; a context-wide final release must not hide per-attempt retention. Retain existing
successful-output and nullable-output tests and run the full suite before accepting the cleanup.

The test fails on the parent: after warming both output sizes, another rejected attempt increases cumulative
allocation from 48 to 80 bytes. With the cleanup, repeated attempts reuse the returned mappings and the complete
2,190-test suite passes with no failures or errors and 394 skips. No output-admission or SQL-performance gain is
claimed. Evidence: `rejected-grouped-mapping-parent-test-20260914.log` and
`rejected-grouped-mapping-full-tests-20260914.log` in the investigation archive.
