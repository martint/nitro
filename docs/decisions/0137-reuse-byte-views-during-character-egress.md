# ADR-0137: Reuse byte views during character egress

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §4–5, §17–18
- **Related:** ADR-0012, ADR-0136

## Context

Repeated parent q22 runs exposed a stable-within-JVM allocation cliff: about 20.8 GB versus 25.1 GB allocated per
query, accompanied by roughly 1.8 seconds more buffered host conversion time. Grouping work counters did not explain
the difference. Retained CHAR conversion already uses allocation-free binary region access, but wraps each value's
backing array in a temporary Slice before invoking the host trailing-space helper. Elimination of those wrappers
depends on JIT inlining and escape analysis. This was a plausible contributor at proposal time; the ordered-prefix
allocation capture described below subsequently confirmed the wrapper-allocation mechanism.

## Decision and correlation

Expose an optional position-invariant backing array through BinaryRegions. Flat binary access supplies it; region,
dictionary and run mappings propagate it without reading a logical position. The default capability is absent, so
future multi-buffer accessors need not pretend to share storage. The array remains borrowed for the binding lifetime;
the capability does not prove immutability or ownership transfer.

Bind one conversion-call-local Slice view when that capability is present, outside the CHAR length loop. Invoke the
same host normalization helper with the same offset and length. Otherwise retain ordinary per-position conversion.
Null or unselected values are not inspected. Output still copies selected bytes into host-owned storage. No cache
survives the conversion, and no allocator ownership changes.

This conforms to the provider-owned logical semantics and allocation-minimizing boundary contracts. It adds no
function semantics to the engine and changes no normalization, physical-policy default, scheduler or buffering rule.
Do not use this as justification to revive ADR-0136's unqualified dictionary-cache admission change.

## Qualification

Cover padded and multibyte text, all-space values, nulls, sparse selections and encoded mappings. Use an allocation-
profiled component benchmark and warmed SQL controls to distinguish eliminated wrapper allocation from final block
payload allocation, which remains necessary at this retaining boundary. Keep the production change only after
correctness and query-neighborhood qualification, and report control-to-control instability explicitly.

The first prototype checked backing-array identity inside the row loop. It reduced worst-case wrapper multiplicity,
but added a branch and mutable loop state; normal JMH cases became 18–45% slower while parent escape analysis already
eliminated most wrappers. That prototype is not the proposed final implementation. Resolving storage once through a
physical capability avoids this extra per-row check and exposes the invariant to consumers directly.

### Completed qualification

The Nitro suite passed 2,182 tests with no failures/errors and 394 skipped at this slice. Host adapter coverage passed
614 tests; four unrelated legacy test classes fail on the byte-identical parent as well, with their failures retained
separately rather than weakening native admission. Additional descendant join work does not form part of this decision.

Ordered TPC-DS q01–q22 controls reproduce the allocation cliff. q22 allocates 25.814/25.783 GB in the two parent
arms versus 21.548/21.544 GB in two candidate arms: a reproducible 16.4–16.5% reduction. The parent allocation capture
contains 40,776 Slice observations under flatBlock, versus nine in a fresh-parent capture that did not exhibit the
cliff. Samples establish allocation sites, not exact byte totals. The mixed-encoding component benchmark improves
15–19% under normal escape analysis; monomorphic cases already eliminate most wrappers and remain approximately flat.

Do not claim a general CPU gain. q22 CPU improved modestly, with some paired ranges overlapping. An initial q09
increase of approximately 2.3–2.5% did not survive further unprofiled prefix qualification: a later candidate used
6.304 seconds versus its control's 6.729 seconds, and a new-SPI/old-host diagnostic was faster still. Profiles put q09
cost in numeric Parquet filtering/decoding, not the changed CHAR conversion. The original full-prefix and shorter-prefix
cohorts are not one synchronized board. Their different outcomes demonstrate sensitivity to JVM execution history;
they do not establish that the SPI capability makes numeric filtering faster. q12/q17 increases also did not repeat in
longer local guards. Keep this bounded, semantically equivalent allocation fix and carry CPU uncertainty into the next
full-board qualification instead of selecting a favorable point ratio or adding a query-specific opt-out.

Evidence is retained under the September 14 character-conversion campaign: the ordered parent/candidate/repeat arms,
the subsequent unprofiled candidate repeat, the q09 SPI-only isolation, allocation/CPU captures, exact/native result
checks, and byte-identity manifests. The synchronized 182-point board remains unchanged until a new complete run.
