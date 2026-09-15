# ADR-0159: Stabilize physical null-accessor receivers

- **Status:** rejected; implementation and trial-only tests removed after remote SQL regression
- **Date:** 2026-09-15
- **Spec:** §5–6, §11–13, §16–18
- **Related:** ADR-0007, ADR-0011, ADR-0027, ADR-0036
- **Evidence:** `~/notes/nitro/2026-09-15-grouping-workload-history.md`

## Context and correlation

With identical accepted binaries and source rows/bytes, q22 consumes 13.633 CPU-seconds in a fresh query cohort
and 17.010 after a full-suite prewarm. q67 likewise rises from 12.937 to 14.827 seconds. Forty target-query warmups
do not remove the difference. A separate history/fresh/history profile bracket reproduces higher CPU in both
history arms. Interface-dispatch samples grow substantially, especially beneath physical-key null access.

The physical key layout already uses one reusable receiver class for integer fields, but its null accessor array
contains encoding-specific lambdas. These receiver classes vary across fields and workloads at shared hot call
sites. An isolated monomorphic benchmark does not exercise that mixed history.

The proposal conforms to the physical-capability and explicit batch-lifetime contracts; it fills an implementation
gap rather than changing logical null semantics, physical admission defaults or host scheduling. It does not claim
that all history sensitivity comes from this one mechanism. The implementation was evaluated and removed; it does
not change the accepted specification or null-access contract.

## Evaluated proposal (not adopted)

Bind each layout field's null access through one owner-held concrete receiver. Direct flat, dictionary and simple
region shapes use borrowed arrays and mappings; other encodings retain their exact existing accessor behavior.
Existing pooled dictionary composition, all-null/null-free batch proofs and generated-kernel contracts remain.
Rebinding occurs once per batch, not per position. Ordinary hot reads and generated equality receive the same
receiver family. No query, function or logical type is recognized and no new feature flag or public SPI is added.

Clear borrowed references when a batch ends, when a field becomes constant/null-free and when layout storage is
released. The receiver owns no vector storage and must never release a borrowed mapping. Reuse removes common-shape
per-batch accessor allocation; bounded owner-held metadata and its reference array remain. Complex fallback bindings
may still allocate their existing wrappers. Do not label the whole engine allocation-free on that basis.

## Qualification

Test flat, dictionary, nested dictionary, region/dictionary orderings, multi-run RLE and concatenated null streams,
including nonmonotonic positions, successive rebindings and intervening constant/null-free batches. Existing
generated hashing/equality, grouping, joins, distinct, selection and lifetime tests remain mandatory.

Compare frozen parent/candidate/parent SQL after identical full-suite histories and with fresh histories, followed
by neighboring queries from all four suites. Keep the unrelated payload-floor candidate fixed on both sides.
Measure whole-query CPU, latency ranges, allocation and peak memory; verify source/intermediate work and profile
the actual dispatch change. A changed sample share alone is not sufficient. Remove an unqualified implementation.

Focused tests pass 181 cases; full tests pass 2,199 cases with zero failures/errors and 394 skipped. The normal-flag
history-matched parent/candidate/parent bracket reduces q22 CPU by 2.3–4.9% and q67 by about 2.6%, with separate CPU
ranges against both parents. Source rows and bytes match; allocation and peak-memory ranges overlap.

Fresh-history q22 is between the two parent controls, not a reproducible gain. History-profile runs remove sampled
interface dispatch at the targeted null-read site, but other unchanged dispatch sites vary too; do not assign the
entire CPU change to this mechanism. The three-suite neighboring checks show no CPU loss with separate ranges
against both parents. Nested row/map checks preserve results and source work; a short map-query latency warning
does not repeat in a longer reverse bracket. Larger Coverage b06 CPU, latency, allocation and peak ranges overlap
both controls. These bounded results are not an accepted board update or proof that this change removes the entire
workload-history penalty.

## Decision and rejection evidence

Remove the reusable null-accessor implementation and its trial-only test. Keep the previously accepted production
path, with no opt-in variant or dormant feature flag. Preserve the experiment's source snapshot, tests, profiles,
measurements and reasoning outside the working copy for reproducibility.

The normal-flag remote full-history bracket reverses the local q22 result: candidate CPU is 54.870 seconds versus
54.330 and 52.732 seconds in the parents. Candidate ranges are higher and separate from both controls, ratios
1.0100 and 1.0406. Input rows and bytes match. Latency, allocation and peak-memory ranges overlap; no corresponding
memory benefit compensates for the CPU loss. q67 is 46.820 / 48.012 / 48.816 CPU-seconds: the candidate loses
separately against the first parent but overlaps the final parent. It establishes neither a reproducible remote
gain nor a separated loss against both controls. All arms passed independent completed-history and manifest audits.

The complete local TPC-H combined candidate also produces a q03 warning (2.802 versus 2.652 / 2.641 CPU-seconds,
separate), but a subsequent prefix bracket holding the payload default fixed does not reproduce it. Do not assign
that warning to this revision or discard it as proved noise. The repeated remote q22 loss is sufficient to reject
this implementation even though the local history-sensitive behavior is a real remaining investigation.

Eliminating interface-stub samples is a mechanism observation, not a portable whole-query improvement. The exact
compiler/receiver-state reason for the remote reversal is not established by these captures. Future work must
demonstrate a different mechanism and cross-machine SQL benefit rather than relabel this trial as a cleanup.

After removal, the restored full suite passes 2,198 tests, zero failures/errors, 394 skipped. Production and test
source exactly match the accepted parent. The stale compiled trial-only receiver class was moved out of build output
into the retained experiment artifacts; no trial implementation remains on the normal runtime classpath.
