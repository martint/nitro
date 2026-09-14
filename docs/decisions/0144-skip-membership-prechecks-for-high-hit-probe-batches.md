# ADR-0144: Skip membership prechecks for high-hit probe batches

- **Status:** rejected
- **Date:** 2026-09-14
- **Spec:** §13, §16, §18
- **Related:** ADR-0129 and ADR-0139 (unchanged)

## Hypothesis and correlation

ADR-0139 requires complete-build membership checks before batched probing. Those checks are a physical prefilter,
not the authority for exact matches. Once repeated high-hit observations admit independent table probes, most
membership checks may be redundant work. Trial omitting them only in that admitted path; retain null checks,
selected-position order, exact collision resolution and pooled probe-local scratch. Scalar and low-hit paths keep
membership rejection, and all capacity, batch-size and adaptive thresholds remain unchanged.

This would amend ADR-0139's prefilter requirement and bound ADR-0129's unconditional pre-lookup endpoint checks
to paths outside high-hit batching. It does not weaken exact membership semantics or alter planning,
dynamic-filter publication or scheduler behavior. The accepted spec remains unchanged pending qualification.

## Qualification required

Verify absent keys inside and outside build bounds, null and inaccessible unselected values, independent prepared
probes, and exit on a distribution shift. Run focused and full tests, then frozen parent/candidate/parent SQL
comparisons covering q09, q13 and unaffected/low-hit joins. The first low-hit batch after a shift may perform extra
table work; measure this tradeoff rather than assuming removing a filter always wins. No enabling flag or dormant
production path will remain if the experiment fails to qualify.

## Outcome

Focused 10 tests and the full 2,191-test candidate pass, including interior/exterior absent keys and adaptive exit.
The first 40-warmup/15-measurement q09 bracket has CPU medians 5.636/5.555/5.666 seconds; all ranges overlap.
q13 is neutral, and q21 also improves despite its previously observed probes not meeting batching admission.
That warns against attributing every between-JVM difference to this code.

The longer 80-warmup/60-measurement q09 bracket gives 5.629/5.600/5.634 CPU-seconds. Ranges again overlap;
the apparent gain shrinks to 0.5–0.6%, with no compensating latency or allocation benefit. This is insufficient
evidence to change the accepted prefilter behavior and trade extra absent-key lookups against a small uncertain gain.
Remove the implementation and candidate-only test. ADR-0129/0139 and the spec remain unchanged; no opt-in path remains.
Retain the patch, immutable runtime and raw samples as negative evidence.

Evidence: `2026-09-14-high-hit-membership-qualification.md`, `high-hit-membership-20260914` and
`high-hit-membership-long-20260914` results. These are local distributed tc4 measurements, not board replacements.
