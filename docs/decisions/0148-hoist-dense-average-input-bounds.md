# ADR-0148: Hoist dense average input bounds

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §5, §16, §18
- **Related:** ADR-0006, ADR-0012

## Motivation and correlation

Fresh JVMs running the same ClickBench prefix can settle into distinct q02 CPU states. An external
per-iteration profile captured 70.9% of slow-iteration driver samples in the registry-provided global
average loop, compared with 10.0% in a subsequent fast iteration. This localizes the cost but does not
prove which compilation or inlining decision caused it.

The admitted null-free, all-selected loop currently asks the general Mask for its maximum position on
every iteration. Resolve that invariant once per input batch. Preserve selected bounds rather than
using backing-vector capacity, sequential floating-point accumulation, dictionary indirection, null
fallback and provider ownership. This conforms to the existing physical-kernel and function contracts;
it introduces no engine knowledge of average, new execution policy, scheduler change or enabling flag.

## Qualification

Cover flat and dictionary I32/I64/F64 input, changing batch bounds, oversized backing vectors, sparse
fallback, and floating-point input order. Retain the existing empty-mask precondition of the admitted
path by resolving maxPosition rather than silently substituting backing size.

Run focused and eligible host tests, followed by frozen parent/candidate/parent query trials in fresh
JVMs using the unchanged prefix and adaptive protocol. Inspect the complete warmup/measurement series;
do not select a fast tail or replace a board cell with profiled timing. Qualify neighboring queries before
acceptance. Remove the trial if it does not establish a useful complete-query improvement.

Evidence: `~/notes/nitro/2026-09-14-q02-runtime-state-localization.md`.

## Outcome

Accept the bound hoist as a lower-tier/startup efficiency improvement, not as a proven steady-state q02 win.
All 61 focused aggregation tests, 643 eligible host tests, and 2,190 Nitro reactor tests passed; the reactor
has 394 skips. The complete distributed native gate passed all 409 queries across the four suites. The
eligible host gate retains the same four documented parent-failing class exclusions.

In a short isolated probe of the actual provider, C1-only candidate/parent time per row was 0.167–0.516
across the six vector forms. Ordinary tiered loops were essentially unchanged. Frozen local warmed q02
CPU medians were 0.315/0.310/0.321 seconds in parent/candidate/parent order; neighboring query ranges
overlapped. Six remote fresh-JVM prefix runs all reached the fast measured state, including the parents.
The candidate's first q02 warmups used 17.3–18.3 seconds CPU versus 22.5–80.8 for parents, consistent
with the isolated lower-tier improvement, but no exact same-execution compiler-state attribution is claimed.

Do not replace the nonstationary board cell with a diagnostic fast tail or declare that slow measured states
are impossible. Retain that validation item for the next synchronized board. The same bound pattern occurs
in sibling grouped average, sum-bigint and count-all loops; inspect and qualify those separately.

Full evidence: `~/notes/nitro/2026-09-14-average-bound-qualification.md`.
