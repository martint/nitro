# ADR-0122: Report late admission observations

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §16
- **Related:** [ADR-0116](0116-record-completed-pipeline-timing-evidence.md)

## Evidence

ClickBench q39's source diagnostics reported zero cardinality observations despite sampling and choosing
pass-through. A local execution trace found two observations after an earlier empty snapshot had marked the shared
admission state as already reported. Reading those zeros as absence of sampling misidentified the cause of extra
exchange rows. The aggregate/pass-through flush counters already reported incremental deltas; admission counters
incorrectly used a one-shot snapshot.

## Decision

Shared admission state reports the change since its previous report, under its existing synchronization. Later
sources can publish observations made after an early source reported. Repeated reports without new observations
contribute zero, so summing driver/operator metrics counts shared evidence once. Fixed primitive counters retain
the previous reported values; no row-level allocation or hot-loop instrumentation is introduced.

This repairs the existing observability contract; it does not change admission, sampling cadence, partial aggregation
thresholds, source ownership, plans or scheduling. Do not reinterpret historical zeros as measured absence of work.

## Validation

A regression test reports an empty snapshot, samples through another control sharing the admission state, and
reports through both controls again. It reproduces missing observations before the fix and verifies complete,
nonduplicated counters afterward. All 28 adaptive-partial-aggregation tests pass, along with 15 aggregation, 127
page-processor and 9 source-aggregation tests. Local q39 W30/M15 passes exact results and native-island checks;
its source now reports five observations and two bypass decisions instead of zeros. No admission change is made.

Evidence: `q39-admission-metrics-*-tests*-20260913.log` and the September13 scalar/admission investigation notes.
