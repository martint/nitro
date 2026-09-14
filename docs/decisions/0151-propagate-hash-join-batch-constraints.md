# ADR-0151: Propagate hash-join batch constraints to lazy payloads

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §6, §9, §11, §18
- **Related:** ADR-0132, ADR-0134, rejected ADR-0149

## Context and correlation

HashJoinOperator returned a Batch with a no-op constraint handler. Selecting consumers correctly notified the batch
once under ADR-0134, but the join's materialization mask stayed dense. Its lazy probe-payload constraint therefore
included output rows already rejected downstream. ADR-0149's selected-demand test exposed this separate defect;
the dictionary optimization is rejected and is not required for the repair.

This fills the existing mask propagation/lifetime contract, without reverting to duplicate operator and batch
notifications. The handler runs after Batch invalidates constraint-sensitive output generations. It does not grant
retained-input or constrained-reborrow capabilities that the join does not own, and does not change scheduling,
relational semantics, dictionary representation, defaults or function implementations.

## Decision

Connect the returned batch to the existing producer constrain method so lazy materialization uses the selected
output mask. Keep the producer's existing upstream reborrow/fully-consumed-batch guards. Do not copy this change
blindly into retained-output operators whose callbacks require batch-scoped state rather than the current operator.

## Evidence and qualification

A dedicated behavior test selects 1 of 3 joined rows before borrowing lazy probe payload. The old callback requests
all 3 source rows; the repair requests only 1 and returns the correct value. This fails before the repair independently
of dictionary compaction. Callback-only W20/M15 q22/q67 A/B/A allocation ranges overlap; CPU is not consistently
better or worse across variable fresh-JVM controls. No SQL speedup is claimed from the notification test alone.

The separated implementation passes 2,192 Nitro tests (394 skipped), 644 eligible host tests, and all 409 exact/native
SQL cases across the four suites. The final remote gate completed at 19:28:09 UTC on 2026-09-14. The four previously
established parent-failing host test classes remain excluded from the host total. No scheduler or host production
change is required. No query speedup is claimed from this correctness repair.
See `~/notes/nitro/2026-09-14-native-partition-ownership-qualification.md` for final source and validation evidence.
See `~/notes/nitro/2026-09-14-cumulative-build-domain-qualification.md` for original isolation and negative-test logs.
