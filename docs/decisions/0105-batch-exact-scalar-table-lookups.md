# ADR-0105: Batch exact scalar table lookups

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §11, §12, §16, §18
- **Depends on:** [ADR-0036](0036-share-physical-key-contracts-across-consumers.md),
  [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

The scalar-long distinct table resolves each hash lookup before starting the next. A local paired ClickBench
q04 profile identifies this table as the dominant Nitro CPU consumer; Cork overlaps independent initial table
loads using bounded batches. Direct primitive-vector accessor specialization did not previously improve this
workload. The opportunity is independent memory work, not another logical-type or carrier interface.

## Decision

For a dense, non-null batch after physical representation admission has completed, stage physical values and
initial hash slots, load initial slot contents independently, and then resolve exact insertion in input order.
The existing table retains its hash, growth, zero-key, novelty, and equality rules. Previously observed hits
remain hits because insertion never removes a key. An initial miss is never proof that insertion is safe:
reload and resolve collisions against earlier inserts in the same batch. Growth invalidates slot coordinates,
but not an already proven exact hit; recompute missing-key coordinates under the current table mask.

The scratch arrays belong to the injected primitive pool, participate in retained-byte accounting, and are
released with the table. Batch size belongs to its immutable instance policy. This is not a feature switch.
Sparse masks, nullable keys, and other admitted physical layouts retain their existing exact algorithms.
The physical accessor remains classloader-neutral and no SQL type or function semantics enter the table.

## Validation required before acceptance

- Exact first-occurrence positions, zero and full-width keys, collisions, within-batch duplicates, growth,
  bounded reads, scratch reuse/release, and carrier/mask/null transitions.
- Exact parent/candidate/reverse-order query measurements and neighboring distinct workloads.
- Full Nitro tests and the relevant SQL gate; remote validation without changing the frozen host protocol.
- Reject and remove the candidate if it does not improve the measured workload with acceptable breadth.

## Acceptance evidence

The complete Nitro suite passed (2,150 tests; zero failures/errors; 394 skipped), as did all 409 SQL members.
Exact local candidate/parent/candidate q04 means were 2.140 / 2.595 / 2.129 core-seconds. Local breadth retained
the benefit without separated CPU regressions in neighboring distinct workloads. A separate sparse borrowed-output
extent defect reproduced on the exact parent as well as the candidate; ADR-0106 fixes that boundary independently.

After that repair, all nine remote breadth queries completed with 20 warmups and seven measurements. A matched
Cork q04 capture reported median CPU 6.980 seconds versus 6.038 for Cork+Nitro (ratio 0.865), and latency
1.182 versus 1.050 seconds (ratio 0.888); both measured ranges were separated. These are targeted captures,
not a silently substituted full-board result. The wider board still contains independent regressions.
