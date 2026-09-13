# ADR-0108: Pool non-escaping partition remapping scratch

- **Status:** accepted; remote performance qualification pending
- **Date:** 2026-09-13
- **Spec:** §11, §15, §18
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md),
  [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

Representation-preserving partition copies derive destination dictionary IDs using a temporary lookup indexed by
the source dictionary domain. The lookup was allocated anew for every partition. A q31 allocation capture places
it among the largest allocation sites. Its lifetime ends before copying values or publishing output, whereas the
derived destination IDs can be shared by multiple output vectors and outlive the copy operation.

## Decision

Borrow the temporary lookup from the partitioner's injected primitive pool, initialize every entry, and release it
in a finally block after the remapping loop. Keep the existing array-versus-hash-map admission and size bounds.
The output IDs and selected dictionary positions remain separate storage; returning scratch must never invalidate
an already-published output or an output retained across later partitions.

This closes an allocation-contract gap without changing partition assignments, dictionary admission, output
encoding, reservation ownership, or scheduling. It introduces no policy switch or new resource lifetime.

## Validation

Prove dirty-buffer reuse, independent published IDs, and release on failure. Run full tests and exact warmed
parent/candidate comparisons including allocation bytes, then neighboring SQL workloads. An allocation improvement
is meaningful independently of CPU only if CPU and latency breadth remain acceptable; never call it a CPU win
from allocation counts alone.

The implementation passes 2,152 full-suite tests (zero failures/errors, 394 skipped). Exact local warmed
parent/candidate/parent q31 comparisons reduce median CPU from 5.340/5.393 to 5.073 seconds, with separated
ranges, and allocation by approximately one GB per query. A four-query neighboring workload check also has
separated q31 CPU and allocation ranges, with no established neighboring CPU or latency regression.
Latency ranges overlap: this evidence establishes a local CPU/allocation improvement, not a latency win.
The running remote suite-context investigation uses the preceding production revision; no remote improvement
or synchronized-board revision is claimed until this change is tested there.
