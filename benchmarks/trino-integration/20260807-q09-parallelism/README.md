# TPC-DS q09 cross-join plan fidelity

TPC-DS q09 was the board's worst wall-time laggard even though Nitro used less
query CPU.  The accepted five-warmup/three-measurement board measured about
2.46x Trino wall time.

Stage timelines showed that Nitro's leaf work completed sooner than Trino's,
but its root task did not start until the scalar-subquery chain had drained.
The root exchange blocked for only a few milliseconds once scheduled.  The
cause was physical-plan drift: Nitro admitted equality-free joins into its hash
join implementation, while Trino's corresponding physical plan uses nested
loop joins.  That substitution introduced build/probe driver dependencies and
serialized the 15-deep scalar-subquery pipeline.

The production fix makes Nitro hash-join admission require an equality
criterion.  Equality-free joins remain on Trino's nested-loop physical plan.
No Nitro or Trino operator implementation was changed.

The clean post-fix run in `q09-cross-join-plan-fix.log` used both engines, five
warmups, three measurements, a 12 GiB test JVM, exact result comparison,
allocation measurement, and operator CPU metrics:

| Engine | p50 wall | query CPU |
| --- | ---: | ---: |
| Trino | 718.476 ms | 3,633 ms |
| Nitro | 506.768 ms | 2,240 ms |
| Nitro / Trino | 0.705x | 0.617x |

The complete 233-test `TestTrinoNitro*` core cohort passes with no failures or
skips.  Earlier logs in this directory record rejected hypotheses concerning
GC, dynamic-filter waits, shared decompression caching, operator diagnostics,
local planning, and generated-class compilation.  Native `perf` captures are
retained as benchmark evidence; no JFR, heap dump, or Kata artifact was
created.
