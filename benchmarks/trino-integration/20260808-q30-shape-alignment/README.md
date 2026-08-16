# ClickBench q30 SQL-shape reconciliation

Q30 computes 90 global `sum(bigint)` expressions over `ResolutionWidth + offset`. The previous operator harness used
one multi-file scan/projection/aggregation pipeline, while the SQL plan uses 165 split-local partial aggregations,
an exchange, and one final aggregation. The corrected Nitro harness now reproduces those stages, uses the same shared
Parquet resources and adaptive host-boundary batching as the integration, and preserves Trino's per-input exact
overflow semantics. The corresponding Trino operator harness uses the same partial/final shape.

The exact, SQL-shaped sequential Nitro control is 4,915.06 ms/op and allocates 3.959 GB/op
(`nitro-split-exact.json`). Exact overflow checks do not explain the SQL gap: the earlier unchecked shaped run was
5,368.49 ms/op (`nitro-split.json`). The old Trino operator harness is built against an older Trino dependency and
serializes 165 host pipelines, so it is not used as the current SQL baseline.

The useful control is the actual integrated SQL engine pinned to one CPU after warmup:

| backend | wall p50 (ms) | mean CPU (ms) | allocation p50 (MiB) |
|---|---:|---:|---:|
| Nitro | 7,339.335 | 6,057.333 | 5,273.681 |
| Trino | 20,902.124 | 19,142.000 | 51,246.915 |
| Nitro / Trino | 0.351x | 0.316x | 0.103x |

By contrast, the normal parallel board measured Nitro at about 2,990 ms wall / 21,416 ms aggregate CPU and Trino at
3,120 / 22,302. Pinning Nitro to one CPU therefore trades 2.45x wall time for a 3.54x reduction in consumed CPU. Q30's
90 wide projected vectors and sums saturate shared memory/cache bandwidth under concurrent split execution; this
parallel CPU amplification, rather than function adaptation or a slower aggregation kernel, accounts for the lost
operator-level ratio.

The general follow-up is a planner-owned physical rewrite that can share or eliminate affine aggregate work only
when it proves equivalent SQL overflow behavior. It must not recognize q30, ClickBench, a table, or a fixed arity.

Raw integrated reports are `sql-nitro-one-core.xml` and `sql-trino-one-core.xml`. Runs used JDK 26, a 12 GiB test JVM,
an 8 GiB query limit, five Nitro/three Trino warmups followed by three measurements, and CPU 0 affinity. No JFR, heap
dump, or Kata artifact was created.
