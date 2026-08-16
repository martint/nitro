# ClickBench q31 SQL-shape reconciliation

All SQL measurements use 10 warmups followed by 5 measurements, a 12 GiB JVM,
an 8 GiB per-node query limit, the Nitro Parquet reader, and thread-allocation
measurement.

## Operator-shape correction

The earlier ~5x operator result attributed to ClickBench q31 was actually the
TPC-DS q31 row.  The source-consistent pre-architecture ClickBench q31 operator
result was 2515.9 ms for Nitro versus 3307.8 ms for Trino (1.315x).

`split-isolation.json` establishes that physical split isolation alone costs
about 7.7% (2848.1 ms versus 2644.3 ms for the whole-directory direct shape),
but materially increases allocation.  The corrected SQL-shaped harness adds
source-driver-local adaptive partial aggregation, native hash exchange, final
aggregation, average finalization, and TopN.  It initially retained every split
pipeline and exhausted a 12 GiB heap (`sql-shape.json` and
`sql-shape-lazy.json`).  Giving each lazily-created split pipeline a query-local
allocator fixed the lifecycle and produced 3146.6 ms/op and 4.72 GiB/op
(`sql-shape-query-allocator.json`).

That intermediate shape still reset adaptive aggregation for each of 165
physical splits.  The final harness uses eight long-lived source-driver
pipelines, so the controller observes the same cross-split cardinality history
as SQL execution.  With 5 warmups and 5 measurements
(`operator-driver-adaptive.json`), q31 is 3718.4 ms and 2.50 GB/op, q32 is
4308.6 ms and 4.30 GB/op, and q33 allocates 20.17 GB/op.  q33's measured wall
time ranged from 15.84 to 22.36 seconds with 0.68--2.44 seconds of GC; its
low-GC result and a separate warmed smoke result (16.36 seconds) agree with the
SQL engine's 17.39 CPU-seconds.  The prior always-aggregate q33 shape took
26.17 seconds and 29.36 GB/op.

## SQL regression isolation

The accepted bounded adaptive-output implementation measured 5.287 s CPU and
9.38 GiB allocation for Nitro q31 (`sql-candidate.log`).  Two controls isolate
the cause:

| control | CPU | allocation | conclusion |
|---|---:|---:|---|
| Force Trino Page exchange | 5.494 s | 10.56 GiB | Native exchange is beneficial |
| 64K-row adaptive output batches | 5.469 s | 10.57 GiB | Larger bounded slices do not solve it |
| Preserve each memory-bounded adaptive flush | 3.810 s | 5.29 GiB | Dense slice materialization is the regression |

The adaptive grouped aggregation retains its complete state until every output
slice has been consumed.  Bounded output therefore does not release that state;
it materializes dense slices, loses useful dictionary structure, and makes the
exchange recopy/partition the same logical output in a less efficient form.
Each adaptive flush is already bounded by the host-owned partial-aggregation
memory policy, so preserving it as one native batch is both the semantic and
efficient boundary.

## Production candidate

The construction-owned aggregation policy now carries a separate native-source
adaptive-flush output limit.  Fused native source pipelines preserve a completed
memory-bounded flush, while ordinary aggregation operators and terminals retain
Nitro's normal bounded streaming behavior.

`sql-guarded-q31.log`:

| engine | wall p50 | CPU p50 | allocation p50 | tracked query-memory p50 |
|---|---:|---:|---:|---:|
| Nitro | 788.9 ms | 4029 ms | 5.07 GiB | 192.1 MiB |
| Trino | 1103.7 ms | 5347 ms | 11.23 GiB | 248.1 MiB |
| Nitro / Trino | 0.715x | 0.754x | 0.451x | 0.775x |

This restores the historical ~3.9 CPU-second Nitro result and closes the main
gap between the corrected SQL-shaped operator benchmark and the SQL engine.

The topology guard matters.  Applying intact output globally increased q33's
tracked peak from about 4.21 GiB to 5.06 GiB without improving CPU.  With the
guard, q33 remains stable at 8.54 s wall, 17.39 s CPU, 28.47 GiB allocated, and
4.21 GiB tracked peak versus Trino's 9.36 s, 19.74 s, 29.05 GiB, and 4.91 GiB
(`sql-guarded-q33-no-shared.log`).  A proposed shared count/sum/average update
kernel was also rejected: it made q33 enter full-GC pressure and eventually hit
the page-transport timeout, despite a small bounded-q31 win.
