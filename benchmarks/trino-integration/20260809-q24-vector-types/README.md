# ClickBench q24 cross-allocator vector reuse

The vector-type diagnostic split the production-range versus whole-file allocator delta as follows:

| vector type | 165 ranges | 100 whole files | range delta |
|---|---:|---:|---:|
| BinaryVector | 3,446,001,168 | 2,286,059,535 | 1,159,941,633 |
| I32Vector | 570,160,156 | 368,569,236 | 201,590,920 |
| BooleanVector | 177,841,894 | 117,157,804 | 60,684,090 |
| ErrorVector | 150,181,749 | 97,965,252 | 52,216,497 |
| I64Vector | 143,343,856 | 93,342,920 | 50,000,936 |
| F64Vector | 35,760,000 | 23,280,000 | 12,480,000 |
| Dictionary/RLE | 259,251,840 | 257,571,504 | 1,680,336 |
| total | 4,782,540,663 | 3,243,946,251 | 1,538,594,412 |

Binary vectors explain 75.4% of the split-induced vector allocation. The structural miss was allocator lifetime:
each Trino split owns an allocator, allocator-local vector pools ended with the split, and the explicitly owned
`AllocationResources` pool offered only exact-capacity lookup even though variable-width vectors use ceiling-capacity
reuse.

Nitro commit `30e1ed7b` now transfers idle pooled vectors into that bounded instance-owned pool when an allocator
closes and lets ceiling-capacity callers borrow the smallest sufficient retained buffer. It does not introduce a
static pool or change Trino split scheduling.

The warmed three-measurement candidate produced:

| metric | before | after |
|---|---:|---:|
| wall p50 | about 5,103 ms | 4,968 ms |
| CPU mean | about 21,288 ms | 21,256 ms |
| thread allocation | about 35.7 GiB | 33.1 GiB |
| Nitro vector allocation | 4.783 GB | 1.895 GB (last one-measurement control) |
| BinaryVector allocation | 3.446 GB | 0.544 GB (last one-measurement control) |
| sampled query-memory peak | about 146--181 MiB | 347 MiB mean |

The optimization removes roughly 2.9 GB of vector creation and about 2.6 GiB of measured JVM allocation without
changing CPU work. It trades some live capacity for reuse; the retained owner remains bounded by the existing
`PrimitiveArrayPool` byte ceiling. Nitro's full suite passed 1,696 tests with 567 skipped.

`q24-current-artifacts.log` and `q24-whole-file.log` are the vector attribution controls.
`q24-cross-allocator-candidate.log` and `q24-cross-allocator-candidate-3m.log` are the accepted candidate runs.
The earlier `q24.log`, `q24-driver-metrics.log`, and `q24-vector-total.log` loaded stale snapshot artifacts from a
different local Maven repository and are retained only as diagnostic history. No JFR, heap dump, or Kata artifact
was created.
