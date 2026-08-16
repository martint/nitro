# Rejected detached async-exchange storage recycling

This experiment returned backing arrays with the `RecyclableVectorStorage` reset contract to the instance-owned
bounded primitive pool after the final asynchronous exchange lease closed. Vector wrappers were never published
across owners. Focused tests and q22/q23a checks passed, but the subsequent full-suite gate rejected the change.

TPC-DS q02 produced 2,513 duplicated result rows. Reverting storage release made the same five-warmup/three-
measurement exact-result run pass. A downstream grouping representation retains dictionary-backed storage beyond
the lifetime of the input `Batch` and exchange lease, so transport close is too early to recycle its arrays. The
performance below is diagnostic only. Safe reuse requires retained consumers to acquire their own storage lease or
copy retained data into independently owned state.

TPC-DS SF10 Parquet controls used a fresh 12 GiB JVM, five warmups, three measurements, an 8 GiB query limit,
thread-allocation accounting, exact result checks, and the normal Nitro-native Parquet reader.

| q22 adjacent A/B | wall p50 ms | CPU mean ms | allocation mean MiB | query peak mean MiB |
|---|---:|---:|---:|---:|
| unsafe candidate | 4,722.673 | 10,734.000 | 10,951.022 | 310.212 |
| parent | 4,805.186 | 10,922.333 | 12,580.131 | 312.040 |
| candidate / parent | 0.983x | 0.983x | 0.871x | 0.994x |

The normal q23a path also improved against the preceding same-day control: wall p50 moved from 5,566.829 to
5,506.281 ms, CPU mean from 28,388.667 to 27,925.667 ms, and allocation mean from 51,672.455 to 46,187.699 MiB.

A separate diagnostic temporarily enabled raw native input for q23a's adaptive partial aggregation. Recycling cut
allocation to 44,526 MiB from roughly 51,636 MiB in the earlier raw-native experiment, but CPU remained 34,554 ms.
The activation was reverted: storage recycling is useful, but raw retained vectors still do not reproduce the Page
boundary's bounded dictionary representation or adaptive logical-size contract.

A follow-up causal screen supplied the raw native adaptive nodes with their measured Page-equivalent logical rate
(123 bytes per row) instead of retained bytes. q23a remained at 34,487 mean CPU-ms and 44,074 MiB allocation, nearly
identical to the retained-byte run. The input-cost mismatch therefore is not responsible for the CPU regression, and
no speculative cost API was added. The unresolved difference is physical representation and lifetime work.

Plan-node accounting localized the raw-native regression to the two high-cardinality adaptive grouping nodes. Each
rose from about 1.43 CPU-seconds with Page input to 5.11 CPU-seconds with raw native input; join CPU fell by roughly
2.4 seconds, so the native path shifted work rather than merely adding boundary overhead. A Nitro-core experiment
replaced the oversized dictionary's dense lazy global-ID metadata with an exact primitive sparse map. It was rejected:
q23a increased to 35,056 mean CPU-ms and the two nodes remained about 5.4 CPU-seconds each. The candidate was fully
removed. Page's advantage therefore comes from its bounded physical key representation, not dense metadata clearing.

The candidate was fully undone. Nitro commit `6ec25808` keeps detached storage lease-owned and adds an explicit
cross-owner non-reuse regression. The isolated TPC-DS q02 guard passes exact comparison after the revert. No JFR,
heap dump, or Kata artifact was created.
