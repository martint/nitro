# Native remote output experiments

The prototype admitted only identity-layout, ordinary partitioned output. It used Trino's existing partition function,
then compacted the native batch by destination and serialized every non-empty destination immediately. TPC-DS q22
passed exact result comparison.

The five-warmup/three-measurement 12 GiB gate was:

| engine | wall p50 (ms) | CPU p50 (ms) | allocation p50 (MiB) | query peak p50 (MiB) |
|---|---:|---:|---:|---:|
| Trino | 6,034.596 | 16,400 | 33,646.273 | 298.277 |
| Nitro candidate | 7,556.455 | 16,907 | 13,556.361 | 294.582 |
| Nitro / Trino | 1.252x | 1.031x | 0.403x | 0.988x |

Despite low allocation, immediate destination serialization repeated native ownership/copy work and emitted
destination fragments without the coalescing performed by the ordinary partition output builders. The Trino prototype
was fully reverted. The next viable design needs destination-owned native accumulation and explicit flush thresholds,
or direct serialization from selected native mappings. No JFR or heap dump was created.

## Accepted buffered output

The follow-up keeps the same capability gate, authoritative Trino assignment, and Nitro destination compaction, but
appends each destination page to Trino's existing per-destination output builder. Normal fullness thresholds therefore
coalesce producer batches before serialization. Exact q22 result comparison and eight focused tests passed.

The same five-warmup/three-measurement 12 GiB gate was:

| engine | wall p50 (ms) | CPU p50 (ms) | allocation p50 (MiB) | query peak p50 (MiB) |
|---|---:|---:|---:|---:|
| Trino | 5,901.918 | 16,036 | 33,700.518 | 291.593 |
| Nitro candidate | 4,577.393 | 10,971 | 13,452.781 | 309.919 |
| Nitro / Trino | 0.776x | 0.684x | 0.399x | 1.063x |

Buffering reduced Nitro join output from roughly 7.1 to 3.2 CPU-seconds and aggregation from roughly 8.1 to 6.0
CPU-seconds. The native partitioned-output path itself remained about 0.83 seconds, but it now produces the page shape
needed by downstream exchange consumers. `q22-buffered-candidate-5w3m.log` is the accepted control.
