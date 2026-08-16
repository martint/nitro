# Compact native partition dictionaries

TPC-DS q23a was run against the analyzed SF10 Parquet inputs in separate fresh 12 GiB JVMs, Nitro only, with five
complete warmups and five measurements. Allocation tracking and operator metrics were enabled. No JFR or heap dump was
created.

| implementation | wall p50 (ms) | CPU p50 (ms) | allocation p50 (MiB) | query peak p50 (MiB) |
|---|---:|---:|---:|---:|
| parent | 6,367.446 | 29,246 | 54,607.669 | 2,995.213 |
| compact destination dictionaries | 6,335.919 | 29,580 | 53,388.024 | 2,934.662 |
| candidate / parent | 0.995x | 1.011x | 0.978x | 0.980x |

The result is duration-neutral and reduces allocation and peak memory. The structural change is broader than this
single query: each destination now copies only referenced dictionary values, computes a shared source mapping once,
and reuses the compact destination IDs across sibling encoded streams. Remapping storage is bounded by the immutable
partition policy even when the source dictionary is much larger.
