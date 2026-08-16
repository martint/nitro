# Post-sparse TPC-DS q23 controls

These fresh controls measure commit `c82f2235` with a 12 GiB test JVM, five
query-specific warmups, five alternating measurements, exact result
comparison, allocation and peak-memory sampling, operator CPU, plan-node, and
boundary metrics. No JFR, heap dump, or Kata artifact was created.

| Query | Wall ratio | CPU ratio | Allocation ratio | Query peak MiB (Nitro / Trino) |
| --- | ---: | ---: | ---: | ---: |
| q23a | 1.002x | 1.008x | 0.678x | 2,851.8 / 2,333.6 |
| q23b | 0.881x | 0.925x | 0.654x | 2,843.4 / 2,504.8 |

The long-suite q23 CPU rows do not represent stable regressions. q23a is
within one percent of CPU parity and q23b is a clear Nitro win. Existing
experiments had already rejected blanket dictionary ownership transfer,
per-batch dictionary compaction, and local key-copy specializations because
they retained upstream generations, added remapping work, or regressed q22.
The fresh results do not justify reviving those production changes.

Nitro's higher sampled query peak is repeatable and remains a separate memory
issue associated with dictionary-backed intermediate composite keys. Nitro
still allocates about one third fewer total bytes than Trino on both variants.
