# Rejected repeated initial-aggregation dictionary transport

Post-screen runtime attribution showed that this policy's admission fired zero times in q23b: the intervening
Nitro-to-Page-to-Nitro boundary creates a new backing identity for each batch. The A/B below is retained as raw
evidence, but its original causal interpretation was invalid; the policy and commit were removed. Follow-up native
Project-over-Join aggregation fusion reduced Page crossings and allocation but regressed a fully warmed q23b from
5,958 / 28,413 to 7,515 / 36,713 ms wall/CPU. Dictionary-chain normalization did not recover the loss, and sparse-base
compaction worsened the screen further. Those experiments were also removed.

A final ordering screen normalized before Project, where the former Page boundary actually sat. Flattening all join
columns exhausted the 12 GiB heap, demonstrating that the needed boundary must be projection-aware, encoded, and
strictly batch-lifetime-bounded. No heap dump was created; the Surefire crash-stream artifact was removed.

The final lazy, projection-aware encoded compactor avoided the OOM and recovered most CPU, but a five-warmup,
three-measurement run was still worse than the established Page-separated plan: 6,607 / 30,218 ms wall/CPU,
48,855 MiB allocated, and 3,713 MiB sampled query peak versus 5,958 / 28,413, 48,327, and 2,906. It was removed as
well. The Page boundary remains the faster physical normalization barrier for this shape.

This slice preserves a repeatedly observed flat variable-width dictionary backing through Nitro's initial aggregation
boundary. Admission requires the exact backing identity and content generation twice, uses weak observations, copies
selected IDs and side streams into result-owned storage, leases at most 64 MiB of distinct backing per batch, and
materializes any ownership take before the lease ends.

Fresh q23b Nitro-only runs used five complete warmups in a 12 GiB JVM with an 8 GiB query-memory limit. The detailed
three-measurement A/B is:

| q23b | enabled | disabled | ratio |
| --- | ---: | ---: | ---: |
| wall p50 | 5,958.275 ms | 6,262.349 ms | 0.951x |
| CPU p50 | 28,413 ms | 28,512 ms | 0.997x |
| allocation p50 | 48,326.806 MiB | 50,680.608 MiB | 0.954x |
| sampled query peak | 2,906.446 MiB | 2,885.399 MiB | 1.007x |

Thus the transport saves about 2.35 GiB of allocation per q23b execution without reproducing the rejected
per-generation transfer prototype's roughly 808 MiB peak-memory increase. The allocator-owned-ID cleanup after this
screen removes an additional defensive ID-array copy.

Guardrails use the same five warmups and three measurements. q22 measured 4,582.589 ms / 10,611 CPU-ms / 11,537.133
MiB allocation, matching its preceding 4,573.603 / 10,727 / 11,535.199 control. q67 measured 4,325.523 ms / 13,782
CPU-ms / 23,682.928 MiB, matching its preceding 4,365.132 / 13,703 / 23,645.941 control. Exact SF10 Parquet results
passed. The Nitro full suite passed 1,681 tests, zero failures, and 567 skipped.

Artifacts are `q23b-enabled-metrics.xml`, `q23b-disabled.xml`, `q22.xml`, and `q67.xml`; the first exploratory enabled
run is retained as `q23b-enabled.xml`. No JFR, heap dump, or Kata artifact was created.
