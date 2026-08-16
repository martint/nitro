# Native exchange batch high-water and adaptive pool-trim control

The Nitro/Trino boundary now reports the maximum retained bytes of any native exchange batch per producer operator,
in addition to the existing batch/position/byte totals. This distinguishes a physically oversized vector tree from
idle allocator storage without changing execution.

On TPC-DS q23a, the two adaptive partial aggregations (`6067` and `6125`) produced maximum native batches of only
31.7--31.9 MiB, while their reported operator peaks were about 74--75 MiB. The prior peak therefore was not one
unbounded dictionary tree or an exchange ownership leak; roughly 43 MiB was idle reusable storage overlapping the
largest live output batch.

A general one-time trim when adaptive aggregation first entered passthrough reduced q23a median sampled query peak
from the preceding 2,524.7 MiB control to 2,440.1 MiB. It was not retained: q23a moved to 5,897.969 ms p50 wall,
27,713.2 mean CPU-ms, and 48,961.8 MiB allocation (about +1.7%, +0.9%, and +2.7% versus the preceding isolated
control), while q22 moved to 4,702.857 ms / 10,924.3 CPU-ms with flat allocation (about +2.1%/+0.4%). Rebuilding the
discarded input-pipeline working set costs more than the modest peak reduction repays.

Files:

- `q23a.log`: short diagnostic run before the trim, establishing the 31.7--31.9 MiB maximum batches.
- `q23a-pool-trim.log`: five-warmup/five-measurement trim control.
- `q22-pool-trim.log`: five-warmup/three-measurement non-regression control.

All runs used JDK 26, a 12 GiB JVM, the SF10 Parquet data, the native Nitro reader, an 8 GiB query limit, allocation
and sampled peak-memory measurement, and per-plan-node operator metrics. No JFR, heap dump, or Kata artifact was
created.
