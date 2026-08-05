# TPC-DS q87 native local exchange

Trino change `5afc38be` lets composed Nitro pipelines negotiate native input and output across the
partitioned local exchange in q87. The ordinary Trino exchange remains unchanged.

## Boundary attribution

| Metric | Page path | Native path | Difference |
| --- | ---: | ---: | ---: |
| Page ingress positions | 23,740,693 | 22,838,765 | -901,928 |
| Page egress positions | 25,241,792 | 24,339,864 | -901,928 |
| Native batches | 0 | 6 | +6 |
| Native rows | 0 | 901,928 | +901,928 |
| Native bytes | 0 | 44,868,276 | +44,868,276 |
| Producing projection output CPU | ~19 ms | 3.4--3.6 ms | ~-15.5 ms |
| Local exchange sink CPU | ~23 ms | ~40 ms | ~+17 ms |

The native transfer removes exactly one Page egress/ingress round trip. The current general native
hash partitioner consumes the saved CPU while hashing and compacting independently owned partition
batches, identifying partition-output construction as the next optimization target.

The benchmark fixes task concurrency at one, so this exchange has only one destination. A follow-up
ownership-transfer path avoids hashing and copying in that case. It retains the same detached batch;
real fan-out continues to use the partitioner's compact, independently owned outputs. Local exchange
sink CPU falls from roughly 40 ms to 0.7 ms.

## Warmed alternating SQL gate

Five warmups preceded seven measurements of each engine.

| Engine | Median wall (ms) | Median CPU (ms) | Mean CPU (ms) |
| --- | ---: | ---: | ---: |
| Trino | 1,033.325 | 3,164 | 3,161.714 |
| Nitro | 951.234 | 2,267 | 2,285.143 |
| Nitro / Trino | 0.921 | 0.717 | 0.723 |

Across the seven measured Nitro queries, native exchange transferred 42 batches, 6,313,496 rows,
and 314,077,932 bytes. Page ingress still handled 159,871,355 positions and Page egress handled
170,379,048 positions, principally because remote exchanges retain the explicit Page transport
boundary.

## Single-partition ownership-transfer gate

Five warmups again preceded seven alternating measurements.

| Engine | Median wall (ms) | Median CPU (ms) | Mean CPU (ms) |
| --- | ---: | ---: | ---: |
| Trino | 1,077.607 | 3,163 | 3,184.143 |
| Nitro | 945.561 | 2,223 | 2,216.857 |
| Nitro / Trino | 0.877 | 0.703 | 0.696 |

The native exchange still transfers 42 batches, 6,313,496 rows, and 314,077,932 bytes across the
seven measurements. Result comparison is exact. The roughly 39 ms/query sink reduction explains the
CPU ratio movement from 0.717 to 0.703; the remaining integrated/operator gap is elsewhere in the
distributed plan.

## Current-plan fixture correction

A fresh distributed explain showed that the fixture's sales-key DISTINCT does not exist in the SQL
plan. Removing it from both operator fixtures preserves exact results and produces the following
three-warmup/five-measurement result:

| Metric | Nitro | Trino | Nitro / Trino |
| --- | ---: | ---: | ---: |
| Median elapsed (ms) | 878.782 | 2,257.706 | 0.389 |
| Instructions | 22.78B | 63.14B | 0.361 |
| Cycles | 5.326B | 19.02B | 0.280 |
| Branch misses | 17.96M | 94.57M | 0.190 |
| L1 data-load misses | 189.24M | 542.25M | 0.349 |
| Allocation | 282.2 MB | 5.666 GB | 0.050 |

The accepted counter artifact is `q87-current-plan-no-early-distinct.json`. The fixture still
collapses partitioned joins, remote exchanges, and partial/final aggregation pairs, so 0.389x is not
yet the integrated SQL target.

## Partial/final topology correction

Adding the physical plan's partial/final DISTINCT, presence-sum, and global-count stages produces:

| Metric | Nitro | Trino | Nitro / Trino |
| --- | ---: | ---: | ---: |
| Median elapsed (ms) | 1,508.952 | 2,444.916 | 0.617 |
| Instructions | 34.99B | 65.85B | 0.531 |
| Cycles | 9.479B | 18.28B | 0.519 |
| Branch misses | 31.55M | 100.63M | 0.313 |
| L1 data-load misses | 306.09M | 512.73M | 0.597 |
| Allocation | 497.5 MB | 5.930 GB | 0.084 |

The accepted counter artifact is `q87-partial-final-topology.json`. The warmed integrated CPU ratio
is 0.703x, leaving about nine ratio points to explain. Partitioned joins and remote exchanges remain
collapsed, and exchange materialization is not yet symmetric between the two fixtures.
