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

## Symmetric aggregation exchanges

The Trino fixture now terminates each partial aggregation pipeline, copies/hash-partitions its Page
output, and starts the corresponding final aggregation from the exchanged Pages. This matches the
native-vector materialization that the Nitro fixture was already performing for DISTINCT and grouped
presence aggregation. The final global count exchanges one row and remains a negligible harness
special case.

| Metric | Nitro | Trino | Nitro / Trino |
| --- | ---: | ---: | ---: |
| Median elapsed (ms) | 1,730.437 | 2,517.397 | 0.687 |
| Instructions | 35.12B | 66.95B | 0.525 |
| Cycles | 9.788B | 18.94B | 0.517 |
| Branch misses | 32.06M | 105.76M | 0.303 |
| L1 data-load misses | 317.09M | 517.33M | 0.613 |
| Allocation | 497.4 MB | 6.020 GB | 0.083 |

The accepted counter artifact is `q87-symmetric-aggregation-exchanges.json`; the distinct-only
control is `q87-distinct-exchange-symmetric.json`. Wall time for Nitro remains visibly multimodal in
this short run, so normalized counters are the stronger signal. Symmetric exchange materialization
changes the instruction ratio only from 0.531x to 0.525x. The remaining difference from integrated
SQL CPU (0.703x) is therefore not aggregation exchange copying; partitioned join topology and its
remote Page boundaries are the next fixture gap.

## Partitioned-join exchange topology

At task concurrency one, each of q87's date and customer joins still has four material transport
inputs: the sales/date sides of the first join and the joined-sales/customer sides of the second.
The standalone fixtures now copy each input at that boundary while retaining runtime-filter
propagation. Nitro copies native vectors and Trino copies Pages; neither helper invents a scheduler
or a multi-destination partition policy.

| Metric | Nitro | Trino | Nitro / Trino |
| --- | ---: | ---: | ---: |
| Median elapsed (ms) | 1,592.828 | 2,697.290 | 0.591 |
| Instructions | 37.76B | 68.61B | 0.550 |
| Cycles | 12.40B | 19.19B | 0.646 |
| Branch misses | 35.89M | 106.26M | 0.338 |
| L1 data-load misses | 399.74M | 582.28M | 0.686 |
| Allocation | 1.025 GB | 7.650 GB | 0.134 |

The accepted artifact is `q87-join-exchange-topology.json`. Nitro elapsed samples remain multimodal,
but the cycle ratio moves from 0.517x to 0.646x and is now close to the integrated CPU ratio. A fresh
one-warmup/one-measurement SQL attribution check reported 0.671x CPU; the controlled five-warmup gate
remains 0.703x and is still the board value.

The attribution also exposes a physical runtime-filter difference. The standalone Nitro date filter
reaches the Parquet scan before probe consumption and reduces the three sales inputs from about 50.4M
to 9.8M rows. The standalone Trino fixture's synthetic source sends all 50.4M rows. Current integrated
SQL is between those endpoints: its Nitro pipeline sources emitted 11.33M rows, while Trino scan/filter
operators emitted 41.59M. This is not a function-adaptation or join-kernel regression. Runtime-filter
arrival and connector wait policy are part of the SQL execution shape and must be reported separately
from pure operator efficiency; forcing either standalone fixture to the other engine's row counts
would cease to model the corresponding integrated engine.

## Dynamic-filter-disabled control

Disabling dynamic filtering symmetrically removes that row-count confound. With three warmups and
five measurements, the pre-partition-correction standalone fixture reports:

| Metric | Nitro | Trino | Nitro / Trino |
| --- | ---: | ---: | ---: |
| Mean elapsed (ms) | 1,983.640 | 2,785.786 | 0.712 |
| Instructions | 42.613B | 68.460B | 0.622 |
| Cycles | 11.994B | 19.642B | 0.611 |
| Allocation | 2.099 GB | 7.650 GB | 0.274 |

The corresponding integrated SQL control, after five warmups, is effectively at parity: median
wall is 1,085.328 ms versus 1,055.043 ms (1.029x), and median CPU is 3,503 ms versus 3,440 ms
(1.018x). A separate allocation run measured 8,395 MiB for Nitro and 11,858 MiB for Trino (0.708x),
so allocation remains materially better even though CPU does not.

Direct hardware counters around one warmed measured integrated query confirm that this is real work,
not just scheduling noise: Nitro executed 103.001B instructions and 45.055B cycles versus Trino's
86.642B instructions and 39.822B cycles, ratios of 1.189x and 1.131x. The perf events were
multiplexed with comparable coverage, so absolute counts are approximate but the same-run ratios are
the useful signal.

Inclusive plan-node attribution had initially made the Nitro join appear slower. The Nitro hash-join
operator also owns its fused downstream partial DISTINCT. Comparing the composed units instead shows
Nitro saving about 206 ms across the three join-plus-partial-DISTINCT stages. That saving is then
largely consumed by later presence/final grouping, where the Nitro stages use about 193 ms more CPU.

## Task-partitioned aggregation topology

The SQL plan runs the DISTINCT partial/final and presence partial/final stages with two task
partitions. Both fixtures now do the same: partial input is round-robin partitioned and final input is
hash partitioned on the grouping keys. The one-row global count remains single partition because the
standalone grouped-session helper deliberately does not implement ungrouped aggregation.

| Metric | Nitro | Trino | Nitro / Trino |
| --- | ---: | ---: | ---: |
| Mean elapsed (ms) | 1,958.377 | 2,912.936 | 0.672 |
| Instructions | 42.803B | 73.963B | 0.579 |
| Cycles | 12.236B | 21.061B | 0.581 |
| Allocation | 2.149 GB | 8.269 GB | 0.260 |

This is a more faithful physical topology, but it does not close the integrated gap. The standalone
Nitro fixture still represents remote exchanges as native-vector copies; integrated Trino represents
them as Page transport and pays Page-to-vector and vector-to-Page adaptation. The standalone module
also targets stock Trino 479, while the production adapters are in the 484 integration tree, so a
faithful boundary benchmark must live with the integration code rather than copy a second adapter
implementation into the fixture.

## Production Page-boundary encoding

The Trino integration now records top-level flat, dictionary, and RLE channel positions at materialized Nitro Page
ingress and egress. It deliberately does not inspect `SourcePage` blocks because doing so would force lazy scan
materialization. A production-adapter JMH benchmark models q87's two name columns and date column. Five one-second
warmups preceded five one-second measurements:

| Input representation | ns/row | Instructions/row | Cycles/row | Allocation B/row |
| --- | ---: | ---: | ---: | ---: |
| Dictionary names + flat date | 1.667 | 45.689 | 8.766 | 14.432 |
| Flat names + flat date | 5.715 | 179.068 | 30.015 | 29.150 |

The accepted JMH artifact is `/tmp/q87-page-boundary.json`. The benchmark isolates adapter cost and must not be
multiplied by total Page positions without accounting for the actual channel shape.

With dynamic filtering disabled, five warmups and seven alternating integrated measurements report 1.020x median
wall, 1.006x median CPU, and 1.017x mean CPU for Nitro/Trino. Per Nitro query, materialized Page ingress contains only
about 151 thousand dictionary channel positions versus 131.147 million flat channel positions. Egress contains about
10.074 million dictionary positions versus 125.726 million flat positions. Thus Nitro produces useful dictionary
identity, but almost none survives remote transport.

The loss is not in `PagePartitioner` alone. Trino's standard `DictionaryBlockEncoding.readBlock` deliberately expands
the dictionary mapping with `copyPositions`, so even a compact dictionary serialized on the producer becomes a flat
block on the consumer. A trial that flushed every encoded Nitro partition page was rejected and removed: it preserved
the producer encoding but the receiver still flattened it, while producing more network pages. The next integration
slice must negotiate a Nitro-native remote exchange representation between Nitro fragments. Ordinary Trino exchange
semantics must remain unchanged.
