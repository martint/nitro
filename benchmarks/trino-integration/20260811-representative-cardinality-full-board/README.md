# Representative native-source cardinality board

This board qualifies Nitro `7d8defa9` (`Expose mergeable partial aggregation samples`) with Trino integration
`b29ed68d` (`Admit source aggregation from representative key samples`). All suites ran sequentially in fresh JVMs
with JDK 26, five warmups, three measured executions, exact result comparison, a 12 GiB heap, an 8 GiB query-memory
limit, thread-allocation measurement, sampled peak memory, and plan-node/operator metrics. Heap-dump generation was
explicitly disabled. All 103 TPC-DS variants, 22 TPC-H queries, and 43 ClickBench queries passed.

| Suite | Queries | Wall geo | CPU geo | Weighted wall | Weighted CPU | Allocation geo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-DS | 103 | 0.5887 | 0.4208 | 0.6650 | 0.5763 | 0.1815 |
| TPC-H | 22 | 0.7021 | 0.6697 | 0.6887 | 0.7095 | 0.1904 |
| ClickBench | 43 | 0.7674 | 0.5874 | 0.6840 | 0.6420 | 0.2671 |

Across all 168 equal-weight variants, Nitro measures **0.6447x wall, 0.4871x CPU, and 0.2011x valid
allocation**. Workload-weighted wall and CPU are 0.6755x and 0.6190x. The preceding accepted board measured
0.6517x / 0.4906x / 0.2183x, with workload-weighted wall and CPU of 0.6746x / 0.6356x. The new admission therefore
improves equal-weight wall, CPU, and allocation plus weighted CPU, while weighted wall remains effectively flat.

| Suite | Preceding wall/CPU geo | Current wall/CPU geo | Preceding weighted wall/CPU | Current weighted wall/CPU |
| --- | --- | --- | --- | --- |
| TPC-DS | 0.5956 / 0.4184 | 0.5887 / 0.4208 | 0.6953 / 0.5933 | 0.6650 / 0.5763 |
| TPC-H | 0.7118 / 0.6930 | 0.7021 / 0.6697 | 0.7037 / 0.7423 | 0.6887 / 0.7095 |
| ClickBench | 0.7729 / 0.6020 | 0.7674 / 0.5874 | 0.6520 / 0.6555 | 0.6840 / 0.6420 |

TPC-H q21 is the motivating high-cardinality source aggregation. Nitro now observes one bounded sample spread across
a physical batch before constructing grouping state, finds no useful key reuse, and passes all 59,986,052 source
rows through. It measures 1,627 ms / 6,454 CPU-ms versus Trino's 1,920 ms / 7,147 CPU-ms: 0.847x wall and 0.903x CPU. Allocation
is 0.573x and sampled query peak is 0.304x. The preceding accepted board was 0.854x wall / 0.958x CPU.

The initially committed 128-row sparse observation was rejected by the first full board. It treated a locally unique
prefix as evidence that the complete source could not compress, even for large key spaces with substantial reuse.
TPC-DS q02 rose from about 202 ms / 402 CPU-ms to 495 ms / 1,054 CPU-ms, TPC-DS q59 rose to 1,008 ms / 4,648
CPU-ms, and ClickBench q16 rose to 1,882 ms / 4,621 CPU-ms. Raw evidence is preserved under
`20260810-preaggregation-cardinality-full-board/`.

The corrected 4,096-row physical-batch observation preserves those aggregations. TPC-DS q02 is 234 ms / 290 CPU-ms,
q59 is 552 ms / 2,227 CPU-ms, and ClickBench q16 is 1,165 ms / 3,566 CPU-ms. q16 again reduces 99,997,497 input
rows to 21,542,203 source-exchange rows and uses 0.905x Trino CPU. A 10,000-row diagnostic produced the same
classification but added unnecessary q21 sampling work; the smaller evenly spread sample retained both behaviors.

The remaining material compute laggards are narrow: ClickBench q40 is 1.107x CPU (484 versus 437 CPU-ms), TPC-H
q16 is 1.063x (747 versus 703 CPU-ms), and TPC-DS q23a is 1.009x (19,620 versus 19,442 CPU-ms). TPC-DS q20 is
1.269x but only 91 versus 72 CPU-ms. The larger wall-only laggards are short scheduling-scale queries or known
backpressure/parallelism cases: ClickBench q43/q41/q01 and TPC-DS q65 have lower or nearly equal Nitro CPU despite
higher wall time.

For queries where both sampled query peaks exceed 1 MiB, peak-memory geomean / byte-weighted ratios are 0.6337x /
0.8306x for TPC-DS, 0.3081x / 0.5575x for TPC-H, and 0.7019x / 0.9824x for ClickBench. Maximum sampled Nitro query
peaks are 2,088.4 MiB, 1,106.3 MiB, and 4,205.4 MiB respectively, below the configured query limit.

Thread-retirement accounting produced invalid allocation deltas for some rows; raw records are preserved and rows
with non-positive aggregate allocation are excluded from allocation geomeans, matching preceding-board methodology.
No JFR recording, heap dump, or Kata artifact was created.
