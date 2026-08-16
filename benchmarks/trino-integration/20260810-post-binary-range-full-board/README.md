# Post retained-binary-range full board

This board qualifies Nitro `c8da47fe` (`Copy dense retained binary inputs by range`) with the accepted Trino
integration `85ae484e` (`Optimize Nitro remote page adaptation`). All suites ran sequentially in separate JVMs with
JDK 26, five warmups, three measured executions, exact result comparison, a 12 GiB heap, an 8 GiB query-memory limit,
thread-allocation measurement, sampled peak memory, and plan-node/operator metrics. Heap-dump generation was
explicitly disabled. All 103 TPC-DS variants, 22 TPC-H queries, and 43 ClickBench queries passed.

| Suite | Queries | Wall geo | CPU geo | Weighted wall | Weighted CPU | Allocation geo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-DS | 103 | 0.5863 | 0.4131 | 0.7047 | 0.5871 | 0.2001 |
| TPC-H | 22 | 0.7405 | 0.6970 | 0.7249 | 0.7385 | 0.1860 |
| ClickBench | 43 | 0.7737 | 0.6096 | 0.6688 | 0.6602 | 0.2913 |

Across all 168 equal-weight query variants, Nitro measures 0.6490x wall, 0.4887x CPU, and 0.2182x valid allocation.
The corresponding preceding board was 0.6596x / 0.4891x / 0.2178x. Workload-weighted wall improves from 0.6932x to
0.6902x; weighted CPU moves from 0.6308x to 0.6343x. The latter shift is dominated by known long-lived-JVM TPC-DS
rows whose fresh controls already clear, rather than a broad compute regression.

The per-suite comparison against the immediately preceding accepted board is:

| Suite | Preceding wall/CPU geo | Current wall/CPU geo | Preceding weighted wall/CPU | Current weighted wall/CPU |
| --- | --- | --- | --- | --- |
| TPC-DS | 0.5977 / 0.4121 | 0.5863 / 0.4131 | 0.7011 / 0.5781 | 0.7047 / 0.5871 |
| TPC-H | 0.7346 / 0.6851 | 0.7405 / 0.6970 | 0.7137 / 0.7215 | 0.7249 / 0.7385 |
| ClickBench | 0.7906 / 0.6207 | 0.7737 / 0.6096 | 0.6815 / 0.6653 | 0.6688 / 0.6602 |

TPC-DS q30, the profile-proven activation query, reduces Nitro CPU from 216 to 207 CPU-ms and allocation from
431.3 to 420.9 MiB. Its wall ratio rises from 1.224x to 1.295x only because the Trino side moves from 119.2 to
109.6 ms; Nitro itself moves from 146.0 to 142.0 ms. The adjacent repeated profile had already established stable
Nitro CPU at 476--479 ms versus roughly 486 ms before the retained-range change, and removed
`JoinBufferSupport.copyBinaryPositions` from the hot-method board. The complete sweep therefore corroborates the
localized improvement without attributing scheduler-scale wall movement to the kernel change.

The largest raw CPU ratios remain previously qualified shapes: TPC-DS q23a/q57 and ClickBench q40 are sweep-history
or bounded lifecycle rows with fresh controls at approximately parity or better; TPC-H q16 remains a small near-parity
edge. ClickBench q12/q19/q31 are long enough to establish aggregation CPU parity, not a regression large enough to
justify changing an already-qualified operator kernel. The next useful work remains a structural boundary reduction
or a planner-supplied general capacity signal, supported by targeted evidence rather than raw sequential rank.

Thread-retirement accounting produced impossible negative allocation deltas for four TPC-DS rows and one ClickBench
row; those raw records are preserved and excluded from allocation geomeans. For queries where both sampled query
peaks exceed 1 MiB, peak-memory geomean / byte-weighted ratios are 0.7163x / 0.8897x for TPC-DS, 0.1680x / 0.5286x
for TPC-H, and 0.7704x / 0.9676x for ClickBench. Maximum sampled Nitro peaks are 2,207.1 MiB, 959.5 MiB, and
4,205.4 MiB respectively, all comfortably below the configured query limit.

No source change, JFR recording, heap dump, or Kata artifact was created by this qualification sweep.
