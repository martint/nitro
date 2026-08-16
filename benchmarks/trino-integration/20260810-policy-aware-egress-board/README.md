# Policy-aware Nitro egress and ownership-safe partitioning

This board validates a policy-aware Nitro-to-Trino remote-output boundary. Low-reuse dictionaries are flattened
before Trino adaptation, admitted dictionaries are evaluated after selected-base compaction, dense flat null streams
build validity words directly, and canonical dense `CHAR` vectors may be borrowed without a trim/copy pass. Mixed
and flat pages use Trino's columnar partition path with encoding retention disabled, so borrowed Nitro storage is
consumed synchronously and copied into destination-owned buffers.

All runs used JDK 26, five warmups, three measured executions, exact result comparison, a 12 GiB JVM heap, 8 GiB
query memory, thread-allocation measurement, peak-memory reporting, and operator metrics. Heap-dump generation was
explicitly disabled. The complete gate passed 103 TPC-DS variants, 22 TPC-H queries, and 43 ClickBench queries.

| Suite | Queries | Wall geo | CPU geo | Weighted wall | Weighted CPU | Allocation geo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-DS | 103 | 0.5977 | 0.4121 | 0.7011 | 0.5781 | 0.2003 |
| TPC-H | 22 | 0.7346 | 0.6851 | 0.7137 | 0.7215 | 0.1843 |
| ClickBench | 43 | 0.7906 | 0.6207 | 0.6815 | 0.6653 | 0.2888 |

The preceding accepted board was 0.5917/0.4115 TPC-DS, 0.7368/0.7010 TPC-H, and 0.7806/0.6168 ClickBench wall/CPU
geomeans. Equal-query wall shifts are about 1% in TPC-DS and ClickBench, while TPC-H CPU and the expensive-query-
weighted TPC-DS/TPC-H results improve. ClickBench allocation rises from 0.2664x to 0.2888x but remains less than
one third of Trino. Peak-memory geomeans are distorted by several 1 KiB Trino denominators; among queries where both
engines exceed 1 MiB, ClickBench peak-memory geo is 0.8702x and the sum-weighted ratio is 1.0045x, versus 0.7310x and
0.9863x on the preceding board.

TPC-DS q30 in the full warmed board improves from 1.286x to 1.224x wall while remaining CPU-efficient at 0.885x
(previously 0.883x). Trino measured 119.2 ms wall / 244 CPU-ms / 834 MiB allocation; Nitro measured 146.0 ms /
216 CPU-ms / 431 MiB. A separate 20-warmup/five-measurement control measured 1.027x wall / 1.130x CPU and retained
Nitro's allocation advantage at 1.48 GiB versus 1.62 GiB.

An intermediate implementation passed borrowed pages to the encoding-preserving PagePartitioner path. Q21 passed in
a fresh process but failed after q20, receiving q20 strings from recycled Nitro storage. The minimal q20-to-q21
sequence proved that returning from the callback was not a lifetime boundary when the host path could retain encoded
storage. The rejected logs are retained in `../20260810-policy-aware-egress/`. Disabling host encoding retention while
using the ordinary columnar partition path passes that sequence and the complete suite-order gate.

The focused adapter/partitioner selection passes 110 tests, and the final `TestTrinoNitro*` plus
`TestPagePartitioner` cohort passes 270 tests with zero failures.
