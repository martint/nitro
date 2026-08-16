# Core execution-path audit and profiles

This capture audits whether the synchronized three-suite SQL board performs avoidable work in scan, filter,
projection, and aggregation, then profiles representative scan/filter/TopN and high-cardinality aggregation shapes.
Measurements use JDK 26, five warmups before measurement, a 12 GiB heap, an 8 GiB query-memory limit, native Nitro
Parquet input, and the same deterministic SQL and data for both backends.

## Work parity audit

- 645 of 647 matched source plan nodes consume exactly the same physical input-position count in Nitro and Trino.
- The two differences, TPC-DS q05 and q54, are beneficial: Nitro consumes fewer positions after dynamic filtering.
- Remaining scan-output differences are explained by fused filtering, aggregation, or dynamic filtering. No case shows
  Nitro scanning extra source rows relative to the matched Trino plan node.
- Projection generated paths report 60,041 successes with no layout or error fallback. Dictionary flattening is only
  1.07 million positions over all three suites.
- Across the board's aggregation diagnostics, grouping consumes 583.0 of 767.5 aggregate-input CPU-seconds (76%);
  accumulation consumes 95.3 seconds and output 0.38 seconds. Grouping is therefore the primary aggregate target.

## ClickBench q24: filtered wide TopN

The query scans 99,997,497 rows, evaluates `URL LIKE '%google%'`, retains 15,911 filter survivors, and returns ten
wide rows ordered by event time. JFR attributes 41.7% of Java samples to Snappy decompression, 9.9% to Parquet page
decode, 5.5% to segment bounds checks, and 9.5% collectively to RLE decode. LIKE evaluation is 1.6%. The scan is
therefore decompression/page-decode dominated rather than expression dominated.

The audit found a lifecycle gap in the host-driven TopN session: lazy payload was resolved against all filter
survivors before the session copied its retained candidates. The session now feeds its retained positions back into
the batch mask before resolving payload. In otherwise identical seven-execution source-work captures, selected
payload decode falls from 3.386 million to 1.613 million positions (52%). The source publishes 1,407 local candidates
from 15,911 filter survivors; subsequent Trino TopN stages reduce those candidates to ten rows.

This work reduction does not yet move end-to-end q24. Whole-page binary decode remains exactly 13.497 billion
reported positions across the capture, TopNSession still allocates about 10.4 GiB per measured query, and warmed CPU
remains roughly 19--21 CPU-seconds. The harness's thread-local allocation sample varied from 12 to 35 GiB according
to worker scheduling and is not accepted as evidence; Nitro allocator counters are the reliable attribution here.

Residual q24 CPU remains dominated by compressed input work. Fragmented selected binary decoding was tested and
rejected because sparse survivors touch most compressed pages: it retains the decompression work while adding offset
construction and losing dictionary encoding.

The five-query hardware capture records 1.525 trillion instructions, 946.8 billion cycles (1.61 IPC), 299.6 billion
branches with 1.53% misses, 4.86 billion cache misses, and 323.7 million dTLB-load misses. Counters were multiplexed
at approximately 62% coverage. Compared with q33, q24 has lower IPC and substantially more branch misses, consistent
with compression, page-format decode, and variable-width control flow rather than hash-table locality.

## ClickBench q33: high-cardinality pair grouping

The query groups 99,997,497 rows by `(WatchID, ClientIP)`, computes count, sum, and average, then returns the ten
largest groups. The warmed Nitro capture measures 8,541 ms wall, 17,171 CPU-ms, 29,373 MiB thread allocation, and
4,205 MiB query peak memory.

Aggregation diagnostics attribute 11.81 of 13.09 aggregate-input CPU-seconds (90%) to group assignment, 1.00 seconds
to accumulation, and 0.28 seconds to state preparation. JFR attributes 39.8% of Java samples to flat-table lookup,
9.2% to fixed-record chunk addressing, 9.5% to key insertion/copy, and 2.5% to average accumulation. Perf attributes
about 7.5% of sampled cycles to G1 work.

The hardware-counter capture over five measured queries records 1.334 trillion instructions, 649.4 billion cycles
(2.05 IPC), 260.0 billion branches with 0.32% misses, 5.02 billion cache misses, and 1.03 billion dTLB-load misses.
Counters were multiplexed at approximately 62% coverage, so absolute counts should be normalized before comparison;
the ratios still identify the flat hash/record representation as locality-bound.

This SQL result reproduces the current Nitro operator benchmark closely: 8,541 ms here versus 8,479 ms in the
operator board. The remaining SQL-vs-Trino ratio gap is not caused by a slower Nitro aggregation implementation;
it comes from CPU outside the aggregation operator, especially partitioned exchange, page serialization, and scan.
Nitro already generates arbitrary-arity multi-long batch kernels. The remaining low-level target is to bring the
compact packed-identity representation and locality advantages of the admitted flat path to general fixed-width
layouts without losing the generated kernel's register-resident keys and monomorphic loop. The two-field q33 layout
is the first validation target, not an arity-specific execution design.

A smaller append-chunk pointer cache was rejected by a forward/control/confirmation sequence. The confirmation was
8,595 ms wall / 17,793 CPU-ms versus the reverse control's 8,610 / 17,638, so the apparent first-run wall improvement
was not causal. Chunk addressing in the profile is part of the broader random record-access path; caching only the
sequential append chunk does not remove it.

## Artifacts

- `q24-profile/`: accepted JFR and perf call-stack capture.
- `q24-topn-retained-constraint-w5m5.log`: paired q24 validation of retained-position feedback.
- `q24-source-work-w5m1.log`: column-level decode, skip, publish, and copy accounting.
- `q24-source-work-retained-w5m1.log`: the corresponding capture after retained-position feedback.
- `q24-stat/`: hardware-counter capture over five warmed measured executions.
- `q33-profile/`: accepted JFR and perf call-stack capture.
- `q33-stat/`: hardware-counter capture over five warmed measured executions.
