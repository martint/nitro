# Nitro SQL structural audit

## Findings

- Nitro native and Trino `Page` encoding attribution agrees at every instrumented producer in TPC-DS q22 and
  q87. Dictionary vectors remain dictionary blocks, flat vectors remain flat blocks, and neither query exposes a
  hidden RLE or dictionary flattening step at the adapter.
- TPC-DS q87's customer joins contain fused distinct aggregation. Comparing only
  `TrinoNitroHashJoinOperator` with `LookupJoinOperator` assigns the aggregation work to Nitro but not Trino. At
  stage 2, Nitro's fused join plus distinct costs 455 ms versus 347 ms for Trino's join plus 267 ms for Trino's
  `HashAggregationOperator`, or 0.74x for the same physical shape.
- A ten-warmup, three-measurement paired q87 control reports 0.637x Nitro/Trino CPU, 0.761x wall, 0.367x
  allocation, and 0.378x query peak memory. Nitro operator CPU is 1,075 ms join, 342 ms aggregation, 333 ms
  scan/pipeline, and 187 ms partitioned output. These categories account for essentially all 2,013 ms of query
  CPU.
- The borrowed-block remote-output prototype removes most `Page` egress and reduces q87 allocation by about
  4.7%, but increases median wall time from 660 ms to 722 ms and partitioned-output CPU from about 187 ms to
  261 ms. The required asynchronous native ownership transfer plus colder downstream access outweighs the
  avoided copy, so the prototype was rejected and not committed.
- The two encoding-attribution commits are not responsible for q87's isolated CPU level. An exact pre-diagnostic
  revision control measured 2,059 ms, versus 2,018 ms on the diagnostic revision, with identical row counts.
- ClickBench q24 does use the native Parquet pipeline for all 165 source drivers. A Nitro-only driver control
  processes 99,997,497 rows in 6.01 s wall and 26.30 CPU-s (4.38 average cores). Its two scan pipeline groups
  contain 82 and 83 drivers and finish in 5.94 s and 5.40 s maximum elapsed time. The corresponding captured
  Trino groups finish in 5.69 s and 5.78 s. This does not reproduce the earlier 7.55 s Nitro wall outlier.
- The full warmed TPC-H sweep exposed 5.24 million native RLE channel-positions, all in q21 and q22, becoming
  flat blocks at Page egress. The primitive adapter now preserves uniform single-run vectors (including sparse
  masks and consistently-null runs) as `RunLengthEncodedBlock`, and Nitro remote output admits RLE pages through
  its instance-owned encoding policy. Mixed-null runs still materialize. A q21/q22 gate preserved all 5.24
  million positions through Page egress; q21 CPU improved from 6.67 to 6.36 seconds while q22 remained stable at
  0.70 seconds. The focused integration suite passes 271 tests plus 21 partition-output tests.
- The completed structural board passes all 168 queries. Nitro/Trino geomeans are 0.726 wall / 0.683 CPU for
  TPC-H, 0.568 wall / 0.391 CPU for TPC-DS, and 0.859 wall / 0.685 CPU for ClickBench. The combined geomeans are
  0.652 wall and 0.485 CPU. The TPC-H sweep preceded the RLE-egress correction; its q21/q22 follow-up is the
  post-correction performance gate.
- Encoding accounting closes almost exactly. Across TPC-DS, native egress contains 2.257 billion dictionary,
  5.983 billion flat, and 22.062 million RLE channel-positions; Page egress contains 2.257 billion dictionary,
  5.983 billion flat, and 22.067 million RLE positions. Across ClickBench, 15.382 million native dictionary and
  5.223 billion native flat positions become 15.381 million dictionary and 5.223 billion flat Page positions.
  Only 945 ClickBench positions (q22/q23) move from dictionary to flat. There is no bulk hidden encoding loss.
- Incoming Page boundaries contain 35.5 million RLE channel-positions in TPC-DS, led by q75 (10.27 million),
  q97 (9.21 million), q04 (5.05 million), q11 (3.59 million), q05 (3.32 million), and q72 (3.17 million).
  A symmetric adapter prototype retained these as Nitro `RleVector`s instead of flattening them. A same-host
  5W5M A/B rejected blanket preservation: q97 was neutral (2,215 versus 2,196 ms CPU), while q75 regressed from
  2,929 to 3,178 ms CPU. The one-time boundary materialization is beneficial when downstream grouping/join code
  repeatedly accesses the values. The prototype was removed; any future RLE-ingress admission must be selected
  by the consuming operator rather than applied globally.
- The current ClickBench sweep has only three CPU rows at or above parity: q05 at 1.009, q19 at 1.042, and q40
  at 1.000. Existing isolated controls put all three at parity (q05 0.973, q19 0.995, q40 1.007). Its short-query
  wall regressions therefore remain fixed overhead and utilization effects, not evidence of hidden row work.
- TPC-DS q22's apparent full-sweep regression is also not a stable join defect. Its 15-warmup, 20-measurement
  isolated run is 10.44 versus 16.17 CPU-seconds (0.645x) and 4.68 versus 6.37 seconds wall (0.736x). Nitro's
  item join itself is about 2.93 CPU-seconds versus Trino's 2.61, but Nitro aggregation is about 6.24 seconds
  versus Trino's 10.1 and exchange/output is substantially cheaper. The end-to-end physical shape is faster;
  changing the join based on the contaminated suite row would be unjustified.
- ClickBench q33's higher Nitro peak memory is real but is not a Page/encoding artifact. The query groups all
  100 million rows by `(WatchID, ClientIP)` and produces almost 100 million groups. Both engines expose flat
  input/output at this shape. Nitro peaks at 5.92 GiB versus Trino's 4.72 GiB, while remaining faster at 0.834x
  CPU and 0.881x wall. The excess is therefore owned by the packed-pair grouping/state layout, not adaptation.
  Similar memory-heavy distinct/grouping shapes (notably ClickBench q14) are the next memory-efficiency target,
  but they should be treated as an explicit grouping-table tradeoff rather than another boundary rewrite.

## Interpretation

The current gap from the old microbenchmarks is not an unaccounted mask, dictionary, or RLE tax. The old
fixtures did not represent several SQL physical shapes. The SQL-shaped operator board and end-to-end query CPU
now reconcile: fused work must be compared with all of the Trino operators it replaces, and unreplaced scan,
exchange, and output work imposes the expected Amdahl limit.

The remaining production opportunities are query-shape specific: the q22 item join, native scan/code generation
for wide filtered projections, and eventually a genuine native remote exchange format. Page ingress already
borrows compact long, integer, dictionary, and variable-width storage; flat egress must copy because Trino owns
the resulting blocks. Passing borrowed native arrays into the existing Trino partition/serialization path is not
a viable substitute for native exchange. Remote exchange is now the main remaining bulk adaptation boundary;
consumer-aware encoding admission may still help individual shapes, but blanket RLE preservation does not.

## Artifacts

- `q87-paired-10w3m.log`: warmed paired engine and operator attribution.
- `q87-plan-node-5w1m.log`: plan-node attribution proving fused join/distinct accounting.
- `q87-5w3m-pre-diagnostics.log`: exact pre-diagnostic revision control.
- `q87-5w3m-control.log`: committed diagnostic revision control.
- `../20260806-borrowed-remote-output/q87-5w3m.log`: rejected borrowed-output candidate.
- `clickbench-q24-nitro-driver-3w1m.log`: Nitro-only compact pipeline distribution (benchmark query passed; an
  unrelated benchmark-harness test assumes a baseline runner and fails in Nitro-only mode).
- `../20260806-structural-full-board/tpch.log`: full pre-RLE TPC-H structural sweep.
- `../20260806-structural-full-board/tpch-q21-q22-rle.log`: focused RLE preservation performance gate.
- `../20260806-structural-full-board/tpcds.log`: complete warmed TPC-DS structural and encoding sweep.
- `../20260806-structural-full-board/clickbench.log`: complete warmed ClickBench structural and encoding sweep.
- `../20260806-rle-ingress/q75-parent-5w5m.log` and `q75-candidate-5w5m.log`: rejected RLE-ingress A/B.
- `../20260806-rle-ingress/q97-parent-5w5m.log` and `q97-candidate-5w5m.log`: neutral RLE-ingress A/B.
