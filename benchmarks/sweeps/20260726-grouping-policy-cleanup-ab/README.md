# Grouping-policy cleanup A/B

This focused gate compares the completed grouping-policy cleanup at `b932e5e0`
with its adjacent pre-cleanup source at `7ad3e244`. It covers composite/flat
grouping and generated/adaptive multi-long execution with TPC-DS q79/q85/q97
and ClickBench q32/q36.

Both sides used JDK 26 from `/opt/java/openjdk`, one unpinned JMH thread,
`-Xmx12g`, transparent huge pages, five one-second warmups and measurements,
three fresh forks, and JMH's GC allocation profiler. No JFR artifacts were
created. Minimum sampled available memory was 35.3 GiB for the control and
36.3 GiB for the candidate.

| Query | Control ms | Candidate ms | Candidate/control | Control B/op | Candidate B/op |
|---|---:|---:|---:|---:|---:|
| ClickBench q32 | 2685.70 | 2634.36 | 0.981x | 1,192,293,236 | 1,193,081,391 |
| ClickBench q36 | 2103.97 | 2050.99 | 0.975x | 305,859,836 | 307,070,321 |
| TPC-DS q79 | 478.99 | 482.24 | 1.007x | 72,573,160 | 72,833,508 |
| TPC-DS q85 | 342.20 | 335.16 | 0.979x | 106,844,772 | 112,944,494 |
| TPC-DS q97 | 1530.88 | 1406.74 | 0.919x | 872,119,703 | 1,094,385,913 |

The five-query duration geometric mean is `0.972x`. The three stable
allocation rows (q32/q36/q79) have a `1.003x` geometric mean. Q85 allocation
varies substantially within both sources.

Q97's headline allocation delta is not source-causal. A two-fork capture at
the first cleanup commit (`5b1ab375`) alternated between roughly 869/874 MB and
1.108/1.112 GB within each fork. Ten-warmup reverse captures then put current
source mostly in the 862/874 MB regime while the unchanged `7ad3e244` control
moved to roughly 0.98/1.10 GB. This is a compilation/runtime-placement regime,
not a changed policy default or representation admission. The complete raw
fork/iteration distributions are retained in the JSON artifacts.

The container was then recreated with `CAP_PERFMON`, restoring
`perf_event_open`. The same five-query, three-fork protocol captured
instructions, cycles, L1D misses/loads, dTLB misses/loads, branch misses, and
branches together with duration and allocation. No event was missing.

The first control/candidate placement was 1.005x in duration, 0.979x in
allocation, 1.012x in instructions/cycles, 1.004x/1.013x in L1D misses/loads,
1.085x/1.074x in dTLB misses/loads, and 1.005x/1.013x in branch
misses/branches. Because translation movement was concentrated in q32/q97, an
immediate reverse control followed the candidate. Against that reverse
control, the candidate was 0.957x in duration, 1.003x in allocation, 0.984x in
instructions, 0.966x in cycles, 0.969x/0.984x in L1D misses/loads,
0.921x/0.914x in dTLB misses/loads, and 0.987x/0.986x in branch
misses/branches.

The unchanged control itself moved 1.051x in duration, 1.029x in instructions,
1.048x in cycles, and 1.177x/1.174x in dTLB misses/loads between placements.
The large translation delta therefore does not follow source. Q32 and q36 do
retain small, consistent instruction-count increases of roughly 1.3--1.5% and
2.6--3.3%; their duration movements remain about 1--2% and should be watched
in the next source-consistent broad capture. The focused gate finds no causal
whole-cohort duration, allocation, or hardware-counter regression.

The original denied pre-execution log remains as `control.log` for provenance;
it contains no measurements. The valid counter bundles are
`control-counters.json`, `candidate-counters.json`, and
`control-counters-reverse.json`.
