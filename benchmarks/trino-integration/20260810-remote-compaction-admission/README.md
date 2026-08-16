# Remote destination-compaction admission

This directory contains the complete post-change SQL gate for admitting Nitro native remote destination compaction
only when the configured share of output channels has a reusable dictionary or RLE values vector. The immutable
`TrinoNitroRemoteExchangePolicy` default is enabled with a 100% threshold, so mixed encoded/flat output uses the
single borrowed-Page assignment path instead of copying every lane through native partition vectors.

All runs used JDK 26, five warmups, three measured executions, both engines, exact result comparison, 12 GiB JVM heap,
8 GiB query memory, thread allocation measurement, peak-memory reporting, and operator/plan-node CPU metrics.

| Suite | Queries | Wall geo | CPU geo | Weighted wall | Weighted CPU | Allocation geo | Peak-memory geo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-DS | 103 | 0.5917 | 0.4115 | 0.7123 | 0.6016 | 0.2051 | 0.7263 |
| TPC-H | 22 | 0.7368 | 0.7010 | 0.7248 | 0.7449 | 0.1757 | 0.2619 |
| ClickBench | 43 | 0.7806 | 0.6168 | 0.6739 | 0.6635 | 0.2664 | 0.5759 |

The accepted parent board was 0.5953/0.4115 TPC-DS, 0.7236/0.6942 TPC-H, and 0.7911/0.6172 ClickBench wall/CPU
geomeans. The candidate is neutral to mildly positive overall, with a roughly 1.8% TPC-H wall regression balanced by
small TPC-DS and ClickBench improvements. CPU is essentially flat geometrically; weighted CPU improves for TPC-DS
and ClickBench. Every suite passed without OOM or correctness failure.

The focused exchange-batch suite passes 11 tests and the full `TestTrinoNitro*` core integration selection passes
247 tests, both with zero failures.

TPC-DS q30 in the full warmed board measured Trino 109.1 ms wall / 247 CPU-ms / 863 MiB allocation and Nitro 140.3
ms / 218 CPU-ms / 404 MiB, or 1.286x wall and 0.883x CPU. The accepted board's Nitro result was 143.4 ms / 220 CPU-ms
/ 435 MiB. This change removes some mixed-encoding copy work, but q30's remaining wall/CPU disconnect is not remote
compaction CPU.

The preceding q30 JFR, perf-stat, isolated controls, and rejected 25%-threshold/disabled controls are in
`../20260810-q30-deep-profile/`. Rejected dynamic-filter wait-budget and split-admission experiments are in
`../20260810-dynamic-filter-wait-budget/`; the warmed full board showed q02 was already fast, so none of those changes
was retained.
