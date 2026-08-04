# Compact dictionary remap board refresh (2026-08-04)

This directory records a paired Nitro/Trino refresh after Trino commit `c15e4e77`
(`Remap compact Nitro dictionaries in one sorted pass`).  All suites use native
Parquet inputs, JDK 26, task concurrency one, a 12 GiB test JVM, one warmup and
one measured iteration, allocation measurement, and no JFR or heap-dump output.

## Raw full-sweep board

Ratios are Nitro / Trino; lower is better.

| Suite | Queries | Wall geomean | CPU geomean | Total wall | Total CPU | CPU wins | Allocation total |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.749 | 0.715 | 0.726 | 0.747 | 20 | 0.276 |
| TPC-DS | 103 | 0.622 | 0.420 | 0.730 | 0.594 | 95 | 0.300 |
| ClickBench | 43 | 0.872 | 0.734 | 0.792 | 0.827 | 36 | 0.349 |
| Overall | 168 | 0.695 | 0.520 | 0.758 | 0.716 | 151 | 0.316 |

The raw board is deliberately retained without replacing long-process outliers.
It is a throughput/stability observation, not a reliable per-query regression
classification: later queries can be distorted by compilation, GC, and retained
runner state.

## Fresh-JVM controls for apparent outliers

Each control uses two warmups and three measurements.

| Query | Wall p50 | CPU mean | Allocation | Conclusion |
|---|---:|---:|---:|---|
| TPC-DS q15 | 0.401 | 0.139 | 0.071 | Raw CPU loss is sequencing noise |
| TPC-DS q20 | 0.561 | 0.174 | 0.082 | Raw CPU loss is sequencing noise |
| TPC-DS q38 | 0.751 | 0.653 | 0.361 | Raw CPU loss is sequencing noise |
| TPC-DS q85 | 1.472 | 0.629 | 0.411 | CPU win; real wall/coordination issue |
| ClickBench q12 | 1.011 | 0.998 | 0.301 | CPU parity; raw CPU loss is noise |
| ClickBench q40 | 1.240 | 0.983 | 0.597 | Small CPU win; wall issue remains |
| ClickBench q42, baseline | 1.685 | 1.152 | 0.734 | Genuine CPU and wall regression |
| ClickBench q42, columnar ordering candidate | 1.045--1.393 | 0.760 | 0.413--0.452 | CPU regression removed; wall remains noisy |

Earlier current-code fresh controls already cover TPC-DS q22, q23a, and q67,
and ClickBench q19/q30.  They are not reclassified from this single-iteration
full sweep.

## Current interpretation

The engine remains substantially ahead on aggregate CPU and allocation in all
three suites.  TPC-H is the most stable suite.  TPC-DS has the strongest CPU
geomean but some wall/CPU disconnects.  The q42 candidate changes host-driven
TopN from per-position ordering copies to columnar ordering storage when every
ordering vector has a physically fixed-width representation and the configured
minimum candidate capacity is met. Its three TopN
stages fall from about 22--24 ms of input CPU apiece to 3.5--4.4 ms. Variable-
width and structural ordering retain the established row-backed path. ClickBench
q40 and TPC-DS q85 remain scheduling or coordination targets because CPU is at
or below parity while wall time is higher.

The rejected native `SINGLE` local-exchange experiment exposed an architectural
gap: exchange consumers can close vector leases on a different thread from the
producer, while Nitro's allocator recycling is producer-context-local.  Native
exchange needs an explicit asynchronous transfer/release contract rather than
sharing producer-owned leases across the boundary.
