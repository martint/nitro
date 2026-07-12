# Nitro / Trino / Velox board — JDK 26 — refreshed 2026-07-14

Ratios are Nitro time divided by the comparison engine; lower than 1.0 favors Nitro. Only queries that
completed in both engines enter pairwise aggregates.

Every overall comparison reports two complementary aggregates over that same common-query set: the geometric
mean of the per-query ratios (the typical multiplicative speedup) and the sum of measured query durations (the
total suite-time comparison). A comparison-engine/Nitro speedup above 1.0 favors Nitro. Sums are never compared
across different coverage sets.

## Methodology

Nitro and Trino used JDK 26, one unpinned JMH worker, a fresh fork per query, and
`-wi 3 -i 5 -w 1s -r 1s -f1 -t1`. Both JVM heaps were capped with `-Xmx12g`; the Maven daemon was
separately capped at 2 GiB. Velox used `taskset -c 0`, one driver, one I/O thread, one split per file, and
`--cache_gb=12`; TPC-H additionally used `--filters_as_node`. The Velox cache setting bounds its managed
pool, not total RSS, so RSS and host memory were monitored. Engine runs never overlapped.

Published engine comparisons use only the Nitro and Trino `BenchmarkQueries` operator harnesses. Query-compiler
benchmarks are development diagnostics and never enter suite rows, geometric means, duration sums, or hardware-
counter comparisons.

Velox TPC-H used one warmup plus three measured repetitions. TPC-DS used the minimum of three independent
processes, each with two in-process warmups before its reported execution. ClickBench used the minimum of
three independent processes. Nitro/Trino and Velox use the same logical operator sequence, build/probe
orientation, stage boundaries, and accumulator-style terminal sinks.

The original broad sweep used the JDK default huge-page policy. A 2026-07-14 same-build confirmation established
`-XX:+UseTransparentHugePages` as the JVM locality policy for the next full refresh: it applies equally to unpinned
Nitro and Trino, while the host remains in `madvise` mode. The conclusion is based on warmed, adjacent runs rather
than the old pre-optimization Q24 sample. It improves every rechecked Nitro sentinel and does not disadvantage the
Trino Q9 guard. The confirmation rows below therefore supersede older isolated rows; the suite aggregates above are
updated suite-by-suite. TPC-H is now fully refreshed; TPC-DS and ClickBench retain the original-policy aggregates
until their THP sweeps complete.

## Suite summary

| suite / pair | common | geo mean N/C | geo speedup C/N | Nitro sum ms | comparison sum ms | sum speedup C/N | Nitro wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H N/T | 22 | 0.732 | 1.366x | 44,065.5 | 58,241.4 | 1.322x | 20 |
| TPC-H N/V | 22 | 0.881 | 1.135x | 44,065.5 | 51,648.0 | 1.172x | 16 |
| TPC-DS N/T | 99 | 0.244 | 4.098x | 106,496.0 | 343,722.2 | 3.228x | 98 |
| TPC-DS N/V | 98 | 0.858 | 1.166x | pending | pending | pending | 64 |
| ClickBench N/T | 37 | 0.498 | 2.008x | 115,897.4 | 177,797.7 | 1.534x | 35 |
| ClickBench N/V | 36 | 0.709 | 1.410x | pending | pending | pending | 29 |

Across the 158 current common Nitro/Trino rows, the coverage-weighted geometric mean is approximately **0.336
Nitro/Trino**, or a **2.976x Trino/Nitro geometric-mean speedup**. Nitro's summed duration is **266,458.9 ms**
versus **579,761.3 ms** for Trino, a **2.176x sum-duration speedup** (54.0% less total Nitro time). This rollup
combines TPC-H's THP refresh with the still-default-policy TPC-DS and ClickBench sweeps and is therefore labeled a
current-board baseline, not a homogeneous final refresh. Across the 156 common Nitro/Velox rows, the corresponding
geometric-mean result is approximately **0.824 Nitro/Velox**, or **1.213x Velox/Nitro**. A cross-suite N/V duration
sum is intentionally not reported because the original TPC-DS and ClickBench Velox per-query duration totals were
not retained; the next refresh must retain them before publishing that aggregate.

TPC-DS includes the isolated standard-protocol Q50 repair rerun (Nitro 1375.986 ms, Trino 1895.896 ms,
Velox 1210 ms) and the corrected ten-column Q39 rerun (Nitro 660.6 ms, Trino 18270.0 ms, Velox 598 ms).
The prior Q39 sweep value was stale and its old Velox binary emitted only eight columns; the rebuilt harness now
has the required shape. Aggregates replace that row while retaining unchanged fresh-fork samples.

TPC-H and ClickBench aggregates include the isolated 2026-07-14 coverage reruns. They are recomputed from the
original common-query aggregate plus the newly common rows, so unchanged queries retain the same fresh-fork sweep
samples. Q21 uses the same supplier-range plan in all engines. ClickBench Q19/Q33-Q35 use three fresh independent
Velox processes and one standard fresh-fork Nitro JMH sample per query.

## Coverage and failures

| suite / engine | completed | missing | reason |
|---|---:|---|---|
| TPC-H Nitro | 22/22 | — | q21 restored with exact bounded supplier-range aggregation |
| TPC-H Trino | 22/22 | — | — |
| TPC-H Velox | 22/22 | — | — |
| TPC-DS Nitro | 99/99 | — | q50 pooled-cache failure repaired and rerun |
| TPC-DS Trino | 99/99 | — | — |
| TPC-DS Velox | 98/99 | q14 | Unsupported by retained harness |
| ClickBench Nitro | 44/44 | — | q19 passes; q33-q35 restored by lazy direct-row grouped output for TopN |
| ClickBench Trino | 37/44 | q0, q23, q24, q29, q34, q35, q40 | Harness gaps and 12 GiB OOMs |
| ClickBench Velox | 36/44 | q0, q37-q43 | Retained harness failures/crashes; sparse core artifacts removed |

Newly restored rows:

| suite/query | Nitro ms | Trino ms | Velox ms | N/T | N/V |
|---|---:|---:|---:|---:|---:|
| TPC-H q21 | 7876.2 | 8216.7 | 10300 | 0.959 | 0.765 |
| ClickBench q19 | 13207.0 | 13406.1 | 20660 | 0.985 | 0.639 |
| ClickBench q33 | 10725.1 | 17064.4 | 25700 | 0.628 | 0.417 |
| ClickBench q34 | 15074.3 | OOM | 18180 | — | 0.829 |
| ClickBench q35 | 14665.7 | OOM | 16430 | — | 0.893 |

## Current laggards

TPC-H Nitro/Velox fresh counters: q11 is now 0.84x after its shape, primitive-sort, and columnar-buffer repairs;
q19 is now about 0.96x after moving its multi-column predicate into the join and compiling categorical IN-list mask
construction into one projection loop; q16 is about 1.13x after the bounded columnar buffer and matching Velox's
four-column join output layout, and q9 about 1.12x after correcting two reversed join orientations, adding shared
sparse-range membership, and integrating the already-narrow join output layouts. Q13's
fresh 4.07 s versus Velox 4.42 s removes it from the laggard list. Nitro's strongest common
wins include q17 0.56x, q4 0.65x, q6 0.81x, and q20 0.85x.

Q11's final full sort now appends narrow/medium results into pooled dense columns once, sorts primitive row IDs, and
returns dictionary views over the sorted IDs. Against the preceding primitive row-slot control it moves 256.7 to
170.8 ms and 104.7 to 35.7 MB/op. Instructions are 3.602B versus 5.208B, cycles 0.965B versus 1.540B, L1D misses
24.59M versus 37.87M, dTLB misses 0.155M versus 1.186M, and branch misses 2.77M versus 6.00M. Fresh Velox is
203 ms / 3.726B / 1.091B / 25.94M / 0.144M / 1.98M: Nitro now wins wall, instructions, cycles, and L1D, is within
8% on dTLB misses, and still has 40% more branch misses.

The shared UTF-8 word equality first moved Q19 from 1943 to 1811 ms. Matching Velox's join-residual shape then moved
the raw timing to 1795 ms. Compiling literal UTF-8 `IN` predicates with their dependent boolean/CASE mask expression
removes that projection overhead: the isolated on/off counter A/B is 1484/1816 ms, 25.477/34.889B instructions,
8.940/9.696B cycles, 330.98/352.78M L1D misses, 14.60/14.55M dTLB misses, 29.18/34.71M dTLB loads, and
28.91/27.48M branch misses. Fresh Velox is 1540 ms, 8.097B cycles, 207.92M L1D misses, 0.471M dTLB misses,
17.19M dTLB loads, and 34.29M branch misses: Nitro now wins wall and branches, while cache/TLB misses remain worse.

Q16's first join now exposes the same `(suppkey, brand, type, size)` layout as Velox through a general integrated
join-output selector; keys remain private execution inputs and discarded payloads stay lazy. Against the six-column
control, current counters improve 437.4 to 430.7ms, 9.702 to 9.461B instructions, 2.529 to 2.477B cycles, and
90.04 to 88.75M L1D misses. dTLB traffic is neutral. Velox remains 382ms / 7.268B / 2.022B / 61.68M; the residual
gap is the required two-level DISTINCT/count grouping pipeline.

The subsequent allocation audit found that Q16's flat-group fixed-record chunks fell below the primitive pool's
retention floor. Pool-aware chunk sizing halves allocation from 120.40 to 60.96 MB/op and reduces measured GC time
from 16 to 5 ms over five iterations. Wall remains neutral in the perfnorm pair (426.2/423.9 ms enabled/control),
but cycles improve 2.452/2.480B, L1D misses 88.07/90.34M, dTLB misses 2.179/2.373M, dTLB loads 11.94/12.25M, and
branch misses 8.03/8.40M; instructions are effectively flat at 9.554/9.525B. This does not change the board ratio,
but removes a general steady-state allocation defect in the remaining Q16 grouping pipeline.

Recycling the grouped binary dictionary's ID mapping through `DictionaryVector`'s owned-child contract removes a
further 9.48 MB/op (51.55 MB/op remains) and improves all captured counters: 9.464/9.544B instructions,
2.443/2.495B cycles, 86.14/88.46M L1D misses, 2.096/2.276M dTLB misses, 11.72/11.96M dTLB loads, and
7.90/8.16M branch misses enabled/control. Wall remains neutral at 429.0/427.5 ms.

Removing a double defensive copy in allocator-owned sparse masks lowers Q16 allocation again, from 51.46 to
48.83 MB/op. Its counter A/B is mixed and wall-neutral (425.8/423.6 ms), so it is recorded as an allocation fix,
not a board speedup.

The follow-on Q9 allocation baseline is 3404.6 ms and 1.080 GB/op. A 1 GiB global primitive-pool experiment removes
almost exactly the additional 512 MiB of retained capacity but leaves wall flat, demonstrating cross-query cache
retention rather than better within-query buffer turnover. It is rejected; the process-wide cap stays at 512 MiB.

Identity-aware release of copied build batches after coalescing is the first actual within-query Q9 reuse win:
steady allocation falls by about 72 MB/op to 1.008 GB/op. Wall is neutral (3481/3485 ms with perfnorm), dTLB misses
improve 72.22/75.26M, while instructions, cycles, L1D, dTLB loads, and branches regress by roughly 1--3%. It is
retained for the bounded lifetime/allocation correction rather than counted as a board speedup.

A follow-up pool-integrity repair makes raw-array releases identity-idempotent, keeps allocator-owned vectors on their
existing allocator lease lifecycle, and gives flat fixed-record and variable-width chunks separate semantic families.
Binary grouped output checks its field null bit before reading a recycled length/pointer payload. This fixes a repeat-invocation TPC-DS Q24
stale-pointer failure and prevents duplicate entries from displacing useful buffers under the unchanged 512 MiB cap.
Q24 is stable across repeated measurements and dataset-backed SQL equivalence; focused poisoned-reuse tests pass.
Applying the identity lifecycle to allocator-owned vectors was rejected. A raw-array-only capture measured Q9 at
228.98 MB/op, but subsequent clean restoration runs on identical retained source measured 1.024 and 1.018 GB/op
(three and eight warmups). The 512 MiB cap is unchanged; the 228.98 MB figure is quarantined as non-reproducible and
must not be used as a board baseline.

Diagnostic controls narrow the discrepancy: the default pool stabilizes at 535.58 MB retained and 503--529 MB
reused per Q9 invocation; a 1 GiB ceiling measures 413.91 MB/op, and a zero minimum-retention threshold measures
1.031 GB/op. Neither reproduces 228.98 MB/op. These configurations are diagnostic only and the defaults are retained.

The final retained-build Q9 perfnorm capture is 3132.8 ms, 37.790B instructions, 16.492B cycles (2.291 IPC),
608.18M L1D misses, 0.697M dTLB misses, 17.023M dTLB loads, and 287.98M branch misses. Treat these as an absolute
post-change checkpoint; the immediately paired capacity-8/2 data below, rather than older captures with different
counter behavior, is the apples-to-apples comparison.

A capped broad-suite audit separately found 18 failures and one error in the compiled-plan bridge tests. Q02
reproduces with the primitive pool disabled, so those mismatches are not attributed to this reuse change. Focused
operator/SQL validation is green, but the broad suite is not currently green and the compiled bridge requires its own
correctness pass before the board can claim full-suite validation again.

An eight-entry hash-join duplicate-list seed was tested and rejected after the pool repair. It saves only 0.36 MB/op
on Q9 and has mixed counters (slightly fewer instructions/branches but more L1D/dTLB traffic); the general default
remains two. This avoids attributing a corrupted pool-budget signal to a join data-structure heuristic.

A subsequent general flat duplicate-chain prototype was removed after it failed to demonstrate a paired win.
Although it passed duplicate/Q9 correctness, its 988.07 MB/op result could not be attributed to the prototype: the
restored implementation measured 1.024 GB/op, and eight warmups measured 1.018 GB/op. This is not a board result;
it exposes a retention/configuration reproducibility problem that must be solved before evaluating the layout.

Q9's previously separate input-only boundary projections now use the integrated join output selector, including
dropping the final nation key at the same boundary as Velox. The effect is deliberately characterized as small:
3459.0/3468.3ms, 57.316/57.573B instructions, 19.495/19.533B cycles, 453.53/456.10M L1D misses, and
75.31/76.41M dTLB misses integrated/wrapper; branch misses move 35.83M to 36.15M. The remaining N/V gap is still
translation locality, not projection control-plane work.

TPC-DS Nitro/Velox: q24 is now about 1.06x after aligning the residual-join predicate shape, removing hot-path
accessor/uppercase allocations, binding the outer residual value once per probe row, and matching every retained
join-output boundary; it remains a small verified
laggard, but no longer dominates the suite at 1.49x.
Q86 is no longer a laggard after stable value IDs and mixed-composite grouping; the same retained path moves Q70
below Velox, row-major stable IDs bring Q67 to parity/slightly ahead, and nested-encoding-aware expression fusion
moves Q99 decisively below Velox. Wide-key DISTINCT batching moves Q75 slightly below Velox. Previously targeted
queries now read:

| query | Nitro ms | Trino ms | Velox ms | N/V |
|---|---:|---:|---:|---:|
| q23 | 5788.5 | 12944.2 | 6860 | 0.844 |
| q24 | 1118.3 | OOM | 1050 | 1.065 |
| q11 | 2898.3 | 11396.0 | 3300 | 0.878 |
| q9 | 2933.2 | 5311.2 | 3920 | 0.748 |
| q45 | 67.2 | 988.0 | 109 | 0.617 |
| q67 | 8384.4 | — | 8730 | 0.960 |
| q70 | 850.4 | — | 1030 | 0.826 |
| q86 | 90.7 | 476.0 | 173 | 0.524 |
| q99 | 318.5 | 1323.4 | 596 | 0.534 |
| q75 | 2084.6 | — | 2180 | 0.956 |
| q2 | 906.7 | — | 965 | 0.940 |
| q36 | 347.6 | — | 447 | 0.778 |
| q51 | 1628.9 | — | 3730 | 0.437 |
| q39 | 660.6 | 18270.0 | 598 | 1.105 |
| q50 | 1081.5 | 1895.9 | 1150 | 0.940 |
| q64 | 2425.7 | 13857.2 | 2380 | 1.019 |
| q82 | 2418.0 | 3557.1 | 3980 | 0.608 |
| q94 | 1359.2 | 3994.2 | 2470 | 0.550 |
| q95 | 1341.2 | 4339.5 | 2410 | 0.557 |
| q83 | 34.0 | 639.5 | 56 | 0.607 |

Fresh same-shape Q64 counters use the gated decoder policy. Nitro/Velox are 2425.7/2380 ms, 39.887/43.872B
instructions, 13.922/12.681B cycles, 464.3/435.7M L1D misses, 47.52/1.079M dTLB misses, 130.84/34.18M dTLB
loads, and 54.53/62.73M branch misses. Nitro wins instructions and branch misses and is within 2% on wall; the
remaining issue is translation locality, not excess logical work.

Corrected same-shape Q39 is also a modest row rather than the stale sweep's apparent laggard. Nitro/Velox are
660.6/598 ms. Nitro counters are 13.305B instructions, 3.823B cycles, 70.08M L1D misses, 2.018M dTLB misses,
12.51M dTLB loads, and 13.84M branch misses. Velox's ten-repeat capture is 8.494B, 3.169B, 155.49M, 0.218M,
8.00M, and 4.49M respectively. Nitro wins L1D misses but retains instruction, TLB, and branch gaps.

The first ClickBench laggard list is now stale after matched counter-guided work. Fresh isolated Nitro/Velox ratios
are q29 0.94x, q18 0.95x, q12 1.02x, q6 1.04x, q10 1.04x, q11 about 1.04x, and q22 about 1.06x. Q22 is the
remaining apparent common-path laggard. Strong common wins still include q7 0.23x, q26 0.26x, q27 0.37x, q30
0.40x, q25 0.50x, and q1 0.51x.

## ClickBench laggard refresh

All rows below are isolated captures: Nitro is unpinned with a 12 GiB heap and Velox is pinned to one core with one
driver/I/O thread and a 12 GiB cache cap. Engines did not overlap. Counters are normalized per operation.

| query | Nitro ms | Velox ms | N/V | Nitro instructions | Velox instructions | Nitro cycles | Velox cycles | Nitro L1D misses | Velox L1D misses | Nitro dTLB misses | Velox dTLB misses | Nitro branch misses | Velox branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q6 | 3676 | 3540 | 1.038 | 70.028B | 49.660B | 20.635B | 18.902B | 375.2M | 327.1M | 38.24M | 2.72M | 117.9M | 145.4M |
| q10 | 2834 | 2720 | 1.042 | 42.408B | 33.886B | 15.355B | 14.359B | 497.2M | 492.1M | 58.15M | 15.29M | 75.9M | 60.5M |
| q11 | 562 | 539 | 1.043 | 10.664B | — | 3.281B | — | 191.3M | — | 3.23M | — | 14.7M | — |
| q12 | 718 | 704 | 1.019 | 13.375B | 11.647B | 3.986B | 3.731B | 237.3M | 150.6M | 3.80M | 0.84M | 18.2M | 22.7M |
| q18 | 7430 | 7820 | 0.950 | 122.159B | 103.897B | 42.169B | 42.243B | 789.3M | 1070.0M | 155.1M | 36.0M | 139.2M | 249.6M |
| q22 | 7711 | 7200 | 1.071 | 119.970B | 105.351B | 43.034B | 38.596B | 1378.3M | 998.9M | 5.94M | 5.68M | 422.6M | 371.8M |
| q29 | 47703 | 50897 | 0.937 | 1216.874B | 1168.483B | 302.329B | 265.309B | 5065.6M | 2283.2M | 101.36M | 11.33M | 1127.8M | 1149.1M |

The retained general changes are: direct dictionary-mask narrowing with an all-false-null proof (Q11/Q12), pooled
scalar-tagged long-pair DISTINCT plus a null-free batch kernel (Q10), an exact empty-binary side state for flat
single-key DISTINCT (Q6), and a width-gated branchless ULEB128 decoder layered over the bounds-safe unrolled path
(Q22), plus zero-copy range matching and
constant encoded-argument hoisting for regexp replacement (Q29). Q18's old 1.32x lag was stale:
fresh counters show equal cycles, fewer cache and branch misses than Velox, and a Nitro wall win; no Q18-specific
change was retained.

Q29 now wins wall and branches, while its residual work versus Velox is 4.1% instructions, 14.0% cycles, 2.2x L1D
misses, and 8.9x dTLB misses. A compiled-Q22 diagnostic using Nitro's alternate Trino reader is 7.22 seconds, near
Velox, but is not substituted into this board because the operator harness and reader shape must remain comparable.
Fusing the hybrid-RLE header and repeated-value loads was a negative control (7.682 versus 7.650 seconds) and was
removed.

The later wide-header SWAR decoder is a separate retained result. On Q22 it improves the adjacent control from
7.870 to 7.711 seconds, 120.751B to 119.970B instructions, 46.488B to 43.034B cycles, 1460.4M to 1378.3M L1D
misses, 7.50M to 5.94M dTLB misses, and 424.5M to 422.6M branch misses. Applying it indiscriminately regressed
TPC-DS Q64 from 2.509 to 2.998 seconds; narrow streams retain the predictable unrolled decoder, while bit widths
of 16 or more use SWAR. The gated Q64 result is 2.426 seconds.

Q24's scalar UTF-8 equality now shares the word/intrinsic strategy already used by hash operators. The matched
enabled/disabled capture is 1552.9/1567.7 ms, 36.814/37.134B instructions, 8.617/8.816B cycles, 327.2/328.5M L1D
misses, and 27.99/29.01M branch misses. dTLB misses moved the wrong way (8.09/7.78M), so this is retained as a
modest total-work win, not represented as the main translation solution.

## Q75 nullable wide-key DISTINCT batching

Q75 performs SQL `UNION` de-duplication over seven numeric columns. The original generic flat index processed the
wide key row by row. The retained implementation generates an arity-specialized multi-long DISTINCT probe, omits
the reverse group-key map and group-id payload that a set never materializes, and uses a null-free batch kernel
after the retain-nulls wrapper partitions each input mask once. True null-containing tuples remain in the generic
SQL-null set; the two ordered survivor streams are merged without changing operator output order. Focused duplicate,
RLE-null-stream, real-null, rehash, and SF10 reference tests pass.

| Q75 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned | 2084.6 | 36.539B | 11.931B | 3.062 | 444.18M | 14.531M | 88.89M | 74.91M |
| Velox, fresh pinned | 2180 | 37.563B | 11.415B | 3.291 | 375.16M | 1.359M | 46.72M | 57.68M |

Nitro is 4.4% faster and executes 2.7% fewer instructions. It remains 4.5% higher on cycles, 18.4% higher on L1D
misses, 1.90x on dTLB loads, 10.7x on dTLB misses, and 30% on branch misses. Versus the pre-change Nitro capture
(2921.1 ms, 47.975B instructions, 16.207B cycles, 694.60M L1D, 25.057M dTLB misses, 159.36M dTLB loads), the
retained framework is 28.6% faster and removes 23.8% instructions, 26.4% cycles, 36.1% L1D misses, 42.0% dTLB
misses, and 44.2% dTLB loads. `-XX:+UseTransparentHugePages` was a negative control and remains disabled.

## Q2 categorical projection and nullable fused aggregation

Q2 repeats the same weekly-sales branch for two years. Each branch projects seven day-name CASE buckets and then
groups by week. The interpreter previously evaluated seven UTF-8 equalities and seven conditional SUM inputs in
separate vector passes. The retained projection compiler classifies each UTF-8 value once against the branch's
unique literals, shares that categorical tag across all bucket outputs, and accepts arbitrary nested input encoding
at the generated boundary. The general single-long-key aggregation kernel now accepts a flat nullable input per
accumulator: a null skips only that accumulator while the shared group probe and the other accumulators continue.

| Q2 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned | 906.7 | 26.461B | 4.933B | 5.364 | 316.05M | 0.902M | 7.14M | 6.90M |
| Velox, fresh pinned | 965 | 16.381B | 5.079B | 3.225 | 161.24M | 0.449M | 15.98M | 13.00M |

Nitro is 6.0% faster, with 2.9% fewer cycles, 55% fewer dTLB loads, and 47% fewer branch misses. It still executes
1.62x the instructions, incurs 1.96x the L1D misses, and 2.01x the dTLB misses, so the remaining categorical
projection locality is visible even though Q2 is no longer a wall-time laggard. Focused nullable multi-accumulator
tests and the SF10 result comparison against Trino pass.

## Q36 and Q51 fresh window audit

Fresh isolated counters showed that Q36's sweep ranking was stale: Nitro is 347.6 ms versus fresh Velox 447 ms.
Nitro records 7.462B instructions, 2.034B cycles, 78.68M L1D misses, 0.439M dTLB misses, 7.34M dTLB loads, and
15.74M branch misses; Velox records 7.240B, 2.383B, 59.14M, 0.246M, 6.95M, and 17.79M respectively. Q36 is a
22% wall-time win with a modest locality deficit and is not an optimization target.

Q51 was real: the initial Nitro capture was 4888.4 ms versus Velox 3730 ms, with 157.9M dTLB misses. Its two large
per-channel windows received one retained page each, yet allocated one heap `RowReference` per row and comparison-
sorted those objects through indirect page accesses. `WindowOperator` now has a general retained-single-page path:
it stores row order in primitive arrays, uses stable LSD radix sorting for flat integer partition/order keys, skips
key bytes that are constant over the batch, and preserves the comparison fallback for other/multi-page shapes.

| Q51 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned | 1628.9 | 21.747B | 9.216B | 2.360 | 402.02M | 73.150M | 180.39M | 49.17M |
| Velox, fresh pinned | 3730 | 76.349B | 21.252B | 3.592 | 543.12M | 5.844M | 52.33M | 118.42M |

Nitro is 56.3% faster, with 72% fewer instructions, 57% fewer cycles, 26% fewer L1D misses, and 58% fewer branch
misses. Its translation gap remains explicit at 3.45x dTLB loads and 12.5x misses. The comparison-sort intermediate
control was 3885.8 ms; radix ordering and constant-byte elimination, rather than primitive row IDs alone, are the
decisive lever. The large order/sort scratch arrays now use the shared bounded primitive pool, while one reusable
batch-position frame serves every output stream. Signed, nullable, descending-order tests and SF10 Q51 comparison pass. The comparison also exposed
and fixed a test bug that normalized SQL dates to `Integer` but Nitro date keys to `Long`.

## TPC-H fresh laggard counters

Q11's scalar-total branch decoded `ps_partkey` even though its aggregate consumed only extended value. Velox's same
branch scans supplier key, cost, and quantity only. Matching that shape moves Nitro from 334.5 to 301.2 ms; SF10
reference results pass. Fresh Velox is 203 ms. Nitro/Velox counters are 5.833/3.726B instructions, 1.804/1.091B
cycles, 45.86/25.94M L1D misses, 2.083/0.144M dTLB misses, 9.33/3.27M dTLB loads, and 7.96/1.98M branches. A fused
cast/multiply projection and an 8M-row filter window were negative/neutral and were removed. Forced bulk payload
decode raised L1D misses 77% and remained disabled.

The other fresh Nitro/Velox results are Q9 3423/3080 ms, Q13 4070/4420 ms, Q16 428/382 ms, and Q19
1789-1795/1540 ms.
Q9's corrected physical shape now matches Velox: `partsupp` and `orders` stream over the filtered intermediate,
and dead join columns are projected at the same boundaries. That audit initially exposed Nitro's multi-batch build
payload path (7738 ms and 272M branch misses); bounded 4M-row build coalescing recovers 37% wall and 61% branches.
The corrected shape then gained a 16-slot tag table for predominantly-negative single-key probes, a pooled exact
sparse-range membership bitset, and safe sharing of that immutable bitset with the orders scan's generic dynamic
filter. Final Q9 counters are instructions 56.14/55.46B, cycles 18.61/16.43B, L1D misses 441.42/741.92M, dTLB
misses 70.87/18.10M, dTLB loads 138.83/115.26M, and branch misses 33.91/44.31M. Q16's stable-ID-only binary group records reduce the prior 501 ms, 10.65B instructions,
3.01B cycles, and 121.15M L1D misses to 486 ms, 10.33B, 2.87B, and 115.76M; Velox remains at 382 ms,
7.27B, 2.02B, and 61.68M. Q19 now matches both Velox's pre-join revenue projection and join-residual shape and
remains primarily locality-bound (352.1/207.9M L1D and 14.82/0.471M dTLB misses). Q13 now wins wall/instructions/cycles/L1D,
though its TLB translation deficit remains.

The forced-bulk control also exposed a lifecycle bug independent of that mode: a final lead-filter survivor buffer
is deliberately visible through both `col*` and `window*`, but scan close returned both aliases to the primitive
pool. Later scans could borrow the same array concurrently. Close now returns an aliased buffer exactly once; the
multi-invocation forced-bulk reproduction no longer loses output schema.

## Q99 encoded multi-output projection

Q99's four joins leave the shipping/sold-date inputs behind four nested dictionary mappings. The multi-output
projection compiler already shared the delay subtraction and five CASE buckets in one loop, but accepted only one
dictionary layer and silently fell back to the interpreter. The fallback copied a full row-ID mapping for every
expression node and repeatedly scanned the same IDs for base cardinality. Dictionary peeling now borrows immutable
IDs for the input batch lifetime, copies only on an explicit output take, caches mapping metadata for one evaluation
cycle, and lets a fused kernel flatten arbitrary encoded integer/null inputs once at its boundary.

| Q99 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned | 318.5 | 7.896B | 1.735B | 4.552 | 65.79M | 0.202M | 1.86M | 11.03M |
| Velox, fresh pinned | 596 | 9.857B | 3.127B | 3.153 | 82.06M | 0.049M | 6.73M | 19.43M |

Nitro is 46.6% faster, with 19.9% fewer instructions, 44.5% fewer cycles, 19.8% fewer L1D misses, 72.4% fewer
dTLB loads, and 43.2% fewer branch misses. Its dTLB miss count remains 4.1x Velox despite much lower total TLB
traffic. The matched default-off control was 642.0 ms and 16.611B instructions; the improvement is expression-DAG
fusion over the real nested encoding, not a query-specific operator.

## Q11 wide shared-dictionary grouping

Q11's six customer-attribute keys arrived with a shared dictionary mapping, but the shared-dictionary shortcut
retained six owned object keys per distinct customer. Routing composites wider than four fields to the packed flat
grouping table removes that high-cardinality object state while preserving the narrow shared-dictionary path used by
Q45. Dataset-backed Q11 and Q45 both match Trino.

| Q11 Nitro | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| previous shared-object route | 4671.5 | 104.133B | 28.945B | 3.598 | 526.1M | 35.754M | 106.04M | 94.37M |
| retained wide-flat route | 2898.3 | 62.727B | 15.770B | 3.978 | 313.1M | 19.017M | 63.37M | 56.21M |

The retained three-process Velox wall baseline is 3300 ms, making Nitro/Velox 0.878 after the change (previously
1.416). The native harness was subsequently rebuilt from the current source after restoring its exact dependency
ABI in a workspace-local prefix. A pinned ten-execution counter capture reported 3250 ms for its final execution;
the totals below are divided by ten to amortize startup and match the established Velox counter protocol.

| Q11 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned | 2898 | 62.727B | 15.770B | 3.978 | 313.1M | 19.017M | 63.37M | 56.21M |
| Velox, fresh pinned | 3250 | 59.103B | 17.275B | 3.421 | 430.9M | 1.130M | 27.62M | 60.18M |

Nitro is 10.8% faster in this capture and uses 8.7% fewer cycles, 27.3% fewer L1D misses, and 6.6% fewer branch
misses despite 6.1% more instructions. The remaining locality gap is explicit: 2.29x dTLB loads and 16.8x dTLB
misses. The wide-flat change recovered the wall/cycle/L1/branch advantage, but it did not recover Velox's page
translation behavior.

Q86's isolated pre-change control was 227.2 ms, 5.303B instructions, 1.275B cycles, 46.94M L1D misses,
0.181M dTLB misses, 14.12M dTLB loads, and 3.74M branch misses. The retained path interns dictionary values once,
stores their compatible grouping hash, packs the two binary IDs plus the small grouping-set ID into a compact direct
key, specializes that three-field layout outside the per-row generic dispatch, shares dense immutable dictionary IDs
through GroupId, and represents known-false side streams as one-run RLE. The fresh Velox counter capture reported
164 ms for its final execution (the suite board retains the independent-process 173 ms baseline):

| Q86 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned (three-run median) | 90.7 | 2.374B | 0.512B | 4.637 | 27.21M | 0.0980M | 4.39M | 1.52M |
| Velox, fresh pinned | 164 | 2.822B | 0.880B | 3.209 | 25.26M | 0.0737M | 3.37M | 3.92M |

Nitro is 45% faster than the fresh Velox execution and uses 16% fewer instructions, 42% fewer cycles, and 61% fewer
branch misses. The remaining gaps are now narrow and explicit: 1.08x L1D misses, 1.33x dTLB misses, and 1.30x dTLB
loads. A bounded lazy per-dictionary-entry hash cache, eager wide-rollup classification, retained-build dictionary
recompaction, generic owned-ID leases, and omission of schema-present false streams were all measured and rejected.
The structural win is stable metadata plus a reversible direct key, not a per-batch cache.

## Q70 hardware counters

Q70 uses the same three-field rollup layout over the much smaller store county/state dictionary. Separating mixed-key
shape eligibility from large-dictionary eager classification prevents a sparse 1025x1025 direct cache: tiny bases use
their already-known compact radices without an eager scan, while Q67's wide key remains excluded. The current Nitro
capture is 850 ms; the rebuilt Velox harness reports 1030 ms and the ten-execution counters below are divided by ten.

| Q70 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned | 850 | 21.082B | 4.662B | 4.522 | 135.1M | 0.532M | 3.13M | 26.86M |
| Velox, fresh pinned | 1030 | 17.605B | 5.397B | 3.262 | 112.8M | 0.115M | 10.05M | 28.81M |

Nitro is 17% faster and wins cycles, dTLB loads, and branches. It still uses 20% more instructions/L1D misses and
4.6x the dTLB misses. Perfasm distributes those misses across scan decode, GroupId materialization, dictionary/RLE
copies, and composite assignment; no single retained-state layout explains the residual.

## Q67 hardware counters

Q67's nine-key rollup remains on the wide flat grouping table. Stable dictionary IDs were previously stored as one
array per field, making equality touch up to eight distant pages. The retained row-major layout stores one packed
`record x field` ID frame, keeping a record's comparison metadata together. A per-batch remapped-ID frame was
rejected because it added a write/read pass and regressed wall/cycles/branches.

| Q67 engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro, retained unpinned (three-run median) | 8384 | 147.922B | 49.561B | 2.985 | 1.760B | 104.50M | 771.27M | 41.83M |
| Velox, fresh pinned | 8730 | 161.037B | 46.487B | 3.464 | 1.261B | 6.495M | 123.18M | 155.63M |

Nitro is 4% faster with 8% fewer instructions and 73% fewer branch misses, but uses 7% more cycles, 40% more L1D
misses, 6.3x the dTLB loads, and 16.1x the dTLB misses. Versus the matched column-wise control, packing is wall-flat
and reduces instructions 2.8%, L1D 4.8%, dTLB loads 15.6%, dTLB misses 37%, and branches 8%.

## Q82 hardware counters

The counter run retained the same inventory-build/item-probe shape in all three engines. Nitro and Trino
were unpinned; Velox was whole-process pinned. Counts are per query after warmup.

| engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro | 2335 | 40.670B | 12.572B | 3.235 | 514.3M | 1.283M | 33.23M | 104.28M |
| Trino | 3342 | 59.650B | 20.159B | 2.959 | 808.1M | 58.16M | 166.89M | 38.82M |
| Velox | 4020 | 89.415B | 20.992B | 4.260 | 687.6M | 0.625M | 41.53M | 96.04M |

Against Velox, Nitro uses 54.5% fewer instructions, 40.1% fewer cycles, and 25.2% fewer L1D misses. Its
remaining deficits are 2.05x dTLB misses and 1.09x branch misses; dTLB loads are already 20% lower.

## Q24 hardware counters

The audited row counts match exactly: both engines reduce the two 28.8M-row `store_sales` inputs to 46,806
and 2,168,102 rows before the `(ticket_number,item_sk)` pair lookup. Mapping Velox's node order shows that
the following `HashProbe` is the pair join; the scan reduction comes from existing downstream single-key runtime
filters. The remaining gap is therefore genuine pair-table/probe and downstream layout cost, not missing pruning.

| engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro | 1135 | 25.067B | 6.458B | 3.882 | 241.6M | 9.404M | 80.80M | 25.59M |
| Velox | 1050 | 19.513B | 5.604B | 3.48 | 168.0M | 0.754M | 16.78M | 24.60M |

Nitro/Velox is now 1.28x instructions, 1.15x cycles, 1.44x L1D misses, 12.47x dTLB misses, 4.82x dTLB
loads, and 1.04x branch misses. The latest dTLB count is noisy and worse than the preceding 6.80M capture even as
wall, instructions, and cycles improve; it is not evidence that the outer-value cache regressed translation. The
earlier harness had a physical-shape mismatch: Velox evaluated the
birth-country equality as a residual predicate inside the zip hash join, while Nitro emitted 62.8M candidates and
filtered above the join. Nitro now has a general residual-filter API, with encoded binary accessors and dictionary
depth bound once per batch. Hoisting the null accessor fixed a discovered two-lambda-per-candidate allocation bug;
Q24 allocation dropped from 2.93GB to 366MB/op. Writing `upper_utf8` directly into its pooled output removed another
101MB/op (265MB/op current). Binding the outer encoded residual once per probe row instead of resolving it for every
candidate is a clean A/B win (1122 versus 1233 ms). A retained normalized pair-key layout packs two signed-32-bit keys into one
long and automatically promotes on a wide key. It cuts entry storage by one third and improves the latest paired
dTLB count (8.458M versus 8.822M), with neutral whole-query wall. Composite Bloom experiments were redundant
with the already-matching single-key pruning and were removed: the eager window regressed to 2.216s, while a
correct lazy-reader A/B raised instructions 2.1%, L1D misses 6.3%, dTLB misses 5.6%, dTLB loads 15%, and branch
misses 5%. The lazy prototype also exposed why reusable buffers need role-typed pool families: sharing its word
array with generic `long[]` buckets corrupted the third execution; a dedicated family fixed correctness before the
performance rejection.

The pair index now also allocates its `LongArrayList[]` duplicate side table only on the first duplicate key.
Unique builds use the flat row reference already stored in each entry and no longer reserve one object reference per
hash slot. The Q24 A/B is counter-neutral within run noise (1553/1546 ms, equal cycles); the retained version removes
the unused live/pooled side-table footprint. GC allocation is unchanged because both variants recycle this state.

## Q95 corrected-shape hardware counters

Q94/Q95 now attach `ws_warehouse_sk <> r_warehouse_sk` to the web-sales self hash join, matching Velox instead of
materializing the equi-join and filtering above it. Dataset-backed Q94/Q95 SQL checks pass. The retained Q95 numbers
include an adaptive pooled paged bitmap for dense single-long DISTINCT state and a compact residual payload ordered
with each duplicate-key join range.

| engine | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro | 1341 | 40.757B | 7.382B | 5.52 | 121.6M | 1.328M | 10.47M | 29.26M |
| Velox | 2410 | 47.429B | 12.746B | 3.72 | 213.8M | 0.413M | 29.55M | 54.16M |

Nitro is 44% faster and wins instructions, cycles, L1D misses, dTLB loads, and branch misses. Its remaining exception
is 3.22x dTLB misses. The paged DISTINCT A/B reduced dTLB misses 13.8%; ordering the fixed-width residual alongside
the compact join ranges reduced them another 8.4% against its direct-access control. A separate candidate-batch
compaction experiment was neutral and was removed.

## TPC-H Q11 columnar full-sort buffer

The retained full-sort path no longer builds one-element vectors per row and then consolidates them. For results up
to eight columns it appends batches into pooled dense columns, sorts primitive row IDs with the existing stable
radix/adaptive algorithms, and exposes the final order as dictionary views. This removes the allocator and
`IdentityHashMap.resize` dTLB hotspots. Q11 improves from 256.7 to 170.8ms and allocation from 104.7 to 35.7MB/op.

| engine/control | wall ms | instructions | cycles | IPC | L1D misses | dTLB misses | dTLB loads | branch misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Nitro columnar | 171 | 3.602B | 0.965B | 3.73 | 24.59M | 0.155M | 2.96M | 2.77M |
| Nitro row-slot control | 258 | 5.208B | 1.540B | 3.38 | 37.87M | 1.186M | 8.03M | 6.00M |
| Velox | 203 | 3.726B | 1.091B | 3.42 | 25.94M | 0.144M | — | 1.98M |

The multi-key comparator uses the same dense columns and improves Q16 from 457.9 to 428.5ms. Eagerly densifying
Q64's 21 output columns regressed 2435 to 2570ms, so the default is width-gated at eight columns; Q64 remains on its
lazy row-slot payload path (2426ms confirmation). Full tests plus explicit stable-radix and multi-key/null-ordering
tests pass.

## Five-priority follow-up

The next-laggard pass rechecked Q16, Q9, Q39, Q22, and Q24/Q64 under the same methodology: Nitro and Trino
unpinned, Velox pinned to its established benchmark core, one engine at a time, and a 12 GiB JVM heap ceiling.

- Q16's full-batch grouping-capacity reservation was rejected: 431.2 ms and 9.671B instructions versus the adjacent
  428.5 ms and 9.481B control. It reduced dTLB misses but was not an end-to-end win.
- Q9's residual allocation is query-lifetime accumulator/join state. A larger global pool only reuses it across query
  invocations, so the 512 MiB default remains; the scan-to-sink batch path already turns over its scratch buffers.
- Q39 remains the corrected ten-column 660.6/598 ms Nitro/Velox comparison; the older apparent laggard was stale and
  had an eight-column Velox result.
- Q22 retains the width-gated SWAR result, 7711/7200 ms, with improvements in wall, instructions, cycles, L1D,
  dTLB, and branches against its Nitro control.
- Q24 freshly measures 1142.3 ms with 25.681B instructions, 6.661B cycles, 252.42M L1D misses, 7.780M dTLB misses,
  54.60M dTLB loads, and 26.19M branch misses, versus Velox's 1050 ms. Q64's fresh unpinned wall result is 2383.3 ms,
  effectively identical to Velox's 2380 ms. The remaining translation-counter gaps stay documented; previously
  tested eager densification, global branchless decoding, and huge-page controls lost wall time and remain removed.

## JDK 26 locality confirmation (2026-07-14)

TPC-H is the first suite completed across all three engines under the refreshed method. Non-converged Nitro rows
were rerun with five warmups; Q7/Q13/Q18/Q20/Q21 received a final isolated eight-warmup pass. Q18 remains noisy
and is reported with its five-iteration mean rather than cherry-picking its minimum.

| query | Nitro ms | Trino ms | Velox ms | N/T | N/V |
|---|---:|---:|---:|---:|---:|
| q01 | 1922.6 | 2169.7 | 2118 | 0.886 | 0.908 |
| q02 | 344.0 | 640.3 | 395 | 0.537 | 0.870 |
| q03 | 1605.8 | 2278.7 | 1856 | 0.705 | 0.865 |
| q04 | 1754.8 | 1731.8 | 2731 | 1.013 | 0.642 |
| q05 | 1544.8 | 2813.2 | 1828 | 0.549 | 0.845 |
| q06 | 811.7 | 788.4 | 1012 | 1.030 | 0.802 |
| q07 | 3570.4 | 5733.3 | 3323 | 0.623 | 1.074 |
| q08 | 1896.9 | 2617.0 | 1881 | 0.725 | 1.008 |
| q09 | 3277.2 | 6989.1 | 3220 | 0.469 | 1.018 |
| q10 | 2194.5 | 2459.2 | 2465 | 0.892 | 0.890 |
| q11 | 176.9 | 522.5 | 203 | 0.339 | 0.872 |
| q12 | 840.0 | 1104.8 | 983 | 0.760 | 0.855 |
| q13 | 3856.8 | 4971.1 | 4170 | 0.776 | 0.925 |
| q14 | 1346.5 | 1421.8 | 1340 | 0.947 | 1.005 |
| q15 | 1576.7 | 1645.2 | 1527 | 0.958 | 1.033 |
| q16 | 406.3 | 542.4 | 404 | 0.749 | 1.006 |
| q17 | 2653.5 | 3346.9 | 5164 | 0.793 | 0.514 |
| q18 | 2707.1 | 3108.6 | 2771 | 0.871 | 0.977 |
| q19 | 1459.7 | 1906.7 | 1540 | 0.766 | 0.948 |
| q20 | 1549.0 | 2489.9 | 1804 | 0.622 | 0.858 |
| q21 | 8000.8 | 8087.4 | 10300 | 0.989 | 0.777 |
| q22 | 569.5 | 873.4 | 613 | 0.652 | 0.929 |

TPC-H geometric means are **0.732 Nitro/Trino** and **0.881 Nitro/Velox**. Nitro wins 20/22 against Trino and
16/22 against Velox. Q7 is now the only material wall residual at 1.074x; Q8/Q9/Q14/Q16 are effectively parity rows.

Q7 had not actually matched Velox's physical plan. Nitro joined both complete nation tables, retained every column
through five joins, and evaluated the country pair in a separate filter. The corrected harness filters each nation
leaf to France/Germany, evaluates binary inequality as the final join residual, and uses integrated join-output
selection to expose exactly Velox's five columns after every join. No projection operator was inserted. Dataset
parity remains green. The clean unpinned THP capture is 3570.4 ms, 46.193B instructions, 19.037B cycles, 635.80M
L1D misses, 1.662M dTLB misses, 69.80M dTLB loads, and 35.84M branch misses. Fresh Velox is 3320 ms, 60.957B,
18.588B, 675.65M, 1.920M, 105.25M, and 46.38M respectively. Nitro now wins every captured hardware counter except
cycles (2.4% higher); the remaining wall difference is 7.4%.

Q11's corrected rerun is 176.9 ms, 3.591B instructions, 0.968B cycles, 24.62M L1D misses, 0.061M dTLB misses,
0.470M dTLB loads, and 2.77M branch misses. Its pure pass-through projection now advertises the general
batch-lifetime borrow-stability capability needed by dense sort buffering. This is deliberately weaker than
retention across source advance: a consumer that copies the current open batch needs stable repeated borrows, not
ownership after the batch closes. The corrected path restores the handoff result and beats Velox on wall and all
captured work/locality counters except branch misses.

Q13's left-join boundary is also exact: both engines expose only `(c_custkey, o_orderkey)` and do not carry the
duplicate build key into the two aggregations. A second general repair prevents a very large bounded-domain build
from pre-sizing its full hash table solely because a 4K sample looks distinct. Large samples whose observed numeric
domain fits the direct-range ceiling start with bounded state and retain exact fallback. Q13's 14.84M orders rows
remain in the 1.5M-key direct domain. A follow-on general layout raises the safe direct ceiling for Q7's unique dense
15M-key build without paying duplicate metadata up front: the pooled head map is allocated first, while tail/count
maps appear only after the first duplicate. Exact out-of-range fallback is unchanged. Against an adjacent forced-hash
control this reduces instructions 2.5%, cycles 1.6%, and branch misses 5.2%, with wall neutral inside phase noise.
The final warmed Q13 Nitro capture is
3856.8 ms, 46.358B instructions, 20.675B cycles, 419.41M L1D misses,
49.35M dTLB misses, 111.87M dTLB loads, and 97.17M branch misses. Fresh Velox is 4170 ms, 56.874B, 23.012B,
512.18M, 10.43M, 61.76M, and 86.88M respectively. Nitro now wins wall, instructions, cycles, and L1D; translation
and branch counters remain worse. The host had roughly 39 GiB of anonymous pressure and THP counters were volatile,
so the dTLB result is recorded rather than attributed to the join change.

A later healthy-memory Q7 confirmation measured 3859.5 ms, 46.336B instructions, 20.133B cycles, 622.67M L1D
misses, 0.399M dTLB misses, 64.83M dTLB loads, and 32.73M branch misses. Wall remained phase-sensitive and the
stable 3570.4 ms row above is retained, but the reduced work and translation counts reproduce. Velox remains
3320 ms / 60.957B / 18.588B / 675.65M / 1.920M / 105.25M / 46.38M.

## 2026-07-15 operator-only ClickBench refresh

All published rows in this refresh use the operator harnesses. Nitro and Trino are unpinned JDK 26 JVMs with THP
and 12 GiB heaps; Velox is pinned to core 0 with one driver, one I/O thread, one split per file, and a 12 GiB cache.
Velox reports the minimum of three independent processes per query. Nitro completed 44/44, Trino 37/44, and Velox
36/44. Velox still lacks Q0 and Q37--Q43; Trino lacks Q0, Q23, Q24, Q29, Q34, Q35, and Q40.

On the strict 31-query set completed by all engines, summed durations are **110,428.5 / 153,332.5 / 150,459.0 ms**
for Nitro / Trino / Velox. The geometric-mean ratios are **0.595 Nitro/Trino**, **0.683 Nitro/Velox**, and
**1.149 Trino/Velox**. Equivalently, Nitro leads Trino by **1.682x geometric mean and 1.389x summed duration**,
Nitro leads Velox by **1.463x and 1.363x**, and Velox leads Trino by **1.149x and 1.019x**.

Pairwise coverage gives Nitro/Trino 37 rows at 0.435 geometric mean and 112,413.4/180,962.2 ms summed duration
(2.297x geometric, 1.610x summed), and Nitro/Velox 36 rows at 0.698 geometric mean and
209,130.3/267,739.0 ms (1.432x geometric, 1.280x summed). These are broad-sweep values pending convergence reruns
for noisy JVM rows. Velox Q24 is a stable 21.99 s across all three processes versus Nitro's 11.34 s and requires a
physical-plan parity audit before the result is treated as causal. Velox Q30 spans 17.18--20.04 s and is flagged
despite using the predefined minimum policy.

Fresh isolated runs corrected the ranking and invalidated the earlier conclusion drawn from a stale, noisy Q24
huge-page experiment. Nitro and Trino remain unpinned; Velox retains its established pinned-core method; every
process is bounded at 12 GiB and engines do not overlap.

| suite/query | Nitro ms | Velox ms | N/V | status |
|---|---:|---:|---:|---|
| TPC-H q09 | 3289.9 | 3220 | 1.022 | THP removes the translation bottleneck |
| TPC-H q16 | 420.9 | 404 | 1.042 | no longer a material laggard |
| TPC-DS q24 | 1127.8 | 1030 | 1.095 | page-directory sharing prototype rejected |
| TPC-DS q39 | 647.1 | 598 | 1.082 | corrected ten-column shape; modest residual |
| TPC-DS q64 | 2348.0 | 2320 | 1.012 | parity |
| ClickBench q22 | 7539.6 | 7450 | 1.012 | parity |

Q9 is the decisive locality result. The same retained code without THP measured 3776.8 ms, 56.934B instructions,
20.413B cycles, 76.678M dTLB misses, and 139.155M dTLB loads. THP measures 3289.9 ms, 56.782B instructions,
18.585B cycles, 0.429M dTLB misses, and 14.261M dTLB loads. Logical work is flat while wall, cycles, and address
translation improve. Fresh Velox is 3220 ms / 55.459B instructions / 16.428B cycles / 18.102M dTLB misses /
115.262M dTLB loads. The JVM policy, rather than a query-specific layout change, gets Nitro within about 2%.

Q39's clean post-revert THP capture is 646.0 ms, 12.932B instructions, 3.560B cycles, 65.76M L1D misses,
0.142M dTLB misses, 1.296M dTLB loads, and 11.54M branch misses. Velox is 598 ms / 8.494B / 3.169B /
155.49M / 0.218M / 8.00M / 4.49M. Nitro wins the cache and translation counters but retains instruction and
branch overhead. A general generated flat-array grouping entry point was tested and removed: adjacent wall was
647.1/647.8 ms enabled/control, and the counter result did not reproduce against a clean control. The retained stack
is led by Snappy decompression (~22% of runnable samples); multi-long grouping is only ~6--7%.

The rejected Q24 prototype shared the mapping, footer, and compact page-header directory across same-query scans.
It regressed wall/instructions/cycles (1176.8 ms / 26.011B / 6.873B versus 1168.6 ms / 25.369B / 6.708B control),
so all source changes were removed. General design rule: share parsed metadata only when parsing is demonstrated to
be causal; do not trade a transient parse cost for a retained object graph or extra steady-state indirection.

## Raw results

JVM sweep CSVs are `/tmp/nitro-jdk26-20260713-{tpch,tpcds,clickbench}.csv` and the matching
`/tmp/trino-jdk26-20260713-*.csv`. The repaired Q50 result is
`/tmp/nitro-jdk26-20260713-tpcds-q50-fixed.csv`; Velox TPC-H JSON is
`/tmp/velox-jdk26-20260713-tpch.json`. Velox TPC-DS and ClickBench minima were collected from the three
independent process outputs during the sweep.
