# JDK 26 corrected single-thread TPC-DS N/V board (2026-07-12)

Superseded for the current three-engine baseline by
[`three-engine-board-jdk26-20260713.md`](three-engine-board-jdk26-20260713.md).

Nitro was rebuilt from scratch for JDK 26 preview and benchmarked on SF10 with JMH. This original sweep snapshot pinned
the whole JVM to CPU 0; retain the table as historical pinned data, but leave Nitro unpinned in subsequent refreshes so
JIT compiler and JVM service machinery do not contend with the measured thread. The run used one fork, one thread,
3 x 1-second warmup iterations, and 5 x 2-second measurement iterations. The JMH fork was capped at `-Xmx12g`; the
isolated Maven daemon was capped at 2 GiB. Available system memory stayed above 32 GiB during the full sweep.

Velox times are the corrected single-thread reference from `velox-single-thread-corrected-20260704.md`: min-of-3,
`taskset -c 0`, `--num_drivers=1`, and `--num_io_threads=1`. The Q45 and JDK changes did not alter the Velox harness or
logical operator trees, so those reference times remain the apples-to-apples comparison. Ratio above 1 means Nitro is
slower.

The full sweep initially exposed dictionary-peeling bounds bugs in q50, q62, q66, and q99. The bounds guards were folded
into the Q45 change, the four Nitro plans then executed successfully, and their board rows were rerun with the same JMH
configuration. Overall, Nitro/Velox geomean is **0.950x** across 98 matched queries; Nitro wins 58 and loses 40.

**Q45 counter revalidation:** Nitro is intentionally left unpinned so JIT compiler and JVM service machinery do not
contend with the measured thread; that transient machinery is not a steady-state tax for the normal long-lived JVM.
Fresh unpinned runs reproduce the handoff's performance (`67.52`--`69.20 ms`) and branch-miss win (`1.496M` latest vs
Velox `1.80M`). dTLB varies more: `91.7K`--`118.1K` in revalidation versus the historical `81.8K` and Velox `81K`, so
record it as near parity with run-to-run variance, not a stable Nitro win. Whole-process-pinned Nitro measurements are
diagnostic only and are not used for the board.

The leading three sweep laggards were rerun in isolation with 5 warmup and 8 measurement iterations. The results held:
q48 **3104.0 ms** (2.72x Velox), q54 **846.7 ms** (1.70x), and q84 **201.2 ms** (1.66x). q48 is the next frontier.

**Post-board q48 correction:** the Q45 stack exposed large join-output binary dictionary backings to q48's UTF-8
membership/equality filters, which evaluated every backing entry once per output batch. Gating dictionary-wide
evaluation by backing cardinality versus active rows restores q48 to **1147.3 ms** (1.006x the 1140 ms Velox
reference), with byte-identical output. The table below remains the original sweep snapshot; q48 is closed by the
follow-up measurement, leaving q54/q84 as the next candidates.

**Unpinned q54 follow-up:** the fresh baseline was **807.8 ms** (1.62x Velox). Its customer-key build produced about
15.6K rows, exceeding the 8,192-entry hash-join dynamic-filter cap, so all 28.8M `store_sales` rows reached the join
probe. Raising the hash-join cap to 65,536, still bounded by the existing 65,536 build-row collection guard, restores
probe-side scan filtering. The retained unpinned result is **671.1 ms** (1.35x), with instructions falling from
15.18B to 11.04B, cycles from 4.52B to 3.73B, and L1D misses from 158.6M to 128.3M. Unpinned sentinels held: q45
**68.3 ms**, q48 **1086.1 ms** (now 0.953x Velox), and q84 **175.1 ms** (within its noisy baseline interval).
The remaining cost exposed a missing `UnionAllOperator.pushDynamicFilter`: q54's eligible-item filter stopped at the
union, leaving 21.6M catalog/web rows per query to be rejected by the item hash probe. Broadcasting the schema-
preserving filter to both union branches closes q54 at **276.6 ms** (0.555x Velox), with **5.94B** instructions,
**1.60B** cycles, **52.5M** L1D misses, and **9.42M** branch misses. Post-change unpinned sentinels are q45 **67.83
ms**, q48 **1067.63 ms**, and q84 **171.76 ms**. q54 correctness and 163 operator tests pass; q84 is next.

**Unpinned q84 follow-up:** q84 built a duplicate-chain index over 2.88M `store_returns` rows containing only the join
key, then eagerly copied every row reference into contiguous ranges before a probe that emitted only about 1.2K rows.
Disabling chain compaction globally improved q84 but regressed q24 from **2129.6** to **2307.8 ms**. The retained policy
keeps compaction for payload-bearing builds and skips it only when every build output is a join key: multiplicity is
unchanged, while duplicate values are identical and the large eager copy is avoided. q84 falls from **171.76** to
**144.25 ms** (1.192x Velox), with **2.06B** instructions, **849M** cycles, **28.2M** L1D misses, **10.1M** dTLB
misses, and **3.76M** branch misses. q24 retains its compaction win at **2137 ms**; q45/q48/q54 sentinels hold at
**70.6/1101.7/275.8 ms** in the shorter regression pass. Dataset-backed q84 equivalence passes.

**Velox hardware-counter capture:** the retained Velox binary was run pinned to CPU 0 with one driver, one I/O thread,
and ten in-process executions (`VELOX_REPEAT=10`). `perf stat` totals were divided by ten, amortizing process startup.
Nitro figures are the retained unpinned JMH/perfnorm results above, per the long-lived-JVM methodology.

| query/engine | wall | instructions | cycles | IPC | L1D misses | dTLB misses | branch misses | dTLB loads |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q54 Nitro | 276.6 ms | 5.94B | 1.60B | 3.72 | 52.5M | 305K | 9.42M | 4.45M |
| q54 Velox | 467 ms | 7.83B | 2.44B | 3.21 | 116.3M | 621K | 10.90M | 22.23M |
| q84 Nitro | 144.3 ms | 2.06B | 849M | 2.42 | 28.2M | 10.10M | 3.76M | 13.55M |
| q84 Velox | 110 ms | 2.58B | 594M | 4.34 | 15.84M | 33.5K | 1.62M | 0.434M |

q54 Nitro wins every captured counter. q84 Nitro executes about 20% fewer instructions, but needs 43% more cycles;
its L1D misses are 1.78x Velox, branch misses 2.33x, dTLB loads 31x, and dTLB misses roughly 301x. The q84 residual is
therefore a translation/locality problem, not excess logical work.

**q84 duplicate-reference follow-up:** perfasm attributed 43% of q84 dTLB samples to `LongJoinIndex.add`. For a
key-only build, all duplicate rows have identical output values, so the index now stores one representative reference
plus the exact multiplicity rather than one reference per duplicate. The sparse hash table remains row-count-sized:
shrinking it reduced dTLB but regressed wall time, and decoupling only the reference-array size also regressed. The
retained result is **127.29 ms**, **1.94B** instructions, **747M** cycles, **24.8M** L1D misses, **7.81M** dTLB
misses, and **3.59M** branch misses—within 5% of the retained 121 ms Velox reference. Velox source confirms its normal
inner join keeps duplicates as inline-linked `RowContainer` rows; its contiguous native allocation/page behavior is
the material layout advantage. `-XX:+UseTransparentHugePages` brings q84 to **120.04 ms**, but is not retained because
it regresses and destabilizes q24 (`2496 ms`, high variance). q24/q45/q48/q54 and q84 correctness sentinels pass.

**Next-laggard recheck and q82 correction (2026-07-13):** fresh unpinned counters ranked q82 clearly first at
**7164 ms** (1.66x Velox), versus q18 **456.6 ms** (1.30x) and q93 **472.0 ms** (1.11x). q82 executed **48.6B**
instructions and **40.4B** cycles at only **1.20 IPC**, with **934M** L1D and **304M** dTLB misses. Operator
attribution showed q82 eagerly compacting roughly 50M duplicate inventory build references before probing with only
15 eligible items. Velox uses the same join orientation, so this was a physical-layout policy issue rather than a plan
mismatch. Chain compaction is now gated by the initial probe batch size (minimum 256 rows): q82 skips the O(build)
copy, while high-probe q24 retains its sequential-chain win. q82 falls to **3329.7 ms** (0.771x Velox), **46.6B**
instructions, **19.7B** cycles, **2.36 IPC**, **806M** L1D, and **205M** dTLB misses. q24 holds at **2130 ms**;
q18/q45/q48/q54/q84/q93 sentinels hold, and dataset-backed q82 equivalence passes.

**Velox counters for the next-laggard trio:** using the same pinned one-driver/one-I/O-thread, ten-repeat Velox
protocol, fresh execution times were q82 `4.04s`, q18 `564ms` (confirmed twice; the older `352ms` reference is not
representative of the current retained binary), and q93 `406ms`. Counts below are per query; Nitro uses the latest
unpinned retained result, including adaptive q82 compaction.

| query/engine | instructions | cycles | IPC | L1D misses | dTLB misses | branch misses | dTLB loads |
|---|---:|---:|---:|---:|---:|---:|---:|
| q82 Nitro | 46.59B | 19.75B | 2.36 | 806.2M | 205.4M | 109.2M | 258.0M |
| q82 Velox | 89.45B | 20.98B | 4.26 | 691.9M | 0.541M | 96.1M | 41.0M |
| q18 Nitro | 8.50B | 2.54B | 3.35 | 133.7M | 8.84M | 12.33M | 21.51M |
| q18 Velox | 9.57B | 2.94B | 3.25 | 97.5M | 0.680M | 8.33M | 11.54M |
| q93 Nitro | 8.86B | 2.55B | 3.48 | 77.2M | 0.356M | 18.35M | 10.24M |
| q93 Velox | 6.71B | 2.13B | 3.15 | 42.9M | 0.372M | 13.33M | 5.07M |

q82 Nitro does 48% fewer instructions and slightly fewer cycles, explaining its wall win despite 1.17x L1D,
6.3x dTLB loads, and roughly 379x dTLB misses. q18 similarly wins instructions/cycles in the fresh capture but loses
locality (1.37x L1D, 13x dTLB, 1.48x branch misses). q93 is the only one that loses both logical work and cache/branch
counters; its dTLB misses are already at Velox parity.

**q82 same-shape locality continuation:** the tempting item-build/inventory-probe swap produced excellent counters
but was rejected and reverted because it changed the physical harness shape relative to Velox and Trino. On the valid
inventory-build/item-probe shape, post-compaction perfasm attributed 23% of dTLB samples to `LongJoinIndex.add`, with
another ~24% in retained-column copying and mask growth. Nitro had sized hash slots from 50M build rows despite a much
smaller repeated item-key domain. The retained runtime policy starts payload-bearing single-long builds at 256K slots
when the build exceeds 40M rows or a first-batch sample is strongly repetitive; the table still grows normally.
q82 is **2995.9 ms** (0.694x the 4320 ms reference). Representative counters are **46.08B** instructions, **18.67B**
cycles, **2.47 IPC**, **821M** L1D, **5.05M** dTLB, and **117M** branch misses. This cuts dTLB about 60x from the
original 304M and cycles by 54%, but still loses Velox's 692M L1D, 0.541M dTLB, and 96M branches. THP lowers dTLB
further to 2.8M but is noisy and not retained. Velox source selects range-array hash modes and stores rows packed in
`RowContainer`. Nitro now mirrors the range-array choice for these bounded repetitive builds: a growable direct-key
head/tail/count index preserves duplicate chains and falls back to the hash table if a key leaves the supported range.
The same-shaped retained result is **2643.5 ms** (0.612x Velox), **43.51B** instructions, **16.54B** cycles, **2.63
IPC**, **658.5M** L1D, **3.13M** dTLB, **113.7M** branch misses, and **42.46M** dTLB loads. Nitro now beats Velox on
wall, instructions, cycles, and L1D and is within 4% on dTLB loads. Remaining gaps are dTLB misses (`5.8x`) and branch
misses (`1.18x`), attributed mainly to retained build-column/mask storage and inventory range filtering. A branchless
mask-compaction source experiment washed and was reverted. q82 equivalence and 164 tests pass; q24/q45/q48/q54/q84
sentinels hold.

**q82 range-filter follow-up:** the null-free dense long bounds scan now uses the JDK Vector API to produce selected
positions from comparison masks. On the unchanged inventory-build/item-probe shape, q82 remains statistically flat at
**2654.0 ms**, while cycles improve to **16.30B** and branch misses to **111.36M**; instructions are **43.84B**, L1D
misses **656.7M**, dTLB misses **3.13M**, and dTLB loads **44.93M**. This is a small branch/cycle win, not a solution
to the remaining translation gap: perfasm still places the residual dTLB cost primarily in retained build-column and
mask/buffer storage. Short unpinned sentinels hold at q24/q45/q48/q54/q84 =
**2111.7/69.64/1132.8/285.4/132.7 ms**. The JMH fork remained capped at `-Xmx12g`; 42 GiB was available afterward.

**q82 build-buffer locality follow-up:** join output/probe batches remain 10K, but non-retained build input is now
buffered at the packed row reference's natural 65,536-position boundary. This does not alter the operator tree or
build/probe orientation; it reduces separately allocated retained-column fragments. A 32K control point was worse
on every locality counter. At 64K, q82 is **2632.1 ms**, **43.45B** instructions, **15.48B** cycles, **2.81 IPC**,
**635.7M** L1D misses, **2.84M** dTLB misses, **109.03M** branch misses, and **42.59M** dTLB loads. Relative to
Velox, Nitro now wins L1D by 8%, dTLB loads are within 4%, and branch misses are 1.13x; dTLB misses remain 5.25x.
Dataset-backed q82 equivalence passes. Short unpinned sentinels hold at q24/q45/q48/q54/q84 =
**2128.7/70.29/1147.0/284.8/131.6 ms**; 46 GiB remained available after the capped run.

**q82 conjunctive-range fusion:** the evaluator now recognizes strict lower/upper constant bounds on the same flat
integer column inside an AND mask and applies them in one null/error-aware scan; unrelated terms retain adaptive
ordering. This is expression execution only and does not change the filter or join operator shape. Two q82 captures
were **2580.8/2540.9 ms**. The retained repeat used **42.61B** instructions, **15.79B** cycles, **2.70 IPC**,
**627.4M** L1D misses, **2.75M** dTLB misses, **105.01M** branch misses, and **41.06M** dTLB loads. Nitro now beats
Velox on dTLB loads as well as wall/instructions/cycles/L1D; the remaining gaps are dTLB misses (`5.08x`) and branch
misses (`1.09x`). Dataset q82 equivalence and null-aware evaluator tests pass. The short sentinel pass was
q24/q45/q48/q54/q84 = **2208.1/70.61/1162.2/296.3/133.8 ms**; a longer q24 A/B was 2149 ms fusion-on versus
2110 ms off, but q24 has no eligible constant-range conjunction, so the difference is environmental variance.

**q82 compact chain references and mask ownership:** capped repetitive long builds now keep duplicate-chain row
references in the existing 31-bit batch/position encoding. If any reference does not fit, the index promotes all
entries to the prior 64-bit representation before continuing. The direct A/B was **2495 ms** compact versus
**2584 ms** forced-64-bit, with dTLB misses **2.80M** versus **3.07M**. Separately, `FilterOperator` now transfers or
releases the selection mask it owns instead of retaining every batch's copied positions until close; replacement
masks owned downstream are deliberately untouched. Its direct recycle/leak A/B favored recycling on wall,
instructions, cycles, L1D, and dTLB misses. The final retained capture is **2497.5 ms**, **42.29B** instructions,
**15.78B** cycles, **2.68 IPC**, **620.7M** L1D misses, **2.76M** dTLB misses, **104.76M** branch misses, and
**41.26M** dTLB loads. Nitro is now within 1% of Velox on dTLB loads and 9% on branches; dTLB misses remain the only
large counter gap at `5.1x`. Dataset equivalence and 164 tests pass. Short sentinels hold at q24/q45/q48/q54/q84 =
**2130.1/72.68/1148.1/282.6/129.1 ms**. The fork remained `-Xmx12g`, with 46 GiB available afterward.

**q82 copied-build batch lifetime:** post-recycling perfasm showed that `Mask.ensureCapacity` still dominated because
`BufferedJoinInput` copied non-retained wrapper batches without closing them. The underlying scan auto-closed its own
previous batch, but the filter wrapper—and therefore its owned selection mask—remained live. Fully copied build
batches are now closed immediately; retained/deferred batches are unchanged. The direct close/old-lifetime A/B was
**2522/2594 ms**, cutting cycles **14.43B/16.21B**, L1D **573M/621M**, and dTLB misses **2.41M/2.76M**. An attempted
zero-copy use of the mask's positions array regressed wall and dTLB locality and was fully reverted; the short-lived
contiguous copy is beneficial. The final retained capture is **2436.0 ms**, **41.45B** instructions, **14.14B**
cycles, **2.93 IPC**, **577.9M** L1D misses, **2.39M** dTLB misses, **104.61M** branch misses, and **41.18M** dTLB
loads. Nitro now beats Velox by 16% on L1D and is within 0.5% on dTLB loads and 9% on branches; dTLB misses remain
`4.4x`. Dataset equivalence, 164 tests, and q24/q45/q48/q54/q84 sentinels pass. Memory remained safe with 45 GiB
available after the `-Xmx12g` run.

**q82 residual diagnostics:** `-XX:+AlwaysPreTouch` lowers dTLB misses to **1.87M** and dTLB loads to **36.89M**
with statistically unchanged wall (**2449 ms**), confirming a material first-touch/page-commit component. It is not
retained in the board methodology. A 256K build-fragment/64-bit-reference control regressed to **2559 ms** and
**2.90M** dTLB misses, so a wider dynamic packed-reference format is not warranted. Vector-API compression of
matching range lanes also regressed instructions/cycles without reducing branches and was reverted. Post-close
perfasm now attributes dTLB primarily to build-column copying/indexing plus Linux VMA/page-fault handling; the former
mask-capacity hotspot is gone.

**q82 steady-state translation check:** extending warmup from three to ten full q82 executions, without pre-touch or
pinning, leaves wall unchanged at **2436.6 ms** but converges to **41.91B** instructions, **13.89B** cycles,
**569.6M** L1D misses, **1.80M** dTLB misses, **103.22M** branch misses, and **39.13M** dTLB loads. This agrees with
the pre-touch diagnostic and shows that roughly 0.6M of the standard short-run dTLB count is continued G1 heap-page
commitment rather than steady-state join translation. Against Velox, the steady-state residual is `3.3x` dTLB misses
and `1.07x` branches; Nitro wins all other captured counters, including dTLB loads. Two fused-range branch-removal
experiments were rejected: scalar branchless compaction was **2703 ms**, while lookup-packed fixed stores were
**2734 ms** and destabilized long forks. Both were fully reverted.

| query | Nitro ms | Velox ms | N/V |
|---|---:|---:|---:|
| q48 | 3156.360 | 1140 | 2.769 |
| q84 | 213.459 | 121 | 1.764 |
| q54 | 856.696 | 498 | 1.720 |
| q82 | 7274.225 | 4320 | 1.684 |
| q18 | 589.655 | 352 | 1.675 |
| q93 | 712.552 | 427 | 1.669 |
| q75 | 4609.630 | 2860 | 1.612 |
| q13 | 2026.361 | 1270 | 1.596 |
| q11 | 5229.605 | 3300 | 1.585 |
| q26 | 345.150 | 218 | 1.583 |
| q99 | 940.163 | 598 | 1.572 |
| q37 | 7170.197 | 4570 | 1.569 |
| q85 | 778.298 | 504 | 1.544 |
| q20 | 91.071 | 60 | 1.518 |
| q86 | 261.394 | 173 | 1.511 |
| q57 | 2961.797 | 1980 | 1.496 |
| q70 | 1479.395 | 1040 | 1.422 |
| q7 | 841.918 | 592 | 1.422 |
| q51 | 5242.836 | 3890 | 1.348 |
| q67 | 11731.275 | 8710 | 1.347 |
| q15 | 145.331 | 110 | 1.321 |
| q36 | 643.444 | 493 | 1.305 |
| q62 | 384.664 | 309 | 1.245 |
| q47 | 4962.313 | 3990 | 1.244 |
| q50 | 1497.691 | 1210 | 1.238 |
| q72 | 4193.560 | 3400 | 1.233 |
| q39 | 774.403 | 645 | 1.201 |
| q65 | 1390.036 | 1170 | 1.188 |
| q22 | 1184.838 | 1000 | 1.185 |
| q27 | 711.275 | 612 | 1.162 |
| q32 | 70.826 | 63 | 1.124 |
| q79 | 676.633 | 612 | 1.106 |
| q41 | 10.779 | 10 | 1.078 |
| q98 | 229.760 | 215 | 1.069 |
| q4 | 2069.242 | 1940 | 1.067 |
| q64 | 2668.278 | 2510 | 1.063 |
| q81 | 281.017 | 268 | 1.049 |
| q76 | 600.119 | 590 | 1.017 |
| q21 | 484.915 | 484 | 1.002 |
| q44 | 981.349 | 979 | 1.002 |
| q12 | 69.254 | 70 | 0.989 |
| q8 | 248.273 | 252 | 0.985 |
| q1 | 216.338 | 220 | 0.983 |
| q89 | 355.921 | 371 | 0.959 |
| q34 | 326.713 | 343 | 0.953 |
| q66 | 293.911 | 310 | 0.948 |
| q24 | 2271.525 | 2410 | 0.943 |
| q97 | 1541.651 | 1650 | 0.934 |
| q40 | 132.186 | 143 | 0.924 |
| q83 | 57.784 | 63 | 0.917 |
| q30 | 239.647 | 263 | 0.911 |
| q23 | 6685.538 | 7370 | 0.907 |
| q87 | 1129.283 | 1250 | 0.903 |
| q46 | 567.412 | 629 | 0.902 |
| q69 | 426.944 | 474 | 0.901 |
| q49 | 358.179 | 404 | 0.887 |
| q43 | 461.074 | 524 | 0.880 |
| q9 | 3431.980 | 3920 | 0.876 |
| q35 | 562.708 | 651 | 0.864 |
| q91 | 90.677 | 106 | 0.855 |
| q96 | 119.971 | 141 | 0.851 |
| q60 | 343.834 | 408 | 0.843 |
| q88 | 1095.274 | 1300 | 0.843 |
| q10 | 413.605 | 492 | 0.841 |
| q38 | 1040.233 | 1240 | 0.839 |
| q73 | 270.508 | 323 | 0.837 |
| q71 | 287.530 | 344 | 0.836 |
| q56 | 297.382 | 360 | 0.826 |
| q68 | 542.845 | 664 | 0.818 |
| q31 | 714.533 | 920 | 0.777 |
| q61 | 473.087 | 615 | 0.769 |
| q53 | 246.175 | 323 | 0.762 |
| q33 | 273.188 | 363 | 0.753 |
| q63 | 241.500 | 324 | 0.745 |
| q19 | 256.544 | 345 | 0.744 |
| q77 | 236.143 | 321 | 0.736 |
| q45 | 93.279 | 127 | 0.734 |
| q92 | 47.658 | 65 | 0.733 |
| q74 | 1575.728 | 2170 | 0.726 |
| q52 | 121.236 | 172 | 0.705 |
| q55 | 117.735 | 168 | 0.701 |
| q59 | 1006.623 | 1450 | 0.694 |
| q78 | 3020.991 | 4400 | 0.687 |
| q42 | 115.457 | 169 | 0.683 |
| q16 | 6016.030 | 9060 | 0.664 |
| q29 | 1042.851 | 1590 | 0.656 |
| q25 | 1036.469 | 1590 | 0.652 |
| q95 | 1600.369 | 2470 | 0.648 |
| q17 | 1033.088 | 1620 | 0.638 |
| q80 | 550.392 | 901 | 0.611 |
| q5 | 260.154 | 437 | 0.595 |
| q94 | 1439.886 | 2470 | 0.583 |
| q28 | 2586.985 | 4830 | 0.536 |
| q3 | 89.481 | 175 | 0.511 |
| q90 | 36.561 | 73 | 0.501 |
| q2 | 1323.602 | 2940 | 0.450 |
| q6 | 570.812 | 2009 | 0.284 |
| q58 | 505.185 | 1890 | 0.267 |
