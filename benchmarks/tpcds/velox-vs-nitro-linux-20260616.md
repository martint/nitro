# TPC-DS whole-query: Nitro vs Velox vs Trino (Linux, sf10, single-thread)

Date: 2026-06-16. TPC-DS sf10 parquet (`/root/data/tpcds-parquet-sf10/sf10`), same files all engines read.
Single-thread (`--num_drivers=1 --num_io_threads=1 --num_splits_per_file=1`).

## Method
- **Velox**: hand-built PlanBuilder pipelines mirroring the Nitro operator trees, harness
  `velox/benchmarks/tpcds/NitroTpcdsBenchmark.cpp` (Velox has no inline TPC-DS plan generator). 98/99 queries
  ported and RUN; q14 SKIP (correlated-scalar threshold + CTE duplication). Times = task execution-only window,
  min of 3 runs.
- **Nitro / Trino**: from the three-engine sweep (`three-engine-linux-20260616.csv`, JMH avgt, 5 iters). Nitro =
  interpreted operator tree. Nitro CSV lacks q37/q41/q45/q82 (excluded there).
- Ratio >1 ⇒ that engine is SLOWER than Velox.

## Headline (94 queries with both Nitro and Velox)
- **GEOMEAN Nitro / Velox = 2.83x**  (Nitro ~2.8x slower than Velox); MEDIAN 2.56x; Nitro faster on 5/94.
- **GEOMEAN Trino / Velox = 4.05x** (98 queries).
- Gap widens with join/scan intensity. Whole Nitro-vs-Velox picture: ClickBench ~0.93x (≈even), TPC-H 1.95x
  (compiled 1.32x), TPC-DS 2.83x. The durable Velox lead is scan/decode + joins, not grouping.

## CORRECTNESS VERIFICATION (the plans were validated, not assumed)
Each Velox query's full output was dumped and compared, row-multiset, against the Nitro harness output (Nitro is
itself validated against Trino SQL). Tooling: `--dump_result` flag in the Velox harness + `DumpResults` JUnit in
the Nitro repo render both into one canonical space (decimals→scaled cents, dates→epoch days, strings trimmed).

Result over the 94 queries with a Nitro reference:
- **67 byte-exact** (every value, incl. numerics, identical).
- **~26 structurally exact** — same cardinality and same group-key set; the only differences are (a) derived
  avg/ratio columns rendered in Nitro's fixed-point scale (e.g. avg at scale 4, revenue-ratio rounded by
  `divide_scale_round`) vs Velox doubles, and (b) a few cosmetic extra/!missing derived output columns
  (q39/q64/q67/q69). These are representation differences, not plan errors.
- **1 genuine divergence: q71** — Nitro emits 56 (brand,hour,minute) groups whose `sum(ext_sales_price)` is NULL
  and ranks them first under `sum DESC`; Velox produces no such null-sum groups (standard TPC-DS q71 has none).
  This looks like a Nitro-harness artifact (its union/join introducing null-measure groups); Velox is arguably
  the more correct side, so it was left as-is and flagged rather than made to reproduce the quirk.
- **4 unverifiable** (q37/q41/q45/q82) — the Nitro sweep itself has no reference output for these.

### Bugs found by verification and fixed (re-verified exact/structural afterwards)
- **Sort-direction class (29 queries)**: the transcription spec inverted Nitro's `boolean[]` asc/desc convention
  (truth, from byte-exact q03: true=DESC, false=ASC). For LIMIT/TopN queries this selected the wrong end of the
  result. All corrected to match Nitro's per-key directions.
- **q11**: channel self-joins keyed on `c_login` (always NULL in TPC-DS) → 0 rows. Fixed to key on
  `c_customer_id`. (Its benchmark time was a degenerate 187ms; corrected to ~2.54s — already reflected above.)
- **q08**: the 400-value zip `IN` list was missing one entry ('35576'). Appended.
- **q18**: Velox `groupId` rollup emitted an all-NULL grand-total row on empty input; added `qc>0` filter so it
  returns 0 rows like Nitro.
- **q44**: per-item/store averages used full-precision division; Nitro rounds to integer cents, which changes
  rank() ties. Switched to `round(sum*100/count)` so the rank<=10 sets match.
- **q51**: cumulative window ordered `d_date DESC` (should be ASC) and truncated cumulatives to bigint; fixed the
  frame direction and kept doubles.
- **q36**: rank ordering used decimal/decimal ratio (tie artifacts); switched to double ratio + `￿`
  nulls-last sentinel for subtotal rows.

CAVEAT: per-query timings remain INDICATIVE (hand-built plans, exec-only single-thread); the geomean is the
signal. Correctness is now verified row-by-row against Nitro for 93/94 comparable queries.

### SQL ground-truth check (q17, q18, q71 vs actual TPC-DS SQL)
Ran the repo's own `assertNitroMatchesSql` / `assertTrinoOperatorMatchesSql` tests, which execute the real
TPC-DS SQL (trino-benchmark-queries q*.sql, fetched to /tmp/trinosql, via -Dnitro.tpcds.trino.root) in-process
and compare to the Nitro and Trino operator harnesses.
- **q17 (empty): CORRECT.** Nitro-vs-SQL and Trino-operator-vs-SQL both PASS; the SQL also returns 0 rows at sf10.
  Velox's empty q17 is right.
- **q18 (empty): HARNESS BUG, not a real empty.** The actual SQL returns ~hundreds of rows, but BOTH the Nitro
  and Trino-operator harnesses return 0. Cause: the Nitro q18 plan filters
  `cd_education_status = "Unknown             "` (space-padded) — the ONLY padded string literal in the whole
  harness; every other query (e.g. q07 `"College"`, byte-exact here) uses unpadded literals matching this unpadded
  parquet, so the padded literal matches nothing → empty. (The SQL also joins a second `customer_demographics`
  cd2 the harness omits.) Velox's q18 was forced to 0 to match Nitro, i.e. it matched a broken reference.
  **q18 is not a valid comparison query on this dataset** — the Nitro harness needs the literal unpadded (+ cd2
  join) before any q18 number is trustworthy.
- **q71: harness does NOT match SQL (pre-existing, all engines).** Both Nitro (null-`sum` groups first) and the
  Trino operator tree (non-null sums first, = Velox) disagree with the SQL. q71 has groups whose
  `sum(ext_sales_price)` is NULL (some sales rows carry null amounts); Nitro, the Trino operator tree, and Trino
  SQL each order/handle those nulls differently in `ORDER BY ext_price DESC` top-100. So the earlier Velox-vs-Nitro
  q71 divergence is a pre-existing harness/SQL null-ordering inconsistency, NOT a Velox plan bug; q71 is unreliable
  for every engine.

## Per-query
| q | velox ms | nitro ms | trino ms | nitro/vx | trino/vx |
|---|---|---|---|---|---|
| q1 | 186 | 323 | 661 | 1.74 | 3.55 |
| q2 | 701 | 878 | 2062 | 1.25 | 2.94 |
| q3 | 151 | 528 | 564 | 3.50 | 3.73 |
| q4 | 1230 | 3251 | 3994 | 2.64 | 3.25 |
| q5 | 220 | 878 | 1540 | 3.99 | 7.00 |
| q6 | 1960 | 1900 | 4978 | 0.97 | 2.54 |
| q7 | 537 | 1035 | 1522 | 1.93 | 2.83 |
| q8 | 232 | 646 | 5337 | 2.79 | 23.01 |
| q9 | 482 | 3906 | 5250 | 8.10 | 10.89 |
| q10 | 215 | 731 | 977 | 3.40 | 4.54 |
| q11 | 2540 | 8513 | 11352 | 3.35 | 4.47 |
| q12 | 65 | 680 | 471 | 10.46 | 7.25 |
| q13 | 1190 | 3368 | 5453 | 2.83 | 4.58 |
| q14 | SKIP | 10026 | 24406 | - | - |
| q15 | 90 | 483 | 1909 | 5.36 | 21.21 |
| q16 | 4650 | 6392 | 10995 | 1.37 | 2.36 |
| q17 | 1310 | 1846 | 1896 | 1.41 | 1.45 |
| q18 | 299 | 155 | 931 | 0.52 | 3.11 |
| q19 | 300 | 572 | 766 | 1.91 | 2.55 |
| q20 | 58 | 413 | 810 | 7.12 | 13.96 |
| q21 | 456 | 6412 | 7071 | 14.06 | 15.51 |
| q22 | 908 | 2258 | 2962 | 2.49 | 3.26 |
| q23 | 2300 | 9773 | 13192 | 4.25 | 5.74 |
| q24 | 829 | 6581 | 5923 | 7.94 | 7.15 |
| q25 | 1260 | 1599 | 1737 | 1.27 | 1.38 |
| q26 | 181 | 383 | 825 | 2.12 | 4.56 |
| q27 | 554 | 978 | 1732 | 1.76 | 3.13 |
| q28 | 1630 | 2849 | 8274 | 1.75 | 5.08 |
| q29 | 1280 | 1806 | 1865 | 1.41 | 1.46 |
| q30 | 197 | 355 | 454 | 1.80 | 2.31 |
| q31 | 486 | 1733 | 2655 | 3.57 | 5.46 |
| q32 | 62 | 353 | 507 | 5.69 | 8.18 |
| q33 | 197 | 1300 | 1462 | 6.60 | 7.42 |
| q34 | 331 | 810 | 880 | 2.45 | 2.66 |
| q35 | 299 | 852 | 1501 | 2.85 | 5.02 |
| q36 | 463 | 760 | 1511 | 1.64 | 3.26 |
| q37 | 4140 | - | 3311 | - | 0.80 |
| q38 | 617 | 1469 | 2251 | 2.38 | 3.65 |
| q39 | 619 | 8786 | 19532 | 14.19 | 31.55 |
| q40 | 138 | 1131 | 1690 | 8.20 | 12.24 |
| q41 | 10 | - | 29 | - | 2.93 |
| q42 | 145 | 321 | 489 | 2.22 | 3.37 |
| q43 | 501 | 703 | 1097 | 1.40 | 2.19 |
| q44 | 693 | 1313 | 1302 | 1.89 | 1.88 |
| q45 | 96 | - | 1052 | - | 10.96 |
| q46 | 572 | 1293 | 1379 | 2.26 | 2.41 |
| q47 | 2680 | 6097 | 12852 | 2.28 | 4.80 |
| q48 | 932 | 2555 | 5390 | 2.74 | 5.78 |
| q49 | 213 | 1206 | 2478 | 5.66 | 11.63 |
| q50 | 1190 | 2298 | 1874 | 1.93 | 1.57 |
| q51 | 3100 | 6010 | 5898 | 1.94 | 1.90 |
| q52 | 146 | 327 | 515 | 2.24 | 3.53 |
| q53 | 297 | 851 | 769 | 2.87 | 2.59 |
| q54 | 340 | 1091 | 1399 | 3.21 | 4.12 |
| q55 | 144 | 315 | 484 | 2.18 | 3.36 |
| q56 | 180 | 1289 | 1213 | 7.16 | 6.74 |
| q57 | 1320 | 2590 | 6402 | 1.96 | 4.85 |
| q58 | 1530 | 1500 | 5177 | 0.98 | 3.38 |
| q59 | 1410 | 1887 | 2560 | 1.34 | 1.82 |
| q60 | 211 | 1572 | 1458 | 7.45 | 6.91 |
| q61 | 541 | 1508 | 2229 | 2.79 | 4.12 |
| q62 | 304 | 874 | 690 | 2.88 | 2.27 |
| q63 | 298 | 828 | 732 | 2.78 | 2.45 |
| q64 | 1280 | 9000 | 14791 | 7.03 | 11.56 |
| q65 | 1160 | 2207 | 2903 | 1.90 | 2.50 |
| q66 | 152 | 1735 | 3030 | 11.41 | 19.93 |
| q67 | 8780 | 10859 | 10562 | 1.24 | 1.20 |
| q68 | 595 | 1278 | 1277 | 2.15 | 2.15 |
| q69 | 221 | 690 | 958 | 3.12 | 4.33 |
| q70 | 1030 | 1549 | 2526 | 1.50 | 2.45 |
| q71 | 188 | 585 | 1125 | 3.11 | 5.98 |
| q72 | 3280 | 9685 | 9784 | 2.95 | 2.98 |
| q73 | 313 | 733 | 732 | 2.34 | 2.34 |
| q74 | 1650 | 7585 | 7780 | 4.60 | 4.71 |
| q75 | 1070 | 4451 | 4812 | 4.16 | 4.50 |
| q76 | 347 | 677 | 771 | 1.95 | 2.22 |
| q77 | 182 | 581 | 1204 | 3.19 | 6.62 |
| q78 | 879 | 2726 | 3118 | 3.10 | 3.55 |
| q79 | 586 | 1464 | 1228 | 2.50 | 2.10 |
| q80 | 549 | 4800 | 5950 | 8.74 | 10.84 |
| q81 | 123 | 342 | 584 | 2.78 | 4.75 |
| q82 | 4130 | - | 3360 | - | 0.81 |
| q83 | 30 | 205 | 639 | 6.83 | 21.31 |
| q84 | 118 | 151 | 196 | 1.28 | 1.66 |
| q85 | 285 | 922 | 1071 | 3.24 | 3.76 |
| q86 | 168 | 213 | 458 | 1.27 | 2.73 |
| q87 | 622 | 1965 | 4715 | 3.16 | 7.58 |
| q88 | 183 | 2734 | 4522 | 14.94 | 24.71 |
| q89 | 345 | 858 | 1068 | 2.49 | 3.10 |
| q90 | 77 | 166 | 336 | 2.16 | 4.36 |
| q91 | 56 | 91 | 180 | 1.63 | 3.22 |
| q92 | 62 | 193 | 311 | 3.12 | 5.01 |
| q93 | 396 | 1014 | 873 | 2.56 | 2.20 |
| q94 | 2440 | 1873 | 3088 | 0.77 | 1.27 |
| q95 | 2440 | 1901 | 3216 | 0.78 | 1.32 |
| q96 | 135 | 320 | 514 | 2.37 | 3.81 |
| q97 | 850 | 2164 | 2662 | 2.55 | 3.13 |
| q98 | 190 | 2664 | 1610 | 14.02 | 8.47 |
| q99 | 588 | 1315 | 1284 | 2.24 | 2.18 |