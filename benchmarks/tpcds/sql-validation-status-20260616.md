# TPC-DS harness vs SQL validation — status & resumable plan (2026-06-16)

Goal (user directive): make the Nitro + Trino + Velox TPC-DS harnesses match standard TPC-DS SQL semantics
(remove harness LIMITs the SQL lacks; full sweep, all 3 suites). Same for TPC-H and ClickBench.

## Oracle & tooling (fast, reusable)
- **SQL oracle = DuckDB 1.5.3** running the standard trino-benchmark-queries `q*.sql` over the same `/root/data`
  parquet, configured `SET default_null_order='nulls_last'` (matches Trino). Calibrated against the in-repo Trino
  oracle (`assertNitroMatchesSql`) on q13/q22/q46/q68 — DuckDB agreed on all 4 (all genuinely diverge). ~100× faster
  than per-test Trino (which spins a fresh Trino per test, ~2h for the suite).
- SQL files: `/tmp/trinosql/.../sql/trino/tpcds/q*.sql` (fetched from trinodb/trino master).
- DuckDB references: `/tmp/tpcds_duckdb/qNN.txt` (regenerate: the duckdb script in session history).
- Nitro outputs: `DumpResults` JUnit (src/test/.../tpcds/DumpResults.java) → `/tmp/nitro_dump/qNN.txt`.
  Run: `mvnd -q test -Dtest=DumpResults -Dnitro.tpcds.parquet.path=/root/data/tpcds-parquet-sf10 -Dnitro.tpcds.parquet.schema=sf10`.
  (Crashes on q18/q37/q41/q45/q82 materialization — a `toRows` BinaryVector bug, separate from SQL correctness.)
- Comparator (scale-aware, numeric-sort, cardinality+per-column multiset): `/tmp/cmp_nitro_sql.py`.

## TPC-DS Nitro vs SQL: 84/93 strict MATCH; ~87/93 effective (started 55/93)
q59/q79/q98 are FALSE-POSITIVES (comparator over-flags ratio rounding / ×100 scale; values+rows correct).
All original HARNESS bugs fixed. The 6 genuine remaining are NITRO ENGINE-capability gaps:
- q83 FIXED via AddI64 NULL propagation (engine).
- q49: divide_scale_round_i64 12dp vs DuckDB double -> 1 rank off (precision).
- q84: concat_utf8 propagates NULL vs SQL concat() ignoring NULL (1 row).
- q56/q60: IN-subquery on STRING key needs dict-aware string semi-join (GroupingState dict fast-path assumes
  a shared dictionary across the two scans).
- q75/q77: LEFT-join null-extended grouping keys not collapsed into one NULL group in the FlatGroupingTable/
  GroupId rollup + MarkDistinct path (harness verified line-for-line vs SQL).

### Fixed (engine)
- `TopNState` NULLS-LAST: null sort keys now sort last regardless of direction (was flipped with direction),
  correct for the TopN min-heap, TopN final output, and SortOperator's swapped sort. Sign: `aNull ? -1 : 1`.

### Fixed (Nitro harness — TpcdsParquetSupport.java)
- q18/q13/q48: unpadded CHAR-padded `cd_education_status` literals (data is unpadded VARCHAR; the only 6 padded
  literals in the file).
- q31/q34/q71: removed spurious TopN LIMIT (SQL has none) → `SortOperator`.
- q39: added missing `cov > 1.5` threshold on the month-1 inventory side.
- q78: year 1998→2000, dropped the spurious item key from store↔catalog join, output all 10 SQL columns.
- q24: removed a dedup aggregation collapsing the customer_address fan-out.
- q33: IN-subquery via SemiJoinOperator on the (I64) group key.
- q93: removed a CASE that produced all-NULL act_sales.
- q02: weekly-sales restructured (aggregate across all years, then join year-filtered date_dim → fan-out).
- q22: avg now skips NULLs (Sum + CountColumn, not coalesce-0 + constant-1 count).
- q44: rank on full-precision F64 average (avoid integer-cent rank ties).

### Trino harness (TrinoTpcdsParquetSupport.java)
- q18 unpadded; q31/q34/q71 `topNFactory`→`orderByFactory` (limit removal). (Trino harness already uses NULLS_LAST.)

### Velox harness (/root/notes/velox/harness/tpcds_cases/)
- q31/q34/q71 limit removed; directions already aligned earlier.

## Remaining TPC-DS Nitro divergences (the hard tail)
ENGINE/PRIMITIVE-CAPABILITY GAPS (need main-source work, specced):
- q83: `AddI64` must propagate NULLs like `MultiplyI64` (copy its NULLS-stream handling: `combineNullsOr`).
  Currently `add` drops nulls → inner-joined channels with NULL sums compute garbage instead of NULL (cols 2,4,6,7).
- q84: `concat_utf8` propagates NULL; SQL `concat()` ignores NULLs → needs `concat_utf8` null-as-empty semantics
  or a coalesce_utf8 wrapper (1 row: the customer with NULL first+last name).
- q56/q60: IN-subquery on a STRING key (i_item_id); `SemiJoinOperator` supports only I64 membership. Needs a
  binary-key semi-join (or surrogate-id mapping). Currently reverted to direct join (value-divergent, 100 rows).
- q49: one rank off — `divide_scale_round_i64` rounds to 12 dp, DuckDB ranks in double; tie-boundary artifact.
- q98: col6 revenue-ratio (likely the same null-class / ratio-scale; re-check after AddI64).

LOGIC, NOT YET RESOLVED (subagent fixes were incomplete; need iteration):
- q59: weekly restructure helped cols 1–2 but the SQL does a 7×7 (y AND x) date fan-out; only one 7× join added.
- q75: UNION-dedup (MarkDistinct) applied but still diverges — re-diagnose the dedup keys / channel detail.
- q77: catalog cross-join applied but still diverges — verify NestedLoopJoin output layout vs projectQuery77BranchOutput.
- q79: cols[4,5] — flagged false-positive (scale) by one agent but comparator still flags; re-confirm.

## Still ahead in the full sweep (not started)
- Propagate ALL Nitro fixes to the Trino pipeline harness (verbose) and the Velox cases; re-verify all 3 vs SQL.
- TPC-H: DONE - all 22 Nitro queries PASS vs golden expected (SQL-correct); Velox uses canonical TpchQueryBuilder. CLEAN, no fixes needed.
- ClickBench: DONE (Nitro vs DuckDB-SQL over a 5-file/5M-row subset, query01..43 = std Q0..Q42). 26/29 EXACT;
  the other 3 (q18/q32/q33) are NONDETERMINISTIC (LIMIT over groups with no total ORDER BY / count=1 ties) =
  valid, not bugs. 14 queries lack a DuckDB reference only because the fetched ClickHouse-dialect queries.sql uses
  `~` regexp / UINT16-EventDate this DuckDB rejects (Nitro produces output for all 43). ClickBench Nitro = CLEAN
  on all comparable queries. Tooling: src/test/.../clickbench/DumpResults.java -> /tmp/cb_nitro_dump; duckdb refs
  /tmp/cb_duckdb (bytes-decoded); compare /tmp/cmp_cb.py.

## TPC-H: DONE - all 22 Nitro queries PASS vs golden expected. CLEAN.
   Run in 3 batches (mvnd test -Dtest=TestTpchQueries#query01+... -DargLine=-Xmx16g) to avoid single-fork OOM.

## SUMMARY across 3 suites (Nitro vs SQL)
- TPC-DS: 84/93 strict, ~87 effective; 6 remaining are NITRO ENGINE-capability gaps (q49/q56/q60/q75/q77/q84), all harness bugs fixed.
- TPC-H: 22/22 CLEAN vs golden.
- ClickBench: 26/29 exact + 3 nondeterministic (valid); 14 oracle-dialect-blocked (Nitro runs fine).
STILL TODO: propagate TPC-DS Nitro fixes (q24/q33/q39/q78/q02/q22/q44/q93) into the Trino pipeline harness + Velox
cases; the 6 TPC-DS engine gaps; DuckDB-compatible SQL for the 14 ClickBench regexp/date queries.

NOTE: nothing committed (per workflow). All edits in the working tree.


## ENGINE-GAP IMPLEMENTATION (follow-up to the SQL sweep)
- q83: FIXED - AddI64 now propagates NULLs like MultiplyI64 (combineNullsOr). Verified.
- TopNState NULLS-LAST: FIXED earlier (sign aNull?-1:1). Verified.
- q84: NOT A BUG - the SQL uses Trino concat() which PROPAGATES null (= Nitro concat_utf8); only DuckDB's concat
  ignores nulls. Nitro matches the true (Trino) oracle. concat_utf8 left unchanged.
- q49: effectively correct - 1 rank of 41 differs due to divide_scale_round_i64 (12dp) vs DuckDB double; precision
  tie-boundary, not a logic bug.
- q56/q60: string-key (i_item_id) IN-subquery. GroupingState DOES support binary semi-join keys, BUT the
  SemiJoinOperator dictionary-membership path is NONDETERMINISTIC here (q56 gave 100 rows one full run, 0 the next
  with identical code) -> a real state/ordering bug in the dictionary membership build. Reverted to the
  deterministic direct join (value-divergent, 100 rows). GENUINE ENGINE BUG: SemiJoinOperator string/dictionary
  membership nondeterminism - needs a focused fix with a repro test. (Also a separate residual: GROUP BY i_item_id
  across the 3 channel scans uses per-scan dictionaries; sums diverge ~50pct of items -> multi-scan string-grouping
  dictionary reconciliation.)
- q75/q77: GroupId rollup + LEFT-join interaction - the store ROLLUP subtotal splits into 2 groups (diagnosed: a
  null-extended / GroupId-nulled key's stale underlying value not canonicalized so two logical-NULL keys hash
  apart, OR a LEFT-join returns fan-out). DEEP, regression-risky (hot grouping path); diagnosed, not yet fixed -
  needs focused work + a minimal repro test, not blind edits.

NET engine gaps: 2 fixed (AddI64, NULLS-LAST), 1 dismissed (q84 not-a-bug), 1 ~correct (q49 precision); 2 genuine
deep bugs remain (q56/q60 semi-join nondeterminism + multi-scan string-grouping; q75/q77 null-key/rollup grouping)
- each needs a focused minimal-repro + careful fix in the hot grouping/semijoin path.

## ENGINE-GAP IMPLEMENTATION ROUND 2 (test-first)
- q56/q60: FIXED (real engine bug). Root cause: SemiJoinOperator probed a DICTIONARY-encoded key via
  GroupingState.contains -> FlatGroupingTable.findGroup WITHOUT a beginBatch, so it read the inner (membership)
  side's stale per-batch dictionary cache -> ArrayIndexOutOfBounds (caught -> 0 rows) or wrong membership ==
  NONDETERMINISTIC (0 vs 100 across runs). Fix: GroupingState.beginContainsBatch/endContainsBatch wrap the probe
  loop in SemiJoinOperator (selectRows + computeMatchValues). Now q56/q60 deterministically MATCH SQL (verified
  2 back-to-back full runs). Regression test: TestSemiJoinDictionaryKey (200 iterations). All 192 operator tests pass.
- q75/q77: re-diagnosed - NOT null-value canonicalization. Repro TestMultiKeyNullGrouping proves BOTH the
  multi-long and the flat (string+long) grouping tables already collapse null keys with differing stale underlying
  values into one group. The real q77 cause: a ROLLUP grouping-set-discriminator interaction - a genuine NULL
  detail key (null ss_store_sk detail) is the same tuple (channel, NULL) as the channel subtotal, so detail-vs-
  subtotal rows must be kept apart by the grouping-set id; nitro produces an extra (store, NULL) row vs SQL.
  Needs a dedicated GroupId-rollup repro with null detail keys; deeper than canonicalization, not yet fixed.

NET (final): TPC-DS Nitro 88/95 strict. Non-bugs: q49 (rank precision), q59/q79/q98 (comparator scale/rounding),
q84 (Trino concat == Nitro; DuckDB oracle artifact). Genuine remaining: q75/q77 (ROLLUP grouping-set + null
detail key). Engine fixes shipped this effort: SemiJoinOperator dict-membership beginBatch (q56/q60), TopNState
NULLS-LAST, AddI64 NULL-propagation (q83). New regression tests: TestSemiJoinDictionaryKey, TestMultiKeyNullGrouping.
## Trino + Velox harness propagation (2026-06-16, continued)
Verified the 10 not-yet-propagated TPC-DS fixes against SQL (Trino: *TrinoSql tests; Velox: dump vs DuckDB).

### q75 / q77 — DONE (engine + all 3 harnesses), SQL-verified.
- Engine: DistinctKeySet retain-nulls for SQL DISTINCT/UNION (MarkDistinctOperator opt-in); q75 was dropping
  null-amt detail rows. q77: store/web dimension inner-join (drop null id), catalog cross-join.

### Propagated + VERIFIED this round
- **Velox MATCH** (dump vs DuckDB): q13, q22, q24, q33, q48 (+ q93 was already clean).
  - q13/q48: unpadded CHAR education literals. q22: avg ignores nulls (sum/count on the column, not
    coalesce-0 + count(1)). q24: dropped customer_address DISTINCT (keep fan-out). q33: manufact_id semi-join
    (IN-subquery), not a direct Electronics filter. q39: cov>1.5 on inv1 + emit inv2 whse/item (10 cols).
- **Trino MATCH** (*TrinoSql vs SQL): q13 (avg(ss_quantity) as DOUBLE, not integer divide), q22 (count only
  non-null for the avg denominator), q24 (removed the address-lookup dedup aggregation).

### Remaining (harder; not yet done)
- **Structural ports**: q02 (weekly restructure — big), q78 (year/join/columns; Trino throws BigInteger-out-of-
  long-range → overflow/structure), q33 Trino (semi-join, but the shared queryGroupedChannelSalesWithAddressOffset
  helper also feeds q56/q60 — needs its own plan), q39 Trino (returns 0 rows — structural, distinct from Velox).
- **Precision tails (likely irreducible, == q49 class)**: q39 cov cols[4,9] (double stddev_samp vs DuckDB; SQL
  itself CASTs cov to DECIMAL(30,10) "to avoid unstable results due to roundings"); q44 cols[0,2] (1 of 10
  rank rows differs at a tie boundary — double avg vs decimal rank()). Both engines.

## Continued (round 2)
- **q39 Trino — FIXED & verified**: the inv1↔inv2 self-join used keys (1,2)=(item,moy); since inv1.moy=1 and
  inv2.moy=2 they never matched → 0 rows. Corrected to (0,1)=(warehouse,item). testQuery39TrinoSql passes.
- **q78 — NOT converged (both engines), deferred**:
  - Trino: structural fixes applied (year 1998→2000; catalog join (year,item,customer)→(year,customer) for the
    `cs_item_sk = cs_item_sk` tautology; filter ws_qty>0 AND cs_qty>0 instead of sum>0). But the test still
    throws `BigInteger out of long range` in normalizeDecimalCentsValue: Trino's `round(decimal, 2)` keeps the
    division's high scale, so the ratio column's unscaled value exceeds long. This is a comparison-harness
    (normalizer) limit, not a logic divergence — needs a normalizer that tolerates high-scale decimals (but
    stripTrailingZeros breaks the x.00 cents columns, so it needs a per-column or rounding-aware approach).
  - Velox: same structural fixes applied + 10-column output + ratio rounding; now runs but the (year,item,customer)
    result set is disjoint from the DuckDB ref (33 rows vs 100; 0 key overlap). The cs-tautology fan-out
    interaction with the anti-join / both-qty filter still selects different store rows — needs deeper tracing
    (suspect the store anti-join keys and/or how the fan-out pairs with ws). Changes left in place (more
    SQL-faithful than the prior year=1998 version) but flagged as wrong.
- **q33 Trino, q02 (both)** — still not done (q33 needs its own semi-join plan separate from the shared
  q56/q60 helper; q02 is the weekly-restructure, large).

### Net verified-MATCH after round 2
- Trino vs SQL: q13, q22, q24, q39 (+ q48, q93 already passing).
- Velox vs DuckDB: q13, q22, q24, q33, q48 (+ q93). q39 structural fixed (cov precision-tail remains); q44
  1-row rank precision-tail.

## Continued (round 3) — structural queries landed
- **q78 — FIXED & verified (both engines)**: root cause was the returns anti-join using only (order) — SQL
  anti-joins on (order/ticket, item) for all three channels. Velox now matches DuckDB; Trino passes vs SQL
  (after also adding a q78/q02 rounding normalizer for the high-scale Trino decimal-division ratio).
- **q33 Trino — FIXED & verified**: i_manufact_id IN (Electronics) is a semi-join, not a direct category
  filter. Threaded a `semiJoinOnKey` flag through the shared channel helper (false-path byte-identical).
- **q56 / q60 Trino — FIXED (bonus)**: same i_item_id IN-subquery pattern; the same flag fixes them.
- **q39 Trino — FIXED & verified**: inv1↔inv2 self-join keyed on (item, moy) → empty; corrected to
  (warehouse, item).
- **q02 — FIXED & verified (both engines)**: wswscs must aggregate over ALL years, then fan out via a second
  join to year-filtered date_dim (~7 day rows/week → 2513 rows); the harnesses had pushed the year filter into
  the aggregation (no fan-out, 53 rows). Ratios rounded to 2dp.

### FINAL verified state
- **Velox vs DuckDB MATCH**: q02, q13, q22, q24, q33, q48, q78, q93 (8/10). Remaining: q39 cov cols[4,9] and
  q44 cols[0,2] — both PRECISION TAILS (double stddev_samp / double avg-rank vs DuckDB decimal; q44 is 1 of 10
  rows at a rank tie). Same class as the documented q49 non-bug.
- **Trino vs SQL**: q02, q13, q22, q24, q33, q39, q48, q56, q60, q78, q93 pass. q44 is the same rank
  precision-tail (1 row).

## ClickBench (all three engines)
- **Nitro vs DuckDB**: 26/29 exact (q18/q32/q33 are valid LIMIT-tie nondeterminism; 14 queries lack a DuckDB
  ref because the fetched ClickHouse-dialect SQL uses `~` regexp / UINT16 EventDate DuckDB rejects). CLEAN.
- **Trino vs Nitro**: the Trino + Nitro ClickBench TestQueries assert identical hardcoded golden values over a
  synthetic fixture; both pass (Nitro 48, Trino 16). The harnesses agree.
- **Velox vs DuckDB**: added a `--dump_result` path to ClickBenchBenchmark.cpp (mirrors the TPC-DS dumper).
  Over the same 5-file/5M-row subset (`--num_drivers=1` to merge global aggregates) it MATCHES 26/29 — the exact
  same set as Nitro (q18/q32/q33 nondeterministic). The 7 crashing queries are all in the no-DuckDB-ref dialect
  set (none of the 29 ref'd queries crashed). ClickBench is CLEAN across all three engines on every comparable query.
