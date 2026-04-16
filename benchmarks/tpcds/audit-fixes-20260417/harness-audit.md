# Harness audit — Nitro vs Trino harness vs Trino optimizer plan

## Method

1. Captured Trino optimizer's logical plan via `ExplainUnsupportedQueries` for Q64/Q65/Q66/Q80/Q97 → `/tmp/nitro-tpcds-explain/q*.explain.txt`.
2. Spawned 5 sub-agents to compare each harness against the plan + SQL.
3. Verified each sub-agent's claim by direct file read before including it below. Unverified/wrong claims discarded.

Legend: **Plan-shape** = optimizer-equivalent correctness; **Perf-asymmetry** = harness does unequal amount of work vs its counterpart (can't be apples-to-apples); **Correctness** = wrong answer.

Rule from CLAUDE.md: "both sides must be assembled from the same logical sequence of operators and stage boundaries … do not compare a Nitro operator tree against a Trino SQL query and treat that as an operator-level comparison." Every divergence below either violates this rule or silently invalidates a comparison.

---

## Q64

### Verified divergences

| # | Where | Nitro | Trino harness | Trino planner | Severity |
|---|---|---|---|---|---|
| 1 | `d_year` filter pushdown on the three `date_dim` joins | **Not pushed** — applied post-aggregation via `query64YearTransitionPredicate` at `TpcdsParquetSupport.java:425` | **Not pushed** — same (`TrinoTpcdsParquetSupport.java:10988-11012`, `Optional.empty()` for `soldDates`, `salesDates`, `shipDates`) | Pushes `d_year = 1999` (cs1) / `2000` (cs2) into `date_dim` scan (`q64.explain.txt:187`, 470) | Perf-asymmetry from optimizer |
| 2 | `cs_ui` build-side cardinality | Filter keeps **100,142 items / branch** | Filter keeps **819 items / branch** | Unknown without running SQL, but `testQuery64TrinoSql` FAILS — harness ≠ SQL | Correctness (at least one side) |
| 3 | `cd_marital_status <> cd_marital_status` inequality | Separate filter after all 17 joins (`TpcdsParquetSupport.java:9361`) | Embedded as join filter on `cd2` join (`q64.explain.txt:85`) | Embedded as join filter on `cd2` join | Plan-shape (minor) |
| 4 | Dynamic filtering (DF) — semi-join reduction from build side to probe side | Not implemented | Not implemented | Used pervasively (`dynamicFilterAssignments = {...}` on every ScanFilter in plan) | Perf-asymmetry from optimizer (both harnesses lack it) |

### Not a divergence (audit claims verified false)

- **"Nitro doesn't scale DECIMAL(7,2) to cents."** Nitro reads parquet DECIMAL(7,2) raw INT64 (`TrinoParquetScanOperator.java:458` → `copyI64`) which already is the cent value. Trino's `scaledCents` casts DECIMAL→DOUBLE→×100→BIGINT which recovers the same cent value. Mathematically identical. The 100k-vs-819 cardinality gap is *not* a scaling bug.
- **Join order.** Nitro and Trino harness both mirror the Trino optimizer's join order exactly (store_returns → cs_ui → store → customer → cd1 → cd2 → hd1 → hd2 → ca1 → ca2 → d1 → d2 → d3 → promotion → ib1 → ib2 → item). No reordering happened in the optimizer — the SQL-text order is already the plan order.

### Actual root cause of `testQuery64TrinoSql` failure

The assertion diff I captured earlier shows the Trino harness returns 0 rows where the Trino SQL returns 32. Two possibilities that fit:

- The post-aggregation year filter in the Trino harness (`field(12) = 1999 AND field(31) = 2000`, `TrinoTpcdsParquetSupport.java:10832-10838`) reads the wrong column indices after the join chain, so the year check always fails.
- `scaledCents` combined with DECIMAL summation in Trino harness produces different aggregated sums than the SQL planner emits, which runs the SQL's HAVING against the original decimal values without the `×100` scaling.

Needs a targeted diagnostic to pick between them (e.g., dump `(item_sk, sum_sales_cents, sum_refund_cents, passed)` from the Trino-harness cs_ui filter and compare with Trino-SQL intermediate).

### Still unexplained

Why do Nitro's cs_ui filter (keeps 100k) and Trino-harness's cs_ui filter (keeps 819) disagree on the *same* predicate `sum(sales) > 2 * sum(refund)` over structurally equivalent aggregations? Something in `scaledCents` or DECIMAL addition in the Trino harness is not producing what it appears to. The fact that `testQuery64TrinoSql` fails says the Trino harness side is wrong — Nitro's 100k is likely closer to correct SQL semantics.

---

## Q65

### Verified

Nitro calls `query65StoreItemSales` twice: once at `TpcdsParquetSupport.java:1938` (the outer aggregation) and once at line `9514` inside `query65StoreThresholds` (the average-store aggregation). These are two independently assembled operator pipelines, not a shared/cached reference. **This is correct per CLAUDE.md** — the audit claimed a "CTE duplication bug" but the Nitro harness is actually apples-to-apples here.

### Remaining potential divergences (not independently verified)

- Placement of the `revenue <= 0.1 * avg` filter (Nitro: separate Filter op after the threshold join; Trino plan: possibly baked into the join node). Plan-shape, not correctness.

---

## Q66

### Verified

Audit claimed the Nitro UNION ALL across web and catalog branches mixes `ws_net_paid` (pre-tax) with `cs_net_paid_inc_tax` (tax-inclusive) and that this causes the pre-existing correctness failure.

**Both the SQL and both harnesses do this.** Q66 SQL itself uses `ws_net_paid` for the web branch (`q66.sql:69`) and `cs_net_paid_inc_tax` for the catalog branch (`q66.sql:117`). That's a quirk of the TPC-DS query spec, not a harness bug. Nitro mirrors it faithfully (`TpcdsParquetSupport.java:1972` / `1984`). Audit finding discarded.

### Remaining potential divergences (not independently verified)

- Trino plan apparently has three-level aggregation (PARTIAL → INTERMEDIATE → FINAL) with nested `d_moy` grouping; the harnesses do a single GROUP BY. Pure optimizer expansion, not a semantic divergence.
- The pre-existing Q66 correctness failure baseline predates this work and was not introduced by the refactor. Needs its own bisection investigation; unlikely to be harness-level.

---

## Q80

### Verified — this is the big finding

**The Trino harness is missing the LEFT OUTER JOIN on the returns table.** Q80 SQL uses `LEFT OUTER JOIN store_returns / catalog_returns / web_returns`; Trino's optimizer plan confirms `LeftJoin` (`q80.explain.txt`).

- Nitro Q80 at `TpcdsParquetSupport.java:4374-4380` correctly passes `probeOuterJoin = true` to `HashJoinOperator`.
- Trino harness Q80 at `TrinoTpcdsParquetSupport.java:7278` uses `namedHashJoinStep(..., new HashJoinSpec(...))` which constructs an **inner join** (HashJoinSpec has no join-type field, so the default is INNER).

Consequence: the Trino harness's Q80 drops every sales row that has no matching return. The downstream `ifExpression(isNull(field(9, ...)), 0, scaledCents(...))` at `TrinoTpcdsParquetSupport.java:7289-7290` — which was clearly written expecting nullable right-side columns — becomes dead code. This is the plausible reason Trino harness Q80 benchmarks look fast: it's processing only the small minority of sales with returns. **This directly invalidates Q80 as an apples-to-apples comparison.**

Fix: `HashJoinSpec` needs a join-type field (or a separate `leftOuter` variant), and call site at 7278 needs to use it.

### Not a divergence (audit claim verified false)

- **"Item filter is `> 5_000` which should be `> 50.00`."** Both harnesses pass `constant(5_000L, itemTypes.get(1))` where `itemTypes.get(1)` is DECIMAL(7,2). DECIMAL(7,2) scale 2 stores values as unscaled × 100, so 5000 = $50.00. Matches SQL's `> 50`. Audit finding discarded.

### Remaining

- No `d_year` / `d_date` pushdown verification done for Q80 (the audit observed it but I didn't separately verify — SQL uses a 30-day date range filter which the planner should push into the date_dim scan).

---

## Q97

### Verified — no real divergences

Audit claimed `d_month_seq > 1199 AND < 1212` is off-by-one vs `BETWEEN 1200 AND 1211`. These are **equivalent** — `> 1199` means `≥ 1200` on integers, `< 1212` means `≤ 1211`. Audit finding discarded.

The `FullJoin` in Trino's plan is emulated by both harnesses via `UNION ALL` of two branches with presence flags. Emulation is logically correct and cheaper; this is why Q97 landed within 8% of Trino. No action needed.

---

## Consolidated verdict — per harness

### Nitro harness

Mostly apples-to-apples vs Trino's plan *shape*. Two real asymmetries:
- Q64 `d_year` filter not pushed into `date_dim` scan (3 joins affected, both branches).
- Q64 `cd1/cd2.cd_marital_status <>` is a late filter instead of a join predicate.

Neither is a correctness bug; both are pure plan-shape. Fixing them should shrink Nitro Q64 wall-clock by a meaningful fraction (the post-join `d_year` filter vs a scan-pushed `d_year = 1999` means Nitro processes ~100× more date rows through 3 date joins × 2 branches).

No confirmed correctness bugs on the Nitro side for any of the five queries.

### Trino harness

- **Q64**: `testQuery64TrinoSql` FAILS. The Trino harness's cs_ui filter keeps 819 rows while SQL expects something else (needs diagnostic). Result: Trino harness returns 0 rows vs SQL's 32 rows on the test assertion. This is a real correctness bug in the harness.
- **Q80**: Returns join is INNER, should be LEFT OUTER. Result: Trino harness benchmarks for Q80 are running a fundamentally different (smaller) plan than the SQL, and the Nitro-vs-Trino-harness comparison is invalid.
- **Q64 / Q80**: same missing `d_year` pushdown as Nitro.

### Impact on existing benchmark numbers

The `FINAL.md` "Trino/Nitro" ratios for Q64 and Q80 compare against the Trino *harness*, not Trino SQL. Given the Q80 INNER-vs-OUTER bug and the Q64 `testQuery64TrinoSql` failure, **the "Trino: 11309ms" and "Trino: 5929ms" numbers for Q64/Q80 are not comparable baselines**. The real apples-to-apples Trino-harness timing would be *slower* (because the harness would process many more rows once the outer join is fixed and the filter arithmetic is corrected). The gap to Trino on these queries is almost certainly smaller than currently reported.

Q65/Q66/Q97 harness comparisons appear clean.

---

## Recommended fixes, in priority order

1. **Fix Trino harness Q80 returns join to LEFT OUTER.** `HashJoinSpec` needs a join-type parameter; the call at `TrinoTpcdsParquetSupport.java:7278` must use it. This probably makes Q80's current "Trino harness 5929ms" number rise substantially.

2. **Investigate why `testQuery64TrinoSql` fails.** Likely in the Trino-harness cs_ui path. Add a one-shot diagnostic test that dumps `(item_sk, sum_sales_cents, sum_refund_cents, passed)` from both harnesses plus the Trino SQL intermediate for a small sample, and find the first divergence. This will either fix the Trino harness or reveal a Nitro correctness bug we currently miss.

3. **Push `d_year` filter into `date_dim` scans in both Q64 harnesses** (and Nitro Q80's date_dim scan). Symmetric fix — keeps the comparison apples-to-apples.

4. **Re-run Q64/Q80 benchmarks after (1) and (3)** to get the corrected "Trino harness" baseline and the corrected Nitro number. The published "Trino/Nitro" ratios for these queries will change, and so will the narrative about whether Nitro is slower or faster than Trino on Q64/Q80.

5. Low priority / deferred: place `cd_marital_status <>` as a join predicate instead of a late filter (Q64); investigate pre-existing Q66 correctness failure (likely orthogonal).
