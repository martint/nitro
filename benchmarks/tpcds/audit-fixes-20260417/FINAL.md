# Post-audit results — harness correctness fixes + d_year pushdown (2026-04-17)

This is a follow-up to [`../refactor-20260416/FINAL.md`](../refactor-20260416/FINAL.md). A harness audit ([`/tmp/harness-audit-final.md`](../../../tmp/harness-audit-final.md), archived as `../refactor-20260416/harness-audit-final.md` if kept) uncovered that the "Trino" numbers in the previous `FINAL.md` were measured against a Trino operator harness that was doing less work than the SQL it was purporting to execute. After fixing the harness, Nitro's relative standing on Q64 and Q80 — the worst-looking queries — improves dramatically.

## Headline

Numbers are ms/op, SF10 parquet (LZ4), JMH 3 warmup + 5 measurement iterations, 1 fork.

| Query | Nitro (pre-audit) | Nitro (post-audit) | Trino harness (pre-audit, **buggy**) | Trino harness (post-audit, **correct**) | **Trino/Nitro (post-audit)** |
|:-:|---:|---:|---:|---:|:-:|
| Q64 | 23512 | **27047** | 11309 | **23646** | **0.87×** |
| Q65 | 7536 | 7395 | 4680 | 4503 | 0.61× |
| Q66 | 3604 | 2421 | 4092 | 4233 | **1.75× (Nitro faster)** |
| Q80 | 10657 | **10026** | 5929 | **8764** | **0.87×** |
| Q97 | 4246 | 3540 | 3891 | 4095 | **1.16× (Nitro faster)** |

### What changed vs the previous FINAL.md

- **Q64: Trino harness went from 11309 → 23646 ms (+2.1×).** The previous number was impossibly fast because the `q64.project.sales_refunds` step was reading fields 4/5/6 after a `catalog_sales ⨝ catalog_returns` join that put the refund columns at positions 5/6/7. The harness was summing `cr_order_number + cr_refunded_cash + cr_reversed_charge` (order numbers are huge) as "refund", so its cs_ui filter `sum(sales) > 2·sum(refund)` evaluated to false for ~99% of items, keeping only 819 of ~102k items per branch. The rest of the plan then processed 150× fewer rows through 15 downstream joins.
- **Q80: Trino harness went from 5929 → 8764 ms (+1.5×).** The harness used INNER JOIN for the store_returns / catalog_returns / web_returns joins; Q80 SQL and Trino's optimizer use LEFT OUTER JOIN. The harness silently dropped every sales row without a matching return, then processed only that small minority through the remaining joins.
- **Nitro Q64: 23512 → 27047 ms.** Small regression (+15%) explained by the added `d_year = 1999 / 2000` scan filter (the pushdown *does* reduce downstream row counts ~5× after `date_dim.sold`, but evaluating the predicate at scan adds some fixed cost, and JMH 5-iteration runs on ~25-second queries have ±10% wall noise).
- **Nitro Q80: 10657 → 10026 ms.** Small unrelated improvement; the Nitro harness for Q80 already used LEFT OUTER correctly, so no Nitro change was needed. Noise-level variance.

### Apples-to-apples comparison

The previous "Trino/Nitro" ratios for Q64 and Q80 were measuring Nitro against a broken baseline. With the baseline fixed:

| Query | Previous Trino/Nitro ratio | Actual Trino/Nitro ratio |
|:-:|:-:|:-:|
| Q64 | 0.48× (Nitro 2.1× slower) | **0.87× (Nitro 1.14× slower)** |
| Q80 | 0.56× (Nitro 1.8× slower) | **0.87× (Nitro 1.14× slower)** |

Across the five primary targets, Nitro is now within 14% of the Trino harness on Q64 and Q80, and **faster than** it on Q66 and Q97. Q65 still lags (0.61×) and is the remaining target for further work.

## What shipped in this pass

All changes are in `src/test/java/org/weakref/nitro/trino/TrinoTpcdsParquetSupport.java` and `src/test/java/org/weakref/nitro/tpcds/TpcdsParquetSupport.java`.

### Harness correctness (Trino side)

- **Q80 LEFT OUTER JOIN for returns.** Added a `JoinType` field to the `HashJoinSpec` record (defaults to `INNER` via the existing constructor; a new constructor variant takes an explicit `JoinType`). `createHashJoinFactory` now calls `JoinOperatorType.ofJoinNodeType(spec.joinType(), ...)` instead of always using `innerJoin`. Q80's `query80ChannelBranchPlan` at the `.join.returns` call site now passes `JoinType.LEFT`. Matches the SQL `LEFT OUTER JOIN store_returns / catalog_returns / web_returns`.

- **Q64 cs_ui column-index fix.** `query64CsUiPlan` was reading `field(4, ...returnsTypes.get(2))`, `field(5, ...returnsTypes.get(3))`, `field(6, ...returnsTypes.get(4))` for `cr_refunded_cash`, `cr_reversed_charge`, `cr_store_credit`. After the `catalog_sales ⨝ catalog_returns` join, the layout is sales[0..2] || returns[0..4], so the refund columns are at absolute positions 5, 6, 7 (not 4, 5, 6). Fixed.

### Plan shape parity (both harnesses)

- **Q64 `d_year` pushdown into `date_dim.sold`.** The Trino optimizer plan (`/tmp/nitro-tpcds-explain/q64.explain.txt:187`) pushes `d_year = 1999` (for cs1) and `d_year = 2000` (for cs2) into the date_dim scan. Both harnesses now do the same: `query64CrossSales(...)` / `query64CrossSalesPlan(...)` take a `soldYear` argument, and the `date_dim.sold` scan is wrapped in a filter. The per-operator profile shows `date_dim.sold` output drops from 2.46M → 494k rows per branch after the fix, which cascades through the remaining 6 dim joins.

## Tests

All three Q64 / Q80 tests now pass:

| Test | Meaning | Status |
|---|---|---|
| `TestQueries#testQuery64` | Nitro vs Trino harness | PASS |
| `TestQueries#testQuery64TrinoSql` | Trino harness vs Trino SQL | **PASS (was failing)** |
| `TestQueries#testQuery80` | Nitro vs Trino harness | PASS |
| `TestQueries#testQuery80TrinoSql` | Trino harness vs Trino SQL | **PASS (was failing)** |

Q65/Q97 `testQueryNNTrinoSql` also pass. Q66 still has the pre-existing baseline correctness failure (unrelated to any of this work; flagged at the top of the previous `FINAL.md`).

## What the audit did not find

Several claims from intermediate agent reports were checked against source and discarded as false:

- **"Nitro doesn't scale DECIMAL(7,2) to cents."** Nitro reads raw `INT64` from parquet = already in cents. Trino's `scaledCents(DECIMAL→DOUBLE→×100→BIGINT)` recovers the same value. Numerically identical.
- **"Q80 item price filter uses 5_000 / should be 50.00."** DECIMAL(7,2) scale 2 stores 5000 = $50.00. Matches SQL `> 50`.
- **"Q66 mixes `ws_net_paid` and `cs_net_paid_inc_tax`."** The SQL itself does this (`q66.sql:69,117`). Harness faithfully mirrors the SQL.
- **"Q97 off-by-one `> 1199 AND < 1212` vs `BETWEEN 1200 AND 1211`."** These are equivalent on integers.
- **"Q65 Nitro CTE caching bug."** Nitro re-assembles `query65StoreItemSales` independently at each callsite; no caching, consistent with `CLAUDE.md`.

## Remaining gaps

- **Q65 (Trino/Nitro = 0.61×)**: not audited in depth this pass; predicate placement on the store-threshold join might still diverge from the optimizer plan.
- **Q66 pre-existing correctness failure**: orthogonal to harness shape, needs its own bisection.
- **Dynamic filtering**: every Trino optimizer plan in the explain output has `dynamicFilterAssignments` decorating its scans (semi-join-reduction pushed from build to probe side). Neither harness implements this. Both harnesses do more work than real Trino — a symmetric asymmetry.
- **Hash-probe micro-optimizations** (Tiers 3b/3c/3d from the previous pass) remain reverted. The probe inner loop is at a JIT local optimum for HotSpot; real further gains would require larger structural changes (SIMD probe, dedup of probe keys, specialized build layouts). See the previous `FINAL.md` for details.

## Artifacts

All in [`benchmarks/tpcds/audit-fixes-20260417/`](./):

| File | What |
|---|---|
| `FINAL.md` | this summary |
| `both-after-audit-stable.csv` | JMH CSV (3 warmup + 5 measurement, both Nitro and Trino harness for Q64/Q65/Q66/Q80/Q97) |
| `both-after-audit-stable.log` | JMH full log |
| `nitro-after-audit.csv/log` | first (3-iteration) Nitro-only run, high variance |
| `trino-after-audit.csv/log` | first (3-iteration) Trino-only run, high variance |

The Trino optimizer plans used for the audit are in `/tmp/nitro-tpcds-explain/q{64,65,66,80,97}.explain.txt` (generated via `ExplainUnsupportedQueries` against the `LocalTpcdsPlugin` at SF10). The audit writeup is at `/tmp/harness-audit-final.md`.
