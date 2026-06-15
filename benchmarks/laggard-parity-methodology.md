# Diagnosing a Velox-vs-Nitro laggard — methodology (2026-06-19)

A repeatable procedure for any query where Nitro lags Velox. Derived from the q59 deep dive, where the
"broad constant factor" hypothesis was **wrong**: the real cause was a harness that pushed 5× more rows
through projection + aggregation than Velox did. **Always run step 1 before profiling.**

## Step 1 — Verify the two engines do the SAME work at each stage (do this FIRST)

Most of the apparent gap can be an apples-to-apples defect: the two harnesses are not the same logical
operator sequence, so one processes more rows/positions than the other. Profiling a query that is simply
doing more work just tells you *where* the extra work lands, not *that it is extra*. Catch it up front by
comparing **per-stage row counts** and **per-column positions decoded**.

**Velox side — per-operator and per-scan rows** (already printed by the harness):
```
cd /root/notes/velox/velox/_build/release/velox/benchmarks/tpcds
LD_LIBRARY_PATH=/root/notes/velox/boostlibs:/usr/local/lib ./velox_nitro_tpcds_benchmark \
  --data_path=/root/data/tpcds-parquet-sf10/sf10 --run_query_verbose=<N> \
  --num_drivers=1 --num_io_threads=1 --num_splits_per_file=1
```
Read `OperatorRows (by type)` → `Aggregation: input=… output=…`, `FilterProject: input=…`, `HashProbe: input=…`,
and `TableScans (per node): rawInput -> output` (Velox's scan `output` is **post-dynamic-filter**, i.e. the rows
that actually decode their payload columns).

**Nitro side — per-column decoded positions** (the decisive measurement). `TrinoParquetScanOperator.resolveColumn`
reads `readFullColumn` (all positions) vs `readMaskedColumn` (mask only); instrument both with a per-column
counter keyed by `column.name()`. Run the single query (reflect `query<NN>` and drain it) and print the tally.
For the skip-decode path, use `SkipDecodeScanOperator.Profile` (`rowsScanned`, `survivorsEmitted`,
`recordFilter` pass-rate). See the q59 case study below for the throwaway probe (`Q59DecodeProbe.java`).

**Compare:**
- Scan `rawInput` should match (both decode the fact key for every fact row).
- **Projection / aggregation input must match.** If Nitro's `survivorsEmitted` / join-output ≫ Velox's
  `Aggregation input`, the harnesses filter at different plan positions — STOP and fix the harness (step 2).
- Aggregation `output` (group count) should match if results are correct.

## Step 2 — If row counts differ, confirm same RESULT, then align the plan shape

Before "fixing", prove both engines compute the same answer (the extra work might be load-bearing). Dump both
and diff in the shared canonical space:
```
# Velox: D\t-prefixed, \x01-separated, money DOUBLE rendered as round(v*100) cents
./velox_nitro_tpcds_benchmark … --run_query_verbose=<N> --dump_result | grep '^D' | sed 's/^D\t//' | sort > /tmp/velox.txt
# Nitro: DumpResults writes \x01-separated rows to /tmp/nitro_dump/q<NN>.txt
sort /tmp/dump_base/q<NN>.txt > /tmp/nitro.txt
diff /tmp/velox.txt /tmp/nitro.txt
```
Watch for pure **rendering** differences (Velox keeps cents, Nitro may round a ratio to an integer: `8297`↔`83`).
If the values agree to rounding, the **sums are identical** ⇒ the engine that processes fewer rows is using a
valid filter-pushdown, and Nitro's harness is doing redundant work.

Then realign Nitro's harness to Velox's plan shape (e.g. push a dimension filter before the fact join instead
of after the aggregation), and **re-verify byte-identical** against the Nitro baseline (`/tmp/dump_base`).

## Step 3 — Only now profile the residual constant factor

With row counts equal, the remaining gap is genuine per-row cost. Map perfasm/JFR hot methods to operators and
compare implementation + assembly Velox-vs-Nitro (see `perf-tooling-recipe`, the q24/q59 assembly notes). Don't
attribute a gap to "constant factor" until step 1 says the work is equal.

## Case study: q59 (3215ms → 1405ms, parity with Velox 1410ms; byte-identical)

- **Step 1.** Per-column tally: every `store_sales` column decoded **57.6M full**; `survivorsEmitted=55,009,628`.
  Velox `Aggregation input=10,846,794`. Nitro pushed **5.07×** more rows through projection + aggregation. The DF
  (with `-Dnitro.dynamicFilter -Dnitro.skipScan`) passed **95.5%** — not selective — because Nitro joined the
  *unfiltered* date_dim.
- **Step 2.** Output diff: rows matched exactly to rounding ⇒ identical sums ⇒ Velox's pre-filter is valid.
- **Root cause.** Velox filters `date_dim` by `d_month_seq` (a ~12-month window) **before** the `store_sales`
  join. Nitro's harness joined **all** of date_dim, aggregated every week across all time, and applied the month
  filter **after** aggregation (a second join on `d_week_seq`) — so out-of-window weeks were aggregated then
  discarded.
- **Fix.** Push the month filter into q59's first date_dim join: scan `{d_date_sk, d_week_seq, d_day_name,
  d_month_seq}`, filter `and(greaterThan(3, lo-1), lessThan(3, hi+1))`, project `0,1,2` (drop `d_month_seq` so the
  join layout is unchanged). Byte-identical; parity reached.
- **Non-levers proven along the way.** Lazy payload decode (`skipScan`) moved q59 only **−1.3%** — payload decode
  is cheap; the cost was compute row-volume. A flat `SumStateVector` (vs the chunked 2D one) is a real but small
  **−5.3%** general win.

## Gotcha: JMH forks a fresh JVM — pass engine flags via `-jvmArgsAppend`

A bare `-Dnitro.foo=true` on the `mvn`/`mvnd` command line does **not** reach the forked benchmark JVM. Flags
that must affect the benchmark go inside `benchmark.options`:
```
-Dbenchmark.options="… -jvmArgsAppend '-Dnitro.dynamicFilter=true -Dnitro.skipScan=true' …"
```
A prior "dynamic filtering is dead, df-on ≈ df-off" conclusion was a false negative from exactly this: the flags
were dropped, so both runs measured df-off. With them forwarded correctly: q21 6589→1028ms (6.4×), q88 2785→1282ms
(2.2×), byte-identical. **When a flag "does nothing", first confirm it reached the fork.**

## Takeaways

1. **Per-stage row-count parity is a precondition for any perf comparison.** Run step 1 first, every time.
2. Velox transparently pushes build-side / dimension filters before fact joins (dynamic filtering). Several Nitro
   TPC-DS harnesses filter post-aggregation and so aggregate more rows than Velox. **Audit the other laggards for
   the same "Nitro project/agg input ≫ Velox Aggregation input" defect** before chasing engine constant factors.
3. Scan/decode volume is often **not** the bottleneck (q59 skipScan −1.3%); compute row-volume usually is.
4. Throwaway instrumentation (`TrinoParquetScanOperator.DECODE_COUNTS`, `SkipDecodeScanOperator.Profile`,
   a single-query decode probe) is the right tool — revert it before committing.
