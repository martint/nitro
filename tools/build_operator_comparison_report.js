#!/usr/bin/env bun
import {existsSync, readFileSync, readdirSync, writeFileSync} from "node:fs";

const root = new URL("../", import.meta.url).pathname;
const sweep = `${root}benchmarks/sweeps/20260715-full-counters`;
const targeted = `${root}benchmarks/sweeps/20260716-targeted`;
const targeted17 = `${root}benchmarks/sweeps/20260717-targeted`;
const targeted18 = `${root}benchmarks/sweeps/20260718-targeted`;
const targeted21 = `${root}benchmarks/sweeps/20260721-targeted`;
const targeted22 = `${root}benchmarks/sweeps/20260722-targeted`;
const closeBatchSweep = `${root}benchmarks/sweeps/20260717-close-batches`;
const events = [
  "instructions",
  "cycles",
  "L1-dcache-load-misses",
  "L1-dcache-loads",
  "dTLB-load-misses",
  "dTLB-loads",
  "branch-misses",
  "branches",
];

const queryId = benchmark => benchmark.match(/query(\d+)$/)?.[1];
const readJvm = (path, benchmarkPrefix = null) => {
  const rows = new Map();
  for (const result of JSON.parse(readFileSync(path, "utf8"))) {
    if (benchmarkPrefix != null && !result.benchmark.startsWith(benchmarkPrefix)) continue;
    const query = queryId(result.benchmark);
    if (!query) continue;
    rows.set(query, {
      duration: result.primaryMetric.score,
      counters: Object.fromEntries(events.map(event => [event, result.secondaryMetrics[event]?.score])),
      allocationBytes: result.secondaryMetrics["gc.alloc.rate.norm"]?.score,
      allocationScope: result.secondaryMetrics["gc.alloc.rate.norm"] == null ? null : "JVM heap",
      source: path.slice(root.length),
    });
  }
  return rows;
};

const overlayJvmAllocation = (rows, path, selected = null) => {
  if (!existsSync(path)) return;
  for (const [query, allocationRow] of readJvm(path)) {
    if (selected != null && !selected.has(query)) continue;
    const row = rows.get(query);
    if (row && allocationRow.allocationBytes != null) {
      row.allocationBytes = allocationRow.allocationBytes;
      row.allocationScope = allocationRow.allocationScope;
      row.allocationSource = allocationRow.source;
    }
  }
};

const overlayJvm = (rows, path, selected = null, benchmarkPrefix = null) => {
  if (!existsSync(path)) return;
  for (const [query, row] of readJvm(path, benchmarkPrefix)) {
    if (selected == null || selected.has(query)) rows.set(query, row);
  }
};

const readVeloxCapture = (query, logPath, perfPath, source) => {
  const log = readFileSync(logPath, "utf8");
  const duration = log.match(/^Execution time:\s*([0-9.]+)(ms|s)$/m);
  const allocationBytes = log.match(/^Managed allocation bytes:\s*(\d+)$/m);
  const allocationCount = log.match(/^Managed allocation count:\s*(\d+)$/m);
  const peakBytes = log.match(/^Managed peak bytes:\s*(\d+)$/m);
  if (!duration || !existsSync(perfPath)) return null;
  const counters = {};
  for (const line of readFileSync(perfPath, "utf8").split(/\r?\n/)) {
    const fields = line.split(",");
    if (events.includes(fields[2]) && Number.isFinite(Number(fields[0]))) counters[fields[2]] = Number(fields[0]) / 10;
  }
  return {
    duration: Number(duration[1]) * (duration[2] === "s" ? 1000 : 1),
    counters,
    allocationBytes: allocationBytes == null ? undefined : Number(allocationBytes[1]),
    allocationCount: allocationCount == null ? undefined : Number(allocationCount[1]),
    peakBytes: peakBytes == null ? undefined : Number(peakBytes[1]),
    allocationScope: allocationBytes == null ? null : "Velox managed pool",
    source,
  };
};

const readVeloxDirectory = (suite, directory, sourceDirectory) => {
  const rows = new Map();
  if (!existsSync(directory)) return rows;
  for (const name of readdirSync(directory).filter(name => /^q\d+\.log$/.test(name))) {
    const query = name.match(/^q(\d+)\.log$/)[1];
    const row = readVeloxCapture(
      query,
      `${directory}/${name}`,
      `${directory}/q${query}.perf.csv`,
      `${sourceDirectory}/q${query}.{log,perf.csv}`,
    );
    if (row != null) rows.set(query, row);
  }
  return rows;
};

const readVelox = suite => readVeloxDirectory(
  suite,
  `${sweep}/velox-${suite}`,
  `benchmarks/sweeps/20260715-full-counters/velox-${suite}`,
);

const overlayVelox = (rows, suite, directory, sourceDirectory, selected = null) => {
  for (const [query, row] of readVeloxDirectory(suite, directory, sourceDirectory)) {
    if (selected == null || selected.has(query)) rows.set(query, row);
  }
};

const overlayVeloxCapture = (rows, query, logPath, perfPath, source) => {
  if (!existsSync(logPath) || !existsSync(perfPath)) return;
  const row = readVeloxCapture(query, logPath, perfPath, source);
  if (row != null) rows.set(query, row);
};

const suites = [];
for (const [display, suite, first, last] of [["TPC-H", "tpch", 1, 22], ["TPC-DS", "tpcds", 1, 99], ["ClickBench", "clickbench", 0, 43]]) {
  const nitro = readJvm(`${sweep}/nitro-${suite}.json`);
  const trino = readJvm(`${sweep}/trino-${suite}.json`);
  const velox = readVelox(suite);
  if (suite === "tpch") {
    overlayJvm(nitro, `${sweep}/nitro-tpch-q08-velox-join-shape.json`, new Set(["08"]));
    overlayJvm(trino, `${sweep}/trino-tpch-q08-velox-join-shape.json`, new Set(["08"]));
    overlayJvm(nitro, `${sweep}/nitro-tpch-q13-bmh-confirm-on.json`);
    overlayJvm(nitro, `${sweep}/nitro-tpch-q16-grouped-distinct.json`, new Set(["16"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q14-velox-shape.json`, new Set(["14"]));
    overlayJvm(trino, `${targeted}/trino-tpch-q14-velox-shape.json`, new Set(["14"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q15-narrow-final-join.json`, new Set(["15"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q18-one-scan-shape.json`, new Set(["18"]));
    overlayJvm(trino, `${targeted}/trino-tpch-q18-one-scan-shape.json`, new Set(["18"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q21-baseline.json`, new Set(["21"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q21-dense-minmax-run-reduction-3fork.json`, new Set(["21"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q09-explicit-join-boundaries.json`, new Set(["09"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q09-batch-pair-canonical.json`, new Set(["09"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q14-static-range-pushdown.json`, new Set(["14"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q15-static-range-pushdown.json`, new Set(["15"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q04-q10-static-range-pushdown.json`, new Set(["04"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q06-static-range-pushdown.json`, new Set(["06"]));
    overlayJvm(nitro, `${targeted}/nitro-tpch-q07-direct-domain-64m-canonical.json`, new Set(["07"]));
    overlayVelox(
      velox,
      suite,
      `${targeted}/velox-tpch-q09`,
      "benchmarks/sweeps/20260716-targeted/velox-tpch-q09",
      new Set(["09"]),
    );
    overlayJvm(nitro, `${targeted}/nitro-tpch-q13-direct-null-canonical.json`, new Set(["13"]));
    overlayJvmAllocation(nitro, `${targeted}/nitro-tpch-q13-direct-null-gc.json`, new Set(["13"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q16-close-consumed-batches-final-3fork.json`, new Set(["16"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q09-domain-aware-final-3fork-retry.json`, new Set(["09"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q16-page-header-fast-on-canonical-3fork.json`, new Set(["16"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q09-fast-page-header-final-3fork.json`, new Set(["09"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q09-direct-compacted-range-final-on-canonical-3fork.json`, new Set(["09"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q09-pair-tag-mask-guard-final-3fork.json`, new Set(["09"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q19-build-zero-prune-batch-close-final-3fork.json`, new Set(["19"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q22-membership-vector-branchless-final-3fork.json`, new Set(["22"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpch-q09-wide-deferred-payload-final-3fork.json`, new Set(["09"]));
    // A mask-sensitive computed output must be re-resolved after its retained batch is constrained. The old q17
    // row predates that lifecycle repair and is not a valid current-source measurement.
    overlayJvm(nitro, `${targeted18}/nitro-tpch-q17-sensitive-output-final-3fork.json`, new Set(["17"]));
    // Publish the complete versioned-predicate activation cohort from one final-source capture. Older focused
    // thermal sequences are intentionally superseded, including rows with lower absolute duration.
    overlayJvm(nitro, `${targeted18}/nitro-tpch-versioned-predicate-final-3fork.json`, new Set([
      "05", "08", "11", "21",
    ]));
    overlayJvm(nitro, `${targeted21}/tpch-q09-current-default-publication-3fork.json`, new Set(["09"]));
    overlayJvm(
      nitro,
      `${targeted22}/tpch-q12-residual/tpch-q12-constrained-steady-candidate-3fork-b.json`,
      new Set(["12"]),
    );
    const sparseDynamicFilterLongWarmup = `${targeted22}/tpcds-q26-build-key-audit/sparse-publication-q27-q08-wi15-joint-3fork.json`;
    overlayJvm(nitro, sparseDynamicFilterLongWarmup, new Set(["08"]), "org.weakref.nitro.");
    overlayJvm(trino, sparseDynamicFilterLongWarmup, new Set(["08"]), "org.weakref.trino.");
    const q20CurrentPublication = `${targeted22}/tpch-q20-current/q20-publication-joint-3fork.json`;
    overlayJvm(nitro, q20CurrentPublication, new Set(["20"]), "org.weakref.nitro.");
    overlayJvm(trino, q20CurrentPublication, new Set(["20"]), "org.weakref.trino.");
  }
  if (suite === "tpcds") {
    overlayJvm(trino, `${targeted}/trino-tpcds-q04-compact-join-layouts.json`, new Set(["04"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q88-compact-join-layouts.json`, new Set(["88"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q87-compact-join-layouts-confirm.json`, new Set(["87"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q11-compact-join-layouts.json`, new Set(["11"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q13-compact-join-layouts.json`, new Set(["13"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q25-compact-join-layouts.json`, new Set(["25"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q48-compact-join-layouts.json`, new Set(["48"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q49-compact-join-layouts.json`, new Set(["49"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q78-compact-join-layouts.json`, new Set(["78"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q79-compact-join-layouts.json`, new Set(["79"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q85-compact-join-layouts.json`, new Set(["85"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q76-compact-join-layouts.json`, new Set(["76"]));
    overlayJvm(nitro, `${sweep}/nitro-tpcds-q82-q93-range-order.json`, new Set(["93"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q04-compact-join-layouts.json`, new Set(["04"]));
    overlayJvm(nitro, `${sweep}/nitro-tpcds-q82-range-order-confirm.json`, new Set(["82"]));
    overlayJvm(nitro, `${sweep}/nitro-tpcds-q47-q57-q78-nested-dictionary.json`);
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q78-compact-join-layouts.json`, new Set(["78"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q79-compact-join-layouts.json`, new Set(["79"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q02-compact-join-layouts.json`, new Set(["02"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q02-fused-conditional-sums-final-3fork.json`, new Set(["02"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q21-adaptive-shared-dictionary-3fork.json`, new Set(["21"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q21-compact-joins.json`, new Set(["21"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q11-compact-join-layouts.json`, new Set(["11"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q13-compact-join-layouts.json`, new Set(["13"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q87-compact-join-layouts.json`, new Set(["87"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q88-compact-join-layouts.json`, new Set(["88"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q25-compact-join-layouts-confirm.json`, new Set(["25"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q48-compact-join-layouts.json`, new Set(["48"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q49-compact-join-layouts.json`, new Set(["49"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q85-compact-join-layouts.json`, new Set(["85"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q76-compact-join-layouts.json`, new Set(["76"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q56-compact-sql-shape.json`, new Set(["56"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q60-compact-sql-shape.json`, new Set(["60"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q56-compact-sql-shape.json`, new Set(["56"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q60-compact-sql-shape.json`, new Set(["60"]));
    overlayVelox(
      velox,
      suite,
      `${targeted}/velox-tpcds-corrected`,
      "benchmarks/sweeps/20260716-targeted/velox-tpcds-corrected",
      new Set(["56", "60"]),
    );
    // The old compact-layout q22 capture returned zero rows because the streaming direct-range index advertised a
    // single-match path it did not implement. Use only rows validated transitively against Trino SQL.
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q22-q32-fused-dictionary-final-3fork.json`, new Set(["32"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q22-completed-sequential-range-final-3fork.json`, new Set(["22"]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q22-mapped-fusion-final-3fork.json`, new Set(["22"]));
    overlayJvm(nitro, `${sweep}/nitro-tpcds-q23-packed-int-pair-compact-control.json`, new Set(["23"]));
    overlayJvm(nitro, `${sweep}/nitro-tpcds-q24-fragmented-skip-guard.json`, new Set(["24"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q24-direct-binary-dispatch.json`, new Set(["24"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q24-key-only-pair-on-canonical.json`, new Set(["24"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q24-streaming-batch-pair-on.json`, new Set(["24"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q24-promoted-equality-canonical-retry.json`, new Set(["24"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q20-q98-fast-pool-canonical.json`, new Set(["20"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q98-fast-pool-standalone-canonical.json`, new Set(["98"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q31-q34-computed-columnar-canonical.json`, new Set(["31", "34"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q98-computed-columnar-on-canonical.json`, new Set(["98"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q12-q20-q98-binary-hash-window-canonical.json`, new Set(["12"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q20-binary-hash-window-on.json`, new Set(["20"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q20-current-final-3fork.json`, new Set(["20"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q98-binary-hash-window-steady-canonical.json`, new Set(["98"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q32-adaptive-window-canonical.json`, new Set(["32"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q32-fast-on-canonical-3fork.json`, new Set(["32"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q32-sql-parity-canonical-stable.json`, new Set(["32"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q78-compact-tail-canonical-long.json`, new Set(["78"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q23-q78-q97-adaptive-final-3fork.json`, new Set(["23", "78", "97"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q23-generated-pair-default-guard.json`, new Set(["23"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q65-generated-pair-final-3fork.json`, new Set(["65"]));
    overlayJvm(nitro, `${targeted}/nitro-tpcds-q75-compact-distinct-final-3fork.json`, new Set(["75"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q72-compact-hybrid-boundaries-final-3fork.json`, new Set(["72"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q72-byte-bounded-pair-cap-on-canonical-3fork.json`, new Set(["72"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q72-flat-pair-duplicate-store-on-canonical-3fork.json`, new Set(["72"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q44-current-final-3fork.json`, new Set(["44"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q85-direct-exact-implicit-refs-final-3fork.json`, new Set(["85"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q95-dense-bitmap-batch-final-3fork.json`, new Set(["95"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q31-q34-q58-q60-q83-flat-identity-null-sort-final-3fork.json`, new Set(["31", "34", "58", "60", "83"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q21-sparse64-page-boundary-on-final-3fork.json`, new Set(["21"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q64-pair-tag-mask-guard-on-3fork.json`, new Set(["64"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q76-definition-mask-final-3fork.json`, new Set(["76"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q64-wide-deferred-payload-final-3fork.json`, new Set(["64"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q64-compact-sql-shape-final-3fork.json`, new Set(["64"]));
    overlayJvm(trino, `${targeted17}/trino-tpcds-q64-compact-sql-shape-final-3fork.json`, new Set(["64"]));
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q45-q64-q82-adaptive-tiny-compose-final-3fork.json`, new Set(["45", "64", "82"]));
    // The previous q11 overlay projected sold-date and sales values into the customer identity slots, omitted
    // birth/login, and returned no rows. Replace it only with the shape-correct capture validated transitively
    // against Trino SQL (Nitro operator == Trino operator == SQL).
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q11-shape-correct-final-3fork.json`, new Set(["11"]));
    overlayJvm(trino, `${targeted17}/trino-tpcds-q72-compact-join-layouts-3fork.json`, new Set(["72"]));
    overlayJvm(trino, `${targeted}/trino-tpcds-q78-all-metrics.json`, new Set(["78"]));
    overlayVelox(
      velox,
      suite,
      `${targeted}/velox-tpcds-q78-all-metrics-confirm`,
      "benchmarks/sweeps/20260716-targeted/velox-tpcds-q78-all-metrics-confirm",
      new Set(["78"]),
    );
    // q26 now follows Trino's optimized SQL join order, projects dead columns after each join, and uses
    // explicit sum/count finalization for the three DECIMAL averages in every engine.
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q26-sql-shape-final-3fork.json`, new Set(["26"]));
    overlayJvm(trino, `${targeted18}/trino-tpcds-q26-sql-shape-final-3fork.json`, new Set(["26"]));
    overlayVelox(
      velox,
      suite,
      `${targeted18}/velox-tpcds-q26-sql-shape`,
      "benchmarks/sweeps/20260718-targeted/velox-tpcds-q26-sql-shape",
      new Set(["26"]),
    );
    // The old q39 captures either skipped the streamed unique join lookup, used an invalid compact projection,
    // or omitted warehouse name from the comparison engine's physical grouping. These replacements preserve the
    // SQL's exact warehouse name+key+item+month grouping and return the expected 92 SF10 rows.
    overlayJvm(nitro, `${targeted17}/nitro-tpcds-q39-flat-lookahead-final-3fork.json`, new Set(["39"]));
    overlayJvm(trino, `${targeted18}/trino-tpcds-q39-four-key-final-3fork.json`, new Set(["39"]));
    overlayVelox(
      velox,
      suite,
      `${targeted18}/velox-tpcds-q39-four-key`,
      "benchmarks/sweeps/20260718-targeted/velox-tpcds-q39-four-key",
      new Set(["39"]),
    );
    // Replace every structurally eligible current-source row together. The normalized-key accelerator remains a
    // generic optional FlatKeyLayout representation and falls back to complete record equality.
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-normalized-int-key-activations-final-3fork.json`, new Set([
      "03", "19", "24", "27", "29", "36", "38", "42", "46", "52", "62", "68", "70", "71",
      "72", "77", "80", "86", "87", "99",
    ]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q39-normalized-int-key-final-3fork.json`, new Set(["39"]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-mapped-direct-final-3fork.json`, new Set(["22", "32", "92"]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-nullable-filter-final-3fork.json`, new Set(["12", "20", "98"]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-homogeneous-nullable-filter-final-3fork.json`, new Set(["12", "20", "98"]));
    overlayJvm(nitro, `${sweep}/nitro-tpcds-q84-streaming-unused-build.json`, new Set(["84"]));
    overlayJvm(trino, `${sweep}/trino-tpcds-q84-shaped.json`, new Set(["84"]));
    overlayJvm(nitro, `${sweep}/nitro-tpcds-q66-conditional-aggregation.json`, new Set(["66"]));
    // Publish the complete activation cohort from one final-source invocation. This deliberately supersedes every
    // older focused row for an affected query, including faster thermal sequences, so the board does not cherry-pick
    // exact-build coalescing results across captures.
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-auto-exact-variable-majority-final-cohort-3fork.json`, new Set([
      "04", "06", "11", "24", "30", "34", "38", "73", "74", "76", "79", "81", "87", "94",
    ]));
    // Q34/Q73 subsequently adopted the dead-column boundaries in Trino's optimized SQL plan. Both JVM harnesses
    // were repaired together and validated transitively; these current-shape rows supersede the pre-repair cohort.
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q34-q73-sql-shape-final-3fork.json`, new Set(["34", "73"]));
    overlayJvm(trino, `${targeted18}/trino-tpcds-q34-q73-sql-shape-final-3fork.json`, new Set(["34", "73"]));
    // Reuse page-wide selected numeric IDs only when more than one output window can consume the decoded page.
    // This generic reader admission rule was first qualified for q79 and also removes repeated page-head work from
    // the shape-correct q34/q73 fact scans. Publish all affected current-source rows, including fresh full Velox
    // counter/allocation captures for the two former duration losses.
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q34-q73-current-final-3fork.json`, new Set(["34", "73"]));
    for (const query of ["34", "73"]) {
      const prefix = `${targeted18}/velox-tpcds-q34-q73-current-final-q${query}`;
      overlayVeloxCapture(
        velox,
        query,
        `${prefix}.log`,
        `${prefix}.perf.csv`,
        `benchmarks/sweeps/20260718-targeted/velox-tpcds-q34-q73-current-final-q${query}.{log,perf.csv}`,
      );
    }
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q79-null-semantics-final-3fork.json`, new Set(["79"]));
    overlayJvm(trino, `${targeted18}/trino-tpcds-q79-null-semantics-final-3fork.json`, new Set(["79"]));
    overlayVeloxCapture(
      velox,
      "79",
      `${targeted18}/velox-tpcds-q79-null-semantics-final.log`,
      `${targeted18}/velox-tpcds-q79-null-semantics-final.perf.csv`,
      "benchmarks/sweeps/20260718-targeted/velox-tpcds-q79-null-semantics-final.{log,perf.csv}",
    );
    // Current-source publication rows after the allocation/reader campaign. Keep each complete same-invocation
    // duration/counter/allocation bundle together instead of mixing it with an older favorable capture.
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q96-current-3fork.json`, new Set(["96"]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q20-q57-q60-current-3fork.json`, new Set(["20", "57", "60"]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-q26-q47-q50-q56-exact-coverage-final-3fork.json`, new Set(["26", "47", "50", "56"]));
    // Predicate-derived dictionary acceptance is reusable only when predicate identity and content generation both
    // match, the physical dictionary is large enough to amortize the cache, and the survivor density is outside the
    // measured warm branchy exclusion band. Replace every actual activation from one final-source invocation.
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-versioned-predicate-final-3fork.json`, new Set([
      "01", "03", "07", "18", "24", "26", "27", "40", "49", "53", "54", "63", "64", "71",
      "89", "93",
    ]));
    overlayJvm(nitro, `${targeted18}/nitro-tpcds-versioned-predicate-64k-guards-final-3fork.json`, new Set(["88", "96"]));
    overlayJvm(nitro, `${targeted21}/tpcds-q97-grouped-probe-default-publication-fixed-events-3fork.json`, new Set(["97"]));
    const q97CurrentJoint = `${targeted22}/tpcds-q97-triple-grouped-probe/current-joint-nitro-trino-3fork.json`;
    overlayJvm(nitro, q97CurrentJoint, new Set(["97"]), "org.weakref.nitro.");
    overlayJvm(trino, q97CurrentJoint, new Set(["97"]), "org.weakref.trino.");
    const directDenseNullJoint = `${targeted22}/tpcds-q97-triple-grouped-probe/direct-is-null-final/current-joint-nitro-trino-q01-q97-3fork.json`;
    overlayJvm(nitro, directDenseNullJoint, new Set(["01", "97"]), "org.weakref.nitro.");
    overlayJvm(trino, directDenseNullJoint, new Set(["01", "97"]), "org.weakref.trino.");
    overlayJvm(
      nitro,
      `${targeted22}/tpcds-q26-vector-id-unpack/publication-current-3fork.json`,
      new Set(["26"]),
      "org.weakref.nitro.");
    overlayJvm(
      nitro,
      `${targeted22}/tpcds-q14-null-free-distinct/shared-candidate-3fork.json`,
      new Set(["14"]),
      "org.weakref.nitro.");
    // This publication contains both JVM namespaces in one invocation. Filter by namespace so every duration,
    // allocation, and full counter bundle remains an internally coherent engine row.
    const mixedCompositeJoinPublication = `${targeted21}/tpcds-q70-flat-join-dictionary/publication-jvm-3fork.json`;
    overlayJvm(nitro, mixedCompositeJoinPublication, new Set(["08", "36", "70", "86"]), "org.weakref.nitro.");
    overlayJvm(trino, mixedCompositeJoinPublication, new Set(["08", "36", "70", "86"]), "org.weakref.trino.");
    // The single-run binary publication contains both benchmark namespaces in one JMH invocation. Filter by
    // namespace so q66/q76 remain coherent same-invocation Nitro/Trino bundles rather than overwriting each other.
    const singleRunBinaryPublication = `${targeted21}/tpcds-q76-single-run-binary-publication/jvm-q66-q76-3fork.json`;
    overlayJvm(nitro, singleRunBinaryPublication, new Set(["66", "76"]), "org.weakref.nitro.");
    overlayJvm(trino, singleRunBinaryPublication, new Set(["66", "76"]), "org.weakref.trino.");
    overlayVeloxCapture(
      velox,
      "76",
      `${targeted21}/tpcds-q76-single-run-binary-publication/velox-q76.log`,
      `${targeted21}/tpcds-q76-single-run-binary-publication/velox-q76.perf.csv`,
      "benchmarks/sweeps/20260721-targeted/tpcds-q76-single-run-binary-publication/velox-q76.{log,perf.csv}",
    );
    overlayJvm(
      nitro,
      `${targeted22}/tpcds-q18-key-only-direct-range/publication/nitro-q16-q18-default-3fork.json`,
      new Set(["16", "18"]),
    );
    const q26LongWarmupPublication = `${targeted22}/tpcds-q26-current/joint-long-warmup-3fork.json`;
    overlayJvm(nitro, q26LongWarmupPublication, new Set(["26"]), "org.weakref.nitro.");
    overlayJvm(trino, q26LongWarmupPublication, new Set(["26"]), "org.weakref.trino.");
    const sparseDynamicFilterPublication = `${targeted22}/tpcds-q26-build-key-audit/sparse-publication-joint-3fork.json`;
    overlayJvm(nitro, sparseDynamicFilterPublication, new Set(["07", "26"]), "org.weakref.nitro.");
    overlayJvm(trino, sparseDynamicFilterPublication, new Set(["07", "26"]), "org.weakref.trino.");
    const sparseDynamicFilterLongWarmup = `${targeted22}/tpcds-q26-build-key-audit/sparse-publication-q27-q08-wi15-joint-3fork.json`;
    overlayJvm(nitro, sparseDynamicFilterLongWarmup, new Set(["27"]), "org.weakref.nitro.");
    overlayJvm(trino, sparseDynamicFilterLongWarmup, new Set(["27"]), "org.weakref.trino.");
  }
  if (suite === "clickbench") {
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q10-partitioned-distinct.json`, new Set(["10"]));
    overlayJvm(nitro, `${sweep}/nitro-clickbench-q11-q12-grouped-distinct.json`);
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q11-q12-partitioned-distinct.json`);
    overlayJvm(nitro, `${sweep}/nitro-clickbench-q17-q18-confirm.json`);
    overlayJvm(nitro, `${sweep}/nitro-clickbench-q17-q22-dense-id.json`);
    overlayJvm(nitro, `${sweep}/nitro-clickbench-q18-dense-id-ab-on.json`);
    overlayJvm(nitro, `${sweep}/nitro-clickbench-q18-sparse-result-on.json`);
    overlayJvm(nitro, `${sweep}/nitro-clickbench-q22-presize-plain-binary-on.json`, new Set(["22"]));
    overlayJvm(nitro, `${sweep}/nitro-clickbench-q06-q18-q43-current.json`);
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q06-current.json`, new Set(["06"]));
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q18-early-composite-reject.json`, new Set(["18"]));
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q17-q18-fast-long-binary.json`, new Set(["17", "18"]));
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q12-fast-composite-on-canonical.json`, new Set(["12"]));
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q20-general-static-eq-true-canonical.json`, new Set(["20"]));
    overlayJvm(nitro, `${targeted}/nitro-clickbench-q29-regexp-host-specialization-canonical.json`, new Set(["29"]));
    // Promptly closing staged-grouping inputs changes a broad ClickBench cohort. Overlay the complete current-source
    // recapture after older focused rows so duration, counters, and allocation all describe the retained lifecycle.
    overlayJvm(nitro, `${closeBatchSweep}/nitro-clickbench.json`);
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q10-partial-fusion-final-3fork.json`, new Set(["10"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q10-group-reserve-final-3fork.json`, new Set(["10"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q10-int-fusion-final-3fork.json`, new Set(["10"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q12-flat-contains-sweep-final-3fork.json`, new Set(["12"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q21-flat-contains-sweep-final-3fork.json`, new Set(["21"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q24-flat-contains-sweep-final-3fork.json`, new Set(["24"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q30-adaptive-pool-final-3fork.json`, new Set(["30"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q43-close-consumed-batches-final-3fork.json`, new Set(["43"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q06-filter-sentinel-final-3fork.json`, new Set(["06"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q06-flat-single-binary-on-final-3fork.json`, new Set(["06"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q06-distinct-identity-final-3fork.json`, new Set(["06"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q05-batch-versioned-final-3fork.json`, new Set(["05"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q36-default-admission-3fork.json`, new Set(["36"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q34-q35-flat-single-identity-final-3fork.json`, new Set(["34", "35"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q11-q12-single-dict-final-3fork.json`, new Set(["11", "12"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q11-reuse-numeric-dict-final-3fork.json`, new Set(["11"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q12-reuse-numeric-dict-final-3fork.json`, new Set(["12"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q06-binary-dict-recycle-on-final-3fork.json`, new Set(["06"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q06-frequent-sentinel-final-3fork.json`, new Set(["06"]));
    overlayJvm(nitro, `${targeted17}/nitro-clickbench-q18-wide-deferred-payload-final-3fork.json`, new Set(["18"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q11-q12-bulk-selected-numeric-ids-final-3fork.json`, new Set(["11", "12"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q24-empty-decode-deferred-final-3fork.json`, new Set(["24"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q22-shared-dense-false-on-final-3fork.json`, new Set(["22"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q17-stream-tuple-reuse-on-3fork.json`, new Set(["17"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q06-q13-q15-q17-q18-dictionary-hash-reuse-final-3fork.json`, new Set(["06", "13", "15", "17", "18"]));
    overlayJvm(nitro, `${targeted18}/nitro-clickbench-q20-versioned-predicate-final-3fork.json`, new Set(["20"]));
    overlayJvm(nitro, `${targeted21}/clickbench-q05-branch/vector-keys-default-publication-3fork.json`, new Set(["05"]));
    overlayJvm(nitro, `${targeted21}/clickbench-q36-grouped-probe/restored-current-publication-3fork.json`, new Set(["36"]));
    overlayJvm(nitro, `${targeted21}/clickbench-q30-deferred-results-publication-3fork.json`, new Set(["30"]));
    const q16IdIndexedPublication = `${targeted22}/clickbench-q16-id-indexed/final-cap16-joint-nitro-trino-3fork.json`;
    overlayJvm(nitro, q16IdIndexedPublication, new Set(["16"]), "org.weakref.nitro.");
    overlayJvm(trino, q16IdIndexedPublication, new Set(["16"]), "org.weakref.trino.");
    overlayJvm(nitro, `${targeted22}/clickbench-q32-grouping/candidate-on-3fork.json`, new Set(["32"]));
    overlayJvm(
      nitro,
      `${targeted22}/tpch-q12-residual/clickbench-constrained-candidate-3fork-b.json`,
      new Set(["00", "24", "31", "32", "37", "38", "39", "40", "41", "42", "43"]),
    );
    overlayJvm(
      nitro,
      `${targeted22}/clickbench-q29-regex-correctness/nitro-q29-corrected-3fork.json`,
      new Set(["29"]),
    );
    const q40LongWarmupPublication = `${targeted22}/clickbench-q40-current/joint-long-warmup-3fork.json`;
    overlayJvm(nitro, q40LongWarmupPublication, new Set(["40"]), "org.weakref.nitro.");
    overlayJvm(trino, q40LongWarmupPublication, new Set(["40"]), "org.weakref.trino.");
    const q43LongWarmupPublication = `${targeted22}/clickbench-q43-current/joint-long-warmup-3fork.json`;
    overlayJvm(nitro, q43LongWarmupPublication, new Set(["43"]), "org.weakref.nitro.");
    overlayJvm(trino, q43LongWarmupPublication, new Set(["43"]), "org.weakref.trino.");
  }
  suites.push({display, suite, first, last, nitro, trino, velox});
}

const fmtDuration = value => value == null ? "—" : value.toFixed(1);
const fmtCounter = value => {
  if (value == null || !Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(3)}B`;
  if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(3)}M`;
  if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(3)}K`;
  return value.toFixed(0);
};
const fmtCounters = row => row == null ? "—" : events.map(event => fmtCounter(row.counters[event])).join(" / ");
const fmtAllocation = row => row?.allocationBytes == null ? "—" : fmtCounter(row.allocationBytes);
const speedup = (nitro, other) => nitro && other ? `${(other.duration / nitro.duration).toFixed(3)}x` : "—";
const queryName = (suite, query) => suite === "clickbench" ? `q${query.padStart(2, "0")}` : `q${query.padStart(2, "0")}`;

const summarize = (suiteRows, otherKey, label) => {
  let count = 0;
  let wins = 0;
  let logarithm = 0;
  let nitroSum = 0;
  let otherSum = 0;
  for (const row of suiteRows) {
    if (!row.nitro || !row[otherKey]) continue;
    count++;
    const ratio = row[otherKey].duration / row.nitro.duration;
    if (ratio > 1) wins++;
    logarithm += Math.log(ratio);
    nitroSum += row.nitro.duration;
    otherSum += row[otherKey].duration;
  }
  return {label, count, wins, geometric: Math.exp(logarithm / count), nitroSum, otherSum, sumSpeedup: otherSum / nitroSum};
};

let out = `# Operator-harness engine comparison — JDK 26\n\n`;
out += `Generated 2026-07-22 from the completed full-counter sweep plus the explicitly named focused overlays below. Nitro and Trino are unpinned with one JMH worker, fresh forks, JDK 26, THP, and 12 GiB heaps. Velox is pinned to CPU 0 with one driver/I/O thread and a 12 GiB managed cache. Compiler benchmarks are excluded.\n\n`;
out += `Speedup is comparison duration divided by Nitro duration; values above 1.0 favor Nitro. Every counter bundle is **instructions / cycles / L1D misses / L1D loads / dTLB misses / dTLB loads / branch misses / branches**, normalized per operation. A dash means the engine/query did not produce a valid capture; no counter is estimated or represented as zero.\n\n`;
out += `Allocation is normalized bytes per measured query invocation. Nitro and Trino report total JVM heap allocation from JMH; Velox reports cumulative bytes allocated through the completed query's managed memory-pool tree. These scopes are useful for within-engine optimization but are not presented as a strict cross-runtime ratio. Peak RSS remains a separate safety metric.\n\n`;

const allRows = [];
const summaries = [];
for (const suite of suites) {
  const rows = [];
  for (let number = suite.first; number <= suite.last; number++) {
    const query = String(number).padStart(2, "0");
    rows.push({query, nitro: suite.nitro.get(query), trino: suite.trino.get(query), velox: suite.velox.get(query)});
  }
  allRows.push(...rows);
  out += `## ${suite.display}\n\n`;
  out += `| query | Nitro ms | Trino ms | Velox ms | N/T speedup | N/V speedup | Nitro alloc B/op | Trino alloc B/op | Velox managed alloc B/op | Nitro counters | Trino counters | Velox counters |\n`;
  out += `|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|\n`;
  for (const row of rows) {
    out += `| ${queryName(suite.suite, row.query)} | ${fmtDuration(row.nitro?.duration)} | ${fmtDuration(row.trino?.duration)} | ${fmtDuration(row.velox?.duration)} | ${speedup(row.nitro, row.trino)} | ${speedup(row.nitro, row.velox)} | ${fmtAllocation(row.nitro)} | ${fmtAllocation(row.trino)} | ${fmtAllocation(row.velox)} | ${fmtCounters(row.nitro)} | ${fmtCounters(row.trino)} | ${fmtCounters(row.velox)} |\n`;
  }
  out += `\n`;
  summaries.push(summarize(rows, "trino", `${suite.display} N/T`));
  summaries.push(summarize(rows, "velox", `${suite.display} N/V`));
}

const summaryTable = rows => {
  let table = `| comparison | common queries | Nitro wins | geometric-mean Nitro speedup | Nitro duration sum | comparison duration sum | sum-duration speedup |\n`;
  table += `|---|---:|---:|---:|---:|---:|---:|\n`;
  for (const row of rows) table += `| ${row.label} | ${row.count} | ${row.wins} | ${row.geometric.toFixed(3)}x | ${row.nitroSum.toFixed(1)} ms | ${row.otherSum.toFixed(1)} ms | ${row.sumSpeedup.toFixed(3)}x |\n`;
  return table;
};
out += `## Per-benchmark summary\n\n${summaryTable(summaries)}\n`;
out += `## Overall summary\n\n${summaryTable([summarize(allRows, "trino", "Overall N/T"), summarize(allRows, "velox", "Overall N/V")])}\n`;

out += `## Sources and qualifications\n\n`;
out += `- The final 2026-07-22 sparse dynamic-filter overlay changes only construction of an already-admitted exact single-long runtime filter. Collection begins with the established exact hash set; at 4,096 distinct values it defers further deduplication only when the observed domain density is at most 0.5%, copies the exact prefix into pooled sequential storage, and rebuilds the same dense-domain or sparse exact membership representation at publication. A complete three-suite activation inventory admits only TPC-DS q07/q26/q27 and TPC-H q08; denser, smaller, multi-key, and over-bound builds retain the existing path. The first broad deferred-dedup prototype regressed q14/q16/q54/q71 and was rejected, as were pooled-set and single-key-loop prototypes. Bidirectional screens and the final publication improve the admitted cohort without recognizing a query, table, column, or logical type. Final Nitro q07/q26/q27 are 415.5/158.3/430.5 ms; q26 now beats Velox instructions (2.910B/2.931B), and q07/q27 beat every available Velox counter except branches. Q26's only remaining Velox counter deficits are branch misses (1.015x) and branches (1.196x). TPC-H q08 is 1659.5 ms and beats every available Velox counter. Nitro and Trino use joint unpinned JDK 26, one-thread, THP, 12 GiB, three-fork captures with allocation and the pinned eight-event bundle. Trino q27/q08 compile deterministically near operation 13, so their first ten-warmup rows are explicitly rejected and the published joint recapture uses fifteen warmups; the late step occurs in warmup in every fork and all measurements are flat. Seven exact filter/set tests and ten real-SF10 operator/SQL/compiled checks pass. Evidence is under \`benchmarks/sweeps/20260722-targeted/tpcds-q26-build-key-audit/\`.\n`;
out += `- The final 2026-07-22 ClickBench q43 row restores the established ten-warmup requirement after the later three-warmup direct-decoder cohort overwrote it with compiler-contaminated counters. Nitro and Trino were recaptured together in one unpinned JDK 26, one-thread, THP, 12 GiB, three-fork invocation; the SQL-shaped scan/filter/minute projection/group/TopN/offset harness is unchanged. Current Nitro is 118.0 ms, 96.933 MB/op, 2.304B instructions, 664.550M cycles, 43.810M L1D misses, 21.687K dTLB misses, and 1.277M branch misses. It is 10.602x faster than same-invocation Trino and 1.169x faster than Velox, and beats every available Velox hardware counter. The first warmup operation remains roughly 3x the stable plateau, proving that a source-cohort overlay must preserve a query's stronger warmup qualification rather than silently reverting to the cohort default. Evidence is under \`benchmarks/sweeps/20260722-targeted/clickbench-q43-current/\`.\n`;
out += `- The final 2026-07-22 ClickBench q40 row replaces a compiler-contaminated three-warmup counter capture with a joint Nitro/Trino ten-warmup, three-fork invocation. The SQL-shaped eight-column scan/filter/CASE/five-key group/TopN/offset harnesses are unchanged and both JVM engines use the same unpinned JDK 26, one-thread, THP, and 12 GiB policy. An inclusive operator profile attributes approximately 54% of Nitro CPU to scan, 20% to group/aggregation, and 8% each to filter and TopN. The old Nitro row's 18.645B instructions and 7.284B cycles were not steady-state data-path work: after compiler quiescence the coherent row is 629.4 ms, 188.051 MB/op, 10.479B instructions, 3.602B cycles, 102.668M L1D misses, 174.174K dTLB misses, and 29.912M branch misses. Nitro is 18.270x faster than the same-invocation Trino row and 1.120x faster than Velox; it beats every available Velox counter except branch misses, whose 1.097x residual remains the established Snappy/build-provenance target. Evidence is under \`benchmarks/sweeps/20260722-targeted/clickbench-q40-current/\`.\n`;
out += `- The final 2026-07-22 ClickBench q29 row repairs the generic exact-pattern host-extraction specialization for Joni newline semantics. The anchored regexp does not match when LF occurs inside the path, while a single final LF is an unmatched suffix that regexp_replace retains after the captured host. Empty authorities and URLs without a path also remain unchanged. The byte-level implementation now preserves all four outcomes without invoking the generic regex engine or changing the SQL-shaped scan/project/group/TopN harness. A nine-case differential dictionary test computes its oracle through Joni; isolated q29 and the q22-q32 real-data compiled/operator partition pass, as do q34-q43 in a fresh JVM. The independent q33 compiled oracle still exceeds the mandated 12 GiB test heap and is recorded as a gate limitation rather than run with a larger heap. The corrected unpinned JDK 26 three-fork publication is 16096.3 ms, 2.403 GB/op, 286.713B instructions, 88.342B cycles, 1.634B L1D misses, 1.201M dTLB misses, and 1.042B branch misses. Nitro remains 9.389x faster than Trino and 3.080x faster than Velox while beating every available Velox counter. Evidence is under \`benchmarks/sweeps/20260722-targeted/clickbench-q29-regex-correctness/\`.\n`;
out += `- The final 2026-07-22 TPC-H q12 and ClickBench numeric-scan overlay decodes null-free fixed-width Parquet pages directly into ordinary pooled output batches: dictionary pages retain page-sized IDs and gather into the final array, while plain pages copy from the decompressed segment without a page-sized value array. Admission is execution-wide, requires at least one scan with three integer columns and 16M rows, and is generic across files and pipelines. An all-numeric scan additionally waits for runtime evidence that a downstream operator constrained its first batch; this keeps unconstrained q10/q33 on the buffered path after isolated q33 A/B/A improved duration 1.69% and L1D misses 6.45% but regressed instructions 0.58%, dTLB misses/loads 5.07%/4.27%, and branch misses/branches 2.31%/1.22%. The final 11-query ClickBench candidate/control/candidate cohort improves geometric-mean duration 4.45%, allocation 0.86%, instructions 2.95%, cycles 5.42%, L1D misses/loads 16.02%/3.74%, dTLB misses/loads 5.16%/7.87%, and branch misses/branches 4.61%/2.99%; every duration improves. TPC-H q12 steady-state A/B/A improves duration 1.93%, allocation 6.77%, cycles 1.67%, and L1D misses 20.17%; its independent publication records 754.6 ms and 186.6M L1D misses, leaving about a 1% L1D-miss residual to Velox while beating Velox on every other available counter. The final activation inventory admits only q12 in TPC-H, no TPC-DS query, and the eleven published ClickBench rows. Thirty-three fixture, real-data, compiled/operator, and q12 reference checks pass. Evidence is under \`benchmarks/sweeps/20260722-targeted/tpch-q12-residual/\`.\n`;
out += `- The final 2026-07-22 TPC-DS q14 overlay resolves a logical dictionary position once across at least three adaptive integer DISTINCT lanes only when every lane has the same dictionary depth and exact ID-array identity at every level. The nullable extension requires every null stream to share that already-proved mapping chain, resolves each selected row once, and tests the sibling boolean bases directly; any mismatch, extra dictionary level, arity change, or unsupported vector retains the established exact path. Symmetric three-fork candidate/control/candidate means improve duration 2.99%, instructions 8.76%, L1D loads 12.21%, dTLB loads 19.60%, and branches 9.79%, with allocation neutral. Cycles, L1D misses, dTLB misses, and branch misses trade by 0.95%/0.93%/7.79%/8.16% after changing direction across adjacent screens; the coherent independent publication is 4394.4 ms, 87.065B instructions, and 37.591B L1D loads, bringing q14 instructions below Velox while retaining large duration and work reductions. A complete final-source 165-query inventory activates the nullable resolver exactly twelve times in TPC-DS q14 and nowhere else. Exact shared/mismatched/promotion/nullable tests, the compiled q14 oracle, real-SF10 Nitro and Trino operator/SQL parity, and the full 1,277-test gate pass. Evidence is under \`benchmarks/sweeps/20260722-targeted/tpcds-q14-{residual,null-free-distinct}/\`.\n`;
out += `- The final 2026-07-22 TPC-DS q26 overlay unrolls four independent bit-packed dictionary-ID reads only inside the existing generic direct nullable cursor admission: at least one million rows, at least 99% present, ID width at least 12, and accepted dictionary coverage from 0.5% through 2%. The exact scalar tail, accepted-ID branches, SQL-derived operator graph, and null semantics are unchanged; no scratch or query/table/column/type special case is introduced. Bidirectional three-fork candidate/control evidence improves duration 1.50%, instructions 0.83%, cycles 1.33%, L1D misses/loads 1.27%/0.52%, dTLB misses/loads 0.90%/9.08%, and branches 1.30%, with allocation and branch-miss tradeoffs of +0.12%/+1.55%. The independent coherent current-source publication is 168.8 ms and is retained despite slower host placement than the superseded row. Nitro remains faster than Velox and wins cycles, L1D, and dTLB work; instructions, branch misses, and branches remain explicit gaps. Focused RLE tests, direct and compiled real-SF10 q26 parity, and the full 1,275-test gate pass. Evidence is under \`benchmarks/sweeps/20260722-targeted/tpcds-q26-vector-id-unpack/\`.\n`;
out += `- The final 2026-07-22 TPC-DS q01/q97 overlay copies a dense physical null stream directly for every typed IS NULL function. Flat boolean streams use bulk copy; a one-level dictionary stream reads its ids and boolean dictionary in one monomorphic loop. Sparse, nested, concatenated, and RLE shapes retain the generic accessor. The expression graph, SQL shape, mask semantics, and null contract are unchanged. A complete 165-query inventory activates exactly TPC-DS q01/q97, with zero TPC-H or ClickBench activation. Against the finalized disabled three-fork control, q97 improves duration 4.11%, instructions 11.12%, cycles 3.99%, L1D loads 5.47%, dTLB misses/loads 30.14%/24.41%, and branches 10.98%; L1D and branch misses trade by +0.10%/+0.49%. Q01 improves duration 0.73%, instructions 1.16%, dTLB misses 6.78%, and branches 0.75%, with sub-0.4% L1D/branch-miss tradeoffs. The matched Nitro/Trino publication is q01 146.5/540.3 ms and q97 1194.9/2380.5 ms. Q97 now records 1.441M dTLB misses versus Velox's 903K; branch misses remain 32.199M/13.805M. Both still win duration and every other available Velox counter. Full inventory, reverse control, publication, and parity evidence is under \`benchmarks/sweeps/20260722-targeted/tpcds-q97-triple-grouped-probe/direct-is-null-final/\`.\n`;
out += `- The final 2026-07-22 TPC-DS q16/q18 overlay admits a large key-only single-long join directly into the existing exact non-negative range representation when a 4K first-batch sample spans no more than twice the known build cardinality. Negative, wider, sparse, duplicate, and unsupported builds retain exact fallback behavior. A complete 165-query inventory activates only TPC-DS q16/q18: TPC-H q21's 7.3M keys over a 60M domain reject before allocating the map, and ClickBench has no activation. Qualified candidate/control/candidate means improve q18 duration 12.65%, instructions 5.52%, cycles 11.88%, L1D misses 26.29%, dTLB misses 0.92%, branch misses 3.09%, and branches 2.07%, with allocation neutral. Q16 is wall-neutral, improves dTLB misses 28.66% and dTLB loads 17.94%, and has a measured 1.23% cycles tradeoff. The independent default three-fork publication is q16/q18 5326.3/276.4 ms. Q16 now beats Velox on every available counter except dTLB misses (1.286M/553K); q18 beats Velox on every counter except a 1.28% branch-miss residual (8.133M/8.030M), down from 14.415M. The unchanged SQL-shaped harnesses pass 110 join/operator tests and both real-SF10 compiled q16/q18 oracles. Evidence is under \`benchmarks/sweeps/20260722-targeted/tpcds-q18-key-only-direct-range/\`.\n`;
out += `- The final 2026-07-22 ClickBench q32 overlay lowers the existing packed full-width primitive-pair grouping admission floor from 4,096 to 1,024 selected rows only when an evenly spaced first-batch sample proves that at least one live key exceeds the signed-32 compact domain. Signed-32 pairs remain on the Classfile-generated compact table; dictionary-backed pairs retain their dictionary-aware path. The packed identity layout stores each exact key once in dense records and uses self-contained {hash, record-id} probe slots instead of duplicating two full-width keys in sparse hash slots and the reverse map. A complete 165-query inventory newly admits only q32, retains the pre-existing q33 admission, rejects q31's signed-32 pair, and finds no TPC-H or TPC-DS activation. The qualified three-fork candidate/control is 2887.2/3124.2 ms, 1.214/3.314 GB/op, 45.965/46.283B instructions, 16.246/18.141B cycles, 601.144/655.696M L1D misses, 1.558/13.800M dTLB misses, and 91.082/113.679M branch misses. Nitro q32 now beats Velox on duration and every available hardware counter. The q31 broad-floor control regressed duration 10.0% and was excluded by the compact-domain gate. Exact compact/full-width admission tests, fixture, real-data, compiled-harness q32 parity, and the 1,273-test full gate pass. Evidence is under \`benchmarks/sweeps/20260722-targeted/clickbench-q32-grouping/\`.\n`;
out += `- The final 2026-07-22 ClickBench q16 overlay replaces duplicate key-by-slot storage in a proven high-cardinality, run-heavy single-long grouping table with packed six-bit hash fragments plus dense group ids. Exact equality resolves through the canonical key-by-group map, and an exact bounded promotion restores ordinary full-key slots before the packed id domain can be crossed. A complete 165-query inventory activates only q16. The packed layout cuts allocation 65.2% and dTLB misses 96.8% against ordinary slots. Dense canonical-map rehash plus a bounded 16x activation capacity horizon then avoid four repeated sparse-table rebuilds: candidate/control/candidate averages 1254.453/1550.337 ms, 14.223B/16.755B instructions, 6.848B/8.324B cycles, 178.141M/232.653M L1D misses, and 45.228M/60.189M branch misses. The final same-invocation Nitro/Trino publication is 1246.159/2093.232 ms with allocation and all eight counters; Nitro beats both Trino and Velox on duration and every hardware counter. Exact rehash/promotion, fixture, real-data, compiled/operator parity, and the 1,272-test full gate pass. Evidence is under \`benchmarks/sweeps/20260722-targeted/clickbench-q16-id-indexed/\`.\n`;
out += `- The 2026-07-21 TPC-DS q23 generated multi-long \`COUNT(*)\` fusion is a rejected diagnostic and does not replace the published row. The Classfile-generated compact probe updated count state at its existing match/insert outcome and omitted the group-ID vector; an exact cold promotion path rolled back and replayed a wide-key prefix. Complete activation inventories initially found TPC-DS q23/q34/q73 and ClickBench q36, with no TPC-H use. A generic two-key, at-least-8K-live-row gate reduced final activation to q23 only. Dense/sparse/promotion tests and compiled/operator parity for all four candidates passed. The decisive identical unpinned fixed-12-GiB three-fork pair is 5227.5 ms enabled versus 5188.9 ms disabled: instructions, L1D misses, branch work, and allocation improve slightly, but duration regresses 0.75%, cycles 0.72%, L1D loads 1.10%, dTLB misses 5.44%, and dTLB loads 1.81%. The complete implementation and property were removed. Evidence is under \`benchmarks/sweeps/20260721-targeted/tpcds-q23-fused-count/\`; the board rows and aggregate summaries are unchanged.\n`;
out += `- The 2026-07-21 compiled TPC-DS repair preserves operator-harness plan shapes rather than weakening parity checks. Q59 aggregates its complete weekly CTE before downstream month fanout and uses the operator harness's million-scale ratio representation; q83 propagates nullable arithmetic companions instead of coalescing missing sums to zero. Q60 exposed a generic pooled-record lifecycle bug: the null-free single-binary writer replaced the key payload but did not reset the nullable layout's flag byte, so a fixed-record chunk retained by an earlier query could collapse valid keys. The writer now establishes that byte for every new record, with a poisoned-chunk regression. Focused q57/q60 and q59/q83 parity pass, followed by the full 104-test real-SF10 compiled TPC-DS gate with zero failures or skips. Performance rows are unchanged. Evidence is under \`benchmarks/sweeps/20260721-targeted/compiled-correctness-repair/\`.\n`;
out += `- The final 2026-07-21 TPC-DS q66/q76 overlay recognizes a one-run RLE binary grouping field once at the generic flat-layout batch boundary, interns the exact constant for execution-stable identity, and stores only that identity in each group record. A complete 165-query inventory activates only q66/q76 without naming a query, table, column, or logical type. Adjacent enabled/disabled controls improve q66/q76 duration 1.4%/3.6%, instructions 2.4%/6.3%, cycles 1.8%/3.1%, and L1D loads 1.7%/5.8%. The retained Nitro and Trino rows come from one unpinned JDK 26, \`-Xmx12g\`, THP, ten-warmup, three-fork invocation with allocation and all eight counters; q76 Velox is a fresh CPU0-pinned, one-driver/I/O, 12 GiB-cache ten-repeat capture. Nitro q66 remains behind Velox on branch misses and total branches, while q76 retains a 1.474x L1D-miss excess; these remain explicit targets. Exact q66/q76 real-SF10 parity, focused lifecycle/table checks, and the full 1,260-test gate pass. The q66 compiled oracle was repaired to preserve the SQL stage boundary by rounding each UNION branch's ratio before the outer SUM. Evidence is under \`benchmarks/sweeps/20260721-targeted/tpcds-q76-single-run-binary-*\`.\n`;
out += `- The final 2026-07-21 ClickBench q30 overlay removes unobservable intermediate result materialization from the generic blocking \`AggregationOperator\`. The operator still initializes and accumulates every input batch in the same order, but calls each accumulator's \`result\` method once after the source is exhausted instead of once before and after every batch. The Trino-SQL-derived scan/project/90-sum shape is unchanged. An adjacent reverse control preserves the old lifecycle exactly and improves duration 8.5%, allocation 9.1%, instructions 12.0%, cycles 7.6%, L1D loads 7.0%, dTLB loads 6.9%, branch misses 28.1%, and branches 6.3%; its small absolute dTLB-miss increase remains far below Velox. The canonical unpinned JDK 26, \`-Xmx12g\`, THP, ten-warmup, three-fork publication is 4923.1 ms and beats Velox on duration, instructions, cycles, L1D misses, and dTLB work. Fixture, real-data, compiled-query, the 1,257-test full gate, and an exact multi-batch single-materialization regression pass. Evidence is under \`benchmarks/sweeps/20260721-targeted/clickbench-q30-*\`.\n`;
out += `- Base Nitro/Trino rows: \`benchmarks/sweeps/20260715-full-counters/{nitro,trino}-{tpch,tpcds,clickbench}.json\`; the complete current-source Nitro ClickBench recapture is \`benchmarks/sweeps/20260717-close-batches/nitro-clickbench.json\`.\n`;
out += `- Base Velox rows: \`benchmarks/sweeps/20260715-full-counters/velox-{tpch,tpcds,clickbench}/qNN.{log,perf.csv}\`; perf totals are divided by the configured 10 repeats. Corrected q09 and q78 captures replaced their invalid harness rows in place; corrected q26 and four-key q39 are overlaid from \`benchmarks/sweeps/20260718-targeted/velox-tpcds-q{26-sql-shape,39-four-key}\`.\n`;
out += `- New full sweeps collect allocation in the same invocation as duration and counters: JMH \`gc.alloc.rate.norm\` for Nitro/Trino, and task-pool cumulative allocation bytes/count plus peak managed bytes for Velox. Historical rows remain dashes until recaptured; focused Nitro q13 and q78 allocation captures are overlaid without replacing their canonical duration/counter rows.\n`;
out += `- Focused overlays: canonical TPC-H q07 64M bounded direct join domain, q08 shape-correct Nitro/Trino recaptures, q09 one-copy bounded build coalescing plus density-aware single-key hash layout and allocation-free page headers, q17 constraint-sensitive computed-output lifecycle repair, q19 generic zero-bitwise-overlap build pruning plus prompt consumed-probe batch closure, q21 recapture, q22 allocation-free substring slices, packed same-width short-key IN dispatch, exact adaptive membership state, direct shared-mask compaction, fixed-width SIMD dispatch, and branchless bit insertion, q13 stream-specific outer-join NULLS materialization, q14/q15 static numeric-range scan pushdown (with the SQL predicate retained above the scan), q14/q15 narrow join boundaries, q16 direct grouped-distinct aggregation plus prompt-close and allocation-free page-header recaptures, and q18 one-scan aggregate-reuse shape-correct Nitro/Trino recaptures; TPC-DS q02/q04/q11/q13/q25/q48/q49/q76/q78/q79/q85/q87/q88 shape-correct compact join layouts, q20/q98 constant-time common pooled-vector unlink, q22 row-group-cardinality-safe dynamic filtering plus compact join layouts, q23 fused null-as-zero arithmetic plus adaptive packed-pair compact-control grouping, q24 fragmented binary skip-decode guard, direct single-binary-residual dispatch, exact scan cardinality, payload-free pair-key build layout, and exact binary-equality residual promotion, q31/q34/q98 eager columnar full-sort buffering across computed projections, q32 bounded adaptive narrow-scan filtering plus allocation-free page headers and a SQL-parity Trino recapture, q39 compact join layouts plus exact normalized primitive key lanes, q47/q57/q78 nested-dictionary composition, q66 conditional pivot aggregation, q72 shape-correct compact live columns for Nitro/Trino plus selective and final physical dictionary-collapse boundaries in Nitro, q75 adaptive compact wide-key DISTINCT plus batch-hoisted null accessors, q78 nullable packed three-int grouping, q82 range-order confirmation, q84 shape-correct streaming unused-build indexing (plus a shape-correct Trino recapture), q85 shared dictionary composition, lazy duplicate state, one-copy exact build coalescing, and exact-promotable implicit row references, and q93 range-density filter order; ClickBench uses the complete current-source prompt-close recapture, with q06 set-owned record identity, three-fork q10 partial plain-accumulator fusion, bounded grouped-DISTINCT batch reservation, and primitive-width-specialized generated grouping/aggregation, q12/q21/q24 dense flat UTF-8 containment sweeps, q30 adaptive-pool, and q43 prompt-close canonical overlays. Each overlay replaces duration, allocation when present, and the complete counter bundle from the same invocation.\n`;
out += `- The 2026-07-17 scalar-DISTINCT overlays add ClickBench q05's pooled long set, bounded representation admission, and batch-versioned steady-state hash loop, plus TPC-DS q95's batch-versioned dense bitmap loop.\n`;
out += `- The 2026-07-17 ClickBench q36 overlay admits the same generic Classfile-generated compact long-key grouping table for structurally eligible arity-four-and-wider schemas; its reverse control disables that admission without changing the harness.\n`;
out += `- The final 2026-07-21 ClickBench q36 row is a restored-current recapture after rejecting a broader grouped hash-fragment scan. The adjacent reverse control showed that the scan reduced selected load and branch-miss counters but increased duration, instructions, cycles, dTLB misses, and total branches, so its arity-four admission was removed. The retained generic compact grouping implementation and SQL-derived scan/project/four-key-group/count/top-N harness are unchanged. Fixture, real-data, compiled-query, and all-arity table checks pass. The replacement is one unpinned JDK 26, 12 GiB, ten-warmup, three-fork invocation with duration, allocation, and all eight counters; it reduces the prior row's duration 2.4%, allocation 68.8%, cycles 5.8%, L1D misses 12.0%, dTLB misses 94.1%, dTLB loads 46.5%, and branch misses 7.7%. Evidence is under \`benchmarks/sweeps/20260721-targeted/clickbench-q36-grouped-probe/\`.\n`;
out += `- The 2026-07-17 record-identity overlays use one bounded high-cardinality admission policy for ClickBench q34/q35 and TPC-DS q60/q83; q31 is a measured rejected control. The same TPC-DS bundle also replaces q34 after the trailing-empty columnar-sort fix and q58/q83 after recursively preserving SQL-null companion streams and matching Trino decimal output scale.\n`;
out += `- The 2026-07-17 TPC-DS q64 overlay bounds ordinary post-load join-build coalescing at one million rows while preserving the existing four-million-row limit for explicit direct one-copy builds. It also includes prompt release of untaken result masks and materialized result vectors. The absolute duration is host-state sensitive; the adjacent 1M/4M A/B is the causal performance evidence.\n`;
out += `- The final 2026-07-17 q64/TPC-H q09 overlays guard the JDK 26 VectorMask-to-scalar-bitset conversion in the generic long-pair join index. Negative probes first use a Vector any-match reduction and convert only actual tag hits; empty-slot tests stay as Vector reductions. Q45 and q82 are retained guard cohorts, and allocation is neutral.\n`;
out += `- The 2026-07-17 TPC-DS q21 overlay replaces linear survivor-page boundary discovery with binary search only for batches containing at least 64 survivors and at most 1% selected rows. Broader admission was rejected on q45/q82; the retained bounded policy passed a three-fork reverse control and preserves linear discovery for those cohorts.\n`;
out += `- The 2026-07-17 TPC-DS q76 overlay compacts flat Boolean predicates directly into the Filter operator's owned output mask, lets every typed IS NULL scalar use one shared direct null-mask contract, and delegates that contract to a lazy definition-level-only Parquet sibling reader. Reverse-order controls establish all three changes; together they remove the temporary intersection mask, its exact-size copy, the materialized IS NULL Boolean result, and unnecessary value/id decoding while preserving the SQL-derived operator graph.\n`;
out += `- The 2026-07-17 ClickBench q11/q12 overlay specializes any dictionary predicate with exactly one matching dictionary entry into a direct row-id comparison. The evaluator, SQL-derived filter/grouped-DISTINCT graph, null semantics, and generic fallback remain unchanged; reverse controls cover both equality polarity and the q11/q12 cohort.\n`;
out += `- The final 2026-07-17 TPC-DS q64, TPC-H q09, and ClickBench q18 overlays enable batch-local deferred payload decoding for selective all-numeric filter windows with at least four non-filter payload columns. Filter/key columns remain materialized, the first downstream consumer may constrain the live batch, and unresolved payload readers decode only the retained positions; weak constraints retain the eager survivor decode. Admission depends only on physical width and selectivity, mask scratch is pooled, and the Trino-SQL-derived operator graphs are unchanged. Adjacent enable/disable captures cover all three suites; the board rows use unpinned 12 GiB three-fork captures with full counters and JVM allocation.\n`;
out += `- The final 2026-07-17 TPC-DS q11 overlay explicitly invalidates the old 1015.2 ms row: that compact harness projected the wrong post-date-join columns and returned no rows. The repaired projection retains list price, discount, customer identity/name/preference, birth country, and login in the positions consumed downstream, matching the Trino operator harness; that harness independently matches Trino SQL on SF10. The retained generic grouping hash still performs complete composite equality, and the flat join stores the common unique-key row reference in a pooled primitive array while allocating duplicate lists only on the first duplicate. Its unpinned 12 GiB three-fork row includes the complete counter and JVM-allocation bundle.\n`;
out += `- The final 2026-07-17 TPC-DS q64 Nitro and Trino rows replace the earlier wide-harness captures. Velox and Trino's optimized SQL plan project dead probe keys and build payload at every join, while the old Nitro/Trino operator harnesses accumulated all build columns to a 53-column intermediate. Both JVM harnesses now use the same per-join live-column lists as the Velox SQL-derived plan and remain transitively value-checked against Trino SQL. This is an apples-to-apples harness repair, not a Nitro-only plan optimization; both replacement rows are unpinned 12 GiB three-fork captures with full counters and JVM allocation.\n`;
out += `- The final q45/q64/q82 adaptive-composition overlay collapses a nested probe-row mapping at depth four only when the current join result has at most 1,024 rows, sharing the composed IDs across columns with identical encoding chains. Larger batches remain lazy. Adjacent and reverse controls retain the policy: q64 improves the main work and locality counters, q45 is positive, and q82 is the large-batch guard. The SQL-derived operator graphs are unchanged.\n`;
out += `- The old TPC-DS q22 compact-layout row is invalidated: replaying its frozen source returns zero rows because a streaming direct-range index advertised flat single-match probing without implementing that lookup. The replacement retains the SQL-derived compact layout, converts a completed unique compact streaming range to the existing dense single-batch representation, and returns the correct 100 rows. Nitro equals the Trino operator harness and that harness equals Trino SQL on SF10. The replacement is an unpinned 12 GiB three-fork capture with full counters and JVM allocation.\n`;
out += `- The final 2026-07-18 TPC-DS q22 overlay keeps the generated single-long-key grouping/aggregation kernel active beyond the ordinary 32K cache-local boundary only when the physical key is dictionary-mapped or sampled adjacent rows repeatedly reuse a key. Flat high-cardinality inputs retain the staged two-pass path. Mapped continuation state uses the grouping table's geometric high-water mark rather than applying generic vector headroom to a conservative dictionary-cardinality bound; the broader form was rejected after q06 exposed extra grow/copy work. Bidirectional q22 controls, q32/q44/q92 guards, SF10 Nitro-to-Trino-operator-to-Trino-SQL parity, and the 1,188-test gate pass. The board row is an unpinned 12 GiB three-fork capture with full counters and JVM allocation.\n`;
out += `- The final 2026-07-18 mapped-direct grouping bundle supersedes the preceding q22 row and replaces the complete retained cohort: TPC-DS q22/q32/q92. After at least 8K groups, a depth-one dictionary-mapped non-negative long key may replace open-address hashing with direct group-id indexing only when the observed range is below 128K, no more than 11.5x the group count, and proves eligibility by 16K groups. A later wider/negative batch rebuilds canonical hashing before processing that batch. Flat q44 and ClickBench q10 are excluded by physical shape; q04 is excluded by range; q64 is excluded because density becomes viable only after the latest-admission boundary. The rejected forms exposed extra instructions/allocation or unstable wall/dTLB behavior. The generated direct variant has no hash, collision loop, or key-equality branch. Q22 is 744.019 ms with 16.927B instructions, 4.025B cycles, 113.830M L1D misses, 155.358K dTLB misses, 6.871M branch misses, and 101.659 MB/op. Transitive SF10 SQL parity and the 1,189-test gate pass; the three board rows come from one unpinned 12 GiB three-fork capture with full counters and JVM allocation.\n`;
out += `- The final 2026-07-18 nullable dictionary-filter bundle replaces TPC-DS q12/q20/q98. Optional dictionary pages with actual nulls keep definition levels and dictionary IDs as independent RLE streams and co-advance them in reusable L1-sized tiles, eliminating the page-sized position-to-ID prefix and its two per-row prefix loads. Null-free pages retain their existing run-aware path, plain/binary pages retain their fallbacks, and exact null semantics are unchanged. Reverse q20 controls and q12/q98 guards improve wall, instructions, cycles, L1D misses, allocation, and branch work; dTLB movements are small and mixed. SF10 Nitro/operator/Trino-SQL parity and the 1,190-test gate pass. The board rows use one unpinned 12 GiB three-fork capture with duration, full counters, and JVM allocation.\n`;
out += `- The final homogeneous-definition repair supersedes the preceding nullable dictionary-filter rows for TPC-DS q12/q20/q98. The RLE cursor intentionally leaves definition scratch untouched when an entire tile is all-present or all-null; the filter consumer had nevertheless reread that stale scratch. Homogeneous tiles now bypass scratch and consume every dense ID or none. A dictionary-encoded regression file returns 4,500 expected survivors with the repair and only 108 under the frozen unsafe control. SF10 transitive SQL parity and the 1,191-test gate pass. The current three-fork rows retain complete duration/counter/allocation bundles.\n`;
out += `- The final ClickBench q12 overlay reuses numeric Parquet dictionary storage as reader-owned high-water scratch across row-group/file chunks. Numeric outputs materialize values and never retain the dictionary arrays, unlike binary DictionaryVector output, so clearing the numeric references at every chunk boundary created about 172.7 MB/op of avoidable garbage. Bidirectional q12 controls improve wall, cycles, instructions, L1D, dTLB misses, and branch misses while reducing adjacent allocation 59.7% (289.2 to 116.5 MB/op); post-fix allocation sampling no longer contains numeric dictionary growth. The operator graph remains the Trino-SQL-derived scan/filter/group/grouped-DISTINCT/TopN shape. The board row is an unpinned 12 GiB three-fork capture with full counters and JVM allocation.\n`;
out += `- The final ClickBench q11 overlay applies the same numeric dictionary-scratch lifetime to its two numeric dictionary scans. Against the preceding canonical row, duration falls from 437.8 to 423.9 ms and allocation from 323.4 to 109.2 MB/op; instructions, cycles, L1D misses, and dTLB misses also improve, while branch misses are flat within noise. The SQL-derived scan/filter/group/grouped-DISTINCT/TopN graph is unchanged.\n`;
out += `- The final ClickBench q06 overlay recycles binary dictionary payload storage only after the batch that exposed its generation has closed. A scan-specific BatchBufferOwner marks dictionary values that cross take(), so escaped generations remain live while unescaped generations return exact-size offsets to the primitive pool and retain the largest byte payload as reader-owned scratch. The canonical enabled/disabled three-fork comparison improves duration 1.1%, cycles 0.9%, L1D loads 0.8%, L1D misses 0.4%, and allocation 36.2% (508.1 to 324.1 MB/op); instructions are 0.2% lower, while dTLB misses and branch work are slightly adverse. An independent q22 guard reduces allocation 56.8% with otherwise nearly flat counters. Allocation sampling reduces sampled byte-array sites from 223 to 39, the retained-generation ownership regression passes, and the full 1,193-test gate passes. The Trino-SQL-derived operator graphs are unchanged.\n`;
out += `- The final ClickBench q06 overlay admits the concrete null-free single-binary dictionary record path only when the first active batch proves that the empty sentinel is frequent. The decision samples 128 logical mask ranks, is frozen for the distinct-set lifetime so sentinel ownership cannot change across batches, and depends only on physical encoding plus active-row value frequency. Q06 admits at 112/128 empty samples; all other 164 operator queries reject it. The three-fork enable/disable pair improves duration 29.7%, instructions 47.5%, cycles 29.3%, L1D loads 52.9%, L1D misses 4.0%, dTLB loads 10.4%, branch misses 3.6%, and branches 49.1%, with allocation neutral. Nitro is 1.469x faster than Velox with 29.8% fewer instructions and 28.4% fewer cycles; its remaining q06 L1D-miss excess is 8.7%. Fixture, compiled/harness, and real-data parity pass, as does the full 1,197-test gate. The Trino-SQL-derived scan -> distinct -> count graph is unchanged.\n`;
out += `- The final ClickBench q11/q12 overlay decodes each eligible numeric dictionary page's compact ID stream once into reader-owned row-aligned high-water scratch, then looks up wide numeric values only for actual survivors. This replaces a stateless selected decoder that restarted from the page head for every output batch. Admission still requires a constrained mask at or below 20% survivors, amortized contiguous survivor runs, and a dictionary-encoded physical page; plain and fragmented inputs retain bulk decode. Adjacent three-fork controls improve q11 wall 7.1%, cycles 7.5%, and L1D misses 24.1%, and improve q12 wall 8.4%, cycles 8.8%, L1D misses 30.9%, and dTLB loads 18.6%. Q12 instructions and branches increase 2.1% and 3.0%, while q11 branch misses increase 1.7%; those residuals remain explicit optimization targets. The SQL-derived scan/filter/group/grouped-DISTINCT/TopN graphs are unchanged.\n`;
out += `- The final ClickBench q24 overlay establishes a general empty-mask reader lifecycle: a schema-only borrow after a filter removes every row returns a correctly typed pooled vector, accumulates pending reader advancement, and does not decode or commit the column to an irreversible page strategy. The first batch with actual survivors chooses the path and drains preceding rows in that same path. This removes q24 from the numeric-selected cohort without a query/column/type-specific guard; its later fragmented masks retain bulk decode. Against the frozen behavior, the unpinned 12 GiB three-fork control improves duration 5.1%, instructions 5.0%, cycles 5.3%, L1D misses 3.2%, dTLB misses 13.4%, branch misses 5.0%, and allocation 12.9% (3.476 to 3.027 GB/op). Real-data q11/q12/q24 parity, fallback controls, all 65 Parquet tests, and the full 1,195-test gate pass.\n`;
out += `- The final ClickBench q22 overlay replaces per-result dense all-false NULLS/ERRORS buffers with one allocator-wide immutable dense vector per logical batch length. The borrowed constant is unowned, safe to release repeatedly, and shared by interpreter and fused-project completion paths; operators do not recognize it. A one-run RLE prototype cut allocation but regressed instructions, cycles, and L1D through Boolean type-profile pollution, so the retained cache preserves the established dense shape. The three-fork enable/disable control is CPU-neutral (6659.2/6653.8 ms, 98.754B/98.877B instructions, 35.322B/35.280B cycles) while reducing allocation by 148.1 MB/op (32.4%). Closest-laggard guards span all three suites; fixture, compiled, real-data parity, and the 1,198-test gate pass. The board row is an unpinned 12 GiB three-fork capture with the complete counter/allocation bundle.\n`;
out += `- The old TPC-DS q39 rows are invalidated for physical-shape as well as correctness defects. Nitro's streamed unique lookup skipped matches, then its first compact projection dropped the warehouse key while treating the warehouse name as that key. Separately, the Trino and Velox operator harnesses grouped by only warehouse key, item, and month; SF10's functional dependency let them return the same visible values while omitting the variable-width warehouse-name key required by Trino SQL. All three harnesses now group by the literal four-key SQL shape and return the same 92 rows; the Trino operator harness independently matches Trino SQL. Corrected q39 is Nitro 616.0 ms, Trino 18,813.8 ms, and Velox 726.0 ms with full same-invocation counter/allocation bundles. Nitro uses a generic exact normalized-key accelerator for eligible three- and four-field flat layouts: nullable non-negative int-domain longs and query-stable binary IDs pack into two primitive lanes, while complete record equality remains the correctness fallback. Batch sampling, minimum-live-work, and address-density admission prevent sparse masks or unsupported domains from allocating scratch proportional to the source position range.\n`;
out += `- The old TPC-DS q26 harness row is invalidated. All three operator harnesses joined date, item, demographics, and promotion in a SQL-equivalent order that did not match Trino's optimized SQL plan, and the JVM harnesses retained dead join columns. They now follow the optimized order catalog_sales -> customer_demographics -> date_dim -> item -> promotion and project the same live columns at every boundary. The three DECIMAL averages also use explicit sum/count states followed by scale-two rounding in every engine; previously the JVM harnesses returned fractional physical cents. Nitro and Trino operator results match Trino SQL on SF10. The first shape-qualified capture was Nitro 171.9 ms, Trino 579.3 ms, and Velox 184.0 ms; Nitro is subsequently superseded by the current-source exact-coverage cohort below.\n`;
out += `- The final 2026-07-18 TPC-H q17 row invalidates the prior current-source crash: constraining a retained computed build batch could reuse a vector resolved under an older selection mask. Computed producers now opt into a general constraint-sensitive output lifecycle; Batch invalidates only those cached resolutions before changing the constraint, while stable scan/pass-through outputs remain reusable. SF10 reference parity and the 1,184-test gate pass. The replacement is an unpinned 12 GiB three-fork capture with duration, full counters, and JVM allocation.\n`;
out += `- The final 2026-07-18 normalized-key publication bundle replaces the complete eligible TPC-DS cohort from one current-source sweep, including slower absolute recaptures; it does not cherry-pick earlier thermal sequences. Adjacent enable/disable controls establish the accelerator's causal effect, while the board remains a reconstructable cross-engine prioritization view.\n`;
out += `- The final automatic exact-build publication bundle initially replaced the complete 14-query TPC-DS activation cohort from one final-source invocation, including slower absolute recaptures. Admission is generic: an exact build of 262,144 to 1,000,000 rows must expose a strict majority of variable-width value columns in its first physical batch. The optimization performs one exact coalescing copy but retains ordinary packed build-row references; coupling it to implicit sequential references regressed work counters and was removed. Broad cardinality-only admission was also rejected after q57/q82 controls. Pooled binary storage now carries a content generation so identity-keyed derived dictionaries cannot survive reuse with stale contents, and build/output buffers use separate ownership scopes while sharing a compatible pool. SF10 q38/q57 transitive SQL parity, the pooled-generation regression, and the 1,199-test full gate pass. Q34/q73 are superseded by the later SQL-shape rows below; projecting their dimension builds to join keys also removes them from the current automatic-coalescing activation set. The remaining board rows are unpinned, 12 GiB, three-fork captures with duration, the complete counter bundle, and JVM allocation.\n`;
out += `- The current TPC-DS q34/q73 rows retain the JVM harness repair that follows Trino SQL's live-column shape after each date, store, and household-demographics join. All five real-SF10 checks pass with no skips: Nitro q34/q73 versus the Trino operator harness, direct Nitro q73 versus SQL, and both Trino operator harnesses versus SQL. A later general Parquet admission repair now decodes page-wide selected numeric dictionary IDs only when another output window can reuse them; a full-page output window stays on the streaming selected decoder. The reverse control reproduces the superseded q34/q73 rows at 369.3/337.6 ms and 8.440B/7.574B instructions. Current unpinned 12 GiB three-fork Nitro is 271.5/223.1 ms with complete counters and allocation; fresh pinned Velox captures are 332/314 ms with the complete eight-counter and managed-allocation bundle. Nitro therefore wins all 99 TPC-DS durations.\n`;
out += `- The final 2026-07-18 TPC-DS q79 row invalidates the prior all-non-null aggregate harness. Trino SQL uses bare \`sum(ss_coupon_amt)\` and \`sum(ss_net_profit)\`, so an all-null group must remain NULL; Nitro and Trino had inserted zero and Velox had explicit \`coalesce\`. All three operator harnesses now preserve nullable aggregate inputs, and the corrected Nitro and Velox canonical 100-row multisets are byte-identical, including the three NULL-bearing rows; Nitro and Trino independently match Trino SQL. The replacement uses unpinned 12 GiB three-fork JMH captures for Nitro/Trino and a pinned one-driver, one-I/O-thread, ten-repeat, 12 GiB managed-cache Velox capture. Every engine row contains duration, the complete eight-counter bundle, and allocation telemetry from the same invocation.\n`;
out += `- The final 2026-07-18 ClickBench q17 row gives every accumulator result-copy implementation the same backing-vector-identity reuse contract. Immutable \`Streams\` transport tuples survive repeated TopN comparisons until a vector actually grows; no accumulator, data type, column, or query receives a bespoke path. Allocation sampling drops the hot \`Streams\` site from 2,946 samples (43.0% of events) to zero. The adjacent three-fork control is CPU-neutral within noise and reduces measured allocation by 1.10 MB/op; the board uses the enabled unpinned 12 GiB capture with the complete counter/allocation bundle. The full 1,201-test gate passes.\n`;
out += `- The final 2026-07-18 ClickBench q06/q13/q15/q17/q18 cohort reuses dictionary-entry hashes only after the same physical vector identity and content generation recur, proving a reuse horizon before paying for a complete entry pass. Pooled vectors advance a general content-generation contract whenever their logical lifetime changes; unsupported vectors conservatively decline cross-batch derived-state reuse. Bidirectional q18 controls improve wall, instructions, cycles, L1D misses, and branch work; q06/q13/q15/q17 guards retain the policy, with small JVM-allocation movements kept explicit. A Velox-style 0.70 grouping load factor was rejected because its larger table reduced some retired work but worsened whole-query wall, dTLB loads, and allocation. The publication replaces the complete five-query cohort from one unpinned 12 GiB three-fork invocation with full counters and JVM allocation; q17's slower absolute recapture is retained rather than cherry-picking its earlier row. Real-data q18 parity and the full 1,202-test gate pass.\n`;
out += `- The final 2026-07-18 TPC-DS q96 row is a current-source recapture, not a harness change. Its Trino-SQL-derived fact scan still applies the three filtered dimension joins before \`count(*)\`. Unpinned 12 GiB three-fork Nitro is 109.268 ms and 6.603 MB/op, 1.245x faster than Velox with fewer cycles, L1D misses, dTLB misses, and branch misses; only instructions remain 4.9% higher. The existing Rust/FFM skip-decoder reference reaches 99.720 ms and removes that instruction gap, proving bounds/session checks in the portable Java selected decoder are the residual, but it remains diagnostic rather than becoming an environment-dependent default.\n`;
out += `- The final 2026-07-18 TPC-DS q20/q57/q60 current-source cohort comes from one unpinned 12 GiB three-fork invocation with full counters and JVM allocation. Harnesses are unchanged. Q20 is stable; q57 improves from 1784.1 to 1601.4 ms and q60 from 324.8 to 306.4 ms as later generic reader/grouping changes reach these shapes. The complete rows replace their prior bundles rather than mixing favorable counters from separate runs.\n`;
out += `- The final 2026-07-18 exact dictionary-coverage cohort replaces TPC-DS q26/q47/q50/q56 from one current-source invocation. A dynamic filter is dropped only after every physical numeric dictionary value in every row-group chunk is accepted; filter and row-group-local dictionary cardinalities are never treated as proof of value coverage. This restores q50 from 1419.0 to 1029.2 ms in the publication capture while q22 remains neutral in the adjacent control. The repair is reader/scan-wide, leaves the Trino-SQL-derived operator graphs unchanged, and passes the multi-chunk cursor regression, four SF10 Nitro/operator/SQL parity checks, and the full 1,202-test gate. The board rows are unpinned 12 GiB three-fork captures with complete counters and JVM allocation.\n`;
out += `- The final 2026-07-18 versioned dictionary-predicate cohort replaces all 21 retained activations from one current-source publication sequence: TPC-H q05/q08/q11/q21; TPC-DS q01/q03/q07/q18/q24/q26/q27/q40/q49/q53/q54/q63/q64/q71/q89/q93; and ClickBench q20. A pushed predicate exposes a version token only when its identity and content generation make derived acceptance stable; the reader uses that token as cache identity while retaining the established evaluator call shape. Admission requires at least 64K physical dictionary entries and rejects the measured 1/12-through-1/9 warm branchy survivor band. The broad 89-query policy was rejected after aggregate counters and sum duration regressed; q16/q75 are explicit density nonactivations. Three-fork controls subsequently rejected the only two 45.5K-entry shapes, q88 and q96, so the general amortization floor moved from 32K to 64K; their board rows come from one rebuilt-default guard capture and contain the full duration/counter/allocation bundle. Bidirectional q26, q05, and ClickBench q20 controls establish causal wall improvement; q26 branch misses and dTLB loads remain explicit residuals. Real-SF10 q26 Nitro/operator/Trino-SQL parity passes with zero skips, and the full JDK 26 gate passes 1,203 tests with zero failures/errors. Every replacement row uses an unpinned 12 GiB three-fork invocation; slower absolute recaptures are retained rather than cherry-picked away.\n`;
out += `- The 2026-07-22 compact q34/q35 dictionary-cache revisit is diagnostic only and does not replace the published rows. Replacing packed 64-bit \`{generation, group}\` entries with 32-bit groups plus batch-boundary clears was neutral on duration in the canonical reverse three-fork control (q34 12165.4/12169.0 ms; q35 12100.3/12094.8 ms) and regressed stable work: q35 instructions +1.03%, cycles +0.49%, L1D misses/loads +1.11%/+1.05%, dTLB misses/loads +3.24%/+1.49%, and branches +1.27%. The prototype was removed; its favorable one-fork q35 dTLB result was placement noise. Evidence is under \`benchmarks/sweeps/20260721-targeted/clickbench-q34-q35-compact-dictionary-cache/\`.\n`;
out += `- The 2026-07-22 TPC-H q20 fused numeric dictionary-decode experiment is diagnostic only. A generic direct RLE fill plus tiled bit-packed gather reduced several counters in its first reverse comparison but regressed wall, instructions, L1D loads, and allocation. Restricting it to pages whose first run was RLE remained order-sensitive: versus the adjacent disabled fork, enabled was 0.54% faster but raised instructions 0.67%, L1D misses 2.30%, dTLB misses/loads 12.09%/16.53%, and branches 1.10%; most counter directions flipped versus the preceding disabled fork. The first run is not a durable classifier for a hybrid page, so the prototype was removed and no row or aggregate changed. Evidence is under \`benchmarks/sweeps/20260722-targeted/tpch-q20-current/\`.\n`;
out += `- The final 2026-07-22 TPC-H q20 row is a current-source, shape-unchanged Nitro/operator and Trino/SQL recapture from one unpinned 12 GiB, 15-warmup, three-fork invocation with JVM allocation and all eight counters. Nitro is 1341.4 ms versus Trino 2031.0 ms and Velox 1820 ms, and beats both comparison engines on instructions, cycles, dTLB misses/loads, branch misses, and duration; Velox retains the L1D-miss lead at 331.921M versus Nitro 369.617M. Both the operator and compiled SF10 reference tests pass with zero skips. Raising the generic fragmented numeric skip-decode ceiling from 6% to 20% was rejected separately: two enabled forks improved duration about 1.8% and L1D misses about 24% versus the intervening control, but increased instructions about 3.5%, L1D loads about 2.2%, branches about 4.2%, and branch misses about 4.3%. A follow-up survivor-streaming decoder likewise lowered L1D misses below Velox but increased instructions 3.19%, dTLB loads 6.96%, branch misses 16.15%, branches 3.22%, and allocation. The default remains unchanged. Publication and reverse-screen artifacts are under \`benchmarks/sweeps/20260722-targeted/tpch-q20-current/\`.\n`;
out += `- This sweep has complete 165-query coverage for all three engines (22 TPC-H, 99 TPC-DS, and 44 ClickBench queries). Any future missing/failed row is shown as a dash and excluded pairwise rather than silently imputed.\n`;
out += `- This is a reconstructable current board, not a claim that focused overlays and base rows were measured in one thermal sequence. Use adjacent isolated A/B captures for optimization decisions; use this board for cross-engine prioritization.\n`;

const workspaceReport = `${root}benchmarks/operator-comparison-report-jdk26-20260715.md`;
const notesReport = `${process.env.HOME}/notes/nitro/operator-comparison-report-jdk26-20260715.md`;
if (process.env.NITRO_REPORT_DRY_RUN === "1") {
  process.stdout.write(out);
  process.exit(0);
}
// The live board contains qualified overlays newer than this legacy reconstruction script. Never silently erase
// those rows/qualifications when someone adds one more overlay here; require an explicit audit override until every
// published overlay has been encoded in the generator.
if (existsSync(workspaceReport) && process.env.NITRO_ALLOW_INCOMPLETE_REPORT_REBUILD !== "1") {
  const current = readFileSync(workspaceReport, "utf8");
  const qualificationCount = text => (text.match(/^- /gm) ?? []).length;
  if (qualificationCount(current) > qualificationCount(out)) {
    throw new Error(
      `Refusing incomplete report rebuild: generated qualifications ${qualificationCount(out)} < published ${qualificationCount(current)}. ` +
      "Encode the missing overlays or set NITRO_ALLOW_INCOMPLETE_REPORT_REBUILD=1 after auditing every changed row.",
    );
  }
}
writeFileSync(workspaceReport, out);
writeFileSync(notesReport, out);
console.log(workspaceReport);
console.log(notesReport);
