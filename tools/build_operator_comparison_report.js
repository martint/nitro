#!/usr/bin/env bun
import {existsSync, readFileSync, readdirSync, writeFileSync} from "node:fs";

const root = new URL("../", import.meta.url).pathname;
const sweep = `${root}benchmarks/sweeps/20260715-full-counters`;
const targeted = `${root}benchmarks/sweeps/20260716-targeted`;
const targeted17 = `${root}benchmarks/sweeps/20260717-targeted`;
const targeted18 = `${root}benchmarks/sweeps/20260718-targeted`;
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
const readJvm = path => {
  const rows = new Map();
  for (const result of JSON.parse(readFileSync(path, "utf8"))) {
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

const overlayJvm = (rows, path, selected = null) => {
  if (!existsSync(path)) return;
  for (const [query, row] of readJvm(path)) {
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
out += `Generated 2026-07-18 from the completed full-counter sweep plus the explicitly named focused Nitro overlays below. Nitro and Trino are unpinned with one JMH worker, fresh forks, JDK 26, THP, and 12 GiB heaps. Velox is pinned to CPU 0 with one driver/I/O thread and a 12 GiB managed cache. Compiler benchmarks are excluded.\n\n`;
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
out += `- Base Nitro/Trino rows: \`benchmarks/sweeps/20260715-full-counters/{nitro,trino}-{tpch,tpcds,clickbench}.json\`; the complete current-source Nitro ClickBench recapture is \`benchmarks/sweeps/20260717-close-batches/nitro-clickbench.json\`.\n`;
out += `- Base Velox rows: \`benchmarks/sweeps/20260715-full-counters/velox-{tpch,tpcds,clickbench}/qNN.{log,perf.csv}\`; perf totals are divided by the configured 10 repeats. Corrected q09 and q78 captures replaced their invalid harness rows in place; corrected q26 and four-key q39 are overlaid from \`benchmarks/sweeps/20260718-targeted/velox-tpcds-q{26-sql-shape,39-four-key}\`.\n`;
out += `- New full sweeps collect allocation in the same invocation as duration and counters: JMH \`gc.alloc.rate.norm\` for Nitro/Trino, and task-pool cumulative allocation bytes/count plus peak managed bytes for Velox. Historical rows remain dashes until recaptured; focused Nitro q13 and q78 allocation captures are overlaid without replacing their canonical duration/counter rows.\n`;
out += `- Focused overlays: canonical TPC-H q07 64M bounded direct join domain, q08 shape-correct Nitro/Trino recaptures, q09 one-copy bounded build coalescing plus density-aware single-key hash layout and allocation-free page headers, q17 constraint-sensitive computed-output lifecycle repair, q19 generic zero-bitwise-overlap build pruning plus prompt consumed-probe batch closure, q21 recapture, q22 allocation-free substring slices, packed same-width short-key IN dispatch, exact adaptive membership state, direct shared-mask compaction, fixed-width SIMD dispatch, and branchless bit insertion, q13 stream-specific outer-join NULLS materialization, q14/q15 static numeric-range scan pushdown (with the SQL predicate retained above the scan), q14/q15 narrow join boundaries, q16 direct grouped-distinct aggregation plus prompt-close and allocation-free page-header recaptures, and q18 one-scan aggregate-reuse shape-correct Nitro/Trino recaptures; TPC-DS q02/q04/q11/q13/q25/q48/q49/q76/q78/q79/q85/q87/q88 shape-correct compact join layouts, q20/q98 constant-time common pooled-vector unlink, q22 row-group-cardinality-safe dynamic filtering plus compact join layouts, q23 fused null-as-zero arithmetic plus adaptive packed-pair compact-control grouping, q24 fragmented binary skip-decode guard, direct single-binary-residual dispatch, exact scan cardinality, payload-free pair-key build layout, and exact binary-equality residual promotion, q31/q34/q98 eager columnar full-sort buffering across computed projections, q32 bounded adaptive narrow-scan filtering plus allocation-free page headers and a SQL-parity Trino recapture, q39 compact join layouts plus exact normalized primitive key lanes, q47/q57/q78 nested-dictionary composition, q66 conditional pivot aggregation, q72 shape-correct compact live columns for Nitro/Trino plus selective and final physical dictionary-collapse boundaries in Nitro, q75 adaptive compact wide-key DISTINCT plus batch-hoisted null accessors, q78 nullable packed three-int grouping, q82 range-order confirmation, q84 shape-correct streaming unused-build indexing (plus a shape-correct Trino recapture), q85 shared dictionary composition, lazy duplicate state, one-copy exact build coalescing, and exact-promotable implicit row references, and q93 range-density filter order; ClickBench uses the complete current-source prompt-close recapture, with q06 set-owned record identity, three-fork q10 partial plain-accumulator fusion, bounded grouped-DISTINCT batch reservation, and primitive-width-specialized generated grouping/aggregation, q12/q21/q24 dense flat UTF-8 containment sweeps, q30 adaptive-pool, and q43 prompt-close canonical overlays. Each overlay replaces duration, allocation when present, and the complete counter bundle from the same invocation.\n`;
out += `- The 2026-07-17 scalar-DISTINCT overlays add ClickBench q05's pooled long set, bounded representation admission, and batch-versioned steady-state hash loop, plus TPC-DS q95's batch-versioned dense bitmap loop.\n`;
out += `- The 2026-07-17 ClickBench q36 overlay admits the same generic Classfile-generated compact long-key grouping table for structurally eligible arity-four-and-wider schemas; its reverse control disables that admission without changing the harness.\n`;
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
out += `- This sweep has complete 165-query coverage for all three engines (22 TPC-H, 99 TPC-DS, and 44 ClickBench queries). Any future missing/failed row is shown as a dash and excluded pairwise rather than silently imputed.\n`;
out += `- This is a reconstructable current board, not a claim that focused overlays and base rows were measured in one thermal sequence. Use adjacent isolated A/B captures for optimization decisions; use this board for cross-engine prioritization.\n`;

const workspaceReport = `${root}benchmarks/operator-comparison-report-jdk26-20260715.md`;
const notesReport = `${process.env.HOME}/notes/nitro/operator-comparison-report-jdk26-20260715.md`;
writeFileSync(workspaceReport, out);
writeFileSync(notesReport, out);
console.log(workspaceReport);
console.log(notesReport);
