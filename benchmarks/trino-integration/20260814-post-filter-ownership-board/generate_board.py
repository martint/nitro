#!/usr/bin/env python3

import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).parent
LOGS = [
    ROOT / "tpch-w3m3.log",
    ROOT / "clickbench-w3m3.log",
    ROOT / "tpcds-q01-q71-w3m3.log",
    ROOT / "tpcds-q73-q99-w3m3.log",
]
SUITES = {
    "tpch-parquet-sf10": "tpch",
    "tpcds-parquet-sf10": "tpcds",
    "clickbench": "clickbench",
}


def payloads(path):
    with path.open(errors="replace") as source:
        for line in source:
            marker = "\tstdout\t"
            if marker in line:
                yield line.split(marker, 1)[1].strip()


def ratio(candidate, baseline):
    return candidate / baseline if baseline else math.nan


def geomean(values):
    usable = [value for value in values if value > 0 and math.isfinite(value)]
    return math.exp(sum(map(math.log, usable)) / len(usable))


def operator_category(name):
    if "Scan" in name or "PipelineSource" in name or "AggregationSource" in name:
        return "scan/filter/project"
    if "Aggregation" in name:
        return "aggregation"
    if "Join" in name or "Lookup" in name:
        return "join"
    if "Window" in name or "RowNumber" in name or "TopNRanking" in name:
        return "window/ranking"
    if "Sort" in name or "OrderBy" in name or "TopN" in name:
        return "sort/top-n"
    if "Exchange" in name or "Merge" in name or "PartitionedOutput" in name or "TaskOutput" in name:
        return "exchange/output"
    if "Distinct" in name or "MarkDistinct" in name:
        return "distinct"
    if "Unnest" in name:
        return "unnest"
    return "other"


query = {}
allocation = {}
peak = {}
operator_iterations = defaultdict(float)
operator_category_iterations = defaultdict(float)

for log in LOGS:
    for payload in payloads(log):
        row = next(csv.reader([payload]))
        if not row:
            continue
        if row[0] in {"trino", "nitro"} and len(row) >= 10:
            engine, suite, query_id = row[:3]
            suite = SUITES[suite]
            query[engine, suite, query_id] = {
                "measurements": int(row[3]),
                "wall_min": float(row[4]),
                "wall": float(row[5]),
                "wall_max": float(row[6]),
                "wall_mean": float(row[7]),
                "cpu": float(row[8]),
                "cpu_mean": float(row[9]),
            }
        elif row[0] == "allocation":
            allocation[row[1], SUITES[row[2]], row[3]] = float(row[5])
        elif row[0] == "peak_memory":
            peak[row[1], SUITES[row[2]], row[3]] = (float(row[5]), float(row[7]))
        elif row[0] == "operator_cpu":
            engine, suite, query_id, iteration, name = row[1:6]
            suite = SUITES[suite]
            cpu = sum(map(float, row[7:10]))
            key = (engine, suite, query_id, int(iteration))
            operator_iterations[key] += cpu
            operator_category_iterations[key + (operator_category(name),)] += cpu


keys = sorted({key[1:] for key in query}, key=lambda value: (value[0], value[1]))
missing_pairs = [key for key in keys if ("trino",) + key not in query or ("nitro",) + key not in query]
if missing_pairs:
    raise RuntimeError(f"missing engine pairs: {missing_pairs}")

header = [
    "suite", "query",
    "trino_wall_min_ms", "trino_wall_p50_ms", "trino_wall_max_ms",
    "nitro_wall_min_ms", "nitro_wall_p50_ms", "nitro_wall_max_ms", "wall_ratio",
    "trino_cpu_p50_ms", "nitro_cpu_p50_ms", "cpu_ratio",
    "trino_allocation_p50_mib", "nitro_allocation_p50_mib", "allocation_ratio",
    "trino_query_peak_p50_mib", "nitro_query_peak_p50_mib", "query_peak_ratio",
    "trino_heap_peak_p50_mib", "nitro_heap_peak_p50_mib", "heap_peak_ratio",
]

board = []
for suite, query_id in keys:
    trino = query["trino", suite, query_id]
    nitro = query["nitro", suite, query_id]
    trino_alloc = allocation["trino", suite, query_id]
    nitro_alloc = allocation["nitro", suite, query_id]
    if trino_alloc < 0 or nitro_alloc < 0:
        trino_alloc = nitro_alloc = math.nan
    trino_query_peak, trino_heap_peak = peak["trino", suite, query_id]
    nitro_query_peak, nitro_heap_peak = peak["nitro", suite, query_id]
    board.append([
        suite, query_id,
        trino["wall_min"], trino["wall"], trino["wall_max"],
        nitro["wall_min"], nitro["wall"], nitro["wall_max"], ratio(nitro["wall"], trino["wall"]),
        trino["cpu"], nitro["cpu"], ratio(nitro["cpu"], trino["cpu"]),
        trino_alloc, nitro_alloc, ratio(nitro_alloc, trino_alloc),
        trino_query_peak, nitro_query_peak, ratio(nitro_query_peak, trino_query_peak),
        trino_heap_peak, nitro_heap_peak, ratio(nitro_heap_peak, trino_heap_peak),
    ])

with (ROOT / "board.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(header)
    for row in board:
        writer.writerow(row[:2] + [(f"{value:.6f}" if math.isfinite(value) else "") for value in row[2:]])


def measured_operator_values(engine, suite, query_id):
    count = query[engine, suite, query_id]["measurements"]
    iterations = sorted(iteration for item_engine, item_suite, item_query, iteration in operator_iterations
                        if item_engine == engine and item_suite == suite and item_query == query_id)
    measured = set(iterations[-count:])
    return [operator_iterations[engine, suite, query_id, iteration] for iteration in iterations if iteration in measured]


operator = []
for suite, query_id in keys:
    trino_values = measured_operator_values("trino", suite, query_id)
    nitro_values = measured_operator_values("nitro", suite, query_id)
    trino = statistics.median(trino_values)
    nitro = statistics.median(nitro_values)
    operator.append([suite, query_id, trino, nitro, ratio(nitro, trino)])

with (ROOT / "operator-cpu-all.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["suite", "query", "nitro_ms", "trino_ms", "ratio"])
    writer.writerows([[row[0], row[1], f"{row[3]:.6f}", f"{row[2]:.6f}", f"{row[4]:.6f}"] for row in operator])

summary_header = [
    "suite", "queries", "wall_geomean", "wall_weighted", "wall_wins",
    "cpu_geomean", "cpu_weighted", "cpu_wins", "operator_cpu_geomean", "operator_cpu_weighted", "operator_cpu_wins",
    "allocation_geomean", "allocation_weighted", "query_peak_geomean",
    "query_peak_weighted", "heap_peak_geomean", "heap_peak_weighted",
]
summaries = []
for suite in sorted({row[0] for row in board}) + ["overall"]:
    rows = board if suite == "overall" else [row for row in board if row[0] == suite]
    op_rows = operator if suite == "overall" else [row for row in operator if row[0] == suite]
    allocation_rows = [row for row in rows if math.isfinite(row[12]) and math.isfinite(row[13])]
    query_peak_rows = [row for row in rows if row[15] > 0]
    heap_peak_rows = [row for row in rows if row[18] > 0]
    summaries.append([
        suite, len(rows),
        geomean(row[8] for row in rows), sum(row[5] for row in rows) / sum(row[3] for row in rows), sum(row[8] < 1 for row in rows),
        geomean(row[11] for row in rows), sum(row[10] for row in rows) / sum(row[9] for row in rows), sum(row[11] < 1 for row in rows),
        geomean(row[4] for row in op_rows), sum(row[3] for row in op_rows) / sum(row[2] for row in op_rows), sum(row[4] < 1 for row in op_rows),
        geomean(row[14] for row in allocation_rows), sum(row[13] for row in allocation_rows) / sum(row[12] for row in allocation_rows),
        geomean(row[17] for row in query_peak_rows), sum(row[16] for row in query_peak_rows) / sum(row[15] for row in query_peak_rows),
        geomean(row[20] for row in heap_peak_rows), sum(row[19] for row in heap_peak_rows) / sum(row[18] for row in heap_peak_rows),
    ])

with (ROOT / "summary.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(summary_header)
    writer.writerows(summaries)

with (ROOT / "query-board.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["suite", "query", "wall_ratio", "cpu_ratio", "nitro_cpu_ms", "trino_cpu_ms",
                     "alloc_ratio", "nitro_alloc_mib", "trino_alloc_mib", "peak_ratio", "nitro_peak_mib", "trino_peak_mib"])
    for row in board:
        writer.writerow([row[0], row[1], row[8], row[11], row[10], row[9],
                         row[14], row[13], row[12], row[17], row[16], row[15]])


categories = sorted({key[-1] for key in operator_category_iterations})
category_rows = []
for suite in sorted({row[0] for row in board}) + ["overall"]:
    suite_keys = keys if suite == "overall" else [key for key in keys if key[0] == suite]
    for category in categories:
        values = {}
        for engine in ("trino", "nitro"):
            total = 0.0
            for item_suite, query_id in suite_keys:
                count = query[engine, item_suite, query_id]["measurements"]
                iterations = sorted(iteration for item_engine, s, q, iteration in operator_iterations
                                    if item_engine == engine and s == item_suite and q == query_id)[-count:]
                per_iteration = [operator_category_iterations.get((engine, item_suite, query_id, iteration, category), 0.0)
                                 for iteration in iterations]
                total += statistics.median(per_iteration)
            values[engine] = total
        category_rows.append([suite, category, values["trino"], values["nitro"], ratio(values["nitro"], values["trino"]), values["trino"] - values["nitro"]])

with (ROOT / "operator-type-summary.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["suite", "operator_type", "trino_cpu_ms", "nitro_cpu_ms", "ratio", "cpu_saved_ms"])
    writer.writerows(category_rows)

print(f"generated synchronized board for {len(board)} paired queries")
