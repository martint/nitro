#!/usr/bin/env python3

import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).parent
LOGS = [ROOT / "tpch.log", ROOT / "clickbench.log", ROOT / "tpcds.log"]
SUITE_NAMES = {
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


query = {}
allocation = {}
peak = {}
operator_iterations = defaultdict(float)

for log in LOGS:
    for payload in payloads(log):
        row = next(csv.reader([payload]))
        if not row:
            continue
        if row[0] in {"trino", "nitro"} and len(row) >= 10:
            engine, suite, query_id = row[:3]
            suite = SUITE_NAMES[suite]
            query[engine, suite, query_id] = {
                "wall": float(row[5]),
                "cpu": float(row[8]),
            }
        elif row[0] == "allocation":
            allocation[row[1], SUITE_NAMES[row[2]], row[3]] = float(row[5])
        elif row[0] == "peak_memory":
            peak[row[1], SUITE_NAMES[row[2]], row[3]] = (float(row[5]), float(row[7]))
        elif row[0] == "operator_cpu":
            engine, suite, query_id, iteration = row[1:5]
            suite = SUITE_NAMES[suite]
            operator_iterations[engine, suite, query_id, int(iteration)] += sum(map(float, row[7:10]))


keys = sorted({key[1:] for key in query}, key=lambda value: (value[0], value[1]))
header = [
    "suite", "query",
    "trino_wall_p50_ms", "nitro_wall_p50_ms", "wall_ratio",
    "trino_cpu_p50_ms", "nitro_cpu_p50_ms", "cpu_ratio",
    "trino_allocation_p50_mib", "nitro_allocation_p50_mib", "allocation_ratio",
    "trino_query_peak_p50_mib", "nitro_query_peak_p50_mib", "query_peak_ratio",
    "trino_heap_peak_p50_mib", "nitro_heap_peak_p50_mib", "heap_peak_ratio",
]


def ratio(nitro, trino):
    return nitro / trino if trino else math.nan


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
        trino["wall"], nitro["wall"], ratio(nitro["wall"], trino["wall"]),
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


operator = []
for suite, query_id in keys:
    values = {}
    for engine in ("trino", "nitro"):
        iterations = [value for (item_engine, item_suite, item_query, _), value in operator_iterations.items()
                      if item_engine == engine and item_suite == suite and item_query == query_id]
        values[engine] = statistics.median(iterations)
    operator.append([suite, query_id, values["nitro"], values["trino"], ratio(values["nitro"], values["trino"])])

with (ROOT / "operator-cpu-all.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["suite", "query", "nitro_ms", "trino_ms", "ratio"])
    for row in operator:
        writer.writerow(row[:2] + [f"{value:.6f}" for value in row[2:]])

representative_queries = {"q20", "q21", "q64", "q72"}
with (ROOT / "operator-cpu.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["suite", "query", "nitro_ms", "trino_ms", "ratio"])
    for row in operator:
        if row[0] == "tpcds" and row[1] in representative_queries:
            writer.writerow(row[:2] + [f"{value:.6f}" for value in row[2:]])


def geomean(values):
    usable = [value for value in values if value > 0 and math.isfinite(value)]
    return math.exp(sum(map(math.log, usable)) / len(usable))


summary_header = [
    "suite", "queries", "wall_geomean", "wall_weighted", "wall_wins",
    "cpu_geomean", "cpu_weighted", "cpu_wins", "operator_cpu_geomean",
    "allocation_geomean", "allocation_weighted", "query_peak_geomean",
    "query_peak_weighted", "heap_peak_geomean", "heap_peak_weighted",
]
summaries = []
for suite in sorted({row[0] for row in board}) + ["overall"]:
    rows = board if suite == "overall" else [row for row in board if row[0] == suite]
    op_rows = operator if suite == "overall" else [row for row in operator if row[0] == suite]
    allocation_rows = [row for row in rows if math.isfinite(row[8]) and math.isfinite(row[9])]
    query_peak_rows = [row for row in rows if row[11] > 0]
    heap_peak_rows = [row for row in rows if row[14] > 0]
    summaries.append([
        suite, len(rows),
        geomean(row[4] for row in rows), sum(row[3] for row in rows) / sum(row[2] for row in rows), sum(row[4] < 1 for row in rows),
        geomean(row[7] for row in rows), sum(row[6] for row in rows) / sum(row[5] for row in rows), sum(row[7] < 1 for row in rows),
        geomean(row[4] for row in op_rows),
        geomean(row[10] for row in allocation_rows), sum(row[9] for row in allocation_rows) / sum(row[8] for row in allocation_rows),
        geomean(row[13] for row in query_peak_rows), sum(row[12] for row in query_peak_rows) / sum(row[11] for row in query_peak_rows),
        geomean(row[16] for row in heap_peak_rows), sum(row[15] for row in heap_peak_rows) / sum(row[14] for row in heap_peak_rows),
    ])

with (ROOT / "summary.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(summary_header)
    for row in summaries:
        writer.writerow(row[:2] + [row[2], row[3], row[4], row[5], row[6], row[7]] + [f"{value:.6f}" for value in row[8:]])

with (ROOT / "query-board.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["suite", "query", "wall_ratio", "cpu_ratio", "nitro_cpu_ms", "trino_cpu_ms",
                     "alloc_ratio", "nitro_alloc_mib", "trino_alloc_mib", "peak_ratio", "nitro_peak_mib", "trino_peak_mib"])
    for row in board:
        writer.writerow([row[0], row[1], row[4], row[7], row[6], row[5],
                         "" if not math.isfinite(row[10]) else row[10],
                         "" if not math.isfinite(row[9]) else row[9],
                         "" if not math.isfinite(row[8]) else row[8],
                         row[13], row[12], row[11]])
