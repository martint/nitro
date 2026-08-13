#!/usr/bin/env python3

import csv
import math
from pathlib import Path


ROOT = Path(__file__).parent
BOARD = ROOT / "board.csv"


def number(row, name):
    try:
        value = float(row[name])
        return value if math.isfinite(value) else None
    except (KeyError, TypeError, ValueError):
        return None


def geomean(values):
    usable = [value for value in values if value is not None and value > 0]
    return math.exp(sum(map(math.log, usable)) / len(usable))


def weighted(rows, nitro_name, trino_name):
    pairs = [(number(row, nitro_name), number(row, trino_name)) for row in rows]
    usable = [(nitro, trino) for nitro, trino in pairs if nitro is not None and trino not in (None, 0)]
    return sum(nitro for nitro, _ in usable) / sum(trino for _, trino in usable)


with BOARD.open(newline="") as source:
    board = list(csv.DictReader(source))

with (ROOT / "query-board.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow([
        "suite", "query", "wall_ratio", "cpu_ratio", "nitro_cpu_ms", "trino_cpu_ms",
        "alloc_ratio", "nitro_alloc_mib", "trino_alloc_mib",
        "peak_ratio", "nitro_peak_mib", "trino_peak_mib",
    ])
    for row in board:
        suite = {"tpch-parquet-sf10": "tpch", "tpcds-parquet-sf10": "tpcds"}.get(row["suite"], row["suite"])
        def finite_text(name):
            value = number(row, name)
            return "" if value is None else f"{value:.6f}"
        writer.writerow([
            suite, row["query"], finite_text("wall_ratio"), finite_text("cpu_ratio"),
            finite_text("nitro_cpu_p50_ms"), finite_text("trino_cpu_p50_ms"),
            finite_text("allocation_ratio"), finite_text("nitro_allocation_p50_mib"), finite_text("trino_allocation_p50_mib"),
            finite_text("query_peak_ratio"), finite_text("nitro_query_peak_p50_mib"), finite_text("trino_query_peak_p50_mib"),
        ])

metric_columns = {
    "wall": ("nitro_wall_p50_ms", "trino_wall_p50_ms"),
    "cpu": ("nitro_cpu_p50_ms", "trino_cpu_p50_ms"),
    "allocation": ("nitro_allocation_p50_mib", "trino_allocation_p50_mib"),
    "query_peak": ("nitro_query_peak_p50_mib", "trino_query_peak_p50_mib"),
    "heap_peak": ("nitro_heap_peak_p50_mib", "trino_heap_peak_p50_mib"),
}
summary_header = ["suite", "queries"]
for metric in metric_columns:
    summary_header.extend([f"{metric}_geomean", f"{metric}_weighted", f"{metric}_wins", f"{metric}_pairs"])

with (ROOT / "summary.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(summary_header)
    suites = ["tpch-parquet-sf10", "tpcds-parquet-sf10", "clickbench", "overall"]
    for suite in suites:
        rows = board if suite == "overall" else [row for row in board if row["suite"] == suite]
        output = [suite, len(rows)]
        for metric, (nitro_name, trino_name) in metric_columns.items():
            ratios = [number(row, f"{metric}_ratio") for row in rows]
            usable = [ratio for ratio in ratios if ratio is not None]
            output.extend([
                f"{geomean(usable):.6f}", f"{weighted(rows, nitro_name, trino_name):.6f}",
                sum(ratio < 1 for ratio in usable), len(usable),
            ])
        writer.writerow(output)
