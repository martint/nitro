#!/usr/bin/env python3
"""Generate an operator board from instrumented Nitro/Trino SQL benchmark logs."""

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path


SUITES = {
    "tpch": "tpch-parquet-sf10",
    "tpcds": "tpcds-parquet-sf10",
    "clickbench": "clickbench",
}
ENGINES = ("nitro", "trino")
OPERATOR_PATTERN = re.compile(
    r"operator_cpu,(nitro|trino),([^,]+),(\d+),([0-9.]+),([0-9.]+),([0-9.]+)"
    r"(?:,(\d+),(\d+),(\d+),([0-9.]+))?")
QUERY_OPERATOR_PATTERN = re.compile(
    r"operator_cpu,(nitro|trino),([^,]+),(q[0-9]+[ab]?),(\d+),([^,]+),(\d+),"
    r"([0-9.]+),([0-9.]+),([0-9.]+),(\d+),(\d+),(\d+),([0-9.]+)")


def operator_family(operator):
    if "AggregationSource" in operator:
        return "fused_scan_aggregation"
    if "Aggregation" in operator:
        return "aggregation"
    if "Join" in operator or "HashBuild" in operator or "HashBuilder" in operator:
        return "join"
    if any(name in operator for name in ("TopN", "Sort", "OrderBy", "Window", "RowNumber")):
        return "ranking_sort_window"
    if any(name in operator for name in ("PipelineSource", "PageProcessor", "ScanFilterAndProject", "TableScan", "FilterAndProject")):
        return "scan_filter_project"
    if any(name in operator for name in ("Exchange", "PartitionedOutput", "TaskOutput", "Merge")):
        return "exchange_output"
    return "other"


def split_operator_key(key):
    stage = ""
    operator_node = key
    if "/" in key:
        stage, operator_node = key.split("/", 1)
    operator, _, plan_node = operator_node.partition("@")
    return stage, operator, plan_node


def parse_log(path, suite_name):
    pending = defaultdict(list)
    rows = []
    query_pattern = re.compile(
        rf"(nitro|trino),{re.escape(suite_name)},(q[0-9]+[ab]?),"
        r"(\d+),[0-9.]+,[0-9.]+,[0-9.]+,[0-9.]+,([0-9.]+),([0-9.]+),")
    for line in path.read_text().splitlines():
        operator = QUERY_OPERATOR_PATTERN.search(line)
        if operator and operator.group(2) == suite_name:
            values = operator.groups()
            pending[(values[0], values[2])].append((values[0], values[4], *values[5:]))
            continue
        operator = OPERATOR_PATTERN.search(line)
        if operator:
            values = operator.groups()
            pending[values[0]].append(values)
            continue
        query = query_pattern.search(line)
        if not query:
            continue
        engine, query_id, measurements, cpu_p50, cpu_mean = query.groups()
        measurements = int(measurements)
        values_for_query = pending.pop((engine, query_id), []) + pending.pop(engine, [])
        for values in values_for_query:
            stage, operator_name, plan_node = split_operator_key(values[1])
            numeric = [float(value) if value is not None else 0.0 for value in values[2:]]
            rows.append({
                "engine": engine,
                "query": query_id,
                "measurements": measurements,
                "query_cpu_p50_ms": float(cpu_p50),
                "query_cpu_mean_ms": float(cpu_mean),
                "stage": stage,
                "plan_node": plan_node,
                "operator": operator_name,
                "family": operator_family(operator_name),
                "drivers": numeric[0] / measurements,
                "add_input_cpu_ms": numeric[1] / measurements,
                "get_output_cpu_ms": numeric[2] / measurements,
                "finish_cpu_ms": numeric[3] / measurements,
                "physical_input_positions": numeric[4] / measurements,
                "input_positions": numeric[5] / measurements,
                "output_positions": numeric[6] / measurements,
                "blocked_wall_ms": numeric[7] / measurements,
            })
    if pending:
        engines = ", ".join(sorted(pending))
        raise ValueError(f"{path}: operator metrics for {engines} are not followed by query results")
    return rows


def load(directory):
    rows = []
    for suite, suite_name in SUITES.items():
        combined_path = directory / f"{suite}.log"
        if combined_path.is_file():
            parsed = parse_log(combined_path, suite_name)
            for row in parsed:
                row["suite"] = suite
            rows.extend(parsed)
            continue
        for engine in ENGINES:
            path = directory / f"{suite}-{engine}-queryphase-instrumented.log"
            if not path.is_file():
                raise ValueError(f"missing SQL-shape input: {path}")
            parsed = parse_log(path, suite_name)
            for row in parsed:
                row["suite"] = suite
            rows.extend(parsed)
    return rows


def write_csv(path, rows):
    fields = [
        "suite", "query", "engine", "measurements", "query_cpu_p50_ms", "query_cpu_mean_ms",
        "stage", "plan_node", "operator", "family", "drivers", "add_input_cpu_ms",
        "get_output_cpu_ms", "finish_cpu_ms", "physical_input_positions", "input_positions",
        "output_positions", "blocked_wall_ms",
    ]
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path, rows):
    totals = defaultdict(float)
    query_counts = defaultdict(set)
    stage_counts = defaultdict(set)
    query_operator_cpu = defaultdict(float)
    query_cpu = {}
    for row in rows:
        key = (row["suite"], row["engine"])
        operator_cpu = row["add_input_cpu_ms"] + row["get_output_cpu_ms"] + row["finish_cpu_ms"]
        totals[key, row["family"]] += operator_cpu
        query_counts[key].add(row["query"])
        query_key = (row["suite"], row["query"], row["engine"])
        query_operator_cpu[query_key] += operator_cpu
        query_cpu[query_key] = row["query_cpu_mean_ms"]
        if row["stage"]:
            stage_counts[key].add((row["query"], row["stage"]))
    families = sorted({row["family"] for row in rows})
    lines = [
        "# SQL-shape operator benchmark board",
        "",
        "Generated from operator summaries of the actual distributed SQL physical plans. Unlike the legacy",
        "single-threaded query fixtures, these measurements retain partial/final stages, exchanges, driver counts,",
        "and the optimizer-selected join and ranking shapes.",
        "",
        "CPU values are milliseconds per measured query invocation.",
        "",
        "| suite | engine | queries | query-stages | " + " | ".join(families) + " |",
        "|---|---|---:|---:|" + "---:|" * len(families),
    ]
    for suite in SUITES:
        for engine in ENGINES:
            key = (suite, engine)
            values = [f"{totals[key, family]:.1f}" for family in families]
            stages = len(stage_counts[key]) if stage_counts[key] else "unavailable"
            lines.append(f"| {suite} | {engine} | {len(query_counts[key])} | {stages} | " + " | ".join(values) + " |")
    lines.extend([
        "",
        "## Operator-family CPU comparison",
        "",
        "| suite | family | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | CPU difference (ms) |",
        "|---|---|---:|---:|---:|---:|",
    ])
    for suite in SUITES:
        for family in families:
            nitro = totals[(suite, "nitro"), family]
            trino = totals[(suite, "trino"), family]
            ratio = f"{nitro / trino:.3f}" if trino else "n/a"
            lines.append(f"| {suite} | {family} | {nitro:.1f} | {trino:.1f} | {ratio} | {nitro - trino:.1f} |")
    lines.extend([
        "",
        "## End-to-end operator CPU reconciliation",
        "",
        "| suite | Nitro operator CPU (ms) | Trino operator CPU (ms) | Nitro / Trino | Nitro CPU coverage | Trino CPU coverage |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for suite in SUITES:
        nitro_operator = sum(query_operator_cpu[suite, query, "nitro"] for query in query_counts[suite, "nitro"])
        trino_operator = sum(query_operator_cpu[suite, query, "trino"] for query in query_counts[suite, "trino"])
        nitro_query = sum(query_cpu[suite, query, "nitro"] for query in query_counts[suite, "nitro"])
        trino_query = sum(query_cpu[suite, query, "trino"] for query in query_counts[suite, "trino"])
        lines.append(
            f"| {suite} | {nitro_operator:.1f} | {trino_operator:.1f} | {nitro_operator / trino_operator:.3f} | "
            f"{nitro_operator / nitro_query:.3%} | {trino_operator / trino_query:.3%} |")

    regressions = []
    for suite in SUITES:
        for query in query_counts[suite, "nitro"] & query_counts[suite, "trino"]:
            nitro = query_operator_cpu[suite, query, "nitro"]
            trino = query_operator_cpu[suite, query, "trino"]
            if nitro > trino:
                regressions.append((nitro - trino, nitro / trino, suite, query, nitro, trino))
    lines.extend([
        "",
        "## Largest single-sweep Nitro CPU regressions",
        "",
        "These are triage candidates. Confirm them with fresh-JVM or interleaved runs before changing production code;",
        "a suite sweep can expose JIT, cache, allocator-pool, and run-order effects.",
        "",
        "| suite | query | Nitro CPU (ms) | Trino CPU (ms) | Nitro / Trino | excess (ms) |",
        "|---|---|---:|---:|---:|---:|",
    ])
    for delta, ratio, suite, query, nitro, trino in sorted(regressions, reverse=True)[:15]:
        lines.append(f"| {suite} | {query} | {nitro:.1f} | {trino:.1f} | {ratio:.3f} | {delta:.1f} |")
    lines.extend([
        "",
        "The CSV is the source of record. Each row retains stage, plan-node, operator type, driver count, CPU phases,",
        "physical/input/output positions, and blocked wall time. Missing stage counts mean the input was captured",
        "before plan-node instrumentation was enabled and must be recaptured before shape-level comparison.",
        "",
    ])
    path.write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_directory", type=Path)
    parser.add_argument("--output-prefix", type=Path)
    args = parser.parse_args()
    prefix = args.output_prefix or args.artifact_directory / "sql-shape-operator-board"
    rows = load(args.artifact_directory)
    write_csv(prefix.with_suffix(".csv"), rows)
    write_markdown(prefix.with_suffix(".md"), rows)


if __name__ == "__main__":
    main()
