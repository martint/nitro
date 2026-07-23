#!/usr/bin/env python3
"""Validate and render a complete, source-consistent operator baseline."""

import argparse
import csv
import json
import math
import re
from pathlib import Path


SUITES = {"tpch": range(1, 23), "tpcds": range(1, 100), "clickbench": range(0, 44)}
ENGINES = ("nitro", "trino", "velox")
EVENTS = (
    "instructions",
    "cycles",
    "L1-dcache-load-misses",
    "L1-dcache-loads",
    "dTLB-load-misses",
    "dTLB-loads",
    "branch-misses",
    "branches",
)
JVM_METRICS = ("gc.alloc.rate.norm", "gc.alloc.rate", "gc.count", "gc.time")
REPEATS = 10
FULL_VELOX_RECAPTURES = {
    ("tpcds", "q14"): "recapture-q14-restored",
    ("clickbench", "q34"): "recapture-q34-clean",
}
RSS_RECAPTURE_DIRECTORY = "recapture-rss"


def finite(value, context):
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{context}: expected finite number, got {value!r}")
    return float(value)


def query_name(number):
    return f"q{number:02d}"


def load_jmh(root, engine, suite):
    path = root / f"{engine}-{suite}.json"
    with path.open() as source:
        rows = json.load(source)
    expected = {query_name(number) for number in SUITES[suite]}
    result = {}
    for row in rows:
        expected_configuration = {
            "threads": 1,
            "forks": 1,
            "jvm": "/opt/java/openjdk/bin/java",
            "jdkVersion": "26.0.1",
            "warmupIterations": 5,
            "warmupTime": "1 s",
            "measurementIterations": 5,
            "measurementTime": "1 s",
        }
        for field, expected_value in expected_configuration.items():
            if row.get(field) != expected_value:
                raise ValueError(
                    f"{path}:{row.get('benchmark')}:{field}: "
                    f"expected {expected_value!r}, got {row.get(field)!r}")
        if "-Xmx12g" not in row.get("jvmArgs", ()):
            raise ValueError(f"{path}:{row.get('benchmark')}: missing -Xmx12g")
        match = re.search(r"\.query(\d+)$", row["benchmark"])
        if not match:
            raise ValueError(f"{path}: unexpected benchmark {row['benchmark']}")
        query = query_name(int(match.group(1)))
        if query in result:
            raise ValueError(f"{path}: duplicate {query}")
        secondary = row["secondaryMetrics"]
        metrics = {"duration_ms": finite(row["primaryMetric"]["score"], f"{path}:{query}:duration")}
        if row["primaryMetric"]["scoreUnit"] != "ms/op":
            raise ValueError(f"{path}:{query}: expected ms/op")
        for metric in EVENTS + JVM_METRICS:
            expected_unit = "#/op" if metric in EVENTS else {
                "gc.alloc.rate.norm": "B/op",
                "gc.alloc.rate": "MB/sec",
                "gc.count": "counts",
                "gc.time": "ms",
            }[metric]
            if secondary[metric]["scoreUnit"] != expected_unit:
                raise ValueError(
                    f"{path}:{query}:{metric}: expected {expected_unit}, "
                    f"got {secondary[metric]['scoreUnit']}")
            metrics[metric] = finite(secondary[metric]["score"], f"{path}:{query}:{metric}")
            if metric in EVENTS and metrics[metric] <= 0:
                raise ValueError(f"{path}:{query}:{metric}: expected positive event count")
        result[query] = metrics
    if set(result) != expected:
        raise ValueError(f"{path}: missing={sorted(expected - set(result))}, extra={sorted(set(result) - expected)}")
    return result


def parse_velox_perf(path, query):
    values = {}
    with path.open(newline="") as source:
        for row in csv.reader(source):
            if len(row) < 3 or row[2] not in EVENTS:
                continue
            values[row[2]] = finite(float(row[0]), f"{path}:{query}:{row[2]}") / REPEATS
    missing = set(EVENTS) - set(values)
    if missing:
        raise ValueError(f"{path}:{query}: missing counters {sorted(missing)}")
    return values


def load_statuses(capture_root, suite):
    status_path = capture_root / f"velox-{suite}.status.csv"
    with status_path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    statuses = {row["query"]: row for row in rows}
    if len(statuses) != len(rows):
        raise ValueError(f"{status_path}: duplicate query status")
    return status_path, statuses


def load_velox(root, suite):
    status_path, statuses = load_statuses(root, suite)
    rss_status_path, rss_statuses = load_statuses(root / RSS_RECAPTURE_DIRECTORY, suite)
    result = {}
    for number in SUITES[suite]:
        query = query_name(number)
        capture_root = root
        if (suite, query) in FULL_VELOX_RECAPTURES:
            capture_root = root / FULL_VELOX_RECAPTURES[(suite, query)]
            query_status_path, query_statuses = load_statuses(capture_root, suite)
            status = query_statuses.get(query)
        else:
            query_status_path = status_path
            status = statuses.get(query)
        if status is None or status["exit_code"] != "0":
            raise ValueError(f"{query_status_path}:{query}: incomplete status {status}")
        directory = capture_root / f"velox-{suite}"
        log_path = directory / f"{query}.log"
        text = log_path.read_text()
        duration = re.search(r"^Execution time: ([0-9.]+)(ms|s)$", text, re.MULTILINE)
        if not duration:
            raise ValueError(f"{log_path}:{query}: missing duration_ms")
        duration_scale = 1.0 if duration.group(2) == "ms" else 1000.0
        metrics = {
            "duration_ms": finite(
                float(duration.group(1)) * duration_scale, f"{log_path}:{query}:duration_ms")
        }
        patterns = {
            "managed_alloc_bytes": (r"^Managed allocation bytes: ([0-9]+)$", 1.0),
            "managed_alloc_count": (r"^Managed allocation count: ([0-9]+)$", 1.0),
            "managed_peak_bytes": (r"^Managed peak bytes: ([0-9]+)$", 1.0),
        }
        for name, (pattern, scale) in patterns.items():
            match = re.search(pattern, text, re.MULTILINE)
            if not match:
                raise ValueError(f"{log_path}:{query}: missing {name}")
            metrics[name] = finite(float(match.group(1)) * scale, f"{log_path}:{query}:{name}")
        metrics.update(parse_velox_perf(directory / f"{query}.perf.csv", query))
        peak_status_path = query_status_path
        peak_status = status
        if float(peak_status["peak_rss_kib"]) <= 0:
            peak_status_path = rss_status_path
            peak_status = rss_statuses.get(query)
            if peak_status is None or peak_status["exit_code"] != "0":
                raise ValueError(f"{rss_status_path}:{query}: incomplete RSS recapture {peak_status}")
        metrics["peak_rss_kib"] = finite(
            float(peak_status["peak_rss_kib"]), f"{peak_status_path}:{query}:peak RSS")
        if metrics["peak_rss_kib"] <= 0:
            raise ValueError(f"{peak_status_path}:{query}: non-positive peak RSS")
        result[query] = metrics
    return result


def memory_summary(path, process):
    minimum_available = math.inf
    maximum_rss = 0
    available_pattern = re.compile(r"mem_available_kib=(\d+)")
    process_pattern = re.compile(r"^\s*\d+\s+\d+\s+(\d+)\s+(\S+)")
    for line in path.read_text().splitlines():
        match = available_pattern.search(line)
        if match:
            minimum_available = min(minimum_available, int(match.group(1)))
        match = process_pattern.match(line)
        if match and match.group(2).startswith(process):
            maximum_rss = max(maximum_rss, int(match.group(1)))
    if minimum_available == math.inf or maximum_rss == 0:
        raise ValueError(f"{path}: incomplete memory samples")
    return minimum_available, maximum_rss


def minimum_available(path):
    values = [
        int(match.group(1))
        for line in path.read_text().splitlines()
        if (match := re.search(r"mem_available_kib=(\d+)", line))
    ]
    if not values:
        raise ValueError(f"{path}: missing host-memory samples")
    return min(values)


def geometric_mean(values):
    return math.exp(sum(math.log(value) for value in values) / len(values))


def compact(value):
    for scale, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= scale:
            return f"{value / scale:.3f}{suffix}"
    return f"{value:.3f}"


def counter_bundle(metrics):
    return " / ".join(compact(metrics[event]) for event in EVENTS)


def write_csv(path, data):
    fields = ["suite", "query"]
    per_engine = ["duration_ms", *EVENTS]
    fields += [f"{engine}_{metric}" for engine in ENGINES for metric in per_engine]
    fields += [
        "nitro_alloc_bytes_per_op", "nitro_alloc_bytes_per_sec", "nitro_gc_count", "nitro_gc_time_ms",
        "trino_alloc_bytes_per_op", "trino_alloc_bytes_per_sec", "trino_gc_count", "trino_gc_time_ms",
        "velox_managed_alloc_bytes", "velox_managed_alloc_count", "velox_managed_peak_bytes",
        "velox_peak_rss_kib",
    ]
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for suite in SUITES:
            for number in SUITES[suite]:
                query = query_name(number)
                row = {"suite": suite, "query": query}
                for engine in ENGINES:
                    for metric in per_engine:
                        row[f"{engine}_{metric}"] = data[suite][engine][query][metric]
                for engine in ("nitro", "trino"):
                    metrics = data[suite][engine][query]
                    row[f"{engine}_alloc_bytes_per_op"] = metrics["gc.alloc.rate.norm"]
                    row[f"{engine}_alloc_bytes_per_sec"] = metrics["gc.alloc.rate"]
                    row[f"{engine}_gc_count"] = metrics["gc.count"]
                    row[f"{engine}_gc_time_ms"] = metrics["gc.time"]
                metrics = data[suite]["velox"][query]
                for metric in ("managed_alloc_bytes", "managed_alloc_count", "managed_peak_bytes", "peak_rss_kib"):
                    row[f"velox_{metric}"] = metrics[metric]
                writer.writerow(row)


def render(root, data, memory):
    lines = [
        "# Pre-architecture operator baseline — JDK 26 — 2026-07-23",
        "",
        "This is one source-consistent, full-counter baseline captured before architectural changes. "
        "Nitro and Trino are unpinned, one JMH thread, 5×1 s warmup, 5×1 s measurement, one fresh fork, "
        "JDK 26, THP, and `-Xmx12g`. Velox is pinned to CPU 0 with one driver/I/O thread, ten repeats, "
        "and a 12 GiB managed cache.",
        "",
        "The main sweep is used except for three qualified recapture classes: restored/current-source Velox "
        "TPC-DS q14 (exact 100-row parity with Nitro), isolated Velox ClickBench q34 (the main run's high "
        "memory pressure was reproduced and determined intrinsic), and peak-RSS-only recaptures for short "
        "Velox queries that completed between the original 5 s samples. Duration, allocation, and counters "
        "for those short queries remain from the main sweep.",
        "",
        "Speedup is comparison duration divided by Nitro duration. Counter bundles are **instructions / "
        "cycles / L1D misses / L1D loads / dTLB misses / dTLB loads / branch misses / branches**, per operation.",
        "",
    ]
    summaries = []
    all_pairs = {"trino": [], "velox": []}
    for suite in SUITES:
        title = {"tpch": "TPC-H", "tpcds": "TPC-DS", "clickbench": "ClickBench"}[suite]
        lines += [
            f"## {title}",
            "",
            "| query | Nitro ms | Trino ms | Velox ms | N/T | N/V | Nitro alloc B/op | Trino alloc B/op | Velox managed alloc | Nitro counters | Trino counters | Velox counters |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
        ]
        pairs = {"trino": [], "velox": []}
        for number in SUITES[suite]:
            query = query_name(number)
            n, t, v = (data[suite][engine][query] for engine in ENGINES)
            nt, nv = t["duration_ms"] / n["duration_ms"], v["duration_ms"] / n["duration_ms"]
            pairs["trino"].append((n["duration_ms"], t["duration_ms"]))
            pairs["velox"].append((n["duration_ms"], v["duration_ms"]))
            lines.append(
                f"| {query} | {n['duration_ms']:.1f} | {t['duration_ms']:.1f} | {v['duration_ms']:.1f} | "
                f"{nt:.3f}x | {nv:.3f}x | {compact(n['gc.alloc.rate.norm'])} | "
                f"{compact(t['gc.alloc.rate.norm'])} | {compact(v['managed_alloc_bytes'])} | "
                f"{counter_bundle(n)} | {counter_bundle(t)} | {counter_bundle(v)} |"
            )
        lines.append("")
        for comparison, values in pairs.items():
            all_pairs[comparison].extend(values)
            ratios = [other / nitro for nitro, other in values]
            summaries.append((title, comparison, values, ratios))

    lines += [
        "## Duration summary",
        "",
        "| suite | comparison | queries | Nitro wins | geometric-mean speedup | Nitro sum | comparison sum | sum speedup |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for title, comparison, values, ratios in summaries:
        nsum, osum = sum(n for n, _ in values), sum(o for _, o in values)
        lines.append(
            f"| {title} | N/{comparison[0].upper()} | {len(values)} | {sum(n < o for n, o in values)} | "
            f"{geometric_mean(ratios):.3f}x | {nsum:.1f} ms | {osum:.1f} ms | {osum / nsum:.3f}x |"
        )
    for comparison, values in all_pairs.items():
        ratios = [other / nitro for nitro, other in values]
        nsum, osum = sum(n for n, _ in values), sum(o for _, o in values)
        lines.append(
            f"| Overall | N/{comparison[0].upper()} | {len(values)} | {sum(n < o for n, o in values)} | "
            f"{geometric_mean(ratios):.3f}x | {nsum:.1f} ms | {osum:.1f} ms | {osum / nsum:.3f}x |"
        )

    lines += [
        "",
        "## Hardware-counter summary",
        "",
        "Counter ratio is comparison/Nitro; values above 1.0 mean Nitro used fewer events. "
        "Wins count queries where Nitro used fewer events.",
        "",
        "| suite | comparison | counter | Nitro wins | geometric-mean ratio |",
        "|---|---|---|---:|---:|",
    ]
    for suite_key in (*SUITES, "overall"):
        suite_keys = tuple(SUITES) if suite_key == "overall" else (suite_key,)
        title = "Overall" if suite_key == "overall" else {
            "tpch": "TPC-H", "tpcds": "TPC-DS", "clickbench": "ClickBench"}[suite_key]
        for comparison in ("trino", "velox"):
            for event in EVENTS:
                pairs = [
                    (data[suite]["nitro"][query_name(number)][event],
                     data[suite][comparison][query_name(number)][event])
                    for suite in suite_keys
                    for number in SUITES[suite]
                ]
                ratios = [other / nitro for nitro, other in pairs]
                lines.append(
                    f"| {title} | N/{comparison[0].upper()} | `{event}` | "
                    f"{sum(nitro < other for nitro, other in pairs)}/{len(pairs)} | "
                    f"{geometric_mean(ratios):.3f}x |"
                )

    lines += [
        "",
        "## Memory profile",
        "",
        "JVM allocation and GC metrics are present for every query in the tables and companion CSV. "
        "JVM peak RSS and minimum host memory are sampled per suite leg; Velox peak RSS and managed-pool "
        "allocation/peak are captured per query in the CSV.",
        "",
        "| engine/suite leg | peak process RSS | minimum host MemAvailable |",
        "|---|---:|---:|",
    ]
    for suite in SUITES:
        for engine in ENGINES:
            minimum, peak = memory[(engine, suite)]
            lines.append(f"| {engine}/{suite} | {peak / 1024:.1f} MiB | {minimum / 1024:.1f} MiB |")
    lines += [
        "",
        "## Artifacts",
        "",
        f"- Machine-readable flattened data: `{root.name}/operator-architecture-baseline-jdk26-20260723.csv`",
        f"- JVM JMH JSON/log/memory files: `{root.name}/{{nitro,trino}}-{{tpch,tpcds,clickbench}}.*`",
        f"- Velox per-query logs/counters/memory: `{root.name}/velox-{{tpch,tpcds,clickbench}}/`",
        f"- Velox completion manifests: `{root.name}/velox-{{tpch,tpcds,clickbench}}.status.csv`",
        f"- Qualified full recaptures: `{root.name}/recapture-q14-restored/`, "
        f"`{root.name}/recapture-q34-clean/`",
        f"- Short-query peak-RSS recaptures: `{root.name}/{RSS_RECAPTURE_DIRECTORY}/`",
        f"- q14 correctness evidence: `{root.name}/q14-correctness/normalized/`",
        f"- Source and binary identities: `{root.name}/source-manifest.md`",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_directory", type=Path)
    args = parser.parse_args()
    root = args.artifact_directory
    data = {}
    memory = {}
    for suite in SUITES:
        data[suite] = {
            "nitro": load_jmh(root, "nitro", suite),
            "trino": load_jmh(root, "trino", suite),
            "velox": load_velox(root, suite),
        }
        for engine in ("nitro", "trino"):
            memory[(engine, suite)] = memory_summary(root / f"{engine}-{suite}.memory.log", "java")
        velox_peaks = [metrics["peak_rss_kib"] for metrics in data[suite]["velox"].values()]
        memory_paths = list((root / f"velox-{suite}").glob("q*.memory.log"))
        memory_paths += [
            root / capture / f"velox-{suite}" / f"{query}.memory.log"
            for (capture_suite, query), capture in FULL_VELOX_RECAPTURES.items()
            if capture_suite == suite
        ]
        minimum = min(minimum_available(path) for path in memory_paths)
        memory[("velox", suite)] = minimum, max(velox_peaks)
    csv_path = root / "operator-architecture-baseline-jdk26-20260723.csv"
    markdown_path = root / "operator-architecture-baseline-jdk26-20260723.md"
    write_csv(csv_path, data)
    markdown_path.write_text(render(root, data, memory))
    print(markdown_path)
    print(csv_path)


if __name__ == "__main__":
    main()
