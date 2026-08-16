#!/usr/bin/env python3

import csv
import math
import statistics
from pathlib import Path


ROOT = Path(__file__).parent
SUITES = {
    "tpch": "tpch-sf10",
    "tpcds": "tpcds-sf10",
    "clickbench": "clickbench",
}
METRICS = {
    "wall": "elapsed",
    "cpu": "cpu",
    "allocation": "allocated_bytes",
    "peak_memory": "peak_memory_bytes",
}


def geomean(values):
    values = [value for value in values if value > 0 and math.isfinite(value)]
    return math.exp(sum(map(math.log, values)) / len(values))


def load(engine, suite, result_directory):
    path = ROOT / f"{engine}-{suite}" / result_directory / "timings.csv"
    with path.open(newline="") as source:
        return {
            row["query"]: {
                metric: statistics.median(
                    float(row[f"run_{run}_{field}"]) for run in (1, 2, 3))
                for metric, field in METRICS.items()
            }
            for row in csv.DictReader(source)
        }


rows = []
for suite, result_directory in SUITES.items():
    nitro = load("nitro-c", suite, result_directory)
    cork = load("cork", suite, result_directory)
    if nitro.keys() != cork.keys():
        raise RuntimeError(f"{suite} query sets differ")
    for query in sorted(nitro):
        row = {"suite": suite, "query": query}
        for metric in METRICS:
            row[f"nitro_c_{metric}"] = nitro[query][metric]
            row[f"cork_{metric}"] = cork[query][metric]
            row[f"nitro_c_over_cork_{metric}"] = (
                nitro[query][metric] / cork[query][metric]
                if cork[query][metric] > 0
                else math.nan)
        rows.append(row)

with (ROOT / "cork-pair-query-board.csv").open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

summaries = []
for suite in (*SUITES, "overall"):
    matching = rows if suite == "overall" else [row for row in rows if row["suite"] == suite]
    summary = {"suite": suite, "queries": len(matching)}
    for metric in METRICS:
        ratio = f"nitro_c_over_cork_{metric}"
        valid = [row for row in matching if row[ratio] > 0 and math.isfinite(row[ratio])]
        summary[f"{metric}_geomean"] = geomean(row[ratio] for row in valid)
        summary[f"{metric}_sum_ratio"] = (
            sum(row[f"nitro_c_{metric}"] for row in valid) /
            sum(row[f"cork_{metric}"] for row in valid))
        summary[f"{metric}_wins"] = sum(row[ratio] < 1 for row in valid)
        summary[f"{metric}_count"] = len(valid)
    summaries.append(summary)

with (ROOT / "cork-pair-summary.csv").open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=list(summaries[0]))
    writer.writeheader()
    writer.writerows(summaries)

print(f"generated {len(rows)} Nitro-C/Cork query pairs")
