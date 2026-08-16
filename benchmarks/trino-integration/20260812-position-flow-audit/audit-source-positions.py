#!/usr/bin/env python3

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


SOURCE_OPERATORS = {
    "ScanFilterAndProjectOperator",
    "TableScanOperator",
    "TrinoNitroAggregationSourceOperator",
    "TrinoNitroPipelineSourceOperator",
}


def records(paths):
    for path in paths:
        with path.open(errors="replace") as source:
            for line in source:
                start = line.find("operator_cpu,")
                if start < 0:
                    continue
                row = next(csv.reader([line[start:].strip()]))
                if len(row) < 13:
                    continue
                operator_type = row[5].rsplit("/", 1)[-1].split("@", 1)[0]
                if operator_type not in SOURCE_OPERATORS:
                    continue
                yield {
                    "engine": row[1],
                    "suite": row[2],
                    "query": row[3],
                    "measurement": int(row[4]),
                    "physical": int(row[10]),
                    "processed": int(row[11]),
                    "output": int(row[12]),
                }


def main():
    paths = [Path(value) for value in sys.argv[1:]]
    if not paths:
        raise SystemExit("usage: audit-source-positions.py LOG...")

    measurements = defaultdict(lambda: [0, 0, 0])
    for row in records(paths):
        totals = measurements[(row["engine"], row["suite"], row["query"], row["measurement"])]
        totals[0] += row["physical"]
        totals[1] += row["processed"]
        totals[2] += row["output"]

    medians = {}
    engine_queries = {(engine, suite, query) for engine, suite, query, _ in measurements}
    for engine, suite, query in engine_queries:
        samples = [
            totals
            for (sample_engine, sample_suite, sample_query, _), totals in measurements.items()
            if (sample_engine, sample_suite, sample_query) == (engine, suite, query)
        ]
        medians[(engine, suite, query)] = [
            int(statistics.median(sample[column] for sample in samples))
            for column in range(3)
        ]

    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow([
        "suite",
        "query",
        "trino_physical_source_positions",
        "nitro_physical_source_positions",
        "physical_ratio",
        "trino_source_processed_positions",
        "nitro_source_processed_positions",
        "trino_source_output_positions",
        "nitro_source_output_positions",
    ])
    queries = sorted({(suite, query) for _, suite, query in medians})
    for suite, query in queries:
        trino = medians.get(("trino", suite, query))
        nitro = medians.get(("nitro", suite, query))
        if trino is None or nitro is None:
            continue
        ratio = nitro[0] / trino[0] if trino[0] else ""
        writer.writerow([
            suite,
            query,
            trino[0],
            nitro[0],
            f"{ratio:.6f}" if ratio != "" else "",
            trino[1],
            nitro[1],
            trino[2],
            nitro[2],
        ])


if __name__ == "__main__":
    main()
