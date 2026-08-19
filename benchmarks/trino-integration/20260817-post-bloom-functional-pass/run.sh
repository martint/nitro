#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-post-bloom-functional-pass
SOURCE=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-interleaved-independent-w5m5-board/nitro-c
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$(</tmp/cork-benchmark.cp)"

run_one()
{
    local suite=$1
    local query=$2
    local benchmark_class=$3
    local data=$4
    local result_directory=$5
    local destination="$ROOT/$suite/$query"
    local timings="$destination/$result_directory/timings.csv"
    if [[ -s "$timings" ]]; then
        return
    fi

    mkdir -p "$destination"
    JDK_JAVA_OPTIONS="-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=NITRO -Dcork.benchmark.output-directory=$destination -Dcork.benchmark.required-nitro-code-source-root=$NITRO" \
        taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" "$benchmark_class" run \
        -m NITRO -W 0 -w 0 -r 1 --concurrency 1 -q "$query" --data "$data" \
        >"$destination/run.log" 2>&1
}

run_suite()
{
    local suite=$1
    local benchmark_class=$2
    local data=$3
    local result_directory=$4
    local source_timings=$5
    while IFS=, read -r query _; do
        run_one "$suite" "$query" "$benchmark_class" "$data" "$result_directory"
    done < <(tail -n +2 "$source_timings")
}

run_suite clickbench io.trino.tests.benchmark.BenchmarkHiveClickBench /root/data/clickbench clickbench "$SOURCE/clickbench/timings.csv"
run_suite tpch io.trino.tests.benchmark.BenchmarkHiveTpchSf10 /root/data/tpch-parquet-sf10/sf10 tpch-sf10 "$SOURCE/tpch-sf10/timings.csv"
run_suite tpcds io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 /root/data/tpcds-parquet-sf10/sf10 tpcds-sf10 "$SOURCE/tpcds-sf10/timings.csv"
