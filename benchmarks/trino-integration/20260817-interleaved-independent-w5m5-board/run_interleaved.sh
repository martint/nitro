#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-interleaved-independent-w5m5-board
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$(</tmp/cork-benchmark.cp)"

run_one()
{
    local engine=$1
    local suite=$2
    local query=$3
    local benchmark_class=$4
    local data=$5
    local result_directory=$6
    local destination="$ROOT/raw/$engine/$suite/$query"
    local timings="$destination/$result_directory/timings.csv"
    if [[ -s "$timings" ]]; then
        return
    fi

    mkdir -p "$destination"
    local mode=CPU
    local expected=CPU
    local origin=""
    if [[ "$engine" == nitro-c ]]; then
        mode=NITRO
        expected=NITRO
        origin=" -Dcork.benchmark.required-nitro-code-source-root=$NITRO"
    fi
    JDK_JAVA_OPTIONS="-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=$expected -Dcork.benchmark.output-directory=$destination$origin" \
        taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" "$benchmark_class" run \
        -m "$mode" -W 0 -w 5 -r 5 --concurrency 1 -q "$query" --data "$data" \
        >"$destination/run.log" 2>&1
}

run_suite()
{
    local suite=$1
    local benchmark_class=$2
    local data=$3
    local result_directory=$4
    local source_timings=$5
    local ordinal=0
    while IFS=, read -r query _; do
        if (( ordinal % 2 == 0 )); then
            run_one nitro-c "$suite" "$query" "$benchmark_class" "$data" "$result_directory"
            run_one cork "$suite" "$query" "$benchmark_class" "$data" "$result_directory"
        else
            run_one cork "$suite" "$query" "$benchmark_class" "$data" "$result_directory"
            run_one nitro-c "$suite" "$query" "$benchmark_class" "$data" "$result_directory"
        fi
        ordinal=$((ordinal + 1))
    done < <(tail -n +2 "$source_timings")
}

SOURCE=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-post-q39-w5m5-board/nitro-c
run_suite clickbench io.trino.tests.benchmark.BenchmarkHiveClickBench /root/data/clickbench clickbench "$SOURCE/clickbench/timings.csv"
run_suite tpch io.trino.tests.benchmark.BenchmarkHiveTpchSf10 /root/data/tpch-parquet-sf10/sf10 tpch-sf10 "$SOURCE/tpch-sf10/timings.csv"
run_suite tpcds io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 /root/data/tpcds-parquet-sf10/sf10 tpcds-sf10 "$SOURCE/tpcds-sf10/timings.csv"
