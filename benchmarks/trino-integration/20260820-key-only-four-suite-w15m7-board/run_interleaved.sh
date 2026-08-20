#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260820-key-only-four-suite-w15m7-board
MANIFEST_ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260819-post-island-four-suite-w15m7-board/nitro-c
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork

mkdir -p "$ROOT"
exec 9>"$ROOT/run.lock"
if ! flock -n 9; then
    printf 'Another benchmark runner already owns %s\n' "$ROOT/run.lock" >&2
    exit 1
fi

reactor_classpath=$(find "$CORK" -type d \( -path '*/target/classes' -o -path '*/target/test-classes' \) -printf '%p:' | sort)
dependency_classpath=$(tr ':' '\n' < "$CORK/testing/trino-cork-benchmarks/target/benchmark-classpath.txt" | while IFS= read -r dependency; do
    relative=${dependency#/root/.m2/repository/}
    if [[ "$dependency" != *482-cork-20260707-SNAPSHOT* ]]; then
        printf '/root/.m2/repository/remote-cache/%s:' "$relative"
    fi
done)
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$CORK/testing/trino-benchmark-queries/target/classes:$CORK/core/trino-main/target/classes:$CORK/plugin/trino-hive/target/classes:$reactor_classpath$dependency_classpath"

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
        printf 'SKIP %s %s %s\n' "$engine" "$suite" "$query"
        return
    fi

    local mode=CPU
    local expected=CPU
    local origin=""
    if [[ "$engine" == nitro-c ]]; then
        mode=NITRO
        expected=NITRO
        origin=" -Dcork.benchmark.required-nitro-code-source-root=$NITRO -Dcork.benchmark.required-trino-code-source-root=$CORK/core/trino-main/target/classes"
    fi

    mkdir -p "$destination"
    printf 'START %s %s %s\n' "$engine" "$suite" "$query"
    JDK_JAVA_OPTIONS="-Xms2g -Xmx12g -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=$expected -Dcork.benchmark.output-directory=$destination$origin" \
        taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" "$benchmark_class" run \
        -m "$mode" -W 0 -w 15 -r 7 --concurrency 1 -q "$query" --data "$data" \
        >"$destination/run.log" 2>&1
    printf 'PASS %s %s %s\n' "$engine" "$suite" "$query"
}

run_pair()
{
    local ordinal=$1
    shift
    if (( ordinal % 2 == 0 )); then
        run_one nitro-c "$@"
        run_one cork "$@"
    else
        run_one cork "$@"
        run_one nitro-c "$@"
    fi
}

ordinal=0
for query_number in $(seq -w 0 42); do
    run_pair "$ordinal" clickbench "q$query_number" io.trino.tests.benchmark.BenchmarkHiveClickBench /root/data/clickbench clickbench
    ordinal=$((ordinal + 1))
done

run_manifest_suite()
{
    local suite=$1
    local benchmark_class=$2
    local data=$3
    local result_directory=$4
    local manifest="$MANIFEST_ROOT/$result_directory/timings.csv"
    while IFS=, read -r query _; do
        run_pair "$ordinal" "$suite" "$query" "$benchmark_class" "$data" "$result_directory"
        ordinal=$((ordinal + 1))
    done < <(tail -n +2 "$manifest")
}

run_manifest_suite tpch io.trino.tests.benchmark.BenchmarkHiveTpchSf10 /root/data/tpch-parquet-sf10/sf10 tpch-sf10
run_manifest_suite tpcds io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 /root/data/tpcds-parquet-sf10/sf10 tpcds-sf10

for query_number in $(seq -w 1 27); do
    run_pair "$ordinal" engine-coverage "q$query_number" io.trino.tests.benchmark.BenchmarkHiveEngineCoverage /root/data/engine-coverage-v3 engine-coverage
    ordinal=$((ordinal + 1))
done
