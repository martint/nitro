#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260820-key-only-functional-pass
MANIFEST_ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260819-post-island-four-suite-w15m7-board/nitro-c
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork

mkdir -p "$ROOT"
exec 9>"$ROOT/run.lock"
flock -n 9 || exit 1

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
    local suite=$1
    local query=$2
    local benchmark_class=$3
    local data=$4
    local result_directory=$5
    local destination="$ROOT/$suite/$query"
    local timings="$destination/$result_directory/timings.csv"
    if [[ -s "$timings" ]]; then
        printf 'SKIP %s %s\n' "$suite" "$query"
        return
    fi

    mkdir -p "$destination"
    printf 'START %s %s\n' "$suite" "$query"
    if JDK_JAVA_OPTIONS="-Xms2g -Xmx12g -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=NITRO -Dcork.benchmark.output-directory=$destination -Dcork.benchmark.required-nitro-code-source-root=$NITRO -Dcork.benchmark.required-trino-code-source-root=$CORK/core/trino-main/target/classes" \
            taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" "$benchmark_class" run \
            -m NITRO -W 0 -w 0 -r 1 --concurrency 1 -q "$query" --data "$data" \
            >"$destination/run.log" 2>&1; then
        printf 'PASS %s %s\n' "$suite" "$query"
    else
        printf 'FAIL %s %s\n' "$suite" "$query"
        return 1
    fi
}

run_manifest_suite()
{
    local suite=$1
    local benchmark_class=$2
    local data=$3
    local result_directory=$4
    local manifest="$MANIFEST_ROOT/$result_directory/timings.csv"
    while IFS=, read -r query _; do
        run_one "$suite" "$query" "$benchmark_class" "$data" "$result_directory"
    done < <(tail -n +2 "$manifest")
}

# ClickBench's published query identifiers are q00--q42.
for query_number in $(seq -w 0 42); do
    run_one clickbench "q$query_number" io.trino.tests.benchmark.BenchmarkHiveClickBench /root/data/clickbench clickbench
done
run_manifest_suite tpch io.trino.tests.benchmark.BenchmarkHiveTpchSf10 /root/data/tpch-parquet-sf10/sf10 tpch-sf10
run_manifest_suite tpcds io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 /root/data/tpcds-parquet-sf10/sf10 tpcds-sf10

for query_number in $(seq -w 1 27); do
    run_one engine-coverage "q$query_number" io.trino.tests.benchmark.BenchmarkHiveEngineCoverage /root/data/engine-coverage-v3 engine-coverage
done
