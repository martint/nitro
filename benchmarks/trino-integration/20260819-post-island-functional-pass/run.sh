#!/usr/bin/env bash
set -uo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260819-post-island-functional-pass
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork
MANIFEST_ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-auto-g1-w15m7-board/nitro-c

reactor_classpath=$(find "$CORK" -type d \( -path '*/target/classes' -o -path '*/target/test-classes' \) -printf '%p:' | sort)
dependency_classpath=$(tr ':' '\n' < "$CORK/testing/trino-cork-benchmarks/target/benchmark-classpath.txt" | while IFS= read -r dependency; do
    relative=${dependency#/root/.m2/repository/}
    if [[ "$dependency" != *482-cork-20260707-SNAPSHOT* ]]; then
        printf '/root/.m2/repository/remote-cache/%s:' "$relative"
    fi
done)
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$CORK/testing/trino-benchmark-queries/target/classes:$CORK/core/trino-main/target/classes:$CORK/plugin/trino-hive/target/classes:$reactor_classpath$dependency_classpath"

mkdir -p "$ROOT"
status_file="$ROOT/status.csv"
if [[ ! -e "$status_file" ]]; then
    printf 'suite,query,status,elapsed_seconds\n' > "$status_file"
fi

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
        printf 'SKIP %s %s (already passed)\n' "$suite" "$query"
        return 0
    fi

    mkdir -p "$destination"
    local started=$SECONDS
    printf 'START %s %s\n' "$suite" "$query"
    if JDK_JAVA_OPTIONS="-Xms2g -Xmx12g -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=NITRO -Dcork.benchmark.output-directory=$destination -Dcork.benchmark.required-nitro-code-source-root=$NITRO -Dcork.benchmark.required-trino-code-source-root=$CORK/core/trino-main/target/classes" \
            taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" "$benchmark_class" run \
            -m NITRO -W 0 -w 0 -r 1 --concurrency 1 -q "$query" --data "$data" \
            > "$destination/run.log" 2>&1; then
        printf '%s,%s,PASS,%s\n' "$suite" "$query" "$((SECONDS - started))" >> "$status_file"
        printf 'PASS %s %s (%ss)\n' "$suite" "$query" "$((SECONDS - started))"
        return 0
    fi

    printf '%s,%s,FAIL,%s\n' "$suite" "$query" "$((SECONDS - started))" >> "$status_file"
    printf 'FAIL %s %s (%ss)\n' "$suite" "$query" "$((SECONDS - started))"
    tail -40 "$destination/run.log"
    return 1
}

run_manifest_suite()
{
    local suite=$1
    local benchmark_class=$2
    local data=$3
    local result_directory=$4
    local manifest="$MANIFEST_ROOT/$result_directory/timings.csv"
    while IFS=, read -r query _; do
        run_one "$suite" "$query" "$benchmark_class" "$data" "$result_directory" || true
    done < <(tail -n +2 "$manifest")
}

run_manifest_suite clickbench io.trino.tests.benchmark.BenchmarkHiveClickBench /root/data/clickbench clickbench
run_manifest_suite tpch io.trino.tests.benchmark.BenchmarkHiveTpchSf10 /root/data/tpch-parquet-sf10/sf10 tpch-sf10
run_manifest_suite tpcds io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 /root/data/tpcds-parquet-sf10/sf10 tpcds-sf10

for query_number in $(seq -w 1 27); do
    run_one engine-coverage "q$query_number" io.trino.tests.benchmark.BenchmarkHiveEngineCoverage /root/data/engine-coverage-v3 engine-coverage || true
done

latest_status_file="$ROOT/status-latest.csv"
awk -F, '
    NR == 1 {next}
    {key = $1 FS $2; order[++entries] = key; latest[key] = $0}
    END {
        print "suite,query,status,elapsed_seconds"
        for (entry = 1; entry <= entries; entry++) {
            key = order[entry]
            if (!written[key]++) {
                positions[++count] = key
            }
        }
        for (position = 1; position <= count; position++) {
            print latest[positions[position]]
        }
    }
' "$status_file" > "$latest_status_file"
passes=$(awk -F, '$3 == "PASS" {count++} END {print count + 0}' "$latest_status_file")
failures=$(awk -F, '$3 == "FAIL" {count++} END {print count + 0}' "$latest_status_file")
printf 'COMPLETE passes=%s failures=%s\n' "$passes" "$failures"
(( failures == 0 ))
