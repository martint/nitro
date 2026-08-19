#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-q86-post-bloom-paired-w15m20
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$(</tmp/cork-benchmark.cp)"

run_one()
{
    local pair=$1
    local engine=$2
    local destination="$ROOT/pair-$pair/$engine"
    local timings="$destination/tpcds-sf10/timings.csv"
    if [[ -s "$timings" ]]; then
        return
    fi

    local mode=CPU
    local expected=CPU
    local origin=""
    if [[ "$engine" == nitro-c ]]; then
        mode=NITRO
        expected=NITRO
        origin=" -Dcork.benchmark.required-nitro-code-source-root=$NITRO"
    fi

    mkdir -p "$destination"
    JDK_JAVA_OPTIONS="-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=$expected -Dcork.benchmark.output-directory=$destination$origin" \
        taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 run \
        -m "$mode" -W 0 -w 15 -r 20 --concurrency 1 -q q86 --data /root/data/tpcds-parquet-sf10/sf10 \
        >"$destination/run.log" 2>&1
}

for pair in 1 2 3; do
    if (( pair % 2 == 1 )); then
        run_one "$pair" nitro-c
        run_one "$pair" cork
    else
        run_one "$pair" cork
        run_one "$pair" nitro-c
    fi
done
