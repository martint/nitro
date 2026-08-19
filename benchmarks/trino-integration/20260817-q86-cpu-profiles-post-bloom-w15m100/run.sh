#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-q86-cpu-profiles-post-bloom-w15m100
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork
PROFILER=/root/.m2/repository/tools/profiler/async-profiler/4.3/async-profiler-4.3.jar
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$(</tmp/cork-benchmark.cp)"

run_one()
{
    local engine=$1
    local destination="$ROOT/$engine"
    local mode=CPU
    local expected=CPU
    local origin=""
    if [[ "$engine" == nitro-c ]]; then
        mode=NITRO
        expected=NITRO
        origin=" -Dcork.benchmark.required-nitro-code-source-root=$NITRO"
    fi

    mkdir -p "$destination"
    JDK_JAVA_OPTIONS="-javaagent:$PROFILER=start,event=cpu,file=$destination/profile-%p.collapsed,collapsed -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=$expected -Dcork.benchmark.output-directory=$destination$origin" \
        taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 run \
        -m "$mode" -W 0 -w 15 -r 100 --concurrency 1 -q q86 --data /root/data/tpcds-parquet-sf10/sf10 \
        >"$destination/run.log" 2>&1
}

run_one nitro-c
run_one cork
