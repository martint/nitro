#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260817-q86-value-id-mask-candidate-w15m100
NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$(</tmp/cork-benchmark.cp)"

mkdir -p "$ROOT"
JDK_JAVA_OPTIONS="-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=4 -Dcork.benchmark.expected-mode=NITRO -Dcork.benchmark.output-directory=$ROOT -Dcork.benchmark.required-nitro-code-source-root=$NITRO" \
    taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" io.trino.tests.benchmark.BenchmarkHiveTpcdsSf10 run \
    -m NITRO -W 0 -w 15 -r 100 --concurrency 1 -q q86 --data /root/data/tpcds-parquet-sf10/sf10 \
    >"$ROOT/run.log" 2>&1
