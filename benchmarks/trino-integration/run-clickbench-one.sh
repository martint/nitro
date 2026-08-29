#!/usr/bin/env bash
set -euo pipefail

engine=${1:?engine}
query=${2:?query}
destination=${3:?destination}
warmups=${4:-15}
measurements=${5:-7}
task_concurrency=${6:-4}
profile=${7:-no}
extra_java_options=${EXTRA_JAVA_OPTIONS:-}

# ClickBench's published query identifiers are zero-based (q00--q42). Keep
# that convention at the command boundary so artifacts never need relabeling.
if [[ ! "$query" =~ ^q[0-9]{2}$ ]] || (( 10#${query#q} > 42 )); then
    printf 'ClickBench query must use the published q00--q42 identifier: %s\n' "$query" >&2
    exit 2
fi

NITRO=/home/martin/src/nitro.opt
CORK=/root/notes/cork
PROFILER=/root/.m2/repository/remote-cache/tools/profiler/async-profiler/4.3/async-profiler-4.3.jar
REACTOR_CLASSPATH=$(find "$CORK" -type d \( -path '*/target/classes' -o -path '*/target/test-classes' \) -printf '%p:' | sort)
DEPENDENCY_CLASSPATH=$(tr ':' '\n' < "$CORK/testing/trino-cork-benchmarks/target/benchmark-classpath.txt" | while IFS= read -r dependency; do
    relative=${dependency#/root/.m2/repository/}
    if [[ "$dependency" != *482-cork-20260707-SNAPSHOT* ]]; then
        printf '/root/.m2/repository/remote-cache/%s:' "$relative"
    fi
done)
CLASSPATH="$NITRO/nitro-spi/target/classes:$NITRO/nitro-engine/target/classes:$NITRO/nitro-parquet/target/classes:$NITRO/nitro-parquet-compatibility/target/classes:$NITRO/nitro-connector-loader/target/classes:$CORK/testing/trino-cork-benchmarks/target/test-classes:$CORK/testing/trino-cork-benchmarks/target/classes:$REACTOR_CLASSPATH$DEPENDENCY_CLASSPATH"

mode=CPU
expected=CPU
origin=""
if [[ "$engine" == nitro-c ]]; then
    mode=NITRO
    expected=NITRO
    origin=" -Dcork.benchmark.required-nitro-code-source-root=$NITRO -Dcork.benchmark.required-trino-code-source-root=$CORK/core/trino-main/target/classes"
fi
agent=""
if [[ "$profile" == yes ]]; then
    agent=" -javaagent:$PROFILER=start,event=cpu,file=$destination/profile-%p.collapsed,collapsed"
fi

mkdir -p "$destination"
JDK_JAVA_OPTIONS="-Xms2g -Xmx12g -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:-HeapDumpOnOutOfMemoryError -XX:ActiveProcessorCount=6 -Dcork.benchmark.task-concurrency=$task_concurrency -Dcork.benchmark.expected-mode=$expected -Dcork.benchmark.output-directory=$destination$origin$agent $extra_java_options" \
    taskset -c 0-5 /opt/java/openjdk/bin/java -cp "$CLASSPATH" io.trino.tests.benchmark.BenchmarkHiveClickBench run \
    -m "$mode" -W 0 -w "$warmups" -r "$measurements" --concurrency 1 -q "$query" --data /root/data/clickbench \
    >"$destination/run.log" 2>&1
