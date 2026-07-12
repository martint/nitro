#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "usage: $0 <nitro|trino> <tpch|tpcds|clickbench> <output-directory>" >&2
    exit 2
fi

engine=$1
suite=$2
output_dir=$3

case "$engine" in
    nitro|trino) ;;
    *) echo "unknown engine: $engine" >&2; exit 2 ;;
esac

case "$suite" in
    tpch)
        data_args="-Dnitro.tpch.parquet.path=/root/data/tpch-parquet-sf10 -Dnitro.tpch.parquet.schema=sf10"
        ;;
    tpcds)
        data_args="-Dnitro.tpcds.parquet.path=/root/data/tpcds-parquet-sf10 -Dnitro.tpcds.parquet.schema=sf10"
        ;;
    clickbench)
        data_args="-Dnitro.clickbench.hits.path=/root/data/clickbench"
        ;;
    *) echo "unknown suite: $suite" >&2; exit 2 ;;
esac

mkdir -p "$output_dir"
prefix="$output_dir/${engine}-${suite}"
benchmark="org\\.weakref\\.${engine}\\.${suite}\\.BenchmarkQueries\\.query.*$"

monitor_memory()
{
    while kill -0 "$1" 2>/dev/null; do
        awk -v now="$(date -u +%FT%TZ)" '/MemAvailable:/{printf "%s mem_available_kib=%s\n", now, $2}' /proc/meminfo
        sleep 10
    done
}

parent=$$
monitor_memory "$parent" > "${prefix}.memory.log" &
monitor_pid=$!
trap 'kill "$monitor_pid" 2>/dev/null || true' EXIT

export MAVEN_OPTS=-Xmx2g
mvnd -Dmaven.gitcommitid.skip=true exec:exec@benchmark \
    -Dbenchmark.include="$benchmark" \
    -Dbenchmark.options="-wi 3 -i 5 -w 1s -r 1s -f 1 -foe false -prof perfnorm -prof gc -rf json -rff ${prefix}.json -jvmArgsAppend \"-Xmx12g -XX:+UseTransparentHugePages ${data_args}\"" \
    > "${prefix}.log" 2>&1
