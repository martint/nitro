#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "usage: $0 <nitro|trino> <tpch|tpcds|clickbench> <output-directory>" >&2
    exit 2
fi

engine=$1
suite=$2
output_dir=$(realpath "$3")

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
events="instructions,cycles,L1-dcache-load-misses,L1-dcache-loads,dTLB-load-misses,dTLB-loads,branch-misses,branches"

warmup_iterations=${JMH_WARMUP_ITERATIONS:-5}
measurement_iterations=${JMH_MEASUREMENT_ITERATIONS:-5}
forks=${JMH_FORKS:-1}

monitor_memory()
{
    while kill -0 "$1" 2>/dev/null; do
        awk -v now="$(date -u +%FT%TZ)" '
            /MemAvailable:/ {available = $2}
            /SwapFree:/ {swap = $2}
            /AnonHugePages:/ {huge = $2}
            END {
                printf "%s mem_available_kib=%s swap_free_kib=%s anon_huge_pages_kib=%s\n",
                    now, available, swap, huge
            }' /proc/meminfo
        ps -eo pid,ppid,rss,comm,args --sort=-rss | head -12
        sleep 10
    done
}

parent=$$
monitor_memory "$parent" > "${prefix}.memory.log" &
monitor_pid=$!
trap 'kill "$monitor_pid" 2>/dev/null || true' EXIT

export MAVEN_OPTS=-Xmx2g
mvnd -Dmaven.gitcommitid.skip=true -pl nitro-tests exec:exec@benchmark \
    -Dbenchmark.include="$benchmark" \
    -Dbenchmark.options="-wi ${warmup_iterations} -i ${measurement_iterations} -w 1s -r 1s -f ${forks} -foe false -prof perfnorm:events=${events} -prof gc -rf json -rff ${prefix}.json -jvmArgsAppend \"-Xmx12g -XX:+UseTransparentHugePages ${data_args}\"" \
    > "${prefix}.log" 2>&1
