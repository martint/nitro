#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <tpch|tpcds|clickbench> <output-directory>" >&2
    exit 2
fi

suite=$1
output_dir=$2
velox_root=/root/notes/velox/velox

case "$suite" in
    tpch)
        binary="$velox_root/_build/release/velox/benchmarks/tpch/velox_tpch_benchmark"
        data_path=/root/data/tpch-parquet-sf10/sf10
        first=1
        last=22
        extra=(--filters_as_node=true)
        ;;
    tpcds)
        binary="$velox_root/_build/release/velox/benchmarks/tpcds/velox_nitro_tpcds_benchmark"
        data_path=/root/data/tpcds-parquet-sf10/sf10
        first=1
        last=99
        extra=()
        ;;
    clickbench)
        binary="$velox_root/_build/release/velox/benchmarks/clickbench/velox_clickbench_benchmark"
        data_path=/root/data/clickbench
        first=0
        last=43
        extra=()
        ;;
    *) echo "unknown suite: $suite" >&2; exit 2 ;;
esac

mkdir -p "$output_dir/velox-$suite"
summary="$output_dir/velox-${suite}.status.csv"
echo 'query,exit_code,mem_available_kib,peak_rss_kib' > "$summary"

monitor_memory()
{
    local process=$1
    local output=$2
    while kill -0 "$process" 2>/dev/null; do
        awk -v now="$(date -u +%FT%TZ)" '
            /MemAvailable:/ {available = $2}
            /SwapFree:/ {swap = $2}
            /AnonHugePages:/ {huge = $2}
            END {
                printf "%s mem_available_kib=%s swap_free_kib=%s anon_huge_pages_kib=%s\n",
                    now, available, swap, huge
            }' /proc/meminfo
        ps -eo pid,ppid,rss,comm,args --sort=-rss | head -12
        sleep 5
    done > "$output"
}

monitor_peak_rss()
{
    local process=$1
    local output=$2
    while kill -0 "$process" 2>/dev/null; do
        if [[ -r "/proc/$process/task/$process/children" ]]; then
            for child in $(< "/proc/$process/task/$process/children"); do
                awk '/^VmHWM:/{print $2}' "/proc/$child/status" 2>/dev/null || true
            done
        fi
        sleep 0.05
    done > "$output"
}

queries=${VELOX_QUERIES:-$(seq "$first" "$last")}

for query in $queries; do
    available=$(awk '/MemAvailable:/{print $2}' /proc/meminfo)
    if (( available < 8 * 1024 * 1024 )); then
        echo "stopping before q$query: only ${available} KiB available" >&2
        exit 75
    fi

    q=$(printf '%02d' "$query")
    log="$output_dir/velox-$suite/q${q}.log"
    counters="$output_dir/velox-$suite/q${q}.perf.csv"
    memory="$output_dir/velox-$suite/q${q}.memory.log"
    peak_memory="$output_dir/velox-$suite/q${q}.peak-rss-kib.txt"
    set +e
    /usr/local/bin/perf stat -x, -o "$counters" \
        -e instructions,cycles,L1-dcache-load-misses,L1-dcache-loads,dTLB-load-misses,dTLB-loads,branch-misses,branches \
        -- taskset -c 0 env LD_LIBRARY_PATH=/root/notes/velox/miniconda/lib:/root/notes/velox/boostlibs:/root/notes/velox/local/lib:/usr/local/lib \
        "$binary" \
        --data_path="$data_path" \
        --run_query_verbose="$query" \
        --num_drivers=1 \
        --num_io_threads=1 \
        --num_splits_per_file=1 \
        --num_repeats=10 \
        --cache_gb=12 \
        "${extra[@]}" \
        > "$log" 2>&1 &
    perf_pid=$!
    monitor_memory "$perf_pid" "$memory" &
    monitor_pid=$!
    monitor_peak_rss "$perf_pid" "$peak_memory" &
    peak_monitor_pid=$!
    wait "$perf_pid"
    status=$?
    kill "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
    kill "$peak_monitor_pid" 2>/dev/null || true
    wait "$peak_monitor_pid" 2>/dev/null || true
    set -e
    if ! grep -q '^Execution time:' "$log"; then
        status=90
    fi
    available=$(awk '/MemAvailable:/{print $2}' /proc/meminfo)
    sampled_peak_rss=$(awk 'NR > 1 && $4 ~ /^velox_/ {if ($3 > peak) peak = $3} END {print peak + 0}' "$memory")
    measured_peak_rss=$(awk '{if ($1 > peak) peak = $1} END {print peak + 0}' "$peak_memory")
    peak_rss=$(( sampled_peak_rss > measured_peak_rss ? sampled_peak_rss : measured_peak_rss ))
    echo "q$q,$status,$available,$peak_rss" >> "$summary"
done
