#!/usr/bin/env bash
set -euo pipefail

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
echo 'query,exit_code,mem_available_kib' > "$summary"

for query in $(seq "$first" "$last"); do
    available=$(awk '/MemAvailable:/{print $2}' /proc/meminfo)
    if (( available < 8 * 1024 * 1024 )); then
        echo "stopping before q$query: only ${available} KiB available" >&2
        exit 75
    fi

    q=$(printf '%02d' "$query")
    log="$output_dir/velox-$suite/q${q}.log"
    counters="$output_dir/velox-$suite/q${q}.perf.csv"
    set +e
    perf stat -x, -o "$counters" \
        -e instructions,cycles,L1-dcache-load-misses,dTLB-load-misses,dTLB-loads,branch-misses \
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
        > "$log" 2>&1
    status=$?
    set -e
    if ! grep -q '^Execution time:' "$log"; then
        status=90
    fi
    available=$(awk '/MemAvailable:/{print $2}' /proc/meminfo)
    echo "q$q,$status,$available" >> "$summary"
done
