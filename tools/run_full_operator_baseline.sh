#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <output-directory>" >&2
    exit 2
fi

output_dir=$1
mkdir -p "$output_dir"

minimum_available_kib=$((16 * 1024 * 1024))

check_memory()
{
    local phase=$1
    local available
    available=$(awk '/MemAvailable:/{print $2}' /proc/meminfo)
    if (( available < minimum_available_kib )); then
        echo "refusing to start $phase: only ${available} KiB available" >&2
        exit 75
    fi
    printf '%s phase=%s mem_available_kib=%s\n' "$(date -u +%FT%TZ)" "$phase" "$available" |
        tee -a "$output_dir/phases.log"
}

export JMH_WARMUP_ITERATIONS=${JMH_WARMUP_ITERATIONS:-5}
export JMH_MEASUREMENT_ITERATIONS=${JMH_MEASUREMENT_ITERATIONS:-5}
export JMH_FORKS=${JMH_FORKS:-1}

for suite in tpch tpcds clickbench; do
    for engine in nitro trino; do
        phase="${engine}-${suite}"
        check_memory "$phase"
        tools/run_jvm_operator_counter_sweep.sh "$engine" "$suite" "$output_dir"
    done
done

for suite in tpch tpcds clickbench; do
    phase="velox-${suite}"
    check_memory "$phase"
    tools/run_velox_operator_counter_sweep.sh "$suite" "$output_dir"
done

check_memory complete
