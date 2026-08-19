#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/martin/src/nitro.opt/benchmarks/trino-integration/20260819-post-island-four-suite-w15m7-board

merge_suite()
{
    local engine=$1
    local suite=$2
    local result_directory=$3
    local destination="$ROOT/$engine/$result_directory"
    mkdir -p "$destination/explain"

    local first=true
    local source
    for source in "$ROOT/raw/$engine/$suite"/*/"$result_directory"/timings.csv; do
        if [[ "$first" == true ]]; then
            head -n 1 "$source" >"$destination/timings.csv"
            first=false
        fi
        tail -n +2 "$source" >>"$destination/timings.csv"
        local query
        query=$(basename "$(dirname "$(dirname "$source")")")
        cp "$(dirname "$source")/explain/$query.explain-analyze.txt" "$destination/explain/"
    done
}

for engine in nitro-c cork; do
    merge_suite "$engine" clickbench clickbench
    merge_suite "$engine" tpch tpch-sf10
    merge_suite "$engine" tpcds tpcds-sf10
    merge_suite "$engine" engine-coverage engine-coverage
done
