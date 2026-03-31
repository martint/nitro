#!/bin/zsh
set -euo pipefail

stamp=$(date +%Y%m%d-%H%M%S)
dir=/tmp/tpcds-full-benchmark-$stamp
mkdir -p "$dir"

echo "$dir"

cd /Users/martin/projects/nitro
export JAVA_TOOL_OPTIONS='-Dnitro.tpcds.parquet.path=/Users/martin/tmp/tpcds-parquet-sf10'

mvn -q exec:exec@benchmark \
    -Dbenchmark.include='org.weakref.nitro.tpcds.BenchmarkQueries.query.*$' \
    -Dbenchmark.options="-wi 5 -i 5 -w 1s -r 1s -f 0 -foe true -rf csv -rff $dir/nitro.csv" \
    > "$dir/nitro.log" 2>&1
echo $? > "$dir/nitro.exit"

mvn -q exec:exec@benchmark \
    -Dbenchmark.include='org.weakref.trino.tpcds.BenchmarkQueries.query.*$' \
    -Dbenchmark.options="-wi 5 -i 5 -w 1s -r 1s -f 0 -foe true -rf csv -rff $dir/trino.csv" \
    > "$dir/trino.log" 2>&1
echo $? > "$dir/trino.exit"
