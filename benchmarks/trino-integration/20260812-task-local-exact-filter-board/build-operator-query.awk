BEGIN {
    FS = ","
    OFS = ","
}

function median3(a, b, c, tmp) {
    if (a > b) { tmp = a; a = b; b = tmp }
    if (b > c) { tmp = b; b = c; c = tmp }
    if (a > b) { tmp = a; a = b; b = tmp }
    return b
}

function short_suite(suite) {
    if (suite == "tpch-parquet-sf10") return "tpch"
    if (suite == "tpcds-parquet-sf10") return "tpcds"
    return suite
}

$1 == "operator_cpu" {
    engine = $2
    suite = $3
    query = $4
    iteration = $5 + 0
    cpu_ms[engine, suite, query, iteration] += $8 + $9 + $10
    query_seen[suite, query] = 1
}

END {
    print "suite", "query", "nitro_ms", "trino_ms", "ratio"
    for (key in query_seen) {
        split(key, parts, SUBSEP)
        suite = parts[1]
        query = parts[2]
        trino = median3(cpu_ms["trino", suite, query, 0], cpu_ms["trino", suite, query, 1], cpu_ms["trino", suite, query, 2])
        nitro = median3(cpu_ms["nitro", suite, query, 0], cpu_ms["nitro", suite, query, 1], cpu_ms["nitro", suite, query, 2])
        print short_suite(suite), query, sprintf("%.6f", nitro), sprintf("%.6f", trino), \
                (trino == 0 ? "" : sprintf("%.6f", nitro / trino))
    }
}
