BEGIN {
    FS = ","
    OFS = ","
}

function remember(key) {
    if (!(key in seen)) {
        seen[key] = 1
        order[++count] = key
    }
}

$1 == "trino" || $1 == "nitro" {
    engine = $1
    suite = $2
    query = $3
    key = suite SUBSEP query
    remember(key)
    wall[engine, key] = $6 + 0
    cpu[engine, key] = $9 + 0
    next
}

$1 == "allocation" {
    engine = $2
    key = $3 SUBSEP $4
    allocation[engine, key] = $6 + 0
    next
}

$1 == "peak_memory" {
    engine = $2
    key = $3 SUBSEP $4
    query_peak[engine, key] = $6 + 0
    heap_peak[engine, key] = $8 + 0
    next
}

END {
    print "suite", "query", \
            "trino_wall_p50_ms", "nitro_wall_p50_ms", "wall_ratio", \
            "trino_cpu_p50_ms", "nitro_cpu_p50_ms", "cpu_ratio", \
            "trino_allocation_p50_mib", "nitro_allocation_p50_mib", "allocation_ratio", \
            "trino_query_peak_p50_mib", "nitro_query_peak_p50_mib", "query_peak_ratio", \
            "trino_heap_peak_p50_mib", "nitro_heap_peak_p50_mib", "heap_peak_ratio"
    for (row = 1; row <= count; row++) {
        key = order[row]
        split(key, parts, SUBSEP)
        trino_allocation = allocation["trino", key]
        nitro_allocation = allocation["nitro", key]
        allocation_ratio = (trino_allocation > 0 && nitro_allocation >= 0) ? nitro_allocation / trino_allocation : ""
        print parts[1], parts[2], \
                sprintf("%.3f", wall["trino", key]), sprintf("%.3f", wall["nitro", key]), sprintf("%.6f", wall["nitro", key] / wall["trino", key]), \
                sprintf("%.3f", cpu["trino", key]), sprintf("%.3f", cpu["nitro", key]), sprintf("%.6f", cpu["nitro", key] / cpu["trino", key]), \
                (trino_allocation >= 0 ? sprintf("%.3f", trino_allocation) : ""), (nitro_allocation >= 0 ? sprintf("%.3f", nitro_allocation) : ""), (allocation_ratio == "" ? "" : sprintf("%.6f", allocation_ratio)), \
                sprintf("%.3f", query_peak["trino", key]), sprintf("%.3f", query_peak["nitro", key]), sprintf("%.6f", query_peak["nitro", key] / query_peak["trino", key]), \
                sprintf("%.3f", heap_peak["trino", key]), sprintf("%.3f", heap_peak["nitro", key]), sprintf("%.6f", heap_peak["nitro", key] / heap_peak["trino", key])
    }
}
