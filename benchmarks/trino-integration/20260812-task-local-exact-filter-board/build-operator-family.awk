BEGIN {
    FS = ","
    OFS = ","
    family_order[1] = "Scan / filter / project / aggregation"
    family_order[2] = "Join & build"
    family_order[3] = "Sort / TopN / window"
    family_order[4] = "Exchange & output"
    family_order[5] = "Set / row expansion / control"
    family_order[6] = "Other"
}

function family(operator, type) {
    type = operator
    sub(/^.*\//, "", type)
    sub(/@.*$/, "", type)
    if (type ~ /(Join|HashBuild|HashBuilder|DynamicFilter)/) {
        return family_order[2]
    }
    if (type ~ /(Sort|OrderBy|TopN|Window|Rank)/) {
        return family_order[3]
    }
    if (type ~ /(Exchange|Output|Merge)/) {
        return family_order[4]
    }
    if (type ~ /(Scan|Source|Filter|Project|Aggregation)/) {
        return family_order[1]
    }
    if (type ~ /(Distinct|GroupId|Unnest|RowNumber|AssignUniqueId|EnforceSingleRow)/) {
        return family_order[5]
    }
    return family_order[6]
}

function median3(a, b, c, tmp) {
    if (a > b) { tmp = a; a = b; b = tmp }
    if (b > c) { tmp = b; b = c; c = tmp }
    if (a > b) { tmp = a; a = b; b = tmp }
    return b
}

$1 == "operator_cpu" {
    engine = $2
    suite = $3
    query = $4
    iteration = $5 + 0
    group = family($6)
    cpu_ms[engine, suite, query, group, iteration] += $8 + $9 + $10
    query_seen[suite, query] = 1
    suite_seen[suite] = 1
}

END {
    for (key in query_seen) {
        split(key, parts, SUBSEP)
        suite = parts[1]
        query = parts[2]
        for (family_index = 1; family_index <= 6; family_index++) {
            group = family_order[family_index]
            trino = median3(cpu_ms["trino", suite, query, group, 0], cpu_ms["trino", suite, query, group, 1], cpu_ms["trino", suite, query, group, 2])
            nitro = median3(cpu_ms["nitro", suite, query, group, 0], cpu_ms["nitro", suite, query, group, 1], cpu_ms["nitro", suite, query, group, 2])
            suite_trino[suite, group] += trino
            suite_nitro[suite, group] += nitro
            suite_total_trino[suite] += trino
            suite_total_nitro[suite] += nitro
            global_trino[group] += trino
            global_nitro[group] += nitro
            global_total_trino += trino
            global_total_nitro += nitro
        }
    }

    print "suite", "operator_family", "trino_cpu_s", "nitro_cpu_s", "ratio", "cpu_saved_s", "share_of_suite_savings"
    for (suite in suite_seen) {
        suite_saved = suite_total_trino[suite] - suite_total_nitro[suite]
        for (family_index = 1; family_index <= 6; family_index++) {
            group = family_order[family_index]
            trino = suite_trino[suite, group]
            nitro = suite_nitro[suite, group]
            saved = trino - nitro
            print suite, group, sprintf("%.6f", trino / 1000), sprintf("%.6f", nitro / 1000), \
                    (trino == 0 ? "" : sprintf("%.6f", nitro / trino)), sprintf("%.6f", saved / 1000), sprintf("%.6f", saved / suite_saved)
        }
    }
    global_saved = global_total_trino - global_total_nitro
    for (family_index = 1; family_index <= 6; family_index++) {
        group = family_order[family_index]
        trino = global_trino[group]
        nitro = global_nitro[group]
        saved = trino - nitro
        print "overall", group, sprintf("%.6f", trino / 1000), sprintf("%.6f", nitro / 1000), \
                (trino == 0 ? "" : sprintf("%.6f", nitro / trino)), sprintf("%.6f", saved / 1000), sprintf("%.6f", saved / global_saved)
    }
}
