/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.tpcds;

import org.weakref.nitro.trino.TrinoTpcdsSupport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class ExplainUnsupportedQueries
{
    private static final Set<String> SUPPORTED_QUERY_IDS = Set.of("10", "35", "41", "62", "69", "73", "84", "88", "90", "96", "99");
    private static final List<String> PLAN_MARKERS = List.of(
            "CorrelatedJoin",
            "EnforceSingleRow",
            "TopNRanking",
            "MarkDistinct",
            "SemiJoin",
            "Window",
            "RowNumber",
            "DistinctLimit",
            "GroupId",
            "FullJoin",
            "LeftJoin",
            "CrossJoin",
            "AssignUniqueId",
            "Aggregate",
            "TopN",
            "Sort",
            "Join",
            "Union",
            "Exchange",
            "Limit",
            "Filter",
            "Project",
            "TableScan");

    private ExplainUnsupportedQueries() {}

    public static void main(String[] args)
    {
        String schema = System.getProperty("nitro.tpcds.explain.schema", args.length > 0 ? args[0] : "sf1");
        Path outputDirectory = Path.of(System.getProperty("nitro.tpcds.explain.output", "target/tpcds-explain")).toAbsolutePath().normalize();
        List<String> queryIds = configuredQueryIds();

        try {
            Files.createDirectories(outputDirectory);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to create explain output directory: " + outputDirectory, exception);
        }

        try (TrinoTpcdsSupport support = new TrinoTpcdsSupport(schema)) {
            StringBuilder summary = new StringBuilder();
            summary.append("# Unsupported TPC-DS Query EXPLAIN Summary").append(System.lineSeparator()).append(System.lineSeparator());
            summary.append("Schema: `").append(schema).append('`').append(System.lineSeparator()).append(System.lineSeparator());

            for (String queryId : queryIds) {
                String explain = support.explainBenchmarkQuery(queryId, schema);
                write(outputDirectory.resolve("q" + queryId + ".explain.txt"), explain);

                summary.append("## Q").append(queryId).append(System.lineSeparator()).append(System.lineSeparator());
                summary.append("Plan markers: ");
                Set<String> markers = extractPlanMarkers(explain);
                if (markers.isEmpty()) {
                    summary.append("_none detected_");
                }
                else {
                    summary.append(String.join(", ", markers));
                }
                summary.append(System.lineSeparator()).append(System.lineSeparator());
                summary.append("Raw explain: `q").append(queryId).append(".explain.txt`").append(System.lineSeparator()).append(System.lineSeparator());
            }

            write(outputDirectory.resolve("SUMMARY.md"), summary.toString());
        }
    }

    private static List<String> unsupportedQueryIds()
    {
        return TpcdsQueryCatalog.benchmarkQueryIds().stream()
                .filter(queryId -> !SUPPORTED_QUERY_IDS.contains(stripLeadingZero(queryId)))
                .toList();
    }

    private static List<String> configuredQueryIds()
    {
        String configured = System.getProperty("nitro.tpcds.explain.queryIds", "").trim();
        if (configured.isEmpty()) {
            return unsupportedQueryIds();
        }
        return java.util.Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(value -> String.format("%02d", Integer.parseInt(value)))
                .toList();
    }

    private static String stripLeadingZero(String queryId)
    {
        return Integer.toString(Integer.parseInt(queryId));
    }

    private static Set<String> extractPlanMarkers(String explain)
    {
        Set<String> markers = new LinkedHashSet<>();
        for (String marker : PLAN_MARKERS) {
            if (Pattern.compile("\\b" + Pattern.quote(marker) + "\\b").matcher(explain).find()) {
                markers.add(marker);
            }
        }
        return markers;
    }

    private static void write(Path path, String content)
    {
        try {
            Files.writeString(path, content);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to write explain output: " + path, exception);
        }
    }
}
