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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class TpcdsQueryCatalog
{
    public static final String TPCDS_TRINO_ROOT_PROPERTY = "nitro.tpcds.trino.root";

    private static final Path DEFAULT_TRINO_ROOT = Path.of("..", "trino", "trino");
    private static final Path BENCHMARK_SQL_DIRECTORY = Path.of("testing", "trino-benchmark-queries", "src", "main", "resources", "sql", "trino", "tpcds");
    private static final Path PRODUCT_TEST_SQL_DIRECTORY = Path.of("testing", "trino-product-tests", "src", "main", "resources", "sql-tests", "testcases", "tpcds");

    private static final List<String> BENCHMARK_QUERY_IDS = IntStream.rangeClosed(1, 99)
            .mapToObj(value -> String.format("%02d", value))
            .toList();

    private static final List<String> REFERENCE_QUERY_IDS = Stream.concat(
                    IntStream.range(1, 100)
                            .filter(value -> value != 14)
                            .filter(value -> value != 23)
                            .filter(value -> value != 24)
                            .filter(value -> value != 39)
                            .filter(value -> value != 72)
                            .mapToObj(value -> String.format("%02d", value)),
                    Stream.of("14_1", "14_2", "23_1", "23_2", "24_2", "39_1", "39_2"))
            .sorted()
            .toList();

    private TpcdsQueryCatalog() {}

    public static boolean isAvailable()
    {
        return Files.isDirectory(benchmarkSqlDirectory()) && Files.isDirectory(productTestSqlDirectory());
    }

    public static List<String> benchmarkQueryIds()
    {
        return BENCHMARK_QUERY_IDS;
    }

    public static List<String> referenceQueryIds()
    {
        return REFERENCE_QUERY_IDS;
    }

    public static String benchmarkQuerySql(String queryId, String catalog, String schema)
    {
        String sql = readString(benchmarkSqlDirectory().resolve("q" + benchmarkResourceId(queryId) + ".sql"));
        return sql
                .replace("${database}", quoteIdentifierIfNeeded(catalog))
                .replace("${schema}", quoteIdentifierIfNeeded(schemaNameForSql(schema)));
    }

    public static String referenceQuerySql(String queryId)
    {
        return stripCommentLines(readString(productTestSqlDirectory().resolve("q" + normalizeQueryId(queryId) + ".sql")));
    }

    public static List<String> referenceResultLines(String queryId)
    {
        return readLines(productTestSqlDirectory().resolve("q" + normalizeQueryId(queryId) + ".result")).stream()
                .filter(line -> !line.startsWith("--"))
                .toList();
    }

    public static Path trinoRoot()
    {
        String configured = System.getProperty(TPCDS_TRINO_ROOT_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_TRINO_ROOT.toAbsolutePath().normalize();
        }
        return Path.of(configured).toAbsolutePath().normalize();
    }

    private static Path benchmarkSqlDirectory()
    {
        return trinoRoot().resolve(BENCHMARK_SQL_DIRECTORY);
    }

    private static Path productTestSqlDirectory()
    {
        return trinoRoot().resolve(PRODUCT_TEST_SQL_DIRECTORY);
    }

    private static String normalizeQueryId(String queryId)
    {
        return queryId.startsWith("q") ? queryId.substring(1) : queryId;
    }

    private static String benchmarkResourceId(String queryId)
    {
        return switch (normalizeQueryId(queryId)) {
            // Trino's benchmark suite splits these query templates into two concrete variants. The
            // operator board uses Q14a, scalar Q23a, the "pale" Q24a, and Q39b (whose first-month
            // coefficient-of-variation threshold is 1.5) as Q14, Q23, Q24, and Q39.
            case "14" -> "14a";
            case "23" -> "23a";
            case "24" -> "24a";
            case "39" -> "39b";
            default -> normalizeQueryId(queryId);
        };
    }

    private static String schemaNameForSql(String schema)
    {
        return "sf0.01".equals(schema) ? "tiny" : schema;
    }

    private static String quoteIdentifierIfNeeded(String identifier)
    {
        if (identifier.chars().allMatch(character ->
                character == '_' ||
                        character == '$' ||
                        Character.isLetterOrDigit(character))) {
            return identifier;
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String stripCommentLines(String sql)
    {
        return sql.lines()
                .filter(line -> !line.startsWith("--"))
                .collect(java.util.stream.Collectors.joining(System.lineSeparator()));
    }

    private static String readString(Path path)
    {
        try {
            return Files.readString(path);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to read TPC-DS resource: " + path, exception);
        }
    }

    private static List<String> readLines(Path path)
    {
        try {
            return Files.readAllLines(path);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to read TPC-DS resource: " + path, exception);
        }
    }
}
