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
package org.weakref.nitro;

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.tpcds.TpcdsParquetTables;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;
import org.weakref.nitro.trino.TrinoTpcdsSupport;

import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestTpcdsParquetQueries
{
    @Test
    void testQuery41MatchesTrinoOperatorAssembly()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = TpcdsParquetSupport.query41(new Allocator(), primitiveRegistry, tables)) {
            nitroRows = OperatorAssertions.OperatorAssert.toRows(query);
        }

        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            MaterializedResult result = support.query41(tables);
            assertThat(result.getMaterializedRows().stream()
                    .map(row -> new org.weakref.nitro.data.Row(row.getFields().stream()
                            .map(field -> field instanceof String value ? normalize(value) : field)
                            .toArray()))
                    .toList())
                    .containsExactlyElementsOf(nitroRows.stream()
                            .map(row -> new org.weakref.nitro.data.Row(java.util.Arrays.stream(row.values())
                                    .map(value -> value instanceof String string ? normalize(string) : value)
                                    .toArray()))
                            .toList());
        }
    }

    @Test
    void testNitroQuery41MatchesTrinoSqlReference()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        List<String> nitroRows;
        try (Operator query = TpcdsParquetSupport.query41(new Allocator(), primitiveRegistry, tables)) {
            nitroRows = OperatorAssertions.OperatorAssert.toRows(query).stream()
                    .map(row -> normalize((String) row.values()[0]))
                    .toList();
        }

        try (TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery("41", "sf10");
            assertThat(nitroRows)
                    .containsExactlyElementsOf(sqlResult.getMaterializedRows().stream()
                            .map(row -> normalize(Objects.toString(row.getField(0))))
                            .toList());
        }
    }

    @Test
    void testQuery41MatchesTrinoSqlReference()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        try (TrinoTpcdsParquetSupport operatorSupport = new TrinoTpcdsParquetSupport();
                TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            MaterializedResult operatorResult = operatorSupport.query41(tables);
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery("41", "sf10");
            assertThat(operatorResult.getMaterializedRows().stream()
                    .map(row -> normalize((String) row.getField(0)))
                    .toList())
                    .containsExactlyElementsOf(sqlResult.getMaterializedRows().stream()
                            .map(row -> normalize(Objects.toString(row.getField(0))))
                            .toList());
        }
    }

    @Test
    void testQuery62MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("62", tables -> TpcdsParquetSupport.query62(new Allocator(), tables), support -> support.query62(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery62MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("62", tables -> TpcdsParquetSupport.query62(new Allocator(), tables));
    }

    @Test
    void testQuery62MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("62", support -> support.query62(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery96MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("96", tables -> TpcdsParquetSupport.query96(new Allocator(), tables), support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery96MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("96", tables -> TpcdsParquetSupport.query96(new Allocator(), tables));
    }

    @Test
    void testQuery96MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("96", support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery99MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("99", tables -> TpcdsParquetSupport.query99(new Allocator(), tables), support -> support.query99(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery99MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("99", tables -> TpcdsParquetSupport.query99(new Allocator(), tables));
    }

    @Test
    void testQuery99MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("99", support -> support.query99(TpcdsParquetTables.requiredActual("sf10")));
    }

    private static void assertOperatorMatches(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = nitroQuery.apply(tables)) {
            nitroRows = normalizeNitroRows(OperatorAssertions.OperatorAssert.toRows(query));
        }

        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            assertThat(normalizeTrinoRows(trinoQuery.apply(support)))
                    .as("TPC-DS Q%s operator assembly result", queryId)
                    .containsExactlyElementsOf(nitroRows);
        }
    }

    private static void assertNitroMatchesSql(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = nitroQuery.apply(tables)) {
            nitroRows = normalizeNitroRows(OperatorAssertions.OperatorAssert.toRows(query));
        }

        try (TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            assertThat(nitroRows)
                    .as("TPC-DS Q%s Nitro vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId, "sf10")));
        }
    }

    private static void assertTrinoOperatorMatchesSql(String queryId, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        try (TrinoTpcdsParquetSupport operatorSupport = new TrinoTpcdsParquetSupport();
                TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            assertThat(normalizeTrinoRows(trinoQuery.apply(operatorSupport)))
                    .as("TPC-DS Q%s Trino operator vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId, "sf10")));
        }
    }

    private static List<org.weakref.nitro.data.Row> normalizeNitroRows(List<org.weakref.nitro.data.Row> rows)
    {
        return rows.stream()
                .map(row -> new org.weakref.nitro.data.Row(java.util.Arrays.stream(row.values())
                        .map(TestTpcdsParquetQueries::normalizeValue)
                        .toArray()))
                .toList();
    }

    private static List<org.weakref.nitro.data.Row> normalizeTrinoRows(MaterializedResult result)
    {
        return result.getMaterializedRows().stream()
                .map(row -> new org.weakref.nitro.data.Row(row.getFields().stream()
                        .map(TestTpcdsParquetQueries::normalizeValue)
                        .toArray()))
                .toList();
    }

    private static Object normalizeValue(Object value)
    {
        return value instanceof String string ? normalize(string) : value;
    }

    private static String normalize(String value)
    {
        return value.stripTrailing();
    }
}
