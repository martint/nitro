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

import io.trino.spi.type.SqlDecimal;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.tpcds.TpcdsParquetTables;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;
import org.weakref.nitro.trino.TrinoTpcdsSupport;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
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
    void testQuery41EligibleManufacturersMatchTrinoOperatorAssembly()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Set<String> nitroManufacturers = TpcdsParquetSupport.query41EligibleManufacturers(new Allocator(), primitiveRegistry, tables);

        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            assertThat(support.query41EligibleManufacturers(tables))
                    .containsExactlyInAnyOrderElementsOf(nitroManufacturers);
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
        assertOperatorMatches("62", tables -> TpcdsParquetSupport.query62(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query62(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery10MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("10", tables -> TpcdsParquetSupport.query10(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query10(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery35MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("35", tables -> TpcdsParquetSupport.query35(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query35(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery10MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("10", tables -> TpcdsParquetSupport.query10(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery10MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("10", support -> support.query10(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery35MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("35", tables -> TpcdsParquetSupport.query35(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery35MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("35", support -> support.query35(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery62MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("62", tables -> TpcdsParquetSupport.query62(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery62MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("62", support -> support.query62(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery96MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("96", tables -> TpcdsParquetSupport.query96(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery73MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("73", tables -> TpcdsParquetSupport.query73(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query73(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery73MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("73", tables -> TpcdsParquetSupport.query73(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery73MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("73", support -> support.query73(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery69MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("69", tables -> TpcdsParquetSupport.query69(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query69(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery69MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("69", tables -> TpcdsParquetSupport.query69(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery69MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("69", support -> support.query69(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery84MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("84", tables -> TpcdsParquetSupport.query84(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query84(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery84MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("84", tables -> TpcdsParquetSupport.query84(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestTpcdsParquetQueries::normalizeQuery84Value);
    }

    @Test
    void testQuery84MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("84", support -> support.query84(TpcdsParquetTables.requiredActual("sf10")), TestTpcdsParquetQueries::normalizeQuery84Value);
    }

    @Test
    void testQuery90MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("90", tables -> TpcdsParquetSupport.query90(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query90(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery90MatchesTrinoSqlReference()
    {
        assertApproximateNitroMatchesSql("90", tables -> TpcdsParquetSupport.query90(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery90MatchesTrinoSqlReference()
    {
        assertApproximateTrinoOperatorMatchesSql("90", support -> support.query90(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery96MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("96", tables -> TpcdsParquetSupport.query96(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery96MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("96", support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery99MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("99", tables -> TpcdsParquetSupport.query99(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query99(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery88MatchesTrinoOperatorAssembly()
    {
        assertOperatorMatches("88", tables -> TpcdsParquetSupport.query88(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query88(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery88MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("88", tables -> TpcdsParquetSupport.query88(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery88MatchesTrinoSqlReference()
    {
        assertTrinoOperatorMatchesSql("88", support -> support.query88(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testNitroQuery99MatchesTrinoSqlReference()
    {
        assertNitroMatchesSql("99", tables -> TpcdsParquetSupport.query99(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
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
        assertNitroMatchesSql(queryId, nitroQuery, TestTpcdsParquetQueries::normalizeValue);
    }

    private static void assertNitroMatchesSql(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<Object, Object> valueNormalizer)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = nitroQuery.apply(tables)) {
            nitroRows = normalizeNitroRows(OperatorAssertions.OperatorAssert.toRows(query), valueNormalizer);
        }

        try (TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            assertThat(nitroRows)
                    .as("TPC-DS Q%s Nitro vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId, "sf10"), valueNormalizer));
        }
    }

    private static void assertTrinoOperatorMatchesSql(String queryId, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        assertTrinoOperatorMatchesSql(queryId, trinoQuery, TestTpcdsParquetQueries::normalizeValue);
    }

    private static void assertTrinoOperatorMatchesSql(String queryId, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery, java.util.function.Function<Object, Object> valueNormalizer)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        try (TrinoTpcdsParquetSupport operatorSupport = new TrinoTpcdsParquetSupport();
                TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            assertThat(normalizeTrinoRows(trinoQuery.apply(operatorSupport), valueNormalizer))
                    .as("TPC-DS Q%s Trino operator vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId, "sf10"), valueNormalizer));
        }
    }

    private static void assertApproximateNitroMatchesSql(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        double actual;
        try (Operator query = nitroQuery.apply(tables)) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            actual = ((Number) rows.getFirst().values()[0]).doubleValue();
        }

        try (TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery(queryId, "sf10");
            assertThat(actual)
                    .as("TPC-DS Q%s Nitro vs SQL", queryId)
                    .isCloseTo(bigDecimalValue(sqlResult).doubleValue(), within(1e-9));
        }
    }

    private static void assertApproximateTrinoOperatorMatchesSql(String queryId, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        try (TrinoTpcdsParquetSupport operatorSupport = new TrinoTpcdsParquetSupport();
                TrinoTpcdsSupport sqlSupport = new TrinoTpcdsSupport("sf10")) {
            MaterializedResult operatorResult = trinoQuery.apply(operatorSupport);
            assertThat(operatorResult.getMaterializedRows()).hasSize(1);
            double actual = ((Number) operatorResult.getMaterializedRows().getFirst().getField(0)).doubleValue();
            assertThat(actual)
                    .as("TPC-DS Q%s Trino operator vs SQL", queryId)
                    .isCloseTo(bigDecimalValue(sqlSupport.executeBenchmarkQuery(queryId, "sf10")).doubleValue(), within(1e-9));
        }
    }

    private static BigDecimal bigDecimalValue(MaterializedResult result)
    {
        assertThat(result.getMaterializedRows()).hasSize(1);
        Object value = result.getMaterializedRows().getFirst().getField(0);
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof SqlDecimal decimal) {
            return decimal.toBigDecimal();
        }
        return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    private static List<org.weakref.nitro.data.Row> normalizeNitroRows(List<org.weakref.nitro.data.Row> rows)
    {
        return normalizeNitroRows(rows, TestTpcdsParquetQueries::normalizeValue);
    }

    private static List<org.weakref.nitro.data.Row> normalizeNitroRows(List<org.weakref.nitro.data.Row> rows, java.util.function.Function<Object, Object> valueNormalizer)
    {
        return rows.stream()
                .map(row -> new org.weakref.nitro.data.Row(java.util.Arrays.stream(row.values())
                        .map(valueNormalizer)
                        .toArray()))
                .toList();
    }

    private static List<org.weakref.nitro.data.Row> normalizeTrinoRows(MaterializedResult result)
    {
        return normalizeTrinoRows(result, TestTpcdsParquetQueries::normalizeValue);
    }

    private static List<org.weakref.nitro.data.Row> normalizeTrinoRows(MaterializedResult result, java.util.function.Function<Object, Object> valueNormalizer)
    {
        return result.getMaterializedRows().stream()
                .map(row -> new org.weakref.nitro.data.Row(row.getFields().stream()
                        .map(valueNormalizer)
                        .toArray()))
                .toList();
    }

    private static Object normalizeValue(Object value)
    {
        return value instanceof String string ? normalize(string) : value;
    }

    private static String normalize(String value)
    {
        return value == null ? null : value.stripTrailing();
    }

    private static Object normalizeQuery84Value(Object value)
    {
        if (!(value instanceof String string)) {
            return value;
        }
        String normalized = normalize(string);
        if (normalized == null) {
            return null;
        }
        int delimiter = normalized.indexOf(", ");
        if (delimiter < 0) {
            return normalized;
        }
        return normalize(normalized.substring(0, delimiter)) + ", " + normalize(normalized.substring(delimiter + 2));
    }
}
