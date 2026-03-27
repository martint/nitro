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

import io.trino.spi.type.SqlDecimal;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;
import org.weakref.nitro.trino.TrinoTpcdsSupport;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestQueries
{
    @Test
    void testQuery41()
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
    void testQuery41EligibleManufacturers()
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
    void testQuery41Sql()
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
    void testQuery41TrinoSql()
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
    void testQuery62()
    {
        assertOperatorMatches("62", tables -> TpcdsParquetSupport.query62(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query62(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery45()
    {
        assertOperatorMatches("45", tables -> TpcdsParquetSupport.query45(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query45(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery10()
    {
        assertOperatorMatches("10", tables -> TpcdsParquetSupport.query10(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query10(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery35()
    {
        assertOperatorMatches("35", tables -> TpcdsParquetSupport.query35(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query35(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery10Sql()
    {
        assertNitroMatchesSql("10", tables -> TpcdsParquetSupport.query10(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery10TrinoSql()
    {
        assertTrinoOperatorMatchesSql("10", support -> support.query10(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery35Sql()
    {
        assertNitroMatchesSql("35", tables -> TpcdsParquetSupport.query35(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery35TrinoSql()
    {
        assertTrinoOperatorMatchesSql("35", support -> support.query35(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery62Sql()
    {
        assertNitroMatchesSql("62", tables -> TpcdsParquetSupport.query62(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery62TrinoSql()
    {
        assertTrinoOperatorMatchesSql("62", support -> support.query62(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery45Sql()
    {
        assertNitroMatchesSql("45", tables -> TpcdsParquetSupport.query45(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery45TrinoSql()
    {
        assertTrinoOperatorMatchesSql("45", support -> support.query45(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery96()
    {
        assertOperatorMatches("96", tables -> TpcdsParquetSupport.query96(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery73()
    {
        assertOperatorMatches("73", tables -> TpcdsParquetSupport.query73(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query73(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery73Sql()
    {
        assertNitroMatchesSql("73", tables -> TpcdsParquetSupport.query73(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery73TrinoSql()
    {
        assertTrinoOperatorMatchesSql("73", support -> support.query73(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery69()
    {
        assertOperatorMatches("69", tables -> TpcdsParquetSupport.query69(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query69(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery69Sql()
    {
        assertNitroMatchesSql("69", tables -> TpcdsParquetSupport.query69(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery69TrinoSql()
    {
        assertTrinoOperatorMatchesSql("69", support -> support.query69(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery84()
    {
        assertOperatorMatches("84", tables -> TpcdsParquetSupport.query84(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query84(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery84Sql()
    {
        assertNitroMatchesSql("84", tables -> TpcdsParquetSupport.query84(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeQuery84Value);
    }

    @Test
    void testQuery84TrinoSql()
    {
        assertTrinoOperatorMatchesSql("84", support -> support.query84(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeQuery84Value);
    }

    @Test
    void testQuery90()
    {
        assertOperatorMatches("90", tables -> TpcdsParquetSupport.query90(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query90(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery90Sql()
    {
        assertApproximateNitroMatchesSql("90", tables -> TpcdsParquetSupport.query90(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery90TrinoSql()
    {
        assertApproximateTrinoOperatorMatchesSql("90", support -> support.query90(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery96Sql()
    {
        assertNitroMatchesSql("96", tables -> TpcdsParquetSupport.query96(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery96TrinoSql()
    {
        assertTrinoOperatorMatchesSql("96", support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery99()
    {
        assertOperatorMatches("99", tables -> TpcdsParquetSupport.query99(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query99(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery88()
    {
        assertOperatorMatches("88", tables -> TpcdsParquetSupport.query88(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query88(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery88Sql()
    {
        assertNitroMatchesSql("88", tables -> TpcdsParquetSupport.query88(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery88TrinoSql()
    {
        assertTrinoOperatorMatchesSql("88", support -> support.query88(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery99Sql()
    {
        assertNitroMatchesSql("99", tables -> TpcdsParquetSupport.query99(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery99TrinoSql()
    {
        assertTrinoOperatorMatchesSql("99", support -> support.query99(TpcdsParquetTables.requiredActual("sf10")));
    }

    private static void assertOperatorMatches(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        assertOperatorMatches(queryId, nitroQuery, trinoQuery, TestQueries::normalizeValue);
    }

    private static void assertOperatorMatches(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery, java.util.function.Function<Object, Object> valueNormalizer)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = nitroQuery.apply(tables)) {
            nitroRows = normalizeNitroRows(OperatorAssertions.OperatorAssert.toRows(query), valueNormalizer);
        }

        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            assertThat(normalizeTrinoRows(trinoQuery.apply(support), valueNormalizer))
                    .as("TPC-DS Q%s operator assembly result", queryId)
                    .containsExactlyElementsOf(nitroRows);
        }
    }

    private static void assertNitroMatchesSql(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery)
    {
        assertNitroMatchesSql(queryId, nitroQuery, TestQueries::normalizeValue);
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
        assertTrinoOperatorMatchesSql(queryId, trinoQuery, TestQueries::normalizeValue);
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
        return normalizeNitroRows(rows, TestQueries::normalizeValue);
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
        return normalizeTrinoRows(result, TestQueries::normalizeValue);
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

    private static Object normalizeDecimalCentsValue(Object value)
    {
        if (value instanceof SqlDecimal decimal) {
            return decimal.toBigDecimal().unscaledValue().longValueExact();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.unscaledValue().longValueExact();
        }
        return normalizeValue(value);
    }
}
