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

import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.trino.TrinoTpcdsParquetSqlSupport;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;

import java.lang.reflect.Method;
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
    void testQuery01()
    {
        assertOperatorMatches("01", tables -> TpcdsParquetSupport.query01(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query01(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery01Sql()
    {
        assertNitroMatchesSql("01", tables -> TpcdsParquetSupport.query01(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery01TrinoSql()
    {
        assertTrinoOperatorMatchesSql("01", support -> support.query01(TpcdsParquetTables.requiredActual("sf10")));
    }

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

        try (TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery("41");
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
                TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult operatorResult = operatorSupport.query41(tables);
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery("41");
            assertThat(operatorResult.getMaterializedRows().stream()
                    .map(row -> normalize((String) row.getField(0)))
                    .toList())
                    .containsExactlyElementsOf(sqlResult.getMaterializedRows().stream()
                            .map(row -> normalize(Objects.toString(row.getField(0))))
                            .toList());
        }
    }

    @Test
    void testQuery44()
    {
        assertOperatorMatches("44", tables -> TpcdsParquetSupport.query44(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query44(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery44Sql()
    {
        assertNitroMatchesSql("44", tables -> TpcdsParquetSupport.query44(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery44TrinoSql()
    {
        assertTrinoOperatorMatchesSql("44", support -> support.query44(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery44FilteredProjectedScanMatchesMaterializedTable()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Method equalMethod = TpcdsParquetSupport.class.getDeclaredMethod("equal", int.class, long.class);
        Method filteredProjectedScanMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedScan", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method filteredProjectedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedTable", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        equalMethod.setAccessible(true);
        filteredProjectedScanMethod.setAccessible(true);
        filteredProjectedTableMethod.setAccessible(true);

        Object equalStore = equalMethod.invoke(null, 1, 4L);
        String[] columns = {"ss_item_sk", "ss_store_sk", "ss_net_profit"};
        int[] inputs = {0, 2};

        List<Row> liveRows;
        List<Row> materializedRows;
        try (Operator live = (Operator) filteredProjectedScanMethod.invoke(null, allocator, primitiveRegistry, tables, "store_sales", equalStore, columns, inputs);
                Operator materialized = (Operator) filteredProjectedTableMethod.invoke(null, allocator, primitiveRegistry, tables, "store_sales", equalStore, columns, inputs)) {
            liveRows = OperatorAssertions.OperatorAssert.toRows(live);
            materializedRows = OperatorAssertions.OperatorAssert.toRows(materialized);
        }

        assertThat(liveRows).containsExactlyElementsOf(materializedRows);
    }

    @Test
    void testQuery44ItemAggregatesMatchBetweenLiveScanAndMaterializedTable()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Method equalMethod = TpcdsParquetSupport.class.getDeclaredMethod("equal", int.class, long.class);
        Method greaterThanMethod = TpcdsParquetSupport.class.getDeclaredMethod("greaterThan", int.class, long.class);
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method filteredProjectedScanMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedScan", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method filteredProjectedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedTable", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method projectQuery44ItemAggregatesMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44ItemAggregates", Allocator.class, PrimitiveRegistry.class, Operator.class);
        for (Method method : List.of(equalMethod, greaterThanMethod, filterMethod, filteredProjectedScanMethod, filteredProjectedTableMethod, projectQuery44ItemAggregatesMethod)) {
            method.setAccessible(true);
        }

        Object equalStore = equalMethod.invoke(null, 1, 4L);
        String[] columns = {"ss_item_sk", "ss_store_sk", "ss_net_profit"};
        int[] inputs = {0, 2};

        List<Row> liveRows;
        List<Row> materializedRows;
        try (Operator live = groupedQuery44ItemAggregates(allocator, primitiveRegistry, tables, equalStore, columns, inputs, filteredProjectedScanMethod, filterMethod, greaterThanMethod, projectQuery44ItemAggregatesMethod);
                Operator materialized = groupedQuery44ItemAggregates(allocator, primitiveRegistry, tables, equalStore, columns, inputs, filteredProjectedTableMethod, filterMethod, greaterThanMethod, projectQuery44ItemAggregatesMethod)) {
            liveRows = OperatorAssertions.OperatorAssert.toRows(live);
            materializedRows = OperatorAssertions.OperatorAssert.toRows(materialized);
        }

        assertThat(liveRows).containsExactlyElementsOf(materializedRows);
    }

    @Test
    void testQuery44EligibleRowsMatchBetweenLiveScanAndMaterializedTable()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Class<?> filterSpecArrayClass = java.lang.reflect.Array.newInstance(filterSpecClass, 0).getClass();
        Method equalMethod = TpcdsParquetSupport.class.getDeclaredMethod("equal", int.class, long.class);
        Method greaterThanMethod = TpcdsParquetSupport.class.getDeclaredMethod("greaterThan", int.class, long.class);
        Method isNullMethod = TpcdsParquetSupport.class.getDeclaredMethod("isNull", int.class);
        Method andMethod = TpcdsParquetSupport.class.getDeclaredMethod("and", filterSpecClass, filterSpecClass, filterSpecArrayClass);
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method filteredProjectedScanMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedScan", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method filteredProjectedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedTable", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method projectQuery44ItemAggregatesMethod = projectQuery44ItemAggregatesMethod();
        Method projectQuery44ScalarAggregateMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44ScalarAggregate", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method projectQuery44AverageKeysMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44AverageKeys", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method query44ThresholdPredicateMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44ThresholdPredicate", int.class, int.class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        for (Method method : List.of(
                equalMethod,
                greaterThanMethod,
                isNullMethod,
                andMethod,
                filterMethod,
                filteredProjectedScanMethod,
                filteredProjectedTableMethod,
                projectQuery44ItemAggregatesMethod,
                projectQuery44ScalarAggregateMethod,
                projectQuery44AverageKeysMethod,
                query44ThresholdPredicateMethod,
                projectInputsMethod)) {
            method.setAccessible(true);
        }

        List<Row> liveRows;
        List<Row> materializedRows;
        try (Operator live = query44EligibleRows(
                allocator,
                primitiveRegistry,
                tables,
                filteredProjectedScanMethod,
                equalMethod,
                greaterThanMethod,
                isNullMethod,
                andMethod,
                filterMethod,
                projectQuery44ItemAggregatesMethod,
                projectQuery44ScalarAggregateMethod,
                projectQuery44AverageKeysMethod,
                query44ThresholdPredicateMethod,
                projectInputsMethod);
                Operator materialized = query44EligibleRows(
                        allocator,
                        primitiveRegistry,
                        tables,
                        filteredProjectedTableMethod,
                        equalMethod,
                        greaterThanMethod,
                        isNullMethod,
                        andMethod,
                        filterMethod,
                        projectQuery44ItemAggregatesMethod,
                        projectQuery44ScalarAggregateMethod,
                        projectQuery44AverageKeysMethod,
                        query44ThresholdPredicateMethod,
                        projectInputsMethod)) {
            liveRows = OperatorAssertions.OperatorAssert.toRows(live);
            materializedRows = OperatorAssertions.OperatorAssert.toRows(materialized);
        }

        assertThat(liveRows).containsExactlyElementsOf(materializedRows);
    }

    @Test
    void testQuery44RankedItemsMatchMaterializedRanking()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Method rankedItemsMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44RankedItems", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, boolean.class);
        rankedItemsMethod.setAccessible(true);

        List<Row> liveAscending;
        List<Row> liveDescending;
        try (Operator ascending = (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, false);
                Operator descending = (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, true);
                Operator expectedAscending = materializedQuery44RankedItems(allocator, primitiveRegistry, tables, false);
                Operator expectedDescending = materializedQuery44RankedItems(allocator, primitiveRegistry, tables, true)) {
            liveAscending = OperatorAssertions.OperatorAssert.toRows(ascending);
            liveDescending = OperatorAssertions.OperatorAssert.toRows(descending);
            assertThat(liveAscending).containsExactlyElementsOf(OperatorAssertions.OperatorAssert.toRows(expectedAscending));
            assertThat(liveDescending).containsExactlyElementsOf(OperatorAssertions.OperatorAssert.toRows(expectedDescending));
        }
    }

    @Test
    void testQuery44RankedItemJoinMatchesItemTable()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Method rankedItemsMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44RankedItems", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, boolean.class);
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        rankedItemsMethod.setAccessible(true);
        scannedTableMethod.setAccessible(true);
        projectInputsMethod.setAccessible(true);

        List<Row> expectedAscending;
        List<Row> expectedDescending;
        try (Operator expectedAscendingOperator = materializedQuery44NamedRanks(allocator, primitiveRegistry, tables, false, scannedTableMethod, projectInputsMethod);
                Operator expectedDescendingOperator = materializedQuery44NamedRanks(allocator, primitiveRegistry, tables, true, scannedTableMethod, projectInputsMethod);
                Operator actualAscendingOperator = joinedQuery44NamedRanks(allocator, primitiveRegistry, tables, false, rankedItemsMethod, scannedTableMethod, projectInputsMethod);
                Operator actualDescendingOperator = joinedQuery44NamedRanks(allocator, primitiveRegistry, tables, true, rankedItemsMethod, scannedTableMethod, projectInputsMethod)) {
            expectedAscending = OperatorAssertions.OperatorAssert.toRows(expectedAscendingOperator);
            expectedDescending = OperatorAssertions.OperatorAssert.toRows(expectedDescendingOperator);
            assertThat(OperatorAssertions.OperatorAssert.toRows(actualAscendingOperator)).containsExactlyElementsOf(expectedAscending);
            assertThat(OperatorAssertions.OperatorAssert.toRows(actualDescendingOperator)).containsExactlyElementsOf(expectedDescending);
        }
    }

    @Test
    void testQuery44JoinProjectionMatchesRawJoinColumns()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Method rankedItemsMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44RankedItems", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, boolean.class);
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        rankedItemsMethod.setAccessible(true);
        scannedTableMethod.setAccessible(true);
        projectInputsMethod.setAccessible(true);

        List<Row> rawRows;
        List<Row> projectedRows;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, false),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0);
                Operator projected = (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, new org.weakref.nitro.operator.HashJoinOperator(
                        allocator,
                        (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, false),
                        0,
                        (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                        0), new int[] {1, 3})) {
            rawRows = OperatorAssertions.OperatorAssert.toRows(join).stream()
                    .map(row -> Row.row(row.values()[1], row.values()[3]))
                    .toList();
            projectedRows = OperatorAssertions.OperatorAssert.toRows(projected);
        }

        assertThat(projectedRows).containsExactlyElementsOf(rawRows);
    }

    @Test
    void testQuery44SingleItemJoinMatchesRawItemName()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        scannedTableMethod.setAccessible(true);
        projectInputsMethod.setAccessible(true);

        List<Row> itemRows;
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            itemRows = OperatorAssertions.OperatorAssert.toRows(items);
        }
        java.util.Map<Long, Object> namesByItem = new java.util.HashMap<>();
        for (Row row : itemRows) {
            namesByItem.put(((Number) row.values()[0]).longValue(), row.values()[1]);
        }

        List<Row> joinedRows;
        try (Operator projected = (Operator) projectInputsMethod.invoke(
                null,
                allocator,
                TestPrimitiveFunctions.primitiveRegistry(),
                new org.weakref.nitro.operator.HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 2, List.of(Row.row(40583L, 1L))),
                        0,
                        (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                        0),
                new int[] {1, 3})) {
            joinedRows = OperatorAssertions.OperatorAssert.toRows(projected);
        }

        assertThat(joinedRows).containsExactly(Row.row(1L, namesByItem.get(40583L)));
    }

    @Test
    void testQuery44TwoItemJoinMatchesRawItemNames()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        scannedTableMethod.setAccessible(true);
        projectInputsMethod.setAccessible(true);

        java.util.Map<Long, Object> namesByItem = new java.util.HashMap<>();
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            for (Row row : OperatorAssertions.OperatorAssert.toRows(items)) {
                namesByItem.put(((Number) row.values()[0]).longValue(), row.values()[1]);
            }
        }

        List<Row> joinedRows;
        try (Operator projected = (Operator) projectInputsMethod.invoke(
                null,
                allocator,
                TestPrimitiveFunctions.primitiveRegistry(),
                new org.weakref.nitro.operator.HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 2, List.of(Row.row(40583L, 1L), Row.row(54967L, 2L))),
                        0,
                        (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                        0),
                new int[] {1, 3})) {
            joinedRows = OperatorAssertions.OperatorAssert.toRows(projected);
        }

        assertThat(joinedRows).containsExactly(
                Row.row(1L, namesByItem.get(40583L)),
                Row.row(2L, namesByItem.get(54967L)));
    }

    @Test
    void testQuery44TenItemJoinPreservesMatchedInnerKeys()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        scannedTableMethod.setAccessible(true);

        List<Row> rankedRows = List.of(
                Row.row(40583L, 1L),
                Row.row(54967L, 2L),
                Row.row(96451L, 3L),
                Row.row(31211L, 4L),
                Row.row(10705L, 5L),
                Row.row(46297L, 6L),
                Row.row(76179L, 7L),
                Row.row(91091L, 8L),
                Row.row(66772L, 8L),
                Row.row(81361L, 10L));

        List<Row> rawJoinRows;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, rankedRows),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0)) {
            rawJoinRows = OperatorAssertions.OperatorAssert.toRows(join);
        }

        assertThat(rawJoinRows).allSatisfy(row -> assertThat(row.values()[2]).isEqualTo(row.values()[0]));
    }

    @Test
    void testBufferedItemJoinInputPreservesKeyNameAlignment()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        scannedTableMethod.setAccessible(true);

        java.util.Map<Long, Object> namesByItem = new java.util.HashMap<>();
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            for (Row row : OperatorAssertions.OperatorAssert.toRows(items)) {
                namesByItem.put(((Number) row.values()[0]).longValue(), row.values()[1]);
            }
        }

        Class<?> joinBufferSupportClass = Class.forName("org.weakref.nitro.operator.JoinBufferSupport");
        Class<?> bufferedJoinInputClass = Class.forName("org.weakref.nitro.operator.BufferedJoinInput");
        java.lang.reflect.Constructor<?> buffersConstructor = joinBufferSupportClass.getDeclaredConstructor(Allocator.class, Allocator.Context.class);
        java.lang.reflect.Constructor<?> bufferedConstructor = bufferedJoinInputClass.getDeclaredConstructor(joinBufferSupportClass, int.class);
        Method loadAllMethod = bufferedJoinInputClass.getDeclaredMethod("loadAll", Operator.class, int.class, int[].class, boolean.class);
        Method batchesMethod = bufferedJoinInputClass.getDeclaredMethod("batches");
        buffersConstructor.setAccessible(true);
        bufferedConstructor.setAccessible(true);
        loadAllMethod.setAccessible(true);
        batchesMethod.setAccessible(true);

        Object buffers = buffersConstructor.newInstance(allocator, new Allocator.Context("BufferedItemJoinInput"));
        Object buffered = bufferedConstructor.newInstance(buffers, 2);
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            loadAllMethod.invoke(buffered, items, 4_096, new int[] {0}, false);
        }

        @SuppressWarnings("unchecked")
        List<Object> batches = (List<Object>) batchesMethod.invoke(buffered);
        for (Object batch : batches) {
            Method columnsMethod = batch.getClass().getDeclaredMethod("columns");
            Method lengthMethod = batch.getClass().getDeclaredMethod("length");
            columnsMethod.setAccessible(true);
            lengthMethod.setAccessible(true);
            Object[] columns = (Object[]) columnsMethod.invoke(batch);
            Streams keys = (Streams) columns[0];
            Streams names = (Streams) columns[1];
            for (int position = 0; position < (int) lengthMethod.invoke(batch); position++) {
                long key = ((org.weakref.nitro.data.I64Vector) keys.values()).values()[position];
                Object expectedName = namesByItem.get(key);
                Object actualName = decodeStreamValue(names, position);
                assertThat(actualName).isEqualTo(expectedName);
            }
        }
    }

    @Test
    void testBufferedItemNameCopyPathMatchesExpectedNames()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        scannedTableMethod.setAccessible(true);

        long[] rankedItemKeys = {40583L, 54967L, 96451L, 31211L, 10705L, 46297L, 76179L, 91091L, 66772L, 81361L};
        java.util.Map<Long, Object> namesByItem = new java.util.HashMap<>();
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            for (Row row : OperatorAssertions.OperatorAssert.toRows(items)) {
                namesByItem.put(((Number) row.values()[0]).longValue(), row.values()[1]);
            }
        }

        Class<?> joinBufferSupportClass = Class.forName("org.weakref.nitro.operator.JoinBufferSupport");
        Class<?> bufferedJoinInputClass = Class.forName("org.weakref.nitro.operator.BufferedJoinInput");
        java.lang.reflect.Constructor<?> buffersConstructor = joinBufferSupportClass.getDeclaredConstructor(Allocator.class, Allocator.Context.class);
        java.lang.reflect.Constructor<?> bufferedConstructor = bufferedJoinInputClass.getDeclaredConstructor(joinBufferSupportClass, int.class);
        Method loadAllMethod = bufferedJoinInputClass.getDeclaredMethod("loadAll", Operator.class, int.class, int[].class, boolean.class);
        Method batchesMethod = bufferedJoinInputClass.getDeclaredMethod("batches");
        Method copySinglePositionMethod = joinBufferSupportClass.getDeclaredMethod("copySinglePosition", Streams.class, Streams.class, int.class, int.class, int.class);
        buffersConstructor.setAccessible(true);
        bufferedConstructor.setAccessible(true);
        loadAllMethod.setAccessible(true);
        batchesMethod.setAccessible(true);
        copySinglePositionMethod.setAccessible(true);

        Object buffers = buffersConstructor.newInstance(allocator, new Allocator.Context("BufferedItemNameCopy"));
        Object buffered = bufferedConstructor.newInstance(buffers, 2);
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            loadAllMethod.invoke(buffered, items, 4_096, new int[] {0}, false);
        }

        @SuppressWarnings("unchecked")
        List<Object> batches = (List<Object>) batchesMethod.invoke(buffered);
        java.util.Map<Long, Object[]> locationsByKey = new java.util.HashMap<>();
        for (Object batch : batches) {
            Method columnsMethod = batch.getClass().getDeclaredMethod("columns");
            Method lengthMethod = batch.getClass().getDeclaredMethod("length");
            columnsMethod.setAccessible(true);
            lengthMethod.setAccessible(true);
            Object[] columns = (Object[]) columnsMethod.invoke(batch);
            Streams keys = (Streams) columns[0];
            Streams names = (Streams) columns[1];
            for (int position = 0; position < (int) lengthMethod.invoke(batch); position++) {
                long key = ((org.weakref.nitro.data.I64Vector) keys.values()).values()[position];
                locationsByKey.putIfAbsent(key, new Object[] {names, position});
            }
        }

        Streams copied = null;
        for (int outputPosition = 0; outputPosition < rankedItemKeys.length; outputPosition++) {
            Object[] location = locationsByKey.get(rankedItemKeys[outputPosition]);
            copied = (Streams) copySinglePositionMethod.invoke(buffers, copied, (Streams) location[0], rankedItemKeys.length, outputPosition, (int) location[1]);
        }

        List<Object> actualNames = new java.util.ArrayList<>();
        for (int position = 0; position < rankedItemKeys.length; position++) {
            actualNames.add(decodeStreamValue(copied, position));
        }
        List<Object> expectedNames = java.util.Arrays.stream(rankedItemKeys)
                .mapToObj(namesByItem::get)
                .toList();

        assertThat(actualNames).containsExactlyElementsOf(expectedNames);
    }

    @Test
    void testBufferedItemNameGroupedCopyPathMatchesExpectedNames()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        scannedTableMethod.setAccessible(true);

        long[] rankedItemKeys = {40583L, 54967L, 96451L, 31211L, 10705L, 46297L, 76179L, 91091L, 66772L, 81361L};
        java.util.Map<Long, Object> namesByItem = new java.util.HashMap<>();
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            for (Row row : OperatorAssertions.OperatorAssert.toRows(items)) {
                namesByItem.put(((Number) row.values()[0]).longValue(), row.values()[1]);
            }
        }

        Class<?> joinBufferSupportClass = Class.forName("org.weakref.nitro.operator.JoinBufferSupport");
        Class<?> bufferedJoinInputClass = Class.forName("org.weakref.nitro.operator.BufferedJoinInput");
        java.lang.reflect.Constructor<?> buffersConstructor = joinBufferSupportClass.getDeclaredConstructor(Allocator.class, Allocator.Context.class);
        java.lang.reflect.Constructor<?> bufferedConstructor = bufferedJoinInputClass.getDeclaredConstructor(joinBufferSupportClass, int.class);
        Method loadAllMethod = bufferedJoinInputClass.getDeclaredMethod("loadAll", Operator.class, int.class, int[].class, boolean.class);
        Method batchesMethod = bufferedJoinInputClass.getDeclaredMethod("batches");
        Method copyPositionsMethod = joinBufferSupportClass.getDeclaredMethod("copyPositions", Streams.class, Streams.class, int[].class, int.class, int.class, int.class);
        Method copySinglePositionMethod = joinBufferSupportClass.getDeclaredMethod("copySinglePosition", Streams.class, Streams.class, int.class, int.class, int.class);
        buffersConstructor.setAccessible(true);
        bufferedConstructor.setAccessible(true);
        loadAllMethod.setAccessible(true);
        batchesMethod.setAccessible(true);
        copyPositionsMethod.setAccessible(true);
        copySinglePositionMethod.setAccessible(true);

        Object buffers = buffersConstructor.newInstance(allocator, new Allocator.Context("BufferedItemNameGroupedCopy"));
        Object buffered = bufferedConstructor.newInstance(buffers, 2);
        try (Operator items = (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"})) {
            loadAllMethod.invoke(buffered, items, 4_096, new int[] {0}, false);
        }

        @SuppressWarnings("unchecked")
        List<Object> batches = (List<Object>) batchesMethod.invoke(buffered);
        java.util.Map<Long, Object[]> locationsByKey = new java.util.HashMap<>();
        for (Object batch : batches) {
            Method columnsMethod = batch.getClass().getDeclaredMethod("columns");
            Method lengthMethod = batch.getClass().getDeclaredMethod("length");
            columnsMethod.setAccessible(true);
            lengthMethod.setAccessible(true);
            Object[] columns = (Object[]) columnsMethod.invoke(batch);
            Streams keys = (Streams) columns[0];
            Streams names = (Streams) columns[1];
            for (int position = 0; position < (int) lengthMethod.invoke(batch); position++) {
                long key = ((org.weakref.nitro.data.I64Vector) keys.values()).values()[position];
                locationsByKey.putIfAbsent(key, new Object[] {names, position});
            }
        }

        Streams copied = null;
        int outputStart = 0;
        while (outputStart < rankedItemKeys.length) {
            Streams names = (Streams) locationsByKey.get(rankedItemKeys[outputStart])[0];
            int runLength = 1;
            while (outputStart + runLength < rankedItemKeys.length && locationsByKey.get(rankedItemKeys[outputStart + runLength])[0] == names) {
                runLength++;
            }
            if (runLength == 1) {
                copied = (Streams) copySinglePositionMethod.invoke(
                        buffers,
                        copied,
                        names,
                        rankedItemKeys.length,
                        outputStart,
                        (int) locationsByKey.get(rankedItemKeys[outputStart])[1]);
            }
            else {
                int[] positions = new int[runLength];
                for (int index = 0; index < runLength; index++) {
                    positions[index] = (int) locationsByKey.get(rankedItemKeys[outputStart + index])[1];
                }
                copied = (Streams) copyPositionsMethod.invoke(buffers, copied, names, positions, runLength, outputStart, rankedItemKeys.length);
            }
            outputStart += runLength;
        }

        List<Object> actualNames = new java.util.ArrayList<>();
        for (int position = 0; position < rankedItemKeys.length; position++) {
            actualNames.add(decodeStreamValue(copied, position));
        }
        List<Object> expectedNames = java.util.Arrays.stream(rankedItemKeys)
                .mapToObj(namesByItem::get)
                .toList();

        assertThat(actualNames).containsExactlyElementsOf(expectedNames);
    }

    @Test
    void testQuery44TenItemJoinNameMaterializationOrderDoesNotChangeNames()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        scannedTableMethod.setAccessible(true);

        List<Row> rankedRows = List.of(
                Row.row(40583L, 1L),
                Row.row(54967L, 2L),
                Row.row(96451L, 3L),
                Row.row(31211L, 4L),
                Row.row(10705L, 5L),
                Row.row(46297L, 6L),
                Row.row(76179L, 7L),
                Row.row(91091L, 8L),
                Row.row(66772L, 8L),
                Row.row(81361L, 10L));

        List<Object> namesFirst;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, rankedRows),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0);
                Batch batch = join.next()) {
            org.weakref.nitro.data.BinaryVector names = (org.weakref.nitro.data.BinaryVector) batch.output(3).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            namesFirst = new java.util.ArrayList<>();
            for (int position : batch.borrowMask()) {
                namesFirst.add(new String(names.copyBytes(position), java.nio.charset.StandardCharsets.UTF_8));
            }
        }

        List<Object> namesAfterKeys;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, rankedRows),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0);
                Batch batch = join.next()) {
            batch.output(0).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            batch.output(1).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            batch.output(2).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            org.weakref.nitro.data.BinaryVector names = (org.weakref.nitro.data.BinaryVector) batch.output(3).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            namesAfterKeys = new java.util.ArrayList<>();
            for (int position : batch.borrowMask()) {
                namesAfterKeys.add(new String(names.copyBytes(position), java.nio.charset.StandardCharsets.UTF_8));
            }
        }

        assertThat(namesAfterKeys).containsExactlyElementsOf(namesFirst);
    }

    @Test
    void testQuery44RankJoinMaterializedThenItemJoinMatchesLiveItemJoin()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();
        Method rankedItemsMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44RankedItems", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, boolean.class);
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        rankedItemsMethod.setAccessible(true);
        scannedTableMethod.setAccessible(true);

        List<Row> rankJoinRows;
        try (Operator rankJoin = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, false),
                1,
                (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, true),
                1)) {
            rankJoinRows = OperatorAssertions.OperatorAssert.toRows(rankJoin);
        }

        List<Row> materializedJoinRows;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, rankJoinRows),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0)) {
            materializedJoinRows = OperatorAssertions.OperatorAssert.toRows(join);
        }

        List<Row> liveJoinRows;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new org.weakref.nitro.operator.HashJoinOperator(
                        allocator,
                        (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, false),
                        1,
                        (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, true),
                        1),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0)) {
            liveJoinRows = OperatorAssertions.OperatorAssert.toRows(join);
        }

        assertThat(liveJoinRows).containsExactlyElementsOf(materializedJoinRows);
    }

    @Test
    void testQuery44FourColumnOuterJoinNameMaterializationOrderDoesNotChangeNames()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();
        Method rankedItemsMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44RankedItems", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, boolean.class);
        Method scannedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("scannedTable", Allocator.class, TpcdsParquetTables.class, String.class, String[].class);
        rankedItemsMethod.setAccessible(true);
        scannedTableMethod.setAccessible(true);

        List<Row> rankJoinRows;
        try (Operator rankJoin = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, false),
                1,
                (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, true),
                1)) {
            rankJoinRows = OperatorAssertions.OperatorAssert.toRows(rankJoin);
        }

        List<Object> namesFirst;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, rankJoinRows),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0);
                Batch batch = join.next()) {
            org.weakref.nitro.data.BinaryVector names = (org.weakref.nitro.data.BinaryVector) batch.output(5).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            namesFirst = new java.util.ArrayList<>();
            for (int position : batch.borrowMask()) {
                namesFirst.add(new String(names.copyBytes(position), java.nio.charset.StandardCharsets.UTF_8));
            }
        }

        List<Object> namesAfterOtherColumns;
        try (Operator join = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, rankJoinRows),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0);
                Batch batch = join.next()) {
            batch.output(0).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            batch.output(1).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            batch.output(2).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            batch.output(3).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            batch.output(4).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            org.weakref.nitro.data.BinaryVector names = (org.weakref.nitro.data.BinaryVector) batch.output(5).borrow(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES);
            namesAfterOtherColumns = new java.util.ArrayList<>();
            for (int position : batch.borrowMask()) {
                namesAfterOtherColumns.add(new String(names.copyBytes(position), java.nio.charset.StandardCharsets.UTF_8));
            }
        }

        assertThat(namesAfterOtherColumns).containsExactlyElementsOf(namesFirst);
    }

    @Test
    void testQuery44ThresholdFunctionsOnKnownGoodRows()
            throws Exception
    {
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Method projectAverageKeysMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44AverageKeys", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method query44ThresholdPredicateMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44ThresholdPredicate", int.class, int.class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        projectAverageKeysMethod.setAccessible(true);
        filterMethod.setAccessible(true);
        query44ThresholdPredicateMethod.setAccessible(true);
        projectInputsMethod.setAccessible(true);

        List<Row> joinedRows = List.of(
                Row.row(1L, 40583L, -151936L, 2L, 1L, -537446531L, 6367L),
                Row.row(1L, 54967L, -911581L, 12L, 1L, -537446531L, 6367L),
                Row.row(1L, 96451L, -1291387L, 17L, 1L, -537446531L, 6367L),
                Row.row(1L, 31211L, -303855L, 4L, 1L, -537446531L, 6367L));

        Operator projected = (Operator) projectAverageKeysMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                new ConstantTableOperator(allocator, 7, joinedRows));
        projected = (Operator) filterMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                projected,
                query44ThresholdPredicateMethod.invoke(null, 1, 2));
        Operator result = (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, projected, new int[] {0, 1});

        try (result) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(result)).containsExactly(
                    Row.row(40583L, -75_968_000_000L),
                    Row.row(54967L, -75_965_083_333L),
                    Row.row(96451L, -75_963_941_176L),
                    Row.row(31211L, -75_963_750_000L));
        }
    }

    @Test
    void testQuery44ScalarReplicationJoinOnKnownGoodRows()
            throws Exception
    {
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Method projectAverageKeysMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44AverageKeys", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method query44ThresholdPredicateMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44ThresholdPredicate", int.class, int.class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        projectAverageKeysMethod.setAccessible(true);
        filterMethod.setAccessible(true);
        query44ThresholdPredicateMethod.setAccessible(true);
        projectInputsMethod.setAccessible(true);

        List<Row> itemAggregateRows = List.of(
                Row.row(1L, 40583L, -151936L, 2L),
                Row.row(1L, 54967L, -911581L, 12L),
                Row.row(1L, 96451L, -1291387L, 17L),
                Row.row(1L, 31211L, -303855L, 4L));
        List<Row> scalarRow = List.of(Row.row(1L, -537446531L, 6367L));

        Operator joined = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, itemAggregateRows),
                0,
                new ConstantTableOperator(allocator, 3, scalarRow),
                0);
        joined = (Operator) projectAverageKeysMethod.invoke(null, allocator, primitiveRegistry, joined);
        joined = (Operator) filterMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                joined,
                query44ThresholdPredicateMethod.invoke(null, 1, 2));
        Operator result = (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, joined, new int[] {0, 1});

        try (result) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(result)).containsExactly(
                    Row.row(40583L, -75_968_000_000L),
                    Row.row(54967L, -75_965_083_333L),
                    Row.row(96451L, -75_963_941_176L),
                    Row.row(31211L, -75_963_750_000L));
        }
    }

    @Test
    void testQuery44ScalarReplicationJoinWithRetainedSingleRow()
            throws Exception
    {
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Method projectAverageKeysMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44AverageKeys", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method query44ThresholdPredicateMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44ThresholdPredicate", int.class, int.class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        projectAverageKeysMethod.setAccessible(true);
        filterMethod.setAccessible(true);
        query44ThresholdPredicateMethod.setAccessible(true);
        projectInputsMethod.setAccessible(true);

        List<Row> itemAggregateRows = List.of(
                Row.row(1L, 40583L, -151936L, 2L),
                Row.row(1L, 54967L, -911581L, 12L),
                Row.row(1L, 96451L, -1291387L, 17L),
                Row.row(1L, 31211L, -303855L, 4L));
        List<Row> scalarRow = List.of(Row.row(1L, -537446531L, 6367L));

        Operator joined = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, itemAggregateRows),
                0,
                new org.weakref.nitro.operator.EnforceSingleRowOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 3, scalarRow)),
                0);
        joined = (Operator) projectAverageKeysMethod.invoke(null, allocator, primitiveRegistry, joined);
        joined = (Operator) filterMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                joined,
                query44ThresholdPredicateMethod.invoke(null, 1, 2));
        Operator result = (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, joined, new int[] {0, 1});

        try (result) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(result)).containsExactly(
                    Row.row(40583L, -75_968_000_000L),
                    Row.row(54967L, -75_965_083_333L),
                    Row.row(96451L, -75_963_941_176L),
                    Row.row(31211L, -75_963_750_000L));
        }
    }

    @Test
    void testQuery44ScalarReplicationJoinWithActualScalarOperator()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Class<?> filterSpecArrayClass = java.lang.reflect.Array.newInstance(filterSpecClass, 0).getClass();
        Method equalMethod = TpcdsParquetSupport.class.getDeclaredMethod("equal", int.class, long.class);
        Method isNullMethod = TpcdsParquetSupport.class.getDeclaredMethod("isNull", int.class);
        Method andMethod = TpcdsParquetSupport.class.getDeclaredMethod("and", filterSpecClass, filterSpecClass, filterSpecArrayClass);
        Method greaterThanMethod = TpcdsParquetSupport.class.getDeclaredMethod("greaterThan", int.class, long.class);
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method filteredProjectedScanMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedScan", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method projectScalarAggregateMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44ScalarAggregate", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method projectAverageKeysMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44AverageKeys", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method query44ThresholdPredicateMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44ThresholdPredicate", int.class, int.class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        for (Method method : List.of(equalMethod, isNullMethod, andMethod, greaterThanMethod, filterMethod, filteredProjectedScanMethod, projectScalarAggregateMethod, projectAverageKeysMethod, query44ThresholdPredicateMethod, projectInputsMethod)) {
            method.setAccessible(true);
        }

        Object scalarPredicate = andMethod.invoke(
                null,
                equalMethod.invoke(null, 0, 4L),
                isNullMethod.invoke(null, 1),
                java.lang.reflect.Array.newInstance(filterSpecClass, 0));
        Operator scalarAggregate = (Operator) filteredProjectedScanMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                scalarPredicate,
                new String[] {"ss_store_sk", "ss_addr_sk", "ss_net_profit"},
                new int[] {0, 2});
        scalarAggregate = new org.weakref.nitro.operator.GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new org.weakref.nitro.operator.aggregation.Sum(1), new org.weakref.nitro.operator.aggregation.CountColumn(1)),
                scalarAggregate);
        scalarAggregate = (Operator) filterMethod.invoke(null, allocator, primitiveRegistry, scalarAggregate, greaterThanMethod.invoke(null, 2, 0L));
        scalarAggregate = new org.weakref.nitro.operator.EnforceSingleRowOperator(allocator, scalarAggregate);
        scalarAggregate = (Operator) projectScalarAggregateMethod.invoke(null, allocator, primitiveRegistry, scalarAggregate);

        List<Row> itemAggregateRows = List.of(
                Row.row(1L, 40583L, -151936L, 2L),
                Row.row(1L, 54967L, -911581L, 12L),
                Row.row(1L, 96451L, -1291387L, 17L),
                Row.row(1L, 31211L, -303855L, 4L));

        Operator joined = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, itemAggregateRows),
                0,
                scalarAggregate,
                0);
        joined = (Operator) projectAverageKeysMethod.invoke(null, allocator, primitiveRegistry, joined);
        joined = (Operator) filterMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                joined,
                query44ThresholdPredicateMethod.invoke(null, 1, 2));
        Operator result = (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, joined, new int[] {0, 1});

        try (result) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(result)).containsExactly(
                    Row.row(40583L, -75_968_000_000L),
                    Row.row(54967L, -75_965_083_333L),
                    Row.row(96451L, -75_963_941_176L),
                    Row.row(31211L, -75_963_750_000L));
        }
    }

    @Test
    void testQuery44ScalarReplicationJoinWithActualItemAggregatesOperator()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator();

        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Method equalMethod = TpcdsParquetSupport.class.getDeclaredMethod("equal", int.class, long.class);
        Method greaterThanMethod = TpcdsParquetSupport.class.getDeclaredMethod("greaterThan", int.class, long.class);
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method filteredProjectedScanMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedScan", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method projectItemAggregatesMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44ItemAggregates", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method projectAverageKeysMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44AverageKeys", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method query44ThresholdPredicateMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44ThresholdPredicate", int.class, int.class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        for (Method method : List.of(equalMethod, greaterThanMethod, filterMethod, filteredProjectedScanMethod, projectItemAggregatesMethod, projectAverageKeysMethod, query44ThresholdPredicateMethod, projectInputsMethod)) {
            method.setAccessible(true);
        }

        Object equalStore = equalMethod.invoke(null, 1, 4L);
        Operator itemAggregates = (Operator) filteredProjectedScanMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                equalStore,
                new String[] {"ss_item_sk", "ss_store_sk", "ss_net_profit"},
                new int[] {0, 2});
        itemAggregates = new org.weakref.nitro.operator.GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new org.weakref.nitro.operator.aggregation.Sum(1), new org.weakref.nitro.operator.aggregation.CountColumn(1)),
                itemAggregates);
        itemAggregates = (Operator) filterMethod.invoke(null, allocator, primitiveRegistry, itemAggregates, greaterThanMethod.invoke(null, 2, 0L));
        itemAggregates = (Operator) projectItemAggregatesMethod.invoke(null, allocator, primitiveRegistry, itemAggregates);

        Operator joined = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                itemAggregates,
                0,
                new ConstantTableOperator(allocator, 3, List.of(Row.row(1L, -537446531L, 6367L))),
                0);
        joined = (Operator) projectAverageKeysMethod.invoke(null, allocator, primitiveRegistry, joined);
        joined = (Operator) filterMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                joined,
                query44ThresholdPredicateMethod.invoke(null, 1, 2));
        Operator result = (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, joined, new int[] {0, 1});

        try (result) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(result)).contains(
                    Row.row(40583L, -75_968_000_000L),
                    Row.row(54967L, -75_965_083_333L),
                    Row.row(96451L, -75_963_941_176L),
                    Row.row(31211L, -75_963_750_000L));
        }
    }

    private static Operator groupedQuery44ItemAggregates(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            Object equalStore,
            String[] columns,
            int[] inputs,
            Method scanMethod,
            Method filterMethod,
            Method greaterThanMethod,
            Method projectQuery44ItemAggregatesMethod)
            throws Exception
    {
        Operator operator = (Operator) scanMethod.invoke(null, allocator, primitiveRegistry, tables, "store_sales", equalStore, columns, inputs);
        operator = new org.weakref.nitro.operator.GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new org.weakref.nitro.operator.aggregation.Sum(1), new org.weakref.nitro.operator.aggregation.CountColumn(1)),
                operator);
        operator = (Operator) filterMethod.invoke(null, allocator, primitiveRegistry, operator, greaterThanMethod.invoke(null, 2, 0L));
        return (Operator) projectQuery44ItemAggregatesMethod.invoke(null, allocator, primitiveRegistry, operator);
    }

    private static Method projectQuery44ItemAggregatesMethod()
            throws NoSuchMethodException
    {
        return TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44ItemAggregates", Allocator.class, PrimitiveRegistry.class, Operator.class);
    }

    private static Operator materializedQuery44RankedItems(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, boolean descending)
            throws Exception
    {
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        projectInputsMethod.setAccessible(true);

        List<Row> eligibleRows;
        try (Operator eligible = materializedQuery44EligibleRows(allocator, primitiveRegistry, tables)) {
            eligibleRows = OperatorAssertions.OperatorAssert.toRows(eligible);
        }

        Operator ranked = new org.weakref.nitro.operator.TopNRankingOperator(
                allocator,
                10,
                new int[] {1},
                new boolean[] {descending},
                new ConstantTableOperator(allocator, 2, eligibleRows));
        return (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, ranked, new int[] {0, 2});
    }

    private static Operator materializedQuery44EligibleRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
            throws Exception
    {
        Class<?> filterSpecClass = Class.forName("org.weakref.nitro.tpcds.TpcdsParquetSupport$FilterSpec");
        Class<?> filterSpecArrayClass = java.lang.reflect.Array.newInstance(filterSpecClass, 0).getClass();
        Method equalMethod = TpcdsParquetSupport.class.getDeclaredMethod("equal", int.class, long.class);
        Method greaterThanMethod = TpcdsParquetSupport.class.getDeclaredMethod("greaterThan", int.class, long.class);
        Method isNullMethod = TpcdsParquetSupport.class.getDeclaredMethod("isNull", int.class);
        Method andMethod = TpcdsParquetSupport.class.getDeclaredMethod("and", filterSpecClass, filterSpecClass, filterSpecArrayClass);
        Method filterMethod = TpcdsParquetSupport.class.getDeclaredMethod("filter", Allocator.class, PrimitiveRegistry.class, Operator.class, filterSpecClass);
        Method filteredProjectedTableMethod = TpcdsParquetSupport.class.getDeclaredMethod("filteredProjectedTable", Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class, String.class, filterSpecClass, String[].class, int[].class);
        Method projectQuery44ItemAggregatesMethod = projectQuery44ItemAggregatesMethod();
        Method projectQuery44ScalarAggregateMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44ScalarAggregate", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method projectQuery44AverageKeysMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectQuery44AverageKeys", Allocator.class, PrimitiveRegistry.class, Operator.class);
        Method query44ThresholdPredicateMethod = TpcdsParquetSupport.class.getDeclaredMethod("query44ThresholdPredicate", int.class, int.class);
        Method projectInputsMethod = TpcdsParquetSupport.class.getDeclaredMethod("projectInputs", Allocator.class, PrimitiveRegistry.class, Operator.class, int[].class);
        for (Method method : List.of(
                equalMethod,
                greaterThanMethod,
                isNullMethod,
                andMethod,
                filterMethod,
                filteredProjectedTableMethod,
                projectQuery44ItemAggregatesMethod,
                projectQuery44ScalarAggregateMethod,
                projectQuery44AverageKeysMethod,
                query44ThresholdPredicateMethod,
                projectInputsMethod)) {
            method.setAccessible(true);
        }

        return query44EligibleRows(
                allocator,
                primitiveRegistry,
                tables,
                filteredProjectedTableMethod,
                equalMethod,
                greaterThanMethod,
                isNullMethod,
                andMethod,
                filterMethod,
                projectQuery44ItemAggregatesMethod,
                projectQuery44ScalarAggregateMethod,
                projectQuery44AverageKeysMethod,
                query44ThresholdPredicateMethod,
                projectInputsMethod);
    }

    private static Operator joinedQuery44NamedRanks(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            boolean descending,
            Method rankedItemsMethod,
            Method scannedTableMethod,
            Method projectInputsMethod)
            throws Exception
    {
        Operator joined = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                (Operator) rankedItemsMethod.invoke(null, allocator, primitiveRegistry, tables, descending),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0);
        return (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, joined, new int[] {1, 3});
    }

    private static Operator materializedQuery44NamedRanks(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            boolean descending,
            Method scannedTableMethod,
            Method projectInputsMethod)
            throws Exception
    {
        List<Row> rankedRows;
        try (Operator ranked = materializedQuery44RankedItems(allocator, primitiveRegistry, tables, descending)) {
            rankedRows = OperatorAssertions.OperatorAssert.toRows(ranked);
        }

        Operator joined = new org.weakref.nitro.operator.HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, rankedRows),
                0,
                (Operator) scannedTableMethod.invoke(null, allocator, tables, "item", new String[] {"i_item_sk", "i_product_name"}),
                0);
        return (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, joined, new int[] {1, 3});
    }

    private static Object decodeStreamValue(Streams streams, int position)
    {
        org.weakref.nitro.data.BooleanVector nulls = (org.weakref.nitro.data.BooleanVector) streams.getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS);
        if (nulls != null && nulls.values()[position]) {
            return null;
        }
        return switch (streams.values()) {
            case org.weakref.nitro.data.BinaryVector vector -> new String(vector.copyBytes(position), java.nio.charset.StandardCharsets.UTF_8);
            case org.weakref.nitro.data.DictionaryVector vector -> decodeDictionaryValue(vector, position);
            case org.weakref.nitro.data.RleVector vector -> decodeRleValue(vector, position);
            default -> throw new IllegalArgumentException("Unexpected stream value type: " + streams.values().getClass().getSimpleName());
        };
    }

    private static Object decodeDictionaryValue(org.weakref.nitro.data.DictionaryVector vector, int position)
    {
        return switch (vector.values()) {
            case org.weakref.nitro.data.BinaryVector values -> new String(values.copyBytes(vector.ids()[position]), java.nio.charset.StandardCharsets.UTF_8);
            default -> throw new IllegalArgumentException("Unexpected dictionary value type: " + vector.values().getClass().getSimpleName());
        };
    }

    private static Object decodeRleValue(org.weakref.nitro.data.RleVector vector, int position)
    {
        return switch (vector.values()) {
            case org.weakref.nitro.data.BinaryVector values -> new String(values.copyBytes(vector.runIndex(position)), java.nio.charset.StandardCharsets.UTF_8);
            case org.weakref.nitro.data.DictionaryVector values -> decodeDictionaryValue(values, vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unexpected RLE value type: " + vector.values().getClass().getSimpleName());
        };
    }

    private static Operator query44EligibleRows(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            Method scanMethod,
            Method equalMethod,
            Method greaterThanMethod,
            Method isNullMethod,
            Method andMethod,
            Method filterMethod,
            Method projectQuery44ItemAggregatesMethod,
            Method projectQuery44ScalarAggregateMethod,
            Method projectQuery44AverageKeysMethod,
            Method query44ThresholdPredicateMethod,
            Method projectInputsMethod)
            throws Exception
    {
        Object equalStore = equalMethod.invoke(null, 1, 4L);
        Operator itemAggregates = groupedQuery44ItemAggregates(
                allocator,
                primitiveRegistry,
                tables,
                equalStore,
                new String[] {"ss_item_sk", "ss_store_sk", "ss_net_profit"},
                new int[] {0, 2},
                scanMethod,
                filterMethod,
                greaterThanMethod,
                projectQuery44ItemAggregatesMethod);

        Object scalarPredicate = andMethod.invoke(
                null,
                equalMethod.invoke(null, 0, 4L),
                isNullMethod.invoke(null, 1),
                java.lang.reflect.Array.newInstance(andMethod.getParameterTypes()[0], 0));
        Operator scalarAggregate = (Operator) scanMethod.invoke(
                null,
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                scalarPredicate,
                new String[] {"ss_store_sk", "ss_addr_sk", "ss_net_profit"},
                new int[] {0, 2});
        scalarAggregate = new org.weakref.nitro.operator.GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new org.weakref.nitro.operator.aggregation.Sum(1), new org.weakref.nitro.operator.aggregation.CountColumn(1)),
                scalarAggregate);
        scalarAggregate = (Operator) filterMethod.invoke(null, allocator, primitiveRegistry, scalarAggregate, greaterThanMethod.invoke(null, 2, 0L));
        scalarAggregate = new org.weakref.nitro.operator.EnforceSingleRowOperator(allocator, scalarAggregate);
        scalarAggregate = (Operator) projectQuery44ScalarAggregateMethod.invoke(null, allocator, primitiveRegistry, scalarAggregate);

        Operator joined = new org.weakref.nitro.operator.HashJoinOperator(allocator, itemAggregates, 0, scalarAggregate, 0);
        joined = (Operator) projectQuery44AverageKeysMethod.invoke(null, allocator, primitiveRegistry, joined);
        joined = (Operator) filterMethod.invoke(null, allocator, primitiveRegistry, joined, query44ThresholdPredicateMethod.invoke(null, 1, 2));
        return (Operator) projectInputsMethod.invoke(null, allocator, primitiveRegistry, joined, new int[] {0, 1});
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
    void testQuery51()
    {
        assertOperatorMatches("51", tables -> TpcdsParquetSupport.query51(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query51(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDateAndDecimalValue);
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
    void testQuery51Sql()
    {
        assertNitroMatchesSql("51", tables -> TpcdsParquetSupport.query51(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDateAndDecimalValue);
    }

    @Test
    void testQuery45TrinoSql()
    {
        assertTrinoOperatorMatchesSql("45", support -> support.query45(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery51TrinoSql()
    {
        assertTrinoOperatorMatchesSql("51", support -> support.query51(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDateAndDecimalValue);
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
    void testQuery80()
    {
        assertOperatorMatches("80", tables -> TpcdsParquetSupport.query80(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query80(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery80Sql()
    {
        assertNitroMatchesSql("80", tables -> TpcdsParquetSupport.query80(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery80TrinoSql()
    {
        assertTrinoOperatorMatchesSql("80", support -> support.query80(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
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

        try (TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            assertThat(nitroRows)
                    .as("TPC-DS Q%s Nitro vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId), valueNormalizer));
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
                TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            assertThat(normalizeTrinoRows(trinoQuery.apply(operatorSupport), valueNormalizer))
                    .as("TPC-DS Q%s Trino operator vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId), valueNormalizer));
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

        try (TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery(queryId);
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
                TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult operatorResult = trinoQuery.apply(operatorSupport);
            assertThat(operatorResult.getMaterializedRows()).hasSize(1);
            double actual = ((Number) operatorResult.getMaterializedRows().getFirst().getField(0)).doubleValue();
            assertThat(actual)
                    .as("TPC-DS Q%s Trino operator vs SQL", queryId)
                    .isCloseTo(bigDecimalValue(sqlSupport.executeBenchmarkQuery(queryId)).doubleValue(), within(1e-9));
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
        if (value instanceof java.time.LocalDate date) {
            return (int) date.toEpochDay();
        }
        if (value instanceof SqlDate date) {
            return date.getDays();
        }
        if (value instanceof SqlDecimal decimal) {
            return decimal.toBigDecimal().unscaledValue().longValueExact();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.unscaledValue().longValueExact();
        }
        return normalizeValue(value);
    }

    private static Object normalizeDateAndDecimalValue(Object value)
    {
        return normalizeDecimalCentsValue(value);
    }
}
