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
package org.weakref.nitro.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.TopNOperator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PageConsumerOperator;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTaskContext;
import org.weakref.nitro.tpcds.TpcdsParquetTables;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public final class TrinoTpcdsParquetSupport
        implements AutoCloseable
{
    private static final String TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY = "nitro.clickbench.trino.blockedWaitTimeoutSeconds";
    private static final int DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS = 5;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", ImmutableList.of());
    private static final TestingAggregationFunction BIGINT_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TrinoTpcdsParquetSupport"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TrinoTpcdsParquetSupport-scheduled"));
    private final OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());
    private final int blockedWaitTimeoutSeconds = blockedWaitTimeoutSeconds();

    public MaterializedResult query41(TpcdsParquetTables tables)
    {
        List<Path> itemFiles = tables.tableFiles("item");
        // Keep the same lowering as the Nitro harness so the side-by-side benchmark compares
        // equivalent operator assemblies, not different subquery rewrites.
        Set<String> eligibleManufacturers = query41EligibleManufacturerSlices(itemFiles).stream()
                .map(Slice::toStringUtf8)
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
        return execute(
                itemFiles,
                List.of("i_product_name", "i_manufact_id", "i_manufact"),
                List.of(
                        filterAndProjectFactory(
                                10,
                                Optional.of(and(
                                        greaterThan(1, 737),
                                        lessThan(1, 779),
                                        varcharAnyOf(2, eligibleManufacturers))),
                                identityProjections(List.of(VARCHAR, INTEGER, VARCHAR)),
                                List.of(VARCHAR, INTEGER, VARCHAR)),
                        hashAggregationFactory(12, List.of(VARCHAR), List.of(0)),
                        topNFactory(13, List.of(VARCHAR), 100, List.of(0), List.of(ASC_NULLS_LAST))),
                List.of(VARCHAR));
    }

    public MaterializedResult query62(TpcdsParquetTables tables)
    {
        ShippingBucketsLookup lookup = query62Lookup(tables);
        List<String> columns = List.of("ws_ship_date_sk", "ws_sold_date_sk", "ws_warehouse_sk", "ws_ship_mode_sk", "ws_web_site_sk");
        List<Type> factTypes = tableColumnTypes(tables, "web_sales", columns);
        List<Type> projectedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        return execute(
                tables.tableFiles("web_sales"),
                columns,
                List.of(
                        shippingBucketsProjectFactory(20, factTypes, lookup),
                        hashAggregationFactory(
                                21,
                                List.of(VARCHAR, VARCHAR, VARCHAR),
                                List.of(0, 1, 2),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty())),
                        topNFactory(22, projectedTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST))),
                projectedTypes);
    }

    public MaterializedResult query96(TpcdsParquetTables tables)
    {
        Query96Lookup lookup = query96Lookup(tables);
        List<String> columns = List.of("ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk");
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", columns);
        return execute(
                tables.tableFiles("store_sales"),
                columns,
                List.of(
                        integerDimensionFilterFactory(30, factTypes, new int[] {0, 1, 2}, new Set[] {lookup.timeKeys(), lookup.householdKeys(), lookup.storeKeys()}),
                        aggregationFactory(31, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                List.of(BIGINT));
    }

    public MaterializedResult query99(TpcdsParquetTables tables)
    {
        ShippingBucketsLookup lookup = query99Lookup(tables);
        List<String> columns = List.of("cs_ship_date_sk", "cs_sold_date_sk", "cs_warehouse_sk", "cs_ship_mode_sk", "cs_call_center_sk");
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", columns);
        List<Type> projectedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        return execute(
                tables.tableFiles("catalog_sales"),
                columns,
                List.of(
                        shippingBucketsProjectFactory(40, factTypes, lookup),
                        hashAggregationFactory(
                                41,
                                List.of(VARCHAR, VARCHAR, VARCHAR),
                                List.of(0, 1, 2),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty())),
                        topNFactory(42, projectedTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST))),
                projectedTypes);
    }

    public Set<String> query41EligibleManufacturers(TpcdsParquetTables tables)
    {
        return query41EligibleManufacturerSlices(tables.tableFiles("item")).stream()
                .map(Slice::toStringUtf8)
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
    }

    private ShippingBucketsLookup query62Lookup(TpcdsParquetTables tables)
    {
        return new ShippingBucketsLookup(
                dateKeysForMonthSequence(tables, 1200, 1211),
                prefixedUtf8Map(scanTable(tables, "warehouse", List.of("w_warehouse_sk", "w_warehouse_name"), tableColumnTypes(tables, "warehouse", List.of("w_warehouse_sk", "w_warehouse_name"))), 0, 1, 20),
                utf8Map(scanTable(tables, "ship_mode", List.of("sm_ship_mode_sk", "sm_type"), tableColumnTypes(tables, "ship_mode", List.of("sm_ship_mode_sk", "sm_type"))), 0, 1),
                utf8Map(scanTable(tables, "web_site", List.of("web_site_sk", "web_name"), tableColumnTypes(tables, "web_site", List.of("web_site_sk", "web_name"))), 0, 1));
    }

    private Query96Lookup query96Lookup(TpcdsParquetTables tables)
    {
        return new Query96Lookup(
                integerKeySet(scanTable(tables, "time_dim", List.of("t_time_sk", "t_hour", "t_minute"), tableColumnTypes(tables, "time_dim", List.of("t_time_sk", "t_hour", "t_minute"))),
                        row -> ((Number) row.getField(1)).intValue() == 20 && ((Number) row.getField(2)).intValue() >= 30),
                integerKeySet(scanTable(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count"), tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count"))),
                        row -> ((Number) row.getField(1)).intValue() == 7),
                integerKeySet(scanTable(tables, "store", List.of("s_store_sk", "s_store_name"), tableColumnTypes(tables, "store", List.of("s_store_sk", "s_store_name"))),
                        row -> "ese".equals(row.getField(1))));
    }

    private ShippingBucketsLookup query99Lookup(TpcdsParquetTables tables)
    {
        return new ShippingBucketsLookup(
                dateKeysForMonthSequence(tables, 1200, 1211),
                prefixedUtf8Map(scanTable(tables, "warehouse", List.of("w_warehouse_sk", "w_warehouse_name"), tableColumnTypes(tables, "warehouse", List.of("w_warehouse_sk", "w_warehouse_name"))), 0, 1, 20),
                utf8Map(scanTable(tables, "ship_mode", List.of("sm_ship_mode_sk", "sm_type"), tableColumnTypes(tables, "ship_mode", List.of("sm_ship_mode_sk", "sm_type"))), 0, 1),
                utf8Map(scanTable(tables, "call_center", List.of("cc_call_center_sk", "cc_name"), tableColumnTypes(tables, "call_center", List.of("cc_call_center_sk", "cc_name"))), 0, 1));
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private Set<Slice> query41EligibleManufacturerSlices(List<Path> itemFiles)
    {
        MaterializedResult result = execute(
                itemFiles,
                List.of("i_manufact", "i_category", "i_color", "i_units", "i_size"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                Optional.of(query41EligibilityPredicate()),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)),
                        hashAggregationFactory(2, List.of(VARCHAR), List.of(0))),
                List.of(VARCHAR));
        Set<Slice> manufacturers = new HashSet<>();
        result.getMaterializedRows().forEach(row -> manufacturers.add(Slices.utf8Slice((String) row.getField(0))));
        return manufacturers;
    }

    private MaterializedResult execute(List<Path> files, List<String> columns, List<OperatorFactory> factories, List<Type> outputTypes)
    {
        List<Page> outputPages = new ArrayList<>();
        try (TrinoClickBenchPageReader reader = new TrinoClickBenchPageReader(files, columns)) {
            DriverContext driverContext = taskContext().addPipelineContext(0, true, true, false).addDriverContext();
            List<Operator> operators = new ArrayList<>();
            TrinoPageSequenceSourceOperator.Factory sourceFactory = new TrinoPageSequenceSourceOperator.Factory(0, new PlanNodeId("source"), reader);
            operators.add(sourceFactory.createOperator(driverContext));

            for (OperatorFactory factory : factories) {
                operators.add(factory.createOperator(driverContext));
                factory.noMoreOperators();
            }

            operators.add(new PageConsumerOperator(
                    driverContext.addOperatorContext(1000, new PlanNodeId("sink"), PageConsumerOperator.class.getSimpleName()),
                    outputPages::add,
                    java.util.function.Function.identity()));

            try (Driver driver = Driver.createDriver(driverContext, operators)) {
                processDriver(driver, operators);
            }
            catch (Exception exception) {
                throw new RuntimeException("Unable to execute Trino TPC-DS parquet pipeline", exception);
            }

            MaterializedResult.Builder result = MaterializedResult.resultBuilder(driverContext.getSession(), outputTypes);
            for (Page page : outputPages) {
                result.page(page);
            }
            return result.build();
        }
    }

    private MaterializedResult scanTable(TpcdsParquetTables tables, String tableName, List<String> columns, List<Type> outputTypes)
    {
        return execute(tables.tableFiles(tableName), columns, List.of(), outputTypes);
    }

    private List<Type> tableColumnTypes(TpcdsParquetTables tables, String tableName, List<String> columns)
    {
        return TrinoClickBenchPageReader.columnTypes(tables.tableFiles(tableName).getFirst(), columns);
    }

    private Set<Integer> dateKeysForMonthSequence(TpcdsParquetTables tables, int minimumMonthSequence, int maximumMonthSequence)
    {
        return integerKeySet(
                scanTable(tables, "date_dim", List.of("d_date_sk", "d_month_seq"), tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_month_seq"))),
                row -> {
                    int monthSequence = ((Number) row.getField(1)).intValue();
                    return monthSequence >= minimumMonthSequence && monthSequence <= maximumMonthSequence;
                });
    }

    private static Set<Integer> integerKeySet(MaterializedResult result, java.util.function.Predicate<io.trino.testing.MaterializedRow> predicate)
    {
        Set<Integer> values = new HashSet<>();
        for (io.trino.testing.MaterializedRow row : result.getMaterializedRows()) {
            if (predicate.test(row)) {
                values.add(((Number) row.getField(0)).intValue());
            }
        }
        return values;
    }

    private static Map<Integer, String> utf8Map(MaterializedResult result, int keyIndex, int valueIndex)
    {
        Map<Integer, String> values = new HashMap<>();
        result.getMaterializedRows().forEach(row -> values.put(((Number) row.getField(keyIndex)).intValue(), (String) row.getField(valueIndex)));
        return values;
    }

    private static Map<Integer, String> prefixedUtf8Map(MaterializedResult result, int keyIndex, int valueIndex, int prefixLength)
    {
        Map<Integer, String> values = new HashMap<>();
        result.getMaterializedRows().forEach(row -> {
            String value = (String) row.getField(valueIndex);
            values.put(((Number) row.getField(keyIndex)).intValue(), value == null ? null : value.substring(0, Math.min(prefixLength, value.length())));
        });
        return values;
    }

    private io.trino.operator.TaskContext taskContext()
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TestingSession.testSessionBuilder().build())
                .setQueryMaxMemory(DataSize.of(4, GIGABYTE))
                .setMemoryPoolSize(DataSize.of(4, GIGABYTE))
                .build();
    }

    private void processDriver(Driver driver, List<Operator> operators)
            throws Exception
    {
        while (!driver.isFinished()) {
            ListenableFuture<Void> blocked = driver.processUntilBlocked();
            if (!blocked.isDone()) {
                waitForBlocked(blocked);
            }
        }
    }

    private AggregationOperatorFactory aggregationFactory(int operatorId, io.trino.operator.aggregation.AggregatorFactory... aggregators)
    {
        return new AggregationOperatorFactory(operatorId, new PlanNodeId("aggregation-" + operatorId), List.of(aggregators));
    }

    private HashAggregationOperatorFactory hashAggregationFactory(int operatorId, List<Type> groupTypes, List<Integer> groupChannels, io.trino.operator.aggregation.AggregatorFactory... aggregators)
    {
        return new HashAggregationOperatorFactory(
                operatorId,
                new PlanNodeId("grouped-aggregation-" + operatorId),
                groupTypes,
                groupChannels,
                List.of(),
                Step.SINGLE,
                List.of(aggregators),
                OptionalInt.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                hashStrategyCompiler,
                Optional.empty());
    }

    private OperatorFactory topNFactory(int operatorId, List<Type> types, int n, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .toList();
        return TopNOperator.createOperatorFactory(
                operatorId,
                new PlanNodeId("topn-" + operatorId),
                types,
                n,
                orderingCompiler.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrders));
    }

    private OperatorFactory integerDimensionFilterFactory(int operatorId, List<Type> inputTypes, int[] inputChannels, Set<Integer>[] allowedValues)
    {
        return new IntegerDimensionFilterOperator.Factory(operatorId, new PlanNodeId("int-filter-" + operatorId), inputTypes, inputChannels, allowedValues);
    }

    private OperatorFactory shippingBucketsProjectFactory(int operatorId, List<Type> inputTypes, ShippingBucketsLookup lookup)
    {
        return new ShippingBucketsProjectOperator.Factory(operatorId, new PlanNodeId("shipping-buckets-" + operatorId), inputTypes, lookup);
    }

    private OperatorFactory filterAndProjectFactory(int operatorId, Optional<RowExpression> filter, List<RowExpression> projections, List<Type> outputTypes)
    {
        return FilterAndProjectOperator.createOperatorFactory(
                operatorId,
                new PlanNodeId("filter-project-" + operatorId),
                FUNCTION_RESOLUTION.getExpressionCompiler().compilePageProcessor(filter, projections),
                outputTypes,
                DataSize.of(1, MEGABYTE),
                1);
    }

    private static List<RowExpression> identityProjections(List<Type> types)
    {
        List<RowExpression> projections = new ArrayList<>(types.size());
        for (int index = 0; index < types.size(); index++) {
            projections.add(field(index, types.get(index)));
        }
        return projections;
    }

    private static RowExpression query41EligibilityPredicate()
    {
        RowExpression womenPowderKhaki = and(
                equal(1, VARCHAR, "Women"),
                or(equal(2, VARCHAR, "powder"), equal(2, VARCHAR, "khaki")),
                or(equal(3, VARCHAR, "Ounce"), equal(3, VARCHAR, "Oz")),
                or(equal(4, VARCHAR, "medium"), equal(4, VARCHAR, "extra large")));
        RowExpression womenBrownHoneydew = and(
                equal(1, VARCHAR, "Women"),
                or(equal(2, VARCHAR, "brown"), equal(2, VARCHAR, "honeydew")),
                or(equal(3, VARCHAR, "Bunch"), equal(3, VARCHAR, "Ton")),
                or(equal(4, VARCHAR, "N/A"), equal(4, VARCHAR, "small")));
        RowExpression menFloralDeep = and(
                equal(1, VARCHAR, "Men"),
                or(equal(2, VARCHAR, "floral"), equal(2, VARCHAR, "deep")),
                or(equal(3, VARCHAR, "N/A"), equal(3, VARCHAR, "Dozen")),
                or(equal(4, VARCHAR, "petite"), equal(4, VARCHAR, "large")));
        RowExpression menLightCornflower = and(
                equal(1, VARCHAR, "Men"),
                or(equal(2, VARCHAR, "light"), equal(2, VARCHAR, "cornflower")),
                or(equal(3, VARCHAR, "Box"), equal(3, VARCHAR, "Pound")),
                or(equal(4, VARCHAR, "medium"), equal(4, VARCHAR, "extra large")));
        RowExpression womenMidnightSnow = and(
                equal(1, VARCHAR, "Women"),
                or(equal(2, VARCHAR, "midnight"), equal(2, VARCHAR, "snow")),
                or(equal(3, VARCHAR, "Pallet"), equal(3, VARCHAR, "Gross")),
                or(equal(4, VARCHAR, "medium"), equal(4, VARCHAR, "extra large")));
        RowExpression womenCyanPapaya = and(
                equal(1, VARCHAR, "Women"),
                or(equal(2, VARCHAR, "cyan"), equal(2, VARCHAR, "papaya")),
                or(equal(3, VARCHAR, "Cup"), equal(3, VARCHAR, "Dram")),
                or(equal(4, VARCHAR, "N/A"), equal(4, VARCHAR, "small")));
        RowExpression menOrangeFrosted = and(
                equal(1, VARCHAR, "Men"),
                or(equal(2, VARCHAR, "orange"), equal(2, VARCHAR, "frosted")),
                or(equal(3, VARCHAR, "Each"), equal(3, VARCHAR, "Tbl")),
                or(equal(4, VARCHAR, "petite"), equal(4, VARCHAR, "large")));
        RowExpression menForestGhost = and(
                equal(1, VARCHAR, "Men"),
                or(equal(2, VARCHAR, "forest"), equal(2, VARCHAR, "ghost")),
                or(equal(3, VARCHAR, "Lb"), equal(3, VARCHAR, "Bundle")),
                or(equal(4, VARCHAR, "medium"), equal(4, VARCHAR, "extra large")));
        return or(
                womenPowderKhaki,
                womenBrownHoneydew,
                menFloralDeep,
                menLightCornflower,
                womenMidnightSnow,
                womenCyanPapaya,
                menOrangeFrosted,
                menForestGhost);
    }

    private static RowExpression varcharAnyOf(int inputChannel, Set<String> values)
    {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values is empty");
        }

        List<String> sortedValues = values.stream()
                .sorted()
                .toList();

        RowExpression result = equal(inputChannel, VARCHAR, sortedValues.getFirst());
        for (int index = 1; index < sortedValues.size(); index++) {
            result = or(result, equal(inputChannel, VARCHAR, sortedValues.get(index)));
        }
        return result;
    }

    private static RowExpression equal(int inputChannel, Type type, String constantValue)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(field(inputChannel, type), constant(Slices.utf8Slice(constantValue), type)));
    }

    private static RowExpression greaterThan(int inputChannel, long constantValue)
    {
        return lessThan(constant(constantValue, INTEGER), field(inputChannel, INTEGER), INTEGER);
    }

    private static RowExpression lessThan(int inputChannel, long constantValue)
    {
        return lessThan(field(inputChannel, INTEGER), constant(constantValue, INTEGER), INTEGER);
    }

    private static RowExpression lessThan(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression and(RowExpression first, RowExpression second, RowExpression... rest)
    {
        RowExpression result = new SpecialForm(SpecialForm.Form.AND, BOOLEAN, List.of(first, second), List.of());
        for (RowExpression expression : rest) {
            result = new SpecialForm(SpecialForm.Form.AND, BOOLEAN, List.of(result, expression), List.of());
        }
        return result;
    }

    private static RowExpression or(RowExpression first, RowExpression second, RowExpression... rest)
    {
        RowExpression result = new SpecialForm(SpecialForm.Form.OR, BOOLEAN, List.of(first, second), List.of());
        for (RowExpression expression : rest) {
            result = new SpecialForm(SpecialForm.Form.OR, BOOLEAN, List.of(result, expression), List.of());
        }
        return result;
    }

    private record Query96Lookup(Set<Integer> timeKeys, Set<Integer> householdKeys, Set<Integer> storeKeys) {}

    private record ShippingBucketsLookup(Set<Integer> allowedShipDates, Map<Integer, String> firstNames, Map<Integer, String> secondNames, Map<Integer, String> thirdNames) {}

    private static final class IntegerDimensionFilterOperator
            implements Operator
    {
        static final class Factory
                implements OperatorFactory
        {
            private final int operatorId;
            private final PlanNodeId planNodeId;
            private final List<Type> inputTypes;
            private final int[] inputChannels;
            private final Set<Integer>[] allowedValues;
            private boolean closed;

            @SuppressWarnings("unchecked")
            private Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, int[] inputChannels, Set<Integer>[] allowedValues)
            {
                this.operatorId = operatorId;
                this.planNodeId = planNodeId;
                this.inputTypes = List.copyOf(inputTypes);
                this.inputChannels = inputChannels.clone();
                this.allowedValues = allowedValues.clone();
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                if (closed) {
                    throw new IllegalStateException("Factory is already closed");
                }
                return new IntegerDimensionFilterOperator(
                        driverContext.addOperatorContext(operatorId, planNodeId, IntegerDimensionFilterOperator.class.getSimpleName()),
                        inputTypes,
                        inputChannels,
                        allowedValues);
            }

            @Override
            public void noMoreOperators()
            {
                closed = true;
            }

            @Override
            public OperatorFactory duplicate()
            {
                return new Factory(operatorId, planNodeId, inputTypes, inputChannels, allowedValues);
            }
        }

        private final io.trino.operator.OperatorContext operatorContext;
        private final List<Type> inputTypes;
        private final int[] inputChannels;
        private final Set<Integer>[] allowedValues;

        private Page outputPage;
        private boolean finishing;

        @SuppressWarnings("unchecked")
        private IntegerDimensionFilterOperator(io.trino.operator.OperatorContext operatorContext, List<Type> inputTypes, int[] inputChannels, Set<Integer>[] allowedValues)
        {
            this.operatorContext = operatorContext;
            this.inputTypes = List.copyOf(inputTypes);
            this.inputChannels = inputChannels.clone();
            this.allowedValues = allowedValues.clone();
        }

        @Override
        public io.trino.operator.OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public void finish()
        {
            finishing = true;
        }

        @Override
        public boolean isFinished()
        {
            return finishing && outputPage == null;
        }

        @Override
        public boolean needsInput()
        {
            return !finishing && outputPage == null;
        }

        @Override
        public void addInput(Page page)
        {
            if (!needsInput()) {
                throw new IllegalStateException("Operator does not need input");
            }

            int selectedCount = 0;
            for (int position = 0; position < page.getPositionCount(); position++) {
                boolean keep = true;
                for (int index = 0; index < inputChannels.length; index++) {
                    int value = readInt(inputTypes.get(inputChannels[index]), page, inputChannels[index], position);
                    if (!allowedValues[index].contains(value)) {
                        keep = false;
                        break;
                    }
                }
                if (keep) {
                    selectedCount++;
                }
            }

            if (selectedCount == 0) {
                outputPage = null;
                return;
            }
            outputPage = new Page(selectedCount);
        }

        @Override
        public Page getOutput()
        {
            Page page = outputPage;
            outputPage = null;
            return page;
        }
    }

    private static final class ShippingBucketsProjectOperator
            implements Operator
    {
        static final class Factory
                implements OperatorFactory
        {
            private final int operatorId;
            private final PlanNodeId planNodeId;
            private final List<Type> inputTypes;
            private final ShippingBucketsLookup lookup;
            private boolean closed;

            private Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, ShippingBucketsLookup lookup)
            {
                this.operatorId = operatorId;
                this.planNodeId = planNodeId;
                this.inputTypes = List.copyOf(inputTypes);
                this.lookup = lookup;
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                if (closed) {
                    throw new IllegalStateException("Factory is already closed");
                }
                return new ShippingBucketsProjectOperator(
                        driverContext.addOperatorContext(operatorId, planNodeId, ShippingBucketsProjectOperator.class.getSimpleName()),
                        inputTypes,
                        lookup);
            }

            @Override
            public void noMoreOperators()
            {
                closed = true;
            }

            @Override
            public OperatorFactory duplicate()
            {
                return new Factory(operatorId, planNodeId, inputTypes, lookup);
            }
        }

        private final io.trino.operator.OperatorContext operatorContext;
        private final List<Type> inputTypes;
        private final ShippingBucketsLookup lookup;

        private Page outputPage;
        private boolean finishing;

        private ShippingBucketsProjectOperator(io.trino.operator.OperatorContext operatorContext, List<Type> inputTypes, ShippingBucketsLookup lookup)
        {
            this.operatorContext = operatorContext;
            this.inputTypes = List.copyOf(inputTypes);
            this.lookup = lookup;
        }

        @Override
        public io.trino.operator.OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public void finish()
        {
            finishing = true;
        }

        @Override
        public boolean isFinished()
        {
            return finishing && outputPage == null;
        }

        @Override
        public boolean needsInput()
        {
            return !finishing && outputPage == null;
        }

        @Override
        public void addInput(Page page)
        {
            if (!needsInput()) {
                throw new IllegalStateException("Operator does not need input");
            }

            PageBuilder pageBuilder = new PageBuilder(List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT));
            for (int position = 0; position < page.getPositionCount(); position++) {
                int shipDate = readInt(inputTypes.get(0), page, 0, position);
                if (!lookup.allowedShipDates().contains(shipDate)) {
                    continue;
                }

                int firstKey = readInt(inputTypes.get(2), page, 2, position);
                int secondKey = readInt(inputTypes.get(3), page, 3, position);
                int thirdKey = readInt(inputTypes.get(4), page, 4, position);
                if (!lookup.firstNames().containsKey(firstKey) || !lookup.secondNames().containsKey(secondKey) || !lookup.thirdNames().containsKey(thirdKey)) {
                    continue;
                }
                String first = lookup.firstNames().get(firstKey);
                String second = lookup.secondNames().get(secondKey);
                String third = lookup.thirdNames().get(thirdKey);

                int days = shipDate - readInt(inputTypes.get(1), page, 1, position);

                pageBuilder.declarePosition();
                if (first == null) {
                    pageBuilder.getBlockBuilder(0).appendNull();
                }
                else {
                    VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), Slices.utf8Slice(first));
                }
                if (second == null) {
                    pageBuilder.getBlockBuilder(1).appendNull();
                }
                else {
                    VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice(second));
                }
                if (third == null) {
                    pageBuilder.getBlockBuilder(2).appendNull();
                }
                else {
                    VARCHAR.writeSlice(pageBuilder.getBlockBuilder(2), Slices.utf8Slice(third));
                }
                BIGINT.writeLong(pageBuilder.getBlockBuilder(3), days <= 30 ? 1 : 0);
                BIGINT.writeLong(pageBuilder.getBlockBuilder(4), days > 30 && days <= 60 ? 1 : 0);
                BIGINT.writeLong(pageBuilder.getBlockBuilder(5), days > 60 && days <= 90 ? 1 : 0);
                BIGINT.writeLong(pageBuilder.getBlockBuilder(6), days > 90 && days <= 120 ? 1 : 0);
                BIGINT.writeLong(pageBuilder.getBlockBuilder(7), days > 120 ? 1 : 0);
            }

            outputPage = pageBuilder.isEmpty() ? null : pageBuilder.build();
        }

        @Override
        public Page getOutput()
        {
            Page page = outputPage;
            outputPage = null;
            return page;
        }
    }

    private static int readInt(Type type, Page page, int channel, int position)
    {
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(page.getBlock(channel), position);
        }
        if (type.equals(BIGINT)) {
            return toIntExact(BIGINT.getLong(page.getBlock(channel), position));
        }
        throw new IllegalArgumentException("Expected integer-like type but found " + type);
    }

    private static int blockedWaitTimeoutSeconds()
    {
        String configured = System.getProperty(TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS;
        }
        return Integer.parseInt(configured);
    }

    private void waitForBlocked(ListenableFuture<Void> blocked)
            throws Exception
    {
        try {
            blocked.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS);
        }
        catch (TimeoutException exception) {
            throw new IllegalStateException("Timed out waiting for blocked Trino TPC-DS driver", exception);
        }
    }

    private static ThreadFactory daemonThreadsNamed(String nameFormat)
    {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, nameFormat + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
