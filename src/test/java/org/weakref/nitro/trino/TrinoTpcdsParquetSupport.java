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
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
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
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
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
                        integerDimensionFilterFactory(30, factTypes, new int[] {0, 1, 2}, new IntSet[] {lookup.timeKeys(), lookup.householdKeys(), lookup.storeKeys()}),
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

    public MaterializedResult query10(TpcdsParquetTables tables)
    {
        Query10Lookup lookup = query10Lookup(tables);
        List<Type> groupTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        return execute(
                tables.tableFiles("customer"),
                List.of("c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk"),
                List.of(
                        customerDemographicsProjectFactory(50, tableColumnTypes(tables, "customer", List.of("c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk")), lookup),
                        hashAggregationFactory(51, groupTypes, List.of(0, 1, 2, 3, 4, 5, 6, 7), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        filterAndProjectFactory(
                                52,
                                Optional.empty(),
                                List.of(
                                        field(0, VARCHAR),
                                        field(1, VARCHAR),
                                        field(2, VARCHAR),
                                        field(8, BIGINT),
                                        field(3, BIGINT),
                                        field(8, BIGINT),
                                        field(4, VARCHAR),
                                        field(8, BIGINT),
                                        field(5, BIGINT),
                                        field(8, BIGINT),
                                        field(6, BIGINT),
                                        field(8, BIGINT),
                                        field(7, BIGINT),
                                        field(8, BIGINT)),
                                outputTypes),
                        topNFactory(53, outputTypes, 100, List.of(0, 1, 2, 4, 6, 8, 10, 12), List.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST))),
                outputTypes);
    }

    public MaterializedResult query73(TpcdsParquetTables tables)
    {
        Query73Lookup lookup = query73Lookup(tables);
        List<Type> inputTypes = tableColumnTypes(tables, "store_sales", List.of("ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk"));
        List<Type> projectedTypes = List.of(BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT);
        return execute(
                tables.tableFiles("store_sales"),
                List.of("ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk"),
                List.of(
                        ticketCustomerFilterProjectFactory(60, inputTypes, lookup),
                        hashAggregationFactory(61, projectedTypes, List.of(0, 1), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        customerTicketProjectFactory(62, List.of(BIGINT, BIGINT, BIGINT), lookup.customers()),
                        topNFactory(63, outputTypes, 100, List.of(5, 0, 4), List.of(io.trino.spi.connector.SortOrder.DESC_NULLS_LAST, ASC_NULLS_FIRST, ASC_NULLS_FIRST))),
                outputTypes);
    }

    public MaterializedResult query88(TpcdsParquetTables tables)
    {
        Query88Lookup lookup = query88Lookup(tables);
        long[] counts = new long[8];
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"));
        for (int bucket = 0; bucket < counts.length; bucket++) {
            counts[bucket] = singleLongResult(execute(
                    tables.tableFiles("store_sales"),
                    List.of("ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"),
                    List.of(
                            integerDimensionFilterFactory(70 + (bucket * 2), factTypes, new int[] {0, 1, 2}, new IntSet[] {lookup.timeBucketKeys()[bucket], lookup.allowedHouseholdKeys(), lookup.allowedStoreKeys()}),
                            aggregationFactory(71 + (bucket * 2), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                    List.of(BIGINT)));
        }

        MaterializedResult.Builder result = MaterializedResult.resultBuilder(TestingSession.testSessionBuilder().build(), List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT));
        result.row(counts[0], counts[1], counts[2], counts[3], counts[4], counts[5], counts[6], counts[7]);
        return result.build();
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

    private Query10Lookup query10Lookup(TpcdsParquetTables tables)
    {
        IntSet eligibleDates = dateKeysForYearMonthRange(tables, 2002, 1, 4);
        return new Query10Lookup(
                addressKeysForCounties(tables, "Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"),
                customerKeysForDates(tables, "store_sales", List.of("ss_customer_sk", "ss_sold_date_sk"), eligibleDates),
                customerKeysForDates(tables, "web_sales", List.of("ws_bill_customer_sk", "ws_sold_date_sk"), eligibleDates),
                customerKeysForDates(tables, "catalog_sales", List.of("cs_ship_customer_sk", "cs_sold_date_sk"), eligibleDates),
                customerDemographicsLookup(tables));
    }

    private Query73Lookup query73Lookup(TpcdsParquetTables tables)
    {
        return new Query73Lookup(
                dateKeysForDayOfMonthAndYears(tables, 1, 2, 1999, 2000, 2001),
                storeKeysForCounties(tables, "Williamson County", "Franklin Parish", "Bronx County", "Orange County"),
                householdKeysForQuery73(tables),
                customerIdentityLookup(tables));
    }

    @SuppressWarnings("unchecked")
    private Query88Lookup query88Lookup(TpcdsParquetTables tables)
    {
        return new Query88Lookup(
                timeBucketKeysForQuery88(tables),
                householdKeysForQuery88(tables),
                storeKeysByName(tables, "ese"));
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

    private IntSet dateKeysForMonthSequence(TpcdsParquetTables tables, int minimumMonthSequence, int maximumMonthSequence)
    {
        return integerKeySet(
                scanTable(tables, "date_dim", List.of("d_date_sk", "d_month_seq"), tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_month_seq"))),
                row -> {
                    int monthSequence = ((Number) row.getField(1)).intValue();
                    return monthSequence >= minimumMonthSequence && monthSequence <= maximumMonthSequence;
                });
    }

    private IntSet dateKeysForYearMonthRange(TpcdsParquetTables tables, int year, int minimumMonthOfYear, int maximumMonthOfYear)
    {
        return integerKeySet(
                scanTable(tables, "date_dim", List.of("d_date_sk", "d_year", "d_moy"), tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_year", "d_moy"))),
                row -> ((Number) row.getField(1)).intValue() == year &&
                        ((Number) row.getField(2)).intValue() >= minimumMonthOfYear &&
                        ((Number) row.getField(2)).intValue() <= maximumMonthOfYear);
    }

    private IntSet dateKeysForDayOfMonthAndYears(TpcdsParquetTables tables, int minimumDayOfMonth, int maximumDayOfMonth, int... years)
    {
        IntSet allowedYears = new IntOpenHashSet(years);
        return integerKeySet(
                scanTable(tables, "date_dim", List.of("d_date_sk", "d_dom", "d_year"), tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_dom", "d_year"))),
                row -> ((Number) row.getField(1)).intValue() >= minimumDayOfMonth &&
                        ((Number) row.getField(1)).intValue() <= maximumDayOfMonth &&
                        allowedYears.contains(((Number) row.getField(2)).intValue()));
    }

    private IntSet customerKeysForDates(TpcdsParquetTables tables, String tableName, List<String> columns, IntSet allowedDates)
    {
        return integerKeySet(
                scanTable(tables, tableName, columns, tableColumnTypes(tables, tableName, columns)),
                row -> row.getField(0) != null &&
                        row.getField(1) != null &&
                        allowedDates.contains(((Number) row.getField(1)).intValue()));
    }

    private IntSet addressKeysForCounties(TpcdsParquetTables tables, String... counties)
    {
        Set<String> allowedCounties = Set.of(counties);
        return integerKeySet(
                scanTable(tables, "customer_address", List.of("ca_address_sk", "ca_county"), tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_county"))),
                row -> {
                    String county = (String) row.getField(1);
                    return county != null && allowedCounties.contains(county);
                });
    }

    private IntSet storeKeysForCounties(TpcdsParquetTables tables, String... counties)
    {
        Set<String> allowedCounties = Set.of(counties);
        return integerKeySet(
                scanTable(tables, "store", List.of("s_store_sk", "s_county"), tableColumnTypes(tables, "store", List.of("s_store_sk", "s_county"))),
                row -> {
                    String county = (String) row.getField(1);
                    return county != null && allowedCounties.contains(county);
                });
    }

    private IntSet storeKeysByName(TpcdsParquetTables tables, String storeName)
    {
        return integerKeySet(
                scanTable(tables, "store", List.of("s_store_sk", "s_store_name"), tableColumnTypes(tables, "store", List.of("s_store_sk", "s_store_name"))),
                row -> {
                    String name = (String) row.getField(1);
                    return name != null && storeName.equals(name);
                });
    }

    private IntSet householdKeysForQuery73(TpcdsParquetTables tables)
    {
        return integerKeySet(
                scanTable(tables, "household_demographics", List.of("hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"), tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"))),
                row -> {
                    String buyPotential = (String) row.getField(1);
                    int vehicleCount = ((Number) row.getField(2)).intValue();
                    int dependentCount = ((Number) row.getField(3)).intValue();
                    return (">10000".equals(buyPotential) || "Unknown".equals(buyPotential)) &&
                            vehicleCount > 0 &&
                            ((double) dependentCount / vehicleCount) > 1.0;
                });
    }

    private IntSet householdKeysForQuery88(TpcdsParquetTables tables)
    {
        return integerKeySet(
                scanTable(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"), tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"))),
                row -> {
                    int dependentCount = ((Number) row.getField(1)).intValue();
                    int vehicleCount = ((Number) row.getField(2)).intValue();
                    return (dependentCount == 4 || dependentCount == 2 || dependentCount == 0) &&
                            vehicleCount <= (dependentCount + 2);
                });
    }

    @SuppressWarnings("unchecked")
    private IntSet[] timeBucketKeysForQuery88(TpcdsParquetTables tables)
    {
        IntSet[] buckets = new IntSet[8];
        for (int bucket = 0; bucket < buckets.length; bucket++) {
            buckets[bucket] = new IntOpenHashSet();
        }
        scanTable(tables, "time_dim", List.of("t_time_sk", "t_hour", "t_minute"), tableColumnTypes(tables, "time_dim", List.of("t_time_sk", "t_hour", "t_minute")))
                .getMaterializedRows()
                .forEach(row -> {
                    int hour = ((Number) row.getField(1)).intValue();
                    int minute = ((Number) row.getField(2)).intValue();
                    int bucket = switch (hour) {
                        case 8 -> minute >= 30 ? 0 : -1;
                        case 9 -> minute < 30 ? 1 : 2;
                        case 10 -> minute < 30 ? 3 : 4;
                        case 11 -> minute < 30 ? 5 : 6;
                        case 12 -> minute < 30 ? 7 : -1;
                        default -> -1;
                    };
                    if (bucket >= 0) {
                        buckets[bucket].add(((Number) row.getField(0)).intValue());
                    }
                });
        return buckets;
    }

    private Map<Integer, CustomerDemographicsRecord> customerDemographicsLookup(TpcdsParquetTables tables)
    {
        Map<Integer, CustomerDemographicsRecord> values = new HashMap<>();
        scanTable(tables, "customer_demographics",
                List.of("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"),
                tableColumnTypes(tables, "customer_demographics", List.of("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count")))
                .getMaterializedRows()
                .forEach(row -> values.put(
                        ((Number) row.getField(0)).intValue(),
                        new CustomerDemographicsRecord(
                                (String) row.getField(1),
                                (String) row.getField(2),
                                (String) row.getField(3),
                                ((Number) row.getField(4)).longValue(),
                                (String) row.getField(5),
                                ((Number) row.getField(6)).longValue(),
                                ((Number) row.getField(7)).longValue(),
                                ((Number) row.getField(8)).longValue())));
        return values;
    }

    private Map<Integer, CustomerIdentityRecord> customerIdentityLookup(TpcdsParquetTables tables)
    {
        Map<Integer, CustomerIdentityRecord> values = new HashMap<>();
        scanTable(tables, "customer",
                List.of("c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag"),
                tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag")))
                .getMaterializedRows()
                .forEach(row -> values.put(
                        ((Number) row.getField(0)).intValue(),
                        new CustomerIdentityRecord(
                                (String) row.getField(1),
                                (String) row.getField(2),
                                (String) row.getField(3),
                                (String) row.getField(4))));
        return values;
    }

    private static long singleLongResult(MaterializedResult result)
    {
        if (result.getMaterializedRows().size() != 1) {
            throw new IllegalStateException("Expected exactly one row but found " + result.getMaterializedRows().size());
        }
        return ((Number) result.getMaterializedRows().getFirst().getField(0)).longValue();
    }

    private static IntSet integerKeySet(MaterializedResult result, java.util.function.Predicate<io.trino.testing.MaterializedRow> predicate)
    {
        IntSet values = new IntOpenHashSet();
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

    private OperatorFactory integerDimensionFilterFactory(int operatorId, List<Type> inputTypes, int[] inputChannels, IntSet[] allowedValues)
    {
        return new IntegerDimensionFilterOperator.Factory(operatorId, new PlanNodeId("int-filter-" + operatorId), inputTypes, inputChannels, allowedValues);
    }

    private OperatorFactory shippingBucketsProjectFactory(int operatorId, List<Type> inputTypes, ShippingBucketsLookup lookup)
    {
        return new ShippingBucketsProjectOperator.Factory(operatorId, new PlanNodeId("shipping-buckets-" + operatorId), inputTypes, lookup);
    }

    private OperatorFactory customerDemographicsProjectFactory(int operatorId, List<Type> inputTypes, Query10Lookup lookup)
    {
        return new CustomerDemographicsProjectOperator.Factory(operatorId, new PlanNodeId("customer-demographics-" + operatorId), inputTypes, lookup);
    }

    private OperatorFactory ticketCustomerFilterProjectFactory(int operatorId, List<Type> inputTypes, Query73Lookup lookup)
    {
        return new TicketCustomerFilterProjectOperator.Factory(operatorId, new PlanNodeId("ticket-customer-" + operatorId), inputTypes, lookup);
    }

    private OperatorFactory customerTicketProjectFactory(int operatorId, List<Type> inputTypes, Map<Integer, CustomerIdentityRecord> customers)
    {
        return new CustomerTicketProjectOperator.Factory(operatorId, new PlanNodeId("customer-ticket-" + operatorId), inputTypes, customers);
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

    private record Query10Lookup(
            IntSet eligibleAddressKeys,
            IntSet storeCustomerKeys,
            IntSet webCustomerKeys,
            IntSet catalogCustomerKeys,
            Map<Integer, CustomerDemographicsRecord> demographics) {}

    private record Query73Lookup(
            IntSet allowedDateKeys,
            IntSet allowedStoreKeys,
            IntSet allowedHouseholdKeys,
            Map<Integer, CustomerIdentityRecord> customers) {}

    private record Query88Lookup(IntSet[] timeBucketKeys, IntSet allowedHouseholdKeys, IntSet allowedStoreKeys) {}

    private record Query96Lookup(IntSet timeKeys, IntSet householdKeys, IntSet storeKeys) {}

    private record CustomerDemographicsRecord(
            String gender,
            String maritalStatus,
            String educationStatus,
            long purchaseEstimate,
            String creditRating,
            long dependentCount,
            long employedDependentCount,
            long collegeDependentCount) {}

    private record CustomerIdentityRecord(String lastName, String firstName, String salutation, String preferredCustomerFlag) {}

    private record ShippingBucketsLookup(IntSet allowedShipDates, Map<Integer, String> firstNames, Map<Integer, String> secondNames, Map<Integer, String> thirdNames) {}

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
            private final IntSet[] allowedValues;
            private boolean closed;

            @SuppressWarnings("unchecked")
            private Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, int[] inputChannels, IntSet[] allowedValues)
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
        private final IntSet[] allowedValues;

        private Page outputPage;
        private boolean finishing;

        @SuppressWarnings("unchecked")
        private IntegerDimensionFilterOperator(io.trino.operator.OperatorContext operatorContext, List<Type> inputTypes, int[] inputChannels, IntSet[] allowedValues)
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

    private static final class CustomerDemographicsProjectOperator
            implements Operator
    {
        static final class Factory
                implements OperatorFactory
        {
            private final int operatorId;
            private final PlanNodeId planNodeId;
            private final List<Type> inputTypes;
            private final Query10Lookup lookup;
            private boolean closed;

            private Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, Query10Lookup lookup)
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
                return new CustomerDemographicsProjectOperator(
                        driverContext.addOperatorContext(operatorId, planNodeId, CustomerDemographicsProjectOperator.class.getSimpleName()),
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
        private final Query10Lookup lookup;

        private Page outputPage;
        private boolean finishing;

        private CustomerDemographicsProjectOperator(io.trino.operator.OperatorContext operatorContext, List<Type> inputTypes, Query10Lookup lookup)
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

            PageBuilder pageBuilder = new PageBuilder(List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT));
            for (int position = 0; position < page.getPositionCount(); position++) {
                int addressKey = readInt(inputTypes.get(0), page, 0, position);
                int customerKey = readInt(inputTypes.get(1), page, 1, position);
                if (!lookup.eligibleAddressKeys().contains(addressKey) ||
                        !lookup.storeCustomerKeys().contains(customerKey) ||
                        (!lookup.webCustomerKeys().contains(customerKey) && !lookup.catalogCustomerKeys().contains(customerKey))) {
                    continue;
                }

                CustomerDemographicsRecord demographics = lookup.demographics().get(readInt(inputTypes.get(2), page, 2, position));
                if (demographics == null) {
                    continue;
                }

                pageBuilder.declarePosition();
                writeVarchar(pageBuilder.getBlockBuilder(0), demographics.gender());
                writeVarchar(pageBuilder.getBlockBuilder(1), demographics.maritalStatus());
                writeVarchar(pageBuilder.getBlockBuilder(2), demographics.educationStatus());
                BIGINT.writeLong(pageBuilder.getBlockBuilder(3), demographics.purchaseEstimate());
                writeVarchar(pageBuilder.getBlockBuilder(4), demographics.creditRating());
                BIGINT.writeLong(pageBuilder.getBlockBuilder(5), demographics.dependentCount());
                BIGINT.writeLong(pageBuilder.getBlockBuilder(6), demographics.employedDependentCount());
                BIGINT.writeLong(pageBuilder.getBlockBuilder(7), demographics.collegeDependentCount());
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

    private static final class TicketCustomerFilterProjectOperator
            implements Operator
    {
        static final class Factory
                implements OperatorFactory
        {
            private final int operatorId;
            private final PlanNodeId planNodeId;
            private final List<Type> inputTypes;
            private final Query73Lookup lookup;
            private boolean closed;

            private Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, Query73Lookup lookup)
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
                return new TicketCustomerFilterProjectOperator(
                        driverContext.addOperatorContext(operatorId, planNodeId, TicketCustomerFilterProjectOperator.class.getSimpleName()),
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
        private final Query73Lookup lookup;

        private Page outputPage;
        private boolean finishing;

        private TicketCustomerFilterProjectOperator(io.trino.operator.OperatorContext operatorContext, List<Type> inputTypes, Query73Lookup lookup)
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

            PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT, BIGINT));
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (!lookup.allowedDateKeys().contains(readInt(inputTypes.get(2), page, 2, position)) ||
                        !lookup.allowedStoreKeys().contains(readInt(inputTypes.get(3), page, 3, position)) ||
                        !lookup.allowedHouseholdKeys().contains(readInt(inputTypes.get(4), page, 4, position))) {
                    continue;
                }

                pageBuilder.declarePosition();
                BIGINT.writeLong(pageBuilder.getBlockBuilder(0), readLong(inputTypes.get(0), page, 0, position));
                BIGINT.writeLong(pageBuilder.getBlockBuilder(1), readLong(inputTypes.get(1), page, 1, position));
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

    private static final class CustomerTicketProjectOperator
            implements Operator
    {
        static final class Factory
                implements OperatorFactory
        {
            private final int operatorId;
            private final PlanNodeId planNodeId;
            private final List<Type> inputTypes;
            private final Map<Integer, CustomerIdentityRecord> customers;
            private boolean closed;

            private Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, Map<Integer, CustomerIdentityRecord> customers)
            {
                this.operatorId = operatorId;
                this.planNodeId = planNodeId;
                this.inputTypes = List.copyOf(inputTypes);
                this.customers = customers;
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                if (closed) {
                    throw new IllegalStateException("Factory is already closed");
                }
                return new CustomerTicketProjectOperator(
                        driverContext.addOperatorContext(operatorId, planNodeId, CustomerTicketProjectOperator.class.getSimpleName()),
                        inputTypes,
                        customers);
            }

            @Override
            public void noMoreOperators()
            {
                closed = true;
            }

            @Override
            public OperatorFactory duplicate()
            {
                return new Factory(operatorId, planNodeId, inputTypes, customers);
            }
        }

        private final io.trino.operator.OperatorContext operatorContext;
        private final List<Type> inputTypes;
        private final Map<Integer, CustomerIdentityRecord> customers;

        private Page outputPage;
        private boolean finishing;

        private CustomerTicketProjectOperator(io.trino.operator.OperatorContext operatorContext, List<Type> inputTypes, Map<Integer, CustomerIdentityRecord> customers)
        {
            this.operatorContext = operatorContext;
            this.inputTypes = List.copyOf(inputTypes);
            this.customers = customers;
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

            PageBuilder pageBuilder = new PageBuilder(List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT));
            for (int position = 0; position < page.getPositionCount(); position++) {
                long count = readLong(inputTypes.get(2), page, 2, position);
                if (count < 1 || count > 5) {
                    continue;
                }

                CustomerIdentityRecord customer = customers.get(toIntExact(readLong(inputTypes.get(1), page, 1, position)));
                if (customer == null) {
                    continue;
                }

                pageBuilder.declarePosition();
                writeVarchar(pageBuilder.getBlockBuilder(0), customer.lastName());
                writeVarchar(pageBuilder.getBlockBuilder(1), customer.firstName());
                writeVarchar(pageBuilder.getBlockBuilder(2), customer.salutation());
                writeVarchar(pageBuilder.getBlockBuilder(3), customer.preferredCustomerFlag());
                BIGINT.writeLong(pageBuilder.getBlockBuilder(4), readLong(inputTypes.get(0), page, 0, position));
                BIGINT.writeLong(pageBuilder.getBlockBuilder(5), count);
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

    private static long readLong(Type type, Page page, int channel, int position)
    {
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(page.getBlock(channel), position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(page.getBlock(channel), position);
        }
        throw new IllegalArgumentException("Expected bigint-like type but found " + type);
    }

    private static void writeVarchar(io.trino.spi.block.BlockBuilder blockBuilder, String value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(value));
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
