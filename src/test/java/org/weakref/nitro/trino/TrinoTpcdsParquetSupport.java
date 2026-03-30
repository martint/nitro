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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.EnforceSingleRowOperator;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupIdOperator;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.HashSemiJoinOperator;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetSupplier;
import io.trino.operator.TopNOperator;
import io.trino.operator.TopNRankingOperator;
import io.trino.operator.ValuesOperator;
import io.trino.operator.WindowFunctionDefinition;
import io.trino.operator.WindowOperator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupOuterOperator;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.NestedLoopBuildOperator;
import io.trino.operator.join.NestedLoopJoinOperator;
import io.trino.operator.join.NestedLoopJoinPagesSupplier;
import io.trino.operator.window.AggregationWindowFunctionSupplier;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.RankFunction;
import io.trino.operator.window.ReflectionWindowFunctionSupplier;
import io.trino.operator.window.RegularPartitionerSupplier;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.JoinCompiler;
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
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.tpcds.TpcdsManualQuerySupport;
import org.weakref.nitro.tpcds.TpcdsParquetTables;
import org.weakref.nitro.tpcds.TpcdsQueryLiterals;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
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
import static io.trino.operator.WindowFunctionDefinition.window;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public final class TrinoTpcdsParquetSupport
        implements AutoCloseable
{
    private static final String TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY = "nitro.clickbench.trino.blockedWaitTimeoutSeconds";
    private static final int DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS = 5;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", ImmutableList.of());
    private static final TestingAggregationFunction BIGINT_COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_AVG = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_MIN = FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_MAX = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(BIGINT));
    private static final FrameInfo RUNNING_ROWS_FRAME = new FrameInfo(
            ROWS,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(FrameInfo.Ordering.ASCENDING));
    private static final FrameInfo PARTITION_ROWS_FRAME = new FrameInfo(
            ROWS,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            UNBOUNDED_FOLLOWING,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TrinoTpcdsParquetSupport"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TrinoTpcdsParquetSupport-scheduled"));
    private final OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());
    private final int blockedWaitTimeoutSeconds = blockedWaitTimeoutSeconds();

    public MaterializedResult query01(TpcdsParquetTables tables)
    {
        return executePagesPipeline(
                query01CustomerIdsPages(tables),
                List.of(factoryStep(topNFactory(7, List.of(VARCHAR), 100, List.of(0), List.of(ASC_NULLS_LAST)))),
                List.of(VARCHAR));
    }

    public MaterializedResult query03(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type salesType = salesTypes.get(2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type brandIdType = tableColumnTypes(tables, "item", List.of("i_brand_id")).getFirst();
        Type brandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        List<Type> outputTypes = List.of(INTEGER, brandIdType, brandType, salesSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(equal(1, 11, INTEGER)),
                List.of(field(0, BIGINT), field(2, INTEGER)),
                List.of(BIGINT, INTEGER));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_manufact_id", "i_brand_id", "i_brand"),
                Optional.of(equal(1, 128, INTEGER)),
                List.of(field(0, BIGINT), field(2, brandIdType), field(3, brandType)),
                List.of(BIGINT, brandIdType, brandType));

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(3_0, salesTypes, List.of(0), datePages, List.of(BIGINT, INTEGER), List.of(0))),
                        hashJoinStep(new HashJoinSpec(3_1, concatTypes(salesTypes, List.of(BIGINT, INTEGER)), List.of(1), itemPages, List.of(BIGINT, brandIdType, brandType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                3_2,
                                Optional.empty(),
                                List.of(field(4, INTEGER), field(6, brandIdType), field(7, brandType), field(2, salesType)),
                                List.of(INTEGER, brandIdType, brandType, salesType))),
                        factoryStep(hashAggregationFactory(
                                3_3,
                                List.of(INTEGER, brandIdType, brandType),
                                List.of(0, 1, 2),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(topNFactory(3_4, outputTypes, 100, List.of(0, 3, 1), List.of(ASC_NULLS_LAST, DESC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query04(TpcdsParquetTables tables)
    {
        List<Page> storeFirstYearPages = query04ChannelYearTotalPages(tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_wholesale_cost", "ss_ext_discount_amt", "ss_ext_sales_price", 2001, 4_100);
        List<Page> storeSecondYearPages = query04ChannelYearTotalPages(tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_wholesale_cost", "ss_ext_discount_amt", "ss_ext_sales_price", 2002, 4_110);
        List<Page> catalogFirstYearPages = query04ChannelYearTotalPages(tables, "catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", "cs_ext_list_price", "cs_ext_wholesale_cost", "cs_ext_discount_amt", "cs_ext_sales_price", 2001, 4_120);
        List<Page> catalogSecondYearPages = query04ChannelYearTotalPages(tables, "catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", "cs_ext_list_price", "cs_ext_wholesale_cost", "cs_ext_discount_amt", "cs_ext_sales_price", 2002, 4_130);
        List<Page> webFirstYearPages = query04ChannelYearTotalPages(tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_wholesale_cost", "ws_ext_discount_amt", "ws_ext_sales_price", 2001, 4_140);
        List<Page> webSecondYearPages = query04ChannelYearTotalPages(tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_wholesale_cost", "ws_ext_discount_amt", "ws_ext_sales_price", 2002, 4_150);

        List<Type> branchTypes = query04BranchTypes(tables);
        List<Type> afterStoreSecondYearTypes = concatTypes(branchTypes, branchTypes);
        List<Type> afterCatalogFirstYearTypes = concatTypes(afterStoreSecondYearTypes, branchTypes);
        List<Type> afterCatalogSecondYearTypes = concatTypes(afterCatalogFirstYearTypes, branchTypes);
        List<Type> afterWebFirstYearTypes = concatTypes(afterCatalogSecondYearTypes, branchTypes);
        List<Type> afterWebSecondYearTypes = concatTypes(afterWebFirstYearTypes, branchTypes);
        List<Type> outputTypes = List.of(branchTypes.get(0), branchTypes.get(1), branchTypes.get(2), branchTypes.get(3));

        return executePagesPipeline(
                storeFirstYearPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(4_160, branchTypes, List.of(0), storeSecondYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(4_161, afterStoreSecondYearTypes, List.of(0), catalogFirstYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(4_162, afterCatalogFirstYearTypes, List.of(0), catalogSecondYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(4_163, afterCatalogSecondYearTypes, List.of(0), webFirstYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(4_164, afterWebFirstYearTypes, List.of(0), webSecondYearPages, branchTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                4_165,
                                Optional.of(query04GrowthPredicate()),
                                List.of(field(0, branchTypes.get(0)), field(1, branchTypes.get(1)), field(2, branchTypes.get(2)), field(3, branchTypes.get(3))),
                                outputTypes)),
                        factoryStep(topNFactory(4_166, outputTypes, 100, List.of(0, 1, 2, 3), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query06(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_customer_sk", "ss_sold_date_sk", "ss_item_sk");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk"));
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_state"));
        Type stateType = addressTypes.get(1);
        List<Type> dateTypes = tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_month_seq"));
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_current_price", "i_category"));
        Type priceType = itemTypes.get(1);
        Type categoryType = itemTypes.get(2);
        TestingAggregationFunction priceAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(priceType));
        Type averageType = priceAverage.getFinalType();

        List<Type> afterCustomerTypes = concatTypes(salesTypes, customerTypes);
        List<Type> afterAddressTypes = concatTypes(afterCustomerTypes, addressTypes);
        List<Type> afterDateTypes = concatTypes(afterAddressTypes, dateTypes);
        List<Type> afterItemTypes = concatTypes(afterDateTypes, itemTypes);
        List<Type> afterMonthTypes = concatTypes(afterItemTypes, List.of(dateTypes.get(1)));
        List<Type> projectedTypes = List.of(stateType, priceType, categoryType);
        List<Type> categoryAverageTypes = List.of(categoryType, averageType);
        List<Type> afterAverageTypes = concatTypes(projectedTypes, categoryAverageTypes);
        List<Type> outputTypes = List.of(stateType, BIGINT);

        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT)),
                customerTypes);
        List<Page> addressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_state"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, stateType)),
                addressTypes);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_month_seq"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, dateTypes.get(1))),
                dateTypes);
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_current_price", "i_category"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, priceType), field(2, categoryType)),
                itemTypes);
        List<Page> scalarMonthPages = executePipelinePages(
                relationPages(
                        tables,
                        "date_dim",
                        List.of("d_month_seq", "d_year", "d_moy"),
                        Optional.of(and(equal(1, 2001, INTEGER), equal(2, 1, INTEGER))),
                        List.of(field(0, INTEGER)),
                        List.of(INTEGER)),
                List.of(
                        factoryStep(hashAggregationFactory(610, List.of(INTEGER), List.of(0))),
                        factoryStep(enforceSingleRowFactory(611, List.of(INTEGER)))));
        List<Page> categoryAveragePages = executePipelinePages(
                relationPages(
                        tables,
                        "item",
                        List.of("i_category", "i_current_price"),
                        Optional.empty(),
                        List.of(field(0, categoryType), field(1, priceType)),
                        List.of(categoryType, priceType)),
                List.of(factoryStep(hashAggregationFactory(
                        620,
                        List.of(categoryType),
                        List.of(0),
                        priceAverage.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))));

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(600, salesTypes, List.of(0), customerPages, customerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(601, afterCustomerTypes, List.of(4), addressPages, addressTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(602, afterAddressTypes, List.of(1), datePages, dateTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(603, afterDateTypes, List.of(2), itemPages, itemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(604, afterItemTypes, List.of(8), scalarMonthPages, List.of(INTEGER), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                605,
                                Optional.empty(),
                                List.of(field(6, stateType), field(10, priceType), field(11, categoryType)),
                                projectedTypes)),
                        hashJoinStep(new HashJoinSpec(606, projectedTypes, List.of(2), categoryAveragePages, categoryAverageTypes, List.of(0), JoinOperatorType.probeOuterJoin(false))),
                        factoryStep(filterAndProjectFactory(
                                607,
                                Optional.of(query06ThresholdPredicate(priceType, averageType)),
                                List.of(field(0, stateType)),
                                List.of(stateType))),
                        factoryStep(hashAggregationFactory(
                                608,
                                List.of(stateType),
                                List.of(0),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                609,
                                Optional.of(greaterThan(field(1, BIGINT), constant(9L, BIGINT), BIGINT)),
                                identityProjections(outputTypes),
                                outputTypes)),
                        factoryStep(topNFactory(612, outputTypes, 100, List.of(1, 0), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query12(TpcdsParquetTables tables)
    {
        return queryRevenueRatioByClass(tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price");
    }

    public MaterializedResult query13(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_quantity", "ss_sales_price", "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_net_profit");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type quantityType = salesTypes.get(5);
        Type salesPriceType = salesTypes.get(6);
        Type salesType = salesTypes.get(7);
        Type wholesaleType = salesTypes.get(8);
        Type netProfitType = salesTypes.get(9);
        TestingAggregationFunction salesAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(salesType));
        TestingAggregationFunction wholesaleAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(wholesaleType));
        TestingAggregationFunction wholesaleSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(wholesaleType));

        List<Type> storeTypes = List.of(BIGINT);
        List<Type> customerDemographicsTypes = tableColumnTypes(tables, "customer_demographics", List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"));
        Type maritalStatusType = customerDemographicsTypes.get(1);
        Type educationStatusType = customerDemographicsTypes.get(2);
        List<Type> householdTypes = tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count"));
        Type depCountType = householdTypes.get(1);
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_country", "ca_state"));
        Type countryType = addressTypes.get(1);
        Type stateType = addressTypes.get(2);

        List<Type> afterStoreTypes = concatTypes(salesTypes, storeTypes);
        List<Type> afterDemographicsTypes = concatTypes(afterStoreTypes, customerDemographicsTypes);
        List<Type> afterHouseholdTypes = concatTypes(afterDemographicsTypes, householdTypes);
        List<Type> afterAddressTypes = concatTypes(afterHouseholdTypes, List.of(BIGINT, stateType));
        List<Type> projectedTypes = List.of(BIGINT, salesType, wholesaleType);
        List<Type> outputTypes = List.of(BIGINT_AVG.getFinalType(), salesAverage.getFinalType(), wholesaleAverage.getFinalType(), wholesaleSum.getFinalType());

        RowExpression quantityValue = cast(field(5, quantityType), quantityType, BIGINT);
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT)),
                storeTypes);
        List<Page> customerDemographicsPages = relationPages(
                tables,
                "customer_demographics",
                List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"),
                Optional.empty(),
                identityProjections(customerDemographicsTypes),
                customerDemographicsTypes);
        List<Page> householdPages = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_dep_count"),
                Optional.empty(),
                identityProjections(householdTypes),
                householdTypes);
        List<Page> addressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_country", "ca_state"),
                Optional.of(equal(1, countryType, "United States")),
                List.of(field(0, BIGINT), field(2, stateType)),
                List.of(BIGINT, stateType));
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(equal(1, 2001, INTEGER)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(13_100, salesTypes, List.of(4), storePages, storeTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(13_101, afterStoreTypes, List.of(1), customerDemographicsPages, customerDemographicsTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(13_102, afterDemographicsTypes, List.of(2), householdPages, householdTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                13_103,
                                Optional.of(query13DemographicsPredicate(maritalStatusType, educationStatusType, salesPriceType, depCountType)),
                                identityProjections(afterHouseholdTypes),
                                afterHouseholdTypes)),
                        hashJoinStep(new HashJoinSpec(13_104, afterHouseholdTypes, List.of(3), addressPages, List.of(BIGINT, stateType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                13_105,
                                Optional.of(query13StateProfitPredicate(stateType, netProfitType)),
                                identityProjections(afterAddressTypes),
                                afterAddressTypes)),
                        hashJoinStep(new HashJoinSpec(13_106, afterAddressTypes, List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                13_107,
                                Optional.empty(),
                                List.of(quantityValue, field(7, salesType), field(8, wholesaleType)),
                                projectedTypes)),
                        factoryStep(aggregationFactory(
                                13_108,
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()),
                                salesAverage.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                wholesaleAverage.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                wholesaleSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))),
                outputTypes);
    }

    public MaterializedResult query20(TpcdsParquetTables tables)
    {
        return queryRevenueRatioByClass(tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price");
    }

    public MaterializedResult query39(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = List.of(
                BIGINT,
                BIGINT,
                INTEGER,
                DOUBLE,
                DOUBLE,
                BIGINT,
                BIGINT,
                INTEGER,
                DOUBLE,
                DOUBLE);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (org.weakref.nitro.data.Row row : TpcdsManualQuerySupport.query39Rows(new Allocator(), tables)) {
            result.row(row.values());
        }
        return result.build();
    }

    public MaterializedResult query17(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = List.of(
                tableColumnTypes(tables, "item", List.of("i_item_id")).getFirst(),
                tableColumnTypes(tables, "item", List.of("i_item_desc")).getFirst(),
                tableColumnTypes(tables, "store", List.of("s_state")).getFirst(),
                BIGINT,
                DOUBLE,
                DOUBLE,
                DOUBLE,
                BIGINT,
                DOUBLE,
                DOUBLE,
                DOUBLE,
                BIGINT,
                DOUBLE,
                DOUBLE,
                DOUBLE);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (org.weakref.nitro.data.Row row : TpcdsManualQuerySupport.query17Rows(new Allocator(), tables)) {
            result.row(row.values());
        }
        return result.build();
    }

    public MaterializedResult query64(TpcdsParquetTables tables)
    {
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_street_number", "ca_street_name", "ca_city", "ca_zip"));
        List<Type> outputTypes = List.of(
                tableColumnTypes(tables, "item", List.of("i_product_name")).getFirst(),
                tableColumnTypes(tables, "store", List.of("s_store_name")).getFirst(),
                tableColumnTypes(tables, "store", List.of("s_zip")).getFirst(),
                addressTypes.get(0),
                addressTypes.get(1),
                addressTypes.get(2),
                addressTypes.get(3),
                addressTypes.get(0),
                addressTypes.get(1),
                addressTypes.get(2),
                addressTypes.get(3),
                INTEGER,
                BIGINT,
                BIGINT,
                BIGINT,
                BIGINT,
                BIGINT,
                BIGINT,
                BIGINT,
                INTEGER,
                BIGINT);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (org.weakref.nitro.data.Row row : TpcdsManualQuerySupport.query64Rows(new Allocator(), tables)) {
            result.row(row.values());
        }
        return result.build();
    }

    public MaterializedResult query16(TpcdsParquetTables tables)
    {
        List<Page> eligibleOrders = query16MultiWarehouseOrderPages(tables, 16_100);
        List<Page> returnedEligibleOrders = query16ReturnedEligibleOrderPages(tables, eligibleOrders, 16_200);

        List<String> baseColumns = List.of("cs_ship_date_sk", "cs_ship_addr_sk", "cs_call_center_sk", "cs_warehouse_sk", "cs_order_number", "cs_ext_ship_cost", "cs_net_profit");
        List<Type> baseTypes = tableColumnTypes(tables, "catalog_sales", baseColumns);
        Type shipCostType = baseTypes.get(5);
        Type profitType = baseTypes.get(6);

        List<Page> qualifiedSalesPages = executePipelinePages(
                tables.tableFiles("catalog_sales"),
                baseColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(16_300, baseTypes, List.of(0), relationPages(
                                tables,
                                "date_dim",
                                List.of("d_date_sk", "d_date"),
                                Optional.of(betweenInclusive(field(1, DATE), LocalDate.of(2002, 2, 1).toEpochDay(), LocalDate.of(2002, 4, 2).toEpochDay(), DATE)),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(16_301, concatTypes(baseTypes, List.of(BIGINT)), List.of(1), relationPages(
                                tables,
                                "customer_address",
                                List.of("ca_address_sk", "ca_state"),
                                Optional.of(equal(1, tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_state")).get(1), "GA")),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(16_302, concatTypes(concatTypes(baseTypes, List.of(BIGINT)), List.of(BIGINT)), List.of(2), relationPages(
                                tables,
                                "call_center",
                                List.of("cc_call_center_sk", "cc_county"),
                                Optional.of(equal(1, VARCHAR, "Williamson County")),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(16_303, Optional.empty(), List.of(field(4, BIGINT), field(5, shipCostType), field(6, profitType)), List.of(BIGINT, shipCostType, profitType))),
                        hashJoinStep(new HashJoinSpec(16_304, List.of(BIGINT, shipCostType, profitType), List.of(0), eligibleOrders, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(16_305, Optional.empty(), List.of(field(0, BIGINT), field(1, shipCostType), field(2, profitType)), List.of(BIGINT, shipCostType, profitType))),
                        hashJoinStep(new HashJoinSpec(16_306, List.of(BIGINT, shipCostType, profitType), List.of(0), returnedEligibleOrders, List.of(BIGINT), List.of(0), JoinOperatorType.probeOuterJoin(false))),
                        factoryStep(filterAndProjectFactory(
                                16_307,
                                Optional.of(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(3, BIGINT)), List.of())),
                                List.of(field(0, BIGINT), field(1, shipCostType), field(2, profitType)),
                                List.of(BIGINT, shipCostType, profitType)))));

        TestingAggregationFunction shipCostSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(shipCostType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));

        MaterializedResult totalResult = executePagesPipeline(
                qualifiedSalesPages,
                List.of(factoryStep(aggregationFactory(
                        16_308,
                        shipCostSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                        profitSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))),
                List.of(shipCostSum.getFinalType(), profitSum.getFinalType()));

        long distinctOrderCount = singleLongResult(executePagesPipeline(
                qualifiedSalesPages,
                List.of(
                        factoryStep(hashAggregationFactory(16_309, List.of(BIGINT), List.of(0))),
                        factoryStep(aggregationFactory(16_310, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                List.of(BIGINT)));

        List<Type> outputTypes = List.of(BIGINT, shipCostSum.getFinalType(), profitSum.getFinalType());
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        io.trino.testing.MaterializedRow totals = totalResult.getMaterializedRows().getFirst();
        result.row(distinctOrderCount, totals.getField(0), totals.getField(1));
        return result.build();
    }

    public MaterializedResult query18(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", List.of("cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_net_profit"));
        Type quantityType = factTypes.get(4);
        Type listPriceType = factTypes.get(5);
        Type couponType = factTypes.get(6);
        Type salesPriceType = factTypes.get(7);
        Type netProfitType = factTypes.get(8);
        List<Type> demographicTypes = tableColumnTypes(tables, "customer_demographics", List.of("cd_demo_sk", "cd_dep_count"));
        Type dependentCountType = demographicTypes.get(1);
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk", "c_birth_month", "c_birth_year"));
        Type birthMonthType = customerTypes.get(2);
        Type birthYearType = customerTypes.get(3);
        List<Type> sourceTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);

        RowExpression quantityValue = multiply(cast(field(4, quantityType), quantityType, BIGINT), constant(100L, BIGINT), BIGINT);
        RowExpression listPriceValue = cast(
                multiply(field(5, listPriceType), constant(100L, createDecimalType(3, 0))),
                multiply(field(5, listPriceType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression couponValue = cast(
                multiply(field(6, couponType), constant(100L, createDecimalType(3, 0))),
                multiply(field(6, couponType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression salesPriceValue = cast(
                multiply(field(7, salesPriceType), constant(100L, createDecimalType(3, 0))),
                multiply(field(7, salesPriceType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression netProfitValue = cast(
                multiply(field(8, netProfitType), constant(100L, createDecimalType(3, 0))),
                multiply(field(8, netProfitType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression birthYearValue = multiply(cast(field(16, birthYearType), birthYearType, BIGINT), constant(100L, BIGINT), BIGINT);
        RowExpression dependentCountValue = multiply(cast(field(13, dependentCountType), dependentCountType, BIGINT), constant(100L, BIGINT), BIGINT);

        List<Page> sourcePages = executePipelinePages(
                tables.tableFiles("catalog_sales"),
                List.of("cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_net_profit"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                18_0,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(equal(1, 1998, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                18_1,
                                concatTypes(factTypes, List.of(BIGINT)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_item_id"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                18_2,
                                concatTypes(concatTypes(factTypes, List.of(BIGINT)), List.of(BIGINT, VARCHAR)),
                                List.of(3),
                                relationPages(
                                        tables,
                                        "customer_demographics",
                                        List.of("cd_demo_sk", "cd_gender", "cd_education_status", "cd_dep_count"),
                                        Optional.of(and(equal(1, VARCHAR, "F"), equal(2, VARCHAR, "Unknown             "))),
                                        List.of(field(0, BIGINT), field(3, dependentCountType)),
                                        List.of(BIGINT, dependentCountType)),
                                List.of(BIGINT, dependentCountType),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                18_3,
                                concatTypes(concatTypes(concatTypes(factTypes, List.of(BIGINT)), List.of(BIGINT, VARCHAR)), List.of(BIGINT, dependentCountType)),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "customer",
                                        List.of("c_customer_sk", "c_current_addr_sk", "c_birth_month", "c_birth_year"),
                                        Optional.of(anyOf(field(2, birthMonthType), birthMonthType, 1, 2, 6, 8, 9, 12)),
                                        List.of(field(0, BIGINT), field(1, BIGINT), field(3, birthYearType)),
                                        List.of(BIGINT, BIGINT, birthYearType)),
                                List.of(BIGINT, BIGINT, birthYearType),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                18_4,
                                concatTypes(concatTypes(concatTypes(concatTypes(factTypes, List.of(BIGINT)), List.of(BIGINT, VARCHAR)), List.of(BIGINT, dependentCountType)), List.of(BIGINT, BIGINT, birthYearType)),
                                List.of(15),
                                relationPages(
                                        tables,
                                        "customer_address",
                                        List.of("ca_address_sk", "ca_country", "ca_state", "ca_county"),
                                        Optional.of(varcharAnyOf(2, Set.of("MS", "IN", "ND", "OK", "NM", "VA"))),
                                        List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR), field(3, VARCHAR)),
                                        List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR)),
                                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                18_5,
                                Optional.empty(),
                                List.of(
                                        field(11, VARCHAR),
                                        field(18, VARCHAR),
                                        field(19, VARCHAR),
                                        field(20, VARCHAR),
                                        quantityValue,
                                        listPriceValue,
                                        couponValue,
                                        salesPriceValue,
                                        netProfitValue,
                                        birthYearValue,
                                        dependentCountValue),
                                sourceTypes))));

        record RollupKey(String itemId, String country, String state, String county) {}

        final class RollupState
        {
            private final long[] sums = new long[7];
            private final long[] counts = new long[7];

            private void add(io.trino.testing.MaterializedRow row)
            {
                for (int metricIndex = 0; metricIndex < 7; metricIndex++) {
                    Number value = (Number) row.getField(4 + metricIndex);
                    if (value == null) {
                        continue;
                    }
                    sums[metricIndex] += value.longValue();
                    counts[metricIndex]++;
                }
            }

            private Long average(int metricIndex)
            {
                return counts[metricIndex] == 0 ? null : roundDivide(sums[metricIndex], counts[metricIndex]);
            }
        }

        Map<RollupKey, RollupState> states = new HashMap<>();
        MaterializedResult sourceRows = executePagesPipeline(sourcePages, List.of(), sourceTypes);
        for (io.trino.testing.MaterializedRow row : sourceRows.getMaterializedRows()) {
            String itemId = (String) row.getField(0);
            String country = (String) row.getField(1);
            String state = (String) row.getField(2);
            String county = (String) row.getField(3);
            List<RollupKey> rollups = List.of(
                    new RollupKey(null, null, null, null),
                    new RollupKey(itemId, null, null, null),
                    new RollupKey(itemId, country, null, null),
                    new RollupKey(itemId, country, state, null),
                    new RollupKey(itemId, country, state, county));
            for (RollupKey key : rollups) {
                states.computeIfAbsent(key, ignored -> new RollupState()).add(row);
            }
        }

        Comparator<String> stringComparator = Comparator.nullsLast(String::compareTo);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), sourceTypes);
        states.entrySet().stream()
                .sorted(Map.Entry.<RollupKey, RollupState>comparingByKey(
                        Comparator.comparing(RollupKey::country, stringComparator)
                                .thenComparing(RollupKey::state, stringComparator)
                                .thenComparing(RollupKey::county, stringComparator)
                                .thenComparing(RollupKey::itemId, stringComparator)))
                .limit(100)
                .forEach(entry -> result.row(
                        entry.getKey().itemId(),
                        entry.getKey().country(),
                        entry.getKey().state(),
                        entry.getKey().county(),
                        entry.getValue().average(0),
                        entry.getValue().average(1),
                        entry.getValue().average(2),
                        entry.getValue().average(3),
                        entry.getValue().average(4),
                        entry.getValue().average(5),
                        entry.getValue().average(6)));
        return result.build();
    }

    public MaterializedResult query22(TpcdsParquetTables tables)
    {
        Type quantityType = tableColumnTypes(tables, "inventory", List.of("inv_quantity_on_hand")).getFirst();
        RowExpression quantityValue = cast(field(2, quantityType), quantityType, BIGINT);
        List<Type> sourceTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, DOUBLE);

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_month_seq"),
                Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_product_name", "i_brand", "i_class", "i_category"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR), field(3, VARCHAR), field(4, VARCHAR)),
                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR));

        List<Page> sourcePages = executePipelinePages(
                tables.tableFiles("inventory"),
                List.of("inv_date_sk", "inv_item_sk", "inv_quantity_on_hand"),
                List.of(
                        hashJoinStep(new HashJoinSpec(22_0, List.of(BIGINT, BIGINT, quantityType), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(22_1, concatTypes(List.of(BIGINT, BIGINT, quantityType), List.of(BIGINT)), List.of(1), itemPages, List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                22_2,
                                Optional.empty(),
                                List.of(field(5, VARCHAR), field(6, VARCHAR), field(7, VARCHAR), field(8, VARCHAR), quantityValue),
                                sourceTypes))));

        record RollupKey(String productName, String brand, String itemClass, String category) {}

        record RollupState(long sum, long count)
        {
            RollupState add(long value)
            {
                return new RollupState(sum + value, count + 1);
            }
        }

        record RollupRow(String productName, String brand, String itemClass, String category, double average) {}

        Map<RollupKey, RollupState> states = new HashMap<>();
        MaterializedResult sourceRows = executePagesPipeline(sourcePages, List.of(), sourceTypes);
        for (io.trino.testing.MaterializedRow row : sourceRows.getMaterializedRows()) {
            String productName = (String) row.getField(0);
            String brand = (String) row.getField(1);
            String itemClass = (String) row.getField(2);
            String category = (String) row.getField(3);
            Number quantityField = (Number) row.getField(4);
            if (quantityField == null) {
                continue;
            }
            long quantity = quantityField.longValue();

            List<RollupKey> rollups = List.of(
                    new RollupKey(null, null, null, null),
                    new RollupKey(productName, null, null, null),
                    new RollupKey(productName, brand, null, null),
                    new RollupKey(productName, brand, itemClass, null),
                    new RollupKey(productName, brand, itemClass, category));
            for (RollupKey key : rollups) {
                states.merge(key, new RollupState(quantity, 1), (left, right) -> new RollupState(left.sum() + right.sum(), left.count() + right.count()));
            }
        }

        Comparator<String> stringComparator = Comparator.nullsLast(String::compareTo);
        List<RollupRow> rows = states.entrySet().stream()
                .map(entry -> new RollupRow(
                        entry.getKey().productName(),
                        entry.getKey().brand(),
                        entry.getKey().itemClass(),
                        entry.getKey().category(),
                        (double) entry.getValue().sum() / entry.getValue().count()))
                .sorted(Comparator.comparingDouble(RollupRow::average)
                        .thenComparing(RollupRow::productName, stringComparator)
                        .thenComparing(RollupRow::brand, stringComparator)
                        .thenComparing(RollupRow::itemClass, stringComparator)
                        .thenComparing(RollupRow::category, stringComparator))
                .limit(100)
                .toList();

        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (RollupRow row : rows) {
            result.row(row.productName(), row.brand(), row.itemClass(), row.category(), row.average());
        }
        return result.build();
    }

    public MaterializedResult query21(TpcdsParquetTables tables)
    {
        List<Page> beforePages = query21InventoryByWarehouseItemPages(tables, true, 21_00);
        List<Page> afterPages = query21InventoryByWarehouseItemPages(tables, false, 21_10);
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, BIGINT, BIGINT);

        return executePagesPipeline(
                beforePages,
                List.of(
                        hashJoinStep(new HashJoinSpec(21_20, groupedTypes, List.of(0, 1), afterPages, groupedTypes, List.of(0, 1))),
                        factoryStep(filterAndProjectFactory(
                                21_21,
                                Optional.of(query21InventoryRatioPredicate()),
                                List.of(field(0, VARCHAR), field(1, VARCHAR), field(2, BIGINT), field(5, BIGINT)),
                                outputTypes)),
                        factoryStep(topNFactory(21_22, outputTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query27(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_cdemo_sk", "ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_sales_price");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type quantityType = salesTypes.get(4);
        Type listPriceType = salesTypes.get(5);
        Type couponType = salesTypes.get(6);
        Type salesPriceType = salesTypes.get(7);
        RowExpression quantityValue = cast(field(4, quantityType), quantityType, BIGINT);
        RowExpression listPriceValue = cast(multiply(field(5, listPriceType), constant(100L, createDecimalType(3, 0))), multiply(field(5, listPriceType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        RowExpression couponValue = cast(multiply(field(6, couponType), constant(100L, createDecimalType(3, 0))), multiply(field(6, couponType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        RowExpression salesPriceValue = cast(multiply(field(7, salesPriceType), constant(100L, createDecimalType(3, 0))), multiply(field(7, salesPriceType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        List<Type> sourceTypes = List.of(VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, BIGINT, BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType());

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                27_0,
                                salesTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(equal(1, 2002, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                27_1,
                                concatTypes(salesTypes, List.of(BIGINT)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_item_id"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                27_2,
                                concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, VARCHAR)),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "store",
                                        List.of("s_store_sk", "s_state"),
                                        Optional.of(equal(1, VARCHAR, "TN")),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                27_3,
                                concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, VARCHAR)), List.of(BIGINT, VARCHAR)),
                                List.of(3),
                                relationPages(
                                        tables,
                                        "customer_demographics",
                                        List.of("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"),
                                        Optional.of(and(equal(1, VARCHAR, "M"), equal(2, VARCHAR, "S"), equal(3, VARCHAR, "College"))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                27_4,
                                Optional.empty(),
                                List.of(field(10, VARCHAR), field(12, VARCHAR), quantityValue, listPriceValue, couponValue, salesPriceValue),
                                sourceTypes)),
                        factoryStep(groupIdFactory(
                                27_5,
                                concatTypes(sourceTypes, List.of(BIGINT)),
                                List.of(
                                        Map.of(0, 0, 2, 2, 3, 3, 4, 4, 5, 5),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5)))),
                        factoryStep(hashAggregationFactory(
                                27_6,
                                List.of(VARCHAR, VARCHAR, BIGINT),
                                List.of(0, 1, 6),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                27_7,
                                Optional.empty(),
                                List.of(
                                        field(0, VARCHAR),
                                        field(1, VARCHAR),
                                        ifExpression(equal(field(2, BIGINT), constant(0L, BIGINT), BIGINT), constant(1L, BIGINT), constant(0L, BIGINT), BIGINT),
                                        field(3, BIGINT_AVG.getFinalType()),
                                        field(4, BIGINT_AVG.getFinalType()),
                                        field(5, BIGINT_AVG.getFinalType()),
                                        field(6, BIGINT_AVG.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(27_8, outputTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query28(TpcdsParquetTables tables)
    {
        Type bucketType = tableColumnTypes(tables, "store_sales", List.of("ss_list_price")).getFirst();
        List<Type> bucketOutputTypes = List.of(bucketType, BIGINT, BIGINT);
        List<Page> joined = query28BucketPages(tables, 0, 5, 8_00L, 18_00L, 459_00L, 1459_00L, 57_00L, 77_00L, 28_00);
        joined = executeNestedLoopPages(joined, bucketOutputTypes, query28BucketPages(tables, 6, 10, 90_00L, 100_00L, 2323_00L, 3323_00L, 31_00L, 51_00L, 28_10), bucketOutputTypes);
        joined = executeNestedLoopPages(joined, concatTypes(bucketOutputTypes, bucketOutputTypes), query28BucketPages(tables, 11, 15, 142_00L, 152_00L, 12214_00L, 13214_00L, 79_00L, 99_00L, 28_20), bucketOutputTypes);
        joined = executeNestedLoopPages(joined, concatTypes(concatTypes(bucketOutputTypes, bucketOutputTypes), bucketOutputTypes), query28BucketPages(tables, 16, 20, 135_00L, 145_00L, 6071_00L, 7071_00L, 38_00L, 58_00L, 28_30), bucketOutputTypes);
        joined = executeNestedLoopPages(joined, concatTypes(concatTypes(concatTypes(bucketOutputTypes, bucketOutputTypes), bucketOutputTypes), bucketOutputTypes), query28BucketPages(tables, 21, 25, 122_00L, 132_00L, 836_00L, 1836_00L, 17_00L, 37_00L, 28_40), bucketOutputTypes);
        joined = executeNestedLoopPages(joined, concatTypes(concatTypes(concatTypes(concatTypes(bucketOutputTypes, bucketOutputTypes), bucketOutputTypes), bucketOutputTypes), bucketOutputTypes), query28BucketPages(tables, 26, 30, 154_00L, 164_00L, 7326_00L, 8326_00L, 7_00L, 27_00L, 28_50), bucketOutputTypes);
        return executePagesPipeline(
                joined,
                List.of(),
                concatTypes(concatTypes(concatTypes(concatTypes(concatTypes(bucketOutputTypes, bucketOutputTypes), bucketOutputTypes), bucketOutputTypes), bucketOutputTypes), bucketOutputTypes));
    }

    public MaterializedResult query42(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst();
        Type managerIdType = tableColumnTypes(tables, "item", List.of("i_manager_id")).getFirst();
        Type categoryIdType = tableColumnTypes(tables, "item", List.of("i_category_id")).getFirst();
        Type categoryType = tableColumnTypes(tables, "item", List.of("i_category")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> outputTypes = List.of(INTEGER, categoryIdType, categoryType, salesSum.getFinalType());

        return executePagesPipeline(
                executePipelinePages(
                        tables.tableFiles("store_sales"),
                        List.of("ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"),
                        List.of(
                        hashJoinStep(new HashJoinSpec(
                                42_0,
                                List.of(BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_moy", "d_year"),
                                        Optional.of(and(equal(1, 11, INTEGER), equal(2, 2000, INTEGER))),
                                        List.of(field(0, BIGINT), field(2, INTEGER)),
                                        List.of(BIGINT, INTEGER)),
                                List.of(BIGINT, INTEGER),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                42_1,
                                List.of(BIGINT, BIGINT, salesType, BIGINT, INTEGER),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_manager_id", "i_category_id", "i_category"),
                                        Optional.of(equal(1, 1L, managerIdType)),
                                        List.of(field(0, BIGINT), field(2, categoryIdType), field(3, categoryType)),
                                        List.of(BIGINT, categoryIdType, categoryType)),
                                List.of(BIGINT, categoryIdType, categoryType),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                42_2,
                                Optional.empty(),
                                List.of(field(4, INTEGER), field(6, categoryIdType), field(7, categoryType), field(2, salesType)),
                                List.of(INTEGER, categoryIdType, categoryType, salesType))),
                        factoryStep(hashAggregationFactory(
                                42_3,
                                List.of(INTEGER, categoryIdType, categoryType),
                                List.of(0, 1, 2),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(topNFactory(42_4, outputTypes, 100, List.of(3, 0, 1, 2), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST))))),
                List.of(),
                outputTypes);
    }

    public MaterializedResult query43(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_sales_price")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type storeIdType = tableColumnTypes(tables, "store", List.of("s_store_id")).getFirst();
        Type storeOffsetType = tableColumnTypes(tables, "store", List.of("s_gmt_offset")).getFirst();
        List<Type> outputTypes = List.of(VARCHAR, storeIdType, salesSum.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_day_name", "d_year"),
                Optional.of(equal(2, 2000, INTEGER)),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_gmt_offset", "s_store_name", "s_store_id"),
                Optional.of(equal(1, -500L, storeOffsetType)),
                List.of(field(0, BIGINT), field(2, VARCHAR), field(3, storeIdType)),
                List.of(BIGINT, VARCHAR, storeIdType));

        return executePipeline(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_store_sk", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(43_0, List.of(BIGINT, BIGINT, salesType), List.of(0), datePages, List.of(BIGINT, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(43_1, List.of(BIGINT, BIGINT, salesType, BIGINT, VARCHAR), List.of(1), storePages, List.of(BIGINT, VARCHAR, storeIdType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                43_2,
                                Optional.empty(),
                                query43BucketProjections(field(6, VARCHAR), field(7, storeIdType), field(4, VARCHAR), field(2, salesType), salesType),
                                outputTypes)),
                        factoryStep(hashAggregationFactory(
                                43_3,
                                List.of(VARCHAR, storeIdType),
                                List.of(0, 1),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(8), OptionalInt.empty()))),
                        factoryStep(topNFactory(43_4, outputTypes, 100, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query46(TpcdsParquetTables tables)
    {
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", List.of("ss_coupon_amt", "ss_net_profit"));
        Type couponType = salesTypes.getFirst();
        Type profitType = salesTypes.get(1);
        TestingAggregationFunction couponSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(couponType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));
        List<Type> groupedTypes = List.of(BIGINT, BIGINT, VARCHAR, couponSum.getFinalType(), profitSum.getFinalType());
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk", "c_last_name", "c_first_name"));
        List<Type> projectedCustomerTypes = List.of(BIGINT, customerTypes.get(1), customerTypes.get(2), customerTypes.get(3));
        List<Type> afterCustomerTypes = concatTypes(groupedTypes, projectedCustomerTypes);
        List<Type> currentAddressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_city"));
        List<Type> outputTypes = List.of(customerTypes.get(2), customerTypes.get(3), currentAddressTypes.get(1), VARCHAR, BIGINT, couponSum.getFinalType(), profitSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_dow", "d_year"),
                Optional.of(query46DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_city"),
                Optional.of(varcharAnyOf(1, Set.of("Fairview", "Midway"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Type> householdTypes = tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"));
        List<Page> householdPages = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"),
                Optional.of(query46HouseholdPredicate(householdTypes.get(1), householdTypes.get(2))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> boughtAddressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_city"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk", "c_last_name", "c_first_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, customerTypes.get(1)), field(2, customerTypes.get(2)), field(3, customerTypes.get(3))),
                projectedCustomerTypes);
        List<Page> currentAddressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_city"),
                Optional.empty(),
                List.of(field(0, currentAddressTypes.get(0)), field(1, currentAddressTypes.get(1))),
                currentAddressTypes);

        return executePagesPipeline(
                executePipelinePages(
                        tables.tableFiles("store_sales"),
                        List.of("ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_ticket_number", "ss_customer_sk", "ss_coupon_amt", "ss_net_profit"),
                        List.of(
                                hashJoinStep(new HashJoinSpec(46_0, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, couponType, profitType), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(46_1, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, couponType, profitType, BIGINT), List.of(1), storePages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(46_2, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, couponType, profitType, BIGINT, BIGINT), List.of(2), householdPages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(46_3, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, couponType, profitType, BIGINT, BIGINT, BIGINT), List.of(3), boughtAddressPages, List.of(BIGINT, VARCHAR), List.of(0))),
                                factoryStep(filterAndProjectFactory(
                                        46_4,
                                        Optional.empty(),
                                        List.of(field(4, BIGINT), field(5, BIGINT), field(12, VARCHAR), field(6, couponType), field(7, profitType)),
                                        List.of(BIGINT, BIGINT, VARCHAR, couponType, profitType))),
                                factoryStep(hashAggregationFactory(
                                        46_5,
                                        List.of(BIGINT, BIGINT, VARCHAR),
                                        List.of(0, 1, 2),
                                        couponSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                        profitSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()))))),
                List.of(
                        hashJoinStep(new HashJoinSpec(46_6, groupedTypes, List.of(1), customerPages, projectedCustomerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(46_7, afterCustomerTypes, List.of(6), currentAddressPages, currentAddressTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                46_8,
                                Optional.of(notEqual(field(10, currentAddressTypes.get(1)), field(2, VARCHAR))),
                                List.of(field(7, customerTypes.get(2)), field(8, customerTypes.get(3)), field(10, currentAddressTypes.get(1)), field(2, VARCHAR), field(0, BIGINT), field(3, couponSum.getFinalType()), field(4, profitSum.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(46_9, outputTypes, 100, List.of(0, 1, 2, 3, 4), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query34(TpcdsParquetTables tables)
    {
        List<Type> groupedTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag"));
        List<Type> projectedCustomerTypes = List.of(BIGINT, customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4));
        List<Type> outputTypes = List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4), BIGINT, BIGINT);

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_dom", "d_year"),
                Optional.of(query34DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_county"),
                Optional.of(varcharAnyOf(1, Set.of("Williamson County"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Type> householdTypes = tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"));
        List<Page> householdPages = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"),
                Optional.of(query34HouseholdPredicate(householdTypes.get(2), householdTypes.get(3))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, customerTypes.get(1)), field(2, customerTypes.get(2)), field(3, customerTypes.get(3)), field(4, customerTypes.get(4))),
                projectedCustomerTypes);

        return executePagesPipeline(
                executePipelinePages(
                        tables.tableFiles("store_sales"),
                        List.of("ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk", "ss_ticket_number", "ss_customer_sk"),
                        List.of(
                                hashJoinStep(new HashJoinSpec(34_0, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(34_1, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT), List.of(1), storePages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(34_2, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT), List.of(2), householdPages, List.of(BIGINT), List.of(0))),
                                factoryStep(filterAndProjectFactory(
                                        34_3,
                                        Optional.empty(),
                                        List.of(field(3, BIGINT), field(4, BIGINT)),
                                        List.of(BIGINT, BIGINT))),
                                factoryStep(hashAggregationFactory(
                                        34_4,
                                        List.of(BIGINT, BIGINT),
                                        List.of(0, 1),
                                        COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))))),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                34_5,
                                Optional.of(and(greaterThan(field(2, BIGINT), constant(14L, BIGINT), BIGINT), lessThan(field(2, BIGINT), constant(21L, BIGINT), BIGINT))),
                                identityProjections(groupedTypes),
                                groupedTypes)),
                        hashJoinStep(new HashJoinSpec(34_6, groupedTypes, List.of(1), customerPages, projectedCustomerTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                34_7,
                                Optional.empty(),
                                List.of(field(4, customerTypes.get(1)), field(5, customerTypes.get(2)), field(6, customerTypes.get(3)), field(7, customerTypes.get(4)), field(0, BIGINT), field(2, BIGINT)),
                                outputTypes)),
                        factoryStep(topNFactory(34_8, outputTypes, 100, List.of(0, 1, 2, 3, 4), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, DESC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query50(TpcdsParquetTables tables)
    {
        List<String> factColumns = List.of("ss_sold_date_sk", "ss_ticket_number", "ss_item_sk", "ss_customer_sk", "ss_store_sk");
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", factColumns);
        List<Type> returnTypes = tableColumnTypes(tables, "store_returns", List.of("sr_returned_date_sk", "sr_ticket_number", "sr_item_sk", "sr_customer_sk"));
        List<Type> afterReturnsTypes = concatTypes(factTypes, returnTypes);
        List<Type> afterSoldDateTypes = concatTypes(afterReturnsTypes, List.of(BIGINT));
        List<Type> afterReturnedDateTypes = concatTypes(afterSoldDateTypes, List.of(BIGINT));
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_store_name", "s_company_id", "s_street_number", "s_street_name", "s_street_type", "s_suite_number", "s_city", "s_county", "s_state", "s_zip"));
        List<Type> outputTypes = List.of(storeTypes.get(1), storeTypes.get(2), storeTypes.get(3), storeTypes.get(4), storeTypes.get(5), storeTypes.get(6), storeTypes.get(7), storeTypes.get(8), storeTypes.get(9), storeTypes.get(10), BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);

        List<Page> returnsPages = relationPages(
                tables,
                "store_returns",
                List.of("sr_returned_date_sk", "sr_ticket_number", "sr_item_sk", "sr_customer_sk"),
                Optional.empty(),
                identityProjections(returnTypes),
                returnTypes);
        List<Page> soldDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> returnedDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year", "d_moy"),
                Optional.of(and(equal(1, 2001, INTEGER), equal(2, 8, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name", "s_company_id", "s_street_number", "s_street_name", "s_street_type", "s_suite_number", "s_city", "s_county", "s_state", "s_zip"),
                Optional.empty(),
                identityProjections(storeTypes),
                storeTypes);

        return executePipeline(
                tables.tableFiles("store_sales"),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(50_0, factTypes, List.of(1, 2, 3), returnsPages, returnTypes, List.of(1, 2, 3))),
                        hashJoinStep(new HashJoinSpec(50_1, afterReturnsTypes, List.of(0), soldDatePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(50_2, afterSoldDateTypes, List.of(5), returnedDatePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(50_3, afterReturnedDateTypes, List.of(4), storePages, storeTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                50_4,
                                Optional.empty(),
                                query50BucketProjections(storeTypes),
                                outputTypes)),
                        factoryStep(hashAggregationFactory(
                                50_5,
                                outputTypes.subList(0, 10),
                                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(10), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(11), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(12), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(13), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(14), OptionalInt.empty()))),
                        factoryStep(topNFactory(50_6, outputTypes, 100, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query52(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type salesType = salesTypes.get(2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type brandIdType = tableColumnTypes(tables, "item", List.of("i_brand_id")).getFirst();
        Type brandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        List<Type> outputTypes = List.of(INTEGER, brandIdType, brandType, salesSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(equal(1, 11, INTEGER), equal(2, 2000, INTEGER))),
                List.of(field(0, BIGINT), field(2, INTEGER)),
                List.of(BIGINT, INTEGER));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_manager_id", "i_brand_id", "i_brand"),
                Optional.of(equal(1, 1, INTEGER)),
                List.of(field(0, BIGINT), field(2, brandIdType), field(3, brandType)),
                List.of(BIGINT, brandIdType, brandType));

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(52_0, salesTypes, List.of(0), datePages, List.of(BIGINT, INTEGER), List.of(0))),
                        hashJoinStep(new HashJoinSpec(52_1, concatTypes(salesTypes, List.of(BIGINT, INTEGER)), List.of(1), itemPages, List.of(BIGINT, brandIdType, brandType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                52_2,
                                Optional.empty(),
                                List.of(field(4, INTEGER), field(6, brandIdType), field(7, brandType), field(2, salesType)),
                                List.of(INTEGER, brandIdType, brandType, salesType))),
                        factoryStep(hashAggregationFactory(
                                52_3,
                                List.of(INTEGER, brandIdType, brandType),
                                List.of(0, 1, 2),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(topNFactory(52_4, outputTypes, 100, List.of(0, 3, 1), List.of(ASC_NULLS_LAST, DESC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query68(TpcdsParquetTables tables)
    {
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price", "ss_ext_list_price", "ss_ext_tax"));
        Type extendedPriceType = salesTypes.get(0);
        Type listPriceType = salesTypes.get(1);
        Type extendedTaxType = salesTypes.get(2);
        TestingAggregationFunction extendedPriceSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(extendedPriceType));
        TestingAggregationFunction listPriceSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(listPriceType));
        TestingAggregationFunction extendedTaxSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(extendedTaxType));
        List<Type> groupedTypes = List.of(BIGINT, BIGINT, VARCHAR, extendedPriceSum.getFinalType(), listPriceSum.getFinalType(), extendedTaxSum.getFinalType());
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk", "c_last_name", "c_first_name"));
        List<Type> projectedCustomerTypes = List.of(BIGINT, customerTypes.get(1), customerTypes.get(2), customerTypes.get(3));
        List<Type> afterCustomerTypes = concatTypes(groupedTypes, projectedCustomerTypes);
        List<Type> currentAddressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_city"));
        List<Type> outputTypes = List.of(customerTypes.get(2), customerTypes.get(3), currentAddressTypes.get(1), VARCHAR, BIGINT, extendedPriceSum.getFinalType(), extendedTaxSum.getFinalType(), listPriceSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_dom", "d_year"),
                Optional.of(query68DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_city"),
                Optional.of(varcharAnyOf(1, Set.of("Fairview", "Midway"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Type> householdTypes = tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"));
        List<Page> householdPages = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"),
                Optional.of(query46HouseholdPredicate(householdTypes.get(1), householdTypes.get(2))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> boughtAddressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_city"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk", "c_last_name", "c_first_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, customerTypes.get(1)), field(2, customerTypes.get(2)), field(3, customerTypes.get(3))),
                projectedCustomerTypes);
        List<Page> currentAddressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_city"),
                Optional.empty(),
                List.of(field(0, currentAddressTypes.get(0)), field(1, currentAddressTypes.get(1))),
                currentAddressTypes);

        return executePagesPipeline(
                executePipelinePages(
                        tables.tableFiles("store_sales"),
                        List.of("ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_ticket_number", "ss_customer_sk", "ss_ext_sales_price", "ss_ext_list_price", "ss_ext_tax"),
                        List.of(
                                hashJoinStep(new HashJoinSpec(68_0, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, extendedPriceType, listPriceType, extendedTaxType), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(68_1, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, extendedPriceType, listPriceType, extendedTaxType, BIGINT), List.of(1), storePages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(68_2, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, extendedPriceType, listPriceType, extendedTaxType, BIGINT, BIGINT), List.of(2), householdPages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(68_3, List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, extendedPriceType, listPriceType, extendedTaxType, BIGINT, BIGINT, BIGINT), List.of(3), boughtAddressPages, List.of(BIGINT, VARCHAR), List.of(0))),
                                factoryStep(filterAndProjectFactory(
                                        68_4,
                                        Optional.empty(),
                                        List.of(field(4, BIGINT), field(5, BIGINT), field(13, VARCHAR), field(6, extendedPriceType), field(7, listPriceType), field(8, extendedTaxType)),
                                        List.of(BIGINT, BIGINT, VARCHAR, extendedPriceType, listPriceType, extendedTaxType))),
                                factoryStep(hashAggregationFactory(
                                        68_5,
                                        List.of(BIGINT, BIGINT, VARCHAR),
                                        List.of(0, 1, 2),
                                        extendedPriceSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                        listPriceSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                        extendedTaxSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))))),
                List.of(
                        hashJoinStep(new HashJoinSpec(68_6, groupedTypes, List.of(1), customerPages, projectedCustomerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(68_7, afterCustomerTypes, List.of(7), currentAddressPages, currentAddressTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                68_8,
                                Optional.of(notEqual(field(11, currentAddressTypes.get(1)), field(2, VARCHAR))),
                                List.of(field(8, customerTypes.get(2)), field(9, customerTypes.get(3)), field(11, currentAddressTypes.get(1)), field(2, VARCHAR), field(0, BIGINT), field(3, extendedPriceSum.getFinalType()), field(5, extendedTaxSum.getFinalType()), field(4, listPriceSum.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(68_9, outputTypes, 100, List.of(0, 4), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query79(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk", "ss_ticket_number", "ss_customer_sk", "ss_addr_sk", "ss_coupon_amt", "ss_net_profit");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type couponType = salesTypes.get(6);
        Type profitType = salesTypes.get(7);
        TestingAggregationFunction couponSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(couponType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));
        List<Type> groupedTypes = List.of(BIGINT, BIGINT, BIGINT, VARCHAR, couponSum.getFinalType(), profitSum.getFinalType());
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name"));
        List<Type> projectedCustomerTypes = List.of(BIGINT, customerTypes.get(1), customerTypes.get(2));
        List<Type> outputTypes = List.of(customerTypes.get(1), customerTypes.get(2), VARCHAR, BIGINT, couponSum.getFinalType(), profitSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_dow", "d_year"),
                Optional.of(query79DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_city", "s_number_employees"),
                Optional.of(and(greaterThan(field(2, INTEGER), constant(199L, INTEGER), INTEGER), lessThan(field(2, INTEGER), constant(296L, INTEGER), INTEGER))),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Type> householdTypes = tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"));
        List<Page> householdPages = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"),
                Optional.of(query79HouseholdPredicate(householdTypes.get(1), householdTypes.get(2))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_last_name", "c_first_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, customerTypes.get(1)), field(2, customerTypes.get(2))),
                projectedCustomerTypes);

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(79_0, salesTypes, List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(79_1, concatTypes(salesTypes, List.of(BIGINT)), List.of(1), storePages, List.of(BIGINT, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(79_2, concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, VARCHAR)), List.of(2), householdPages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                79_3,
                                Optional.empty(),
                                List.of(
                                        field(3, BIGINT),
                                        field(4, BIGINT),
                                        field(5, BIGINT),
                                        field(10, VARCHAR),
                                        coalesce(field(6, couponType), constant(0L, couponType), couponType),
                                        coalesce(field(7, profitType), constant(0L, profitType), profitType)),
                                List.of(BIGINT, BIGINT, BIGINT, VARCHAR, couponType, profitType))),
                        factoryStep(hashAggregationFactory(
                                79_4,
                                groupedTypes.subList(0, 4),
                                List.of(0, 1, 2, 3),
                                couponSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                profitSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        hashJoinStep(new HashJoinSpec(79_5, groupedTypes, List.of(1), customerPages, projectedCustomerTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                79_6,
                                Optional.empty(),
                                List.of(
                                        field(7, customerTypes.get(1)),
                                        field(8, customerTypes.get(2)),
                                        substring(field(3, VARCHAR), 1, 30),
                                        field(0, BIGINT),
                                        field(4, couponSum.getFinalType()),
                                        field(5, profitSum.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(79_7, outputTypes, 100, List.of(0, 1, 2, 5), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query91(TpcdsParquetTables tables)
    {
        List<Type> returnTypes = tableColumnTypes(tables, "catalog_returns", List.of("cr_call_center_sk", "cr_returned_date_sk", "cr_returning_customer_sk", "cr_net_loss"));
        Type netLossType = returnTypes.get(3);
        TestingAggregationFunction netLossSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(netLossType));
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, netLossSum.getFinalType());
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, netLossSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year", "d_moy"),
                Optional.of(and(equal(1, 1998, INTEGER), equal(2, 11, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk"));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, customerTypes.get(1)), field(2, customerTypes.get(2)), field(3, customerTypes.get(3))),
                customerTypes);
        List<Type> customerDemographicsTypes = tableColumnTypes(tables, "customer_demographics", List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"));
        List<Page> customerDemographicsPages = relationPages(
                tables,
                "customer_demographics",
                List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"),
                Optional.of(query91CustomerDemographicsPredicate()),
                List.of(field(0, BIGINT), field(1, customerDemographicsTypes.get(1)), field(2, customerDemographicsTypes.get(2))),
                List.of(BIGINT, customerDemographicsTypes.get(1), customerDemographicsTypes.get(2)));
        List<Type> householdTypes = tableColumnTypes(tables, "household_demographics", List.of("hd_demo_sk", "hd_buy_potential"));
        List<Page> householdPages = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_buy_potential"),
                Optional.of(equal(1, VARCHAR, "Unknown")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_gmt_offset"));
        List<Page> addressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_gmt_offset"),
                Optional.of(equal(1, -700L, addressTypes.get(1))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Type> callCenterTypes = tableColumnTypes(tables, "call_center", List.of("cc_call_center_sk", "cc_call_center_id", "cc_name", "cc_manager"));
        List<Page> callCenterPages = relationPages(
                tables,
                "call_center",
                List.of("cc_call_center_sk", "cc_call_center_id", "cc_name", "cc_manager"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, callCenterTypes.get(1)), field(2, callCenterTypes.get(2)), field(3, callCenterTypes.get(3))),
                List.of(BIGINT, callCenterTypes.get(1), callCenterTypes.get(2), callCenterTypes.get(3)));

        return executePipeline(
                tables.tableFiles("catalog_returns"),
                List.of("cr_call_center_sk", "cr_returned_date_sk", "cr_returning_customer_sk", "cr_net_loss"),
                List.of(
                        hashJoinStep(new HashJoinSpec(91_0, returnTypes, List.of(1), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(91_1, concatTypes(returnTypes, List.of(BIGINT)), List.of(2), customerPages, customerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(91_2, concatTypes(concatTypes(returnTypes, List.of(BIGINT)), customerTypes), List.of(6), customerDemographicsPages, List.of(BIGINT, customerDemographicsTypes.get(1), customerDemographicsTypes.get(2)), List.of(0))),
                        hashJoinStep(new HashJoinSpec(91_3, concatTypes(concatTypes(concatTypes(returnTypes, List.of(BIGINT)), customerTypes), List.of(BIGINT, customerDemographicsTypes.get(1), customerDemographicsTypes.get(2))), List.of(7), householdPages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(91_4, concatTypes(concatTypes(concatTypes(concatTypes(returnTypes, List.of(BIGINT)), customerTypes), List.of(BIGINT, customerDemographicsTypes.get(1), customerDemographicsTypes.get(2))), List.of(BIGINT)), List.of(8), addressPages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(91_5, concatTypes(concatTypes(concatTypes(concatTypes(concatTypes(returnTypes, List.of(BIGINT)), customerTypes), List.of(BIGINT, customerDemographicsTypes.get(1), customerDemographicsTypes.get(2))), List.of(BIGINT)), List.of(BIGINT)), List.of(0), callCenterPages, List.of(BIGINT, callCenterTypes.get(1), callCenterTypes.get(2), callCenterTypes.get(3)), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                91_6,
                                Optional.empty(),
                                List.of(field(15, callCenterTypes.get(1)), field(16, callCenterTypes.get(2)), field(17, callCenterTypes.get(3)), field(10, customerDemographicsTypes.get(1)), field(11, customerDemographicsTypes.get(2)), field(3, netLossType)),
                                List.of(callCenterTypes.get(1), callCenterTypes.get(2), callCenterTypes.get(3), customerDemographicsTypes.get(1), customerDemographicsTypes.get(2), netLossType))),
                        factoryStep(hashAggregationFactory(
                                91_7,
                                List.of(callCenterTypes.get(1), callCenterTypes.get(2), callCenterTypes.get(3), customerDemographicsTypes.get(1), customerDemographicsTypes.get(2)),
                                List.of(0, 1, 2, 3, 4),
                                netLossSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        factoryStep(topNFactory(91_8, groupedTypes, 100, List.of(5, 0, 1, 2, 3, 4), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                91_9,
                                Optional.empty(),
                                List.of(field(0, callCenterTypes.get(1)), field(1, callCenterTypes.get(2)), field(2, callCenterTypes.get(3)), field(5, netLossSum.getFinalType())),
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query82(TpcdsParquetTables tables)
    {
        return queryInventorySalesItems(
                tables,
                "store_sales",
                "ss_item_sk",
                LocalDate.of(2000, 5, 25),
                62_00L,
                92_00L,
                129L, 270L, 821L, 423L);
    }

    public MaterializedResult query37(TpcdsParquetTables tables)
    {
        return queryInventorySalesItems(
                tables,
                "catalog_sales",
                "cs_item_sk",
                LocalDate.of(2000, 2, 1),
                68_00L,
                98_00L,
                677L, 940L, 694L, 808L);
    }

    public MaterializedResult query40(TpcdsParquetTables tables)
    {
        LocalDate cutoffDate = LocalDate.of(2000, 3, 11);
        List<Type> salesTypes = tableColumnTypes(tables, "catalog_sales", List.of("cs_order_number", "cs_item_sk", "cs_warehouse_sk", "cs_sold_date_sk", "cs_sales_price"));
        List<Type> returnTypes = tableColumnTypes(tables, "catalog_returns", List.of("cr_order_number", "cr_item_sk", "cr_refunded_cash"));
        Type salesPriceType = salesTypes.get(4);
        Type refundedCashType = returnTypes.get(2);
        RowExpression netSales = subtract(field(4, salesPriceType), coalesce(field(7, refundedCashType), constant(0L, refundedCashType), refundedCashType), salesPriceType);
        RowExpression before = lessThan(field(13, DATE), constant(cutoffDate.toEpochDay(), DATE), DATE);
        RowExpression zero = constant(0L, salesPriceType);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesPriceType));
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, salesSum.getFinalType(), salesSum.getFinalType());

        return executePipeline(
                tables.tableFiles("catalog_sales"),
                List.of("cs_order_number", "cs_item_sk", "cs_warehouse_sk", "cs_sold_date_sk", "cs_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                40_0,
                                salesTypes,
                                List.of(0, 1),
                                relationPages(
                                        tables,
                                        "catalog_returns",
                                        List.of("cr_order_number", "cr_item_sk", "cr_refunded_cash"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, BIGINT), field(2, refundedCashType)),
                                        List.of(BIGINT, BIGINT, refundedCashType)),
                                List.of(BIGINT, BIGINT, refundedCashType),
                                List.of(0, 1),
                                JoinOperatorType.probeOuterJoin(false))),
                        hashJoinStep(new HashJoinSpec(
                                40_1,
                                concatTypes(salesTypes, List.of(BIGINT, BIGINT, refundedCashType)),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "warehouse",
                                        List.of("w_warehouse_sk", "w_state"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                40_2,
                                concatTypes(concatTypes(salesTypes, List.of(BIGINT, BIGINT, refundedCashType)), List.of(BIGINT, VARCHAR)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_current_price", "i_item_id"),
                                        Optional.of(betweenInclusive(field(1, tableColumnTypes(tables, "item", List.of("i_current_price")).getFirst()), 99L, 149L, tableColumnTypes(tables, "item", List.of("i_current_price")).getFirst())),
                                        List.of(field(0, BIGINT), field(2, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                40_3,
                                concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT, BIGINT, refundedCashType)), List.of(BIGINT, VARCHAR)), List.of(BIGINT, VARCHAR)),
                                List.of(3),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_date"),
                                        Optional.of(betweenInclusive(field(1, DATE), cutoffDate.minusDays(30).toEpochDay(), cutoffDate.plusDays(30).toEpochDay(), DATE)),
                                        List.of(field(0, BIGINT), field(1, DATE)),
                                        List.of(BIGINT, DATE)),
                                List.of(BIGINT, DATE),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                40_4,
                                Optional.empty(),
                                List.of(
                                        field(9, VARCHAR),
                                        field(11, VARCHAR),
                                        ifExpression(before, netSales, zero, salesPriceType),
                                        ifExpression(before, zero, netSales, salesPriceType)),
                                List.of(VARCHAR, VARCHAR, salesPriceType, salesPriceType))),
                        factoryStep(hashAggregationFactory(
                                40_5,
                                List.of(VARCHAR, VARCHAR),
                                List.of(0, 1),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(topNFactory(40_6, outputTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query55(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst();
        Type managerIdType = tableColumnTypes(tables, "item", List.of("i_manager_id")).getFirst();
        Type brandIdType = tableColumnTypes(tables, "item", List.of("i_brand_id")).getFirst();
        Type brandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> outputTypes = List.of(brandIdType, brandType, salesSum.getFinalType());

        return executePagesPipeline(
                executePipelinePages(
                        tables.tableFiles("store_sales"),
                        List.of("ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"),
                        List.of(
                        hashJoinStep(new HashJoinSpec(
                                55_0,
                                List.of(BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_moy", "d_year"),
                                        Optional.of(and(equal(1, 11, INTEGER), equal(2, 1999, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                55_1,
                                List.of(BIGINT, BIGINT, salesType, BIGINT),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_manager_id", "i_brand_id", "i_brand"),
                                        Optional.of(equal(1, 28L, managerIdType)),
                                        List.of(field(0, BIGINT), field(2, brandIdType), field(3, brandType)),
                                        List.of(BIGINT, brandIdType, brandType)),
                                List.of(BIGINT, brandIdType, brandType),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                55_2,
                                Optional.empty(),
                                List.of(field(5, brandIdType), field(6, brandType), field(2, salesType)),
                                List.of(brandIdType, brandType, salesType))),
                        factoryStep(hashAggregationFactory(
                                55_3,
                                List.of(brandIdType, brandType),
                                List.of(0, 1),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        factoryStep(topNFactory(55_4, outputTypes, 100, List.of(2, 0), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST))))),
                List.of(),
                outputTypes);
    }

    public MaterializedResult query71(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst();
        Type managerIdType = tableColumnTypes(tables, "item", List.of("i_manager_id")).getFirst();
        Type brandIdType = tableColumnTypes(tables, "item", List.of("i_brand_id")).getFirst();
        Type brandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> outputTypes = List.of(brandIdType, brandType, INTEGER, INTEGER, salesSum.getFinalType());

        List<Page> combinedPages = new ArrayList<>(query71ChannelSalesPages(tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_sold_time_sk", "ws_ext_sales_price", 71_00));
        combinedPages.addAll(query71ChannelSalesPages(tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_sold_time_sk", "cs_ext_sales_price", 71_10));
        combinedPages.addAll(query71ChannelSalesPages(tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_sold_time_sk", "ss_ext_sales_price", 71_20));

        return executePagesPipeline(
                combinedPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                71_30,
                                List.of(BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_manager_id", "i_brand_id", "i_brand"),
                                        Optional.of(equal(1, 1L, managerIdType)),
                                        List.of(field(0, BIGINT), field(2, brandIdType), field(3, brandType)),
                                        List.of(BIGINT, brandIdType, brandType)),
                                List.of(BIGINT, brandIdType, brandType),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                71_31,
                                List.of(BIGINT, BIGINT, salesType, BIGINT, brandIdType, brandType),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "time_dim",
                                        List.of("t_time_sk", "t_hour", "t_minute", "t_meal_time"),
                                        Optional.of(varcharAnyOf(3, Set.of("breakfast", "dinner"))),
                                        List.of(field(0, BIGINT), field(1, INTEGER), field(2, INTEGER)),
                                        List.of(BIGINT, INTEGER, INTEGER)),
                                List.of(BIGINT, INTEGER, INTEGER),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                71_32,
                                Optional.empty(),
                                List.of(field(4, brandIdType), field(5, brandType), field(7, INTEGER), field(8, INTEGER), field(2, salesType)),
                                List.of(brandIdType, brandType, INTEGER, INTEGER, salesType))),
                        factoryStep(hashAggregationFactory(
                                71_33,
                                List.of(brandIdType, brandType, INTEGER, INTEGER),
                                List.of(0, 1, 2, 3),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()))),
                        factoryStep(topNFactory(71_34, outputTypes, 100, List.of(4, 0, 2, 3), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query72(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = List.of(
                tableColumnTypes(tables, "item", List.of("i_item_desc")).getFirst(),
                tableColumnTypes(tables, "warehouse", List.of("w_warehouse_name")).getFirst(),
                tableColumnTypes(tables, "date_dim", List.of("d_week_seq")).getFirst(),
                BIGINT,
                BIGINT,
                BIGINT);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (org.weakref.nitro.data.Row row : TpcdsManualQuerySupport.query72Rows(new Allocator(), tables)) {
            result.row(row.values());
        }
        return result.build();
    }

    public MaterializedResult query33(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst();
        Type manufactIdType = tableColumnTypes(tables, "item", List.of("i_manufact_id")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction totalSalesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesSum.getFinalType()));
        List<Type> channelTypes = List.of(manufactIdType, salesSum.getFinalType());

        List<Page> combinedPages = new ArrayList<>(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_addr_sk",
                "ss_ext_sales_price",
                List.of("i_item_sk", "i_category", "i_manufact_id"),
                Optional.of(equal(1, VARCHAR, "Electronics")),
                List.of(field(0, BIGINT), field(2, manufactIdType)),
                List.of(BIGINT, manufactIdType),
                1998,
                5,
                33_00));
        combinedPages.addAll(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_addr_sk",
                "cs_ext_sales_price",
                List.of("i_item_sk", "i_category", "i_manufact_id"),
                Optional.of(equal(1, VARCHAR, "Electronics")),
                List.of(field(0, BIGINT), field(2, manufactIdType)),
                List.of(BIGINT, manufactIdType),
                1998,
                5,
                33_10));
        combinedPages.addAll(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_item_sk",
                "ws_bill_addr_sk",
                "ws_ext_sales_price",
                List.of("i_item_sk", "i_category", "i_manufact_id"),
                Optional.of(equal(1, VARCHAR, "Electronics")),
                List.of(field(0, BIGINT), field(2, manufactIdType)),
                List.of(BIGINT, manufactIdType),
                1998,
                5,
                33_20));

        return executePagesPipeline(
                combinedPages,
                List.of(
                        factoryStep(hashAggregationFactory(
                                33_30,
                                List.of(manufactIdType),
                                List.of(0),
                                totalSalesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(topNFactory(33_31, channelTypes, 100, List.of(1), List.of(ASC_NULLS_LAST)))),
                channelTypes);
    }

    public MaterializedResult query56(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction totalSalesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesSum.getFinalType()));
        List<Type> channelTypes = List.of(VARCHAR, salesSum.getFinalType());

        RowExpression colorFilter = varcharAnyOf(1, Set.of("slate", "blanched", "burnished"));
        List<Page> combinedPages = new ArrayList<>(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_addr_sk",
                "ss_ext_sales_price",
                List.of("i_item_sk", "i_color", "i_item_id"),
                Optional.of(colorFilter),
                List.of(field(0, BIGINT), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR),
                2001,
                2,
                56_00));
        combinedPages.addAll(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_addr_sk",
                "cs_ext_sales_price",
                List.of("i_item_sk", "i_color", "i_item_id"),
                Optional.of(colorFilter),
                List.of(field(0, BIGINT), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR),
                2001,
                2,
                56_10));
        combinedPages.addAll(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_item_sk",
                "ws_bill_addr_sk",
                "ws_ext_sales_price",
                List.of("i_item_sk", "i_color", "i_item_id"),
                Optional.of(colorFilter),
                List.of(field(0, BIGINT), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR),
                2001,
                2,
                56_20));

        return executePagesPipeline(
                combinedPages,
                List.of(
                        factoryStep(hashAggregationFactory(
                                56_30,
                                List.of(VARCHAR),
                                List.of(0),
                                totalSalesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(topNFactory(56_31, channelTypes, 100, List.of(1, 0), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                channelTypes);
    }

    public MaterializedResult query60(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction totalSalesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesSum.getFinalType()));
        List<Type> channelTypes = List.of(VARCHAR, salesSum.getFinalType());

        RowExpression categoryFilter = equal(1, VARCHAR, "Music");
        List<Page> combinedPages = new ArrayList<>(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_addr_sk",
                "ss_ext_sales_price",
                List.of("i_item_sk", "i_category", "i_item_id"),
                Optional.of(categoryFilter),
                List.of(field(0, BIGINT), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR),
                1998,
                9,
                60_00));
        combinedPages.addAll(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_addr_sk",
                "cs_ext_sales_price",
                List.of("i_item_sk", "i_category", "i_item_id"),
                Optional.of(categoryFilter),
                List.of(field(0, BIGINT), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR),
                1998,
                9,
                60_10));
        combinedPages.addAll(queryGroupedChannelSalesWithAddressOffsetPages(
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_item_sk",
                "ws_bill_addr_sk",
                "ws_ext_sales_price",
                List.of("i_item_sk", "i_category", "i_item_id"),
                Optional.of(categoryFilter),
                List.of(field(0, BIGINT), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR),
                1998,
                9,
                60_20));

        return executePagesPipeline(
                combinedPages,
                List.of(
                        factoryStep(hashAggregationFactory(
                                60_30,
                                List.of(VARCHAR),
                                List.of(0),
                                totalSalesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(topNFactory(60_31, channelTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                channelTypes);
    }

    public MaterializedResult query65(TpcdsParquetTables tables)
    {
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sales_price"));
        Type salesType = salesTypes.getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type revenueType = salesSum.getFinalType();
        Type priceType = tableColumnTypes(tables, "item", List.of("i_current_price")).getFirst();
        Type wholesaleType = tableColumnTypes(tables, "item", List.of("i_wholesale_cost")).getFirst();
        Type brandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        List<Type> groupedSalesTypes = List.of(BIGINT, BIGINT, revenueType);
        List<Type> thresholdTypes = List.of(BIGINT, BIGINT);
        List<Type> afterThresholdTypes = concatTypes(groupedSalesTypes, thresholdTypes);
        List<Type> afterStoreTypes = concatTypes(afterThresholdTypes, List.of(BIGINT, VARCHAR));
        List<Type> afterItemTypes = concatTypes(afterStoreTypes, List.of(BIGINT, VARCHAR, priceType, wholesaleType, brandType));
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, revenueType, priceType, wholesaleType, brandType);

        List<Page> groupedSalesPages = query65StoreItemSalesPages(tables);
        List<Page> thresholdPages = executePipelinePages(
                groupedSalesPages,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                65_10,
                                Optional.empty(),
                                List.of(
                                        field(0, BIGINT),
                                        cast(
                                                multiply(
                                                        field(2, revenueType),
                                                        constant(100L, createDecimalType(3, 0))),
                                                multiply(field(2, revenueType), constant(100L, createDecimalType(3, 0))).type(),
                                                BIGINT)),
                                List.of(BIGINT, BIGINT))),
                        factoryStep(hashAggregationFactory(
                                65_11,
                                List.of(BIGINT),
                                List.of(0),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                65_12,
                                Optional.empty(),
                                List.of(field(0, BIGINT), query44AverageKey(field(2, BIGINT), field(1, BIGINT))),
                                thresholdTypes))));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR), field(2, priceType), field(3, wholesaleType), field(4, brandType)),
                List.of(BIGINT, VARCHAR, priceType, wholesaleType, brandType));

        return executePagesPipeline(
                groupedSalesPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(65_20, groupedSalesTypes, List.of(0), thresholdPages, thresholdTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                65_21,
                                Optional.of(query65ThresholdPredicate(revenueType)),
                                identityProjections(afterThresholdTypes),
                                afterThresholdTypes)),
                        hashJoinStep(new HashJoinSpec(65_22, afterThresholdTypes, List.of(0), storePages, List.of(BIGINT, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(65_23, afterStoreTypes, List.of(1), itemPages, List.of(BIGINT, VARCHAR, priceType, wholesaleType, brandType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                65_24,
                                Optional.empty(),
                                List.of(field(6, VARCHAR), field(8, VARCHAR), field(2, revenueType), field(9, priceType), field(10, wholesaleType), field(11, brandType)),
                                outputTypes)),
                        factoryStep(topNFactory(65_25, outputTypes, 100, List.of(0, 1, 5, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query66(TpcdsParquetTables tables)
    {
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sales_price"));
        Type salesType = salesTypes.getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type revenueType = salesSum.getFinalType();
        Type priceType = tableColumnTypes(tables, "item", List.of("i_current_price")).getFirst();
        Type wholesaleType = tableColumnTypes(tables, "item", List.of("i_wholesale_cost")).getFirst();
        Type brandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        List<Type> groupedSalesTypes = List.of(BIGINT, BIGINT, revenueType);
        List<Type> thresholdTypes = List.of(BIGINT, BIGINT);
        List<Type> afterThresholdTypes = concatTypes(groupedSalesTypes, thresholdTypes);
        List<Type> afterStoreTypes = concatTypes(afterThresholdTypes, List.of(BIGINT, VARCHAR));
        List<Type> afterItemTypes = concatTypes(afterStoreTypes, List.of(BIGINT, VARCHAR, priceType, wholesaleType, brandType));
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, revenueType, priceType, wholesaleType, brandType);

        List<Page> groupedSalesPages = query65StoreItemSalesPages(tables);
        List<Page> thresholdPages = executePipelinePages(
                groupedSalesPages,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                66_10,
                                Optional.empty(),
                                List.of(
                                        field(0, BIGINT),
                                        cast(
                                                multiply(
                                                        field(2, revenueType),
                                                        constant(100L, createDecimalType(3, 0))),
                                                multiply(field(2, revenueType), constant(100L, createDecimalType(3, 0))).type(),
                                                BIGINT)),
                                List.of(BIGINT, BIGINT))),
                        factoryStep(hashAggregationFactory(
                                66_11,
                                List.of(BIGINT),
                                List.of(0),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                66_12,
                                Optional.empty(),
                                List.of(field(0, BIGINT), query44AverageKey(field(2, BIGINT), field(1, BIGINT))),
                                thresholdTypes))));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR), field(2, priceType), field(3, wholesaleType), field(4, brandType)),
                List.of(BIGINT, VARCHAR, priceType, wholesaleType, brandType));

        return executePagesPipeline(
                groupedSalesPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(66_20, groupedSalesTypes, List.of(0), thresholdPages, thresholdTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                66_21,
                                Optional.of(query65ThresholdPredicate(revenueType)),
                                identityProjections(afterThresholdTypes),
                                afterThresholdTypes)),
                        hashJoinStep(new HashJoinSpec(66_22, afterThresholdTypes, List.of(0), storePages, List.of(BIGINT, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(66_23, afterStoreTypes, List.of(1), itemPages, List.of(BIGINT, VARCHAR, priceType, wholesaleType, brandType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                66_24,
                                Optional.empty(),
                                List.of(field(6, VARCHAR), field(8, VARCHAR), field(2, revenueType), field(9, priceType), field(10, wholesaleType), field(11, brandType)),
                                outputTypes)),
                        factoryStep(topNFactory(66_25, outputTypes, 100, List.of(0, 1, 5, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query98(TpcdsParquetTables tables)
    {
        return queryRevenueRatioByClass(tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
    }

    public MaterializedResult query89(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction monthlyAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(salesSum.getFinalType()));
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_category", "i_class", "i_brand"));
        List<Type> projectedItemTypes = List.of(BIGINT, itemTypes.get(1), itemTypes.get(2), itemTypes.get(3));
        List<Type> dateTypes = tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_moy", "d_year"));
        List<Type> projectedDateTypes = List.of(BIGINT, dateTypes.get(1));
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_store_name", "s_company_name"));
        List<Type> groupedTypes = List.of(itemTypes.get(1), itemTypes.get(2), itemTypes.get(3), storeTypes.get(1), storeTypes.get(2), dateTypes.get(1), salesSum.getFinalType());
        List<Type> outputTypes = List.of(itemTypes.get(1), itemTypes.get(2), itemTypes.get(3), storeTypes.get(1), storeTypes.get(2), dateTypes.get(1), salesSum.getFinalType(), monthlyAverage.getFinalType());
        List<Type> sortedTypes = concatTypes(outputTypes, List.of(DOUBLE));

        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_category", "i_class", "i_brand"),
                Optional.of(query89ItemPredicate()),
                List.of(field(0, BIGINT), field(1, itemTypes.get(1)), field(2, itemTypes.get(2)), field(3, itemTypes.get(3))),
                projectedItemTypes);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(equal(2, 1999, INTEGER)),
                List.of(field(0, BIGINT), field(1, dateTypes.get(1))),
                projectedDateTypes);
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name", "s_company_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, storeTypes.get(1)), field(2, storeTypes.get(2))),
                storeTypes);

        return executePipeline(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(89_0, factTypes, List.of(1), itemPages, projectedItemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(89_1, concatTypes(factTypes, projectedItemTypes), List.of(0), datePages, projectedDateTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(89_2, concatTypes(concatTypes(factTypes, projectedItemTypes), projectedDateTypes), List.of(2), storePages, storeTypes, List.of(0))),
                        factoryStep(hashAggregationFactory(
                                89_3,
                                groupedTypes.subList(0, 6),
                                List.of(5, 6, 7, 11, 12, 9),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(windowFactory(
                                89_4,
                                groupedTypes,
                                List.of(0, 1, 2, 3, 4, 5, 6),
                                List.of(0, 2, 3, 4),
                                List.of(),
                                List.of(),
                                List.of(aggregateWindowFunction("avg", List.of(salesSum.getFinalType()), monthlyAverage.getFinalType(), PARTITION_ROWS_FRAME, 6)))),
                        factoryStep(filterAndProjectFactory(
                                89_5,
                                Optional.of(queryRelativeDeviationPredicate(6, 7, salesSum.getFinalType(), monthlyAverage.getFinalType())),
                                List.of(
                                        field(0, itemTypes.get(1)),
                                        field(1, itemTypes.get(2)),
                                        field(2, itemTypes.get(3)),
                                        field(3, storeTypes.get(1)),
                                        field(4, storeTypes.get(2)),
                                        field(5, dateTypes.get(1)),
                                        field(6, salesSum.getFinalType()),
                                        field(7, monthlyAverage.getFinalType()),
                                        subtract(cast(field(6, salesSum.getFinalType()), salesSum.getFinalType(), DOUBLE), cast(field(7, monthlyAverage.getFinalType()), monthlyAverage.getFinalType(), DOUBLE), DOUBLE)),
                                sortedTypes)),
                        factoryStep(topNFactory(89_6, sortedTypes, 100, List.of(8, 3), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                89_7,
                                Optional.empty(),
                                identityProjections(outputTypes),
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query63(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction monthlyAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(salesSum.getFinalType()));
        List<Type> itemTypes = List.of(BIGINT, INTEGER);
        List<Type> dateTypes = List.of(BIGINT, INTEGER);
        List<Type> groupedTypes = List.of(INTEGER, INTEGER, salesSum.getFinalType());
        List<Type> outputTypes = List.of(INTEGER, salesSum.getFinalType(), monthlyAverage.getFinalType());

        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_manager_id", "i_category", "i_class", "i_brand"),
                Optional.of(query53ItemPredicate()),
                List.of(field(0, BIGINT), field(1, INTEGER)),
                itemTypes);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_month_seq"),
                Optional.of(and(greaterThan(2, 1199, INTEGER), lessThan(2, 1212, INTEGER))),
                List.of(field(0, BIGINT), field(1, INTEGER)),
                dateTypes);
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(63_0, factTypes, List.of(1), itemPages, itemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(63_1, concatTypes(factTypes, itemTypes), List.of(0), datePages, dateTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(63_2, concatTypes(concatTypes(factTypes, itemTypes), dateTypes), List.of(2), storePages, List.of(BIGINT), List.of(0))),
                        factoryStep(hashAggregationFactory(
                                63_3,
                                List.of(INTEGER, INTEGER),
                                List.of(5, 7),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(windowFactory(
                                63_4,
                                groupedTypes,
                                List.of(0, 1, 2),
                                List.of(0),
                                List.of(),
                                List.of(),
                                List.of(aggregateWindowFunction("avg", List.of(salesSum.getFinalType()), monthlyAverage.getFinalType(), PARTITION_ROWS_FRAME, 2)))),
                        factoryStep(filterAndProjectFactory(
                                63_5,
                                Optional.of(queryRelativeDeviationPredicate(2, 3, salesSum.getFinalType(), monthlyAverage.getFinalType())),
                                List.of(
                                        field(0, INTEGER),
                                        field(2, salesSum.getFinalType()),
                                        field(3, monthlyAverage.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(63_6, outputTypes, 100, List.of(0, 2, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query86(TpcdsParquetTables tables)
    {
        List<String> factColumns = List.of("ws_sold_date_sk", "ws_item_sk", "ws_net_paid");
        List<Type> factTypes = tableColumnTypes(tables, "web_sales", factColumns);
        Type netPaidType = factTypes.get(2);
        TestingAggregationFunction sumFunction = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(netPaidType));
        List<Type> sourceTypes = List.of(VARCHAR, VARCHAR, netPaidType);
        List<Type> groupIdTypes = concatTypes(sourceTypes, List.of(BIGINT));
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, BIGINT, sumFunction.getFinalType());
        List<Type> rollupTypes = List.of(VARCHAR, VARCHAR, sumFunction.getFinalType(), INTEGER, VARCHAR);
        List<Type> rankedTypes = concatTypes(rollupTypes, List.of(BIGINT));
        List<Type> outputTypes = List.of(sumFunction.getFinalType(), VARCHAR, VARCHAR, INTEGER, BIGINT);

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_month_seq"),
                Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_class", "i_category"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR, VARCHAR));

        return executePipeline(
                tables.tableFiles("web_sales"),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(86_0, factTypes, List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(86_1, concatTypes(factTypes, List.of(BIGINT)), List.of(1), itemPages, List.of(BIGINT, VARCHAR, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                86_2,
                                Optional.empty(),
                                List.of(field(6, VARCHAR), field(5, VARCHAR), field(2, netPaidType)),
                                sourceTypes)),
                        factoryStep(groupIdFactory(
                                86_3,
                                groupIdTypes,
                                List.of(
                                        Map.of(2, 2),
                                        Map.of(0, 0, 2, 2),
                                        Map.of(0, 0, 1, 1, 2, 2)))),
                        factoryStep(hashAggregationFactory(
                                86_4,
                                List.of(VARCHAR, VARCHAR, BIGINT),
                                List.of(0, 1, 3),
                                sumFunction.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                86_5,
                                Optional.empty(),
                                query86RollupProjection(sumFunction.getFinalType()),
                                rollupTypes)),
                        factoryStep(topNRankingFactory(86_6, rollupTypes, List.of(0, 1, 2, 3, 4), List.of(3, 4), List.of(2), List.of(DESC_NULLS_LAST), 100)),
                        factoryStep(topNFactory(86_7, rankedTypes, 100, List.of(3, 4, 5), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                86_8,
                                Optional.empty(),
                                List.of(field(2, sumFunction.getFinalType()), field(0, VARCHAR), field(1, VARCHAR), field(3, INTEGER), field(5, BIGINT)),
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query36(TpcdsParquetTables tables)
    {
        List<String> factColumns = List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_ext_sales_price", "ss_net_profit");
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", factColumns);
        Type salesType = factTypes.get(3);
        Type netProfitType = factTypes.get(4);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction netProfitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(netProfitType));
        Type grossMarginType = createDecimalType(38, 6);
        List<Type> sourceTypes = List.of(VARCHAR, VARCHAR, salesType, netProfitType);
        List<Type> groupIdTypes = concatTypes(sourceTypes, List.of(BIGINT));
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, BIGINT, netProfitSum.getFinalType(), salesSum.getFinalType());
        List<Type> rollupTypes = List.of(VARCHAR, VARCHAR, grossMarginType, INTEGER, VARCHAR);
        List<Type> rankedTypes = concatTypes(rollupTypes, List.of(BIGINT));
        List<Type> outputTypes = List.of(grossMarginType, VARCHAR, VARCHAR, INTEGER, BIGINT);

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(equal(1, 2001, INTEGER)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_class", "i_category"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR, VARCHAR));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_state"),
                Optional.of(equal(1, VARCHAR, "TN")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles("store_sales"),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(36_0, factTypes, List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(36_1, concatTypes(factTypes, List.of(BIGINT)), List.of(1), itemPages, List.of(BIGINT, VARCHAR, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(36_2, concatTypes(concatTypes(factTypes, List.of(BIGINT)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(2), storePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                36_3,
                                Optional.empty(),
                                List.of(field(8, VARCHAR), field(7, VARCHAR), field(3, salesType), field(4, netProfitType)),
                                sourceTypes)),
                        factoryStep(groupIdFactory(
                                36_4,
                                groupIdTypes,
                                List.of(
                                        Map.of(2, 2, 3, 3),
                                        Map.of(0, 0, 2, 2, 3, 3),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3)))),
                        factoryStep(hashAggregationFactory(
                                36_5,
                                List.of(VARCHAR, VARCHAR, BIGINT),
                                List.of(0, 1, 4),
                                netProfitSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                36_6,
                                Optional.empty(),
                                query36RollupProjection(netProfitSum.getFinalType(), salesSum.getFinalType(), grossMarginType),
                                rollupTypes)),
                        factoryStep(topNRankingFactory(36_7, rollupTypes, List.of(0, 1, 2, 3, 4), List.of(3, 4), List.of(2), List.of(ASC_NULLS_LAST), 100)),
                        factoryStep(topNFactory(36_8, rankedTypes, 100, List.of(3, 4, 5, 0, 1), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                36_9,
                                Optional.empty(),
                                List.of(field(2, grossMarginType), field(0, VARCHAR), field(1, VARCHAR), field(3, INTEGER), field(5, BIGINT)),
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query49(TpcdsParquetTables tables)
    {
        Type ratioType = createDecimalType(19, 12);
        List<Type> outputTypes = List.of(VARCHAR, BIGINT, ratioType, BIGINT, BIGINT);

        List<Page> unionPages = new ArrayList<>(query49ChannelPages(tables, "store", "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number", "ss_quantity", "ss_net_paid", "ss_net_profit", "store_returns", "sr_item_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt", 49_000, ratioType));
        unionPages.addAll(query49ChannelPages(tables, "catalog", "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_order_number", "cs_quantity", "cs_net_paid", "cs_net_profit", "catalog_returns", "cr_item_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount", 49_100, ratioType));
        unionPages.addAll(query49ChannelPages(tables, "web", "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_order_number", "ws_quantity", "ws_net_paid", "ws_net_profit", "web_returns", "wr_item_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt", 49_200, ratioType));

        return executePagesPipeline(
                unionPages,
                List.of(factoryStep(topNFactory(49_900, outputTypes, 100, List.of(0, 3, 4, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query47(TpcdsParquetTables tables)
    {
        List<Type> monthlyRankedTypes = query47MonthlyRankedTypes(tables);
        Type categoryType = monthlyRankedTypes.get(0);
        Type brandType = monthlyRankedTypes.get(1);
        Type storeNameType = monthlyRankedTypes.get(2);
        Type companyNameType = monthlyRankedTypes.get(3);
        Type yearType = monthlyRankedTypes.get(4);
        Type monthType = monthlyRankedTypes.get(5);
        Type sumType = monthlyRankedTypes.get(6);
        Type averageType = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(sumType)).getFinalType();

        List<Type> currentTypes = List.of(categoryType, brandType, storeNameType, companyNameType, yearType, monthType, averageType, sumType, BIGINT);
        List<Type> adjacentTypes = List.of(categoryType, brandType, storeNameType, companyNameType, sumType, BIGINT);
        List<Type> afterPreviousTypes = concatTypes(currentTypes, adjacentTypes);
        List<Type> afterNextTypes = concatTypes(afterPreviousTypes, adjacentTypes);
        List<Type> sortedTypes = concatTypes(List.of(categoryType, brandType, storeNameType, companyNameType, yearType, monthType, averageType, sumType, sumType, sumType), List.of(sumType));
        List<Type> outputTypes = List.of(categoryType, brandType, storeNameType, companyNameType, yearType, monthType, averageType, sumType, sumType, sumType);

        List<Page> monthlyRankedPages = query47MonthlyRankedSalesPages(tables);
        List<Page> currentPages = executePipelinePages(
                monthlyRankedPages,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                47_10,
                                Optional.of(equal(4, 1999, INTEGER)),
                                identityProjections(monthlyRankedTypes),
                                monthlyRankedTypes)),
                        factoryStep(windowFactory(
                                47_11,
                                monthlyRankedTypes,
                                List.of(0, 1, 2, 3, 4, 5, 6, 7),
                                List.of(0, 1, 2, 3),
                                List.of(),
                                List.of(),
                                List.of(aggregateWindowFunction("avg", List.of(sumType), averageType, PARTITION_ROWS_FRAME, 6)))),
                        factoryStep(filterAndProjectFactory(
                                47_12,
                                Optional.empty(),
                                List.of(field(0, categoryType), field(1, brandType), field(2, storeNameType), field(3, companyNameType), field(4, yearType), field(5, monthType), field(8, averageType), field(6, sumType), field(7, BIGINT)),
                                currentTypes))));
        List<Page> previousPages = executePipelinePages(
                monthlyRankedPages,
                List.of(factoryStep(filterAndProjectFactory(
                        47_13,
                        Optional.empty(),
                        List.of(
                                field(0, categoryType),
                                field(1, brandType),
                                field(2, storeNameType),
                                field(3, companyNameType),
                                field(6, sumType),
                                add(field(7, BIGINT), constant(1L, BIGINT), BIGINT)),
                        adjacentTypes))));
        List<Page> nextPages = executePipelinePages(
                monthlyRankedPages,
                List.of(factoryStep(filterAndProjectFactory(
                        47_14,
                        Optional.empty(),
                        List.of(
                                field(0, categoryType),
                                field(1, brandType),
                                field(2, storeNameType),
                                field(3, companyNameType),
                                field(6, sumType),
                                subtract(field(7, BIGINT), constant(1L, BIGINT), BIGINT)),
                        adjacentTypes))));

        return executePagesPipeline(
                currentPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(47_20, currentTypes, List.of(0, 1, 2, 3, 8), previousPages, adjacentTypes, List.of(0, 1, 2, 3, 5))),
                        hashJoinStep(new HashJoinSpec(47_21, afterPreviousTypes, List.of(0, 1, 2, 3, 8), nextPages, adjacentTypes, List.of(0, 1, 2, 3, 5))),
                        factoryStep(filterAndProjectFactory(
                                47_22,
                                Optional.of(queryRelativeDeviationPredicate(7, 6, sumType, averageType)),
                                List.of(
                                        field(0, categoryType),
                                        field(1, brandType),
                                        field(2, storeNameType),
                                        field(3, companyNameType),
                                        field(4, yearType),
                                        field(5, monthType),
                                        field(6, averageType),
                                        field(7, sumType),
                                        field(13, sumType),
                                        field(19, sumType),
                                        subtract(field(7, sumType), field(6, averageType), sumType)),
                                sortedTypes)),
                        factoryStep(topNFactory(47_23, sortedTypes, 100, List.of(10, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                47_24,
                                Optional.empty(),
                                List.of(
                                        field(0, categoryType),
                                        field(1, brandType),
                                        field(2, storeNameType),
                                        field(3, companyNameType),
                                        field(4, yearType),
                                        field(5, monthType),
                                        field(6, averageType),
                                        field(7, sumType),
                                        field(8, sumType),
                                        field(9, sumType)),
                                outputTypes))),
                outputTypes);
    }

    private MaterializedResult queryRevenueRatioByClass(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String salesColumn)
    {
        List<String> factColumns = List.of(soldDateColumn, itemColumn, salesColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        Type salesType = factTypes.get(2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type groupedRevenueType = salesSum.getFinalType();
        Type ratioType = createDecimalType(38, 6);
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"));
        List<Type> projectedItemTypes = List.of(BIGINT, itemTypes.get(1), itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5));
        List<Type> groupedTypes = List.of(itemTypes.get(1), itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5), groupedRevenueType);
        List<Type> outputTypes = List.of(itemTypes.get(1), itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5), groupedRevenueType, ratioType);

        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"),
                Optional.of(query12CategoryPredicate()),
                List.of(field(0, BIGINT), field(1, itemTypes.get(1)), field(2, itemTypes.get(2)), field(3, itemTypes.get(3)), field(4, itemTypes.get(4)), field(5, itemTypes.get(5))),
                projectedItemTypes);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date"),
                Optional.of(query12DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(12_0, factTypes, List.of(1), itemPages, projectedItemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(12_1, concatTypes(factTypes, projectedItemTypes), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                12_2,
                                Optional.empty(),
                                List.of(
                                        field(4, itemTypes.get(1)),
                                        field(5, itemTypes.get(2)),
                                        field(6, itemTypes.get(3)),
                                        field(7, itemTypes.get(4)),
                                        field(8, itemTypes.get(5)),
                                        field(2, salesType)),
                                List.of(itemTypes.get(1), itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5), salesType))),
                        factoryStep(hashAggregationFactory(
                                12_3,
                                List.of(itemTypes.get(1), itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5)),
                                List.of(0, 1, 2, 3, 4),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        factoryStep(windowFactory(
                                12_4,
                                groupedTypes,
                                List.of(0, 1, 2, 3, 4, 5),
                                List.of(3),
                                List.of(),
                                List.of(),
                                List.of(aggregateWindowFunction("sum", List.of(groupedRevenueType), groupedRevenueType, PARTITION_ROWS_FRAME, 5)))),
                        factoryStep(filterAndProjectFactory(
                                12_5,
                                Optional.empty(),
                                List.of(
                                        field(0, itemTypes.get(1)),
                                        field(1, itemTypes.get(2)),
                                        field(2, itemTypes.get(3)),
                                        field(3, itemTypes.get(4)),
                                        field(4, itemTypes.get(5)),
                                        field(5, groupedRevenueType),
                                        divide(
                                                multiply(field(5, groupedRevenueType), constant(100L, createDecimalType(10, 0))),
                                                field(6, groupedRevenueType))),
                                outputTypes)),
                        factoryStep(topNFactory(12_6, outputTypes, 100, List.of(2, 3, 0, 1, 6), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query09(TpcdsParquetTables tables)
    {
        List<Type> reasonTypes = tableColumnTypes(tables, "reason", List.of("r_reason_sk"));
        List<Page> reasonPages = relationPages(
                tables,
                "reason",
                List.of("r_reason_sk"),
                Optional.of(equal(0, 1L, BIGINT)),
                List.of(field(0, BIGINT)),
                reasonTypes);
        Type bucketType = createDecimalType(7, 2);
        List<Page> joined = executeNestedLoopPages(reasonPages, List.of(BIGINT), query09BucketPages(tables, 1, 20, 74_129L, 9_00), List.of(bucketType));
        joined = executeNestedLoopPages(joined, List.of(BIGINT, bucketType), query09BucketPages(tables, 21, 40, 122_840L, 9_10), List.of(bucketType));
        joined = executeNestedLoopPages(joined, List.of(BIGINT, bucketType, bucketType), query09BucketPages(tables, 41, 60, 56_580L, 9_20), List.of(bucketType));
        joined = executeNestedLoopPages(joined, List.of(BIGINT, bucketType, bucketType, bucketType), query09BucketPages(tables, 61, 80, 10_097L, 9_30), List.of(bucketType));
        joined = executeNestedLoopPages(joined, List.of(BIGINT, bucketType, bucketType, bucketType, bucketType), query09BucketPages(tables, 81, 100, 165_306L, 9_40), List.of(bucketType));
        return executePagesPipeline(
                joined,
                List.of(factoryStep(filterAndProjectFactory(
                        9_50,
                        Optional.empty(),
                        List.of(
                                field(1, bucketType),
                                field(2, bucketType),
                                field(3, bucketType),
                                field(4, bucketType),
                                field(5, bucketType)),
                        List.of(bucketType, bucketType, bucketType, bucketType, bucketType)))),
                List.of(bucketType, bucketType, bucketType, bucketType, bucketType));
    }

    public MaterializedResult query11(TpcdsParquetTables tables)
    {
        List<Page> storeFirstYearPages = query11ChannelYearTotalPages(tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_discount_amt", 2001, 11_100);
        List<Page> storeSecondYearPages = query11ChannelYearTotalPages(tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_discount_amt", 2002, 11_110);
        List<Page> webFirstYearPages = query11ChannelYearTotalPages(tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_discount_amt", 2001, 11_120);
        List<Page> webSecondYearPages = query11ChannelYearTotalPages(tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_discount_amt", 2002, 11_130);

        List<Type> branchTypes = query11BranchTypes(tables, "store_sales", "ss_ext_list_price");
        Type totalType = branchTypes.get(6);
        List<Type> afterStoreSecondYearTypes = concatTypes(branchTypes, branchTypes);
        List<Type> afterWebFirstYearTypes = concatTypes(afterStoreSecondYearTypes, branchTypes);
        List<Type> afterWebSecondYearTypes = concatTypes(afterWebFirstYearTypes, branchTypes);
        List<Type> outputTypes = List.of(branchTypes.get(0), branchTypes.get(1), branchTypes.get(2), branchTypes.get(3), branchTypes.get(4), branchTypes.get(5));

        return executePagesPipeline(
                storeFirstYearPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(11_140, branchTypes, List.of(0), storeSecondYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(11_141, afterStoreSecondYearTypes, List.of(0), webFirstYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(11_142, afterWebFirstYearTypes, List.of(0), webSecondYearPages, branchTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                11_143,
                                Optional.of(query11GrowthPredicate(totalType)),
                                List.of(field(0, branchTypes.get(0)), field(1, branchTypes.get(1)), field(2, branchTypes.get(2)), field(3, branchTypes.get(3)), field(4, branchTypes.get(4)), field(5, branchTypes.get(5))),
                                outputTypes)),
                        factoryStep(topNFactory(11_144, outputTypes, 100, List.of(0, 1, 2, 3), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query07(TpcdsParquetTables tables)
    {
        return querySalesAveragesByItem(
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_cdemo_sk",
                "ss_promo_sk",
                "ss_quantity",
                "ss_list_price",
                "ss_coupon_amt",
                "ss_sales_price",
                7_00);
    }

    public MaterializedResult query08(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_net_profit")).getFirst();
        Type storeNameType = tableColumnTypes(tables, "store", List.of("s_store_name")).getFirst();
        Type storeZipType = tableColumnTypes(tables, "store", List.of("s_zip")).getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> prefixTypes = List.of(VARCHAR);
        List<Type> projectedTypes = List.of(storeNameType, salesType, VARCHAR);
        List<Type> outputTypes = List.of(storeNameType, salesSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_qoy", "d_year"),
                Optional.of(query08DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name", "s_zip"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, storeNameType), field(2, storeZipType)),
                List.of(BIGINT, storeNameType, storeZipType));

        return executePipeline(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_store_sk", "ss_net_profit"),
                List.of(
                        hashJoinStep(new HashJoinSpec(8_100, List.of(BIGINT, BIGINT, salesType), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(8_101, List.of(BIGINT, BIGINT, salesType, BIGINT), List.of(1), storePages, List.of(BIGINT, storeNameType, storeZipType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                8_102,
                                Optional.empty(),
                                List.of(field(5, storeNameType), field(2, salesType), substring(field(6, storeZipType), 1, 2)),
                                projectedTypes)),
                        hashJoinStep(new HashJoinSpec(8_103, projectedTypes, List.of(2), query08QualifiedZipPrefixPages(tables), prefixTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                8_104,
                                Optional.empty(),
                                List.of(field(0, storeNameType), field(1, salesType)),
                                List.of(storeNameType, salesType))),
                        factoryStep(hashAggregationFactory(
                                8_105,
                                List.of(storeNameType),
                                List.of(0),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(topNFactory(8_106, outputTypes, 100, List.of(0), List.of(ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query19(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_ext_sales_price");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type salesType = salesTypes.get(4);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_manager_id", "i_brand_id", "i_brand", "i_manufact_id", "i_manufact"));
        Type managerType = itemTypes.get(1);
        Type brandIdType = itemTypes.get(2);
        Type brandType = itemTypes.get(3);
        Type manufacturerIdType = itemTypes.get(4);
        Type manufacturerType = itemTypes.get(5);
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk"));
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_zip"));
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_zip"));
        List<Type> outputTypes = List.of(brandIdType, brandType, manufacturerIdType, manufacturerType, salesSum.getFinalType());

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(equal(1, 11, INTEGER), equal(2, 1998, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_manager_id", "i_brand_id", "i_brand", "i_manufact_id", "i_manufact"),
                Optional.of(equal(1, 8, managerType)),
                List.of(field(0, BIGINT), field(2, brandIdType), field(3, brandType), field(4, manufacturerIdType), field(5, manufacturerType)),
                List.of(BIGINT, brandIdType, brandType, manufacturerIdType, manufacturerType));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, customerTypes.get(1))),
                customerTypes);
        List<Page> addressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_zip"),
                Optional.empty(),
                List.of(field(0, addressTypes.get(0)), field(1, addressTypes.get(1))),
                addressTypes);
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_zip"),
                Optional.empty(),
                List.of(field(0, storeTypes.get(0)), field(1, storeTypes.get(1))),
                storeTypes);

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(19_0, salesTypes, List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(19_1, concatTypes(salesTypes, List.of(BIGINT)), List.of(1), itemPages, List.of(BIGINT, brandIdType, brandType, manufacturerIdType, manufacturerType), List.of(0))),
                        hashJoinStep(new HashJoinSpec(19_2, concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, brandIdType, brandType, manufacturerIdType, manufacturerType)), List.of(2), customerPages, customerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(19_3, concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, brandIdType, brandType, manufacturerIdType, manufacturerType)), customerTypes), List.of(12), addressPages, addressTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(19_4, concatTypes(concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, brandIdType, brandType, manufacturerIdType, manufacturerType)), customerTypes), addressTypes), List.of(3), storePages, storeTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                19_5,
                                Optional.of(notEqual(
                                        substring(field(14, addressTypes.get(1)), 1, 5),
                                        substring(field(16, storeTypes.get(1)), 1, 5))),
                                List.of(field(7, brandIdType), field(8, brandType), field(9, manufacturerIdType), field(10, manufacturerType), field(4, salesType)),
                                List.of(brandIdType, brandType, manufacturerIdType, manufacturerType, salesType))),
                        factoryStep(hashAggregationFactory(
                                19_6,
                                List.of(brandIdType, brandType, manufacturerIdType, manufacturerType),
                                List.of(0, 1, 2, 3),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()))),
                        factoryStep(topNFactory(19_7, outputTypes, 100, List.of(4, 1, 0, 2, 3), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query26(TpcdsParquetTables tables)
    {
        return querySalesAveragesByItem(
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_cdemo_sk",
                "cs_promo_sk",
                "cs_quantity",
                "cs_list_price",
                "cs_coupon_amt",
                "cs_sales_price",
                26_00);
    }

    private MaterializedResult querySalesAveragesByItem(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String demographicsColumn, String promotionColumn, String quantityColumn, String listPriceColumn, String couponColumn, String salesPriceColumn, int operatorIdBase)
    {
        List<String> salesColumns = List.of(soldDateColumn, itemColumn, demographicsColumn, promotionColumn, quantityColumn, listPriceColumn, couponColumn, salesPriceColumn);
        List<Type> salesTypes = tableColumnTypes(tables, salesTable, salesColumns);
        Type quantityType = salesTypes.get(4);
        Type listPriceType = salesTypes.get(5);
        Type couponType = salesTypes.get(6);
        Type salesPriceType = salesTypes.get(7);
        Type itemIdType = tableColumnTypes(tables, "item", List.of("i_item_id")).getFirst();

        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(equal(1, 2000, INTEGER)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_id"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, itemIdType)),
                List.of(BIGINT, itemIdType));
        List<Page> demographicsPages = relationPages(
                tables,
                "customer_demographics",
                List.of("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"),
                Optional.of(and(equal(1, VARCHAR, "M"), equal(2, VARCHAR, "S"), equal(3, VARCHAR, "College"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> promotionPages = relationPages(
                tables,
                "promotion",
                List.of("p_promo_sk", "p_channel_email", "p_channel_event"),
                Optional.of(or(equal(1, VARCHAR, "N"), equal(2, VARCHAR, "N"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        RowExpression quantityValue = cast(field(4, quantityType), quantityType, BIGINT);
        RowExpression listPriceValue = cast(multiply(field(5, listPriceType), constant(100L, createDecimalType(3, 0))), multiply(field(5, listPriceType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        RowExpression couponValue = cast(multiply(field(6, couponType), constant(100L, createDecimalType(3, 0))), multiply(field(6, couponType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        RowExpression salesPriceValue = cast(multiply(field(7, salesPriceType), constant(100L, createDecimalType(3, 0))), multiply(field(7, salesPriceType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        List<Type> projectedTypes = List.of(itemIdType, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(itemIdType, BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType());

        return executePipeline(
                tables.tableFiles(salesTable),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 1, concatTypes(salesTypes, List.of(BIGINT)), List.of(1), itemPages, List.of(BIGINT, itemIdType), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 2, concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, itemIdType)), List.of(2), demographicsPages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 3, concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT, itemIdType)), List.of(BIGINT)), List.of(3), promotionPages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 4,
                                Optional.empty(),
                                List.of(field(10, itemIdType), quantityValue, listPriceValue, couponValue, salesPriceValue),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 5,
                                List.of(itemIdType),
                                List.of(0),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()))),
                        factoryStep(topNFactory(operatorIdBase + 6, outputTypes, 100, List.of(0), List.of(ASC_NULLS_LAST)))),
                outputTypes);
    }

    private List<Page> query08QualifiedZipPrefixPages(TpcdsParquetTables tables)
    {
        Type zipType = tableColumnTypes(tables, "customer_address", List.of("ca_zip")).getFirst();
        Type preferredFlagType = tableColumnTypes(tables, "customer", List.of("c_preferred_cust_flag")).getFirst();

        List<Page> literalZipValues = executePipelinePages(
                relationPages(
                        tables,
                        "customer_address",
                        List.of("ca_zip"),
                        Optional.of(query08ZipListPredicate(zipType)),
                        List.of(substring(field(0, zipType), 1, 5)),
                        List.of(VARCHAR)),
                List.of(factoryStep(hashAggregationFactory(8_200, List.of(VARCHAR), List.of(0)))));

        List<Page> preferredZipValues = executePipelinePages(
                relationPages(
                        tables,
                        "customer",
                        List.of("c_current_addr_sk", "c_preferred_cust_flag"),
                        Optional.of(equal(1, preferredFlagType, "Y")),
                        List.of(field(0, BIGINT)),
                        List.of(BIGINT)),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                8_201,
                                List.of(BIGINT),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "customer_address",
                                        List.of("ca_address_sk", "ca_zip"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), substring(field(1, zipType), 1, 5)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(8_202, Optional.empty(), List.of(field(2, VARCHAR)), List.of(VARCHAR))),
                        factoryStep(hashAggregationFactory(8_203, List.of(VARCHAR), List.of(0), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                8_204,
                                Optional.of(greaterThan(field(1, BIGINT), constant(10L, BIGINT), BIGINT)),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)))));
        return executePipelinePages(
                literalZipValues,
                List.of(
                        hashJoinStep(new HashJoinSpec(8_205, List.of(VARCHAR), List.of(0), preferredZipValues, List.of(VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                8_206,
                                Optional.empty(),
                                List.of(substring(field(0, VARCHAR), 1, 2)),
                                List.of(VARCHAR))),
                        factoryStep(hashAggregationFactory(8_207, List.of(VARCHAR), List.of(0)))));
    }

    private List<Page> query21InventoryByWarehouseItemPages(TpcdsParquetTables tables, boolean beforeCutoff, int operatorIdBase)
    {
        LocalDate cutoffDate = LocalDate.of(2000, 3, 11);
        List<Type> inventoryTypes = tableColumnTypes(tables, "inventory", List.of("inv_item_sk", "inv_warehouse_sk", "inv_date_sk", "inv_quantity_on_hand"));
        Type quantityType = inventoryTypes.get(3);
        TestingAggregationFunction quantitySum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(quantityType));
        Type itemPriceType = tableColumnTypes(tables, "item", List.of("i_current_price")).getFirst();
        Type itemIdType = tableColumnTypes(tables, "item", List.of("i_item_id")).getFirst();
        Type warehouseNameType = tableColumnTypes(tables, "warehouse", List.of("w_warehouse_name")).getFirst();

        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_current_price", "i_item_id"),
                Optional.of(betweenInclusive(field(1, itemPriceType), 99L, 149L, itemPriceType)),
                List.of(field(0, BIGINT), field(2, itemIdType)),
                List.of(BIGINT, itemIdType));
        List<Page> warehousePages = relationPages(
                tables,
                "warehouse",
                List.of("w_warehouse_sk", "w_warehouse_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, warehouseNameType)),
                List.of(BIGINT, warehouseNameType));
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date"),
                Optional.of(and(
                        betweenInclusive(field(1, DATE), cutoffDate.minusDays(30).toEpochDay(), cutoffDate.plusDays(30).toEpochDay(), DATE),
                        beforeCutoff ? lessThan(field(1, DATE), constant(cutoffDate.toEpochDay(), DATE), DATE) : greaterThan(field(1, DATE), constant(cutoffDate.toEpochDay() - 1, DATE), DATE))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipelinePages(
                tables.tableFiles("inventory"),
                List.of("inv_item_sk", "inv_warehouse_sk", "inv_date_sk", "inv_quantity_on_hand"),
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, inventoryTypes, List.of(0), itemPages, List.of(BIGINT, itemIdType), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 1, concatTypes(inventoryTypes, List.of(BIGINT, itemIdType)), List.of(1), warehousePages, List.of(BIGINT, warehouseNameType), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 2, concatTypes(concatTypes(inventoryTypes, List.of(BIGINT, itemIdType)), List.of(BIGINT, warehouseNameType)), List.of(2), datePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 3,
                                Optional.empty(),
                                List.of(field(7, warehouseNameType), field(5, itemIdType), cast(field(3, quantityType), quantityType, BIGINT)),
                                List.of(warehouseNameType, itemIdType, BIGINT))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 4,
                                List.of(warehouseNameType, itemIdType),
                                List.of(0, 1),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    private MaterializedResult queryInventorySalesItems(TpcdsParquetTables tables, String salesTable, String salesItemColumn, LocalDate startDate, long minimumPriceInclusive, long maximumPriceInclusive, long... manufacturerIds)
    {
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_item_id", "i_item_desc", "i_current_price", "i_manufact_id"));
        Type priceType = itemTypes.get(3);
        Type manufacturerType = itemTypes.get(4);
        List<Type> inventoryTypes = tableColumnTypes(tables, "inventory", List.of("inv_item_sk", "inv_date_sk", "inv_quantity_on_hand"));
        Type dateType = tableColumnTypes(tables, "date_dim", List.of("d_date")).getFirst();
        List<Type> outputTypes = List.of(itemTypes.get(1), itemTypes.get(2), priceType);

        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_id", "i_item_desc", "i_current_price", "i_manufact_id"),
                Optional.of(and(
                        betweenInclusive(field(3, priceType), minimumPriceInclusive, maximumPriceInclusive, priceType),
                        anyOf(field(4, manufacturerType), manufacturerType, manufacturerIds))),
                List.of(field(0, BIGINT), field(1, itemTypes.get(1)), field(2, itemTypes.get(2)), field(3, priceType)),
                List.of(BIGINT, itemTypes.get(1), itemTypes.get(2), priceType));
        List<Page> inventoryPages = relationPages(
                tables,
                "inventory",
                List.of("inv_item_sk", "inv_date_sk", "inv_quantity_on_hand"),
                Optional.of(betweenInclusive(field(2, inventoryTypes.get(2)), 100, 500, inventoryTypes.get(2))),
                List.of(field(0, BIGINT), field(1, BIGINT)),
                List.of(BIGINT, BIGINT));
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date"),
                Optional.of(betweenInclusive(field(1, dateType), startDate.toEpochDay(), startDate.plusDays(60).toEpochDay(), dateType)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> salesItemPages = relationPages(
                tables,
                salesTable,
                List.of(salesItemColumn),
                Optional.empty(),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePagesPipeline(
                executePipelinePages(
                        itemPages,
                        List.of(
                                hashJoinStep(new HashJoinSpec(82_0, List.of(BIGINT, itemTypes.get(1), itemTypes.get(2), priceType), List.of(0), inventoryPages, List.of(BIGINT, BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(82_1, List.of(BIGINT, itemTypes.get(1), itemTypes.get(2), priceType, BIGINT, BIGINT), List.of(5), datePages, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(82_2, List.of(BIGINT, itemTypes.get(1), itemTypes.get(2), priceType, BIGINT, BIGINT, BIGINT), List.of(0), salesItemPages, List.of(BIGINT), List.of(0))),
                                factoryStep(filterAndProjectFactory(
                                        82_3,
                                        Optional.empty(),
                                        List.of(field(1, itemTypes.get(1)), field(2, itemTypes.get(2)), field(3, priceType)),
                                        outputTypes)),
                                factoryStep(hashAggregationFactory(
                                        82_4,
                                        outputTypes,
                                        List.of(0, 1, 2),
                                        COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                                factoryStep(filterAndProjectFactory(
                                        82_5,
                                        Optional.empty(),
                                        List.of(field(0, itemTypes.get(1)), field(1, itemTypes.get(2)), field(2, priceType)),
                                        outputTypes)))),
                List.of(factoryStep(topNFactory(82_6, outputTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    private List<Page> query01CustomerIdsPages(TpcdsParquetTables tables)
    {
        return executePipelinePages(
                query01FilteredRowsPages(tables),
                List.of(factoryStep(filterAndProjectFactory(
                        6,
                        Optional.empty(),
                        List.of(field(5, VARCHAR)),
                        List.of(VARCHAR)))));
    }

    private List<Page> query01FilteredRowsPages(TpcdsParquetTables tables)
    {
        List<Page> storeTotals = executeStoreTotalsPages(tables);
        List<Type> afterCustomerTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, VARCHAR);
        List<Type> afterStoreTotalsTypes = concatTypes(afterCustomerTypes, List.of(BIGINT, BIGINT, BIGINT));

        return executePipelinePages(
                query01CustomerJoinedRowsPages(tables),
                List.of(
                        hashJoinStep(new HashJoinSpec(5, afterCustomerTypes, List.of(1), storeTotals, List.of(BIGINT, BIGINT, BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                6,
                                Optional.of(query01ReturnThresholdPredicate()),
                                identityProjections(afterStoreTotalsTypes),
                                afterStoreTotalsTypes))));
    }

    private List<Page> query01CustomerJoinedRowsPages(TpcdsParquetTables tables)
    {
        List<Page> customerStoreReturns = query01CustomerStoreReturnsPages(tables);
        List<Page> tnStores = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_state"),
                Optional.of(equal(1, VARCHAR, "TN")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> customers = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_customer_id"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));

        List<Type> customerStoreReturnTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> afterStoreTypes = concatTypes(customerStoreReturnTypes, List.of(BIGINT));
        return executePipelinePages(
                customerStoreReturns,
                List.of(
                        hashJoinStep(new HashJoinSpec(3, customerStoreReturnTypes, List.of(1), tnStores, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(4, afterStoreTypes, List.of(0), customers, List.of(BIGINT, VARCHAR), List.of(0)))));
    }

    private List<Page> executeStoreTotalsPages(TpcdsParquetTables tables)
    {
        return executePipelinePages(
                query01CustomerStoreReturnsWithValueCountsPages(tables),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                1,
                                Optional.empty(),
                                List.of(
                                        field(1, BIGINT),
                                        field(2, BIGINT),
                                        ifExpression(
                                                greaterThan(field(3, BIGINT), constant(0L, BIGINT), BIGINT),
                                                constant(1L, BIGINT),
                                                constant(0L, BIGINT),
                                                BIGINT)),
                                List.of(BIGINT, BIGINT, BIGINT))),
                        factoryStep(hashAggregationFactory(
                                2,
                                List.of(BIGINT),
                                List.of(0),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    public MaterializedResult query41(TpcdsParquetTables tables)
    {
        List<Path> itemFiles = tables.tableFiles("item");
        List<Type> probeTypes = List.of(VARCHAR, INTEGER, VARCHAR);
        List<PipelineStep> steps = List.of(
                factoryStep(filterAndProjectFactory(
                        10,
                        Optional.of(and(
                                greaterThan(1, 737),
                                lessThan(1, 779))),
                        identityProjections(probeTypes),
                        probeTypes)),
                semiJoinStep(new SemiJoinSpec(
                        11,
                        probeTypes,
                        2,
                        Optional.empty(),
                        itemFiles,
                        List.of("i_manufact", "i_category", "i_color", "i_units", "i_size"),
                        List.of(factoryStep(filterAndProjectFactory(
                                11_1,
                                Optional.of(query41EligibilityPredicate()),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)))),
                        List.of(VARCHAR),
                        0)),
                factoryStep(filterAndProjectFactory(
                        12,
                        Optional.of(field(3, BOOLEAN)),
                        List.of(field(0, VARCHAR)),
                        List.of(VARCHAR))),
                factoryStep(hashAggregationFactory(13, List.of(VARCHAR), List.of(0))),
                factoryStep(topNFactory(14, List.of(VARCHAR), 100, List.of(0), List.of(ASC_NULLS_LAST))));
        return executePipeline(
                itemFiles,
                List.of("i_product_name", "i_manufact_id", "i_manufact"),
                steps,
                List.of(VARCHAR));
    }

    public MaterializedResult query44(TpcdsParquetTables tables)
    {
        List<Page> bestItemNames = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_product_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> worstItemNames = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_product_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Type> rankedTypes = List.of(BIGINT, BIGINT);
        List<Type> afterRankJoinTypes = concatTypes(rankedTypes, rankedTypes);
        List<Type> afterBestItemTypes = concatTypes(afterRankJoinTypes, List.of(BIGINT, VARCHAR));
        List<Type> outputTypes = List.of(BIGINT, VARCHAR, VARCHAR);

        return executePagesPipeline(
                query44RankedItemsPages(tables, false),
                List.of(
                        hashJoinStep(new HashJoinSpec(44_20, rankedTypes, List.of(1), query44RankedItemsPages(tables, true), rankedTypes, List.of(1))),
                        hashJoinStep(new HashJoinSpec(44_21, afterRankJoinTypes, List.of(0), bestItemNames, List.of(BIGINT, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(44_22, afterBestItemTypes, List.of(2), worstItemNames, List.of(BIGINT, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                44_23,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(5, VARCHAR), field(7, VARCHAR)),
                                outputTypes)),
                        factoryStep(topNFactory(44_24, outputTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query45(TpcdsParquetTables tables)
    {
        List<String> columns = List.of("ws_item_sk", "ws_sold_date_sk", "ws_bill_customer_sk", "ws_sales_price");
        List<Type> factTypes = tableColumnTypes(tables, "web_sales", columns);
        List<Type> afterCustomerTypes = concatTypes(factTypes, List.of(BIGINT, BIGINT));
        List<Type> afterAddressTypes = concatTypes(afterCustomerTypes, List.of(BIGINT, VARCHAR, VARCHAR));
        List<Type> afterDateTypes = concatTypes(afterAddressTypes, List.of(BIGINT));
        List<Type> afterItemTypes = concatTypes(afterDateTypes, List.of(BIGINT, VARCHAR));
        List<Type> afterSemiJoinTypes = concatTypes(afterItemTypes, List.of(BOOLEAN));
        List<Type> filteredTypes = List.of(factTypes.get(3), VARCHAR, VARCHAR);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(factTypes.get(3)));

        List<Page> customerRows = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT)),
                List.of(BIGINT, BIGINT));
        List<Page> addressRows = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_city", "ca_zip"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR)),
                List.of(BIGINT, VARCHAR, VARCHAR));
        List<Page> allowedDates = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_qoy", "d_year"),
                Optional.of(and(equal(1, 2, INTEGER), equal(2, 2001, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemRows = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_id"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));

        List<PipelineStep> steps = new ArrayList<>();
        steps.add(hashJoinStep(new HashJoinSpec(45_0, factTypes, List.of(2), customerRows, List.of(BIGINT, BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(45_1, afterCustomerTypes, List.of(5), addressRows, List.of(BIGINT, VARCHAR, VARCHAR), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(45_2, afterAddressTypes, List.of(1), allowedDates, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(45_3, afterDateTypes, List.of(0), itemRows, List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(semiJoinStep(new SemiJoinSpec(
                45_4,
                afterItemTypes,
                11,
                Optional.empty(),
                tables.tableFiles("item"),
                List.of("i_item_sk", "i_item_id"),
                List.of(factoryStep(filterAndProjectFactory(
                        45_5,
                        Optional.of(query45ItemPredicate()),
                        List.of(field(1, VARCHAR)),
                        List.of(VARCHAR)))),
                List.of(VARCHAR),
                0)));
        steps.add(factoryStep(filterAndProjectFactory(
                45_6,
                Optional.of(query45FilterPredicate()),
                List.of(field(3, factTypes.get(3)), field(7, VARCHAR), field(8, VARCHAR)),
                filteredTypes)));
        steps.add(factoryStep(hashAggregationFactory(45_7, List.of(VARCHAR, VARCHAR), List.of(2, 1), salesSum.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()))));
        steps.add(factoryStep(topNFactory(45_8, List.of(VARCHAR, VARCHAR, salesSum.getFinalType()), 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))));
        return executePipeline(tables.tableFiles("web_sales"), columns, steps, List.of(VARCHAR, VARCHAR, salesSum.getFinalType()));
    }

    public MaterializedResult query51(TpcdsParquetTables tables)
    {
        List<Type> branchTypes = List.of(BIGINT, DATE, BIGINT);
        List<Type> joinedTypes = concatTypes(branchTypes, branchTypes);
        List<Type> projectedTypes = List.of(BIGINT, DATE, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(BIGINT, DATE, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Page> storePages = query51ChannelPages(tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_sales_price");
        List<Page> webPages = query51ChannelPages(tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_sales_price");
        List<Page> joinedPages = new ArrayList<>(executePipelinePages(
                webPages,
                List.of(hashJoinStep(new HashJoinSpec(
                        51_20,
                        branchTypes,
                        List.of(0, 1),
                        storePages,
                        branchTypes,
                        List.of(0, 1),
                        JoinOperatorType.probeOuterJoin(false))))));
        joinedPages.addAll(executePipelinePages(
                storePages,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                51_21,
                                branchTypes,
                                List.of(0, 1),
                                webPages,
                                branchTypes,
                                List.of(0, 1),
                                JoinOperatorType.probeOuterJoin(false))),
                        factoryStep(filterAndProjectFactory(
                                51_22,
                                Optional.of(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(3, BIGINT)), List.of())),
                                List.of(
                                        field(3, BIGINT),
                                        field(4, DATE),
                                        field(5, BIGINT),
                                        field(0, BIGINT),
                                        field(1, DATE),
                                        field(2, BIGINT)),
                                joinedTypes)))));

        return executePagesPipeline(
                joinedPages,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                51_23,
                                Optional.empty(),
                                List.of(
                                        ifExpression(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(0, BIGINT)), List.of()), field(3, BIGINT), field(0, BIGINT), BIGINT),
                                        ifExpression(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(1, DATE)), List.of()), field(4, DATE), field(1, DATE), DATE),
                                        field(2, BIGINT),
                                        field(5, BIGINT)),
                                projectedTypes)),
                        factoryStep(windowFactory(
                                51_24,
                                projectedTypes,
                                List.of(0, 1, 2, 3),
                                List.of(0),
                                List.of(1),
                                List.of(ASC_NULLS_LAST),
                                List.of(
                                        aggregateWindowFunction("max", List.of(BIGINT), BIGINT, 2),
                                        aggregateWindowFunction("max", List.of(BIGINT), BIGINT, 3)))),
                        factoryStep(filterAndProjectFactory(
                                51_25,
                                Optional.of(greaterThan(field(4, BIGINT), field(5, BIGINT), BIGINT)),
                                identityProjections(outputTypes),
                                outputTypes)),
                        factoryStep(topNFactory(51_26, outputTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query53(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction quarterlyAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(salesSum.getFinalType()));
        List<Type> itemTypes = List.of(BIGINT, INTEGER);
        List<Type> dateTypes = List.of(BIGINT, INTEGER);
        List<Type> quarterlyTypes = List.of(INTEGER, INTEGER, salesSum.getFinalType());
        List<Type> windowTypes = List.of(INTEGER, INTEGER, salesSum.getFinalType(), quarterlyAverage.getFinalType());
        List<Type> outputTypes = List.of(INTEGER, salesSum.getFinalType(), quarterlyAverage.getFinalType());

        List<Page> itemKeys = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_manufact_id", "i_category", "i_class", "i_brand"),
                Optional.of(query53ItemPredicate()),
                List.of(field(0, BIGINT), field(1, INTEGER)),
                itemTypes);
        List<Page> allowedDates = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_qoy", "d_month_seq"),
                Optional.of(and(greaterThan(2, 1199, INTEGER), lessThan(2, 1212, INTEGER))),
                List.of(field(0, BIGINT), field(1, INTEGER)),
                dateTypes);
        List<Page> storeKeys = relationPages(
                tables,
                "store",
                List.of("s_store_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(53_0, factTypes, List.of(1), itemKeys, itemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(53_1, concatTypes(factTypes, itemTypes), List.of(0), allowedDates, dateTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(53_2, concatTypes(concatTypes(factTypes, itemTypes), dateTypes), List.of(2), storeKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(hashAggregationFactory(
                                53_3,
                                List.of(INTEGER, INTEGER),
                                List.of(5, 7),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(windowFactory(
                                53_4,
                                quarterlyTypes,
                                List.of(0, 1, 2),
                                List.of(0),
                                List.of(),
                                List.of(),
                                List.of(aggregateWindowFunction("avg", List.of(salesSum.getFinalType()), quarterlyAverage.getFinalType(), PARTITION_ROWS_FRAME, 2)))),
                        factoryStep(filterAndProjectFactory(
                                53_5,
                                Optional.of(query53DeviationPredicate(salesSum.getFinalType(), quarterlyAverage.getFinalType())),
                                List.of(field(0, INTEGER), field(2, salesSum.getFinalType()), field(3, quarterlyAverage.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(53_6, outputTypes, 100, List.of(2, 1, 0), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query54(TpcdsParquetTables tables)
    {
        List<Type> myCustomersTypes = List.of(BIGINT, BIGINT);
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", List.of("ss_customer_sk", "ss_sold_date_sk", "ss_ext_sales_price"));
        Type revenueType = salesTypes.get(2);
        TestingAggregationFunction revenueSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(revenueType));
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_county", "ca_state"));
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_county", "s_state"));
        List<Type> soldDateTypes = tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_month_seq"));
        List<Type> projectedRevenueTypes = List.of(BIGINT, revenueType, INTEGER);
        List<Type> lowerBoundTypes = concatTypes(projectedRevenueTypes, List.of(INTEGER));
        List<Type> boundedRevenueTypes = concatTypes(lowerBoundTypes, List.of(INTEGER));
        List<Type> customerRevenueTypes = List.of(BIGINT, revenueSum.getFinalType());
        List<Type> decimalSegmentTypes = List.of(revenueType);
        List<Type> rawSegmentTypes = List.of(BIGINT);
        List<Type> groupedSegmentTypes = List.of(BIGINT, BIGINT);
        List<Type> outputTypes = List.of(BIGINT, BIGINT, BIGINT);

        List<Page> myCustomersPages = query54MyCustomersPages(tables);
        List<Page> lowerMonthPages = query54ScalarMonthBoundaryPages(tables, 1);
        List<Page> upperMonthPages = query54ScalarMonthBoundaryPages(tables, 3);

        List<Page> baseRevenuePages = executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_customer_sk", "ss_sold_date_sk", "ss_ext_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(540, salesTypes, List.of(0), myCustomersPages, myCustomersTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(541, concatTypes(salesTypes, myCustomersTypes), List.of(4), relationPages(
                                tables,
                                "customer_address",
                                List.of("ca_address_sk", "ca_county", "ca_state"),
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(1, addressTypes.get(1)), field(2, addressTypes.get(2))),
                                addressTypes), addressTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(542, concatTypes(concatTypes(salesTypes, myCustomersTypes), addressTypes), List.of(6, 7), relationPages(
                                tables,
                                "store",
                                List.of("s_county", "s_state"),
                                Optional.empty(),
                                List.of(field(0, storeTypes.get(0)), field(1, storeTypes.get(1))),
                                storeTypes), storeTypes, List.of(0, 1))),
                        hashJoinStep(new HashJoinSpec(543, concatTypes(concatTypes(concatTypes(salesTypes, myCustomersTypes), addressTypes), storeTypes), List.of(1), relationPages(
                                tables,
                                "date_dim",
                                List.of("d_date_sk", "d_month_seq"),
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(1, INTEGER)),
                                soldDateTypes), soldDateTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                544,
                                Optional.empty(),
                                List.of(field(3, BIGINT), field(2, revenueType), field(11, INTEGER)),
                                projectedRevenueTypes))));

        List<Page> withLowerBoundPages = executeNestedLoopPages(baseRevenuePages, projectedRevenueTypes, lowerMonthPages, List.of(INTEGER));
        List<Page> withBoundsPages = executeNestedLoopPages(withLowerBoundPages, lowerBoundTypes, upperMonthPages, List.of(INTEGER));
        List<Page> customerRevenuePages = executePipelinePages(
                withBoundsPages,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                545,
                                Optional.of(query54MonthBetweenPredicate()),
                                List.of(field(0, BIGINT), field(1, revenueType)),
                                List.of(BIGINT, revenueType))),
                        factoryStep(hashAggregationFactory(
                                546,
                                List.of(BIGINT),
                                List.of(0),
                                revenueSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))));

        return executePagesPipeline(
                customerRevenuePages,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                547,
                                Optional.empty(),
                                List.of(query54SegmentExpression(revenueSum.getFinalType())),
                                decimalSegmentTypes)),
                        factoryStep(filterAndProjectFactory(
                                547_1,
                                Optional.empty(),
                                List.of(cast(field(0, revenueSum.getFinalType()), revenueSum.getFinalType(), BIGINT)),
                                rawSegmentTypes)),
                        factoryStep(filterAndProjectFactory(
                                547_2,
                                Optional.empty(),
                                List.of(divide(field(0, BIGINT), constant(10_000L, BIGINT), BIGINT)),
                                List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(
                                548,
                                List.of(BIGINT),
                                List.of(0),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        factoryStep(topNFactory(549, groupedSegmentTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                550,
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(1, BIGINT), multiply(field(0, BIGINT), constant(50L, BIGINT), BIGINT)),
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query58(TpcdsParquetTables tables)
    {
        List<Page> storePages = query58ChannelPages(tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price", 58_00);
        List<Page> catalogPages = query58ChannelPages(tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price", 58_10);
        List<Page> webPages = query58ChannelPages(tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price", 58_20);

        Type itemIdType = tableColumnTypes(tables, "item", List.of("i_item_id")).getFirst();
        Type revenueType = FUNCTION_RESOLUTION.getAggregateFunction(
                "sum",
                fromTypes(tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst()))
                .getFinalType();
        List<Type> channelTypes = List.of(itemIdType, revenueType);
        List<Type> joinedTypes = List.of(itemIdType, revenueType, revenueType, revenueType);
        List<RowExpression> outputProjections = query58OutputProjections(itemIdType, revenueType);
        List<Type> outputTypes = outputProjections.stream()
                .map(RowExpression::type)
                .toList();

        return executePagesPipeline(
                storePages,
                List.of(
                        hashJoinStep(new HashJoinSpec(58_30, channelTypes, List.of(0), catalogPages, channelTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(58_31, concatTypes(channelTypes, channelTypes), List.of(0), webPages, channelTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                58_32,
                                Optional.of(query58SimilarityPredicate(1, 3, 5, revenueType)),
                                List.of(field(0, itemIdType), field(1, revenueType), field(3, revenueType), field(5, revenueType)),
                                joinedTypes)),
                        factoryStep(topNFactory(58_33, joinedTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                58_34,
                                Optional.empty(),
                                outputProjections,
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query59(TpcdsParquetTables tables)
    {
        Type weekSequenceType = tableColumnTypes(tables, "date_dim", List.of("d_week_seq")).getFirst();
        Type storeKeyType = BIGINT;
        Type storeNameType = tableColumnTypes(tables, "store", List.of("s_store_name")).getFirst();
        Type storeIdType = tableColumnTypes(tables, "store", List.of("s_store_id")).getFirst();
        List<Type> weeklyTypes = List.of(weekSequenceType, storeKeyType, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> adjustedTypes = List.of(weekSequenceType, weekSequenceType, storeKeyType, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(storeNameType, storeIdType, weekSequenceType, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);

        List<Page> currentYearPages = executePipelinePages(
                query59WeeklyStoreSalesPages(tables, 1212, 1223, 59_100),
                List.of(factoryStep(filterAndProjectFactory(
                        59_110,
                        Optional.empty(),
                        List.of(
                                add(field(0, weekSequenceType), constant(52L, weekSequenceType), weekSequenceType),
                                field(0, weekSequenceType),
                                field(1, storeKeyType),
                                field(2, BIGINT),
                                field(3, BIGINT),
                                field(4, BIGINT),
                                field(5, BIGINT),
                                field(6, BIGINT),
                                field(7, BIGINT),
                                field(8, BIGINT)),
                        adjustedTypes))));
        List<Page> nextYearPages = query59WeeklyStoreSalesPages(tables, 1224, 1235, 59_200);
        RowExpression hundred = constant(100L, BIGINT);

        return executePagesPipeline(
                currentYearPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(59_300, adjustedTypes, List.of(0, 2), nextYearPages, weeklyTypes, List.of(0, 1))),
                        hashJoinStep(new HashJoinSpec(
                                59_301,
                                concatTypes(adjustedTypes, weeklyTypes),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "store",
                                        List.of("s_store_sk", "s_store_name", "s_store_id"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, storeNameType), field(2, storeIdType)),
                                        List.of(storeKeyType, storeNameType, storeIdType)),
                                List.of(storeKeyType, storeNameType, storeIdType),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                59_302,
                                Optional.empty(),
                                List.of(
                                        field(20, storeNameType),
                                        field(21, storeIdType),
                                        field(1, weekSequenceType),
                                        query02RatioExpression(3, 12, hundred),
                                        query02RatioExpression(4, 13, hundred),
                                        query02RatioExpression(5, 14, hundred),
                                        query02RatioExpression(6, 15, hundred),
                                        query02RatioExpression(7, 16, hundred),
                                        query02RatioExpression(8, 17, hundred),
                                        query02RatioExpression(9, 18, hundred)),
                                outputTypes)),
                        factoryStep(topNFactory(59_303, outputTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query61(TpcdsParquetTables tables)
    {
        List<Type> salesPriceTypes = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price"));
        Type salesPriceType = salesPriceTypes.getFirst();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesPriceType));
        Type revenueType = salesSum.getFinalType();
        Type percentType = createDecimalType(38, 12);
        List<Type> scalarTypes = List.of(revenueType);
        List<Type> joinedTypes = List.of(revenueType, revenueType);
        List<Type> outputTypes = List.of(revenueType, revenueType, percentType);

        List<Page> promotionalSales = query61SalesPages(tables, true, 61_00);
        List<Page> totalSales = query61SalesPages(tables, false, 61_10);

        return executePagesPipeline(
                executeNestedLoopPages(promotionalSales, scalarTypes, totalSales, scalarTypes),
                List.of(
                        factoryStep(topNFactory(61_20, joinedTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                61_21,
                                Optional.empty(),
                                List.of(field(0, revenueType), field(1, revenueType), query61PercentExpression(revenueType, percentType)),
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query57(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", List.of("cs_sold_date_sk", "cs_call_center_sk", "cs_item_sk", "cs_sales_price"));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction monthlyAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(salesSum.getFinalType()));
        List<Type> currentTypes = List.of(VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, monthlyAverage.getFinalType(), salesSum.getFinalType(), BIGINT);
        List<Type> adjacentTypes = List.of(VARCHAR, VARCHAR, VARCHAR, salesSum.getFinalType(), BIGINT);
        List<Type> afterPreviousTypes = concatTypes(currentTypes, adjacentTypes);
        List<Type> afterNextTypes = concatTypes(afterPreviousTypes, adjacentTypes);
        List<Type> sortedTypes = concatTypes(List.of(VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, monthlyAverage.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType()), List.of(DOUBLE));
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, monthlyAverage.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType(), salesSum.getFinalType());

        List<Page> monthlyRankedPages = query57MonthlyRankedSalesPages(tables);
        List<Page> currentPages = executePipelinePages(
                monthlyRankedPages,
                List.of(factoryStep(filterAndProjectFactory(
                        57_10,
                        Optional.of(equal(3, 1999, INTEGER)),
                        identityProjections(currentTypes),
                        currentTypes))));
        List<Page> previousPages = executePipelinePages(
                monthlyRankedPages,
                List.of(factoryStep(filterAndProjectFactory(
                        57_11,
                        Optional.empty(),
                        List.of(
                                field(0, VARCHAR),
                                field(1, VARCHAR),
                                field(2, VARCHAR),
                                field(6, salesSum.getFinalType()),
                                add(field(7, BIGINT), constant(1L, BIGINT), BIGINT)),
                        adjacentTypes))));
        List<Page> nextPages = executePipelinePages(
                monthlyRankedPages,
                List.of(factoryStep(filterAndProjectFactory(
                        57_12,
                        Optional.empty(),
                        List.of(
                                field(0, VARCHAR),
                                field(1, VARCHAR),
                                field(2, VARCHAR),
                                field(6, salesSum.getFinalType()),
                                subtract(field(7, BIGINT), constant(1L, BIGINT), BIGINT)),
                        adjacentTypes))));

        return executePagesPipeline(
                currentPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(57_20, currentTypes, List.of(0, 1, 2, 7), previousPages, adjacentTypes, List.of(0, 1, 2, 4))),
                        hashJoinStep(new HashJoinSpec(57_21, afterPreviousTypes, List.of(0, 1, 2, 7), nextPages, adjacentTypes, List.of(0, 1, 2, 4))),
                        factoryStep(filterAndProjectFactory(
                                57_22,
                                Optional.of(queryRelativeDeviationPredicate(6, 5, salesSum.getFinalType(), monthlyAverage.getFinalType())),
                                List.of(
                                        field(0, VARCHAR),
                                        field(1, VARCHAR),
                                        field(2, VARCHAR),
                                        field(3, INTEGER),
                                        field(4, INTEGER),
                                        field(5, monthlyAverage.getFinalType()),
                                        field(6, salesSum.getFinalType()),
                                        field(11, salesSum.getFinalType()),
                                        field(16, salesSum.getFinalType()),
                                        subtract(cast(field(6, salesSum.getFinalType()), salesSum.getFinalType(), DOUBLE), cast(field(5, monthlyAverage.getFinalType()), monthlyAverage.getFinalType(), DOUBLE), DOUBLE)),
                                sortedTypes)),
                        factoryStep(topNFactory(57_23, sortedTypes, 100, List.of(9, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                57_24,
                                Optional.empty(),
                                List.of(
                                        field(0, VARCHAR),
                                        field(1, VARCHAR),
                                        field(2, VARCHAR),
                                        field(3, INTEGER),
                                        field(4, INTEGER),
                                        field(5, monthlyAverage.getFinalType()),
                                        field(6, salesSum.getFinalType()),
                                        field(7, salesSum.getFinalType()),
                                        field(8, salesSum.getFinalType())),
                                outputTypes))),
                outputTypes);
    }

    private List<Page> query51ChannelPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String salesPriceColumn)
    {
        List<String> factColumns = List.of(soldDateColumn, itemColumn, salesPriceColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        List<Type> groupedTypes = List.of(BIGINT, DATE, BIGINT);

        List<Page> allowedDates = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date", "d_month_seq"),
                Optional.of(and(greaterThan(2, 1199, INTEGER), lessThan(2, 1212, INTEGER))),
                List.of(field(0, BIGINT), field(1, DATE)),
                List.of(BIGINT, DATE));

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                51_0,
                                Optional.of(equal(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(1, BIGINT)), List.of()), constant(false, BOOLEAN), BOOLEAN)),
                                identityProjections(factTypes),
                                factTypes)),
                        hashJoinStep(new HashJoinSpec(51_1, factTypes, List.of(0), allowedDates, List.of(BIGINT, DATE), List.of(0))),
                        factoryStep(hashAggregationFactory(
                                51_2,
                                List.of(BIGINT, DATE),
                                List.of(1, 4),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        factoryStep(windowFactory(
                                51_3,
                                groupedTypes,
                                List.of(0, 1, 2),
                                List.of(0),
                                List.of(1),
                                List.of(ASC_NULLS_LAST),
                                List.of(aggregateWindowFunction("sum", List.of(BIGINT), BIGINT, 2)))),
                        factoryStep(filterAndProjectFactory(
                                51_4,
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(1, DATE), field(3, BIGINT)),
                                List.of(BIGINT, DATE, BIGINT)))));
    }

    private List<Page> query57MonthlyRankedSalesPages(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", List.of("cs_sold_date_sk", "cs_call_center_sk", "cs_item_sk", "cs_sales_price"));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction monthlyAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(salesSum.getFinalType()));
        List<Type> itemTypes = List.of(BIGINT, VARCHAR, VARCHAR);
        List<Type> dateTypes = List.of(BIGINT, INTEGER, INTEGER);
        List<Type> callCenterTypes = List.of(BIGINT, VARCHAR);
        List<Type> projectedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, salesType);
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, salesSum.getFinalType());
        List<Type> rankedTypes = concatTypes(groupedTypes, List.of(BIGINT));
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, monthlyAverage.getFinalType(), salesSum.getFinalType(), BIGINT);

        List<Page> itemKeys = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_brand", "i_category"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR)),
                itemTypes);
        List<Page> allowedDates = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year", "d_moy"),
                Optional.of(query57DatePredicate()),
                List.of(field(0, BIGINT), field(1, INTEGER), field(2, INTEGER)),
                dateTypes);
        List<Page> callCenters = relationPages(
                tables,
                "call_center",
                List.of("cc_call_center_sk", "cc_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                callCenterTypes);

        return executePipelinePages(
                tables.tableFiles("catalog_sales"),
                List.of("cs_sold_date_sk", "cs_call_center_sk", "cs_item_sk", "cs_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(57_0, factTypes, List.of(2), itemKeys, itemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(57_1, concatTypes(factTypes, itemTypes), List.of(0), allowedDates, dateTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(57_2, concatTypes(concatTypes(factTypes, itemTypes), dateTypes), List.of(1), callCenters, callCenterTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                57_3,
                                Optional.empty(),
                                List.of(
                                        field(6, VARCHAR),
                                        field(5, VARCHAR),
                                        field(11, VARCHAR),
                                        field(8, INTEGER),
                                        field(9, INTEGER),
                                        field(3, salesType)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                57_4,
                                List.of(VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER),
                                List.of(0, 1, 2, 3, 4),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        factoryStep(topNRankingFactory(
                                57_5,
                                groupedTypes,
                                List.of(0, 1, 2, 3, 4, 5),
                                List.of(0, 1, 2),
                                List.of(3, 4),
                                List.of(ASC_NULLS_LAST, ASC_NULLS_LAST),
                                32)),
                        factoryStep(windowFactory(
                                57_6,
                                rankedTypes,
                                List.of(0, 1, 2, 3, 4, 5, 6),
                                List.of(0, 1, 2, 3),
                                List.of(),
                                List.of(),
                                List.of(aggregateWindowFunction("avg", List.of(salesSum.getFinalType()), monthlyAverage.getFinalType(), PARTITION_ROWS_FRAME, 5)))),
                        factoryStep(filterAndProjectFactory(
                                57_7,
                                Optional.empty(),
                                List.of(
                                        field(0, VARCHAR),
                                        field(1, VARCHAR),
                                        field(2, VARCHAR),
                                        field(3, INTEGER),
                                        field(4, INTEGER),
                                        field(7, monthlyAverage.getFinalType()),
                                        field(5, salesSum.getFinalType()),
                                        field(6, BIGINT)),
                                outputTypes))));
    }

    private List<Page> query54MyCustomersPages(TpcdsParquetTables tables)
    {
        List<Type> salesTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> customerTypes = List.of(BIGINT, BIGINT);
        List<Page> catalogSalesPages = relationPages(
                tables,
                "catalog_sales",
                List.of("cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT), field(2, BIGINT)),
                salesTypes);
        List<Page> webSalesPages = relationPages(
                tables,
                "web_sales",
                List.of("ws_sold_date_sk", "ws_bill_customer_sk", "ws_item_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT), field(2, BIGINT)),
                salesTypes);
        List<Page> customerSalesPages = new ArrayList<>(catalogSalesPages);
        customerSalesPages.addAll(webSalesPages);

        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_category", "i_class"));
        List<Page> itemKeys = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_category", "i_class"),
                Optional.of(and(
                        equal(1, itemTypes.get(1), "Women"),
                        equal(2, itemTypes.get(2), "maternity"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> dateKeys = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(equal(1, 12, INTEGER), equal(2, 1998, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> customers = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT)),
                customerTypes);

        return executePipelinePages(
                customerSalesPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(551, salesTypes, List.of(2), itemKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(552, concatTypes(salesTypes, List.of(BIGINT)), List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(553, concatTypes(concatTypes(salesTypes, List.of(BIGINT)), List.of(BIGINT)), List.of(1), customers, customerTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                554,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(6, BIGINT)),
                                customerTypes)),
                        factoryStep(hashAggregationFactory(555, customerTypes, List.of(0, 1)))));
    }

    private List<Page> query54ScalarMonthBoundaryPages(TpcdsParquetTables tables, int offset)
    {
        return executePipelinePages(
                relationPages(
                        tables,
                        "date_dim",
                        List.of("d_month_seq", "d_year", "d_moy"),
                        Optional.of(and(equal(1, 1998, INTEGER), equal(2, 12, INTEGER))),
                        List.of(add(field(0, INTEGER), constant((long) offset, INTEGER), INTEGER)),
                        List.of(INTEGER)),
                List.of(
                        factoryStep(hashAggregationFactory(556 + offset, List.of(INTEGER), List.of(0))),
                        factoryStep(enforceSingleRowFactory(558 + offset, List.of(INTEGER)))));
    }

    private List<Page> query58ChannelPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String salesPriceColumn, int operatorIdBase)
    {
        List<Type> factTypes = tableColumnTypes(tables, salesTable, List.of(soldDateColumn, itemColumn, salesPriceColumn));
        Type salesPriceType = factTypes.get(2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesPriceType));
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_item_id"));
        List<Type> dateTypes = tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_date"));
        List<Type> itemKeyTypes = List.of(BIGINT, itemTypes.get(1));
        List<Type> soldDateTypes = List.of(BIGINT, dateTypes.get(1));
        List<Type> allowedDateTypes = List.of(dateTypes.get(1));

        return executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(soldDateColumn, itemColumn, salesPriceColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_item_id"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, itemTypes.get(1))),
                                        itemKeyTypes),
                                itemKeyTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, itemKeyTypes),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_date"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, dateTypes.get(1))),
                                        soldDateTypes),
                                soldDateTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 2,
                                concatTypes(concatTypes(factTypes, itemKeyTypes), soldDateTypes),
                                List.of(6),
                                query58AllowedDatesPages(tables),
                                allowedDateTypes,
                                List.of(0))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(itemTypes.get(1)),
                                List.of(4),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    private List<Page> query59WeeklyStoreSalesPages(TpcdsParquetTables tables, int minimumMonthSequenceInclusive, int maximumMonthSequenceInclusive, int operatorIdBase)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_sales_price")).getFirst();
        List<Type> projectedTypes = List.of(INTEGER, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_store_sk", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                List.of(BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_week_seq", "d_day_name", "d_month_seq"),
                                        Optional.of(and(
                                                greaterThan(field(3, INTEGER), constant((long) (minimumMonthSequenceInclusive - 1), INTEGER), INTEGER),
                                                lessThan(field(3, INTEGER), constant((long) (maximumMonthSequenceInclusive + 1), INTEGER), INTEGER))),
                                        List.of(field(0, BIGINT), field(1, INTEGER), field(2, VARCHAR)),
                                        List.of(BIGINT, INTEGER, VARCHAR)),
                                List.of(BIGINT, INTEGER, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.empty(),
                                List.of(
                                        field(4, INTEGER),
                                        field(1, BIGINT),
                                        ifExpression(equal(field(5, VARCHAR), constant(Slices.utf8Slice("Sunday"), VARCHAR), VARCHAR), cast(multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT), constant(0L, BIGINT), BIGINT),
                                        ifExpression(equal(field(5, VARCHAR), constant(Slices.utf8Slice("Monday"), VARCHAR), VARCHAR), cast(multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT), constant(0L, BIGINT), BIGINT),
                                        ifExpression(equal(field(5, VARCHAR), constant(Slices.utf8Slice("Tuesday"), VARCHAR), VARCHAR), cast(multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT), constant(0L, BIGINT), BIGINT),
                                        ifExpression(equal(field(5, VARCHAR), constant(Slices.utf8Slice("Wednesday"), VARCHAR), VARCHAR), cast(multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT), constant(0L, BIGINT), BIGINT),
                                        ifExpression(equal(field(5, VARCHAR), constant(Slices.utf8Slice("Thursday"), VARCHAR), VARCHAR), cast(multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT), constant(0L, BIGINT), BIGINT),
                                        ifExpression(equal(field(5, VARCHAR), constant(Slices.utf8Slice("Friday"), VARCHAR), VARCHAR), cast(multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT), constant(0L, BIGINT), BIGINT),
                                        ifExpression(equal(field(5, VARCHAR), constant(Slices.utf8Slice("Saturday"), VARCHAR), VARCHAR), cast(multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT), constant(0L, BIGINT), BIGINT)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 2,
                                List.of(INTEGER, BIGINT),
                                List.of(0, 1),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(8), OptionalInt.empty())))));
    }

    private List<Page> query83ChannelPages(TpcdsParquetTables tables, String returnsTable, String itemColumn, String returnedDateColumn, String quantityColumn, int operatorIdBase)
    {
        List<Type> factTypes = tableColumnTypes(tables, returnsTable, List.of(itemColumn, returnedDateColumn, quantityColumn));
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_item_id"));
        List<Type> itemKeyTypes = List.of(BIGINT, itemTypes.get(1));
        List<Type> projectedTypes = List.of(itemTypes.get(1), BIGINT);

        return executePipelinePages(
                tables.tableFiles(returnsTable),
                List.of(itemColumn, returnedDateColumn, quantityColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_item_id"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, itemTypes.get(1))),
                                        itemKeyTypes),
                                itemKeyTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, itemKeyTypes),
                                List.of(1),
                                query83AllowedDatesPages(tables),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(field(4, itemTypes.get(1)), cast(field(2, factTypes.get(2)), factTypes.get(2), BIGINT)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(itemTypes.get(1)),
                                List.of(0),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))));
    }

    private List<Page> query49ChannelPages(TpcdsParquetTables tables, String channel, String salesTable, String soldDateColumn, String itemColumn, String orderColumn, String quantityColumn, String netPaidColumn, String netProfitColumn, String returnsTable, String returnItemColumn, String returnOrderColumn, String returnQuantityColumn, String returnAmountColumn, int operatorIdBase, Type ratioType)
    {
        List<String> salesColumns = List.of(soldDateColumn, itemColumn, orderColumn, quantityColumn, netPaidColumn, netProfitColumn);
        List<Type> salesTypes = tableColumnTypes(tables, salesTable, salesColumns);
        Type quantityType = salesTypes.get(3);
        Type netPaidType = salesTypes.get(4);
        Type returnAmountType = tableColumnTypes(tables, returnsTable, List.of(returnAmountColumn)).getFirst();
        Type returnQuantityType = tableColumnTypes(tables, returnsTable, List.of(returnQuantityColumn)).getFirst();

        TestingAggregationFunction returnQuantitySum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
        TestingAggregationFunction quantitySum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
        TestingAggregationFunction returnAmountSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType));
        TestingAggregationFunction netPaidSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(netPaidType));

        List<Type> projectedTypes = List.of(BIGINT, BIGINT, BIGINT, returnAmountType, netPaidType);
        List<Type> groupedTypes = List.of(BIGINT, returnQuantitySum.getFinalType(), quantitySum.getFinalType(), returnAmountSum.getFinalType(), netPaidSum.getFinalType());
        List<Type> ratioTypes = List.of(BIGINT, ratioType, ratioType);
        List<Type> firstRankTypes = List.of(BIGINT, ratioType, ratioType, BIGINT);
        List<Type> rankedTypes = List.of(BIGINT, ratioType, ratioType, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, BIGINT, ratioType, BIGINT, BIGINT);

        List<Page> returnPages = relationPages(
                tables,
                returnsTable,
                List.of(returnItemColumn, returnOrderColumn, returnQuantityColumn, returnAmountColumn),
                Optional.of(greaterThan(3, 1_000_000L, returnAmountType)),
                List.of(field(0, BIGINT), field(1, BIGINT), field(2, returnQuantityType), field(3, returnAmountType)),
                List.of(BIGINT, BIGINT, returnQuantityType, returnAmountType));
        List<Page> decemberDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year", "d_moy"),
                Optional.of(and(equal(1, 2001, INTEGER), equal(2, 12, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipelinePages(
                tables.tableFiles(salesTable),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(2, 1), returnPages, List.of(BIGINT, BIGINT, returnQuantityType, returnAmountType), List.of(1, 0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 1, concatTypes(salesTypes, List.of(BIGINT, BIGINT, returnQuantityType, returnAmountType)), List.of(0), decemberDatePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.of(and(
                                        greaterThan(field(3, quantityType), constant(0L, quantityType), quantityType),
                                        and(
                                                greaterThan(field(4, netPaidType), constant(0L, netPaidType), netPaidType),
                                                greaterThan(field(5, netPaidType), constant(100L, netPaidType), netPaidType)))),
                                List.of(
                                        field(1, BIGINT),
                                        coalesce(cast(field(8, returnQuantityType), returnQuantityType, BIGINT), constant(0L, BIGINT), BIGINT),
                                        cast(field(3, quantityType), quantityType, BIGINT),
                                        coalesce(field(9, returnAmountType), constant(0L, returnAmountType), returnAmountType),
                                        field(4, netPaidType)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(BIGINT),
                                List.of(0),
                                returnQuantitySum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                quantitySum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                returnAmountSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                netPaidSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 4,
                                Optional.empty(),
                                List.of(
                                        field(0, BIGINT),
                                        cast(
                                                divide(
                                                        cast(field(1, BIGINT), BIGINT, createDecimalType(15, 4)),
                                                        cast(field(2, BIGINT), BIGINT, createDecimalType(15, 4))),
                                                divide(
                                                        cast(field(1, BIGINT), BIGINT, createDecimalType(15, 4)),
                                                        cast(field(2, BIGINT), BIGINT, createDecimalType(15, 4))).type(),
                                                ratioType),
                                        cast(
                                                divide(
                                                        cast(field(3, returnAmountSum.getFinalType()), returnAmountSum.getFinalType(), createDecimalType(15, 4)),
                                                        cast(field(4, netPaidSum.getFinalType()), netPaidSum.getFinalType(), createDecimalType(15, 4))),
                                                divide(
                                                        cast(field(3, returnAmountSum.getFinalType()), returnAmountSum.getFinalType(), createDecimalType(15, 4)),
                                                        cast(field(4, netPaidSum.getFinalType()), netPaidSum.getFinalType(), createDecimalType(15, 4))).type(),
                                                ratioType)),
                                ratioTypes)),
                        factoryStep(windowFactory(
                                operatorIdBase + 5,
                                ratioTypes,
                                List.of(0, 1, 2),
                                List.of(),
                                List.of(1),
                                List.of(ASC_NULLS_LAST),
                                List.of(rankWindowFunction()))),
                        factoryStep(windowFactory(
                                operatorIdBase + 6,
                                firstRankTypes,
                                List.of(0, 1, 2, 3),
                                List.of(),
                                List.of(2),
                                List.of(ASC_NULLS_LAST),
                                List.of(rankWindowFunction()))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 7,
                                Optional.of(or(
                                        lessThan(field(3, BIGINT), constant(11L, BIGINT), BIGINT),
                                        lessThan(field(4, BIGINT), constant(11L, BIGINT), BIGINT))),
                                List.of(
                                        constant(Slices.utf8Slice(channel), VARCHAR),
                                        field(0, BIGINT),
                                        field(1, ratioType),
                                        field(3, BIGINT),
                                        field(4, BIGINT)),
                                outputTypes))));
    }

    private List<Page> query75ChannelPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String orderColumn, String quantityColumn, String salesAmountColumn, String returnsTable, String returnItemColumn, String returnOrderColumn, String returnQuantityColumn, String returnAmountColumn, int operatorIdBase)
    {
        List<String> salesColumns = List.of(soldDateColumn, itemColumn, orderColumn, quantityColumn, salesAmountColumn);
        List<Type> salesTypes = tableColumnTypes(tables, salesTable, salesColumns);
        Type quantityType = salesTypes.get(3);
        Type salesAmountType = salesTypes.get(4);
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_category", "i_brand_id", "i_class_id", "i_category_id", "i_manufact_id"));
        Type returnQuantityType = tableColumnTypes(tables, returnsTable, List.of(returnQuantityColumn)).getFirst();
        Type returnAmountType = tableColumnTypes(tables, returnsTable, List.of(returnAmountColumn)).getFirst();
        List<Type> projectedTypes = List.of(INTEGER, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5), BIGINT, BIGINT);

        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_category", "i_brand_id", "i_class_id", "i_category_id", "i_manufact_id"),
                Optional.of(equal(1, itemTypes.get(1), "Books")),
                List.of(field(0, BIGINT), field(2, itemTypes.get(2)), field(3, itemTypes.get(3)), field(4, itemTypes.get(4)), field(5, itemTypes.get(5))),
                List.of(BIGINT, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5)));
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(or(equal(1, 2001, INTEGER), equal(1, 2002, INTEGER))),
                List.of(field(0, BIGINT), field(1, INTEGER)),
                List.of(BIGINT, INTEGER));
        List<Page> returnsPages = relationPages(
                tables,
                returnsTable,
                List.of(returnItemColumn, returnOrderColumn, returnQuantityColumn, returnAmountColumn),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT), field(2, returnQuantityType), field(3, returnAmountType)),
                List.of(BIGINT, BIGINT, returnQuantityType, returnAmountType));

        RowExpression salesAmount = cast(multiply(field(4, salesAmountType), constant(100L, createDecimalType(3, 0))), multiply(field(4, salesAmountType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        RowExpression returnedAmount = coalesce(
                cast(multiply(field(15, returnAmountType), constant(100L, createDecimalType(3, 0))), multiply(field(15, returnAmountType), constant(100L, createDecimalType(3, 0))).type(), BIGINT),
                constant(0L, BIGINT),
                BIGINT);

        return executePipelinePages(
                tables.tableFiles(salesTable),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(1), itemPages, List.of(BIGINT, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5)), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 1, concatTypes(salesTypes, List.of(BIGINT, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5))), List.of(0), datePages, List.of(BIGINT, INTEGER), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 2, concatTypes(concatTypes(salesTypes, List.of(BIGINT, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5))), List.of(BIGINT, INTEGER)), List.of(2, 1), returnsPages, List.of(BIGINT, BIGINT, returnQuantityType, returnAmountType), List.of(1, 0), JoinOperatorType.probeOuterJoin(false))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 3,
                                Optional.empty(),
                                List.of(
                                        field(11, INTEGER),
                                        field(6, itemTypes.get(2)),
                                        field(7, itemTypes.get(3)),
                                        field(8, itemTypes.get(4)),
                                        field(9, itemTypes.get(5)),
                                        subtract(cast(field(3, quantityType), quantityType, BIGINT), coalesce(cast(field(14, returnQuantityType), returnQuantityType, BIGINT), constant(0L, BIGINT), BIGINT), BIGINT),
                                        subtract(salesAmount, returnedAmount, BIGINT)),
                                projectedTypes))));
    }

    private List<Page> query75AllSalesPages(TpcdsParquetTables tables, List<Type> itemTypes, int operatorIdBase, int year)
    {
        List<Page> unionPages = new ArrayList<>(query75ChannelPages(tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_order_number", "cs_quantity", "cs_ext_sales_price", "catalog_returns", "cr_item_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount", operatorIdBase));
        unionPages.addAll(query75ChannelPages(tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number", "ss_quantity", "ss_ext_sales_price", "store_returns", "sr_item_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt", operatorIdBase + 100));
        unionPages.addAll(query75ChannelPages(tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_order_number", "ws_quantity", "ws_ext_sales_price", "web_returns", "wr_item_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt", operatorIdBase + 200));
        List<Type> allSalesTypes = List.of(INTEGER, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5), BIGINT, BIGINT);

        return executePipelinePages(
                unionPages,
                List.of(
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 300,
                                List.of(INTEGER, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5)),
                                List.of(0, 1, 2, 3, 4),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 301,
                                Optional.of(equal(field(0, INTEGER), constant((long) year, INTEGER), INTEGER)),
                                identityProjections(allSalesTypes),
                                allSalesTypes))));
    }

    private List<Page> query78ChannelPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String customerColumn, String orderColumn, String quantityColumn, String wholesaleCostColumn, String salesPriceColumn, String returnsTable, String returnItemColumn, String returnOrderColumn, int operatorIdBase)
    {
        List<String> salesColumns = List.of(soldDateColumn, itemColumn, customerColumn, orderColumn, quantityColumn, wholesaleCostColumn, salesPriceColumn);
        List<Type> salesTypes = tableColumnTypes(tables, salesTable, salesColumns);
        Type quantityType = salesTypes.get(4);
        Type wholesaleCostType = salesTypes.get(5);
        Type salesPriceType = salesTypes.get(6);
        List<Type> returnsTypes = List.of(BIGINT, BIGINT);
        List<Type> dateTypes = List.of(BIGINT, INTEGER);
        List<Type> projectedTypes = List.of(INTEGER, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);

        List<Page> returnsPages = relationPages(
                tables,
                returnsTable,
                List.of(returnItemColumn, returnOrderColumn),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT)),
                returnsTypes);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(equal(1, 1998, INTEGER)),
                List.of(field(0, BIGINT), field(1, INTEGER)),
                dateTypes);

        return executePipelinePages(
                tables.tableFiles(salesTable),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(3, 1), returnsPages, returnsTypes, List.of(1, 0), JoinOperatorType.probeOuterJoin(false))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 1, concatTypes(salesTypes, returnsTypes), List.of(0), datePages, dateTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.of(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(8, BIGINT)), List.of())),
                                List.of(
                                        field(10, INTEGER),
                                        field(1, BIGINT),
                                        field(2, BIGINT),
                                        cast(field(4, quantityType), quantityType, BIGINT),
                                        cast(multiply(field(5, wholesaleCostType), constant(100L, createDecimalType(3, 0))), multiply(field(5, wholesaleCostType), constant(100L, createDecimalType(3, 0))).type(), BIGINT),
                                        cast(multiply(field(6, salesPriceType), constant(100L, createDecimalType(3, 0))), multiply(field(6, salesPriceType), constant(100L, createDecimalType(3, 0))).type(), BIGINT)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(INTEGER, BIGINT, BIGINT),
                                List.of(0, 1, 2),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty())))));
    }

    private List<Type> query47MonthlyRankedTypes(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_sales_price")).getFirst();
        Type itemBrandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        Type itemCategoryType = tableColumnTypes(tables, "item", List.of("i_category")).getFirst();
        Type storeNameType = tableColumnTypes(tables, "store", List.of("s_store_name")).getFirst();
        Type companyNameType = tableColumnTypes(tables, "store", List.of("s_company_name")).getFirst();
        Type sumType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType)).getFinalType();
        return List.of(itemCategoryType, itemBrandType, storeNameType, companyNameType, INTEGER, INTEGER, sumType, BIGINT);
    }

    private List<Page> query47MonthlyRankedSalesPages(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type itemBrandType = tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
        Type itemCategoryType = tableColumnTypes(tables, "item", List.of("i_category")).getFirst();
        Type storeNameType = tableColumnTypes(tables, "store", List.of("s_store_name")).getFirst();
        Type companyNameType = tableColumnTypes(tables, "store", List.of("s_company_name")).getFirst();

        List<Type> itemTypes = List.of(BIGINT, itemBrandType, itemCategoryType);
        List<Type> dateTypes = List.of(BIGINT, INTEGER, INTEGER);
        List<Type> storeTypes = List.of(BIGINT, storeNameType, companyNameType);
        List<Type> projectedTypes = List.of(itemCategoryType, itemBrandType, storeNameType, companyNameType, INTEGER, INTEGER, salesType);
        List<Type> groupedTypes = List.of(itemCategoryType, itemBrandType, storeNameType, companyNameType, INTEGER, INTEGER, salesSum.getFinalType());

        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_brand", "i_category"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, itemBrandType), field(2, itemCategoryType)),
                itemTypes);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year", "d_moy"),
                Optional.of(query57DatePredicate()),
                List.of(field(0, BIGINT), field(1, INTEGER), field(2, INTEGER)),
                dateTypes);
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name", "s_company_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, storeNameType), field(2, companyNameType)),
                storeTypes);

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(47_0, factTypes, List.of(1), itemPages, itemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(47_1, concatTypes(factTypes, itemTypes), List.of(0), datePages, dateTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(47_2, concatTypes(concatTypes(factTypes, itemTypes), dateTypes), List.of(2), storePages, storeTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                47_3,
                                Optional.empty(),
                                List.of(
                                        field(6, itemCategoryType),
                                        field(5, itemBrandType),
                                        field(11, storeNameType),
                                        field(12, companyNameType),
                                        field(8, INTEGER),
                                        field(9, INTEGER),
                                        field(3, salesType)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                47_4,
                                List.of(itemCategoryType, itemBrandType, storeNameType, companyNameType, INTEGER, INTEGER),
                                List.of(0, 1, 2, 3, 4, 5),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()))),
                        factoryStep(windowFactory(
                                47_5,
                                groupedTypes,
                                List.of(0, 1, 2, 3, 4, 5, 6),
                                List.of(0, 1, 2, 3),
                                List.of(4, 5),
                                List.of(ASC_NULLS_LAST, ASC_NULLS_LAST),
                List.of(rankWindowFunction())))));
    }

    private List<Page> query71ChannelSalesPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String timeColumn, String salesColumn, int operatorIdBase)
    {
        Type salesType = tableColumnTypes(tables, salesTable, List.of(salesColumn)).getFirst();
        return executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(soldDateColumn, itemColumn, timeColumn, salesColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                List.of(BIGINT, BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_moy", "d_year"),
                                        Optional.of(and(equal(1, 11, INTEGER), equal(2, 1999, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(2, BIGINT), field(3, salesType)),
                                List.of(BIGINT, BIGINT, salesType)))));
    }

    private List<Page> queryGroupedChannelSalesWithAddressOffsetPages(
            TpcdsParquetTables tables,
            String salesTable,
            String soldDateColumn,
            String itemColumn,
            String addressColumn,
            String salesColumn,
            List<String> itemColumns,
            Optional<RowExpression> itemFilter,
            List<RowExpression> itemProjections,
            List<Type> itemTypes,
            int year,
            int month,
            int operatorIdBase)
    {
        List<Type> factTypes = tableColumnTypes(tables, salesTable, List.of(soldDateColumn, itemColumn, addressColumn, salesColumn));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        Type offsetType = tableColumnTypes(tables, "customer_address", List.of("ca_gmt_offset")).getFirst();

        return executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(soldDateColumn, itemColumn, addressColumn, salesColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(1),
                                relationPages(tables, "item", itemColumns, itemFilter, itemProjections, itemTypes),
                                itemTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, itemTypes),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year", "d_moy"),
                                        Optional.of(and(equal(1, year, INTEGER), equal(2, month, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 2,
                                concatTypes(concatTypes(factTypes, itemTypes), List.of(BIGINT)),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "customer_address",
                                        List.of("ca_address_sk", "ca_gmt_offset"),
                                        Optional.of(equal(1, -500L, offsetType)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 3,
                                Optional.empty(),
                                List.of(field(5, itemTypes.get(1)), field(3, salesType)),
                                List.of(itemTypes.get(1), salesType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 4,
                                List.of(itemTypes.get(1)),
                                List.of(0),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 5,
                                Optional.empty(),
                                List.of(
                                        field(0, itemTypes.get(1)),
                                        coalesce(field(1, salesSum.getFinalType()), cast(constant(0L, BIGINT), BIGINT, salesSum.getFinalType()), salesSum.getFinalType())),
                                List.of(itemTypes.get(1), salesSum.getFinalType())))));
    }

    private List<Page> query61SalesPages(TpcdsParquetTables tables, boolean promotionalOnly, int operatorIdBase)
    {
        List<String> salesColumns = promotionalOnly
                ? List.of("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_promo_sk", "ss_ext_sales_price")
                : List.of("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_ext_sales_price");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type salesPriceType = salesTypes.getLast();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesPriceType));
        Type storeOffsetType = tableColumnTypes(tables, "store", List.of("s_gmt_offset")).getFirst();
        Type customerOffsetType = tableColumnTypes(tables, "customer_address", List.of("ca_gmt_offset")).getFirst();
        Type itemCategoryType = tableColumnTypes(tables, "item", List.of("i_category")).getFirst();
        Type promotionFlagType = tableColumnTypes(tables, "promotion", List.of("p_channel_dmail")).getFirst();

        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_gmt_offset"),
                Optional.of(equal(1, -500L, storeOffsetType)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year", "d_moy"),
                Optional.of(and(equal(1, 1998, INTEGER), equal(2, 11, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT)),
                List.of(BIGINT, BIGINT));
        List<Page> addressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_gmt_offset"),
                Optional.of(equal(1, -500L, customerOffsetType)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_category"),
                Optional.of(equal(1, itemCategoryType, "Jewelry")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        List<PipelineStep> steps = new ArrayList<>();
        steps.add(hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(3), storePages, List.of(BIGINT), List.of(0))));

        List<Type> afterStoreTypes = concatTypes(salesTypes, List.of(BIGINT));
        List<Type> afterPromotionTypes = afterStoreTypes;
        if (promotionalOnly) {
            List<Page> promotionPages = relationPages(
                    tables,
                    "promotion",
                    List.of("p_promo_sk", "p_channel_dmail", "p_channel_email", "p_channel_tv"),
                    Optional.of(or(equal(1, promotionFlagType, "Y"), equal(2, promotionFlagType, "Y"), equal(3, promotionFlagType, "Y"))),
                    List.of(field(0, BIGINT)),
                    List.of(BIGINT));
            steps.add(hashJoinStep(new HashJoinSpec(operatorIdBase + 1, afterStoreTypes, List.of(4), promotionPages, List.of(BIGINT), List.of(0))));
            afterPromotionTypes = concatTypes(afterStoreTypes, List.of(BIGINT));
        }

        steps.add(hashJoinStep(new HashJoinSpec(operatorIdBase + 2, afterPromotionTypes, List.of(0), datePages, List.of(BIGINT), List.of(0))));
        List<Type> afterDateTypes = concatTypes(afterPromotionTypes, List.of(BIGINT));
        steps.add(hashJoinStep(new HashJoinSpec(operatorIdBase + 3, afterDateTypes, List.of(2), customerPages, List.of(BIGINT, BIGINT), List.of(0))));
        List<Type> afterCustomerTypes = concatTypes(afterDateTypes, List.of(BIGINT, BIGINT));
        steps.add(hashJoinStep(new HashJoinSpec(operatorIdBase + 4, afterCustomerTypes, List.of(afterCustomerTypes.size() - 1), addressPages, List.of(BIGINT), List.of(0))));
        List<Type> afterAddressTypes = concatTypes(afterCustomerTypes, List.of(BIGINT));
        steps.add(hashJoinStep(new HashJoinSpec(operatorIdBase + 5, afterAddressTypes, List.of(1), itemPages, List.of(BIGINT), List.of(0))));
        steps.add(factoryStep(aggregationFactory(
                operatorIdBase + 6,
                salesSum.createAggregatorFactory(Step.SINGLE, List.of(promotionalOnly ? 5 : 4), OptionalInt.empty()))));

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                salesColumns,
                steps);
    }

    private List<Page> query58AllowedDatesPages(TpcdsParquetTables tables)
    {
        List<Type> dateTypes = List.of(DATE, INTEGER);
        List<Page> withWeekSequence = executeNestedLoopPages(
                relationPages(
                        tables,
                        "date_dim",
                        List.of("d_date", "d_week_seq"),
                        Optional.empty(),
                        List.of(field(0, DATE), field(1, INTEGER)),
                        dateTypes),
                dateTypes,
                query58ScalarWeekSequencePages(tables),
                List.of(INTEGER));

        return executePipelinePages(
                withWeekSequence,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                58_40,
                                Optional.of(equal(field(1, INTEGER), field(2, INTEGER), INTEGER)),
                                List.of(field(0, DATE)),
                                List.of(DATE))),
                        factoryStep(hashAggregationFactory(58_41, List.of(DATE), List.of(0)))));
    }

    private List<Page> query58ScalarWeekSequencePages(TpcdsParquetTables tables)
    {
        return executePipelinePages(
                relationPages(
                        tables,
                        "date_dim",
                        List.of("d_week_seq", "d_date"),
                        Optional.of(equal(1, 10_959L, DATE)),
                        List.of(field(0, INTEGER)),
                        List.of(INTEGER)),
                List.of(
                        factoryStep(hashAggregationFactory(58_42, List.of(INTEGER), List.of(0))),
                        factoryStep(enforceSingleRowFactory(58_43, List.of(INTEGER)))));
    }

    private List<Page> query83AllowedDatesPages(TpcdsParquetTables tables)
    {
        long[] weekSequences = query83WeekSequences(tables);
        return relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_week_seq"),
                Optional.of(anyOf(field(1, INTEGER), INTEGER, weekSequences)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
    }

    private long[] query83WeekSequences(TpcdsParquetTables tables)
    {
        Set<String> targetDates = Set.of(
                "2000-06-30",
                "2000-09-27",
                "2000-11-17");
        MaterializedResult result = scanTable(tables, "date_dim", List.of("d_date", "d_week_seq"), List.of(DATE, INTEGER));
        return result.getMaterializedRows().stream()
                .filter(row -> targetDates.contains(String.valueOf(row.getField(0))))
                .mapToLong(row -> ((Number) row.getField(1)).longValue())
                .distinct()
                .toArray();
    }

    private List<Page> query70ActiveStatesPages(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_store_sk", "ss_net_profit"));
        TestingAggregationFunction netProfitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(factTypes.get(2)));

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_store_sk", "ss_net_profit"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                70_0,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_month_seq"),
                                        Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                70_1,
                                concatTypes(factTypes, List.of(BIGINT)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "store",
                                        List.of("s_store_sk", "s_state"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                70_2,
                                Optional.empty(),
                                List.of(field(5, VARCHAR), field(2, factTypes.get(2))),
                                List.of(VARCHAR, factTypes.get(2)))),
                        factoryStep(hashAggregationFactory(
                                70_3,
                                List.of(VARCHAR),
                                List.of(0),
                                netProfitSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                70_4,
                                Optional.empty(),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)))));
    }

    private List<Page> query70SalesByLocationPages(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_store_sk", "ss_net_profit"));

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_store_sk", "ss_net_profit"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                70_10,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_month_seq"),
                                        Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                70_11,
                                concatTypes(factTypes, List.of(BIGINT)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "store",
                                        List.of("s_store_sk", "s_county", "s_state"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR)),
                                        List.of(BIGINT, VARCHAR, VARCHAR)),
                                List.of(BIGINT, VARCHAR, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                70_12,
                                Optional.empty(),
                                List.of(field(6, VARCHAR), field(5, VARCHAR), field(2, factTypes.get(2))),
                                List.of(VARCHAR, VARCHAR, factTypes.get(2))))));
    }

    private List<Page> query67SalesByRollupKeyPages(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sales_price"));
        Type salesPriceType = factTypes.get(4);
        Type quantityDecimalType = createDecimalType(10, 0);
        Type salesType = createDecimalType(17, 2);

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sales_price"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                67_9,
                                Optional.empty(),
                                List.of(
                                        field(0, BIGINT),
                                        field(1, BIGINT),
                                        field(2, BIGINT),
                                        coalesce(
                                                new CallExpression(
                                                        FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(salesPriceType, quantityDecimalType)),
                                                        List.of(
                                                                field(4, salesPriceType),
                                                                new CallExpression(FUNCTION_RESOLUTION.getCoercion(INTEGER, quantityDecimalType), List.of(field(3, INTEGER))))),
                                                constant(0L, salesType),
                                                salesType)),
                                List.of(BIGINT, BIGINT, BIGINT, salesType))),
                        hashJoinStep(new HashJoinSpec(
                                67_10,
                                List.of(BIGINT, BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year", "d_qoy", "d_moy", "d_month_seq"),
                                        Optional.of(and(greaterThan(4, 1199, INTEGER), lessThan(4, 1212, INTEGER))),
                                        List.of(field(0, BIGINT), field(1, INTEGER), field(2, INTEGER), field(3, INTEGER)),
                                        List.of(BIGINT, INTEGER, INTEGER, INTEGER)),
                                List.of(BIGINT, INTEGER, INTEGER, INTEGER),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                67_11,
                                concatTypes(List.of(BIGINT, BIGINT, BIGINT, salesType), List.of(BIGINT, INTEGER, INTEGER, INTEGER)),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "store",
                                        List.of("s_store_sk", "s_store_id"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                67_12,
                                concatTypes(List.of(BIGINT, BIGINT, BIGINT, salesType), List.of(BIGINT, INTEGER, INTEGER, INTEGER, BIGINT, VARCHAR)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_brand", "i_class", "i_category", "i_product_name"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR), field(2, VARCHAR), field(3, VARCHAR), field(4, VARCHAR)),
                                        List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR)),
                                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                67_13,
                                Optional.empty(),
                                List.of(
                                        field(13, VARCHAR),
                                        field(12, VARCHAR),
                                        field(11, VARCHAR),
                                        field(14, VARCHAR),
                                        field(5, INTEGER),
                                        field(6, INTEGER),
                                        field(7, INTEGER),
                                        field(9, VARCHAR),
                                field(3, salesType)),
                                List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, VARCHAR, salesType)))));
    }

    private List<Page> query65StoreItemSalesPages(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"));
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                65_0,
                                List.of(BIGINT, BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_month_seq"),
                                        Optional.of(and(greaterThan(1, 1175, INTEGER), lessThan(1, 1188, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                65_1,
                                Optional.empty(),
                                List.of(field(2, BIGINT), field(1, BIGINT), field(3, salesType)),
                                List.of(BIGINT, BIGINT, salesType))),
                        factoryStep(hashAggregationFactory(
                                65_2,
                                List.of(BIGINT, BIGINT),
                                List.of(0, 1),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                65_3,
                                Optional.empty(),
                                List.of(
                                        field(0, BIGINT),
                                        field(1, BIGINT),
                                        coalesce(field(2, salesSum.getFinalType()), constant(Int128.ZERO, salesSum.getFinalType()), salesSum.getFinalType())),
                                List.of(BIGINT, BIGINT, salesSum.getFinalType())))));
    }

    private List<Page> query44RankedItemsPages(TpcdsParquetTables tables, boolean descending)
    {
        List<Type> rankedTypes = List.of(BIGINT, BIGINT);
        List<Type> filteredItemTypes = List.of(BIGINT, BIGINT);
        List<Type> itemAggregateTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> itemJoinTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> scalarFilteredTypes = List.of(BIGINT, BIGINT);
        List<Type> scalarAggregateTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> scalarJoinTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> afterCrossTypes = concatTypes(itemJoinTypes, scalarJoinTypes);
        List<Type> rankInputTypes = List.of(BIGINT, BIGINT);
        List<SortOrder> sortOrder = List.of(descending ? DESC_NULLS_LAST : ASC_NULLS_LAST);

        List<Page> scalarPages = executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_store_sk", "ss_addr_sk", "ss_net_profit"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                44_10,
                                Optional.of(and(
                                        equal(0, 4L, BIGINT),
                                        new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(1, BIGINT)), List.of()))),
                                List.of(field(0, BIGINT), field(2, BIGINT)),
                                scalarFilteredTypes)),
                        factoryStep(hashAggregationFactory(
                                44_11,
                                List.of(BIGINT),
                                List.of(0),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                BIGINT_COUNT.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                44_12,
                                Optional.of(greaterThan(field(2, BIGINT), constant(0L, BIGINT), BIGINT)),
                                identityProjections(scalarAggregateTypes),
                                scalarAggregateTypes)),
                        factoryStep(enforceSingleRowFactory(44_13, scalarAggregateTypes)),
                        factoryStep(filterAndProjectFactory(
                                44_14,
                                Optional.empty(),
                                List.of(constant(1L, BIGINT), field(1, BIGINT), field(2, BIGINT)),
                                scalarJoinTypes))));

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_item_sk", "ss_store_sk", "ss_net_profit"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                44_0,
                                Optional.of(equal(1, 4L, BIGINT)),
                                List.of(field(0, BIGINT), field(2, BIGINT)),
                                filteredItemTypes)),
                        factoryStep(hashAggregationFactory(
                                44_1,
                                List.of(BIGINT),
                                List.of(0),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                BIGINT_COUNT.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                44_2,
                                Optional.of(greaterThan(field(2, BIGINT), constant(0L, BIGINT), BIGINT)),
                                List.of(constant(1L, BIGINT), field(0, BIGINT), field(1, BIGINT), field(2, BIGINT)),
                                itemJoinTypes)),
                        hashJoinStep(new HashJoinSpec(44_3, itemJoinTypes, List.of(0), scalarPages, scalarJoinTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                44_4,
                                Optional.of(query44ThresholdPredicate()),
                                List.of(
                                        field(1, BIGINT),
                                        query44AverageKey(field(2, BIGINT), field(3, BIGINT))),
                                rankInputTypes)),
                        factoryStep(topNRankingFactory(44_5, rankInputTypes, List.of(0, 1), List.of(), List.of(1), sortOrder, 10)),
                        factoryStep(filterAndProjectFactory(
                                44_6,
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(2, BIGINT)),
                                rankedTypes))));
    }

    public MaterializedResult query62(TpcdsParquetTables tables)
    {
        List<String> columns = List.of("ws_ship_date_sk", "ws_sold_date_sk", "ws_warehouse_sk", "ws_ship_mode_sk", "ws_web_site_sk");
        List<Type> factTypes = tableColumnTypes(tables, "web_sales", columns);
        List<Type> afterShipDateTypes = concatTypes(factTypes, List.of(BIGINT));
        List<Type> afterWarehouseTypes = concatTypes(afterShipDateTypes, List.of(BIGINT, VARCHAR));
        List<Type> afterShipModeTypes = concatTypes(afterWarehouseTypes, List.of(BIGINT, VARCHAR));
        List<Type> afterSiteTypes = concatTypes(afterShipModeTypes, List.of(BIGINT, VARCHAR));
        List<Type> projectedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Page> allowedShipDates = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_month_seq"),
                Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> warehouseNames = relationPages(
                tables,
                "warehouse",
                List.of("w_warehouse_sk", "w_warehouse_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> shipModeNames = relationPages(
                tables,
                "ship_mode",
                List.of("sm_ship_mode_sk", "sm_type"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> webSiteNames = relationPages(
                tables,
                "web_site",
                List.of("web_site_sk", "web_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<PipelineStep> steps = new ArrayList<>();
        steps.add(hashJoinStep(new HashJoinSpec(20, factTypes, List.of(0), allowedShipDates, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(21, afterShipDateTypes, List.of(2), warehouseNames, List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(22, afterWarehouseTypes, List.of(3), shipModeNames, List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(23, afterShipModeTypes, List.of(4), webSiteNames, List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(factoryStep(filterAndProjectFactory(24, Optional.empty(), shippingBucketProjections(7, 9, 11, 0, 1), projectedTypes)));
        steps.add(factoryStep(hashAggregationFactory(
                25,
                List.of(VARCHAR, VARCHAR, VARCHAR),
                List.of(0, 1, 2),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty()))));
        steps.add(factoryStep(topNFactory(26, outputTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST))));
        return executePipeline(tables.tableFiles("web_sales"), columns, steps, outputTypes);
    }

    public MaterializedResult query96(TpcdsParquetTables tables)
    {
        List<String> columns = List.of("ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk");
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", columns);
        List<Type> afterTimeTypes = concatTypes(factTypes, List.of(BIGINT));
        List<Type> afterHouseholdTypes = concatTypes(afterTimeTypes, List.of(BIGINT));
        List<Page> timeKeys = relationPages(
                tables,
                "time_dim",
                List.of("t_time_sk", "t_hour", "t_minute"),
                Optional.of(query96TimePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> householdKeys = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_dep_count"),
                Optional.of(equal(1, 7, INTEGER)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storeKeys = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name"),
                Optional.of(equal(1, VARCHAR, "ese")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        return executePipeline(
                tables.tableFiles("store_sales"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(30, factTypes, List.of(0), timeKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(31, afterTimeTypes, List.of(1), householdKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(32, afterHouseholdTypes, List.of(2), storeKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(aggregationFactory(33, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                List.of(BIGINT));
    }

    public MaterializedResult query99(TpcdsParquetTables tables)
    {
        List<String> columns = List.of("cs_ship_date_sk", "cs_sold_date_sk", "cs_warehouse_sk", "cs_ship_mode_sk", "cs_call_center_sk");
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", columns);
        List<Type> afterShipDateTypes = concatTypes(factTypes, List.of(BIGINT));
        List<Type> afterWarehouseTypes = concatTypes(afterShipDateTypes, List.of(BIGINT, VARCHAR));
        List<Type> afterShipModeTypes = concatTypes(afterWarehouseTypes, List.of(BIGINT, VARCHAR));
        List<Type> afterCallCenterTypes = concatTypes(afterShipModeTypes, List.of(BIGINT, VARCHAR));
        List<Type> projectedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Page> allowedShipDates = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_month_seq"),
                Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> warehouseNames = relationPages(
                tables,
                "warehouse",
                List.of("w_warehouse_sk", "w_warehouse_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> shipModeNames = relationPages(
                tables,
                "ship_mode",
                List.of("sm_ship_mode_sk", "sm_type"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<Page> callCenterNames = relationPages(
                tables,
                "call_center",
                List.of("cc_call_center_sk", "cc_name"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, VARCHAR)),
                List.of(BIGINT, VARCHAR));
        List<PipelineStep> steps = new ArrayList<>();
        steps.add(hashJoinStep(new HashJoinSpec(40, factTypes, List.of(0), allowedShipDates, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(41, afterShipDateTypes, List.of(2), warehouseNames, List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(42, afterWarehouseTypes, List.of(3), shipModeNames, List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(43, afterShipModeTypes, List.of(4), callCenterNames, List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(factoryStep(filterAndProjectFactory(44, Optional.empty(), shippingBucketProjections(7, 9, 11, 0, 1), projectedTypes)));
        steps.add(factoryStep(hashAggregationFactory(
                45,
                List.of(VARCHAR, VARCHAR, VARCHAR),
                List.of(0, 1, 2),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty()))));
        steps.add(factoryStep(topNFactory(46, outputTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST))));
        return executePipeline(tables.tableFiles("catalog_sales"), columns, steps, outputTypes);
    }

    public MaterializedResult query10(TpcdsParquetTables tables)
    {
        List<Type> groupTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> baseTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> afterDemographicsTypes = concatTypes(baseTypes, List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT));
        List<Page> eligibleAddressKeys = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_county"),
                Optional.of(varcharAnyOf(1, Set.of("Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storeCustomerKeys = customerKeyPagesForEligibleDates(
                tables,
                "store_sales",
                "ss_customer_sk",
                "ss_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_moy"),
                yearMonthRangePredicate(1, 2, 2002, 1, 4),
                54_000);
        List<Page> otherCustomerKeys = new ArrayList<>(customerKeyPagesForEligibleDates(
                tables,
                "web_sales",
                "ws_bill_customer_sk",
                "ws_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_moy"),
                yearMonthRangePredicate(1, 2, 2002, 1, 4),
                54_010));
        otherCustomerKeys.addAll(customerKeyPagesForEligibleDates(
                tables,
                "catalog_sales",
                "cs_ship_customer_sk",
                "cs_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_moy"),
                yearMonthRangePredicate(1, 2, 2002, 1, 4),
                54_020));
        OperatorFactory demographicsProjection = filterAndProjectFactory(
                58,
                Optional.empty(),
                List.of(
                        field(4, VARCHAR),
                        field(5, VARCHAR),
                        field(6, VARCHAR),
                        field(7, BIGINT),
                        field(8, VARCHAR),
                        field(9, BIGINT),
                        field(10, BIGINT),
                        field(11, BIGINT)),
                groupTypes);
        OperatorFactory groupedCount = hashAggregationFactory(59, groupTypes, List.of(0, 1, 2, 3, 4, 5, 6, 7), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()));
        OperatorFactory reorderedProjection = filterAndProjectFactory(
                60,
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
                outputTypes);
        OperatorFactory topN = topNFactory(61, outputTypes, 100, List.of(0, 1, 2, 4, 6, 8, 10, 12), List.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        List<PipelineStep> steps = new ArrayList<>();
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(50, baseTypes, 0, eligibleAddressKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(51, baseTypes, true)));
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(52, baseTypes, 1, storeCustomerKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(53, baseTypes, true)));
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(54, baseTypes, 1, otherCustomerKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(55, baseTypes, true)));
        steps.add(hashJoinStep(new HashJoinSpec(56, baseTypes, List.of(2), customerDemographicsBuildPages(tables), List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT), List.of(0))));
        steps.add(factoryStep(demographicsProjection));
        steps.add(factoryStep(groupedCount));
        steps.add(factoryStep(reorderedProjection));
        steps.add(factoryStep(topN));
        return executePipeline(tables.tableFiles("customer"), List.of("c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk"), steps, outputTypes);
    }

    public MaterializedResult query35(TpcdsParquetTables tables)
    {
        List<Type> baseTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> afterAddressTypes = concatTypes(baseTypes, List.of(BIGINT, VARCHAR));
        List<Type> groupTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(
                VARCHAR, VARCHAR, VARCHAR, BIGINT,
                BIGINT, BIGINT, BIGINT, BIGINT_AVG.getFinalType(),
                BIGINT, BIGINT, BIGINT, BIGINT, BIGINT_AVG.getFinalType(),
                BIGINT, BIGINT, BIGINT, BIGINT, BIGINT_AVG.getFinalType());
        List<Page> storeCustomerKeys = customerKeyPagesForEligibleDates(
                tables,
                "store_sales",
                "ss_customer_sk",
                "ss_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_qoy"),
                yearQuarterRangePredicate(1, 2, 2002, 1, 3),
                35_100);
        List<Page> otherCustomerKeys = new ArrayList<>(customerKeyPagesForEligibleDates(
                tables,
                "web_sales",
                "ws_bill_customer_sk",
                "ws_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_qoy"),
                yearQuarterRangePredicate(1, 2, 2002, 1, 3),
                35_110));
        otherCustomerKeys.addAll(customerKeyPagesForEligibleDates(
                tables,
                "catalog_sales",
                "cs_ship_customer_sk",
                "cs_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_qoy"),
                yearQuarterRangePredicate(1, 2, 2002, 1, 3),
                35_120));
        OperatorFactory demographicsProjection = filterAndProjectFactory(
                35_8,
                Optional.empty(),
                List.of(
                        field(4, VARCHAR),
                        field(6, VARCHAR),
                        field(7, VARCHAR),
                        field(11, BIGINT),
                        field(12, BIGINT),
                        field(13, BIGINT)),
                groupTypes);
        OperatorFactory groupedAggregation = hashAggregationFactory(
                35_9,
                groupTypes,
                List.of(0, 1, 2, 3, 4, 5),
                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                BIGINT_MIN.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                BIGINT_MAX.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                BIGINT_MIN.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                BIGINT_MAX.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                BIGINT_MIN.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                BIGINT_MAX.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()));
        OperatorFactory reorderedProjection = filterAndProjectFactory(
                35_10,
                Optional.empty(),
                List.of(
                        field(0, VARCHAR),
                        field(1, VARCHAR),
                        field(2, VARCHAR),
                        field(3, BIGINT),
                        field(6, BIGINT),
                        field(7, BIGINT),
                        field(8, BIGINT),
                        field(9, BIGINT_AVG.getFinalType()),
                        field(4, BIGINT),
                        field(10, BIGINT),
                        field(11, BIGINT),
                        field(12, BIGINT),
                        field(13, BIGINT_AVG.getFinalType()),
                        field(5, BIGINT),
                        field(14, BIGINT),
                        field(15, BIGINT),
                        field(16, BIGINT),
                        field(17, BIGINT_AVG.getFinalType())),
                outputTypes);
        OperatorFactory topN = topNFactory(35_11, outputTypes, 100, List.of(0, 1, 2, 3, 8, 13), List.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        List<PipelineStep> steps = new ArrayList<>();
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(35_0, baseTypes, 1, storeCustomerKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(35_1, baseTypes, true)));
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(35_2, baseTypes, 1, otherCustomerKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(35_3, baseTypes, true)));
        steps.add(hashJoinStep(new HashJoinSpec(35_4, baseTypes, List.of(0), customerAddressStateBuildPages(tables), List.of(BIGINT, VARCHAR), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(35_5, afterAddressTypes, List.of(2), customerDemographicsBuildPages(tables), List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT), List.of(0))));
        steps.add(factoryStep(demographicsProjection));
        steps.add(factoryStep(groupedAggregation));
        steps.add(factoryStep(reorderedProjection));
        steps.add(factoryStep(topN));
        return executePipeline(tables.tableFiles("customer"), List.of("c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk"), steps, outputTypes);
    }

    public MaterializedResult query69(TpcdsParquetTables tables)
    {
        List<Type> baseTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> groupTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT, VARCHAR, BIGINT);
        List<Page> eligibleAddressKeys = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_state"),
                Optional.of(varcharAnyOf(1, Set.of("KY", "GA", "NM"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storeCustomerKeys = customerKeyPagesForEligibleDates(
                tables,
                "store_sales",
                "ss_customer_sk",
                "ss_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_moy"),
                yearMonthRangePredicate(1, 2, 2001, 4, 6),
                69_100);
        List<Page> excludedCustomerKeys = new ArrayList<>(customerKeyPagesForEligibleDates(
                tables,
                "web_sales",
                "ws_bill_customer_sk",
                "ws_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_moy"),
                yearMonthRangePredicate(1, 2, 2001, 4, 6),
                69_110));
        excludedCustomerKeys.addAll(customerKeyPagesForEligibleDates(
                tables,
                "catalog_sales",
                "cs_ship_customer_sk",
                "cs_sold_date_sk",
                List.of("d_date_sk", "d_year", "d_moy"),
                yearMonthRangePredicate(1, 2, 2001, 4, 6),
                69_120));
        OperatorFactory demographicsProjection = filterAndProjectFactory(
                69_10,
                Optional.empty(),
                List.of(
                        field(4, VARCHAR),
                        field(5, VARCHAR),
                        field(6, VARCHAR),
                        field(7, BIGINT),
                        field(8, VARCHAR)),
                groupTypes);
        OperatorFactory groupedCount = hashAggregationFactory(69_11, groupTypes, List.of(0, 1, 2, 3, 4), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()));
        OperatorFactory reorderedProjection = filterAndProjectFactory(
                69_12,
                Optional.empty(),
                List.of(
                        field(0, VARCHAR),
                        field(1, VARCHAR),
                        field(2, VARCHAR),
                        field(5, BIGINT),
                        field(3, BIGINT),
                        field(5, BIGINT),
                        field(4, VARCHAR),
                        field(5, BIGINT)),
                outputTypes);
        OperatorFactory topN = topNFactory(69_13, outputTypes, 100, List.of(0, 1, 2, 4, 6), List.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        List<PipelineStep> steps = new ArrayList<>();
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(69_0, baseTypes, 0, eligibleAddressKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(69_1, baseTypes, true)));
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(69_2, baseTypes, 1, storeCustomerKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(69_3, baseTypes, true)));
        steps.add(semiJoinPagesStep(new SemiJoinPagesSpec(69_4, baseTypes, 1, excludedCustomerKeys, List.of(BIGINT), 0)));
        steps.add(factoryStep(semiJoinFilterProjectFactory(69_5, baseTypes, false)));
        steps.add(hashJoinStep(new HashJoinSpec(69_8, baseTypes, List.of(2), customerDemographicsBuildPages(tables), List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT), List.of(0))));
        steps.add(factoryStep(demographicsProjection));
        steps.add(factoryStep(groupedCount));
        steps.add(factoryStep(reorderedProjection));
        steps.add(factoryStep(topN));
        return executePipeline(tables.tableFiles("customer"), List.of("c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk"), steps, outputTypes);
    }

    public MaterializedResult query70(TpcdsParquetTables tables)
    {
        Type netProfitType = tableColumnTypes(tables, "store_sales", List.of("ss_net_profit")).getFirst();
        TestingAggregationFunction netProfitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(netProfitType));
        List<Type> locationTypes = List.of(VARCHAR, VARCHAR, netProfitType);
        List<Type> groupIdTypes = concatTypes(locationTypes, List.of(BIGINT));
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, BIGINT, netProfitSum.getFinalType());
        List<Type> rollupTypes = List.of(VARCHAR, VARCHAR, netProfitSum.getFinalType(), INTEGER, VARCHAR);
        List<Type> rankedTypes = concatTypes(rollupTypes, List.of(BIGINT));
        List<Type> outputTypes = List.of(netProfitSum.getFinalType(), VARCHAR, VARCHAR, INTEGER, BIGINT);

        return executePagesPipeline(
                query70SalesByLocationPages(tables),
                List.of(
                        hashJoinStep(new HashJoinSpec(70_20, locationTypes, List.of(0), query70ActiveStatesPages(tables), List.of(VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                70_21,
                                Optional.empty(),
                                List.of(field(0, VARCHAR), field(1, VARCHAR), field(2, netProfitType)),
                                locationTypes)),
                        factoryStep(groupIdFactory(
                                70_22,
                                groupIdTypes,
                                List.of(
                                        Map.of(2, 2),
                                        Map.of(0, 0, 2, 2),
                                        Map.of(0, 0, 1, 1, 2, 2)))),
                        factoryStep(hashAggregationFactory(
                                70_23,
                                List.of(VARCHAR, VARCHAR, BIGINT),
                                List.of(0, 1, 3),
                                netProfitSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                70_24,
                                Optional.empty(),
                                query70RollupProjection(netProfitSum.getFinalType()),
                                rollupTypes)),
                        factoryStep(topNRankingFactory(70_25, rollupTypes, List.of(0, 1, 2, 3, 4), List.of(3, 4), List.of(2), List.of(DESC_NULLS_LAST), 100)),
                        factoryStep(topNFactory(70_26, rankedTypes, 100, List.of(3, 4, 5), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        factoryStep(filterAndProjectFactory(
                                70_27,
                                Optional.empty(),
                                List.of(field(2, netProfitSum.getFinalType()), field(0, VARCHAR), field(1, VARCHAR), field(3, INTEGER), field(5, BIGINT)),
                                outputTypes))),
                outputTypes);
    }

    public MaterializedResult query67(TpcdsParquetTables tables)
    {
        Type salesType = createDecimalType(17, 2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> rollupTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, VARCHAR, salesType);
        List<Type> groupIdTypes = concatTypes(rollupTypes, List.of(BIGINT));
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, VARCHAR, BIGINT, salesSum.getFinalType());
        List<Type> rankedSourceTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, VARCHAR, salesSum.getFinalType());
        List<Type> outputTypes = concatTypes(rankedSourceTypes, List.of(BIGINT));

        return executePagesPipeline(
                query67SalesByRollupKeyPages(tables),
                List.of(
                        factoryStep(groupIdFactory(
                                67_20,
                                groupIdTypes,
                                List.of(
                                        Map.of(8, 8),
                                        Map.of(0, 0, 8, 8),
                                        Map.of(0, 0, 1, 1, 8, 8),
                                        Map.of(0, 0, 1, 1, 2, 2, 8, 8),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 8, 8),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 8, 8),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 8, 8),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 8, 8),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8)))),
                        factoryStep(hashAggregationFactory(
                                67_21,
                                List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, VARCHAR, BIGINT),
                                List.of(0, 1, 2, 3, 4, 5, 6, 7, 9),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(8), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                67_22,
                                Optional.empty(),
                                List.of(
                                        field(0, VARCHAR),
                                        field(1, VARCHAR),
                                        field(2, VARCHAR),
                                        field(3, VARCHAR),
                                        field(4, INTEGER),
                                        field(5, INTEGER),
                                        field(6, INTEGER),
                                        field(7, VARCHAR),
                                        field(9, salesSum.getFinalType())),
                                rankedSourceTypes)),
                        factoryStep(topNRankingFactory(67_23, rankedSourceTypes, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8), List.of(0), List.of(8), List.of(DESC_NULLS_LAST), 100)),
                        factoryStep(topNFactory(67_24, outputTypes, 100, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query80(TpcdsParquetTables tables)
    {
        ChannelPages storeChannel = query80ChannelPages(
                tables,
                "store_sales",
                List.of("ss_sold_date_sk", "ss_item_sk", "ss_promo_sk", "ss_store_sk", "ss_ticket_number", "ss_ext_sales_price", "ss_net_profit"),
                "store_returns",
                List.of("sr_item_sk", "sr_ticket_number", "sr_return_amt", "sr_net_loss"),
                "store",
                List.of("s_store_sk", "s_store_id"),
                "store channel",
                "store",
                80_100);
        ChannelPages catalogChannel = query80ChannelPages(
                tables,
                "catalog_sales",
                List.of("cs_sold_date_sk", "cs_item_sk", "cs_promo_sk", "cs_catalog_page_sk", "cs_order_number", "cs_ext_sales_price", "cs_net_profit"),
                "catalog_returns",
                List.of("cr_item_sk", "cr_order_number", "cr_return_amount", "cr_net_loss"),
                "catalog_page",
                List.of("cp_catalog_page_sk", "cp_catalog_page_id"),
                "catalog channel",
                "catalog_page",
                80_200);
        ChannelPages webChannel = query80ChannelPages(
                tables,
                "web_sales",
                List.of("ws_sold_date_sk", "ws_item_sk", "ws_promo_sk", "ws_web_site_sk", "ws_order_number", "ws_ext_sales_price", "ws_net_profit"),
                "web_returns",
                List.of("wr_item_sk", "wr_order_number", "wr_return_amt", "wr_net_loss"),
                "web_site",
                List.of("web_site_sk", "web_site_id"),
                "web channel",
                "web_site",
                80_300);

        List<Page> unionPages = new ArrayList<>(storeChannel.pages());
        unionPages.addAll(catalogChannel.pages());
        unionPages.addAll(webChannel.pages());

        List<Type> branchTypes = storeChannel.types();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(branchTypes.get(2)));
        TestingAggregationFunction returnsSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(branchTypes.get(3)));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(branchTypes.get(4)));
        List<Type> groupIdTypes = concatTypes(branchTypes, List.of(BIGINT));
        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, BIGINT, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());

        return executePagesPipeline(
                unionPages,
                List.of(
                        factoryStep(groupIdFactory(
                                80_1,
                                groupIdTypes,
                                List.of(
                                        Map.of(2, 2, 3, 3, 4, 4),
                                        Map.of(0, 0, 2, 2, 3, 3, 4, 4),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4)))),
                        factoryStep(hashAggregationFactory(
                                80_2,
                                List.of(VARCHAR, VARCHAR, BIGINT),
                                List.of(0, 1, 5),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                returnsSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                profitSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                80_3,
                                Optional.empty(),
                                List.of(field(0, VARCHAR), field(1, VARCHAR), field(3, salesSum.getFinalType()), field(4, returnsSum.getFinalType()), field(5, profitSum.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(80_4, outputTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query77(TpcdsParquetTables tables)
    {
        ChannelPages storeChannel = query77ChannelPages(
                tables,
                "store_sales",
                List.of("ss_sold_date_sk", "ss_store_sk", "ss_ext_sales_price", "ss_net_profit"),
                "store_returns",
                List.of("sr_returned_date_sk", "sr_store_sk", "sr_return_amt", "sr_net_loss"),
                "store channel",
                77_100);
        ChannelPages catalogChannel = query77ChannelPages(
                tables,
                "catalog_sales",
                List.of("cs_sold_date_sk", "cs_call_center_sk", "cs_ext_sales_price", "cs_net_profit"),
                "catalog_returns",
                List.of("cr_returned_date_sk", "cr_call_center_sk", "cr_return_amount", "cr_net_loss"),
                "catalog channel",
                77_200);
        ChannelPages webChannel = query77ChannelPages(
                tables,
                "web_sales",
                List.of("ws_sold_date_sk", "ws_web_page_sk", "ws_ext_sales_price", "ws_net_profit"),
                "web_returns",
                List.of("wr_returned_date_sk", "wr_web_page_sk", "wr_return_amt", "wr_net_loss"),
                "web channel",
                77_300);

        List<Page> unionPages = new ArrayList<>(storeChannel.pages());
        unionPages.addAll(catalogChannel.pages());
        unionPages.addAll(webChannel.pages());

        List<Type> branchTypes = storeChannel.types();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(branchTypes.get(2)));
        TestingAggregationFunction returnsSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(branchTypes.get(3)));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(branchTypes.get(4)));
        List<Type> groupIdTypes = concatTypes(branchTypes, List.of(BIGINT));
        List<Type> groupedTypes = List.of(VARCHAR, BIGINT, BIGINT, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());
        List<Type> outputTypes = List.of(VARCHAR, BIGINT, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());

        return executePagesPipeline(
                unionPages,
                List.of(
                        factoryStep(groupIdFactory(
                                77_1,
                                groupIdTypes,
                                List.of(
                                        Map.of(2, 2, 3, 3, 4, 4),
                                        Map.of(0, 0, 2, 2, 3, 3, 4, 4),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4)))),
                        factoryStep(hashAggregationFactory(
                                77_2,
                                List.of(VARCHAR, BIGINT, BIGINT),
                                List.of(0, 1, 5),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                returnsSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                profitSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                77_3,
                                Optional.empty(),
                                List.of(
                                        field(0, VARCHAR),
                                        field(1, BIGINT),
                                        field(3, salesSum.getFinalType()),
                                        field(4, returnsSum.getFinalType()),
                                        field(5, profitSum.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(77_4, outputTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query05(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (org.weakref.nitro.data.Row row : TpcdsManualQuerySupport.query05Rows(new Allocator(), tables)) {
            result.row(row.values());
        }
        return result.build();
    }

    public MaterializedResult query94(TpcdsParquetTables tables)
    {
        List<Page> eligibleOrders = query95MultiWarehouseOrderPages(tables, 94_100);
        List<Page> returnedEligibleOrders = query95ReturnedEligibleOrderPages(tables, eligibleOrders, 94_200);

        List<String> baseColumns = List.of("ws_ship_date_sk", "ws_ship_addr_sk", "ws_web_site_sk", "ws_order_number", "ws_ext_ship_cost", "ws_net_profit");
        List<Type> baseTypes = tableColumnTypes(tables, "web_sales", baseColumns);
        Type shipCostType = baseTypes.get(4);
        Type profitType = baseTypes.get(5);

        List<Page> qualifiedSalesPages = executePipelinePages(
                tables.tableFiles("web_sales"),
                baseColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(94_300, baseTypes, List.of(0), relationPages(
                                tables,
                                "date_dim",
                                List.of("d_date_sk", "d_date"),
                                Optional.of(betweenInclusive(field(1, DATE), LocalDate.of(1999, 2, 1).toEpochDay(), LocalDate.of(1999, 4, 2).toEpochDay(), DATE)),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(94_301, concatTypes(baseTypes, List.of(BIGINT)), List.of(1), relationPages(
                                tables,
                                "customer_address",
                                List.of("ca_address_sk", "ca_state"),
                                Optional.of(equal(1, tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_state")).get(1), "IL")),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(94_302, concatTypes(concatTypes(baseTypes, List.of(BIGINT)), List.of(BIGINT)), List.of(2), relationPages(
                                tables,
                                "web_site",
                                List.of("web_site_sk", "web_company_name"),
                                Optional.of(equal(substring(field(1, VARCHAR), 1, 3), constant(Slices.utf8Slice("pri"), VARCHAR), VARCHAR)),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(94_303, Optional.empty(), List.of(field(3, BIGINT), field(4, shipCostType), field(5, profitType)), List.of(BIGINT, shipCostType, profitType))),
                        hashJoinStep(new HashJoinSpec(94_304, List.of(BIGINT, shipCostType, profitType), List.of(0), eligibleOrders, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(94_305, Optional.empty(), List.of(field(0, BIGINT), field(1, shipCostType), field(2, profitType)), List.of(BIGINT, shipCostType, profitType))),
                        hashJoinStep(new HashJoinSpec(94_306, List.of(BIGINT, shipCostType, profitType), List.of(0), returnedEligibleOrders, List.of(BIGINT), List.of(0), JoinOperatorType.probeOuterJoin(false))),
                        factoryStep(filterAndProjectFactory(
                                94_307,
                                Optional.of(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(3, BIGINT)), List.of())),
                                List.of(field(0, BIGINT), field(1, shipCostType), field(2, profitType)),
                                List.of(BIGINT, shipCostType, profitType)))));

        TestingAggregationFunction shipCostSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(shipCostType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));

        MaterializedResult totalResult = executePagesPipeline(
                qualifiedSalesPages,
                List.of(factoryStep(aggregationFactory(
                        94_308,
                        shipCostSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                        profitSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))),
                List.of(shipCostSum.getFinalType(), profitSum.getFinalType()));

        long distinctOrderCount = singleLongResult(executePagesPipeline(
                qualifiedSalesPages,
                List.of(
                        factoryStep(hashAggregationFactory(94_309, List.of(BIGINT), List.of(0))),
                        factoryStep(aggregationFactory(94_310, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                List.of(BIGINT)));

        List<Type> outputTypes = List.of(BIGINT, shipCostSum.getFinalType(), profitSum.getFinalType());
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        io.trino.testing.MaterializedRow totals = totalResult.getMaterializedRows().getFirst();
        result.row(distinctOrderCount, totals.getField(0), totals.getField(1));
        return result.build();
    }

    public MaterializedResult query95(TpcdsParquetTables tables)
    {
        List<Page> eligibleOrders = query95MultiWarehouseOrderPages(tables, 95_100);
        List<Page> returnedEligibleOrders = query95ReturnedEligibleOrderPages(tables, eligibleOrders, 95_200);

        List<String> baseColumns = List.of("ws_ship_date_sk", "ws_ship_addr_sk", "ws_web_site_sk", "ws_order_number", "ws_ext_ship_cost", "ws_net_profit");
        List<Type> baseTypes = tableColumnTypes(tables, "web_sales", baseColumns);
        Type shipCostType = baseTypes.get(4);
        Type profitType = baseTypes.get(5);

        List<Page> qualifiedSalesPages = executePipelinePages(
                tables.tableFiles("web_sales"),
                baseColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(95_300, baseTypes, List.of(0), relationPages(
                                tables,
                                "date_dim",
                                List.of("d_date_sk", "d_date"),
                                Optional.of(betweenInclusive(field(1, DATE), LocalDate.of(1999, 2, 1).toEpochDay(), LocalDate.of(1999, 4, 2).toEpochDay(), DATE)),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(95_301, concatTypes(baseTypes, List.of(BIGINT)), List.of(1), relationPages(
                                tables,
                                "customer_address",
                                List.of("ca_address_sk", "ca_state"),
                                Optional.of(equal(1, tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_state")).get(1), "IL")),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(95_302, concatTypes(concatTypes(baseTypes, List.of(BIGINT)), List.of(BIGINT)), List.of(2), relationPages(
                                tables,
                                "web_site",
                                List.of("web_site_sk", "web_company_name"),
                                Optional.of(equal(substring(field(1, VARCHAR), 1, 3), constant(Slices.utf8Slice("pri"), VARCHAR), VARCHAR)),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(95_303, Optional.empty(), List.of(field(3, BIGINT), field(4, shipCostType), field(5, profitType)), List.of(BIGINT, shipCostType, profitType))),
                        hashJoinStep(new HashJoinSpec(95_304, List.of(BIGINT, shipCostType, profitType), List.of(0), eligibleOrders, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(95_305, Optional.empty(), List.of(field(0, BIGINT), field(1, shipCostType), field(2, profitType)), List.of(BIGINT, shipCostType, profitType))),
                        hashJoinStep(new HashJoinSpec(95_306, List.of(BIGINT, shipCostType, profitType), List.of(0), returnedEligibleOrders, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(95_307, Optional.empty(), List.of(field(0, BIGINT), field(1, shipCostType), field(2, profitType)), List.of(BIGINT, shipCostType, profitType)))));

        TestingAggregationFunction shipCostSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(shipCostType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));

        MaterializedResult totalResult = executePagesPipeline(
                qualifiedSalesPages,
                List.of(factoryStep(aggregationFactory(
                        95_308,
                        shipCostSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                        profitSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))),
                List.of(shipCostSum.getFinalType(), profitSum.getFinalType()));

        long distinctOrderCount = singleLongResult(executePagesPipeline(
                qualifiedSalesPages,
                List.of(
                        factoryStep(hashAggregationFactory(95_309, List.of(BIGINT), List.of(0))),
                        factoryStep(aggregationFactory(95_310, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                List.of(BIGINT)));

        List<Type> outputTypes = List.of(BIGINT, shipCostSum.getFinalType(), profitSum.getFinalType());
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        io.trino.testing.MaterializedRow totals = totalResult.getMaterializedRows().getFirst();
        result.row(distinctOrderCount, totals.getField(0), totals.getField(1));
        return result.build();
    }

    public MaterializedResult query30(TpcdsParquetTables tables)
    {
        Type stateType = query81StateType(tables);
        List<Page> customerTotalReturnPages = query30CustomerTotalReturnPages(tables, 30_100);
        List<Type> customerTotalReturnTypes = List.of(BIGINT, stateType, query30ReturnSumType(tables));
        List<Page> stateAveragePages = query30StateAveragePages(tables);
        List<Type> stateAverageTypes = List.of(stateType, query30StateAverageType(tables));

        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk", "c_customer_id", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country", "c_login", "c_email_address", "c_last_review_date_sk"));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk", "c_customer_id", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country", "c_login", "c_email_address", "c_last_review_date_sk"),
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes);
        List<Page> gaAddressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_state"),
                Optional.of(equal(1, stateType, "GA")),
                identityProjections(List.of(BIGINT, stateType)),
                List.of(BIGINT, stateType));

        List<Type> afterAverageTypes = concatTypes(customerTotalReturnTypes, stateAverageTypes);
        List<Type> afterCustomerTypes = concatTypes(afterAverageTypes, customerTypes);
        List<Type> outputTypes = List.of(
                customerTypes.get(2),
                customerTypes.get(3),
                customerTypes.get(4),
                customerTypes.get(5),
                customerTypes.get(6),
                customerTypes.get(7),
                customerTypes.get(8),
                customerTypes.get(9),
                customerTypes.get(10),
                customerTypes.get(11),
                customerTypes.get(12),
                customerTypes.get(13),
                customerTotalReturnTypes.get(2));

        return executePagesPipeline(
                customerTotalReturnPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(30_120, customerTotalReturnTypes, List.of(1), stateAveragePages, stateAverageTypes, List.of(0), JoinOperatorType.probeOuterJoin(false))),
                        hashJoinStep(new HashJoinSpec(30_121, afterAverageTypes, List.of(0), customerPages, customerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(30_122, afterCustomerTypes, List.of(6), gaAddressPages, List.of(BIGINT, stateType), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                30_123,
                                Optional.of(query81ThresholdPredicate(customerTotalReturnTypes.get(2), stateAverageTypes.get(1))),
                                List.of(
                                        field(7, customerTypes.get(2)),
                                        field(8, customerTypes.get(3)),
                                        field(9, customerTypes.get(4)),
                                        field(10, customerTypes.get(5)),
                                        field(11, customerTypes.get(6)),
                                        field(12, customerTypes.get(7)),
                                        field(13, customerTypes.get(8)),
                                        field(14, customerTypes.get(9)),
                                        field(15, customerTypes.get(10)),
                                        field(16, customerTypes.get(11)),
                                        field(17, customerTypes.get(12)),
                                        field(18, customerTypes.get(13)),
                                        field(2, customerTotalReturnTypes.get(2))),
                                outputTypes)),
                        factoryStep(topNFactory(30_124, outputTypes, 100, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query76(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_ext_sales_price")).getFirst();
        Type sumType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType)).getFinalType();
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, INTEGER, INTEGER, VARCHAR, BIGINT, sumType);

        List<Page> unionPages = new ArrayList<>(query76ChannelCategorySalesPages(tables, "store", "ss_store_sk", "store_sales", "ss_store_sk", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price", 76_100));
        unionPages.addAll(query76ChannelCategorySalesPages(tables, "web", "ws_ship_customer_sk", "web_sales", "ws_ship_customer_sk", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price", 76_200));
        unionPages.addAll(query76ChannelCategorySalesPages(tables, "catalog", "cs_ship_addr_sk", "catalog_sales", "cs_ship_addr_sk", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price", 76_300));

        return executePagesPipeline(
                unionPages,
                List.of(factoryStep(topNFactory(76_400, outputTypes, 100, List.of(0, 1, 2, 3, 4), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query29(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type salesQuantityType = salesTypes.get(5);
        Type returnQuantityType = tableColumnTypes(tables, "store_returns", List.of("sr_return_quantity")).getFirst();
        Type catalogQuantityType = tableColumnTypes(tables, "catalog_sales", List.of("cs_quantity")).getFirst();
        TestingAggregationFunction salesQuantitySum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
        TestingAggregationFunction returnQuantitySum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
        TestingAggregationFunction catalogQuantitySum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));

        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_item_id", "i_item_desc"));
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_store_id", "s_store_name"));
        List<Type> returnsTypes = tableColumnTypes(tables, "store_returns", List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number", "sr_returned_date_sk", "sr_return_quantity"));
        List<Type> catalogSalesTypes = tableColumnTypes(tables, "catalog_sales", List.of("cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", "cs_quantity"));

        List<Type> afterSaleDateTypes = concatTypes(salesTypes, List.of(BIGINT));
        List<Type> afterItemTypes = concatTypes(afterSaleDateTypes, itemTypes);
        List<Type> afterStoreTypes = concatTypes(afterItemTypes, storeTypes);
        List<Type> afterReturnsTypes = concatTypes(afterStoreTypes, returnsTypes);
        List<Type> afterReturnDateTypes = concatTypes(afterReturnsTypes, List.of(BIGINT));
        List<Type> afterCatalogSalesTypes = concatTypes(afterReturnDateTypes, catalogSalesTypes);
        List<Type> outputTypes = List.of(itemTypes.get(1), itemTypes.get(2), storeTypes.get(1), storeTypes.get(2), salesQuantitySum.getFinalType(), returnQuantitySum.getFinalType(), catalogQuantitySum.getFinalType());

        List<Page> saleDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(equal(1, 9, INTEGER), equal(2, 1999, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_id", "i_item_desc"),
                Optional.empty(),
                identityProjections(itemTypes),
                itemTypes);
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_id", "s_store_name"),
                Optional.empty(),
                identityProjections(storeTypes),
                storeTypes);
        List<Page> returnsPages = relationPages(
                tables,
                "store_returns",
                List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number", "sr_returned_date_sk", "sr_return_quantity"),
                Optional.empty(),
                identityProjections(returnsTypes),
                returnsTypes);
        List<Page> returnDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(betweenInclusive(field(1, INTEGER), 9, 12, INTEGER), equal(2, 1999, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> catalogDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(or(equal(1, 1999, INTEGER), equal(1, 2000, INTEGER), equal(1, 2001, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(29_100, salesTypes, List.of(0), saleDatePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(29_101, afterSaleDateTypes, List.of(1), itemPages, itemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(29_102, afterItemTypes, List.of(2), storePages, storeTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(29_103, afterStoreTypes, List.of(3, 1, 4), returnsPages, returnsTypes, List.of(0, 1, 2))),
                        hashJoinStep(new HashJoinSpec(29_104, afterReturnsTypes, List.of(16), returnDatePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(29_105, afterReturnDateTypes, List.of(13, 14), executePipelinePages(tables.tableFiles("catalog_sales"), List.of("cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", "cs_quantity"), List.of()), catalogSalesTypes, List.of(0, 1))),
                        hashJoinStep(new HashJoinSpec(29_106, afterCatalogSalesTypes, List.of(21), catalogDatePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                29_107,
                                Optional.empty(),
                                List.of(
                                        field(8, itemTypes.get(1)),
                                        field(9, itemTypes.get(2)),
                                        field(11, storeTypes.get(1)),
                                        field(12, storeTypes.get(2)),
                                        cast(field(5, salesQuantityType), salesQuantityType, BIGINT),
                                        cast(field(17, returnQuantityType), returnQuantityType, BIGINT),
                                        cast(field(22, catalogQuantityType), catalogQuantityType, BIGINT)),
                                List.of(itemTypes.get(1), itemTypes.get(2), storeTypes.get(1), storeTypes.get(2), BIGINT, BIGINT, BIGINT))),
                        factoryStep(hashAggregationFactory(
                                29_108,
                                List.of(itemTypes.get(1), itemTypes.get(2), storeTypes.get(1), storeTypes.get(2)),
                                List.of(0, 1, 2, 3),
                                salesQuantitySum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                returnQuantitySum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                catalogQuantitySum.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()))),
                        factoryStep(topNFactory(29_109, outputTypes, 100, List.of(0, 1, 2, 3), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query31(TpcdsParquetTables tables)
    {
        Type storeRevenueType = query31RevenueType(tables, "store_sales", "ss_ext_sales_price");
        Type webRevenueType = query31RevenueType(tables, "web_sales", "ws_ext_sales_price");
        Type ratioType = createDecimalType(38, 6);

        List<Page> storeQuarterOnePages = query31ChannelCountyQuarterRevenuePages(tables, "store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 1, 31_100);
        List<Page> storeQuarterTwoPages = query31ChannelCountyQuarterRevenuePages(tables, "store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 2, 31_110);
        List<Page> storeQuarterThreePages = query31ChannelCountyQuarterRevenuePages(tables, "store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 3, 31_120);
        List<Page> webQuarterOnePages = query31ChannelCountyQuarterRevenuePages(tables, "web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 1, 31_130);
        List<Page> webQuarterTwoPages = query31ChannelCountyQuarterRevenuePages(tables, "web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 2, 31_140);
        List<Page> webQuarterThreePages = query31ChannelCountyQuarterRevenuePages(tables, "web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 3, 31_150);

        List<Type> storeTypes = List.of(VARCHAR, storeRevenueType);
        List<Type> webTypes = List.of(VARCHAR, webRevenueType);
        List<Type> afterStoreQuarterTwoTypes = concatTypes(storeTypes, storeTypes);
        List<Type> afterStoreQuarterThreeTypes = concatTypes(afterStoreQuarterTwoTypes, storeTypes);
        List<Type> afterWebQuarterOneTypes = concatTypes(afterStoreQuarterThreeTypes, webTypes);
        List<Type> afterWebQuarterTwoTypes = concatTypes(afterWebQuarterOneTypes, webTypes);
        List<Type> afterWebQuarterThreeTypes = concatTypes(afterWebQuarterTwoTypes, webTypes);
        List<Type> outputTypes = List.of(VARCHAR, INTEGER, ratioType, ratioType, ratioType, ratioType);

        return executePagesPipeline(
                storeQuarterOnePages,
                List.of(
                        hashJoinStep(new HashJoinSpec(31_160, storeTypes, List.of(0), storeQuarterTwoPages, storeTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(31_161, afterStoreQuarterTwoTypes, List.of(0), storeQuarterThreePages, storeTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(31_162, afterStoreQuarterThreeTypes, List.of(0), webQuarterOnePages, webTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(31_163, afterWebQuarterOneTypes, List.of(0), webQuarterTwoPages, webTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(31_164, afterWebQuarterTwoTypes, List.of(0), webQuarterThreePages, webTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                31_165,
                                Optional.of(query31GrowthPredicate(storeRevenueType, webRevenueType)),
                                query31Projection(storeRevenueType, webRevenueType, ratioType),
                                outputTypes)),
                        factoryStep(topNFactory(31_166, outputTypes, 100, List.of(0), List.of(ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query32(TpcdsParquetTables tables)
    {
        Type discountType = tableColumnTypes(tables, "catalog_sales", List.of("cs_ext_discount_amt")).getFirst();
        Type averageType = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(discountType)).getFinalType();
        TestingAggregationFunction discountSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(discountType));
        List<Type> filteredTypes = List.of(BIGINT, discountType);
        List<Type> averagePagesTypes = List.of(BIGINT, averageType);
        List<Type> outputTypes = List.of(discountSum.getFinalType());

        return executePagesPipeline(
                query32FilteredDiscountPages(tables, 32_100),
                List.of(
                        hashJoinStep(new HashJoinSpec(32_120, filteredTypes, List.of(0), query32ItemAveragePages(tables), averagePagesTypes, List.of(0), JoinOperatorType.probeOuterJoin(false))),
                        factoryStep(filterAndProjectFactory(
                                32_121,
                                Optional.of(query92DiscountThresholdPredicate(discountType, averageType)),
                                List.of(field(1, discountType)),
                                List.of(discountType))),
                        factoryStep(aggregationFactory(
                                32_122,
                                discountSum.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))),
                outputTypes);
    }

    public MaterializedResult query23(TpcdsParquetTables tables)
    {
        List<Page> channelPages = new ArrayList<>(query23ChannelPages(
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_bill_customer_sk",
                "cs_item_sk",
                "cs_quantity",
                "cs_list_price",
                23_100));
        channelPages.addAll(query23ChannelPages(
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_bill_customer_sk",
                "ws_item_sk",
                "ws_quantity",
                "ws_list_price",
                23_200));
        Type salesType = createDecimalType(17, 2);
        TestingAggregationFunction sum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        return executePagesPipeline(
                channelPages,
                List.of(factoryStep(aggregationFactory(23_300, sum.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))),
                List.of(sum.getFinalType()));
    }

    public MaterializedResult query24(TpcdsParquetTables tables)
    {
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name", "c_birth_country"));
        Type lastNameType = customerTypes.get(1);
        Type firstNameType = customerTypes.get(2);
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_market_id", "s_store_name", "s_state", "s_zip"));
        Type storeNameType = storeTypes.get(2);
        Type ssalesInputType = createDecimalType(19, 2);
        Type preAggregatedRevenueType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(ssalesInputType)).getFinalType();
        Type revenueType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(preAggregatedRevenueType)).getFinalType();
        TestingAggregationFunction groupedSalesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(revenueType));
        Type groupedRevenueType = groupedSalesSum.getFinalType();
        TestingAggregationFunction revenueCount = FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(revenueType));
        List<Type> groupedTypes = List.of(lastNameType, firstNameType, storeNameType, groupedRevenueType);
        List<Type> scalarTypes = List.of(groupedRevenueType, BIGINT);

        List<Page> groupedSalesPages = executePipelinePages(
                query24SalesPages(tables, Optional.of("pale")),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                24_100,
                                Optional.empty(),
                                List.of(field(0, lastNameType), field(1, firstNameType), field(2, storeNameType), field(10, revenueType)),
                                groupedTypes)),
                        factoryStep(hashAggregationFactory(
                                24_101,
                                List.of(lastNameType, firstNameType, storeNameType),
                                List.of(0, 1, 2),
                                groupedSalesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty())))));
        List<Page> averageSalesPages = executePipelinePages(
                query24AverageSalesPages(tables),
                List.of(factoryStep(aggregationFactory(
                        24_110,
                        groupedSalesSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                        revenueCount.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty())))));

        MaterializedResult fullResult = executePagesPipeline(
                executeNestedLoopPages(groupedSalesPages, groupedTypes, averageSalesPages, scalarTypes),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                24_120,
                                Optional.of(query24ThresholdPredicate(groupedRevenueType)),
                                List.of(field(0, lastNameType), field(1, firstNameType), field(2, storeNameType), field(3, groupedRevenueType)),
                                groupedTypes)),
                        factoryStep(topNFactory(24_121, groupedTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                groupedTypes);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), groupedTypes);
        for (io.trino.testing.MaterializedRow row : fullResult.getMaterializedRows()) {
            result.row(row.getField(0), row.getField(1), row.getField(2), row.getField(3));
        }
        return result.build();
    }

    public MaterializedResult query25(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_ticket_number", "ss_net_profit");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type salesProfitType = salesTypes.get(5);
        Type returnLossType = tableColumnTypes(tables, "store_returns", List.of("sr_net_loss")).getFirst();
        Type catalogProfitType = tableColumnTypes(tables, "catalog_sales", List.of("cs_net_profit")).getFirst();
        TestingAggregationFunction salesProfitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesProfitType));
        TestingAggregationFunction returnLossSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnLossType));
        TestingAggregationFunction catalogProfitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(catalogProfitType));

        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_item_id", "i_item_desc"));
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_store_id", "s_store_name"));
        List<Type> returnsTypes = tableColumnTypes(tables, "store_returns", List.of("sr_item_sk", "sr_customer_sk", "sr_ticket_number", "sr_returned_date_sk", "sr_net_loss"));
        List<Type> catalogSalesTypes = tableColumnTypes(tables, "catalog_sales", List.of("cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", "cs_net_profit"));

        List<Type> afterSaleDateTypes = concatTypes(salesTypes, List.of(BIGINT));
        List<Type> afterReturnsTypes = concatTypes(afterSaleDateTypes, returnsTypes);
        List<Type> afterReturnDateTypes = concatTypes(afterReturnsTypes, List.of(BIGINT));
        List<Type> afterCatalogSalesTypes = concatTypes(afterReturnDateTypes, catalogSalesTypes);
        List<Type> afterCatalogDateTypes = concatTypes(afterCatalogSalesTypes, List.of(BIGINT));
        List<Type> afterStoreTypes = concatTypes(afterCatalogDateTypes, storeTypes);
        List<Type> outputTypes = List.of(itemTypes.get(1), itemTypes.get(2), storeTypes.get(1), storeTypes.get(2), salesProfitSum.getFinalType(), returnLossSum.getFinalType(), catalogProfitSum.getFinalType());

        List<Page> saleDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(equal(1, 4, INTEGER), equal(2, 2001, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> returnsPages = relationPages(
                tables,
                "store_returns",
                List.of("sr_item_sk", "sr_customer_sk", "sr_ticket_number", "sr_returned_date_sk", "sr_net_loss"),
                Optional.empty(),
                identityProjections(returnsTypes),
                returnsTypes);
        List<Page> returnDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(betweenInclusive(field(1, INTEGER), 4, 10, INTEGER), equal(2, 2001, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> catalogDatePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_moy", "d_year"),
                Optional.of(and(betweenInclusive(field(1, INTEGER), 4, 10, INTEGER), equal(2, 2001, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_id", "s_store_name"),
                Optional.empty(),
                identityProjections(storeTypes),
                storeTypes);
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_item_id", "i_item_desc"),
                Optional.empty(),
                identityProjections(itemTypes),
                itemTypes);

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(25_100, salesTypes, List.of(0), saleDatePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(25_101, afterSaleDateTypes, List.of(1, 2, 4), returnsPages, returnsTypes, List.of(0, 1, 2))),
                        hashJoinStep(new HashJoinSpec(25_102, afterReturnsTypes, List.of(10), returnDatePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(25_103, afterReturnDateTypes, List.of(2, 1), executePipelinePages(tables.tableFiles("catalog_sales"), List.of("cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", "cs_net_profit"), List.of()), catalogSalesTypes, List.of(0, 1))),
                        hashJoinStep(new HashJoinSpec(25_104, afterCatalogSalesTypes, List.of(15), catalogDatePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(25_105, afterCatalogDateTypes, List.of(3), storePages, storeTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(25_106, afterStoreTypes, List.of(1), itemPages, itemTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                25_107,
                                Optional.empty(),
                                List.of(field(22, itemTypes.get(1)), field(23, itemTypes.get(2)), field(19, storeTypes.get(1)), field(20, storeTypes.get(2)), field(5, salesProfitType), field(11, returnLossType), field(16, catalogProfitType)),
                                List.of(itemTypes.get(1), itemTypes.get(2), storeTypes.get(1), storeTypes.get(2), salesProfitType, returnLossType, catalogProfitType))),
                        factoryStep(hashAggregationFactory(
                                25_108,
                                List.of(itemTypes.get(1), itemTypes.get(2), storeTypes.get(1), storeTypes.get(2)),
                                List.of(0, 1, 2, 3),
                                salesProfitSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                returnLossSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                catalogProfitSum.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()))),
                        factoryStep(topNFactory(25_109, outputTypes, 100, List.of(0, 1, 2, 3), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query38(TpcdsParquetTables tables)
    {
        List<Page> channelPages = new ArrayList<>(query38ChannelPages(tables, "store_sales", "ss_sold_date_sk", "ss_customer_sk", 38_100, 1, 0, 0));
        channelPages.addAll(query38ChannelPages(tables, "catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", 38_200, 0, 1, 0));
        channelPages.addAll(query38ChannelPages(tables, "web_sales", "ws_sold_date_sk", "ws_bill_customer_sk", 38_300, 0, 0, 1));
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_first_name", "c_last_name"));

        return executePagesPipeline(
                channelPages,
                List.of(
                        factoryStep(hashAggregationFactory(
                                38_400,
                                List.of(customerTypes.get(2), customerTypes.get(1), DATE),
                                List.of(0, 1, 2),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                38_401,
                                Optional.of(and(greaterThan(field(3, BIGINT), constant(0L, BIGINT), BIGINT), greaterThan(field(4, BIGINT), constant(0L, BIGINT), BIGINT), greaterThan(field(5, BIGINT), constant(0L, BIGINT), BIGINT))),
                                List.of(),
                                List.of())),
                        factoryStep(aggregationFactory(38_402, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                List.of(BIGINT));
    }

    public MaterializedResult query48(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ss_sold_date_sk", "ss_cdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_quantity", "ss_sales_price", "ss_net_profit");
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", salesColumns);
        Type salesPriceType = salesTypes.get(5);
        Type netProfitType = salesTypes.get(6);
        TestingAggregationFunction quantitySum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
        List<Type> customerDemographicsTypes = tableColumnTypes(tables, "customer_demographics", List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"));
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_country", "ca_state"));

        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> customerDemographicsPages = relationPages(
                tables,
                "customer_demographics",
                List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"),
                Optional.empty(),
                identityProjections(customerDemographicsTypes),
                customerDemographicsTypes);
        List<Page> addressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_country", "ca_state"),
                Optional.of(equal(1, addressTypes.get(1), "United States")),
                List.of(field(0, BIGINT), field(2, addressTypes.get(2))),
                List.of(BIGINT, addressTypes.get(2)));
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(equal(1, 2000, INTEGER)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles("store_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(48_100, salesTypes, List.of(3), storePages, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(48_101, concatTypes(salesTypes, List.of(BIGINT)), List.of(1), customerDemographicsPages, customerDemographicsTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(48_102, concatTypes(concatTypes(salesTypes, List.of(BIGINT)), customerDemographicsTypes), List.of(2), addressPages, List.of(BIGINT, addressTypes.get(2)), List.of(0))),
                        hashJoinStep(new HashJoinSpec(48_103, concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), customerDemographicsTypes), List.of(BIGINT, addressTypes.get(2))), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                48_104,
                                Optional.of(and(
                                        query48DemographicsPredicate(customerDemographicsTypes.get(1), customerDemographicsTypes.get(2), salesPriceType),
                                        query48StateProfitPredicate(addressTypes.get(2), netProfitType))),
                                List.of(cast(field(4, salesTypes.get(4)), salesTypes.get(4), BIGINT)),
                                List.of(BIGINT))),
                        factoryStep(aggregationFactory(48_105, quantitySum.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))),
                List.of(quantitySum.getFinalType()));
    }

    private List<Page> query38ChannelPages(TpcdsParquetTables tables, String factTable, String soldDateColumn, String customerColumn, int pipelineId, long storeMarker, long catalogMarker, long webMarker)
    {
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_first_name", "c_last_name"));
        List<Type> outputTypes = List.of(customerTypes.get(2), customerTypes.get(1), DATE, BIGINT, BIGINT, BIGINT);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date", "d_month_seq"),
                Optional.of(and(
                        greaterThan(field(2, INTEGER), constant(1199L, INTEGER), INTEGER),
                        lessThan(field(2, INTEGER), constant(1212L, INTEGER), INTEGER))),
                List.of(field(0, BIGINT), field(1, DATE)),
                List.of(BIGINT, DATE));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_first_name", "c_last_name"),
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes);

        return executePipelinePages(
                tables.tableFiles(factTable),
                List.of(soldDateColumn, customerColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(pipelineId, List.of(BIGINT, BIGINT), List.of(0), datePages, List.of(BIGINT, DATE), List.of(0))),
                        hashJoinStep(new HashJoinSpec(pipelineId + 1, List.of(BIGINT, BIGINT, BIGINT, DATE), List.of(1), customerPages, customerTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                pipelineId + 2,
                                Optional.empty(),
                                List.of(
                                        field(6, customerTypes.get(2)),
                                        field(5, customerTypes.get(1)),
                                        field(3, DATE),
                                        constant(storeMarker, BIGINT),
                                        constant(catalogMarker, BIGINT),
                                        constant(webMarker, BIGINT)),
                                outputTypes))));
    }

    private List<Page> query02WeeklySalesPages(TpcdsParquetTables tables, int year, int operatorIdBase)
    {
        Type salesType = tableColumnTypes(tables, "web_sales", List.of("ws_ext_sales_price")).getFirst();
        List<Type> outputTypes = List.of(INTEGER, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);

        List<Page> unionPages = new ArrayList<>(executePipelinePages(
                tables.tableFiles("web_sales"),
                List.of("ws_sold_date_sk", "ws_ext_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                List.of(BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_week_seq", "d_day_name", "d_year"),
                                        Optional.of(equal(3, year, INTEGER)),
                                        List.of(field(0, BIGINT), field(1, INTEGER), field(2, VARCHAR)),
                                        List.of(BIGINT, INTEGER, VARCHAR)),
                                List.of(BIGINT, INTEGER, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.empty(),
                                query02BucketProjections(field(3, INTEGER), field(4, VARCHAR), cast(multiply(field(1, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(1, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT)),
                                outputTypes)))));
        unionPages.addAll(executePipelinePages(
                tables.tableFiles("catalog_sales"),
                List.of("cs_sold_date_sk", "cs_ext_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 10,
                                List.of(BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_week_seq", "d_day_name", "d_year"),
                                        Optional.of(equal(3, year, INTEGER)),
                                        List.of(field(0, BIGINT), field(1, INTEGER), field(2, VARCHAR)),
                                        List.of(BIGINT, INTEGER, VARCHAR)),
                                List.of(BIGINT, INTEGER, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 11,
                                Optional.empty(),
                                query02BucketProjections(field(3, INTEGER), field(4, VARCHAR), cast(multiply(field(1, salesType), constant(100L, createDecimalType(3, 0))), multiply(field(1, salesType), constant(100L, createDecimalType(3, 0))).type(), BIGINT)),
                                outputTypes)))));
        return executePipelinePages(
                unionPages,
                List.of(factoryStep(hashAggregationFactory(
                        operatorIdBase + 20,
                        List.of(INTEGER),
                        List.of(0),
                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty())))));
    }

    public MaterializedResult query14(TpcdsParquetTables tables)
    {
        List<Page> crossItemPages = query14CrossItemPages(tables);
        List<Type> crossItemTypes = List.of(BIGINT);
        List<Page> averageSalesPages = query14AverageSalesPages(tables);
        Type averageType = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(createDecimalType(18, 2))).getFinalType();

        List<Page> unionPages = new ArrayList<>(query14ChannelBranchPages(
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_quantity",
                "ss_list_price",
                "store",
                14_100,
                crossItemPages,
                crossItemTypes,
                averageSalesPages,
                averageType));
        unionPages.addAll(query14ChannelBranchPages(
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_quantity",
                "cs_list_price",
                "catalog",
                14_200,
                crossItemPages,
                crossItemTypes,
                averageSalesPages,
                averageType));
        unionPages.addAll(query14ChannelBranchPages(
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_item_sk",
                "ws_quantity",
                "ws_list_price",
                "web",
                14_300,
                crossItemPages,
                crossItemTypes,
                averageSalesPages,
                averageType));

        Type channelType = VARCHAR;
        Type salesType = createDecimalType(38, 2);
        List<Type> branchTypes = List.of(channelType, INTEGER, INTEGER, INTEGER, salesType, BIGINT);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction countSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
        List<Type> groupIdTypes = concatTypes(branchTypes, List.of(BIGINT));
        List<Type> outputTypes = List.of(channelType, INTEGER, INTEGER, INTEGER, salesSum.getFinalType(), countSum.getFinalType());

        return executePagesPipeline(
                unionPages,
                List.of(
                        factoryStep(groupIdFactory(
                                14_400,
                                groupIdTypes,
                                List.of(
                                        Map.of(4, 4, 5, 5),
                                        Map.of(0, 0, 4, 4, 5, 5),
                                        Map.of(0, 0, 1, 1, 4, 4, 5, 5),
                                        Map.of(0, 0, 1, 1, 2, 2, 4, 4, 5, 5),
                                        Map.of(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5)))),
                        factoryStep(hashAggregationFactory(
                                14_401,
                                List.of(channelType, INTEGER, INTEGER, INTEGER, BIGINT),
                                List.of(0, 1, 2, 3, 6),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                countSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                14_402,
                                Optional.empty(),
                                List.of(field(0, channelType), field(1, INTEGER), field(2, INTEGER), field(3, INTEGER), field(5, salesSum.getFinalType()), field(6, countSum.getFinalType())),
                                outputTypes)),
                        factoryStep(topNFactory(14_403, outputTypes, 100, List.of(0, 1, 2, 3), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query02(TpcdsParquetTables tables)
    {
        Type weekSequenceType = tableColumnTypes(tables, "date_dim", List.of("d_week_seq")).getFirst();
        List<Type> weeklyTypes = List.of(weekSequenceType, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> adjustedTypes = List.of(weekSequenceType, weekSequenceType, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(weekSequenceType, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);

        List<Page> currentYearPages = executePipelinePages(
                query02WeeklySalesPages(tables, 2001, 2_100),
                List.of(factoryStep(filterAndProjectFactory(
                        2_110,
                        Optional.empty(),
                        List.of(
                                add(field(0, weekSequenceType), constant(53L, weekSequenceType), weekSequenceType),
                                field(0, weekSequenceType),
                                field(1, BIGINT),
                                field(2, BIGINT),
                                field(3, BIGINT),
                                field(4, BIGINT),
                                field(5, BIGINT),
                                field(6, BIGINT),
                                field(7, BIGINT)),
                        adjustedTypes))));
        List<Page> nextYearPages = query02WeeklySalesPages(tables, 2002, 2_200);

        RowExpression hundred = constant(100L, BIGINT);
        return executePagesPipeline(
                currentYearPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(2_300, adjustedTypes, List.of(0), nextYearPages, weeklyTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                2_301,
                                Optional.empty(),
                                List.of(
                                        field(1, weekSequenceType),
                                        query02RatioExpression(2, 10, hundred),
                                        query02RatioExpression(3, 11, hundred),
                                        query02RatioExpression(4, 12, hundred),
                                        query02RatioExpression(5, 13, hundred),
                                        query02RatioExpression(6, 14, hundred),
                                        query02RatioExpression(7, 15, hundred),
                                        query02RatioExpression(8, 16, hundred)),
                                outputTypes)),
                        factoryStep(topNFactory(2_302, outputTypes, 10_000, List.of(0), List.of(ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query15(TpcdsParquetTables tables)
    {
        List<String> factColumns = List.of("cs_sold_date_sk", "cs_bill_customer_sk", "cs_sales_price");
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", factColumns);
        Type salesType = factTypes.get(2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk"));
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_zip", "ca_state"));
        List<Type> outputTypes = List.of(addressTypes.get(1), salesSum.getFinalType());

        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk"),
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes);
        List<Page> addressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_zip", "ca_state"),
                Optional.empty(),
                identityProjections(addressTypes),
                addressTypes);
        List<Page> datePages = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_qoy", "d_year"),
                Optional.of(and(equal(1, 2, INTEGER), equal(2, 2001, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipeline(
                tables.tableFiles("catalog_sales"),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(15_100, factTypes, List.of(1), customerPages, customerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(15_101, concatTypes(factTypes, customerTypes), List.of(4), addressPages, addressTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(15_102, concatTypes(concatTypes(factTypes, customerTypes), addressTypes), List.of(0), datePages, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                15_103,
                                Optional.of(query15ZipStateOrPricePredicate(addressTypes.get(1), addressTypes.get(2), salesType)),
                                List.of(field(6, addressTypes.get(1)), field(2, salesType)),
                                List.of(addressTypes.get(1), salesType))),
                        factoryStep(hashAggregationFactory(
                                15_104,
                                List.of(addressTypes.get(1)),
                                List.of(0),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        factoryStep(topNFactory(15_105, outputTypes, 100, List.of(0), List.of(ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query81(TpcdsParquetTables tables)
    {
        Type stateType = query81StateType(tables);
        List<Page> customerTotalReturnPages = query81CustomerTotalReturnPages(tables, 81_100);
        List<Type> customerTotalReturnTypes = List.of(BIGINT, stateType, query81ReturnSumType(tables));
        List<Page> stateAveragePages = query81StateAveragePages(tables);
        List<Type> stateAverageTypes = List.of(stateType, query81StateAverageType(tables));

        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_current_addr_sk", "c_customer_id", "c_salutation", "c_first_name", "c_last_name"));
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_current_addr_sk", "c_customer_id", "c_salutation", "c_first_name", "c_last_name"),
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes);
        List<Type> gaAddressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_street_number", "ca_street_name", "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_state", "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"));
        List<Page> gaAddressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_street_number", "ca_street_name", "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_state", "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"),
                Optional.of(equal(7, gaAddressTypes.get(7), "GA")),
                identityProjections(gaAddressTypes),
                gaAddressTypes);

        List<Type> afterAverageTypes = concatTypes(customerTotalReturnTypes, stateAverageTypes);
        List<Type> afterCustomerTypes = concatTypes(afterAverageTypes, customerTypes);
        List<Type> outputTypes = List.of(
                customerTypes.get(2),
                customerTypes.get(3),
                customerTypes.get(4),
                customerTypes.get(5),
                gaAddressTypes.get(1),
                gaAddressTypes.get(2),
                gaAddressTypes.get(3),
                gaAddressTypes.get(4),
                gaAddressTypes.get(5),
                gaAddressTypes.get(6),
                gaAddressTypes.get(7),
                gaAddressTypes.get(8),
                gaAddressTypes.get(9),
                gaAddressTypes.get(10),
                gaAddressTypes.get(11),
                customerTotalReturnTypes.get(2));

        return executePagesPipeline(
                customerTotalReturnPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(81_120, customerTotalReturnTypes, List.of(1), stateAveragePages, stateAverageTypes, List.of(0), JoinOperatorType.probeOuterJoin(false))),
                        hashJoinStep(new HashJoinSpec(81_121, afterAverageTypes, List.of(0), customerPages, customerTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(81_122, afterCustomerTypes, List.of(6), gaAddressPages, gaAddressTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                81_123,
                                Optional.of(query81ThresholdPredicate(customerTotalReturnTypes.get(2), stateAverageTypes.get(1))),
                                List.of(
                                        field(7, customerTypes.get(2)),
                                        field(8, customerTypes.get(3)),
                                        field(9, customerTypes.get(4)),
                                        field(10, customerTypes.get(5)),
                                        field(12, gaAddressTypes.get(1)),
                                        field(13, gaAddressTypes.get(2)),
                                        field(14, gaAddressTypes.get(3)),
                                        field(15, gaAddressTypes.get(4)),
                                        field(16, gaAddressTypes.get(5)),
                                        field(17, gaAddressTypes.get(6)),
                                        field(18, gaAddressTypes.get(7)),
                                        field(19, gaAddressTypes.get(8)),
                                        field(20, gaAddressTypes.get(9)),
                                        field(21, gaAddressTypes.get(10)),
                                        field(22, gaAddressTypes.get(11)),
                                        field(2, customerTotalReturnTypes.get(2))),
                                outputTypes)),
                        factoryStep(topNFactory(81_124, outputTypes, 100, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query83(TpcdsParquetTables tables)
    {
        List<Page> storePages = query83ChannelPages(tables, "store_returns", "sr_item_sk", "sr_returned_date_sk", "sr_return_quantity", 83_00);
        List<Page> catalogPages = query83ChannelPages(tables, "catalog_returns", "cr_item_sk", "cr_returned_date_sk", "cr_return_quantity", 83_10);
        List<Page> webPages = query83ChannelPages(tables, "web_returns", "wr_item_sk", "wr_returned_date_sk", "wr_return_quantity", 83_20);

        Type itemIdType = tableColumnTypes(tables, "item", List.of("i_item_id")).getFirst();
        List<Type> channelTypes = List.of(itemIdType, BIGINT);
        List<Type> joinedTypes = List.of(itemIdType, BIGINT, BIGINT, BIGINT);
        MaterializedResult topRows = executePagesPipeline(
                storePages,
                List.of(
                        hashJoinStep(new HashJoinSpec(83_30, channelTypes, List.of(0), catalogPages, channelTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(83_31, concatTypes(channelTypes, channelTypes), List.of(0), webPages, channelTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                83_32,
                                Optional.empty(),
                                List.of(field(0, itemIdType), field(1, BIGINT), field(3, BIGINT), field(5, BIGINT)),
                                joinedTypes)),
                        factoryStep(topNFactory(83_33, joinedTypes, 100, List.of(0, 1), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                joinedTypes);

        Type percentType = createDecimalType(7, 2);
        Type averageType = createDecimalType(38, 4);
        List<Type> outputTypes = List.of(itemIdType, BIGINT, percentType, BIGINT, percentType, BIGINT, percentType, averageType);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (io.trino.testing.MaterializedRow row : topRows.getMaterializedRows()) {
            Number storeField = (Number) row.getField(1);
            Number catalogField = (Number) row.getField(2);
            Number webField = (Number) row.getField(3);
            long storeQuantity = storeField == null ? 0 : storeField.longValue();
            long catalogQuantity = catalogField == null ? 0 : catalogField.longValue();
            long webQuantity = webField == null ? 0 : webField.longValue();
            long totalQuantity = storeQuantity + catalogQuantity + webQuantity;
            long denominator = totalQuantity * 3L;

            result.row(
                    row.getField(0),
                    storeField == null ? null : storeQuantity,
                    storeField == null ? null : BigDecimal.valueOf(roundDivide(storeQuantity * 10_000L, denominator), 2),
                    catalogField == null ? null : catalogQuantity,
                    catalogField == null ? null : BigDecimal.valueOf(roundDivide(catalogQuantity * 10_000L, denominator), 2),
                    webField == null ? null : webQuantity,
                    webField == null ? null : BigDecimal.valueOf(roundDivide(webQuantity * 10_000L, denominator), 2),
                    BigDecimal.valueOf(roundDivide(totalQuantity * 10_000L, 3L), 4));
        }
        return result.build();
    }

    public MaterializedResult query97(TpcdsParquetTables tables)
    {
        List<Type> keyTypes = List.of(BIGINT, BIGINT);
        List<Page> storeKeys = query97ChannelPages(tables, "store_sales", "ss_customer_sk", "ss_item_sk", "ss_sold_date_sk", 97_100);
        List<Page> catalogKeys = query97ChannelPages(tables, "catalog_sales", "cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", 97_200);
        TestingAggregationFunction sum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
        List<Type> indicatorTypes = List.of(BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(sum.getFinalType(), sum.getFinalType(), sum.getFinalType());

        return executePagesPipeline(
                executeHashJoinPages(
                        storeKeys,
                        new HashJoinSpec(
                                97_0,
                                keyTypes,
                                List.of(0, 1),
                                catalogKeys,
                                keyTypes,
                                List.of(0, 1),
                                JoinOperatorType.fullOuterJoin())),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                97_1,
                                Optional.empty(),
                                List.of(
                                        ifExpression(
                                                new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(0, BIGINT)), List.of()),
                                                constant(0L, BIGINT),
                                                ifExpression(
                                                        new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(2, BIGINT)), List.of()),
                                                        constant(1L, BIGINT),
                                                        constant(0L, BIGINT),
                                                        BIGINT),
                                                BIGINT),
                                        ifExpression(
                                                new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(0, BIGINT)), List.of()),
                                                ifExpression(
                                                        new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(2, BIGINT)), List.of()),
                                                        constant(0L, BIGINT),
                                                        constant(1L, BIGINT),
                                                        BIGINT),
                                                constant(0L, BIGINT),
                                                BIGINT),
                                        ifExpression(
                                                new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(0, BIGINT)), List.of()),
                                                constant(0L, BIGINT),
                                                ifExpression(
                                                        new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(2, BIGINT)), List.of()),
                                                        constant(0L, BIGINT),
                                                        constant(1L, BIGINT),
                                                        BIGINT),
                                                BIGINT)),
                                indicatorTypes)),
                        factoryStep(aggregationFactory(
                                97_2,
                                sum.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()),
                                sum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                sum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))),
                outputTypes);
    }

    public MaterializedResult query73(TpcdsParquetTables tables)
    {
        List<Type> projectedTypes = List.of(BIGINT, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT);
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk"));
        List<Type> afterDateTypes = concatTypes(factTypes, List.of(BIGINT));
        List<Type> afterStoreTypes = concatTypes(afterDateTypes, List.of(BIGINT));
        List<Type> afterHouseholdTypes = concatTypes(afterStoreTypes, List.of(BIGINT));
        List<Page> allowedDateKeys = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_dom", "d_year"),
                Optional.of(dayOfMonthAndYearsPredicate(1, 2, 1, 2, 1999, 2000, 2001)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> allowedStoreKeys = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_county"),
                Optional.of(varcharAnyOf(1, Set.of("Williamson County", "Franklin Parish", "Bronx County", "Orange County"))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> allowedHouseholdKeys = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"),
                Optional.of(query73HouseholdPredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> customerIdentityPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag"),
                Optional.empty(),
                identityProjections(List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR)),
                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR));
        OperatorFactory groupedCount = hashAggregationFactory(63, projectedTypes, List.of(0, 1), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()));
        OperatorFactory countFilter = filterAndProjectFactory(64, Optional.of(and(greaterThan(2, 0, BIGINT), lessThan(2, 6, BIGINT))), identityProjections(List.of(BIGINT, BIGINT, BIGINT)), List.of(BIGINT, BIGINT, BIGINT));
        OperatorFactory finalProjection = filterAndProjectFactory(
                66,
                Optional.empty(),
                List.of(
                        field(4, VARCHAR),
                        field(5, VARCHAR),
                        field(6, VARCHAR),
                        field(7, VARCHAR),
                        field(0, BIGINT),
                        field(2, BIGINT)),
                outputTypes);
        OperatorFactory topN = topNFactory(67, outputTypes, 100, List.of(5, 0, 4), List.of(io.trino.spi.connector.SortOrder.DESC_NULLS_LAST, ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        List<PipelineStep> steps = new ArrayList<>();
        steps.add(hashJoinStep(new HashJoinSpec(60, factTypes, List.of(2), allowedDateKeys, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(61, afterDateTypes, List.of(3), allowedStoreKeys, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(62, afterStoreTypes, List.of(4), allowedHouseholdKeys, List.of(BIGINT), List.of(0))));
        steps.add(factoryStep(groupedCount));
        steps.add(factoryStep(countFilter));
        steps.add(hashJoinStep(new HashJoinSpec(65, List.of(BIGINT, BIGINT, BIGINT), List.of(1), customerIdentityPages, List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR), List.of(0))));
        steps.add(factoryStep(finalProjection));
        steps.add(factoryStep(topN));
        return executePipeline(tables.tableFiles("store_sales"), List.of("ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk"), steps, outputTypes);
    }

    public MaterializedResult query74(TpcdsParquetTables tables)
    {
        List<Page> storeFirstYearPages = query74ChannelYearTotalPages(tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_net_paid", 2001, 74_100);
        List<Page> storeSecondYearPages = query74ChannelYearTotalPages(tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_net_paid", 2002, 74_110);
        List<Page> webFirstYearPages = query74ChannelYearTotalPages(tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_net_paid", 2001, 74_120);
        List<Page> webSecondYearPages = query74ChannelYearTotalPages(tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_net_paid", 2002, 74_130);

        List<Type> branchTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, VARCHAR);

        return executePagesPipeline(
                storeFirstYearPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(74_140, branchTypes, List.of(0), storeSecondYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(74_141, concatTypes(branchTypes, branchTypes), List.of(0), webFirstYearPages, branchTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(74_142, concatTypes(concatTypes(branchTypes, branchTypes), branchTypes), List.of(0), webSecondYearPages, branchTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                74_143,
                                Optional.of(query74GrowthPredicate()),
                                List.of(field(0, VARCHAR), field(1, VARCHAR), field(2, VARCHAR)),
                                outputTypes)),
                        factoryStep(topNFactory(74_144, outputTypes, 100, List.of(0, 1, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query75(TpcdsParquetTables tables)
    {
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_category", "i_brand_id", "i_class_id", "i_category_id", "i_manufact_id"));
        List<Type> allSalesTypes = List.of(INTEGER, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5), BIGINT, BIGINT);
        List<Type> outputTypes = List.of(INTEGER, INTEGER, itemTypes.get(2), itemTypes.get(3), itemTypes.get(4), itemTypes.get(5), BIGINT, BIGINT, BIGINT, BIGINT);
        List<Page> currentYearPages = query75AllSalesPages(tables, itemTypes, 75_100, 2002);
        List<Page> previousYearPages = query75AllSalesPages(tables, itemTypes, 75_500, 2001);

        RowExpression scaledCurrentCount = multiply(field(5, BIGINT), constant(10L, BIGINT), BIGINT);
        RowExpression scaledPreviousCount = multiply(field(12, BIGINT), constant(9L, BIGINT), BIGINT);

        return executePagesPipeline(
                currentYearPages,
                List.of(
                        hashJoinStep(new HashJoinSpec(75_420, allSalesTypes, List.of(1, 2, 3, 4), previousYearPages, allSalesTypes, List.of(1, 2, 3, 4))),
                        factoryStep(filterAndProjectFactory(
                                75_421,
                                Optional.of(lessThan(scaledCurrentCount, scaledPreviousCount, BIGINT)),
                                List.of(
                                        field(7, INTEGER),
                                        field(0, INTEGER),
                                        field(1, itemTypes.get(2)),
                                        field(2, itemTypes.get(3)),
                                        field(3, itemTypes.get(4)),
                                        field(4, itemTypes.get(5)),
                                        field(12, BIGINT),
                                        field(5, BIGINT),
                                        subtract(field(5, BIGINT), field(12, BIGINT), BIGINT),
                                        subtract(field(6, BIGINT), field(13, BIGINT), BIGINT)),
                                outputTypes)),
                        factoryStep(topNFactory(75_422, outputTypes, 100, List.of(8, 9), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query78(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (org.weakref.nitro.data.Row row : TpcdsManualQuerySupport.query78Rows(new Allocator(), tables)) {
            result.row(row.values());
        }
        return result.build();
    }

    public MaterializedResult query84(TpcdsParquetTables tables)
    {
        List<Type> customerTypes = List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT);
        List<Type> afterAddressTypes = concatTypes(customerTypes, List.of(BIGINT));
        List<Type> afterHouseholdTypes = concatTypes(afterAddressTypes, List.of(BIGINT));
        List<Type> afterCustomerDemographicsTypes = concatTypes(afterHouseholdTypes, List.of(BIGINT));
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR);
        List<Page> eligibleAddressKeys = relationPages(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_city"),
                Optional.of(equal(1, VARCHAR, "Edgewood")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> eligibleIncomeBandKeys = relationPages(
                tables,
                "income_band",
                List.of("ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"),
                Optional.of(and(greaterThan(1, 38127, INTEGER), lessThan(2, 88129, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> eligibleHouseholdKeys = executePipelinePages(
                tables.tableFiles("household_demographics"),
                List.of("hd_demo_sk", "hd_income_band_sk"),
                List.of(
                        hashJoinStep(new HashJoinSpec(84_10, List.of(BIGINT, BIGINT), List.of(1), eligibleIncomeBandKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(84_11, Optional.of(greaterThan(field(0, BIGINT), constant(0L, BIGINT), BIGINT)), List.of(field(0, BIGINT)), List.of(BIGINT)))));
        List<Page> customerDemographicKeys = relationPages(
                tables,
                "customer_demographics",
                List.of("cd_demo_sk"),
                Optional.of(greaterThan(field(0, BIGINT), constant(0L, BIGINT), BIGINT)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> storeReturnCustomerDemographics = relationPages(
                tables,
                "store_returns",
                List.of("sr_cdemo_sk"),
                Optional.of(greaterThan(field(0, BIGINT), constant(0L, BIGINT), BIGINT)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<PipelineStep> steps = new ArrayList<>();
        steps.add(hashJoinStep(new HashJoinSpec(84_0, customerTypes, List.of(3), eligibleAddressKeys, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(84_1, afterAddressTypes, List.of(5), eligibleHouseholdKeys, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(84_2, afterHouseholdTypes, List.of(4), customerDemographicKeys, List.of(BIGINT), List.of(0))));
        steps.add(hashJoinStep(new HashJoinSpec(84_3, afterCustomerDemographicsTypes, List.of(4), storeReturnCustomerDemographics, List.of(BIGINT), List.of(0))));
        steps.add(factoryStep(filterAndProjectFactory(
                84_4,
                Optional.empty(),
                List.of(
                        field(0, VARCHAR),
                        concat(concat(field(1, VARCHAR), constant(Slices.utf8Slice(", "), VARCHAR)), field(2, VARCHAR))),
                outputTypes)));
        steps.add(factoryStep(topNFactory(84_5, outputTypes, 100, List.of(0), List.of(ASC_NULLS_LAST))));
        return executePipeline(
                tables.tableFiles("customer"),
                List.of("c_customer_id", "c_last_name", "c_first_name", "c_current_addr_sk", "c_current_cdemo_sk", "c_current_hdemo_sk"),
                steps,
                outputTypes);
    }

    public MaterializedResult query85(TpcdsParquetTables tables)
    {
        List<String> salesColumns = List.of("ws_web_page_sk", "ws_item_sk", "ws_order_number", "ws_sold_date_sk", "ws_quantity", "ws_sales_price", "ws_net_profit");
        List<Type> salesTypes = tableColumnTypes(tables, "web_sales", salesColumns);
        Type quantityType = salesTypes.get(4);
        Type salesPriceType = salesTypes.get(5);
        Type netProfitType = salesTypes.get(6);
        List<Type> returnsTypes = tableColumnTypes(tables, "web_returns", List.of("wr_item_sk", "wr_order_number", "wr_refunded_cdemo_sk", "wr_returning_cdemo_sk", "wr_refunded_addr_sk", "wr_reason_sk", "wr_refunded_cash", "wr_fee"));
        Type refundedCashType = returnsTypes.get(6);
        Type feeType = returnsTypes.get(7);
        List<Type> outputTypes = List.of(VARCHAR, BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType(), BIGINT_AVG.getFinalType());

        RowExpression quantityValue = cast(field(4, quantityType), quantityType, BIGINT);
        RowExpression refundedCashValue = cast(multiply(field(14, refundedCashType), constant(100L, createDecimalType(3, 0))), multiply(field(14, refundedCashType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);
        RowExpression feeValue = cast(multiply(field(15, feeType), constant(100L, createDecimalType(3, 0))), multiply(field(15, feeType), constant(100L, createDecimalType(3, 0))).type(), BIGINT);

        return executePipeline(
                tables.tableFiles("web_sales"),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(85_100, salesTypes, List.of(0), relationPages(tables, "web_page", List.of("wp_web_page_sk"), Optional.empty(), List.of(field(0, BIGINT)), List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(85_101, concatTypes(salesTypes, List.of(BIGINT)), List.of(1, 2), relationPages(tables, "web_returns", List.of("wr_item_sk", "wr_order_number", "wr_refunded_cdemo_sk", "wr_returning_cdemo_sk", "wr_refunded_addr_sk", "wr_reason_sk", "wr_refunded_cash", "wr_fee"), Optional.empty(), identityProjections(returnsTypes), returnsTypes), returnsTypes, List.of(0, 1))),
                        hashJoinStep(new HashJoinSpec(85_102, concatTypes(concatTypes(salesTypes, List.of(BIGINT)), returnsTypes), List.of(3), relationPages(tables, "date_dim", List.of("d_date_sk", "d_year"), Optional.of(equal(1, 2000, INTEGER)), List.of(field(0, BIGINT)), List.of(BIGINT)), List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(85_103, concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), returnsTypes), List.of(BIGINT)), List.of(10), relationPages(tables, "customer_demographics", List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"), Optional.empty(), identityProjections(List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(85_104, concatTypes(concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), returnsTypes), List.of(BIGINT)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(11), relationPages(tables, "customer_demographics", List.of("cd_demo_sk", "cd_marital_status", "cd_education_status"), Optional.empty(), identityProjections(List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(85_105, concatTypes(concatTypes(concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), returnsTypes), List.of(BIGINT)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(12), relationPages(tables, "customer_address", List.of("ca_address_sk", "ca_country", "ca_state"), Optional.empty(), identityProjections(List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR), List.of(0))),
                        hashJoinStep(new HashJoinSpec(85_106, concatTypes(concatTypes(concatTypes(concatTypes(concatTypes(concatTypes(salesTypes, List.of(BIGINT)), returnsTypes), List.of(BIGINT)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(BIGINT, VARCHAR, VARCHAR)), List.of(13), relationPages(tables, "reason", List.of("r_reason_sk", "r_reason_desc"), Optional.empty(), identityProjections(List.of(BIGINT, VARCHAR)), List.of(BIGINT, VARCHAR)), List.of(BIGINT, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                85_107,
                                Optional.of(and(query85DemographicsAndPricePredicate(salesPriceType), query85AddressProfitPredicate(netProfitType))),
                                List.of(substring(field(27, VARCHAR), 1, 20), quantityValue, refundedCashValue, feeValue),
                                List.of(VARCHAR, BIGINT, BIGINT, BIGINT))),
                        factoryStep(hashAggregationFactory(
                                85_108,
                                List.of(VARCHAR),
                                List.of(0),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        factoryStep(topNFactory(85_109, outputTypes, 100, List.of(0, 1, 2, 3), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query90(TpcdsParquetTables tables)
    {
        List<Page> householdKeys = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_dep_count"),
                Optional.of(equal(1, 6, INTEGER)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> pageKeys = relationPages(
                tables,
                "web_page",
                List.of("wp_web_page_sk", "wp_char_count"),
                Optional.of(and(greaterThan(1, 4999, INTEGER), lessThan(1, 5201, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> morningTimeKeys = relationPages(
                tables,
                "time_dim",
                List.of("t_time_sk", "t_hour"),
                Optional.of(and(greaterThan(1, 7, INTEGER), lessThan(1, 10, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> eveningTimeKeys = relationPages(
                tables,
                "time_dim",
                List.of("t_time_sk", "t_hour"),
                Optional.of(and(greaterThan(1, 18, INTEGER), lessThan(1, 21, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        double ratio = (double) query90Count(tables, morningTimeKeys, householdKeys, pageKeys) /
                query90Count(tables, eveningTimeKeys, householdKeys, pageKeys);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(TestingSession.testSessionBuilder().build(), List.of(DOUBLE));
        result.row(ratio);
        return result.build();
    }

    public MaterializedResult query87(TpcdsParquetTables tables)
    {
        List<Page> unionPages = new ArrayList<>(query87ChannelPresencePages(tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", 0, 87_100));
        unionPages.addAll(query87ChannelPresencePages(tables, "catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", 1, 87_200));
        unionPages.addAll(query87ChannelPresencePages(tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", 2, 87_300));

        List<Type> groupedTypes = List.of(VARCHAR, VARCHAR, DATE, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(BIGINT);

        return executePagesPipeline(
                unionPages,
                List.of(
                        factoryStep(hashAggregationFactory(
                                87_400,
                                List.of(VARCHAR, VARCHAR, DATE),
                                List.of(0, 1, 2),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                87_401,
                                Optional.of(and(
                                        greaterThan(field(3, BIGINT), constant(0L, BIGINT), BIGINT),
                                        equal(field(4, BIGINT), constant(0L, BIGINT), BIGINT),
                                        equal(field(5, BIGINT), constant(0L, BIGINT), BIGINT))),
                                identityProjections(groupedTypes),
                                groupedTypes)),
                        factoryStep(aggregationFactory(87_402, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                outputTypes);
    }

    public MaterializedResult query92(TpcdsParquetTables tables)
    {
        Type discountType = tableColumnTypes(tables, "web_sales", List.of("ws_ext_discount_amt")).getFirst();
        Type averageType = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(discountType)).getFinalType();
        TestingAggregationFunction discountSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(discountType));
        List<Type> filteredTypes = List.of(BIGINT, discountType);
        List<Type> averagePagesTypes = List.of(BIGINT, averageType);
        List<Type> joinedTypes = List.of(BIGINT, discountType, BIGINT, averageType);
        List<Type> outputTypes = List.of(discountSum.getFinalType());

        return executePagesPipeline(
                query92FilteredDiscountPages(tables, 92_100),
                List.of(
                        hashJoinStep(new HashJoinSpec(92_120, filteredTypes, List.of(0), query92ItemAveragePages(tables), averagePagesTypes, List.of(0), JoinOperatorType.probeOuterJoin(false))),
                        factoryStep(filterAndProjectFactory(
                                92_121,
                                Optional.of(query92DiscountThresholdPredicate(discountType, averageType)),
                                List.of(field(1, discountType)),
                                List.of(discountType))),
                        factoryStep(aggregationFactory(
                                92_122,
                                discountSum.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))),
                outputTypes);
    }

    public MaterializedResult query93(TpcdsParquetTables tables)
    {
        List<Page> grouped = query93GroupedRowsPages(tables);
        List<Type> outputTypes = query93OutputTypes(tables);
        return executePagesPipeline(
                grouped,
                List.of(factoryStep(topNFactory(93_104, outputTypes, 100, List.of(1, 0), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                outputTypes);
    }

    public MaterializedResult query93GroupedRows(TpcdsParquetTables tables)
    {
        return executePagesPipeline(query93GroupedRowsPages(tables), List.of(), query93OutputTypes(tables));
    }

    public MaterializedResult query93ProjectedRows(TpcdsParquetTables tables)
    {
        return executePagesPipeline(query93ProjectedRowsPages(tables), List.of(), query93ProjectedOutputTypes(tables));
    }

    private List<Page> query93GroupedRowsPages(TpcdsParquetTables tables)
    {
        RowExpression sales = query93SalesExpression(tables);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(sales.type()));
        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_item_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(93_100, query93SalesTypes(tables), List.of(0, 2), query93ReturnsPages(tables), query93ReturnsTypes(tables), List.of(0, 2))),
                        hashJoinStep(new HashJoinSpec(93_101, query93AfterReturnsTypes(tables), List.of(6), query93ReasonPages(tables), List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                93_102,
                                Optional.empty(),
                                List.of(field(1, BIGINT), sales),
                                List.of(BIGINT, sales.type()))),
                        factoryStep(hashAggregationFactory(
                                93_103,
                                List.of(BIGINT),
                                List.of(0),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))));
    }

    private List<Page> query93ProjectedRowsPages(TpcdsParquetTables tables)
    {
        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_item_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity", "ss_sales_price"),
                List.of(
                        hashJoinStep(new HashJoinSpec(93_100, query93SalesTypes(tables), List.of(0, 2), query93ReturnsPages(tables), query93ReturnsTypes(tables), List.of(0, 2))),
                        hashJoinStep(new HashJoinSpec(93_101, query93AfterReturnsTypes(tables), List.of(6), query93ReasonPages(tables), List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                93_102,
                                Optional.empty(),
                                List.of(field(1, BIGINT), query93SalesExpression(tables)),
                                query93ProjectedOutputTypes(tables)))));
    }

    private List<Type> query93OutputTypes(TpcdsParquetTables tables)
    {
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(query93SalesExpression(tables).type()));
        return List.of(BIGINT, salesSum.getFinalType());
    }

    private List<Type> query93ProjectedOutputTypes(TpcdsParquetTables tables)
    {
        return List.of(BIGINT, query93SalesExpression(tables).type());
    }

    private List<Type> query93SalesTypes(TpcdsParquetTables tables)
    {
        return tableColumnTypes(tables, "store_sales", List.of("ss_item_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity", "ss_sales_price"));
    }

    private List<Type> query93ReturnsTypes(TpcdsParquetTables tables)
    {
        return tableColumnTypes(tables, "store_returns", List.of("sr_item_sk", "sr_reason_sk", "sr_ticket_number", "sr_return_quantity"));
    }

    private List<Type> query93AfterReturnsTypes(TpcdsParquetTables tables)
    {
        return concatTypes(query93SalesTypes(tables), query93ReturnsTypes(tables));
    }

    private List<Page> query93ReturnsPages(TpcdsParquetTables tables)
    {
        List<Type> returnsTypes = query93ReturnsTypes(tables);
        return relationPages(
                tables,
                "store_returns",
                List.of("sr_item_sk", "sr_reason_sk", "sr_ticket_number", "sr_return_quantity"),
                Optional.empty(),
                identityProjections(returnsTypes),
                returnsTypes);
    }

    private List<Page> query93ReasonPages(TpcdsParquetTables tables)
    {
        List<Type> reasonTypes = tableColumnTypes(tables, "reason", List.of("r_reason_sk", "r_reason_desc"));
        return relationPages(
                tables,
                "reason",
                List.of("r_reason_sk", "r_reason_desc"),
                Optional.of(equal(1, reasonTypes.get(1), "reason 28")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
    }

    private RowExpression query93SalesExpression(TpcdsParquetTables tables)
    {
        List<Type> salesTypes = query93SalesTypes(tables);
        Type quantityType = salesTypes.get(3);
        Type salesPriceType = salesTypes.get(4);
        Type returnQuantityType = tableColumnTypes(tables, "store_returns", List.of("sr_return_quantity")).getFirst();
        Type quantityDecimalType = createDecimalType(10, 0);
        RowExpression fullSales = multiply(
                cast(field(3, quantityType), quantityType, quantityDecimalType),
                field(4, salesPriceType));
        RowExpression adjustedSales = multiply(
                cast(subtract(field(3, quantityType), cast(field(8, returnQuantityType), returnQuantityType, quantityType), quantityType), quantityType, quantityDecimalType),
                field(4, salesPriceType));
        return ifExpression(
                new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(8, returnQuantityType)), List.of()),
                fullSales,
                adjustedSales,
                fullSales.type());
    }

    public MaterializedResult query88(TpcdsParquetTables tables)
    {
        long[] counts = new long[8];
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"));
        List<Type> afterTimeTypes = concatTypes(factTypes, List.of(BIGINT));
        List<Type> afterHouseholdTypes = concatTypes(afterTimeTypes, List.of(BIGINT));
        List<Page> allowedHouseholdKeys = relationPages(
                tables,
                "household_demographics",
                List.of("hd_demo_sk", "hd_dep_count", "hd_vehicle_count"),
                Optional.of(query88HouseholdPredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> allowedStoreKeys = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_store_name"),
                Optional.of(equal(1, VARCHAR, "ese")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        for (int bucket = 0; bucket < counts.length; bucket++) {
            List<Page> timeBucketKeys = relationPages(
                    tables,
                    "time_dim",
                    List.of("t_time_sk", "t_hour", "t_minute"),
                    Optional.of(query88TimeBucketPredicate(bucket)),
                    List.of(field(0, BIGINT)),
                    List.of(BIGINT));
            List<PipelineStep> steps = List.of(
                    hashJoinStep(new HashJoinSpec(70 + (bucket * 4), factTypes, List.of(0), timeBucketKeys, List.of(BIGINT), List.of(0))),
                    hashJoinStep(new HashJoinSpec(71 + (bucket * 4), afterTimeTypes, List.of(1), allowedHouseholdKeys, List.of(BIGINT), List.of(0))),
                    hashJoinStep(new HashJoinSpec(72 + (bucket * 4), afterHouseholdTypes, List.of(2), allowedStoreKeys, List.of(BIGINT), List.of(0))),
                    factoryStep(aggregationFactory(73 + (bucket * 4), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))));
            counts[bucket] = singleLongResult(executePipeline(
                    tables.tableFiles("store_sales"),
                    List.of("ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"),
                    steps,
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
        return executePipeline(
                files,
                columns,
                factories.stream()
                        .map(TrinoTpcdsParquetSupport::factoryStep)
                        .toList(),
                outputTypes);
    }

    private MaterializedResult executePagesPipeline(List<Page> inputPages, List<PipelineStep> steps, List<Type> outputTypes)
    {
        List<Page> outputPages = executePipelinePages(inputPages, steps);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (Page page : outputPages) {
            result.page(page);
        }
        return result.build();
    }

    private MaterializedResult executePipeline(List<Path> files, List<String> columns, List<PipelineStep> steps, List<Type> outputTypes)
    {
        List<Page> outputPages = executePipelinePages(files, columns, steps);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (Page page : outputPages) {
            result.page(page);
        }
        return result.build();
    }

    private List<Page> executePipelinePages(List<Path> files, List<String> columns, List<PipelineStep> steps)
    {
        List<Page> outputPages = new ArrayList<>();
        io.trino.operator.TaskContext taskContext = taskContext();
        try (TrinoClickBenchPageReader reader = new TrinoClickBenchPageReader(files, columns)) {
            DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
            List<Operator> operators = new ArrayList<>();
            TrinoPageSequenceSourceOperator.Factory sourceFactory = new TrinoPageSequenceSourceOperator.Factory(0, new PlanNodeId("source"), reader);
            operators.add(sourceFactory.createOperator(driverContext));

            for (PipelineStep step : steps) {
                OperatorFactory factory = step.createOperatorFactory(taskContext, this);
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
        }
        return outputPages;
    }

    private List<Page> executeHashJoinPages(List<Page> probePages, HashJoinSpec spec)
    {
        List<Page> outputPages = new ArrayList<>();
        io.trino.operator.TaskContext taskContext = taskContext();
        HashJoinRuntime hashJoinRuntime = createHashJoinRuntime(taskContext, spec);
        LookupOuterOperator.LookupOuterOperatorFactory outerFactory = hashJoinRuntime.outerFactory();
        DriverContext outerDriverContext = null;
        List<Operator> outerOperators = null;

        if (outerFactory != null) {
            outerDriverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
            outerOperators = new ArrayList<>();
            outerOperators.add(outerFactory.createOperator(outerDriverContext));
            outerFactory.noMoreOperators();
            outerOperators.add(new PageConsumerOperator(
                    outerDriverContext.addOperatorContext(1001, new PlanNodeId("outer-sink"), PageConsumerOperator.class.getSimpleName()),
                    outputPages::add,
                    java.util.function.Function.identity()));
        }

        DriverContext probeDriverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        ValuesOperator.ValuesOperatorFactory sourceFactory = new ValuesOperator.ValuesOperatorFactory(0, new PlanNodeId("values-source"), probePages);
        OperatorFactory joinFactory = hashJoinRuntime.joinFactory();
        List<Operator> joinOperators = new ArrayList<>();
        joinOperators.add(sourceFactory.createOperator(probeDriverContext));
        joinOperators.add(joinFactory.createOperator(probeDriverContext));
        sourceFactory.noMoreOperators();
        joinFactory.noMoreOperators();
        joinOperators.add(new PageConsumerOperator(
                probeDriverContext.addOperatorContext(1000, new PlanNodeId("join-sink"), PageConsumerOperator.class.getSimpleName()),
                outputPages::add,
                java.util.function.Function.identity()));

        try (Driver driver = Driver.createDriver(probeDriverContext, joinOperators)) {
            processDriver(driver, joinOperators);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to execute Trino hash-join probe pipeline", exception);
        }

        if (outerOperators != null && outerDriverContext != null) {
            try (Driver driver = Driver.createDriver(outerDriverContext, outerOperators)) {
                processDriver(driver, outerOperators);
            }
            catch (Exception exception) {
                throw new RuntimeException("Unable to execute Trino hash-join outer pipeline", exception);
            }
        }

        return outputPages;
    }

    private List<Page> executeNestedLoopPages(List<Page> probePages, List<Type> probeTypes, List<Page> buildPages, List<Type> buildTypes)
    {
        List<Page> outputPages = new ArrayList<>();
        io.trino.operator.TaskContext taskContext = taskContext();
        JoinBridgeManager<io.trino.operator.join.NestedLoopJoinBridge> joinBridgeManager = new JoinBridgeManager<>(
                false,
                new NestedLoopJoinPagesSupplier(),
                buildTypes);

        NestedLoopBuildOperator.NestedLoopBuildOperatorFactory buildFactory = new NestedLoopBuildOperator.NestedLoopBuildOperatorFactory(
                9_700,
                new PlanNodeId("nested-loop-build"),
                joinBridgeManager);
        ValuesOperator.ValuesOperatorFactory buildSourceFactory = new ValuesOperator.ValuesOperatorFactory(
                9_701,
                new PlanNodeId("nested-loop-build-values"),
                buildPages);
        DriverContext probeDriverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        ValuesOperator.ValuesOperatorFactory probeSourceFactory = new ValuesOperator.ValuesOperatorFactory(
                9_702,
                new PlanNodeId("nested-loop-probe-values"),
                probePages);
        OperatorFactory nestedLoopFactory = new NestedLoopJoinOperator.NestedLoopJoinOperatorFactory(
                9_703,
                new PlanNodeId("nested-loop-join"),
                joinBridgeManager,
                rangeList(probeTypes.size()),
                rangeList(buildTypes.size()));

        DriverContext buildDriverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
        try (Driver buildDriver = Driver.createDriver(
                buildDriverContext,
                buildSourceFactory.createOperator(buildDriverContext),
                buildFactory.createOperator(buildDriverContext))) {
            buildSourceFactory.noMoreOperators();
            buildFactory.noMoreOperators();
            ListenableFuture<Void> buildFinished = joinBridgeManager.getJoinBridge().whenBuildFinishes();
            while (!buildFinished.isDone()) {
                buildDriver.processForNumberOfIterations(1);
            }
            buildFinished.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to build Trino nested-loop pages", exception);
        }

        List<Operator> operators = new ArrayList<>();
        operators.add(probeSourceFactory.createOperator(probeDriverContext));
        operators.add(nestedLoopFactory.createOperator(probeDriverContext));
        probeSourceFactory.noMoreOperators();
        nestedLoopFactory.noMoreOperators();
        operators.add(new PageConsumerOperator(
                probeDriverContext.addOperatorContext(9_704, new PlanNodeId("nested-loop-sink"), PageConsumerOperator.class.getSimpleName()),
                outputPages::add,
                java.util.function.Function.identity()));

        try (Driver driver = Driver.createDriver(probeDriverContext, operators)) {
            processDriver(driver, operators);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to execute Trino nested-loop probe pipeline", exception);
        }

        return outputPages;
    }

    private List<Page> executePipelinePages(List<Page> inputPages, List<PipelineStep> steps)
    {
        List<Page> outputPages = new ArrayList<>();
        io.trino.operator.TaskContext taskContext = taskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        List<Operator> operators = new ArrayList<>();
        ValuesOperator.ValuesOperatorFactory sourceFactory = new ValuesOperator.ValuesOperatorFactory(0, new PlanNodeId("values-source"), inputPages);
        operators.add(sourceFactory.createOperator(driverContext));
        sourceFactory.noMoreOperators();

        for (PipelineStep step : steps) {
            OperatorFactory factory = step.createOperatorFactory(taskContext, this);
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
            throw new RuntimeException("Unable to execute Trino TPC-DS parquet pages pipeline", exception);
        }

        return outputPages;
    }

    private static PipelineStep factoryStep(OperatorFactory factory)
    {
        return new FactoryStep(factory);
    }

    private static PipelineStep hashJoinStep(HashJoinSpec spec)
    {
        return new HashJoinStep(spec);
    }

    private static PipelineStep semiJoinStep(SemiJoinSpec spec)
    {
        return new SemiJoinStep(spec);
    }

    private static PipelineStep semiJoinPagesStep(SemiJoinPagesSpec spec)
    {
        return new SemiJoinPagesStep(spec);
    }

    private OperatorFactory createHashJoinFactory(io.trino.operator.TaskContext taskContext, HashJoinSpec hashJoinSpec)
    {
        return createHashJoinRuntime(taskContext, hashJoinSpec).joinFactory();
    }

    private HashJoinRuntime createHashJoinRuntime(io.trino.operator.TaskContext taskContext, HashJoinSpec hashJoinSpec)
    {
        boolean buildOuter = switch (hashJoinSpec.joinOperatorType().getType()) {
            case LOOKUP_OUTER, FULL_OUTER -> true;
            default -> false;
        };
        if (buildOuter) {
            io.trino.operator.join.unspilled.PartitionedLookupSourceFactory lookupSourceFactory = new io.trino.operator.join.unspilled.PartitionedLookupSourceFactory(
                    hashJoinSpec.buildTypes(),
                    hashJoinSpec.buildTypes(),
                    hashJoinSpec.buildHashChannels().stream()
                            .map(hashJoinSpec.buildTypes()::get)
                            .toList(),
                    1,
                    true,
                    new TypeOperators());
            JoinBridgeManager<io.trino.operator.join.unspilled.PartitionedLookupSourceFactory> joinBridgeManager = new JoinBridgeManager<>(
                    true,
                    lookupSourceFactory,
                    lookupSourceFactory.getOutputTypes());
            OperatorFactory joinFactory = io.trino.operator.OperatorFactories.join(
                    hashJoinSpec.joinOperatorType(),
                    hashJoinSpec.operatorId(),
                    new PlanNodeId("join-" + hashJoinSpec.operatorId()),
                    joinBridgeManager,
                    false,
                    hashJoinSpec.probeTypes(),
                    hashJoinSpec.probeJoinChannels(),
                    Optional.empty());
            LookupOuterOperator.LookupOuterOperatorFactory outerFactory = new LookupOuterOperator.LookupOuterOperatorFactory(
                    7000 + hashJoinSpec.operatorId(),
                    new PlanNodeId("outer-" + hashJoinSpec.operatorId()),
                    hashJoinSpec.probeTypes(),
                    lookupSourceFactory.getOutputTypes(),
                    joinBridgeManager);
            io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory buildOperatorFactory = new io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory(
                    9000 + hashJoinSpec.operatorId(),
                    new PlanNodeId("build-" + hashJoinSpec.operatorId()),
                    joinBridgeManager,
                    rangeList(hashJoinSpec.buildTypes().size()),
                    hashJoinSpec.buildHashChannels(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableList.of(),
                    100,
                    new PagesIndex.TestingFactory(false),
                    HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier(taskContext.getSession()));
            ValuesOperator.ValuesOperatorFactory valuesOperatorFactory = new ValuesOperator.ValuesOperatorFactory(
                    8000 + hashJoinSpec.operatorId(),
                    new PlanNodeId("values-" + hashJoinSpec.operatorId()),
                    hashJoinSpec.buildPages());

            DriverContext buildDriverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
            try (Driver buildDriver = Driver.createDriver(
                    buildDriverContext,
                    valuesOperatorFactory.createOperator(buildDriverContext),
                    buildOperatorFactory.createOperator(buildDriverContext))) {
                valuesOperatorFactory.noMoreOperators();
                buildOperatorFactory.noMoreOperators();
                ListenableFuture<Void> buildFinished = joinBridgeManager.getJoinBridge().whenBuildFinishes();
                while (!buildFinished.isDone()) {
                    buildDriver.processForNumberOfIterations(1);
                }
                buildFinished.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS);
            }
            catch (Exception exception) {
                throw new RuntimeException("Unable to build Trino full-outer hash-join lookup source", exception);
            }

            return new HashJoinRuntime(joinFactory, outerFactory);
        }

        io.trino.operator.join.unspilled.PartitionedLookupSourceFactory lookupSourceFactory = new io.trino.operator.join.unspilled.PartitionedLookupSourceFactory(
                hashJoinSpec.buildTypes(),
                hashJoinSpec.buildTypes(),
                hashJoinSpec.buildHashChannels().stream()
                        .map(hashJoinSpec.buildTypes()::get)
                        .toList(),
                1,
                false,
                new TypeOperators());
        JoinBridgeManager<io.trino.operator.join.unspilled.PartitionedLookupSourceFactory> joinBridgeManager = new JoinBridgeManager<>(
                false,
                lookupSourceFactory,
                lookupSourceFactory.getOutputTypes());
        OperatorFactory joinFactory = io.trino.operator.OperatorFactories.join(
                hashJoinSpec.joinOperatorType(),
                hashJoinSpec.operatorId(),
                new PlanNodeId("join-" + hashJoinSpec.operatorId()),
                joinBridgeManager,
                false,
                hashJoinSpec.probeTypes(),
                hashJoinSpec.probeJoinChannels(),
                Optional.empty());
        io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory buildOperatorFactory = new io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory(
                9000 + hashJoinSpec.operatorId(),
                new PlanNodeId("build-" + hashJoinSpec.operatorId()),
                joinBridgeManager,
                rangeList(hashJoinSpec.buildTypes().size()),
                hashJoinSpec.buildHashChannels(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                100,
                new PagesIndex.TestingFactory(false),
                HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier(taskContext.getSession()));
        ValuesOperator.ValuesOperatorFactory valuesOperatorFactory = new ValuesOperator.ValuesOperatorFactory(
                8000 + hashJoinSpec.operatorId(),
                new PlanNodeId("values-" + hashJoinSpec.operatorId()),
                hashJoinSpec.buildPages());

        DriverContext buildDriverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
        try (Driver buildDriver = Driver.createDriver(
                buildDriverContext,
                valuesOperatorFactory.createOperator(buildDriverContext),
                buildOperatorFactory.createOperator(buildDriverContext))) {
            valuesOperatorFactory.noMoreOperators();
            buildOperatorFactory.noMoreOperators();
            java.util.concurrent.Future<LookupSource> lookupSource = joinBridgeManager.getJoinBridge().createLookupSource();
            while (!lookupSource.isDone()) {
                buildDriver.processForNumberOfIterations(1);
            }
            lookupSource.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to build Trino hash-join lookup source", exception);
        }

        return new HashJoinRuntime(joinFactory, null);
    }

    private OperatorFactory createSemiJoinFactory(io.trino.operator.TaskContext taskContext, SemiJoinSpec semiJoinSpec)
    {
        JoinCompiler joinCompiler = new JoinCompiler(new TypeOperators());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                9500 + semiJoinSpec.operatorId(),
                new PlanNodeId("semi-build-" + semiJoinSpec.operatorId()),
                semiJoinSpec.buildTypes().get(semiJoinSpec.buildJoinChannel()),
                semiJoinSpec.buildJoinChannel(),
                10_000,
                joinCompiler,
                new TypeOperators());
        SetSupplier setSupplier = setBuilderOperatorFactory.getSetProvider();

        buildSemiJoinSet(taskContext, setBuilderOperatorFactory, semiJoinSpec.buildPages().orElse(null), semiJoinSpec.buildFiles(), semiJoinSpec.buildColumns(), semiJoinSpec.buildSteps(), semiJoinSpec.operatorId());

        return HashSemiJoinOperator.createOperatorFactory(
                semiJoinSpec.operatorId(),
                new PlanNodeId("semi-join-" + semiJoinSpec.operatorId()),
                setSupplier,
                semiJoinSpec.probeTypes(),
                semiJoinSpec.probeJoinChannel());
    }

    private OperatorFactory createSemiJoinFactory(io.trino.operator.TaskContext taskContext, SemiJoinPagesSpec semiJoinSpec)
    {
        JoinCompiler joinCompiler = new JoinCompiler(new TypeOperators());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                9500 + semiJoinSpec.operatorId(),
                new PlanNodeId("semi-build-" + semiJoinSpec.operatorId()),
                semiJoinSpec.buildTypes().get(semiJoinSpec.buildJoinChannel()),
                semiJoinSpec.buildJoinChannel(),
                10_000,
                joinCompiler,
                new TypeOperators());
        SetSupplier setSupplier = setBuilderOperatorFactory.getSetProvider();

        buildSemiJoinSet(taskContext, setBuilderOperatorFactory, semiJoinSpec.buildPages(), List.of(), List.of(), List.of(), semiJoinSpec.operatorId());

        return HashSemiJoinOperator.createOperatorFactory(
                semiJoinSpec.operatorId(),
                new PlanNodeId("semi-join-" + semiJoinSpec.operatorId()),
                setSupplier,
                semiJoinSpec.probeTypes(),
                semiJoinSpec.probeJoinChannel());
    }

    private void buildSemiJoinSet(
            io.trino.operator.TaskContext taskContext,
            SetBuilderOperatorFactory setBuilderOperatorFactory,
            List<Page> buildPages,
            List<Path> buildFiles,
            List<String> buildColumns,
            List<PipelineStep> buildSteps,
            int operatorId)
    {
        try {
            DriverContext buildDriverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
            List<Operator> operators = new ArrayList<>();

            if (buildPages != null) {
                ValuesOperator.ValuesOperatorFactory valuesOperatorFactory = new ValuesOperator.ValuesOperatorFactory(
                        8_500 + operatorId,
                        new PlanNodeId("semi-build-values-" + operatorId),
                        buildPages);
                operators.add(valuesOperatorFactory.createOperator(buildDriverContext));
                valuesOperatorFactory.noMoreOperators();
            }
            else {
                TrinoClickBenchPageReader reader = new TrinoClickBenchPageReader(buildFiles, buildColumns);
                TrinoPageSequenceSourceOperator.Factory sourceFactory = new TrinoPageSequenceSourceOperator.Factory(
                        0,
                        new PlanNodeId("semi-build-source-" + operatorId),
                        reader);
                operators.add(sourceFactory.createOperator(buildDriverContext));
            }

            for (PipelineStep step : buildSteps) {
                OperatorFactory factory = step.createOperatorFactory(taskContext, this);
                operators.add(factory.createOperator(buildDriverContext));
                factory.noMoreOperators();
            }

            operators.add(setBuilderOperatorFactory.createOperator(buildDriverContext));
            setBuilderOperatorFactory.noMoreOperators();

            try (Driver buildDriver = Driver.createDriver(buildDriverContext, operators)) {
                processDriver(buildDriver, operators);
            }
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to build Trino TPC-DS semi-join set", exception);
        }
    }

    private MaterializedResult scanTable(TpcdsParquetTables tables, String tableName, List<String> columns, List<Type> outputTypes)
    {
        return execute(tables.tableFiles(tableName), columns, List.of(), outputTypes);
    }

    private List<Page> relationPages(TpcdsParquetTables tables, String tableName, List<String> columns, Optional<RowExpression> filter, List<RowExpression> projections, List<Type> outputTypes)
    {
        return executePipelinePages(
                tables.tableFiles(tableName),
                columns,
                List.of(factoryStep(filterAndProjectFactory(7_000 + Math.abs(tableName.hashCode() % 1_000), filter, projections, outputTypes))));
    }

    private List<Page> customerKeyPagesForEligibleDates(
            TpcdsParquetTables tables,
            String salesTable,
            String customerColumn,
            String dateColumn,
            List<String> dateColumns,
            RowExpression dateFilter,
            int operatorIdBase)
    {
        List<Page> eligibleDateKeys = relationPages(
                tables,
                "date_dim",
                dateColumns,
                Optional.of(dateFilter),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<PipelineStep> steps = List.of(
                hashJoinStep(new HashJoinSpec(operatorIdBase, List.of(BIGINT, BIGINT), List.of(1), eligibleDateKeys, List.of(BIGINT), List.of(0))),
                factoryStep(filterAndProjectFactory(
                        operatorIdBase + 1,
                        Optional.of(greaterThan(field(0, BIGINT), constant(0L, BIGINT), BIGINT)),
                        List.of(field(0, BIGINT)),
                        List.of(BIGINT))));
        return executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(customerColumn, dateColumn),
                steps);
    }

    private ChannelPages query80ChannelPages(
            TpcdsParquetTables tables,
            String salesTable,
            List<String> salesColumns,
            String returnsTable,
            List<String> returnsColumns,
            String dimensionTable,
            List<String> dimensionColumns,
            String channelName,
            String idPrefix,
            int operatorIdBase)
    {
        List<Type> factTypes = tableColumnTypes(tables, salesTable, salesColumns);
        Type salesType = factTypes.get(5);
        Type profitType = factTypes.get(6);
        List<Type> returnsTypes = tableColumnTypes(tables, returnsTable, returnsColumns);
        Type returnAmountType = returnsTypes.get(2);
        Type returnLossType = returnsTypes.get(3);
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_current_price"));
        List<Type> dimensionTypes = tableColumnTypes(tables, dimensionTable, dimensionColumns);
        Type dimensionIdType = dimensionTypes.get(1);

        List<Page> dateKeys = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date"),
                Optional.of(query80DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> itemKeys = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_current_price"),
                Optional.of(greaterThan(field(1, itemTypes.get(1)), constant(5_000L, itemTypes.get(1)), itemTypes.get(1))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> promotionKeys = relationPages(
                tables,
                "promotion",
                List.of("p_promo_sk", "p_channel_tv"),
                Optional.of(equal(1, VARCHAR, "N")),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> dimensionPages = relationPages(
                tables,
                dimensionTable,
                dimensionColumns,
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, dimensionIdType)),
                List.of(BIGINT, dimensionIdType));

        List<Type> joinedReturnTypes = concatTypes(factTypes, returnsTypes);
        List<Type> projectedTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT, salesType, profitType, returnAmountType, returnLossType);
        List<Type> afterDateTypes = concatTypes(projectedTypes, List.of(BIGINT));
        List<Type> afterDimensionTypes = concatTypes(afterDateTypes, List.of(BIGINT, dimensionIdType));
        List<Type> afterItemTypes = concatTypes(afterDimensionTypes, List.of(BIGINT));
        List<Type> afterPromotionTypes = concatTypes(afterItemTypes, List.of(BIGINT));
        List<Type> preAggregateTypes = List.of(VARCHAR, salesType, returnAmountType, profitType);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction returnsSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));
        List<Type> aggregatedTypes = List.of(VARCHAR, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());

        return new ChannelPages(
                executePipelinePages(
                        tables.tableFiles(salesTable),
                        salesColumns,
                        List.of(
                                hashJoinStep(new HashJoinSpec(
                                        operatorIdBase,
                                        factTypes,
                                        List.of(1, 4),
                                        relationPages(
                                                tables,
                                                returnsTable,
                                                returnsColumns,
                                                Optional.empty(),
                                                identityProjections(returnsTypes),
                                                returnsTypes),
                                        returnsTypes,
                                        List.of(0, 1),
                                        JoinOperatorType.probeOuterJoin(false))),
                                factoryStep(filterAndProjectFactory(
                                        operatorIdBase + 1,
                                        Optional.empty(),
                                        List.of(
                                                field(0, BIGINT),
                                                field(1, BIGINT),
                                                field(2, BIGINT),
                                                field(3, BIGINT),
                                                field(5, salesType),
                                                field(6, profitType),
                                                field(9, returnAmountType),
                                                field(10, returnLossType)),
                                        projectedTypes)),
                                hashJoinStep(new HashJoinSpec(operatorIdBase + 2, projectedTypes, List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(operatorIdBase + 3, afterDateTypes, List.of(3), dimensionPages, List.of(BIGINT, dimensionIdType), List.of(0))),
                                hashJoinStep(new HashJoinSpec(operatorIdBase + 4, afterDimensionTypes, List.of(1), itemKeys, List.of(BIGINT), List.of(0))),
                                hashJoinStep(new HashJoinSpec(operatorIdBase + 5, afterItemTypes, List.of(2), promotionKeys, List.of(BIGINT), List.of(0))),
                                factoryStep(filterAndProjectFactory(
                                        operatorIdBase + 6,
                                        Optional.empty(),
                                        List.of(
                                                asVarchar(field(10, dimensionIdType), dimensionIdType),
                                                field(4, salesType),
                                                coalesce(field(6, returnAmountType), constant(0L, returnAmountType), returnAmountType),
                                                subtract(field(5, profitType), coalesce(field(7, returnLossType), constant(0L, returnLossType), returnLossType), profitType)),
                                        preAggregateTypes)),
                                factoryStep(hashAggregationFactory(
                                        operatorIdBase + 7,
                                        List.of(VARCHAR),
                                        List.of(0),
                                        salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                        returnsSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                        profitSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                                factoryStep(filterAndProjectFactory(
                                        operatorIdBase + 8,
                                        Optional.empty(),
                                        List.of(
                                                constant(Slices.utf8Slice(channelName), VARCHAR),
                                                concat(constant(Slices.utf8Slice(idPrefix), VARCHAR), field(0, VARCHAR)),
                                                field(1, salesSum.getFinalType()),
                                                field(2, returnsSum.getFinalType()),
                                                field(3, profitSum.getFinalType())),
                                        outputTypes)))),
                outputTypes);
    }

    private ChannelPages query77ChannelPages(
            TpcdsParquetTables tables,
            String salesTable,
            List<String> salesColumns,
            String returnsTable,
            List<String> returnsColumns,
            String channelName,
            int operatorIdBase)
    {
        List<Type> salesTypes = tableColumnTypes(tables, salesTable, salesColumns);
        Type salesType = salesTypes.get(2);
        Type profitType = salesTypes.get(3);
        List<Type> returnsTypes = tableColumnTypes(tables, returnsTable, returnsColumns);
        Type returnAmountType = returnsTypes.get(2);
        Type returnLossType = returnsTypes.get(3);

        List<Page> dateKeys = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date"),
                Optional.of(query80DatePredicate()),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));
        TestingAggregationFunction returnsSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType));
        TestingAggregationFunction lossSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnLossType));

        List<Page> salesPages = executePipelinePages(
                tables.tableFiles(salesTable),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(2, salesType), field(3, profitType)),
                                List.of(BIGINT, salesType, profitType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 2,
                                List.of(BIGINT),
                                List.of(0),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                profitSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
        List<Type> salesGroupedTypes = List.of(BIGINT, salesSum.getFinalType(), profitSum.getFinalType());

        List<Page> returnsPages = executePipelinePages(
                tables.tableFiles(returnsTable),
                returnsColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 3, returnsTypes, List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 4,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(2, returnAmountType), field(3, returnLossType)),
                                List.of(BIGINT, returnAmountType, returnLossType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 5,
                                List.of(BIGINT),
                                List.of(0),
                                returnsSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                lossSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
        List<Type> returnsGroupedTypes = List.of(BIGINT, returnsSum.getFinalType(), lossSum.getFinalType());

        List<Type> outputTypes = List.of(VARCHAR, BIGINT, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());
        return new ChannelPages(
                executePipelinePages(
                        salesPages,
                        List.of(
                                hashJoinStep(new HashJoinSpec(
                                        operatorIdBase + 6,
                                        salesGroupedTypes,
                                        List.of(0),
                                        returnsPages,
                                        returnsGroupedTypes,
                                        List.of(0),
                                        JoinOperatorType.probeOuterJoin(false))),
                                factoryStep(filterAndProjectFactory(
                                        operatorIdBase + 7,
                                        Optional.empty(),
                                        List.of(
                                                constant(Slices.utf8Slice(channelName), VARCHAR),
                                                field(0, BIGINT),
                                                field(1, salesSum.getFinalType()),
                                                coalesce(field(4, returnsSum.getFinalType()), constant(Int128.ZERO, returnsSum.getFinalType()), returnsSum.getFinalType()),
                                                subtract(field(2, profitSum.getFinalType()), coalesce(field(5, lossSum.getFinalType()), constant(Int128.ZERO, lossSum.getFinalType()), lossSum.getFinalType()), profitSum.getFinalType())),
                                        outputTypes)))),
                outputTypes);
    }

    private ChannelPages query05ChannelPages(
            TpcdsParquetTables tables,
            String salesTable,
            List<String> salesColumns,
            String returnsTable,
            List<String> returnsColumns,
            String dimensionTable,
            List<String> dimensionColumns,
            String channelName,
            String idPrefix,
            int operatorIdBase)
    {
        List<Type> salesTypes = tableColumnTypes(tables, salesTable, salesColumns);
        Type salesType = salesTypes.get(2);
        Type profitType = salesTypes.get(3);
        List<Type> returnsTypes = tableColumnTypes(tables, returnsTable, returnsColumns);
        Type returnAmountType = returnsTypes.get(2);
        Type returnLossType = returnsTypes.get(3);
        List<Type> dimensionTypes = tableColumnTypes(tables, dimensionTable, dimensionColumns);
        Type dimensionIdType = dimensionTypes.get(1);

        List<Page> dateKeys = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date"),
                Optional.of(betweenInclusive(field(1, DATE), LocalDate.of(2000, 8, 23).toEpochDay(), LocalDate.of(2000, 9, 6).toEpochDay(), DATE)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> dimensionPages = relationPages(
                tables,
                dimensionTable,
                dimensionColumns,
                Optional.empty(),
                List.of(field(0, BIGINT), asVarchar(field(1, dimensionIdType), dimensionIdType)),
                List.of(BIGINT, VARCHAR));

        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction returnsSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));

        List<Page> salesPages = executePipelinePages(
                tables.tableFiles(salesTable),
                salesColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 1, concatTypes(salesTypes, List.of(BIGINT)), List.of(1), dimensionPages, List.of(BIGINT, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(
                                        field(5, VARCHAR),
                                        field(2, salesType),
                                        constant(0L, returnAmountType),
                                        field(3, profitType)),
                                List.of(VARCHAR, salesType, returnAmountType, profitType)))));

        List<Page> returnsPages = executePipelinePages(
                tables.tableFiles(returnsTable),
                returnsColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 3, returnsTypes, List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 4, concatTypes(returnsTypes, List.of(BIGINT)), List.of(1), dimensionPages, List.of(BIGINT, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 5,
                                Optional.empty(),
                                List.of(
                                        field(5, VARCHAR),
                                        constant(0L, salesType),
                                        field(2, returnAmountType),
                                        subtract(constant(0L, profitType), cast(field(3, returnLossType), returnLossType, profitType), profitType)),
                                List.of(VARCHAR, salesType, returnAmountType, profitType)))));

        List<Page> unionPages = new ArrayList<>(salesPages);
        unionPages.addAll(returnsPages);

        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());
        List<PipelineStep> steps = List.of(
                factoryStep(hashAggregationFactory(
                        operatorIdBase + 6,
                        List.of(VARCHAR),
                        List.of(0),
                        salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                        returnsSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                        profitSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                factoryStep(filterAndProjectFactory(
                        operatorIdBase + 7,
                        Optional.empty(),
                        List.of(
                                constant(Slices.utf8Slice(channelName), VARCHAR),
                                concat(constant(Slices.utf8Slice(idPrefix), VARCHAR), field(0, VARCHAR)),
                                field(1, salesSum.getFinalType()),
                                field(2, returnsSum.getFinalType()),
                                field(3, profitSum.getFinalType())),
                        outputTypes)));
        return new ChannelPages(
                executePipelinePages(unionPages, steps),
                outputTypes);
    }

    private ChannelPages query05WebChannelPages(TpcdsParquetTables tables, int operatorIdBase)
    {
        List<Type> salesTypes = tableColumnTypes(tables, "web_sales", List.of("ws_sold_date_sk", "ws_web_site_sk", "ws_ext_sales_price", "ws_net_profit"));
        Type salesType = salesTypes.get(2);
        Type profitType = salesTypes.get(3);
        List<Type> returnsTypes = tableColumnTypes(tables, "web_returns", List.of("wr_returned_date_sk", "wr_item_sk", "wr_order_number", "wr_return_amt", "wr_net_loss"));
        Type returnAmountType = returnsTypes.get(3);
        Type returnLossType = returnsTypes.get(4);
        List<Type> siteTypes = tableColumnTypes(tables, "web_site", List.of("web_site_sk", "web_site_id"));
        Type siteIdType = siteTypes.get(1);

        List<Page> dateKeys = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_date"),
                Optional.of(betweenInclusive(field(1, DATE), LocalDate.of(2000, 8, 23).toEpochDay(), LocalDate.of(2000, 9, 6).toEpochDay(), DATE)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        List<Page> sitePages = relationPages(
                tables,
                "web_site",
                List.of("web_site_sk", "web_site_id"),
                Optional.empty(),
                List.of(field(0, BIGINT), asVarchar(field(1, siteIdType), siteIdType)),
                List.of(BIGINT, VARCHAR));
        List<Page> webSalesSiteKeys = relationPages(
                tables,
                "web_sales",
                List.of("ws_item_sk", "ws_order_number", "ws_web_site_sk"),
                Optional.empty(),
                List.of(field(0, BIGINT), field(1, BIGINT), field(2, BIGINT)),
                List.of(BIGINT, BIGINT, BIGINT));

        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        TestingAggregationFunction returnsSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType));
        TestingAggregationFunction profitSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(profitType));

        List<Page> salesPages = executePipelinePages(
                tables.tableFiles("web_sales"),
                List.of("ws_sold_date_sk", "ws_web_site_sk", "ws_ext_sales_price", "ws_net_profit"),
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, salesTypes, List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 1, concatTypes(salesTypes, List.of(BIGINT)), List.of(1), sitePages, List.of(BIGINT, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(
                                        field(5, VARCHAR),
                                        field(2, salesType),
                                        constant(0L, returnAmountType),
                                        field(3, profitType)),
                                List.of(VARCHAR, salesType, returnAmountType, profitType)))));

        List<Page> returnsPages = executePipelinePages(
                tables.tableFiles("web_returns"),
                List.of("wr_returned_date_sk", "wr_item_sk", "wr_order_number", "wr_return_amt", "wr_net_loss"),
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 3, returnsTypes, List.of(0), dateKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 4, concatTypes(returnsTypes, List.of(BIGINT)), List.of(1, 2), webSalesSiteKeys, List.of(BIGINT, BIGINT, BIGINT), List.of(0, 1))),
                        hashJoinStep(new HashJoinSpec(operatorIdBase + 5, concatTypes(concatTypes(returnsTypes, List.of(BIGINT)), List.of(BIGINT, BIGINT, BIGINT)), List.of(8), sitePages, List.of(BIGINT, VARCHAR), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 6,
                                Optional.empty(),
                                List.of(
                                        field(10, VARCHAR),
                                        constant(0L, salesType),
                                        field(3, returnAmountType),
                                        subtract(constant(0L, profitType), cast(field(4, returnLossType), returnLossType, profitType), profitType)),
                                List.of(VARCHAR, salesType, returnAmountType, profitType)))));

        List<Page> unionPages = new ArrayList<>(salesPages);
        unionPages.addAll(returnsPages);

        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, salesSum.getFinalType(), returnsSum.getFinalType(), profitSum.getFinalType());
        List<PipelineStep> steps = List.of(
                factoryStep(hashAggregationFactory(
                        operatorIdBase + 7,
                        List.of(VARCHAR),
                        List.of(0),
                        salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                        returnsSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                        profitSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                factoryStep(filterAndProjectFactory(
                        operatorIdBase + 8,
                        Optional.empty(),
                        List.of(
                                constant(Slices.utf8Slice("web channel"), VARCHAR),
                                concat(constant(Slices.utf8Slice("web_site"), VARCHAR), field(0, VARCHAR)),
                                field(1, salesSum.getFinalType()),
                                field(2, returnsSum.getFinalType()),
                                field(3, profitSum.getFinalType())),
                        outputTypes)));
        return new ChannelPages(
                executePipelinePages(unionPages, steps),
                outputTypes);
    }

    private List<Page> query16MultiWarehouseOrderPages(TpcdsParquetTables tables, int operatorIdBase)
    {
        List<String> columns = List.of("cs_warehouse_sk", "cs_order_number");
        List<Type> types = tableColumnTypes(tables, "catalog_sales", columns);

        return executePipelinePages(
                tables.tableFiles("catalog_sales"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                types,
                                List.of(1),
                                relationPages(tables, "catalog_sales", columns, Optional.empty(), identityProjections(types), types),
                                types,
                                List.of(1))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.of(notEqual(field(0, BIGINT), field(2, BIGINT))),
                                List.of(field(1, BIGINT)),
                                List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(operatorIdBase + 2, List.of(BIGINT), List.of(0)))));
    }

    private List<Page> query16ReturnedEligibleOrderPages(TpcdsParquetTables tables, List<Page> eligibleOrders, int operatorIdBase)
    {
        List<String> columns = List.of("cr_order_number");
        List<Type> types = tableColumnTypes(tables, "catalog_returns", columns);

        return executePipelinePages(
                tables.tableFiles("catalog_returns"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, types, List.of(0), eligibleOrders, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(operatorIdBase + 1, Optional.empty(), List.of(field(0, BIGINT)), List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(operatorIdBase + 2, List.of(BIGINT), List.of(0)))));
    }

    private List<Page> query95MultiWarehouseOrderPages(TpcdsParquetTables tables, int operatorIdBase)
    {
        List<String> columns = List.of("ws_warehouse_sk", "ws_order_number");
        List<Type> types = tableColumnTypes(tables, "web_sales", columns);
        List<Type> joinedTypes = concatTypes(types, types);

        return executePipelinePages(
                tables.tableFiles("web_sales"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                types,
                                List.of(1),
                                relationPages(tables, "web_sales", columns, Optional.empty(), identityProjections(types), types),
                                types,
                                List.of(1))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.of(notEqual(field(0, BIGINT), field(2, BIGINT))),
                                List.of(field(1, BIGINT)),
                                List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(operatorIdBase + 2, List.of(BIGINT), List.of(0)))));
    }

    private List<Page> query95ReturnedEligibleOrderPages(TpcdsParquetTables tables, List<Page> eligibleOrders, int operatorIdBase)
    {
        List<String> columns = List.of("wr_order_number");
        List<Type> types = tableColumnTypes(tables, "web_returns", columns);

        return executePipelinePages(
                tables.tableFiles("web_returns"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, types, List.of(0), eligibleOrders, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(operatorIdBase + 1, Optional.empty(), List.of(field(0, BIGINT)), List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(operatorIdBase + 2, List.of(BIGINT), List.of(0)))));
    }

    private List<Page> query81CustomerTotalReturnPages(TpcdsParquetTables tables, int operatorIdBase)
    {
        List<String> returnColumns = List.of("cr_returning_customer_sk", "cr_returned_date_sk", "cr_returning_addr_sk", "cr_return_amt_inc_tax");
        List<Type> returnTypes = tableColumnTypes(tables, "catalog_returns", returnColumns);
        Type returnAmountType = returnTypes.get(3);
        Type stateType = query81StateType(tables);
        TestingAggregationFunction returnSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType));
        List<Type> outputTypes = List.of(BIGINT, stateType, returnSum.getFinalType());

        return executePipelinePages(
                tables.tableFiles("catalog_returns"),
                returnColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                returnTypes,
                                List.of(1),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(equal(1, 2000, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(returnTypes, List.of(BIGINT)),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "customer_address",
                                        List.of("ca_address_sk", "ca_state"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, stateType)),
                                        List.of(BIGINT, stateType)),
                                List.of(BIGINT, stateType),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(6, stateType), field(3, returnAmountType)),
                                List.of(BIGINT, stateType, returnAmountType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(BIGINT, stateType),
                                List.of(0, 1),
                                returnSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    private List<Page> query81StateAveragePages(TpcdsParquetTables tables)
    {
        Type stateType = query81StateType(tables);
        Type returnSumType = query81ReturnSumType(tables);
        TestingAggregationFunction average = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(returnSumType));
        return executePipelinePages(
                query81CustomerTotalReturnPages(tables, 81_110),
                List.of(
                        factoryStep(hashAggregationFactory(
                                81_114,
                                List.of(stateType),
                                List.of(1),
                                average.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    private List<Page> query30CustomerTotalReturnPages(TpcdsParquetTables tables, int operatorIdBase)
    {
        List<String> returnColumns = List.of("wr_returning_customer_sk", "wr_returned_date_sk", "wr_returning_addr_sk", "wr_return_amt");
        List<Type> returnTypes = tableColumnTypes(tables, "web_returns", returnColumns);
        Type returnAmountType = returnTypes.get(3);
        Type stateType = query81StateType(tables);
        TestingAggregationFunction returnSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType));

        return executePipelinePages(
                tables.tableFiles("web_returns"),
                returnColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                returnTypes,
                                List.of(1),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(equal(1, 2002, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(returnTypes, List.of(BIGINT)),
                                List.of(2),
                                relationPages(
                                        tables,
                                        "customer_address",
                                        List.of("ca_address_sk", "ca_state"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, stateType)),
                                        List.of(BIGINT, stateType)),
                                List.of(BIGINT, stateType),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(6, stateType), field(3, returnAmountType)),
                                List.of(BIGINT, stateType, returnAmountType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(BIGINT, stateType),
                                List.of(0, 1),
                                returnSum.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    private List<Page> query30StateAveragePages(TpcdsParquetTables tables)
    {
        Type stateType = query81StateType(tables);
        Type returnSumType = query30ReturnSumType(tables);
        TestingAggregationFunction average = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(returnSumType));
        return executePipelinePages(
                query30CustomerTotalReturnPages(tables, 30_110),
                List.of(
                        factoryStep(hashAggregationFactory(
                                30_114,
                                List.of(stateType),
                                List.of(1),
                                average.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    private List<Page> query32FilteredDiscountPages(TpcdsParquetTables tables, int operatorIdBase)
    {
        List<String> columns = List.of("cs_sold_date_sk", "cs_item_sk", "cs_ext_discount_amt");
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", columns);
        Type discountType = factTypes.get(2);

        return executePipelinePages(
                tables.tableFiles("catalog_sales"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_manufact_id"),
                                        Optional.of(equal(1, 977, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, List.of(BIGINT)),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_date"),
                                        Optional.of(query92DatePredicate()),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(2, discountType)),
                                List.of(BIGINT, discountType)))));
    }

    private List<Page> query32ItemAveragePages(TpcdsParquetTables tables)
    {
        Type discountType = tableColumnTypes(tables, "catalog_sales", List.of("cs_ext_discount_amt")).getFirst();
        TestingAggregationFunction discountAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(discountType));
        return executePipelinePages(
                tables.tableFiles("catalog_sales"),
                List.of("cs_sold_date_sk", "cs_item_sk", "cs_ext_discount_amt"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                32_110,
                                List.of(BIGINT, BIGINT, discountType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_date"),
                                        Optional.of(query92DatePredicate()),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(hashAggregationFactory(
                                32_111,
                                List.of(BIGINT),
                                List.of(1),
                                discountAverage.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
    }

    private List<Page> query23FrequentItemPages(TpcdsParquetTables tables)
    {
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", List.of("ss_sold_date_sk", "ss_item_sk"));
        List<Type> groupedTypes = List.of(BIGINT, DATE, BIGINT);

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_sold_date_sk", "ss_item_sk"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                23_10,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year", "d_date"),
                                        Optional.of(query23YearRangePredicate()),
                                        List.of(field(0, BIGINT), field(2, DATE)),
                                        List.of(BIGINT, DATE)),
                                List.of(BIGINT, DATE),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                23_11,
                                concatTypes(factTypes, List.of(BIGINT, DATE)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                23_12,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(3, DATE)),
                                List.of(BIGINT, DATE))),
                        factoryStep(hashAggregationFactory(
                                23_13,
                                List.of(BIGINT, DATE),
                                List.of(0, 1),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                23_14,
                                Optional.of(greaterThan(field(2, BIGINT), constant(4L, BIGINT), BIGINT)),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(23_15, List.of(BIGINT), List.of(0)))));
    }

    private List<Page> query14CrossItemPages(TpcdsParquetTables tables)
    {
        List<Type> tripleTypes = List.of(INTEGER, INTEGER, INTEGER);
        List<Page> sharedTriples = executeHashJoinPages(
                query14ChannelTriplesPages(tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", 14_10),
                new HashJoinSpec(14_13, tripleTypes, List.of(0, 1, 2), query14ChannelTriplesPages(tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", 14_11), tripleTypes, List.of(0, 1, 2)));
        sharedTriples = executePipelinePages(
                sharedTriples,
                List.of(
                        factoryStep(filterAndProjectFactory(14_14, Optional.empty(), List.of(field(0, INTEGER), field(1, INTEGER), field(2, INTEGER)), tripleTypes)),
                        factoryStep(hashAggregationFactory(14_15, tripleTypes, List.of(0, 1, 2)))));
        sharedTriples = executeHashJoinPages(
                sharedTriples,
                new HashJoinSpec(14_16, tripleTypes, List.of(0, 1, 2), query14ChannelTriplesPages(tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", 14_12), tripleTypes, List.of(0, 1, 2)));
        sharedTriples = executePipelinePages(
                sharedTriples,
                List.of(
                        factoryStep(filterAndProjectFactory(14_17, Optional.empty(), List.of(field(0, INTEGER), field(1, INTEGER), field(2, INTEGER)), tripleTypes)),
                        factoryStep(hashAggregationFactory(14_18, tripleTypes, List.of(0, 1, 2)))));

        return executePipelinePages(
                tables.tableFiles("item"),
                List.of("i_item_sk", "i_brand_id", "i_class_id", "i_category_id"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                14_19,
                                List.of(BIGINT, INTEGER, INTEGER, INTEGER),
                                List.of(1, 2, 3),
                                sharedTriples,
                                tripleTypes,
                                List.of(0, 1, 2))),
                        factoryStep(filterAndProjectFactory(14_20, Optional.empty(), List.of(field(0, BIGINT)), List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(14_21, List.of(BIGINT), List.of(0)))));
    }

    private List<Page> query14ChannelTriplesPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, int operatorIdBase)
    {
        List<Type> factTypes = tableColumnTypes(tables, salesTable, List.of(soldDateColumn, itemColumn));
        List<Type> itemTypes = List.of(BIGINT, INTEGER, INTEGER, INTEGER);
        List<Type> tripleTypes = List.of(INTEGER, INTEGER, INTEGER);
        return executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(soldDateColumn, itemColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_brand_id", "i_class_id", "i_category_id"),
                                        Optional.empty(),
                                        identityProjections(itemTypes),
                                        itemTypes),
                                itemTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, itemTypes),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(query14YearRangePredicate()),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(field(3, INTEGER), field(4, INTEGER), field(5, INTEGER)),
                                tripleTypes)),
                        factoryStep(hashAggregationFactory(operatorIdBase + 3, tripleTypes, List.of(0, 1, 2)))));
    }

    private List<Page> query14AverageSalesPages(TpcdsParquetTables tables)
    {
        List<Page> salesPages = new ArrayList<>(query14ChannelSalesValuePages(tables, "store_sales", "ss_sold_date_sk", "ss_quantity", "ss_list_price", 14_30));
        salesPages.addAll(query14ChannelSalesValuePages(tables, "catalog_sales", "cs_sold_date_sk", "cs_quantity", "cs_list_price", 14_40));
        salesPages.addAll(query14ChannelSalesValuePages(tables, "web_sales", "ws_sold_date_sk", "ws_quantity", "ws_list_price", 14_50));

        Type salesType = tableColumnTypes(tables, "store_sales", List.of("ss_list_price")).getFirst();
        salesType = multiply(cast(constant(1L, INTEGER), INTEGER, createDecimalType(10, 0)), field(0, salesType)).type();
        TestingAggregationFunction average = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(salesType));
        return executePipelinePages(
                salesPages,
                List.of(factoryStep(aggregationFactory(14_60, average.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))));
    }

    private List<Page> query14ChannelSalesValuePages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String quantityColumn, String listPriceColumn, int operatorIdBase)
    {
        List<Type> factTypes = tableColumnTypes(tables, salesTable, List.of(soldDateColumn, quantityColumn, listPriceColumn));
        Type quantityType = factTypes.get(1);
        Type priceType = factTypes.get(2);
        Type quantityDecimalType = createDecimalType(10, 0);
        RowExpression sales = multiply(
                cast(field(1, quantityType), quantityType, quantityDecimalType),
                field(2, priceType));
        return executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(soldDateColumn, quantityColumn, listPriceColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(query14YearRangePredicate()),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.empty(),
                                List.of(sales),
                                List.of(sales.type())))));
    }

    private List<Page> query14ChannelBranchPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String quantityColumn, String listPriceColumn, String channelName, int operatorIdBase, List<Page> crossItemPages, List<Type> crossItemTypes, List<Page> averageSalesPages, Type averageType)
    {
        List<Type> factTypes = tableColumnTypes(tables, salesTable, List.of(soldDateColumn, itemColumn, quantityColumn, listPriceColumn));
        Type quantityType = factTypes.get(2);
        Type priceType = factTypes.get(3);
        Type quantityDecimalType = createDecimalType(10, 0);
        RowExpression sales = multiply(
                cast(field(2, quantityType), quantityType, quantityDecimalType),
                field(3, priceType));
        Type salesType = sales.type();
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> groupedTypes = List.of(INTEGER, INTEGER, INTEGER, salesSum.getFinalType(), BIGINT);

        List<Page> groupedPages = executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(soldDateColumn, itemColumn, quantityColumn, listPriceColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, factTypes, List.of(1), crossItemPages, crossItemTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, crossItemTypes),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_brand_id", "i_class_id", "i_category_id"),
                                        Optional.empty(),
                                        identityProjections(List.of(BIGINT, INTEGER, INTEGER, INTEGER)),
                                        List.of(BIGINT, INTEGER, INTEGER, INTEGER)),
                                List.of(BIGINT, INTEGER, INTEGER, INTEGER),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 2,
                                concatTypes(concatTypes(factTypes, crossItemTypes), List.of(BIGINT, INTEGER, INTEGER, INTEGER)),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year", "d_moy"),
                                        Optional.of(and(equal(1, 2001, INTEGER), equal(2, 11, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 3,
                                Optional.empty(),
                                List.of(
                                        field(6, INTEGER),
                                        field(7, INTEGER),
                                        field(8, INTEGER),
                                        sales),
                                List.of(INTEGER, INTEGER, INTEGER, salesType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 4,
                                List.of(INTEGER, INTEGER, INTEGER),
                                List.of(0, 1, 2),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))));

        List<Page> joinedPages = executeNestedLoopPages(groupedPages, groupedTypes, averageSalesPages, List.of(averageType));
        return executePipelinePages(
                joinedPages,
                List.of(factoryStep(filterAndProjectFactory(
                        operatorIdBase + 5,
                        Optional.of(query14ThresholdPredicate(salesSum.getFinalType(), averageType)),
                        List.of(
                                constant(Slices.utf8Slice(channelName), VARCHAR),
                                field(0, INTEGER),
                                field(1, INTEGER),
                                field(2, INTEGER),
                                field(3, salesSum.getFinalType()),
                                field(4, BIGINT)),
                        List.of(VARCHAR, INTEGER, INTEGER, INTEGER, salesSum.getFinalType(), BIGINT)))));
    }

    private List<Page> query09BucketPages(TpcdsParquetTables tables, int minimumQuantityInclusive, int maximumQuantityInclusive, long threshold, int operatorIdBase)
    {
        Type bucketType = createDecimalType(7, 2);
        List<Page> countPages = executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_quantity"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase,
                                Optional.of(query09QuantityPredicate(0, minimumQuantityInclusive, maximumQuantityInclusive)),
                                List.of(),
                                List.of())),
                        factoryStep(aggregationFactory(
                                operatorIdBase + 1,
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))));
        List<Page> discountAveragePages = executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_quantity", "ss_ext_discount_amt"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.of(query09QuantityPredicate(0, minimumQuantityInclusive, maximumQuantityInclusive)),
                                List.of(field(1, bucketType)),
                                List.of(bucketType))),
                        factoryStep(aggregationFactory(
                                operatorIdBase + 3,
                                FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(bucketType)).createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))));
        List<Page> netPaidAveragePages = executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_quantity", "ss_net_paid"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 4,
                                Optional.of(query09QuantityPredicate(0, minimumQuantityInclusive, maximumQuantityInclusive)),
                                List.of(field(1, bucketType)),
                                List.of(bucketType))),
                        factoryStep(aggregationFactory(
                                operatorIdBase + 5,
                                FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(bucketType)).createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))));
        return executePipelinePages(
                executeNestedLoopPages(
                        executeNestedLoopPages(countPages, List.of(BIGINT), discountAveragePages, List.of(bucketType)),
                        List.of(BIGINT, bucketType),
                        netPaidAveragePages,
                        List.of(bucketType)),
                List.of(factoryStep(filterAndProjectFactory(
                        operatorIdBase + 6,
                        Optional.empty(),
                        List.of(ifExpression(
                                greaterThan(field(0, BIGINT), constant(threshold, BIGINT), BIGINT),
                                field(1, bucketType),
                                field(2, bucketType),
                                bucketType)),
                        List.of(bucketType)))));
    }

    private List<Page> query28BucketPages(TpcdsParquetTables tables, long minimumQuantityInclusive, long maximumQuantityInclusive, long minimumListPriceInclusive, long maximumListPriceInclusive, long minimumCouponAmountInclusive, long maximumCouponAmountInclusive, long minimumWholesaleCostInclusive, long maximumWholesaleCostInclusive, int operatorIdBase)
    {
        Type quantityType = tableColumnTypes(tables, "store_sales", List.of("ss_quantity")).getFirst();
        Type bucketType = tableColumnTypes(tables, "store_sales", List.of("ss_list_price")).getFirst();
        TestingAggregationFunction average = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(bucketType));
        RowExpression bucketPredicate = query28BucketPredicate(
                field(0, quantityType),
                field(1, bucketType),
                field(2, bucketType),
                field(3, bucketType),
                quantityType,
                bucketType,
                minimumQuantityInclusive,
                maximumQuantityInclusive,
                minimumListPriceInclusive,
                maximumListPriceInclusive,
                minimumCouponAmountInclusive,
                maximumCouponAmountInclusive,
                minimumWholesaleCostInclusive,
                maximumWholesaleCostInclusive);
        List<Page> averageAndCountPages = executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_wholesale_cost"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase,
                                Optional.of(and(
                                        bucketPredicate,
                                        equal(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(1, bucketType)), List.of()), constant(false, BOOLEAN), BOOLEAN))),
                                List.of(field(1, bucketType)),
                                List.of(bucketType))),
                        factoryStep(aggregationFactory(
                                operatorIdBase + 1,
                                average.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))));
        List<Page> distinctCountPages = executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_wholesale_cost"),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.of(and(
                                        bucketPredicate,
                                        equal(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(1, bucketType)), List.of()), constant(false, BOOLEAN), BOOLEAN))),
                                List.of(field(1, bucketType)),
                                List.of(bucketType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(bucketType),
                                List.of(0))),
                        factoryStep(aggregationFactory(
                                operatorIdBase + 4,
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))));
        return executeNestedLoopPages(averageAndCountPages, List.of(average.getFinalType(), BIGINT), distinctCountPages, List.of(BIGINT));
    }

    private static RowExpression query28BucketPredicate(RowExpression quantity, RowExpression listPrice, RowExpression couponAmount, RowExpression wholesaleCost, Type quantityType, Type priceType, long minimumQuantityInclusive, long maximumQuantityInclusive, long minimumListPriceInclusive, long maximumListPriceInclusive, long minimumCouponAmountInclusive, long maximumCouponAmountInclusive, long minimumWholesaleCostInclusive, long maximumWholesaleCostInclusive)
    {
        return and(
                betweenInclusive(quantity, minimumQuantityInclusive, maximumQuantityInclusive, quantityType),
                or(
                        betweenInclusive(listPrice, minimumListPriceInclusive, maximumListPriceInclusive, priceType),
                        betweenInclusive(couponAmount, minimumCouponAmountInclusive, maximumCouponAmountInclusive, priceType),
                        betweenInclusive(wholesaleCost, minimumWholesaleCostInclusive, maximumWholesaleCostInclusive, priceType)));
    }

    private List<Page> query23CustomerSalesPages(TpcdsParquetTables tables, boolean filterYears, int operatorIdBase)
    {
        List<String> factColumns = filterYears
                ? List.of("ss_customer_sk", "ss_sold_date_sk", "ss_quantity", "ss_sales_price")
                : List.of("ss_customer_sk", "ss_quantity", "ss_sales_price");
        List<Type> factTypes = tableColumnTypes(tables, "store_sales", factColumns);
        Type priceType = factTypes.get(filterYears ? 3 : 2);
        Type quantityDecimalType = createDecimalType(10, 0);
        Type salesType = createDecimalType(17, 2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<PipelineStep> steps = new ArrayList<>();

        if (filterYears) {
            steps.add(hashJoinStep(new HashJoinSpec(
                    operatorIdBase,
                    factTypes,
                    List.of(1),
                    relationPages(
                            tables,
                            "date_dim",
                            List.of("d_date_sk", "d_year"),
                            Optional.of(query23YearRangePredicate()),
                            List.of(field(0, BIGINT)),
                            List.of(BIGINT)),
                    List.of(BIGINT),
                    List.of(0))));
        }

        List<Type> afterDateTypes = filterYears ? concatTypes(factTypes, List.of(BIGINT)) : factTypes;
        steps.add(hashJoinStep(new HashJoinSpec(
                operatorIdBase + 1,
                afterDateTypes,
                List.of(0),
                relationPages(
                        tables,
                        "customer",
                        List.of("c_customer_sk"),
                        Optional.empty(),
                        List.of(field(0, BIGINT)),
                        List.of(BIGINT)),
                List.of(BIGINT),
                List.of(0))));
        List<Type> afterCustomerTypes = concatTypes(afterDateTypes, List.of(BIGINT));
        steps.add(factoryStep(filterAndProjectFactory(
                operatorIdBase + 2,
                Optional.empty(),
                List.of(
                        field(0, BIGINT),
                        coalesce(
                                new CallExpression(
                                        FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(priceType, quantityDecimalType)),
                                        List.of(
                                                field(filterYears ? 3 : 2, priceType),
                                                new CallExpression(FUNCTION_RESOLUTION.getCoercion(INTEGER, quantityDecimalType), List.of(field(filterYears ? 2 : 1, INTEGER))))),
                                constant(0L, salesType),
                                salesType)),
                List.of(BIGINT, salesType))));
        steps.add(factoryStep(hashAggregationFactory(
                operatorIdBase + 3,
                List.of(BIGINT),
                List.of(0),
                salesSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))));
        return executePipelinePages(tables.tableFiles("store_sales"), factColumns, steps);
    }

    private Type query31RevenueType(TpcdsParquetTables tables, String salesTable, String salesColumn)
    {
        Type revenueType = tableColumnTypes(tables, salesTable, List.of(salesColumn)).getFirst();
        return FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(revenueType)).getFinalType();
    }

    private List<Type> query11BranchTypes(TpcdsParquetTables tables, String salesTable, String listPriceColumn)
    {
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_country", "c_login"));
        Type listPriceType = tableColumnTypes(tables, salesTable, List.of(listPriceColumn)).getFirst();
        Type totalType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(listPriceType)).getFinalType();
        return List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4), customerTypes.get(5), customerTypes.get(6), totalType);
    }

    private List<Type> query04BranchTypes(TpcdsParquetTables tables)
    {
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag"));
        return List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4), BIGINT);
    }

    private List<Page> query04ChannelYearTotalPages(TpcdsParquetTables tables, String salesTable, String customerColumn, String soldDateColumn, String listPriceColumn, String wholesaleCostColumn, String discountColumn, String salesPriceColumn, int year, int operatorIdBase)
    {
        List<String> factColumns = List.of(customerColumn, soldDateColumn, listPriceColumn, wholesaleCostColumn, discountColumn, salesPriceColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag"));
        List<Type> projectedTypes = List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4), BIGINT);

        RowExpression listPrice = cast(
                multiply(field(2, factTypes.get(2)), constant(100L, createDecimalType(3, 0))),
                multiply(field(2, factTypes.get(2)), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression wholesaleCost = cast(
                multiply(field(3, factTypes.get(3)), constant(100L, createDecimalType(3, 0))),
                multiply(field(3, factTypes.get(3)), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression discount = cast(
                multiply(field(4, factTypes.get(4)), constant(100L, createDecimalType(3, 0))),
                multiply(field(4, factTypes.get(4)), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression salesPrice = cast(
                multiply(field(5, factTypes.get(5)), constant(100L, createDecimalType(3, 0))),
                multiply(field(5, factTypes.get(5)), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression yearTotal = add(subtract(subtract(listPrice, wholesaleCost, BIGINT), discount, BIGINT), salesPrice, BIGINT);

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "customer",
                                        List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag"),
                                        Optional.empty(),
                                        identityProjections(customerTypes),
                                        customerTypes),
                                customerTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, customerTypes),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(equal(1, year, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(
                                        field(7, customerTypes.get(1)),
                                        field(8, customerTypes.get(2)),
                                        field(9, customerTypes.get(3)),
                                        field(10, customerTypes.get(4)),
                                        yearTotal),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4)),
                                List.of(0, 1, 2, 3),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty())))));
    }

    private List<Page> query11ChannelYearTotalPages(TpcdsParquetTables tables, String salesTable, String customerColumn, String soldDateColumn, String listPriceColumn, String discountColumn, int year, int operatorIdBase)
    {
        List<String> factColumns = List.of(customerColumn, soldDateColumn, listPriceColumn, discountColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        Type listPriceType = factTypes.get(2);
        Type discountType = factTypes.get(3);
        TestingAggregationFunction totalSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(listPriceType));
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_country", "c_login"));
        List<Type> projectedTypes = List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4), customerTypes.get(5), customerTypes.get(6), listPriceType);

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "customer",
                                        List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_country", "c_login"),
                                        Optional.empty(),
                                        identityProjections(customerTypes),
                                        customerTypes),
                                customerTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, customerTypes),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(equal(1, year, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(
                                        field(5, customerTypes.get(1)),
                                        field(6, customerTypes.get(2)),
                                        field(7, customerTypes.get(3)),
                                        field(8, customerTypes.get(4)),
                                        field(9, customerTypes.get(5)),
                                        field(10, customerTypes.get(6)),
                                        subtract(field(2, listPriceType), field(3, discountType), listPriceType)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), customerTypes.get(4), customerTypes.get(5), customerTypes.get(6)),
                                List.of(0, 1, 2, 3, 4, 5),
                                totalSum.createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty())))));
    }

    private List<Page> query76ChannelCategorySalesPages(TpcdsParquetTables tables, String channelName, String columnName, String salesTable, String nullableColumn, String soldDateColumn, String itemColumn, String salesColumn, int operatorIdBase)
    {
        List<String> factColumns = List.of(nullableColumn, soldDateColumn, itemColumn, salesColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        Type salesType = factTypes.get(3);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> projectedTypes = List.of(VARCHAR, VARCHAR, INTEGER, INTEGER, VARCHAR, salesType);
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR, INTEGER, INTEGER, VARCHAR, BIGINT, salesSum.getFinalType());

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase,
                                Optional.of(new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(0, factTypes.getFirst())), List.of())),
                                List.of(field(1, BIGINT), field(2, BIGINT), field(3, salesType)),
                                List.of(BIGINT, BIGINT, salesType))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                List.of(BIGINT, BIGINT, salesType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year", "d_qoy"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, INTEGER), field(2, INTEGER)),
                                        List.of(BIGINT, INTEGER, INTEGER)),
                                List.of(BIGINT, INTEGER, INTEGER),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 2,
                                concatTypes(List.of(BIGINT, BIGINT, salesType), List.of(BIGINT, INTEGER, INTEGER)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_category"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 3,
                                Optional.empty(),
                                List.of(
                                        constant(Slices.utf8Slice(channelName), VARCHAR),
                                        constant(Slices.utf8Slice(columnName), VARCHAR),
                                        field(4, INTEGER),
                                        field(5, INTEGER),
                                        field(7, VARCHAR),
                                        coalesce(field(2, salesType), cast(constant(0L, BIGINT), BIGINT, salesType), salesType)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 4,
                                List.of(VARCHAR, VARCHAR, INTEGER, INTEGER, VARCHAR),
                                List.of(0, 1, 2, 3, 4),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty())))));
    }

    private List<Page> query74ChannelYearTotalPages(TpcdsParquetTables tables, String salesTable, String customerColumn, String soldDateColumn, String netPaidColumn, int year, int operatorIdBase)
    {
        List<String> factColumns = List.of(customerColumn, soldDateColumn, netPaidColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        Type netPaidType = factTypes.get(2);
        RowExpression netPaid = cast(
                multiply(field(2, netPaidType), constant(100L, createDecimalType(3, 0))),
                multiply(field(2, netPaidType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name"));
        List<Type> projectedTypes = List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), BIGINT);

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "customer",
                                        List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name"),
                                        Optional.empty(),
                                        identityProjections(customerTypes),
                                        customerTypes),
                                customerTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, customerTypes),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year"),
                                        Optional.of(equal(1, year, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(
                                        field(4, customerTypes.get(1)),
                                        field(5, customerTypes.get(2)),
                                        field(6, customerTypes.get(3)),
                                        netPaid),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3)),
                                List.of(0, 1, 2),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty())))));
    }

    private List<Page> query87ChannelPresencePages(TpcdsParquetTables tables, String salesTable, String customerColumn, String soldDateColumn, int activeChannel, int operatorIdBase)
    {
        List<String> factColumns = List.of(customerColumn, soldDateColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name"));
        List<Type> projectedTypes = List.of(VARCHAR, VARCHAR, DATE, BIGINT, BIGINT, BIGINT);

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "customer",
                                        List.of("c_customer_sk", "c_last_name", "c_first_name"),
                                        Optional.empty(),
                                        identityProjections(customerTypes),
                                        customerTypes),
                                customerTypes,
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, customerTypes),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_month_seq", "d_date"),
                                        Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                                        List.of(field(0, BIGINT), field(2, DATE)),
                                        List.of(BIGINT, DATE)),
                                List.of(BIGINT, DATE),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(
                                        field(3, VARCHAR),
                                        field(4, VARCHAR),
                                        field(6, DATE),
                                        constant(activeChannel == 0 ? 1L : 0L, BIGINT),
                                        constant(activeChannel == 1 ? 1L : 0L, BIGINT),
                                        constant(activeChannel == 2 ? 1L : 0L, BIGINT)),
                                projectedTypes)),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(VARCHAR, VARCHAR, DATE),
                                List.of(0, 1, 2),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty())))));
    }

    private List<Page> query31ChannelCountyQuarterRevenuePages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String addressColumn, String salesColumn, int quarter, int operatorIdBase)
    {
        List<String> factColumns = List.of(soldDateColumn, addressColumn, salesColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        Type revenueType = factTypes.get(2);
        TestingAggregationFunction revenueSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(revenueType));

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year", "d_qoy"),
                                        Optional.of(and(equal(1, 2000, INTEGER), equal(2, quarter, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, List.of(BIGINT)),
                                List.of(1),
                                relationPages(
                                        tables,
                                        "customer_address",
                                        List.of("ca_address_sk", "ca_county"),
                                        Optional.empty(),
                                        List.of(field(0, BIGINT), field(1, VARCHAR)),
                                        List.of(BIGINT, VARCHAR)),
                                List.of(BIGINT, VARCHAR),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(field(5, VARCHAR), field(2, revenueType)),
                                List.of(VARCHAR, revenueType))),
                        factoryStep(hashAggregationFactory(
                                operatorIdBase + 3,
                                List.of(VARCHAR),
                                List.of(0),
                                revenueSum.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))));
    }

    private List<Page> query23BestCustomerPages(TpcdsParquetTables tables)
    {
        Type lineSalesType = createDecimalType(17, 2);
        Type customerSalesType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(lineSalesType)).getFinalType();
        List<Page> allSalesPages = query23CustomerSalesPages(tables, false, 23_20);
        TestingAggregationFunction salesMax = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(customerSalesType));
        List<Page> maxSalesPages = executePipelinePages(
                query23CustomerSalesPages(tables, true, 23_30),
                List.of(factoryStep(aggregationFactory(23_34, salesMax.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))));
        return executePipelinePages(
                executeNestedLoopPages(allSalesPages, List.of(BIGINT, customerSalesType), maxSalesPages, List.of(customerSalesType)),
                List.of(
                        factoryStep(filterAndProjectFactory(
                                23_35,
                                Optional.of(query23BestCustomerPredicate(customerSalesType)),
                                List.of(field(0, BIGINT)),
                                List.of(BIGINT))),
                        factoryStep(hashAggregationFactory(23_36, List.of(BIGINT), List.of(0)))));
    }

    private List<Page> query23ChannelPages(TpcdsParquetTables tables, String salesTable, String soldDateColumn, String customerColumn, String itemColumn, String quantityColumn, String listPriceColumn, int operatorIdBase)
    {
        List<String> factColumns = List.of(soldDateColumn, customerColumn, itemColumn, quantityColumn, listPriceColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        Type priceType = factTypes.get(4);
        Type quantityDecimalType = createDecimalType(10, 0);
        Type salesType = createDecimalType(17, 2);

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_year", "d_moy"),
                                        Optional.of(and(equal(1, 2000, INTEGER), equal(2, 2, INTEGER))),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        semiJoinPagesStep(new SemiJoinPagesSpec(operatorIdBase + 1, concatTypes(factTypes, List.of(BIGINT)), 2, query23FrequentItemPages(tables), List.of(BIGINT), 0)),
                        factoryStep(semiJoinFilterProjectFactory(operatorIdBase + 2, concatTypes(factTypes, List.of(BIGINT)), true)),
                        semiJoinPagesStep(new SemiJoinPagesSpec(operatorIdBase + 3, concatTypes(factTypes, List.of(BIGINT)), 1, query23BestCustomerPages(tables), List.of(BIGINT), 0)),
                        factoryStep(semiJoinFilterProjectFactory(operatorIdBase + 4, concatTypes(factTypes, List.of(BIGINT)), true)),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 5,
                                Optional.empty(),
                                List.of(coalesce(
                                        new CallExpression(
                                                FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(priceType, quantityDecimalType)),
                                                List.of(
                                                        field(4, priceType),
                                                        new CallExpression(FUNCTION_RESOLUTION.getCoercion(INTEGER, quantityDecimalType), List.of(field(3, INTEGER))))),
                                        constant(0L, salesType),
                                        salesType)),
                                List.of(salesType)))));
    }

    private List<Page> query24SalesPages(TpcdsParquetTables tables, Optional<String> colorFilter)
    {
        Type widenedItemPriceType = createDecimalType(19, 2);
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_market_id", "s_store_name", "s_state", "s_zip"));
        List<Type> filteredStoreTypes = List.of(storeTypes.get(0), storeTypes.get(2), storeTypes.get(3), BIGINT);
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_current_price", "i_size", "i_color", "i_units", "i_manager_id"));
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name", "c_birth_country"));
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_zip", "ca_state", "ca_country"));
        RowExpression upperCountry = upper(field(2, addressTypes.get(2)));
        List<Type> projectedAddressTypes = List.of(BIGINT, upperCountry.type(), addressTypes.get(1));
        Type preAggregatedRevenueType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(createDecimalType(19, 2))).getFinalType();
        TestingAggregationFunction groupedRevenueSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(preAggregatedRevenueType));

        List<Type> customerStoreItemTypes = List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), BIGINT, BIGINT, preAggregatedRevenueType);
        List<Type> afterStoreTypes = concatTypes(customerStoreItemTypes, filteredStoreTypes);
        List<Type> afterItemTypes = concatTypes(afterStoreTypes, itemTypes);

        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_market_id", "s_store_name", "s_state", "s_zip"),
                Optional.of(equal(1, 8, INTEGER)),
                List.of(field(0, storeTypes.get(0)), field(2, storeTypes.get(2)), field(3, storeTypes.get(3)), cast(field(4, storeTypes.get(4)), storeTypes.get(4), BIGINT)),
                filteredStoreTypes);
        List<Page> itemPages = relationPages(
                tables,
                "item",
                List.of("i_item_sk", "i_current_price", "i_size", "i_color", "i_units", "i_manager_id"),
                colorFilter.map(color -> equal(field(3, itemTypes.get(3)), constant(Slices.utf8Slice(color), itemTypes.get(3)), itemTypes.get(3))),
                identityProjections(itemTypes),
                itemTypes);
        List<Page> addressPages = query24AddressLookupPages(tables);

        List<Page> customerStoreItemPages = query24CustomerStoreItemPages(tables);
        List<PipelineStep> steps = new ArrayList<>(List.of(
                hashJoinStep(new HashJoinSpec(24_01, customerStoreItemTypes, List.of(3), storePages, filteredStoreTypes, List.of(0))),
                hashJoinStep(new HashJoinSpec(24_02, afterStoreTypes, List.of(4), itemPages, itemTypes, List.of(0)))));
        steps.add(hashJoinStep(new HashJoinSpec(24_04, afterItemTypes, List.of(9), addressPages, projectedAddressTypes, List.of(0))));
        steps.add(factoryStep(filterAndProjectFactory(
                24_045,
                Optional.of(equal(field(2, customerTypes.get(3)), field(17, projectedAddressTypes.get(1)), projectedAddressTypes.get(1))),
                List.of(
                        field(0, customerTypes.get(1)),
                        field(1, customerTypes.get(2)),
                        field(7, filteredStoreTypes.get(1)),
                        field(18, projectedAddressTypes.get(2)),
                        field(8, filteredStoreTypes.get(2)),
                        field(13, itemTypes.get(3)),
                        cast(field(11, itemTypes.get(1)), itemTypes.get(1), widenedItemPriceType),
                        field(15, itemTypes.get(5)),
                        field(14, itemTypes.get(4)),
                        field(12, itemTypes.get(2)),
                        field(5, preAggregatedRevenueType)),
                List.of(customerTypes.get(1), customerTypes.get(2), filteredStoreTypes.get(1), projectedAddressTypes.get(2), filteredStoreTypes.get(2), itemTypes.get(3), widenedItemPriceType, itemTypes.get(5), itemTypes.get(4), itemTypes.get(2), preAggregatedRevenueType))));
        steps.add(factoryStep(hashAggregationFactory(
                24_05,
                List.of(customerTypes.get(1), customerTypes.get(2), filteredStoreTypes.get(1), projectedAddressTypes.get(2), filteredStoreTypes.get(2), itemTypes.get(3), widenedItemPriceType, itemTypes.get(5), itemTypes.get(4), itemTypes.get(2)),
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                groupedRevenueSum.createAggregatorFactory(Step.SINGLE, List.of(10), OptionalInt.empty()))));
        return executePipelinePages(customerStoreItemPages, steps);
    }

    private List<Page> query24AverageSalesPages(TpcdsParquetTables tables)
    {
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name", "c_birth_country"));
        Type preAggregatedRevenueType = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(createDecimalType(19, 2))).getFinalType();
        TestingAggregationFunction groupedRevenueSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(preAggregatedRevenueType));
        List<Type> storeTypes = tableColumnTypes(tables, "store", List.of("s_store_sk", "s_market_id", "s_store_name", "s_state", "s_zip"));
        List<Type> slimStoreTypes = List.of(storeTypes.get(0), BIGINT);
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_zip", "ca_state", "ca_country"));
        RowExpression upperCountry = upper(field(2, addressTypes.get(2)));
        List<Type> projectedAddressTypes = List.of(BIGINT, upperCountry.type(), addressTypes.get(1));

        List<Type> customerStoreItemTypes = List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), BIGINT, BIGINT, preAggregatedRevenueType);
        List<Type> afterStoreTypes = concatTypes(customerStoreItemTypes, slimStoreTypes);

        List<Page> storePages = relationPages(
                tables,
                "store",
                List.of("s_store_sk", "s_market_id", "s_store_name", "s_state", "s_zip"),
                Optional.of(equal(1, 8, INTEGER)),
                List.of(field(0, storeTypes.get(0)), cast(field(4, storeTypes.get(4)), storeTypes.get(4), BIGINT)),
                slimStoreTypes);
        List<Page> addressPages = query24AddressLookupPages(tables);

        return executePipelinePages(
                query24CustomerStoreItemPages(tables),
                List.of(
                        hashJoinStep(new HashJoinSpec(24_207, customerStoreItemTypes, List.of(3), storePages, slimStoreTypes, List.of(0))),
                        hashJoinStep(new HashJoinSpec(24_209, afterStoreTypes, List.of(7), addressPages, projectedAddressTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                24_210,
                                Optional.of(equal(field(2, customerTypes.get(3)), field(9, projectedAddressTypes.get(1)), projectedAddressTypes.get(1))),
                                List.of(
                                        field(0, customerTypes.get(1)),
                                        field(1, customerTypes.get(2)),
                                        field(10, projectedAddressTypes.get(2)),
                                        field(3, BIGINT),
                                        field(4, BIGINT),
                                        field(5, preAggregatedRevenueType)),
                                List.of(customerTypes.get(1), customerTypes.get(2), projectedAddressTypes.get(2), BIGINT, BIGINT, preAggregatedRevenueType))),
                        factoryStep(hashAggregationFactory(
                                24_211,
                                List.of(customerTypes.get(1), customerTypes.get(2), projectedAddressTypes.get(2), BIGINT, BIGINT),
                                List.of(0, 1, 2, 3, 4),
                                groupedRevenueSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty())))));
    }

    private List<Page> query24CustomerStoreItemPages(TpcdsParquetTables tables)
    {
        List<Type> salesTypes = tableColumnTypes(tables, "store_sales", List.of("ss_ticket_number", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_net_paid"));
        Type salesPriceType = salesTypes.get(4);
        Type widenedSalesType = createDecimalType(19, 2);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(widenedSalesType));
        List<Type> returnsTypes = tableColumnTypes(tables, "store_returns", List.of("sr_ticket_number", "sr_item_sk"));
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name", "c_birth_country"));
        List<Type> afterReturnsTypes = concatTypes(salesTypes, returnsTypes);

        List<Page> returnsPages = relationPages(
                tables,
                "store_returns",
                List.of("sr_ticket_number", "sr_item_sk"),
                Optional.empty(),
                identityProjections(returnsTypes),
                returnsTypes);
        List<Page> customerPages = relationPages(
                tables,
                "customer",
                List.of("c_customer_sk", "c_last_name", "c_first_name", "c_birth_country"),
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes);

        return executePipelinePages(
                tables.tableFiles("store_sales"),
                List.of("ss_ticket_number", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_net_paid"),
                List.of(
                        hashJoinStep(new HashJoinSpec(24_000, salesTypes, List.of(0, 1), returnsPages, returnsTypes, List.of(0, 1))),
                        hashJoinStep(new HashJoinSpec(24_003, afterReturnsTypes, List.of(2), customerPages, customerTypes, List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                24_001,
                                Optional.empty(),
                                List.of(
                                        field(8, customerTypes.get(1)),
                                        field(9, customerTypes.get(2)),
                                        field(10, customerTypes.get(3)),
                                        field(3, BIGINT),
                                        field(1, BIGINT),
                                        cast(field(4, salesPriceType), salesPriceType, widenedSalesType)),
                                List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), BIGINT, BIGINT, widenedSalesType))),
                        factoryStep(hashAggregationFactory(
                                24_002,
                                List.of(customerTypes.get(1), customerTypes.get(2), customerTypes.get(3), BIGINT, BIGINT),
                                List.of(0, 1, 2, 3, 4),
                                salesSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty())))));
    }

    private List<Page> query24AddressLookupPages(TpcdsParquetTables tables)
    {
        List<Type> addressTypes = tableColumnTypes(tables, "customer_address", List.of("ca_zip", "ca_state", "ca_country"));
        RowExpression upperCountry = upper(field(2, addressTypes.get(2)));
        List<Type> projectedAddressTypes = List.of(BIGINT, upperCountry.type(), addressTypes.get(1));
        List<Page> rawAddressPages = relationPages(
                tables,
                "customer_address",
                List.of("ca_zip", "ca_state", "ca_country"),
                Optional.empty(),
                List.of(cast(field(0, addressTypes.get(0)), addressTypes.get(0), BIGINT), upperCountry, field(1, addressTypes.get(1))),
                projectedAddressTypes);
        return executePipelinePages(
                rawAddressPages,
                List.of(
                        factoryStep(hashAggregationFactory(
                                24_012,
                                projectedAddressTypes,
                                List.of(0, 1, 2),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        factoryStep(filterAndProjectFactory(
                                24_013,
                                Optional.empty(),
                                List.of(field(0, projectedAddressTypes.get(0)), field(1, projectedAddressTypes.get(1)), field(2, projectedAddressTypes.get(2))),
                                projectedAddressTypes))));
    }

    private Type query81StateType(TpcdsParquetTables tables)
    {
        return tableColumnTypes(tables, "customer_address", List.of("ca_state")).getFirst();
    }

    private Type query81ReturnSumType(TpcdsParquetTables tables)
    {
        Type returnAmountType = tableColumnTypes(tables, "catalog_returns", List.of("cr_return_amt_inc_tax")).getFirst();
        return FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType)).getFinalType();
    }

    private Type query81StateAverageType(TpcdsParquetTables tables)
    {
        return FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(query81ReturnSumType(tables))).getFinalType();
    }

    private Type query30ReturnSumType(TpcdsParquetTables tables)
    {
        Type returnAmountType = tableColumnTypes(tables, "web_returns", List.of("wr_return_amt")).getFirst();
        return FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(returnAmountType)).getFinalType();
    }

    private Type query30StateAverageType(TpcdsParquetTables tables)
    {
        return FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(query30ReturnSumType(tables))).getFinalType();
    }

    private List<Page> query97ChannelPages(TpcdsParquetTables tables, String salesTable, String customerColumn, String itemColumn, String soldDateColumn, int operatorIdBase)
    {
        List<String> factColumns = List.of(customerColumn, itemColumn, soldDateColumn);
        List<Type> factTypes = tableColumnTypes(tables, salesTable, factColumns);
        List<Page> allowedDates = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_month_seq"),
                Optional.of(and(greaterThan(1, 1199, INTEGER), lessThan(1, 1212, INTEGER))),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));

        return executePipelinePages(
                tables.tableFiles(salesTable),
                factColumns,
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, factTypes, List.of(2), allowedDates, List.of(BIGINT), List.of(0))),
                        factoryStep(hashAggregationFactory(operatorIdBase + 1, List.of(BIGINT, BIGINT), List.of(0, 1)))));
    }

    private List<Page> query01CustomerStoreReturnsPages(TpcdsParquetTables tables)
    {
        return executePipelinePages(
                query01CustomerStoreReturnsWithValueCountsPages(tables),
                List.of(factoryStep(filterAndProjectFactory(
                        9_1,
                        Optional.empty(),
                        List.of(field(0, BIGINT), field(1, BIGINT), field(2, BIGINT)),
                        List.of(BIGINT, BIGINT, BIGINT)))));
    }

    private List<Page> query01CustomerStoreReturnsWithValueCountsPages(TpcdsParquetTables tables)
    {
        List<Page> year2000DateKeys = relationPages(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year"),
                Optional.of(equal(1, 2000, INTEGER)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT));
        return executePipelinePages(
                tables.tableFiles("store_returns"),
                List.of("sr_customer_sk", "sr_store_sk", "sr_return_amt", "sr_returned_date_sk"),
                List.of(
                        hashJoinStep(new HashJoinSpec(8, List.of(BIGINT, BIGINT, BIGINT, BIGINT), List.of(3), year2000DateKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                8_1,
                                Optional.empty(),
                                List.of(
                                        field(0, BIGINT),
                                        field(1, BIGINT),
                                        field(2, BIGINT),
                                        ifExpression(
                                                new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(2, BIGINT)), List.of()),
                                                constant(0L, BIGINT),
                                                constant(1L, BIGINT),
                                                BIGINT)),
                                List.of(BIGINT, BIGINT, BIGINT, BIGINT))),
                        factoryStep(hashAggregationFactory(
                                9,
                                List.of(BIGINT, BIGINT),
                                List.of(0, 1),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty())))));
    }

    private List<Type> tableColumnTypes(TpcdsParquetTables tables, String tableName, List<String> columns)
    {
        return TrinoClickBenchPageReader.columnTypes(tables.tableFiles(tableName).getFirst(), columns);
    }

    private List<Page> customerDemographicsBuildPages(TpcdsParquetTables tables)
    {
        MaterializedResult result = scanTable(
                tables,
                "customer_demographics",
                List.of("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"),
                tableColumnTypes(tables, "customer_demographics", List.of("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count")));
        PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT));
        List<Page> pages = new ArrayList<>();
        result.getMaterializedRows().forEach(row -> {
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), ((Number) row.getField(0)).longValue());
            writeJoinVarchar(pageBuilder.getBlockBuilder(1), (String) row.getField(1));
            writeJoinVarchar(pageBuilder.getBlockBuilder(2), (String) row.getField(2));
            writeJoinVarchar(pageBuilder.getBlockBuilder(3), (String) row.getField(3));
            BIGINT.writeLong(pageBuilder.getBlockBuilder(4), ((Number) row.getField(4)).longValue());
            writeJoinVarchar(pageBuilder.getBlockBuilder(5), (String) row.getField(5));
            BIGINT.writeLong(pageBuilder.getBlockBuilder(6), ((Number) row.getField(6)).longValue());
            BIGINT.writeLong(pageBuilder.getBlockBuilder(7), ((Number) row.getField(7)).longValue());
            BIGINT.writeLong(pageBuilder.getBlockBuilder(8), ((Number) row.getField(8)).longValue());
        });
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    private List<Page> customerAddressStateBuildPages(TpcdsParquetTables tables)
    {
        MaterializedResult result = scanTable(
                tables,
                "customer_address",
                List.of("ca_address_sk", "ca_state"),
                tableColumnTypes(tables, "customer_address", List.of("ca_address_sk", "ca_state")));
        PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT, VARCHAR));
        List<Page> pages = new ArrayList<>();
        result.getMaterializedRows().forEach(row -> {
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), ((Number) row.getField(0)).longValue());
            writeJoinVarchar(pageBuilder.getBlockBuilder(1), (String) row.getField(1));
        });
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    private List<Page> customerIdentityBuildPages(TpcdsParquetTables tables)
    {
        MaterializedResult result = scanTable(
                tables,
                "customer",
                List.of("c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag"),
                tableColumnTypes(tables, "customer", List.of("c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag")));
        PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR));
        List<Page> pages = new ArrayList<>();
        result.getMaterializedRows().forEach(row -> {
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), ((Number) row.getField(0)).longValue());
            writeJoinVarchar(pageBuilder.getBlockBuilder(1), (String) row.getField(1));
            writeJoinVarchar(pageBuilder.getBlockBuilder(2), (String) row.getField(2));
            writeJoinVarchar(pageBuilder.getBlockBuilder(3), (String) row.getField(3));
            writeJoinVarchar(pageBuilder.getBlockBuilder(4), (String) row.getField(4));
        });
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    private List<Page> storeReturnCustomerDemographicsBuildPages(TpcdsParquetTables tables)
    {
        MaterializedResult result = scanTable(
                tables,
                "store_returns",
                List.of("sr_cdemo_sk"),
                tableColumnTypes(tables, "store_returns", List.of("sr_cdemo_sk")));
        PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT));
        List<Page> pages = new ArrayList<>();
        result.getMaterializedRows().forEach(row -> {
            if (row.getField(0) == null) {
                return;
            }
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), ((Number) row.getField(0)).longValue());
        });
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    private long query90Count(TpcdsParquetTables tables, List<Page> timeKeys, List<Page> householdKeys, List<Page> pageKeys)
    {
        List<String> columns = List.of("ws_sold_time_sk", "ws_ship_hdemo_sk", "ws_web_page_sk");
        List<Type> factTypes = tableColumnTypes(tables, "web_sales", columns);
        List<Type> afterTimeTypes = concatTypes(factTypes, List.of(BIGINT));
        List<Type> afterHouseholdTypes = concatTypes(afterTimeTypes, List.of(BIGINT));
        return singleLongResult(executePipeline(
                tables.tableFiles("web_sales"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(90_0, factTypes, List.of(0), timeKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(90_1, afterTimeTypes, List.of(1), householdKeys, List.of(BIGINT), List.of(0))),
                        hashJoinStep(new HashJoinSpec(90_2, afterHouseholdTypes, List.of(2), pageKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(aggregationFactory(90_3, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                List.of(BIGINT)));
    }

    private List<Page> query92FilteredDiscountPages(TpcdsParquetTables tables, int operatorIdBase)
    {
        List<String> columns = List.of("ws_sold_date_sk", "ws_item_sk", "ws_ext_discount_amt");
        List<Type> factTypes = tableColumnTypes(tables, "web_sales", columns);
        Type discountType = factTypes.get(2);

        return executePipelinePages(
                tables.tableFiles("web_sales"),
                columns,
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase,
                                factTypes,
                                List.of(1),
                                relationPages(
                                        tables,
                                        "item",
                                        List.of("i_item_sk", "i_manufact_id"),
                                        Optional.of(equal(1, 350, INTEGER)),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        hashJoinStep(new HashJoinSpec(
                                operatorIdBase + 1,
                                concatTypes(factTypes, List.of(BIGINT)),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_date"),
                                        Optional.of(query92DatePredicate()),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 2,
                                Optional.empty(),
                                List.of(field(1, BIGINT), field(2, discountType)),
                                List.of(BIGINT, discountType)))));
    }

    private List<Page> query92ItemAveragePages(TpcdsParquetTables tables)
    {
        Type discountType = tableColumnTypes(tables, "web_sales", List.of("ws_ext_discount_amt")).getFirst();
        TestingAggregationFunction discountAverage = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(discountType));
        return executePipelinePages(
                tables.tableFiles("web_sales"),
                List.of("ws_sold_date_sk", "ws_item_sk", "ws_ext_discount_amt"),
                List.of(
                        hashJoinStep(new HashJoinSpec(
                                92_110,
                                List.of(BIGINT, BIGINT, discountType),
                                List.of(0),
                                relationPages(
                                        tables,
                                        "date_dim",
                                        List.of("d_date_sk", "d_date"),
                                        Optional.of(query92DatePredicate()),
                                        List.of(field(0, BIGINT)),
                                        List.of(BIGINT)),
                                List.of(BIGINT),
                                List.of(0))),
                        factoryStep(hashAggregationFactory(
                                92_111,
                                List.of(BIGINT),
                                List.of(1),
                                discountAverage.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))));
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

    private static List<Type> concatTypes(List<Type> left, List<Type> right)
    {
        List<Type> types = new ArrayList<>(left.size() + right.size());
        types.addAll(left);
        types.addAll(right);
        return types;
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

    private OperatorFactory enforceSingleRowFactory(int operatorId, List<Type> types)
    {
        return new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(
                operatorId,
                new PlanNodeId("enforce-single-row-" + operatorId),
                types);
    }

    private OperatorFactory groupIdFactory(int operatorId, List<Type> outputTypes, List<Map<Integer, Integer>> groupingSetMappings)
    {
        return new GroupIdOperator.GroupIdOperatorFactory(
                operatorId,
                new PlanNodeId("groupid-" + operatorId),
                outputTypes,
                groupingSetMappings);
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

    private OperatorFactory topNRankingFactory(int operatorId, List<Type> sourceTypes, List<Integer> outputChannels, List<Integer> partitionChannels, List<Integer> sortChannels, List<SortOrder> sortOrders, int limit)
    {
        List<Type> sortTypes = sortChannels.stream()
                .map(sourceTypes::get)
                .toList();
        List<Type> partitionTypes = partitionChannels.stream()
                .map(sourceTypes::get)
                .toList();
        return new TopNRankingOperator.TopNRankingOperatorFactory(
                operatorId,
                new PlanNodeId("topn-ranking-" + operatorId),
                RANK,
                sourceTypes,
                outputChannels,
                partitionChannels,
                partitionTypes,
                sortChannels,
                limit,
                false,
                100_000,
                Optional.empty(),
                hashStrategyCompiler,
                orderingCompiler.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrders),
                new BlockTypeOperators());
    }

    private OperatorFactory windowFactory(int operatorId, List<Type> sourceTypes, List<Integer> outputChannels, List<Integer> partitionChannels, List<Integer> sortChannels, List<SortOrder> sortOrders, List<WindowFunctionDefinition> windowFunctions)
    {
        SpillerFactory spillerFactory = (types, spillContext, aggregatedMemoryContext) -> {
            throw new UnsupportedOperationException("Window spilling is disabled in TrinoTpcdsParquetSupport");
        };
        return new WindowOperator.WindowOperatorFactory(
                operatorId,
                new PlanNodeId("window-" + operatorId),
                sourceTypes,
                outputChannels,
                windowFunctions,
                partitionChannels,
                List.of(),
                sortChannels,
                sortOrders,
                0,
                10_000,
                new PagesIndex.TestingFactory(false),
                false,
                spillerFactory,
                orderingCompiler,
                List.of(),
                new RegularPartitionerSupplier());
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
        return varcharAnyOf(field(inputChannel, VARCHAR), values);
    }

    private static RowExpression varcharStartsWith(int inputChannel, String prefix)
    {
        return equal(substring(field(inputChannel, VARCHAR), 1, prefix.length()), constant(Slices.utf8Slice(prefix), VARCHAR), VARCHAR);
    }

    private static RowExpression varcharAnyOf(RowExpression value, Set<String> values)
    {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values is empty");
        }

        List<String> sortedValues = values.stream()
                .sorted()
                .toList();

        RowExpression result = equal(value, constant(Slices.utf8Slice(sortedValues.getFirst()), VARCHAR), VARCHAR);
        for (int index = 1; index < sortedValues.size(); index++) {
            result = or(result, equal(value, constant(Slices.utf8Slice(sortedValues.get(index)), VARCHAR), VARCHAR));
        }
        return result;
    }

    private static RowExpression anyOf(RowExpression value, Type type, long... values)
    {
        if (values.length == 0) {
            throw new IllegalArgumentException("values is empty");
        }

        RowExpression result = equal(value, constant(values[0], type), type);
        for (int index = 1; index < values.length; index++) {
            result = or(result, equal(value, constant(values[index], type), type));
        }
        return result;
    }

    private static RowExpression equal(int inputChannel, Type type, String constantValue)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(field(inputChannel, type), constant(Slices.utf8Slice(constantValue), type)));
    }

    private static RowExpression equal(int inputChannel, long constantValue, Type type)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(field(inputChannel, type), constant(constantValue, type)));
    }

    private static RowExpression equal(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression equal(RowExpression left, RowExpression right)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(left.type(), right.type())), List.of(left, right));
    }

    private static RowExpression notEqual(RowExpression left, RowExpression right)
    {
        return equal(equal(left, right), constant(false, BOOLEAN), BOOLEAN);
    }

    private static RowExpression lessThan(RowExpression left, RowExpression right)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, List.of(left.type(), right.type())), List.of(left, right));
    }

    private OperatorFactory semiJoinFilterProjectFactory(int operatorId, List<Type> probeTypes, boolean includeMatches)
    {
        RowExpression filter = includeMatches
                ? field(probeTypes.size(), BOOLEAN)
                : equal(field(probeTypes.size(), BOOLEAN), constant(false, BOOLEAN), BOOLEAN);
        return filterAndProjectFactory(operatorId, Optional.of(filter), identityProjections(probeTypes), probeTypes);
    }

    private static RowExpression greaterThan(int inputChannel, long constantValue)
    {
        return greaterThan(inputChannel, constantValue, INTEGER);
    }

    private static RowExpression lessThan(int inputChannel, long constantValue)
    {
        return lessThan(inputChannel, constantValue, INTEGER);
    }

    private static RowExpression greaterThan(int inputChannel, long constantValue, Type type)
    {
        return lessThan(constant(constantValue, type), field(inputChannel, type), type);
    }

    private static RowExpression lessThan(int inputChannel, long constantValue, Type type)
    {
        return lessThan(field(inputChannel, type), constant(constantValue, type), type);
    }

    private static RowExpression betweenInclusive(RowExpression value, long minimumInclusive, long maximumInclusive, Type type)
    {
        return and(
                greaterThan(value, constant(minimumInclusive - 1, type), type),
                lessThan(value, constant(maximumInclusive + 1, type), type));
    }

    private static RowExpression lessThan(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression concat(RowExpression left, RowExpression right)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR)), List.of(left, right));
    }

    private static RowExpression asVarchar(RowExpression value, Type type)
    {
        if (type.equals(VARCHAR)) {
            return value;
        }
        return new CallExpression(FUNCTION_RESOLUTION.getCoercion(type, VARCHAR), List.of(value));
    }

    private static RowExpression coalesce(RowExpression value, RowExpression fallback, Type type)
    {
        return new SpecialForm(SpecialForm.Form.COALESCE, type, List.of(value, fallback), List.of());
    }

    private static RowExpression substring(RowExpression value, long start, long length)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveFunction("substring", fromTypes(VARCHAR, BIGINT, BIGINT)),
                List.of(value, constant(start, BIGINT), constant(length, BIGINT)));
    }

    private static RowExpression upper(RowExpression value)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveFunction("upper", fromTypes(value.type())),
                List.of(value));
    }

    private static RowExpression query46DatePredicate()
    {
        return and(
                or(equal(1, 6, INTEGER), equal(1, 0, INTEGER)),
                or(equal(2, 1999, INTEGER), equal(2, 2000, INTEGER), equal(2, 2001, INTEGER)));
    }

    private static RowExpression query79DatePredicate()
    {
        return and(
                equal(1, 1, INTEGER),
                or(equal(2, 1999, INTEGER), equal(2, 2000, INTEGER), equal(2, 2001, INTEGER)));
    }

    private static RowExpression query34DatePredicate()
    {
        return and(
                or(
                        and(greaterThan(field(1, INTEGER), constant(0L, INTEGER), INTEGER), lessThan(field(1, INTEGER), constant(4L, INTEGER), INTEGER)),
                        and(greaterThan(field(1, INTEGER), constant(24L, INTEGER), INTEGER), lessThan(field(1, INTEGER), constant(29L, INTEGER), INTEGER))),
                or(equal(2, 1999, INTEGER), equal(2, 2000, INTEGER), equal(2, 2001, INTEGER)));
    }

    private static RowExpression query68DatePredicate()
    {
        return and(
                greaterThan(field(1, INTEGER), constant(0L, INTEGER), INTEGER),
                lessThan(field(1, INTEGER), constant(3L, INTEGER), INTEGER),
                or(equal(2, 1999, INTEGER), equal(2, 2000, INTEGER), equal(2, 2001, INTEGER)));
    }

    private static RowExpression query46HouseholdPredicate(Type depCountType, Type vehicleCountType)
    {
        return or(equal(1, 4, depCountType), equal(2, 3, vehicleCountType));
    }

    private static RowExpression query79HouseholdPredicate(Type depCountType, Type vehicleCountType)
    {
        return or(equal(1, 6, depCountType), greaterThan(field(2, vehicleCountType), constant(2L, vehicleCountType), vehicleCountType));
    }

    private static RowExpression query34HouseholdPredicate(Type vehicleCountType, Type depCountType)
    {
        return and(
                varcharAnyOf(1, Set.of(">10000", "Unknown")),
                greaterThan(field(2, vehicleCountType), constant(0L, vehicleCountType), vehicleCountType),
                greaterThan(
                        multiply(field(3, depCountType), constant(10L, depCountType), depCountType),
                        multiply(field(2, vehicleCountType), constant(12L, vehicleCountType), vehicleCountType),
                        depCountType));
    }

    private static RowExpression query45ItemPredicate()
    {
        return or(
                equal(0, 2, BIGINT),
                equal(0, 3, BIGINT),
                equal(0, 5, BIGINT),
                equal(0, 7, BIGINT),
                equal(0, 11, BIGINT),
                equal(0, 13, BIGINT),
                equal(0, 17, BIGINT),
                equal(0, 19, BIGINT),
                equal(0, 23, BIGINT),
                equal(0, 29, BIGINT));
    }

    private static RowExpression query45FilterPredicate()
    {
        return or(
                varcharAnyOf(substring(field(8, VARCHAR), 1, 5), Set.of("80348", "81792", "83405", "85392", "85460", "85669", "86197", "86475", "88274")),
                field(12, BOOLEAN));
    }

    private static RowExpression query80DatePredicate()
    {
        return and(
                greaterThan(field(1, DATE), constant(11_191L, DATE), DATE),
                lessThan(field(1, DATE), constant(11_223L, DATE), DATE));
    }

    private static RowExpression query92DatePredicate()
    {
        return and(
                greaterThan(field(1, DATE), constant(10_982L, DATE), DATE),
                lessThan(field(1, DATE), constant(11_074L, DATE), DATE));
    }

    private static RowExpression query12DatePredicate()
    {
        return and(
                greaterThan(field(1, DATE), constant(10_643L, DATE), DATE),
                lessThan(field(1, DATE), constant(10_675L, DATE), DATE));
    }

    private static RowExpression query23YearRangePredicate()
    {
        return and(
                greaterThan(field(1, INTEGER), constant(1999L, INTEGER), INTEGER),
                lessThan(field(1, INTEGER), constant(2004L, INTEGER), INTEGER));
    }

    private static RowExpression query14YearRangePredicate()
    {
        return and(
                greaterThan(field(1, INTEGER), constant(1998L, INTEGER), INTEGER),
                lessThan(field(1, INTEGER), constant(2002L, INTEGER), INTEGER));
    }

    private static RowExpression query09QuantityPredicate(int quantityIndex, int minimumQuantityInclusive, int maximumQuantityInclusive)
    {
        return and(
                greaterThan(field(quantityIndex, INTEGER), constant((long) (minimumQuantityInclusive - 1), INTEGER), INTEGER),
                lessThan(field(quantityIndex, INTEGER), constant((long) (maximumQuantityInclusive + 1), INTEGER), INTEGER));
    }

    private static RowExpression query81ThresholdPredicate(Type returnType, Type averageType)
    {
        Type multiplierType = createDecimalType(2, 1);
        RowExpression scaledAverage = new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(averageType, multiplierType)),
                List.of(field(4, averageType), constant(12L, multiplierType)));
        return greaterThan(
                cast(field(2, returnType), returnType, scaledAverage.type()),
                scaledAverage,
                scaledAverage.type());
    }

    private static RowExpression query92DiscountThresholdPredicate(Type discountType, Type averageType)
    {
        Type multiplierType = createDecimalType(2, 1);
        RowExpression scaledAverage = new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(averageType, multiplierType)),
                List.of(field(3, averageType), constant(13L, multiplierType)));
        return greaterThan(
                cast(field(1, discountType), discountType, scaledAverage.type()),
                scaledAverage,
                scaledAverage.type());
    }

    private static RowExpression query23BestCustomerPredicate(Type salesType)
    {
        Type multiplierType = createDecimalType(2, 0);
        RowExpression doubledSales = multiply(field(1, salesType), constant(2L, multiplierType));
        return greaterThan(
                doubledSales,
                cast(field(2, salesType), salesType, doubledSales.type()),
                doubledSales.type());
    }

    private static RowExpression query14ThresholdPredicate(Type salesType, Type averageType)
    {
        return greaterThan(
                cast(field(3, salesType), salesType, averageType),
                field(5, averageType),
                averageType);
    }

    private static RowExpression query24ThresholdPredicate(Type salesType)
    {
        Type countType = createDecimalType(19, 0);
        Type factorType = createDecimalType(2, 0);
        RowExpression count = cast(field(5, BIGINT), BIGINT, countType);
        RowExpression scaledSales = multiply(
                multiply(field(3, salesType), count),
                constant(20L, factorType));
        return greaterThan(scaledSales, field(4, salesType));
    }

    private static RowExpression query65ThresholdPredicate(Type salesType)
    {
        RowExpression revenueCents = cast(
                multiply(
                        field(2, salesType),
                        constant(100L, createDecimalType(3, 0))),
                multiply(field(2, salesType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression scaledSales = multiply(revenueCents, constant(10L, BIGINT), BIGINT);
        return or(
                equal(scaledSales, field(4, BIGINT), BIGINT),
                lessThan(scaledSales, field(4, BIGINT), BIGINT));
    }

    private static RowExpression query01ReturnThresholdPredicate()
    {
        RowExpression scaledReturn = multiply(
                multiply(field(8, BIGINT), field(2, BIGINT), BIGINT),
                constant(5L, BIGINT),
                BIGINT);
        RowExpression scaledAverage = multiply(field(7, BIGINT), constant(6L, BIGINT), BIGINT);
        return greaterThan(scaledReturn, scaledAverage, BIGINT);
    }

    private static RowExpression query06ThresholdPredicate(Type priceType, Type averageType)
    {
        Type multiplierType = createDecimalType(2, 1);
        RowExpression scaledAverage = new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(averageType, multiplierType)),
                List.of(field(4, averageType), constant(12L, multiplierType)));
        return greaterThan(
                cast(field(1, priceType), priceType, scaledAverage.type()),
                scaledAverage,
                scaledAverage.type());
    }

    private static RowExpression query13DemographicsPredicate(Type maritalStatusType, Type educationStatusType, Type salesPriceType, Type depCountType)
    {
        return or(
                and(
                        equal(field(12, maritalStatusType), constant(Slices.utf8Slice("M"), maritalStatusType), maritalStatusType),
                        equal(field(13, educationStatusType), constant(Slices.utf8Slice("Advanced Degree     "), educationStatusType), educationStatusType),
                        betweenInclusive(field(6, salesPriceType), 10_000L, 15_000L, salesPriceType),
                        equal(field(15, depCountType), constant(3L, depCountType), depCountType)),
                and(
                        equal(field(12, maritalStatusType), constant(Slices.utf8Slice("S"), maritalStatusType), maritalStatusType),
                        equal(field(13, educationStatusType), constant(Slices.utf8Slice("College             "), educationStatusType), educationStatusType),
                        betweenInclusive(field(6, salesPriceType), 5_000L, 10_000L, salesPriceType),
                        equal(field(15, depCountType), constant(1L, depCountType), depCountType)),
                and(
                        equal(field(12, maritalStatusType), constant(Slices.utf8Slice("W"), maritalStatusType), maritalStatusType),
                        equal(field(13, educationStatusType), constant(Slices.utf8Slice("2 yr Degree         "), educationStatusType), educationStatusType),
                        betweenInclusive(field(6, salesPriceType), 15_000L, 20_000L, salesPriceType),
                        equal(field(15, depCountType), constant(1L, depCountType), depCountType)));
    }

    private static RowExpression query13StateProfitPredicate(Type stateType, Type netProfitType)
    {
        return or(
                and(
                        varcharAnyOf(field(17, stateType), Set.of("TX", "OH")),
                        betweenInclusive(field(9, netProfitType), 10_000L, 20_000L, netProfitType)),
                and(
                        varcharAnyOf(field(17, stateType), Set.of("OR", "NM", "KY")),
                        betweenInclusive(field(9, netProfitType), 15_000L, 30_000L, netProfitType)),
                and(
                        varcharAnyOf(field(17, stateType), Set.of("VA", "TX", "MS")),
                        betweenInclusive(field(9, netProfitType), 5_000L, 25_000L, netProfitType)));
    }

    private static RowExpression query48DemographicsPredicate(Type maritalStatusType, Type educationStatusType, Type salesPriceType)
    {
        return or(
                and(
                        equal(field(9, maritalStatusType), constant(Slices.utf8Slice("M"), maritalStatusType), maritalStatusType),
                        equal(field(10, educationStatusType), constant(Slices.utf8Slice("4 yr Degree         "), educationStatusType), educationStatusType),
                        betweenInclusive(field(5, salesPriceType), 10_000L, 15_000L, salesPriceType)),
                and(
                        equal(field(9, maritalStatusType), constant(Slices.utf8Slice("D"), maritalStatusType), maritalStatusType),
                        equal(field(10, educationStatusType), constant(Slices.utf8Slice("2 yr Degree         "), educationStatusType), educationStatusType),
                        betweenInclusive(field(5, salesPriceType), 5_000L, 10_000L, salesPriceType)),
                and(
                        equal(field(9, maritalStatusType), constant(Slices.utf8Slice("S"), maritalStatusType), maritalStatusType),
                        equal(field(10, educationStatusType), constant(Slices.utf8Slice("College             "), educationStatusType), educationStatusType),
                        betweenInclusive(field(5, salesPriceType), 15_000L, 20_000L, salesPriceType)));
    }

    private static RowExpression query48StateProfitPredicate(Type stateType, Type netProfitType)
    {
        return or(
                and(
                        varcharAnyOf(field(12, stateType), Set.of("CO", "OH", "TX")),
                        betweenInclusive(field(6, netProfitType), 0L, 200_000L, netProfitType)),
                and(
                        varcharAnyOf(field(12, stateType), Set.of("OR", "MN", "KY")),
                        betweenInclusive(field(6, netProfitType), 15_000L, 300_000L, netProfitType)),
                and(
                        varcharAnyOf(field(12, stateType), Set.of("VA", "CA", "MS")),
                        betweenInclusive(field(6, netProfitType), 5_000L, 2_500_000L, netProfitType)));
    }

    private static RowExpression query08DatePredicate()
    {
        return and(equal(1, 2, INTEGER), equal(2, 1998, INTEGER));
    }

    private static RowExpression query08ZipListPredicate(Type zipType)
    {
        return varcharAnyOf(substring(field(0, zipType), 1, 5), TpcdsQueryLiterals.QUERY08_ZIP_SET);
    }

    private static RowExpression query54MonthBetweenPredicate()
    {
        RowExpression month = field(2, INTEGER);
        RowExpression minimum = field(3, INTEGER);
        RowExpression maximum = field(4, INTEGER);
        RowExpression minimumSatisfied = or(
                equal(month, minimum, INTEGER),
                greaterThan(month, minimum, INTEGER));
        RowExpression maximumSatisfied = or(
                equal(month, maximum, INTEGER),
                lessThan(month, maximum, INTEGER));
        return and(minimumSatisfied, maximumSatisfied);
    }

    private static RowExpression query54SegmentExpression(Type revenueType)
    {
        return divide(
                field(1, revenueType),
                constant(Int128.valueOf(5_000L), revenueType),
                revenueType);
    }

    private static RowExpression query58SimilarityPredicate(int storeRevenueIndex, int catalogRevenueIndex, int webRevenueIndex, Type revenueType)
    {
        return and(
                query58WithinTenPercent(field(storeRevenueIndex, revenueType), field(catalogRevenueIndex, revenueType)),
                query58WithinTenPercent(field(storeRevenueIndex, revenueType), field(webRevenueIndex, revenueType)),
                query58WithinTenPercent(field(catalogRevenueIndex, revenueType), field(storeRevenueIndex, revenueType)),
                query58WithinTenPercent(field(catalogRevenueIndex, revenueType), field(webRevenueIndex, revenueType)),
                query58WithinTenPercent(field(webRevenueIndex, revenueType), field(storeRevenueIndex, revenueType)),
                query58WithinTenPercent(field(webRevenueIndex, revenueType), field(catalogRevenueIndex, revenueType)));
    }

    private static RowExpression query58WithinTenPercent(RowExpression left, RowExpression right)
    {
        Type factorType = createDecimalType(2, 0);
        RowExpression leftScaled = multiply(left, constant(10L, factorType));
        RowExpression lowerBound = multiply(right, constant(9L, factorType));
        RowExpression upperBound = multiply(right, constant(11L, factorType));
        return and(
                or(equal(leftScaled, lowerBound), greaterThan(leftScaled, lowerBound)),
                or(equal(leftScaled, upperBound), lessThan(leftScaled, upperBound)));
    }

    private static List<RowExpression> query58OutputProjections(Type itemIdType, Type revenueType)
    {
        Type percentType = createDecimalType(7, 2);
        Type divisorType = createDecimalType(10, 0);
        Type averageType = createDecimalType(38, 6);

        RowExpression totalRevenue = add(add(field(1, revenueType), field(2, revenueType)), field(3, revenueType));
        RowExpression averageBase = divide(totalRevenue, constant(3L, divisorType));
        RowExpression storeDeviationBase = multiply(divide(divide(field(1, revenueType), totalRevenue), constant(3L, divisorType)), constant(100L, divisorType));
        RowExpression catalogDeviationBase = multiply(divide(divide(field(2, revenueType), totalRevenue), constant(3L, divisorType)), constant(100L, divisorType));
        RowExpression webDeviationBase = multiply(divide(divide(field(3, revenueType), totalRevenue), constant(3L, divisorType)), constant(100L, divisorType));

        return List.of(
                field(0, itemIdType),
                field(1, revenueType),
                cast(storeDeviationBase, storeDeviationBase.type(), percentType),
                field(2, revenueType),
                cast(catalogDeviationBase, catalogDeviationBase.type(), percentType),
                field(3, revenueType),
                cast(webDeviationBase, webDeviationBase.type(), percentType),
                cast(averageBase, averageBase.type(), averageType));
    }

    private static RowExpression query61PercentExpression(Type revenueType, Type percentType)
    {
        Type castType = createDecimalType(15, 4);
        Type multiplierType = createDecimalType(10, 0);
        RowExpression percentBase = multiply(
                divide(
                        cast(field(0, revenueType), revenueType, castType),
                        cast(field(1, revenueType), revenueType, castType)),
                constant(100L, multiplierType));
        return cast(percentBase, percentBase.type(), percentType);
    }

    private static RowExpression query44AverageKey(RowExpression sum, RowExpression count)
    {
        RowExpression halfCount = divide(count, constant(2L, BIGINT), BIGINT);
        RowExpression positiveAverage = divide(add(sum, halfCount, BIGINT), count, BIGINT);
        RowExpression negatedSum = subtract(constant(0L, BIGINT), sum, BIGINT);
        RowExpression negativeAverage = subtract(
                constant(0L, BIGINT),
                divide(add(negatedSum, halfCount, BIGINT), count, BIGINT),
                BIGINT);
        return ifExpression(
                lessThan(sum, constant(0L, BIGINT), BIGINT),
                negativeAverage,
                positiveAverage,
                BIGINT);
    }

    private static RowExpression query44ThresholdPredicate()
    {
        RowExpression itemAverage = query44AverageKey(field(2, BIGINT), field(3, BIGINT));
        RowExpression scalarAverage = query44AverageKey(field(5, BIGINT), field(6, BIGINT));
        return greaterThan(
                multiply(itemAverage, constant(10L, BIGINT), BIGINT),
                multiply(scalarAverage, constant(9L, BIGINT), BIGINT),
                BIGINT);
    }

    private static RowExpression query53ItemPredicate()
    {
        RowExpression firstBranch = and(
                varcharAnyOf(2, Set.of("Books", "Children", "Electronics")),
                varcharAnyOf(3, Set.of("personal", "portable", "reference", "self-help")),
                varcharAnyOf(4, Set.of("scholaramalgamalg #14", "scholaramalgamalg #7", "exportiunivamalg #9", "scholaramalgamalg #9")));
        RowExpression secondBranch = and(
                varcharAnyOf(2, Set.of("Women", "Music", "Men")),
                varcharAnyOf(3, Set.of("accessories", "classical", "fragrances", "pants")),
                varcharAnyOf(4, Set.of("amalgimporto #1", "edu packscholar #1", "exportiimporto #1", "importoamalg #1")));
        return or(firstBranch, secondBranch);
    }

    private static RowExpression query12CategoryPredicate()
    {
        return varcharAnyOf(3, Set.of("Sports", "Books", "Home"));
    }

    private static RowExpression query89ItemPredicate()
    {
        RowExpression firstBranch = and(
                varcharAnyOf(1, Set.of("Books", "Electronics", "Sports")),
                varcharAnyOf(2, Set.of("computers", "stereo", "football")));
        RowExpression secondBranch = and(
                varcharAnyOf(1, Set.of("Men", "Jewelry", "Women")),
                varcharAnyOf(2, Set.of("shirts", "birdal", "dresses")));
        return or(firstBranch, secondBranch);
    }

    private static RowExpression query91CustomerDemographicsPredicate()
    {
        RowExpression marriedUnknown = and(equal(1, VARCHAR, "M"), equal(2, VARCHAR, "Unknown"));
        RowExpression widowedAdvancedDegree = and(equal(1, VARCHAR, "W"), equal(2, VARCHAR, "Advanced Degree"));
        return or(marriedUnknown, widowedAdvancedDegree);
    }

    private static RowExpression query53DeviationPredicate(Type sumType, Type averageType)
    {
        return queryRelativeDeviationPredicate(2, 3, sumType, averageType);
    }

    private static RowExpression query57DatePredicate()
    {
        return or(
                equal(1, 1999, INTEGER),
                and(equal(1, 1998, INTEGER), equal(2, 12, INTEGER)),
                and(equal(1, 2000, INTEGER), equal(2, 1, INTEGER)));
    }

    private static RowExpression queryRelativeDeviationPredicate(int sumIndex, int averageIndex, Type sumType, Type averageType)
    {
        RowExpression sum = cast(field(sumIndex, sumType), sumType, DOUBLE);
        RowExpression average = cast(field(averageIndex, averageType), averageType, DOUBLE);
        RowExpression sumLessThanAverage = lessThan(sum, average, DOUBLE);
        RowExpression absoluteDifference = ifExpression(
                sumLessThanAverage,
                subtract(average, sum, DOUBLE),
                subtract(sum, average, DOUBLE),
                DOUBLE);
        return and(
                greaterThan(average, constant(0.0, DOUBLE), DOUBLE),
                greaterThan(multiply(absoluteDifference, constant(10.0, DOUBLE), DOUBLE), average, DOUBLE));
    }

    private static RowExpression greaterThan(RowExpression left, RowExpression right, Type type)
    {
        return lessThan(right, left, type);
    }

    private static RowExpression greaterThan(RowExpression left, RowExpression right)
    {
        return lessThan(right, left);
    }

    private static RowExpression multiply(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression multiply(RowExpression left, RowExpression right)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(left.type(), right.type())), List.of(left, right));
    }

    private static RowExpression add(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression add(RowExpression left, RowExpression right)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(left.type(), right.type())), List.of(left, right));
    }

    private static RowExpression subtract(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression divide(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression divide(RowExpression left, RowExpression right)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(left.type(), right.type())), List.of(left, right));
    }

    private static long roundDivide(long numerator, long denominator)
    {
        if (denominator == 0) {
            return 0;
        }
        long positiveNumerator = numerator >= 0 ? numerator : -numerator;
        long positiveDenominator = denominator >= 0 ? denominator : -denominator;
        long rounded = (positiveNumerator + (positiveDenominator / 2)) / positiveDenominator;
        return (numerator < 0) ^ (denominator < 0) ? -rounded : rounded;
    }

    private static RowExpression cast(RowExpression expression, Type fromType, Type toType)
    {
        return new CallExpression(FUNCTION_RESOLUTION.getCoercion(fromType, toType), List.of(expression));
    }

    private static RowExpression ifExpression(RowExpression condition, RowExpression whenTrue, RowExpression whenFalse, Type outputType)
    {
        return new SpecialForm(SpecialForm.Form.IF, outputType, List.of(condition, whenTrue, whenFalse), List.of());
    }

    private static List<RowExpression> query70RollupProjection(Type sumType)
    {
        RowExpression groupId = field(2, BIGINT);
        RowExpression groupIdIsZero = equal(groupId, constant(0L, BIGINT), BIGINT);
        RowExpression groupIdIsOne = equal(groupId, constant(1L, BIGINT), BIGINT);
        RowExpression groupIdIsTwo = equal(groupId, constant(2L, BIGINT), BIGINT);
        RowExpression hierarchy = ifExpression(
                groupIdIsZero,
                constant(2L, INTEGER),
                ifExpression(
                        groupIdIsOne,
                        constant(1L, INTEGER),
                        constant(0L, INTEGER),
                        INTEGER),
                INTEGER);
        RowExpression stateForRank = ifExpression(
                groupIdIsTwo,
                field(0, VARCHAR),
                constant(Slices.utf8Slice("\uFFFF"), VARCHAR),
                VARCHAR);
        return List.of(
                field(0, VARCHAR),
                field(1, VARCHAR),
                field(3, sumType),
                hierarchy,
                stateForRank);
    }

    private static List<RowExpression> query36RollupProjection(Type numeratorType, Type denominatorType, Type grossMarginType)
    {
        RowExpression groupId = field(2, BIGINT);
        RowExpression groupIdIsZero = equal(groupId, constant(0L, BIGINT), BIGINT);
        RowExpression groupIdIsOne = equal(groupId, constant(1L, BIGINT), BIGINT);
        RowExpression groupIdIsTwo = equal(groupId, constant(2L, BIGINT), BIGINT);
        RowExpression hierarchy = ifExpression(
                groupIdIsZero,
                constant(2L, INTEGER),
                ifExpression(
                        groupIdIsOne,
                        constant(1L, INTEGER),
                        constant(0L, INTEGER),
                        INTEGER),
                INTEGER);
        RowExpression categoryForRank = ifExpression(
                groupIdIsTwo,
                field(0, VARCHAR),
                constant(Slices.utf8Slice("\uFFFF"), VARCHAR),
                VARCHAR);
        RowExpression rawGrossMargin = divide(
                cast(field(3, numeratorType), numeratorType, createDecimalType(15, 4)),
                cast(field(4, denominatorType), denominatorType, createDecimalType(15, 4)));
        RowExpression grossMargin = cast(rawGrossMargin, rawGrossMargin.type(), grossMarginType);
        return List.of(
                field(0, VARCHAR),
                field(1, VARCHAR),
                grossMargin,
                hierarchy,
                categoryForRank);
    }

    private static RowExpression query31GrowthPredicate(Type storeRevenueType, Type webRevenueType)
    {
        RowExpression zeroStore = constant(Int128.ZERO, storeRevenueType);
        RowExpression zeroWeb = constant(Int128.ZERO, webRevenueType);
        RowExpression firstWebGrowth = greaterThan(
                multiply(field(9, webRevenueType), field(1, storeRevenueType)),
                multiply(field(3, storeRevenueType), field(7, webRevenueType)));
        RowExpression secondWebGrowth = greaterThan(
                multiply(field(11, webRevenueType), field(3, storeRevenueType)),
                multiply(field(5, storeRevenueType), field(9, webRevenueType)));
        return and(
                greaterThan(field(1, storeRevenueType), zeroStore, storeRevenueType),
                greaterThan(field(3, storeRevenueType), zeroStore, storeRevenueType),
                greaterThan(field(7, webRevenueType), zeroWeb, webRevenueType),
                greaterThan(field(9, webRevenueType), zeroWeb, webRevenueType),
                firstWebGrowth,
                secondWebGrowth);
    }

    private static RowExpression query11GrowthPredicate(Type totalType)
    {
        RowExpression zero = constant(Int128.ZERO, totalType);
        RowExpression storePositive = greaterThan(field(6, totalType), zero, totalType);
        RowExpression webPositive = greaterThan(field(20, totalType), zero, totalType);
        RowExpression webGrowthGreater = greaterThan(
                multiply(field(27, totalType), field(6, totalType)),
                multiply(field(13, totalType), field(20, totalType)));
        return and(storePositive, webPositive, webGrowthGreater);
    }

    private static RowExpression query74GrowthPredicate()
    {
        RowExpression zero = constant(0L, BIGINT);
        return and(
                greaterThan(field(3, BIGINT), zero, BIGINT),
                greaterThan(field(11, BIGINT), zero, BIGINT),
                greaterThan(
                        multiply(field(15, BIGINT), field(3, BIGINT), BIGINT),
                        multiply(field(7, BIGINT), field(11, BIGINT), BIGINT),
                        BIGINT));
    }

    private static RowExpression query85DemographicsAndPricePredicate(Type salesPriceType)
    {
        RowExpression salesPrice = cast(
                multiply(field(5, salesPriceType), constant(100L, createDecimalType(3, 0))),
                multiply(field(5, salesPriceType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        return or(
                and(
                        equal(field(18, VARCHAR), constant(Slices.utf8Slice("M"), VARCHAR), VARCHAR),
                        equal(field(18, VARCHAR), field(21, VARCHAR), VARCHAR),
                        equal(field(19, VARCHAR), constant(Slices.utf8Slice("Advanced Degree"), VARCHAR), VARCHAR),
                        equal(field(19, VARCHAR), field(22, VARCHAR), VARCHAR),
                        greaterThan(salesPrice, constant(9_999L, BIGINT), BIGINT),
                        lessThan(salesPrice, constant(15_001L, BIGINT), BIGINT)),
                and(
                        equal(field(18, VARCHAR), constant(Slices.utf8Slice("S"), VARCHAR), VARCHAR),
                        equal(field(18, VARCHAR), field(21, VARCHAR), VARCHAR),
                        equal(field(19, VARCHAR), constant(Slices.utf8Slice("College"), VARCHAR), VARCHAR),
                        equal(field(19, VARCHAR), field(22, VARCHAR), VARCHAR),
                        greaterThan(salesPrice, constant(4_999L, BIGINT), BIGINT),
                        lessThan(salesPrice, constant(10_001L, BIGINT), BIGINT)),
                and(
                        equal(field(18, VARCHAR), constant(Slices.utf8Slice("W"), VARCHAR), VARCHAR),
                        equal(field(18, VARCHAR), field(21, VARCHAR), VARCHAR),
                        equal(field(19, VARCHAR), constant(Slices.utf8Slice("2 yr Degree"), VARCHAR), VARCHAR),
                        equal(field(19, VARCHAR), field(22, VARCHAR), VARCHAR),
                        greaterThan(salesPrice, constant(14_999L, BIGINT), BIGINT),
                        lessThan(salesPrice, constant(20_001L, BIGINT), BIGINT)));
    }

    private static RowExpression query85AddressProfitPredicate(Type netProfitType)
    {
        RowExpression netProfit = cast(
                multiply(field(6, netProfitType), constant(100L, createDecimalType(3, 0))),
                multiply(field(6, netProfitType), constant(100L, createDecimalType(3, 0))).type(),
                BIGINT);
        RowExpression unitedStates = equal(field(24, VARCHAR), constant(Slices.utf8Slice("United States"), VARCHAR), VARCHAR);
        return or(
                and(unitedStates, varcharAnyOf(field(25, VARCHAR), Set.of("IN", "OH", "NJ")), greaterThan(netProfit, constant(9_999L, BIGINT), BIGINT), lessThan(netProfit, constant(20_001L, BIGINT), BIGINT)),
                and(unitedStates, varcharAnyOf(field(25, VARCHAR), Set.of("WI", "CT", "KY")), greaterThan(netProfit, constant(14_999L, BIGINT), BIGINT), lessThan(netProfit, constant(30_001L, BIGINT), BIGINT)),
                and(unitedStates, varcharAnyOf(field(25, VARCHAR), Set.of("LA", "IA", "AR")), greaterThan(netProfit, constant(4_999L, BIGINT), BIGINT), lessThan(netProfit, constant(25_001L, BIGINT), BIGINT)));
    }

    private static RowExpression query04GrowthPredicate()
    {
        RowExpression zero = constant(0L, BIGINT);
        RowExpression storePositive = greaterThan(field(4, BIGINT), zero, BIGINT);
        RowExpression catalogPositive = greaterThan(field(14, BIGINT), zero, BIGINT);
        RowExpression webPositive = greaterThan(field(24, BIGINT), zero, BIGINT);
        RowExpression catalogBeatsStore = greaterThan(
                multiply(field(19, BIGINT), field(4, BIGINT), BIGINT),
                multiply(field(9, BIGINT), field(14, BIGINT), BIGINT),
                BIGINT);
        RowExpression catalogBeatsWeb = greaterThan(
                multiply(field(19, BIGINT), field(24, BIGINT), BIGINT),
                multiply(field(29, BIGINT), field(14, BIGINT), BIGINT),
                BIGINT);
        return and(storePositive, catalogPositive, webPositive, catalogBeatsStore, catalogBeatsWeb);
    }

    private static RowExpression query21InventoryRatioPredicate()
    {
        RowExpression afterTimesThree = multiply(field(5, BIGINT), constant(3L, BIGINT), BIGINT);
        RowExpression beforeTimesTwo = multiply(field(2, BIGINT), constant(2L, BIGINT), BIGINT);
        RowExpression afterTimesTwo = multiply(field(5, BIGINT), constant(2L, BIGINT), BIGINT);
        RowExpression beforeTimesThree = multiply(field(2, BIGINT), constant(3L, BIGINT), BIGINT);
        return and(
                greaterThan(field(2, BIGINT), constant(0L, BIGINT), BIGINT),
                notEqual(lessThan(afterTimesThree, beforeTimesTwo), constant(true, BOOLEAN)),
                notEqual(lessThan(beforeTimesThree, afterTimesTwo), constant(true, BOOLEAN)));
    }

    private static List<RowExpression> query31Projection(Type storeRevenueType, Type webRevenueType, Type ratioType)
    {
        Type ratioInputType = createDecimalType(38, 6);
        RowExpression webQuarterOneToTwoRaw = divide(
                cast(field(9, webRevenueType), webRevenueType, ratioInputType),
                cast(field(7, webRevenueType), webRevenueType, ratioInputType));
        RowExpression storeQuarterOneToTwoRaw = divide(
                cast(field(3, storeRevenueType), storeRevenueType, ratioInputType),
                cast(field(1, storeRevenueType), storeRevenueType, ratioInputType));
        RowExpression webQuarterTwoToThreeRaw = divide(
                cast(field(11, webRevenueType), webRevenueType, ratioInputType),
                cast(field(9, webRevenueType), webRevenueType, ratioInputType));
        RowExpression storeQuarterTwoToThreeRaw = divide(
                cast(field(5, storeRevenueType), storeRevenueType, ratioInputType),
                cast(field(3, storeRevenueType), storeRevenueType, ratioInputType));
        return List.of(
                field(0, VARCHAR),
                constant(2000L, INTEGER),
                cast(webQuarterOneToTwoRaw, webQuarterOneToTwoRaw.type(), ratioType),
                cast(storeQuarterOneToTwoRaw, storeQuarterOneToTwoRaw.type(), ratioType),
                cast(webQuarterTwoToThreeRaw, webQuarterTwoToThreeRaw.type(), ratioType),
                cast(storeQuarterTwoToThreeRaw, storeQuarterTwoToThreeRaw.type(), ratioType));
    }

    private static RowExpression query15ZipStateOrPricePredicate(Type zipType, Type stateType, Type salesType)
    {
        RowExpression zipPrefix = substring(field(6, zipType), 1, 5);
        RowExpression zipMatch = or(
                equal(zipPrefix, constant(Slices.utf8Slice("85669"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("86197"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("88274"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("83405"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("86475"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("85392"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("85460"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("80348"), zipType), zipType),
                equal(zipPrefix, constant(Slices.utf8Slice("81792"), zipType), zipType));
        RowExpression stateMatch = or(
                equal(field(7, stateType), constant(Slices.utf8Slice("CA"), stateType), stateType),
                equal(field(7, stateType), constant(Slices.utf8Slice("WA"), stateType), stateType),
                equal(field(7, stateType), constant(Slices.utf8Slice("GA"), stateType), stateType));
        RowExpression priceMatch = greaterThan(field(2, salesType), cast(constant(50_000L, BIGINT), BIGINT, salesType), salesType);
        return or(zipMatch, stateMatch, priceMatch);
    }

    private static List<RowExpression> query86RollupProjection(Type sumType)
    {
        RowExpression groupId = field(2, BIGINT);
        RowExpression groupIdIsZero = equal(groupId, constant(0L, BIGINT), BIGINT);
        RowExpression groupIdIsOne = equal(groupId, constant(1L, BIGINT), BIGINT);
        RowExpression groupIdIsTwo = equal(groupId, constant(2L, BIGINT), BIGINT);
        RowExpression hierarchy = ifExpression(
                groupIdIsZero,
                constant(2L, INTEGER),
                ifExpression(
                        groupIdIsOne,
                        constant(1L, INTEGER),
                        constant(0L, INTEGER),
                        INTEGER),
                INTEGER);
        RowExpression categoryForRank = ifExpression(
                groupIdIsTwo,
                field(0, VARCHAR),
                constant(Slices.utf8Slice("\uFFFF"), VARCHAR),
                VARCHAR);
        return List.of(
                field(0, VARCHAR),
                field(1, VARCHAR),
                field(3, sumType),
                hierarchy,
                categoryForRank);
    }

    private static WindowFunctionDefinition aggregateWindowFunction(String functionName, List<Type> argumentTypes, Type outputType, int... inputChannels)
    {
        return aggregateWindowFunction(functionName, argumentTypes, outputType, RUNNING_ROWS_FRAME, inputChannels);
    }

    private static WindowFunctionDefinition aggregateWindowFunction(String functionName, List<Type> argumentTypes, Type outputType, FrameInfo frameInfo, int... inputChannels)
    {
        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        AggregationWindowFunctionSupplier supplier = new AggregationWindowFunctionSupplier(
                resolvedFunction.signature(),
                FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager().getAggregationImplementation(resolvedFunction),
                resolvedFunction.functionNullability());
        return window(
                supplier,
                outputType,
                frameInfo,
                false,
                List.of(),
                java.util.Arrays.stream(inputChannels)
                        .boxed()
                        .toList());
    }

    private static WindowFunctionDefinition rankWindowFunction()
    {
        return window(
                new ReflectionWindowFunctionSupplier(0, RankFunction.class),
                BIGINT,
                RUNNING_ROWS_FRAME,
                false,
                List.of(),
                List.of());
    }

    private static RowExpression yearMonthRangePredicate(int yearIndex, int monthIndex, int year, int minimumMonthInclusive, int maximumMonthInclusive)
    {
        return and(
                equal(yearIndex, year, INTEGER),
                greaterThan(field(monthIndex, INTEGER), constant((long) minimumMonthInclusive - 1L, INTEGER), INTEGER),
                lessThan(field(monthIndex, INTEGER), constant((long) maximumMonthInclusive + 1L, INTEGER), INTEGER));
    }

    private static RowExpression yearQuarterRangePredicate(int yearIndex, int quarterIndex, int year, int minimumQuarterInclusive, int maximumQuarterInclusive)
    {
        return and(
                equal(yearIndex, year, INTEGER),
                greaterThan(field(quarterIndex, INTEGER), constant((long) minimumQuarterInclusive - 1L, INTEGER), INTEGER),
                lessThan(field(quarterIndex, INTEGER), constant((long) maximumQuarterInclusive + 1L, INTEGER), INTEGER));
    }

    private static RowExpression dayOfMonthAndYearsPredicate(int dayIndex, int yearIndex, int minimumDayInclusive, int maximumDayInclusive, int... years)
    {
        RowExpression yearsPredicate = equal(yearIndex, years[0], INTEGER);
        for (int index = 1; index < years.length; index++) {
            yearsPredicate = or(yearsPredicate, equal(yearIndex, years[index], INTEGER));
        }
        return and(
                greaterThan(field(dayIndex, INTEGER), constant((long) minimumDayInclusive - 1L, INTEGER), INTEGER),
                lessThan(field(dayIndex, INTEGER), constant((long) maximumDayInclusive + 1L, INTEGER), INTEGER),
                yearsPredicate);
    }

    private static RowExpression query96TimePredicate()
    {
        return and(equal(1, 20, INTEGER), greaterThan(field(2, INTEGER), constant(29L, INTEGER), INTEGER));
    }

    private static RowExpression query73HouseholdPredicate()
    {
        return and(
                varcharAnyOf(1, Set.of(">10000", "Unknown")),
                greaterThan(field(2, INTEGER), constant(0L, INTEGER), INTEGER),
                greaterThan(field(3, INTEGER), field(2, INTEGER), INTEGER));
    }

    private static RowExpression query88HouseholdPredicate()
    {
        return and(
                or(equal(1, 4, INTEGER), equal(1, 2, INTEGER), equal(1, 0, INTEGER)),
                lessThan(
                        field(2, INTEGER),
                        new CallExpression(
                                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(INTEGER, INTEGER)),
                                List.of(field(1, INTEGER), constant(3L, INTEGER))),
                        INTEGER));
    }

    private static RowExpression query88TimeBucketPredicate(int bucket)
    {
        return switch (bucket) {
            case 0 -> and(equal(1, 8, INTEGER), greaterThan(field(2, INTEGER), constant(29L, INTEGER), INTEGER));
            case 1 -> and(equal(1, 9, INTEGER), lessThan(field(2, INTEGER), constant(30L, INTEGER), INTEGER));
            case 2 -> and(equal(1, 9, INTEGER), greaterThan(field(2, INTEGER), constant(29L, INTEGER), INTEGER));
            case 3 -> and(equal(1, 10, INTEGER), lessThan(field(2, INTEGER), constant(30L, INTEGER), INTEGER));
            case 4 -> and(equal(1, 10, INTEGER), greaterThan(field(2, INTEGER), constant(29L, INTEGER), INTEGER));
            case 5 -> and(equal(1, 11, INTEGER), lessThan(field(2, INTEGER), constant(30L, INTEGER), INTEGER));
            case 6 -> and(equal(1, 11, INTEGER), greaterThan(field(2, INTEGER), constant(29L, INTEGER), INTEGER));
            case 7 -> and(equal(1, 12, INTEGER), lessThan(field(2, INTEGER), constant(30L, INTEGER), INTEGER));
            default -> throw new IllegalArgumentException("Unexpected Q88 bucket: " + bucket);
        };
    }

    private static List<RowExpression> shippingBucketProjections(int firstNameIndex, int secondNameIndex, int thirdNameIndex, int shipDateIndex, int soldDateIndex)
    {
        RowExpression days = subtract(field(shipDateIndex, BIGINT), field(soldDateIndex, BIGINT), BIGINT);
        RowExpression one = constant(1L, BIGINT);
        RowExpression zero = constant(0L, BIGINT);
        return List.of(
                substring(field(firstNameIndex, VARCHAR), 1, 20),
                field(secondNameIndex, VARCHAR),
                field(thirdNameIndex, VARCHAR),
                ifExpression(lessThan(days, constant(31L, BIGINT), BIGINT), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(30L, BIGINT), BIGINT), lessThan(days, constant(61L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(60L, BIGINT), BIGINT), lessThan(days, constant(91L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(90L, BIGINT), BIGINT), lessThan(days, constant(121L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(greaterThan(days, constant(120L, BIGINT), BIGINT), one, zero, BIGINT));
    }

    private static List<RowExpression> query50BucketProjections(List<Type> storeTypes)
    {
        RowExpression days = subtract(field(5, BIGINT), field(0, BIGINT), BIGINT);
        RowExpression one = constant(1L, BIGINT);
        RowExpression zero = constant(0L, BIGINT);
        return List.of(
                field(12, storeTypes.get(1)),
                field(13, storeTypes.get(2)),
                field(14, storeTypes.get(3)),
                field(15, storeTypes.get(4)),
                field(16, storeTypes.get(5)),
                field(17, storeTypes.get(6)),
                field(18, storeTypes.get(7)),
                field(19, storeTypes.get(8)),
                field(20, storeTypes.get(9)),
                field(21, storeTypes.get(10)),
                ifExpression(lessThan(days, constant(31L, BIGINT), BIGINT), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(30L, BIGINT), BIGINT), lessThan(days, constant(61L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(60L, BIGINT), BIGINT), lessThan(days, constant(91L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(90L, BIGINT), BIGINT), lessThan(days, constant(121L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(greaterThan(days, constant(120L, BIGINT), BIGINT), one, zero, BIGINT));
    }

    private static List<RowExpression> query43BucketProjections(RowExpression storeName, RowExpression storeId, RowExpression dayName, RowExpression sales, Type salesType)
    {
        RowExpression zero = cast(constant(0L, BIGINT), BIGINT, salesType);
        return List.of(
                storeName,
                storeId,
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Sunday"), VARCHAR), VARCHAR), sales, zero, salesType),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Monday"), VARCHAR), VARCHAR), sales, zero, salesType),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Tuesday"), VARCHAR), VARCHAR), sales, zero, salesType),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Wednesday"), VARCHAR), VARCHAR), sales, zero, salesType),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Thursday"), VARCHAR), VARCHAR), sales, zero, salesType),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Friday"), VARCHAR), VARCHAR), sales, zero, salesType),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Saturday"), VARCHAR), VARCHAR), sales, zero, salesType));
    }

    private static List<RowExpression> query02BucketProjections(RowExpression weekSequence, RowExpression dayName, RowExpression sales)
    {
        RowExpression zero = constant(0L, BIGINT);
        return List.of(
                weekSequence,
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Sunday"), VARCHAR), VARCHAR), sales, zero, BIGINT),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Monday"), VARCHAR), VARCHAR), sales, zero, BIGINT),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Tuesday"), VARCHAR), VARCHAR), sales, zero, BIGINT),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Wednesday"), VARCHAR), VARCHAR), sales, zero, BIGINT),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Thursday"), VARCHAR), VARCHAR), sales, zero, BIGINT),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Friday"), VARCHAR), VARCHAR), sales, zero, BIGINT),
                ifExpression(equal(dayName, constant(Slices.utf8Slice("Saturday"), VARCHAR), VARCHAR), sales, zero, BIGINT));
    }

    private static RowExpression query02RatioExpression(int numeratorIndex, int denominatorIndex, RowExpression hundred)
    {
        RowExpression denominator = field(denominatorIndex, BIGINT);
        RowExpression roundedScaledDivision = divide(
                add(
                        multiply(field(numeratorIndex, BIGINT), hundred, BIGINT),
                        divide(denominator, constant(2L, BIGINT), BIGINT),
                        BIGINT),
                denominator,
                BIGINT);
        return ifExpression(
                equal(denominator, constant(0L, BIGINT), BIGINT),
                constant(null, BIGINT),
                roundedScaledDivision,
                BIGINT);
    }

    private static List<RowExpression> shippingBucketGroupProjections(int firstGroupIndex, int secondGroupIndex, int thirdGroupIndex, int shipDateIndex, int soldDateIndex)
    {
        RowExpression days = subtract(field(shipDateIndex, BIGINT), field(soldDateIndex, BIGINT), BIGINT);
        RowExpression one = constant(1L, BIGINT);
        RowExpression zero = constant(0L, BIGINT);
        return List.of(
                field(firstGroupIndex, BIGINT),
                field(secondGroupIndex, BIGINT),
                field(thirdGroupIndex, BIGINT),
                ifExpression(lessThan(days, constant(31L, BIGINT), BIGINT), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(30L, BIGINT), BIGINT), lessThan(days, constant(61L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(60L, BIGINT), BIGINT), lessThan(days, constant(91L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(and(greaterThan(days, constant(90L, BIGINT), BIGINT), lessThan(days, constant(121L, BIGINT), BIGINT)), one, zero, BIGINT),
                ifExpression(greaterThan(days, constant(120L, BIGINT), BIGINT), one, zero, BIGINT));
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

    private record HashJoinSpec(
            int operatorId,
            List<Type> probeTypes,
            List<Integer> probeJoinChannels,
            List<Page> buildPages,
            List<Type> buildTypes,
            List<Integer> buildHashChannels,
            JoinOperatorType joinOperatorType)
    {
        private HashJoinSpec(int operatorId, List<Type> probeTypes, List<Integer> probeJoinChannels, List<Page> buildPages, List<Type> buildTypes, List<Integer> buildHashChannels)
        {
            this(operatorId, probeTypes, probeJoinChannels, buildPages, buildTypes, buildHashChannels, JoinOperatorType.innerJoin(false, false));
        }
    }

    private record HashJoinRuntime(
            OperatorFactory joinFactory,
            LookupOuterOperator.LookupOuterOperatorFactory outerFactory) {}

    private record SemiJoinSpec(
            int operatorId,
            List<Type> probeTypes,
            int probeJoinChannel,
            Optional<List<Page>> buildPages,
            List<Path> buildFiles,
            List<String> buildColumns,
            List<PipelineStep> buildSteps,
            List<Type> buildTypes,
            int buildJoinChannel) {}

    private record SemiJoinPagesSpec(
            int operatorId,
            List<Type> probeTypes,
            int probeJoinChannel,
            List<Page> buildPages,
            List<Type> buildTypes,
            int buildJoinChannel) {}

    private record ChannelPages(
            List<Page> pages,
            List<Type> types) {}

    private sealed interface PipelineStep
            permits FactoryStep, HashJoinStep, SemiJoinStep, SemiJoinPagesStep
    {
        OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support);
    }

    private record FactoryStep(OperatorFactory factory)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support)
        {
            return factory;
        }
    }

    private record HashJoinStep(HashJoinSpec spec)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support)
        {
            return support.createHashJoinFactory(taskContext, spec);
        }
    }

    private record SemiJoinStep(SemiJoinSpec spec)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support)
        {
            return support.createSemiJoinFactory(taskContext, spec);
        }
    }

    private record SemiJoinPagesStep(SemiJoinPagesSpec spec)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support)
        {
            return support.createSemiJoinFactory(taskContext, spec);
        }
    }

    private static void writeJoinVarchar(io.trino.spi.block.BlockBuilder blockBuilder, String value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(value));
    }

    private static List<Integer> rangeList(int size)
    {
        return java.util.stream.IntStream.range(0, size)
                .boxed()
                .toList();
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
