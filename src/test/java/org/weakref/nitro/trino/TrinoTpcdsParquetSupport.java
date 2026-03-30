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
import org.weakref.nitro.tpcds.TpcdsParquetTables;

import java.nio.file.Path;
import java.util.ArrayList;
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
        return executePipelinePages(
                tables.tableFiles(salesTable),
                List.of(customerColumn, dateColumn),
                List.of(
                        hashJoinStep(new HashJoinSpec(operatorIdBase, List.of(BIGINT, BIGINT), List.of(1), eligibleDateKeys, List.of(BIGINT), List.of(0))),
                        factoryStep(filterAndProjectFactory(
                                operatorIdBase + 1,
                                Optional.of(greaterThan(field(0, BIGINT), constant(0L, BIGINT), BIGINT)),
                                List.of(field(0, BIGINT)),
                List.of(BIGINT)))));
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

    private static RowExpression query23YearRangePredicate()
    {
        return and(
                greaterThan(field(1, INTEGER), constant(1999L, INTEGER), INTEGER),
                lessThan(field(1, INTEGER), constant(2004L, INTEGER), INTEGER));
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
