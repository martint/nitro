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
import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.HashSemiJoinOperator;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetSupplier;
import io.trino.operator.TopNOperator;
import io.trino.operator.ValuesOperator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.operator.join.unspilled.PartitionedLookupSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
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
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.weakref.nitro.tpcds.TpcdsParquetTables;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
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
    private static final TestingAggregationFunction BIGINT_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_AVG = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_MIN = FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_MAX = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(BIGINT));

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TrinoTpcdsParquetSupport"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TrinoTpcdsParquetSupport-scheduled"));
    private final OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());
    private final int blockedWaitTimeoutSeconds = blockedWaitTimeoutSeconds();

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
        PartitionedLookupSourceFactory lookupSourceFactory = new PartitionedLookupSourceFactory(
                hashJoinSpec.buildTypes(),
                hashJoinSpec.buildTypes(),
                hashJoinSpec.buildHashChannels().stream()
                        .map(hashJoinSpec.buildTypes()::get)
                        .toList(),
                1,
                false,
                new TypeOperators());
        JoinBridgeManager<PartitionedLookupSourceFactory> joinBridgeManager = new JoinBridgeManager<>(
                false,
                lookupSourceFactory,
                lookupSourceFactory.getOutputTypes());
        OperatorFactory joinFactory = io.trino.operator.OperatorFactories.join(
                JoinOperatorType.innerJoin(false, false),
                hashJoinSpec.operatorId(),
                new PlanNodeId("join-" + hashJoinSpec.operatorId()),
                joinBridgeManager,
                false,
                hashJoinSpec.probeTypes(),
                hashJoinSpec.probeJoinChannels(),
                Optional.empty());
        HashBuilderOperatorFactory buildOperatorFactory = new HashBuilderOperatorFactory(
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
            lookupSource.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS).close();
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to build Trino hash-join lookup source", exception);
        }

        return joinFactory;
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

    private static RowExpression greaterThan(RowExpression left, RowExpression right, Type type)
    {
        return lessThan(right, left, type);
    }

    private static RowExpression subtract(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression ifExpression(RowExpression condition, RowExpression whenTrue, RowExpression whenFalse, Type outputType)
    {
        return new SpecialForm(SpecialForm.Form.IF, outputType, List.of(condition, whenTrue, whenFalse), List.of());
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
            List<Integer> buildHashChannels) {}

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
