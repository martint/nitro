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

import com.google.common.util.concurrent.ListenableFuture;
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
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OrderByOperator;
import io.trino.operator.PagesIndex;
import io.trino.operator.SetBuilderOperator;
import io.trino.operator.TaskContext;
import io.trino.operator.TopNOperator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.NestedLoopJoinBridge;
import io.trino.operator.join.NestedLoopJoinPagesSupplier;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PageConsumerOperator;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTaskContext;
import io.trino.type.LikePattern;
import org.weakref.nitro.tpch.TpchParquetTables;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * The Trino operator-tree twin of {@link org.weakref.nitro.tpch.TpchParquetSupport} for apples-to-apples
 * comparison: each query assembles real Trino operators (`io.trino.operator.*`) into `Driver` pipelines over
 * the same parquet files, following the same logical operator sequence and stage boundaries as the Nitro
 * harness.
 *
 * Wave 1 carries the single-table scan queries (Q1, Q6); wave 2 adds the join family (Q3, Q4, Q5, Q10, Q12,
 * Q14, Q19) together with the hash-join and semi-join pipeline machinery; wave 3 adds the year-extraction
 * joins (Q7, Q8, Q9), the probe-outer join (Q13), the anti-join distinct count (Q16), and the aggregated
 * build side (Q18); wave 4 completes the suite with the decorrelated queries (Q2, Q11, Q15, Q17, Q20, Q21,
 * Q22) and the nested-loop scalar-broadcast machinery. Where the SQL references a view or subquery twice,
 * the harness assembles that subplan twice -- the engines have no operator-result reuse.
 */
public final class TrinoTpchParquetSupport
        implements AutoCloseable
{
    private static final ThreadLocal<TrinoOperatorCpuProfile> CURRENT_OPERATOR_CPU_PROFILE = new ThreadLocal<>();
    private static final String TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY = "nitro.tpch.trino.blockedWaitTimeoutSeconds";
    private static final int DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS = 600;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction COUNT_ALL = FUNCTION_RESOLUTION.getAggregateFunction("count", List.of());
    private static final TestingAggregationFunction BIGINT_MIN = FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_MAX = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(BIGINT));

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TrinoTpchParquetSupport"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TrinoTpchParquetSupport-scheduled"));
    private final AtomicInteger nextDynamicOperatorId = new AtomicInteger(20_000);
    private final AtomicInteger nextDynamicPipelineId = new AtomicInteger(30_000);
    private final OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());
    private final JoinCompiler joinCompiler = new JoinCompiler(new TypeOperators());
    private final int blockedWaitTimeoutSeconds = blockedWaitTimeoutSeconds();

    public static <T> T withOperatorCpuProfile(TrinoOperatorCpuProfile profile, Supplier<T> supplier)
    {
        TrinoOperatorCpuProfile previous = CURRENT_OPERATOR_CPU_PROFILE.get();
        CURRENT_OPERATOR_CPU_PROFILE.set(profile);
        try {
            return supplier.get();
        }
        finally {
            if (previous == null) {
                CURRENT_OPERATOR_CPU_PROFILE.remove();
            }
            else {
                CURRENT_OPERATOR_CPU_PROFILE.set(previous);
            }
        }
    }

    /**
     * Q1: one ScanFilterProject over lineitem (shipdate <= 1998-09-02, disc price and charge expressions)
     * feeding the (l_returnflag, l_linestatus) aggregation, fully sorted by the group keys.
     */
    public MaterializedResult query01(TpchParquetTables tables)
    {
        return executePipelinePlan(query01Plan(tables), query01OutputTypes(tables));
    }

    /**
     * Q6: one ScanFilterProject over lineitem (shipdate year 1994, discount within [0.05, 0.07],
     * quantity < 24), projecting extendedprice * discount into a global sum.
     */
    public MaterializedResult query06(TpchParquetTables tables)
    {
        return executePipelinePlan(query06Plan(tables), query06OutputTypes());
    }

    /**
     * Q3: customer(BUILDING) builds against the date-filtered orders probe, that result builds against the
     * shipdate-filtered lineitem probe; revenue aggregates by (orderkey, orderdate, shippriority); TopN 10
     * by (revenue DESC, orderdate).
     */
    public MaterializedResult query03(TpchParquetTables tables)
    {
        return executePipelinePlan(query03Plan(tables), query03OutputTypes(tables));
    }

    /**
     * Q4: the quarter's orders semi-joined against late lineitems (commitdate < receiptdate); order counts
     * by priority, sorted by priority.
     */
    public MaterializedResult query04(TpchParquetTables tables)
    {
        return executePipelinePlan(query04Plan(tables), query04OutputTypes(tables));
    }

    /**
     * Q5: region(ASIA) -> nation -> supplier build chain; customer -> orders(1994) -> lineitem probe chain;
     * the final join carries both l_suppkey = s_suppkey and c_nationkey = s_nationkey; revenue by n_name,
     * sorted descending.
     */
    public MaterializedResult query05(TpchParquetTables tables)
    {
        return executePipelinePlan(query05Plan(tables), query05OutputTypes(tables));
    }

    /**
     * Q10: customer probes the quarter-filtered orders build, that result probes the R-flagged lineitem build,
     * and nation joins last; revenue is grouped over the seven customer output columns and TopN selects 20.
     * This build/probe order mirrors Trino's optimized SQL plan.
     */
    public MaterializedResult query10(TpchParquetTables tables)
    {
        return executePipelinePlan(query10Plan(tables), query10OutputTypes(tables));
    }

    /**
     * Q12: orders probes the date/shipmode-filtered lineitem build; per-shipmode sums of the priority CASE
     * buckets, sorted by shipmode.
     */
    public MaterializedResult query12(TpchParquetTables tables)
    {
        return executePipelinePlan(query12Plan(tables), query12OutputTypes(tables));
    }

    /**
     * Q14: the September-1995 lineitem probes the part build; promo and total revenue accumulate globally
     * and the single output row carries 100 * promo / total.
     */
    public MaterializedResult query14(TpchParquetTables tables)
    {
        return executePipelinePlan(query14Plan(tables), query14OutputTypes());
    }

    /**
     * Q19: the shipinstruct/shipmode-filtered lineitem probes the part build; the disjunctive brand /
     * container / quantity / size predicate runs post-join (the plan's join filter); revenue sums globally.
     */
    public MaterializedResult query19(TpchParquetTables tables)
    {
        return executePipelinePlan(query19Plan(tables), query19OutputTypes());
    }

    /**
     * Q7: the 1995-1996 lineitem probes the supplier, orders, customer, and the two separate nation builds;
     * the FRANCE/GERMANY pairing runs post-join; volume grouped by (supp_nation, cust_nation,
     * year(l_shipdate)), sorted by the three keys.
     */
    public MaterializedResult query07(TpchParquetTables tables)
    {
        return executePipelinePlan(query07Plan(tables), query07OutputTypes(tables));
    }

    /**
     * Q8: part(ECONOMY ANODIZED STEEL) restricts lineitem; orders(1995-1996), customer, the AMERICA region
     * chain on the customer nation, and the supplier nation provide the CASE source; one output row per year
     * carries sum(brazil volume) / sum(volume), divided after the sort.
     */
    public MaterializedResult query08(TpchParquetTables tables)
    {
        return executePipelinePlan(query08Plan(tables), query08OutputTypes());
    }

    /**
     * Q9: part('%green%') restricts lineitem; supplier, the (suppkey, partkey) partsupp join, orders, and
     * nation provide profit = volume - supplycost * quantity, grouped by (nation, year(o_orderdate)) and
     * sorted (nation, year DESC).
     */
    public MaterializedResult query09(TpchParquetTables tables)
    {
        return executePipelinePlan(query09Plan(tables), query09OutputTypes(tables));
    }

    /**
     * Q13: customer LEFT-joins the NOT-LIKE-filtered orders (the build filter precedes the outer join);
     * count(o_orderkey) per customer counts only matches, then the count distribution sorts (custdist,
     * c_count) descending.
     */
    public MaterializedResult query13(TpchParquetTables tables)
    {
        return executePipelinePlan(query13Plan(tables), query13OutputTypes());
    }

    /**
     * Q16: partsupp joins the brand/type/size-filtered part, anti-joins the complaining suppliers, then the
     * plan's two-level distinct count: group by (brand, type, size, suppkey) with no aggregates, then count
     * per (brand, type, size), sorted (supplier_cnt DESC, brand, type, size).
     */
    public MaterializedResult query16(TpchParquetTables tables)
    {
        return executePipelinePlan(query16Plan(tables), query16OutputTypes(tables));
    }

    /**
     * Q18: lineitem grouped per order with sum(quantity) > 300 keys the orders build, customer joins that
     * build, and the full lineitem probes the result; quantity sums per (c_name, c_custkey, o_orderkey,
     * o_orderdate, o_totalprice); TopN 100 by (o_totalprice DESC, o_orderdate).
     */
    public MaterializedResult query18(TpchParquetTables tables)
    {
        return executePipelinePlan(query18Plan(tables), query18OutputTypes(tables));
    }

    /**
     * Q2: the minimum EUROPE supplycost per partkey joins back by (partkey, cost-equality); the size-15
     * %BRASS parts ride the partsupp / supplier / nation / region chain; top 100 by (acctbal DESC, n_name,
     * s_name, partkey).
     */
    public MaterializedResult query02(TpchParquetTables tables)
    {
        return executePipelinePlan(query02Plan(tables), query02OutputTypes(tables));
    }

    /**
     * Q11: the German partsupp value per partkey, the global value * 0.00001 threshold broadcast as a
     * scalar via nested-loop join (the value subplan assembles twice), and the parts above it, sorted by
     * value descending.
     */
    public MaterializedResult query11(TpchParquetTables tables)
    {
        return executePipelinePlan(query11Plan(tables), query11OutputTypes(tables));
    }

    /**
     * Q15: the quarter's revenue per supplier (the CTE, assembled twice), its max broadcast as a scalar
     * via nested-loop join, and supplier rows whose total equals the max, sorted by suppkey.
     */
    public MaterializedResult query15(TpchParquetTables tables)
    {
        return executePipelinePlan(query15Plan(tables), query15OutputTypes(tables));
    }

    /**
     * Q17: Brand#23 / MED BOX parts restrict lineitem; the per-partkey 0.2 * avg(quantity) threshold joins
     * back; quantities below it contribute extendedprice / 7 to the single output row.
     */
    public MaterializedResult query17(TpchParquetTables tables)
    {
        return executePipelinePlan(query17Plan(tables), query17OutputTypes());
    }

    /**
     * Q20: the forest-part partsupp rows whose availqty exceeds half the 1994 shipped quantity for that
     * (partkey, suppkey) key the CANADA suppliers via a semi join, sorted by name.
     */
    public MaterializedResult query20(TpchParquetTables tables)
    {
        return executePipelinePlan(query20Plan(tables), query20OutputTypes(tables));
    }

    /**
     * Q21: per-order distinct-supplier statistics decorrelate the inequality EXISTS pair -- a qualifying l1
     * row needs other suppliers on the order (count >= 2) and no OTHER late supplier (late count == 1);
     * counts per SAUDI ARABIA supplier name, top 100 by (numwait DESC, name).
     */
    public MaterializedResult query21(TpchParquetTables tables)
    {
        return executePipelinePlan(query21Plan(tables), query21OutputTypes(tables));
    }

    /**
     * Q22: country codes from the phone prefix; the positive-balance average over the seven codes (the
     * coded-customer subplan assembles twice) broadcasts as a scalar; customers above it with no orders
     * aggregate per code.
     */
    public MaterializedResult query22(TpchParquetTables tables)
    {
        return executePipelinePlan(query22Plan(tables), query22OutputTypes(tables));
    }

    private PipelinePlan query01Plan(TpchParquetTables tables)
    {
        List<String> columns = List.of("l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate");
        List<Type> scanTypes = tableColumnTypes(tables, "lineitem", columns);
        Type returnFlagType = scanTypes.get(0);
        Type lineStatusType = scanTypes.get(1);
        Type shipDateType = scanTypes.get(6);

        // disc_price = l_extendedprice * (1 - l_discount); charge = disc_price * (1 + l_tax)
        RowExpression discountedPrice = discountedPrice(3, 4);
        RowExpression charge = multiply(
                discountedPrice,
                add(field(5, DOUBLE), constant(1.0, DOUBLE), DOUBLE),
                DOUBLE);

        // [flag, status, qty, extprice, disc, discPrice, charge]
        List<Type> projectedTypes = List.of(returnFlagType, lineStatusType, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE);
        List<Type> outputTypes = query01OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), columns, "q01.scan.lineitem"),
                List.of(
                        namedFactoryStep("q01.filter.project", filterAndProjectFactory(
                                1_0,
                                Optional.of(lessThanOrEqual(
                                        field(6, shipDateType),
                                        constant(LocalDate.of(1998, 9, 2).toEpochDay(), shipDateType),
                                        shipDateType)),
                                List.of(
                                        field(0, returnFlagType),
                                        field(1, lineStatusType),
                                        field(2, DOUBLE),
                                        field(3, DOUBLE),
                                        field(4, DOUBLE),
                                        discountedPrice,
                                        charge),
                                projectedTypes)),
                        // [flag, status, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count]
                        namedFactoryStep("q01.group.final", hashAggregationFactory(
                                1_1,
                                List.of(returnFlagType, lineStatusType),
                                List.of(0, 1),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(6), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(4), OptionalInt.empty()),
                                COUNT_ALL.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        namedFactoryStep("q01.order_by", orderByFactory(
                                1_2,
                                outputTypes,
                                List.of(0, 1),
                                List.of(ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                "q01.sink",
                outputTypes);
    }

    private List<Type> query01OutputTypes(TpchParquetTables tables)
    {
        List<Type> keyTypes = tableColumnTypes(tables, "lineitem", List.of("l_returnflag", "l_linestatus"));
        return List.of(keyTypes.get(0), keyTypes.get(1), DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, BIGINT);
    }

    private PipelinePlan query06Plan(TpchParquetTables tables)
    {
        List<String> columns = List.of("l_shipdate", "l_discount", "l_quantity", "l_extendedprice");
        List<Type> scanTypes = tableColumnTypes(tables, "lineitem", columns);
        Type shipDateType = scanTypes.get(0);

        // SQL's 0.06 +/- 0.01 is DECIMAL arithmetic: the bounds are exactly 0.05 and 0.07.
        RowExpression filter = and(
                greaterThanOrEqual(field(0, shipDateType), constant(LocalDate.of(1994, 1, 1).toEpochDay(), shipDateType), shipDateType),
                lessThan(field(0, shipDateType), constant(LocalDate.of(1995, 1, 1).toEpochDay(), shipDateType), shipDateType),
                greaterThanOrEqual(field(1, DOUBLE), constant(0.05, DOUBLE), DOUBLE),
                lessThanOrEqual(field(1, DOUBLE), constant(0.07, DOUBLE), DOUBLE),
                lessThan(field(2, DOUBLE), constant(24.0, DOUBLE), DOUBLE));

        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), columns, "q06.scan.lineitem"),
                List.of(
                        namedFactoryStep("q06.filter.project", filterAndProjectFactory(
                                6_0,
                                Optional.of(filter),
                                List.of(multiply(field(3, DOUBLE), field(1, DOUBLE), DOUBLE)),
                                List.of(DOUBLE))),
                        namedFactoryStep("q06.aggregate.final", hashAggregationFactory(
                                6_1,
                                List.of(),
                                List.of(),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))),
                "q06.sink",
                query06OutputTypes());
    }

    private List<Type> query06OutputTypes()
    {
        return List.of(DOUBLE);
    }

    private PipelinePlan query03Plan(TpchParquetTables tables)
    {
        List<String> customerColumns = List.of("c_custkey", "c_mktsegment");
        List<Type> customerScanTypes = tableColumnTypes(tables, "customer", customerColumns);
        List<Type> customerTypes = List.of(customerScanTypes.get(0));
        PipelinePlan customer = relationPlan(
                tables,
                "customer",
                customerColumns,
                Optional.of(equalUtf8(1, "BUILDING", customerScanTypes.get(1))),
                List.of(field(0, customerScanTypes.get(0))),
                customerTypes,
                "q03.filter.customer",
                "q03.sink.customer");

        List<String> ordersColumns = List.of("o_orderkey", "o_custkey", "o_orderdate", "o_shippriority");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        Type orderDateType = ordersTypes.get(2);
        Type shipPriorityType = ordersTypes.get(3);
        // [o_orderkey, o_custkey, o_orderdate, o_shippriority, c_custkey]
        List<Type> ordersWithCustomerTypes = concatTypes(ordersTypes, customerTypes);
        PipelinePlan ordersWithCustomer = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("orders"), ordersColumns, "q03.scan.orders"),
                List.of(
                        namedFactoryStep("q03.filter.orders", filterAndProjectFactory(
                                3_0,
                                Optional.of(lessThan(field(2, orderDateType), constant(LocalDate.of(1995, 3, 15).toEpochDay(), orderDateType), orderDateType)),
                                identityProjections(ordersTypes),
                                ordersTypes)),
                        namedHashJoinStep("q03.join.customer", new HashJoinSpec(3_1, ordersTypes, List.of(1), customer, customerTypes, List.of(0)))),
                "q03.sink.orders",
                ordersWithCustomerTypes);

        List<String> lineitemColumns = List.of("l_orderkey", "l_extendedprice", "l_discount", "l_shipdate");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        Type shipDateType = lineitemTypes.get(3);
        // [orderkey, orderdate, shippriority, discPrice]
        List<Type> groupedTypes = List.of(BIGINT, orderDateType, shipPriorityType, DOUBLE);
        List<Type> outputTypes = query03OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q03.scan.lineitem"),
                List.of(
                        namedFactoryStep("q03.filter.lineitem", filterAndProjectFactory(
                                3_2,
                                Optional.of(greaterThan(field(3, shipDateType), constant(LocalDate.of(1995, 3, 15).toEpochDay(), shipDateType), shipDateType)),
                                identityProjections(lineitemTypes),
                                lineitemTypes)),
                        // [l_orderkey, l_extendedprice, l_discount, l_shipdate, o_orderkey, o_custkey, o_orderdate, o_shippriority, c_custkey]
                        namedHashJoinStep("q03.join.orders", new HashJoinSpec(3_3, lineitemTypes, List.of(0), ordersWithCustomer, ordersWithCustomerTypes, List.of(0))),
                        namedFactoryStep("q03.project.disc_price", filterAndProjectFactory(
                                3_4,
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(6, orderDateType), field(7, shipPriorityType), discountedPrice(1, 2)),
                                groupedTypes)),
                        namedFactoryStep("q03.group", hashAggregationFactory(
                                3_5,
                                List.of(BIGINT, orderDateType, shipPriorityType),
                                List.of(0, 1, 2),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        namedFactoryStep("q03.topn", topNFactory(3_6, groupedTypes, 10, List.of(3, 1), List.of(DESC_NULLS_LAST, ASC_NULLS_LAST))),
                        // SQL order: l_orderkey, revenue, o_orderdate, o_shippriority
                        namedFactoryStep("q03.project.final", filterAndProjectFactory(
                                3_7,
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(3, DOUBLE), field(1, orderDateType), field(2, shipPriorityType)),
                                outputTypes))),
                "q03.sink.final",
                outputTypes);
    }

    private List<Type> query03OutputTypes(TpchParquetTables tables)
    {
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", List.of("o_orderdate", "o_shippriority"));
        return List.of(BIGINT, DOUBLE, ordersTypes.get(0), ordersTypes.get(1));
    }

    private PipelinePlan query04Plan(TpchParquetTables tables)
    {
        List<String> lineitemColumns = List.of("l_orderkey", "l_commitdate", "l_receiptdate");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        Type commitDateType = lineitemTypes.get(1);
        PipelinePlan lateLineitems = relationPlan(
                tables,
                "lineitem",
                lineitemColumns,
                Optional.of(lessThan(field(1, commitDateType), field(2, lineitemTypes.get(2)), commitDateType)),
                List.of(field(0, BIGINT)),
                List.of(BIGINT),
                "q04.filter.lineitem",
                "q04.sink.lineitem");

        List<String> ordersColumns = List.of("o_orderkey", "o_orderdate", "o_orderpriority");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        Type orderDateType = ordersTypes.get(1);
        List<Type> outputTypes = query04OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("orders"), ordersColumns, "q04.scan.orders"),
                List.of(
                        namedFactoryStep("q04.filter.orders", filterAndProjectFactory(
                                4_0,
                                Optional.of(and(
                                        greaterThanOrEqual(field(1, orderDateType), constant(LocalDate.of(1993, 7, 1).toEpochDay(), orderDateType), orderDateType),
                                        lessThan(field(1, orderDateType), constant(LocalDate.of(1993, 10, 1).toEpochDay(), orderDateType), orderDateType))),
                                identityProjections(ordersTypes),
                                ordersTypes)),
                        namedSemiJoinStep("q04.semi.lineitem", new SemiJoinSpec(4_1, ordersTypes, 0, lateLineitems, BIGINT, 0)),
                        namedFactoryStep("q04.filter.matched", filterAndProjectFactory(
                                4_2,
                                Optional.of(field(ordersTypes.size(), BOOLEAN)),
                                identityProjections(ordersTypes),
                                ordersTypes)),
                        namedFactoryStep("q04.group", hashAggregationFactory(
                                4_3,
                                List.of(ordersTypes.get(2)),
                                List.of(2),
                                COUNT_ALL.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        namedFactoryStep("q04.order_by", orderByFactory(4_4, outputTypes, List.of(0), List.of(ASC_NULLS_LAST)))),
                "q04.sink.final",
                outputTypes);
    }

    private List<Type> query04OutputTypes(TpchParquetTables tables)
    {
        Type priorityType = tableColumnTypes(tables, "orders", List.of("o_orderpriority")).getFirst();
        return List.of(priorityType, BIGINT);
    }

    private PipelinePlan query05Plan(TpchParquetTables tables)
    {
        List<String> regionColumns = List.of("r_regionkey", "r_name");
        List<Type> regionScanTypes = tableColumnTypes(tables, "region", regionColumns);
        PipelinePlan region = relationPlan(
                tables,
                "region",
                regionColumns,
                Optional.of(equalUtf8(1, "ASIA", regionScanTypes.get(1))),
                List.of(field(0, regionScanTypes.get(0))),
                List.of(regionScanTypes.get(0)),
                "q05.filter.region",
                "q05.sink.region");

        List<String> nationColumns = List.of("n_nationkey", "n_regionkey", "n_name");
        List<Type> nationScanTypes = tableColumnTypes(tables, "nation", nationColumns);
        Type nationNameType = nationScanTypes.get(2);
        // [n_nationkey, n_regionkey, n_name, r_regionkey] -> [n_nationkey, n_name]
        List<Type> nationInAsiaTypes = List.of(nationScanTypes.get(0), nationNameType);
        PipelinePlan nationInAsia = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("nation"), nationColumns, "q05.scan.nation"),
                List.of(
                        namedHashJoinStep("q05.join.region", new HashJoinSpec(5_0, nationScanTypes, List.of(1), region, List.of(regionScanTypes.get(0)), List.of(0))),
                        namedFactoryStep("q05.project.nation", filterAndProjectFactory(
                                5_1,
                                Optional.empty(),
                                List.of(field(0, nationScanTypes.get(0)), field(2, nationNameType)),
                                nationInAsiaTypes))),
                "q05.sink.nation",
                nationInAsiaTypes);

        List<String> supplierColumns = List.of("s_suppkey", "s_nationkey");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        // [s_suppkey, s_nationkey, n_nationkey, n_name] -> [s_suppkey, s_nationkey, n_name]
        List<Type> supplierInAsiaTypes = List.of(supplierTypes.get(0), supplierTypes.get(1), nationNameType);
        PipelinePlan supplierInAsia = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("supplier"), supplierColumns, "q05.scan.supplier"),
                List.of(
                        namedHashJoinStep("q05.join.nation", new HashJoinSpec(5_2, supplierTypes, List.of(1), nationInAsia, nationInAsiaTypes, List.of(0))),
                        namedFactoryStep("q05.project.supplier", filterAndProjectFactory(
                                5_3,
                                Optional.empty(),
                                List.of(field(0, supplierTypes.get(0)), field(1, supplierTypes.get(1)), field(3, nationNameType)),
                                supplierInAsiaTypes))),
                "q05.sink.supplier",
                supplierInAsiaTypes);

        List<String> customerColumns = List.of("c_custkey", "c_nationkey");
        List<Type> customerTypes = tableColumnTypes(tables, "customer", customerColumns);
        PipelinePlan customer = relationPlan(
                tables,
                "customer",
                customerColumns,
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes,
                "q05.scan.customer",
                "q05.sink.customer");

        List<String> ordersColumns = List.of("o_orderkey", "o_custkey", "o_orderdate");
        List<Type> ordersScanTypes = tableColumnTypes(tables, "orders", ordersColumns);
        Type orderDateType = ordersScanTypes.get(2);
        List<Type> ordersTypes = List.of(ordersScanTypes.get(0), ordersScanTypes.get(1));
        // [o_orderkey, o_custkey, c_custkey, c_nationkey] -> [o_orderkey, c_nationkey]
        List<Type> ordersWithNationTypes = List.of(ordersScanTypes.get(0), customerTypes.get(1));
        PipelinePlan ordersWithNation = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("orders"), ordersColumns, "q05.scan.orders"),
                List.of(
                        namedFactoryStep("q05.filter.orders", filterAndProjectFactory(
                                5_4,
                                Optional.of(and(
                                        greaterThanOrEqual(field(2, orderDateType), constant(LocalDate.of(1994, 1, 1).toEpochDay(), orderDateType), orderDateType),
                                        lessThan(field(2, orderDateType), constant(LocalDate.of(1995, 1, 1).toEpochDay(), orderDateType), orderDateType))),
                                List.of(field(0, ordersScanTypes.get(0)), field(1, ordersScanTypes.get(1))),
                                ordersTypes)),
                        namedHashJoinStep("q05.join.customer", new HashJoinSpec(5_5, ordersTypes, List.of(1), customer, customerTypes, List.of(0))),
                        namedFactoryStep("q05.project.orders", filterAndProjectFactory(
                                5_6,
                                Optional.empty(),
                                List.of(field(0, ordersScanTypes.get(0)), field(3, customerTypes.get(1))),
                                ordersWithNationTypes))),
                "q05.sink.orders",
                ordersWithNationTypes);

        List<String> lineitemColumns = List.of("l_orderkey", "l_suppkey", "l_extendedprice", "l_discount");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        // [n_name, discPrice]
        List<Type> groupedTypes = List.of(nationNameType, DOUBLE);
        List<Type> outputTypes = query05OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q05.scan.lineitem"),
                List.of(
                        // [l_orderkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, c_nationkey]
                        namedHashJoinStep("q05.join.orders", new HashJoinSpec(5_7, lineitemTypes, List.of(0), ordersWithNation, ordersWithNationTypes, List.of(0))),
                        // + [s_suppkey, s_nationkey, n_name]
                        namedHashJoinStep("q05.join.supplier", new HashJoinSpec(5_8, concatTypes(lineitemTypes, ordersWithNationTypes), List.of(1, 5), supplierInAsia, supplierInAsiaTypes, List.of(0, 1))),
                        namedFactoryStep("q05.project.disc_price", filterAndProjectFactory(
                                5_9,
                                Optional.empty(),
                                List.of(field(8, nationNameType), discountedPrice(2, 3)),
                                groupedTypes)),
                        namedFactoryStep("q05.group", hashAggregationFactory(
                                5_10,
                                List.of(nationNameType),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        namedFactoryStep("q05.order_by", orderByFactory(5_11, outputTypes, List.of(1), List.of(DESC_NULLS_LAST)))),
                "q05.sink.final",
                outputTypes);
    }

    private List<Type> query05OutputTypes(TpchParquetTables tables)
    {
        Type nationNameType = tableColumnTypes(tables, "nation", List.of("n_name")).getFirst();
        return List.of(nationNameType, DOUBLE);
    }

    private PipelinePlan query10Plan(TpchParquetTables tables)
    {
        List<String> customerColumns = List.of("c_custkey", "c_name", "c_acctbal", "c_phone", "c_address", "c_comment", "c_nationkey");
        List<Type> customerTypes = tableColumnTypes(tables, "customer", customerColumns);
        PipelinePlan customer = relationPlan(
                tables,
                "customer",
                customerColumns,
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes,
                "q10.scan.customer",
                "q10.sink.customer");

        List<String> ordersColumns = List.of("o_orderkey", "o_custkey", "o_orderdate");
        List<Type> ordersScanTypes = tableColumnTypes(tables, "orders", ordersColumns);
        Type orderDateType = ordersScanTypes.get(2);
        List<Type> ordersTypes = List.of(ordersScanTypes.get(0), ordersScanTypes.get(1));
        PipelinePlan orders = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("orders"), ordersColumns, "q10.scan.orders"),
                List.of(
                        namedFactoryStep("q10.filter.orders", filterAndProjectFactory(
                                10_0,
                                Optional.of(and(
                                        greaterThanOrEqual(field(2, orderDateType), constant(LocalDate.of(1993, 10, 1).toEpochDay(), orderDateType), orderDateType),
                                        lessThan(field(2, orderDateType), constant(LocalDate.of(1994, 1, 1).toEpochDay(), orderDateType), orderDateType))),
                                List.of(field(0, ordersScanTypes.get(0)), field(1, ordersScanTypes.get(1))),
                                ordersTypes))),
                "q10.sink.orders",
                ordersTypes);
        // [c_custkey, c_name, c_acctbal, c_phone, c_address, c_comment, c_nationkey, o_orderkey, o_custkey]
        List<Type> customerWithOrdersTypes = concatTypes(customerTypes, ordersTypes);
        PipelinePlan customerWithOrders = appendPlan(
                customer,
                List.of(namedHashJoinStep("q10.join.orders", new HashJoinSpec(10_1, customerTypes, List.of(0), orders, ordersTypes, List.of(1)))),
                "q10.sink.customer_orders",
                customerWithOrdersTypes);

        List<String> nationColumns = List.of("n_nationkey", "n_name");
        List<Type> nationTypes = tableColumnTypes(tables, "nation", nationColumns);
        PipelinePlan nation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.empty(),
                identityProjections(nationTypes),
                nationTypes,
                "q10.scan.nation",
                "q10.sink.nation");

        List<String> lineitemColumns = List.of("l_orderkey", "l_extendedprice", "l_discount", "l_returnflag");
        List<Type> lineitemScanTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        List<Type> lineitemTypes = List.of(lineitemScanTypes.get(0), DOUBLE, DOUBLE);
        PipelinePlan lineitem = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q10.scan.lineitem"),
                List.of(namedFactoryStep("q10.filter.lineitem", filterAndProjectFactory(
                        10_2,
                        Optional.of(equalUtf8(3, "R", lineitemScanTypes.get(3))),
                        List.of(field(0, lineitemScanTypes.get(0)), field(1, DOUBLE), field(2, DOUBLE)),
                        lineitemTypes))),
                "q10.sink.lineitem",
                lineitemTypes);
        List<Type> customerOrdersLineitemTypes = concatTypes(customerWithOrdersTypes, lineitemTypes);
        PipelinePlan customerOrdersLineitem = appendPlan(
                customerWithOrders,
                List.of(namedHashJoinStep("q10.join.lineitem", new HashJoinSpec(10_3, customerWithOrdersTypes, List.of(7), lineitem, lineitemTypes, List.of(0)))),
                "q10.sink.customer_orders_lineitem",
                customerOrdersLineitemTypes);
        // [c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment, discPrice]
        List<Type> groupedTypes = List.of(
                customerTypes.get(0),
                customerTypes.get(1),
                customerTypes.get(2),
                customerTypes.get(3),
                nationTypes.get(1),
                customerTypes.get(4),
                customerTypes.get(5),
                DOUBLE);
        List<Type> outputTypes = query10OutputTypes(tables);
        return appendPlan(
                customerOrdersLineitem,
                List.of(
                        // + [n_nationkey, n_name]
                        namedHashJoinStep("q10.join.nation", new HashJoinSpec(10_4, customerOrdersLineitemTypes, List.of(6), nation, nationTypes, List.of(0))),
                        namedFactoryStep("q10.project.disc_price", filterAndProjectFactory(
                                10_5,
                                Optional.empty(),
                                List.of(
                                        field(0, customerTypes.get(0)),
                                        field(1, customerTypes.get(1)),
                                        field(2, customerTypes.get(2)),
                                        field(3, customerTypes.get(3)),
                                        field(13, nationTypes.get(1)),
                                        field(4, customerTypes.get(4)),
                                        field(5, customerTypes.get(5)),
                                        discountedPrice(10, 11)),
                                groupedTypes)),
                        namedFactoryStep("q10.group", hashAggregationFactory(
                                10_6,
                                groupedTypes.subList(0, 7),
                                List.of(0, 1, 2, 3, 4, 5, 6),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(7), OptionalInt.empty()))),
                        namedFactoryStep("q10.topn", topNFactory(10_7, groupedTypes, 20, List.of(7), List.of(DESC_NULLS_LAST))),
                        // SQL order: c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment
                        namedFactoryStep("q10.project.final", filterAndProjectFactory(
                                10_8,
                                Optional.empty(),
                                List.of(
                                        field(0, customerTypes.get(0)),
                                        field(1, customerTypes.get(1)),
                                        field(7, DOUBLE),
                                        field(2, customerTypes.get(2)),
                                        field(4, nationTypes.get(1)),
                                        field(5, customerTypes.get(4)),
                                        field(3, customerTypes.get(3)),
                                        field(6, customerTypes.get(5))),
                                outputTypes))),
                "q10.sink.final",
                outputTypes);
    }

    private List<Type> query10OutputTypes(TpchParquetTables tables)
    {
        List<Type> customerTypes = tableColumnTypes(tables, "customer", List.of("c_custkey", "c_name", "c_acctbal", "c_phone", "c_address", "c_comment"));
        Type nationNameType = tableColumnTypes(tables, "nation", List.of("n_name")).getFirst();
        return List.of(customerTypes.get(0), customerTypes.get(1), DOUBLE, customerTypes.get(2), nationNameType, customerTypes.get(4), customerTypes.get(3), customerTypes.get(5));
    }

    private PipelinePlan query12Plan(TpchParquetTables tables)
    {
        List<String> lineitemColumns = List.of("l_orderkey", "l_shipmode", "l_commitdate", "l_receiptdate", "l_shipdate");
        List<Type> lineitemScanTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        Type shipModeType = lineitemScanTypes.get(1);
        Type commitDateType = lineitemScanTypes.get(2);
        Type receiptDateType = lineitemScanTypes.get(3);
        List<Type> lineitemTypes = List.of(lineitemScanTypes.get(0), shipModeType);
        PipelinePlan lineitem = relationPlan(
                tables,
                "lineitem",
                lineitemColumns,
                Optional.of(and(
                        or(equalUtf8(1, "MAIL", shipModeType), equalUtf8(1, "SHIP", shipModeType)),
                        lessThan(field(2, commitDateType), field(3, receiptDateType), commitDateType),
                        lessThan(field(4, lineitemScanTypes.get(4)), field(2, commitDateType), commitDateType),
                        greaterThanOrEqual(field(3, receiptDateType), constant(LocalDate.of(1994, 1, 1).toEpochDay(), receiptDateType), receiptDateType),
                        lessThan(field(3, receiptDateType), constant(LocalDate.of(1995, 1, 1).toEpochDay(), receiptDateType), receiptDateType))),
                List.of(field(0, lineitemScanTypes.get(0)), field(1, shipModeType)),
                lineitemTypes,
                "q12.filter.lineitem",
                "q12.sink.lineitem");

        List<String> ordersColumns = List.of("o_orderkey", "o_orderpriority");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        Type priorityType = ordersTypes.get(1);
        RowExpression isUrgent = or(equalUtf8(1, "1-URGENT", priorityType), equalUtf8(1, "2-HIGH", priorityType));
        // [shipmode, high, low]
        List<Type> bucketTypes = List.of(shipModeType, BIGINT, BIGINT);
        List<Type> outputTypes = query12OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("orders"), ordersColumns, "q12.scan.orders"),
                List.of(
                        // [o_orderkey, o_orderpriority, l_orderkey, l_shipmode]
                        namedHashJoinStep("q12.join.lineitem", new HashJoinSpec(12_0, ordersTypes, List.of(0), lineitem, lineitemTypes, List.of(0))),
                        namedFactoryStep("q12.project.buckets", filterAndProjectFactory(
                                12_1,
                                Optional.empty(),
                                List.of(
                                        field(3, shipModeType),
                                        ifExpression(isUrgent, constant(1L, BIGINT), constant(0L, BIGINT), BIGINT),
                                        ifExpression(isUrgent, constant(0L, BIGINT), constant(1L, BIGINT), BIGINT)),
                                bucketTypes)),
                        namedFactoryStep("q12.group", hashAggregationFactory(
                                12_2,
                                List.of(shipModeType),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT)).createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        namedFactoryStep("q12.order_by", orderByFactory(12_3, outputTypes, List.of(0), List.of(ASC_NULLS_LAST)))),
                "q12.sink.final",
                outputTypes);
    }

    private List<Type> query12OutputTypes(TpchParquetTables tables)
    {
        Type shipModeType = tableColumnTypes(tables, "lineitem", List.of("l_shipmode")).getFirst();
        return List.of(shipModeType, BIGINT, BIGINT);
    }

    private PipelinePlan query14Plan(TpchParquetTables tables)
    {
        List<String> partColumns = List.of("p_partkey", "p_type");
        List<Type> partTypes = tableColumnTypes(tables, "part", partColumns);
        PipelinePlan part = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.empty(),
                identityProjections(partTypes),
                partTypes,
                "q14.scan.part",
                "q14.sink.part");

        List<String> lineitemColumns = List.of("l_partkey", "l_extendedprice", "l_discount", "l_shipdate");
        List<Type> lineitemScanTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        Type shipDateType = lineitemScanTypes.get(3);
        List<Type> lineitemTypes = List.of(lineitemScanTypes.get(0), DOUBLE);
        RowExpression discountedPrice = discountedPrice(1, 2);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q14.scan.lineitem"),
                List.of(
                        namedFactoryStep("q14.filter.lineitem", filterAndProjectFactory(
                                14_0,
                                Optional.of(and(
                                        greaterThanOrEqual(field(3, shipDateType), constant(LocalDate.of(1995, 9, 1).toEpochDay(), shipDateType), shipDateType),
                                        lessThan(field(3, shipDateType), constant(LocalDate.of(1995, 10, 1).toEpochDay(), shipDateType), shipDateType))),
                                List.of(field(0, lineitemScanTypes.get(0)), discountedPrice),
                                lineitemTypes)),
                        // [l_partkey, part_revenue, p_partkey, p_type]
                        namedHashJoinStep("q14.join.part", new HashJoinSpec(14_1, lineitemTypes, List.of(0), part, partTypes, List.of(0))),
                        // [promoAmount, discPrice]
                        namedFactoryStep("q14.project.promo", filterAndProjectFactory(
                                14_2,
                                Optional.empty(),
                                List.of(
                                        ifExpression(like(field(3, partTypes.get(1)), "PROMO%"), field(1, DOUBLE), constant(0.0, DOUBLE), DOUBLE),
                                        field(1, DOUBLE)),
                                List.of(DOUBLE, DOUBLE))),
                        namedFactoryStep("q14.aggregate.final", hashAggregationFactory(
                                14_3,
                                List.of(),
                                List.of(),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        // 100 * promo / total
                        namedFactoryStep("q14.project.final", filterAndProjectFactory(
                                14_4,
                                Optional.empty(),
                                List.of(multiply(constant(100.0, DOUBLE), divide(field(0, DOUBLE), field(1, DOUBLE), DOUBLE), DOUBLE)),
                                query14OutputTypes()))),
                "q14.sink.final",
                query14OutputTypes());
    }

    private List<Type> query14OutputTypes()
    {
        return List.of(DOUBLE);
    }

    private PipelinePlan query19Plan(TpchParquetTables tables)
    {
        List<String> partColumns = List.of("p_partkey", "p_brand", "p_container", "p_size");
        List<Type> partTypes = tableColumnTypes(tables, "part", partColumns);
        PipelinePlan part = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.empty(),
                identityProjections(partTypes),
                partTypes,
                "q19.scan.part",
                "q19.sink.part");

        List<String> lineitemColumns = List.of("l_partkey", "l_quantity", "l_extendedprice", "l_discount", "l_shipinstruct", "l_shipmode");
        List<Type> lineitemScanTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        Type shipInstructType = lineitemScanTypes.get(4);
        Type shipModeType = lineitemScanTypes.get(5);
        List<Type> lineitemTypes = List.of(lineitemScanTypes.get(0), DOUBLE, DOUBLE, DOUBLE);
        Type brandType = partTypes.get(1);
        Type containerType = partTypes.get(2);
        Type sizeType = partTypes.get(3);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q19.scan.lineitem"),
                List.of(
                        namedFactoryStep("q19.filter.lineitem", filterAndProjectFactory(
                                19_0,
                                Optional.of(and(
                                        equalUtf8(4, "DELIVER IN PERSON", shipInstructType),
                                        or(equalUtf8(5, "AIR", shipModeType), equalUtf8(5, "AIR REG", shipModeType)))),
                                List.of(field(0, lineitemScanTypes.get(0)), field(1, DOUBLE), field(2, DOUBLE), field(3, DOUBLE)),
                                lineitemTypes)),
                        // [l_partkey, l_quantity, l_extendedprice, l_discount, p_partkey, p_brand, p_container, p_size]
                        namedHashJoinStep("q19.join.part", new HashJoinSpec(19_1, lineitemTypes, List.of(0), part, partTypes, List.of(0))),
                        // the plan's join filter: disjunctive brand / container / quantity / size branches
                        namedFactoryStep("q19.filter.branches", filterAndProjectFactory(
                                19_2,
                                Optional.of(or(
                                        query19Branch(brandType, containerType, sizeType, "Brand#12", List.of("SM CASE", "SM BOX", "SM PACK", "SM PKG"), 1.0, 11.0, 5),
                                        query19Branch(brandType, containerType, sizeType, "Brand#23", List.of("MED BAG", "MED BOX", "MED PKG", "MED PACK"), 10.0, 20.0, 10),
                                        query19Branch(brandType, containerType, sizeType, "Brand#34", List.of("LG CASE", "LG BOX", "LG PACK", "LG PKG"), 20.0, 30.0, 15))),
                                List.of(discountedPrice(2, 3)),
                                query19OutputTypes())),
                        namedFactoryStep("q19.aggregate.final", hashAggregationFactory(
                                19_3,
                                List.of(),
                                List.of(),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))),
                "q19.sink.final",
                query19OutputTypes());
    }

    private List<Type> query19OutputTypes()
    {
        return List.of(DOUBLE);
    }

    private static RowExpression query19Branch(Type brandType, Type containerType, Type sizeType, String brand, List<String> containers, double quantityLow, double quantityHigh, long sizeHigh)
    {
        return and(
                equalUtf8(5, brand, brandType),
                or(
                        equalUtf8(6, containers.get(0), containerType),
                        equalUtf8(6, containers.get(1), containerType),
                        equalUtf8(6, containers.get(2), containerType),
                        equalUtf8(6, containers.get(3), containerType)),
                greaterThanOrEqual(field(1, DOUBLE), constant(quantityLow, DOUBLE), DOUBLE),
                lessThanOrEqual(field(1, DOUBLE), constant(quantityHigh, DOUBLE), DOUBLE),
                greaterThan(field(7, sizeType), constant(0L, sizeType), sizeType),
                lessThan(field(7, sizeType), constant(sizeHigh + 1, sizeType), sizeType));
    }

    private PipelinePlan query07Plan(TpchParquetTables tables)
    {
        List<String> supplierColumns = List.of("s_suppkey", "s_nationkey");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        PipelinePlan supplier = relationPlan(
                tables,
                "supplier",
                supplierColumns,
                Optional.empty(),
                identityProjections(supplierTypes),
                supplierTypes,
                "q07.scan.supplier",
                "q07.sink.supplier");

        List<String> ordersColumns = List.of("o_orderkey", "o_custkey");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        PipelinePlan orders = relationPlan(
                tables,
                "orders",
                ordersColumns,
                Optional.empty(),
                identityProjections(ordersTypes),
                ordersTypes,
                "q07.scan.orders",
                "q07.sink.orders");

        List<String> customerColumns = List.of("c_custkey", "c_nationkey");
        List<Type> customerTypes = tableColumnTypes(tables, "customer", customerColumns);
        PipelinePlan customer = relationPlan(
                tables,
                "customer",
                customerColumns,
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes,
                "q07.scan.customer",
                "q07.sink.customer");

        // the Nitro harness joins nation twice as two separate builds
        List<String> nationColumns = List.of("n_nationkey", "n_name");
        List<Type> nationTypes = tableColumnTypes(tables, "nation", nationColumns);
        Type nationNameType = nationTypes.get(1);
        PipelinePlan supplierNation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.empty(),
                identityProjections(nationTypes),
                nationTypes,
                "q07.scan.nation_supplier",
                "q07.sink.nation_supplier");
        PipelinePlan customerNation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.empty(),
                identityProjections(nationTypes),
                nationTypes,
                "q07.scan.nation_customer",
                "q07.sink.nation_customer");

        List<String> lineitemColumns = List.of("l_orderkey", "l_suppkey", "l_extendedprice", "l_discount", "l_shipdate");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        Type shipDateType = lineitemTypes.get(4);
        List<Type> withSupplierTypes = concatTypes(lineitemTypes, supplierTypes);
        List<Type> withOrdersTypes = concatTypes(withSupplierTypes, ordersTypes);
        List<Type> withCustomerTypes = concatTypes(withOrdersTypes, customerTypes);
        List<Type> withSupplierNationTypes = concatTypes(withCustomerTypes, nationTypes);
        // [supp_nation, cust_nation, l_year, volume]
        List<Type> groupedTypes = List.of(nationNameType, nationNameType, BIGINT, DOUBLE);
        List<Type> outputTypes = query07OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q07.scan.lineitem"),
                List.of(
                        namedFactoryStep("q07.filter.lineitem", filterAndProjectFactory(
                                7_0,
                                Optional.of(and(
                                        greaterThanOrEqual(field(4, shipDateType), constant(LocalDate.of(1995, 1, 1).toEpochDay(), shipDateType), shipDateType),
                                        lessThanOrEqual(field(4, shipDateType), constant(LocalDate.of(1996, 12, 31).toEpochDay(), shipDateType), shipDateType))),
                                identityProjections(lineitemTypes),
                                lineitemTypes)),
                        // [l x5, s_suppkey, s_nationkey]
                        namedHashJoinStep("q07.join.supplier", new HashJoinSpec(7_1, lineitemTypes, List.of(1), supplier, supplierTypes, List.of(0))),
                        // + [o_orderkey, o_custkey -> 7,8]
                        namedHashJoinStep("q07.join.orders", new HashJoinSpec(7_2, withSupplierTypes, List.of(0), orders, ordersTypes, List.of(0))),
                        // + [c_custkey, c_nationkey -> 9,10]
                        namedHashJoinStep("q07.join.customer", new HashJoinSpec(7_3, withOrdersTypes, List.of(8), customer, customerTypes, List.of(0))),
                        // + [n_nationkey, n_name -> 11,12] (supplier nation)
                        namedHashJoinStep("q07.join.nation_supplier", new HashJoinSpec(7_4, withCustomerTypes, List.of(6), supplierNation, nationTypes, List.of(0))),
                        // + [n_nationkey, n_name -> 13,14] (customer nation)
                        namedHashJoinStep("q07.join.nation_customer", new HashJoinSpec(7_5, withSupplierNationTypes, List.of(10), customerNation, nationTypes, List.of(0))),
                        namedFactoryStep("q07.filter.nation_pair", filterAndProjectFactory(
                                7_6,
                                Optional.of(or(
                                        and(equalUtf8(12, "FRANCE", nationNameType), equalUtf8(14, "GERMANY", nationNameType)),
                                        and(equalUtf8(12, "GERMANY", nationNameType), equalUtf8(14, "FRANCE", nationNameType)))),
                                List.of(
                                        field(12, nationNameType),
                                        field(14, nationNameType),
                                        year(field(4, shipDateType), shipDateType),
                                        discountedPrice(2, 3)),
                                groupedTypes)),
                        namedFactoryStep("q07.group", hashAggregationFactory(
                                7_7,
                                List.of(nationNameType, nationNameType, BIGINT),
                                List.of(0, 1, 2),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty()))),
                        namedFactoryStep("q07.order_by", orderByFactory(
                                7_8,
                                outputTypes,
                                List.of(0, 1, 2),
                                List.of(ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                "q07.sink.final",
                outputTypes);
    }

    private List<Type> query07OutputTypes(TpchParquetTables tables)
    {
        Type nationNameType = tableColumnTypes(tables, "nation", List.of("n_name")).getFirst();
        return List.of(nationNameType, nationNameType, BIGINT, DOUBLE);
    }

    private PipelinePlan query08Plan(TpchParquetTables tables)
    {
        List<String> partColumns = List.of("p_partkey", "p_type");
        List<Type> partScanTypes = tableColumnTypes(tables, "part", partColumns);
        List<Type> partTypes = List.of(partScanTypes.get(0));
        PipelinePlan part = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.of(equalUtf8(1, "ECONOMY ANODIZED STEEL", partScanTypes.get(1))),
                List.of(field(0, partScanTypes.get(0))),
                partTypes,
                "q08.filter.part",
                "q08.sink.part");

        List<String> ordersColumns = List.of("o_orderkey", "o_custkey", "o_orderdate");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        Type orderDateType = ordersTypes.get(2);
        PipelinePlan orders = relationPlan(
                tables,
                "orders",
                ordersColumns,
                Optional.of(and(
                        greaterThanOrEqual(field(2, orderDateType), constant(LocalDate.of(1995, 1, 1).toEpochDay(), orderDateType), orderDateType),
                        lessThanOrEqual(field(2, orderDateType), constant(LocalDate.of(1996, 12, 31).toEpochDay(), orderDateType), orderDateType))),
                identityProjections(ordersTypes),
                ordersTypes,
                "q08.filter.orders",
                "q08.sink.orders");

        List<String> customerColumns = List.of("c_custkey", "c_nationkey");
        List<Type> customerTypes = tableColumnTypes(tables, "customer", customerColumns);
        PipelinePlan customer = relationPlan(
                tables,
                "customer",
                customerColumns,
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes,
                "q08.scan.customer",
                "q08.sink.customer");

        List<String> regionColumns = List.of("r_regionkey", "r_name");
        List<Type> regionScanTypes = tableColumnTypes(tables, "region", regionColumns);
        List<Type> regionTypes = List.of(regionScanTypes.get(0));
        PipelinePlan region = relationPlan(
                tables,
                "region",
                regionColumns,
                Optional.of(equalUtf8(1, "AMERICA", regionScanTypes.get(1))),
                List.of(field(0, regionScanTypes.get(0))),
                regionTypes,
                "q08.filter.region",
                "q08.sink.region");

        List<String> nationRegionColumns = List.of("n_nationkey", "n_regionkey");
        List<Type> nationRegionTypes = tableColumnTypes(tables, "nation", nationRegionColumns);
        // [n_nationkey, n_regionkey, r_regionkey] -> [n_nationkey]
        List<Type> nationInAmericaTypes = List.of(nationRegionTypes.get(0));
        PipelinePlan nationInAmerica = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("nation"), nationRegionColumns, "q08.scan.nation_customer"),
                List.of(
                        namedHashJoinStep("q08.join.region", new HashJoinSpec(8_0, nationRegionTypes, List.of(1), region, regionTypes, List.of(0))),
                        namedFactoryStep("q08.project.nation_customer", filterAndProjectFactory(
                                8_1,
                                Optional.empty(),
                                List.of(field(0, nationRegionTypes.get(0))),
                                nationInAmericaTypes))),
                "q08.sink.nation_customer",
                nationInAmericaTypes);

        List<Type> customersInAmericaTypes = List.of(customerTypes.get(0));
        PipelinePlan customersInAmerica = appendPlan(
                customer,
                List.of(
                        namedHashJoinStep("q08.join.nation_customer", new HashJoinSpec(8_13, customerTypes, List.of(1), nationInAmerica, nationInAmericaTypes, List.of(0))),
                        namedFactoryStep("q08.project.customer", filterAndProjectFactory(
                                8_14,
                                Optional.empty(),
                                List.of(field(0, customerTypes.get(0))),
                                customersInAmericaTypes))),
                "q08.sink.customer_in_america",
                customersInAmericaTypes);

        List<Type> ordersInAmericaTypes = List.of(ordersTypes.get(0), ordersTypes.get(2));
        PipelinePlan ordersInAmerica = appendPlan(
                orders,
                List.of(
                        namedHashJoinStep("q08.join.customer", new HashJoinSpec(8_15, ordersTypes, List.of(1), customersInAmerica, customersInAmericaTypes, List.of(0))),
                        namedFactoryStep("q08.project.orders", filterAndProjectFactory(
                                8_16,
                                Optional.empty(),
                                List.of(field(0, ordersTypes.get(0)), field(2, ordersTypes.get(2))),
                                ordersInAmericaTypes))),
                "q08.sink.orders_in_america",
                ordersInAmericaTypes);

        List<String> nationColumns = List.of("n_nationkey", "n_name");
        List<Type> nationTypes = tableColumnTypes(tables, "nation", nationColumns);
        Type nationNameType = nationTypes.get(1);
        PipelinePlan supplierNation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.empty(),
                identityProjections(nationTypes),
                nationTypes,
                "q08.scan.nation_supplier",
                "q08.sink.nation_supplier");

        List<String> supplierColumns = List.of("s_suppkey", "s_nationkey");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        // [s_suppkey, s_nationkey, n_nationkey, n_name] -> [s_suppkey, n_name]
        List<Type> supplierWithNationTypes = List.of(supplierTypes.get(0), nationNameType);
        PipelinePlan supplierWithNation = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("supplier"), supplierColumns, "q08.scan.supplier"),
                List.of(
                        namedHashJoinStep("q08.join.nation_supplier", new HashJoinSpec(8_2, supplierTypes, List.of(1), supplierNation, nationTypes, List.of(0))),
                        namedFactoryStep("q08.project.supplier", filterAndProjectFactory(
                                8_3,
                                Optional.empty(),
                                List.of(field(0, supplierTypes.get(0)), field(3, nationNameType)),
                                supplierWithNationTypes))),
                "q08.sink.supplier",
                supplierWithNationTypes);

        List<String> lineitemColumns = List.of("l_partkey", "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        List<Type> withOrdersTypes = List.of(lineitemTypes.get(0), lineitemTypes.get(2), orderDateType, lineitemTypes.get(3), lineitemTypes.get(4));
        List<Type> withSupplierTypes = List.of(nationNameType, orderDateType, lineitemTypes.get(0), lineitemTypes.get(3), lineitemTypes.get(4));
        List<Type> joinedTypes = List.of(nationNameType, orderDateType, lineitemTypes.get(3), lineitemTypes.get(4));
        RowExpression volume = discountedPrice(2, 3);
        // [o_year, brazilVolume, volume]
        List<Type> groupedTypes = List.of(BIGINT, DOUBLE, DOUBLE);
        List<Type> outputTypes = query08OutputTypes();
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q08.scan.lineitem"),
                List.of(
                        namedHashJoinStep("q08.join.orders", new HashJoinSpec(8_4, lineitemTypes, List.of(1), ordersInAmerica, ordersInAmericaTypes, List.of(0))),
                        namedFactoryStep("q08.project.after_orders", filterAndProjectFactory(
                                8_5,
                                Optional.empty(),
                                List.of(
                                        field(0, lineitemTypes.get(0)),
                                        field(2, lineitemTypes.get(2)),
                                        field(6, orderDateType),
                                        field(3, lineitemTypes.get(3)),
                                        field(4, lineitemTypes.get(4))),
                                withOrdersTypes)),
                        namedHashJoinStep("q08.join.supplier", new HashJoinSpec(8_6, withOrdersTypes, List.of(1), supplierWithNation, supplierWithNationTypes, List.of(0))),
                        namedFactoryStep("q08.project.after_supplier", filterAndProjectFactory(
                                8_7,
                                Optional.empty(),
                                List.of(
                                        field(6, nationNameType),
                                        field(2, orderDateType),
                                        field(0, lineitemTypes.get(0)),
                                        field(3, lineitemTypes.get(3)),
                                        field(4, lineitemTypes.get(4))),
                                withSupplierTypes)),
                        namedHashJoinStep("q08.join.part", new HashJoinSpec(8_8, withSupplierTypes, List.of(2), part, partTypes, List.of(0))),
                        namedFactoryStep("q08.project.after_part", filterAndProjectFactory(
                                8_17,
                                Optional.empty(),
                                List.of(
                                        field(0, nationNameType),
                                        field(1, orderDateType),
                                        field(3, lineitemTypes.get(3)),
                                        field(4, lineitemTypes.get(4))),
                                joinedTypes)),
                        namedFactoryStep("q08.project.volume", filterAndProjectFactory(
                                8_9,
                                Optional.empty(),
                                List.of(
                                        year(field(1, orderDateType), orderDateType),
                                        ifExpression(equalUtf8(0, "BRAZIL", nationNameType), volume, constant(0.0, DOUBLE), DOUBLE),
                                        volume),
                                groupedTypes)),
                        namedFactoryStep("q08.group", hashAggregationFactory(
                                8_10,
                                List.of(BIGINT),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        namedFactoryStep("q08.order_by", orderByFactory(8_11, groupedTypes, List.of(0), List.of(ASC_NULLS_LAST))),
                        // mkt_share = brazil volume / total volume, divided after the sort
                        namedFactoryStep("q08.project.final", filterAndProjectFactory(
                                8_12,
                                Optional.empty(),
                                List.of(field(0, BIGINT), divide(field(1, DOUBLE), field(2, DOUBLE), DOUBLE)),
                                outputTypes))),
                "q08.sink.final",
                outputTypes);
    }

    private List<Type> query08OutputTypes()
    {
        return List.of(BIGINT, DOUBLE);
    }

    private PipelinePlan query09Plan(TpchParquetTables tables)
    {
        List<String> partColumns = List.of("p_partkey", "p_name");
        List<Type> partScanTypes = tableColumnTypes(tables, "part", partColumns);
        List<Type> partTypes = List.of(partScanTypes.get(0));
        PipelinePlan part = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.of(like(field(1, partScanTypes.get(1)), "%green%")),
                List.of(field(0, partScanTypes.get(0))),
                partTypes,
                "q09.filter.part",
                "q09.sink.part");

        List<String> supplierColumns = List.of("s_suppkey", "s_nationkey");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        PipelinePlan supplier = relationPlan(
                tables,
                "supplier",
                supplierColumns,
                Optional.empty(),
                identityProjections(supplierTypes),
                supplierTypes,
                "q09.scan.supplier",
                "q09.sink.supplier");

        List<String> partsuppColumns = List.of("ps_partkey", "ps_suppkey", "ps_supplycost");
        List<Type> partsuppTypes = tableColumnTypes(tables, "partsupp", partsuppColumns);
        PipelinePlan partsupp = relationPlan(
                tables,
                "partsupp",
                partsuppColumns,
                Optional.empty(),
                identityProjections(partsuppTypes),
                partsuppTypes,
                "q09.scan.partsupp",
                "q09.sink.partsupp");

        List<String> ordersColumns = List.of("o_orderkey", "o_orderdate");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        Type orderDateType = ordersTypes.get(1);
        PipelinePlan orders = relationPlan(
                tables,
                "orders",
                ordersColumns,
                Optional.empty(),
                identityProjections(ordersTypes),
                ordersTypes,
                "q09.scan.orders",
                "q09.sink.orders");

        List<String> nationColumns = List.of("n_nationkey", "n_name");
        List<Type> nationTypes = tableColumnTypes(tables, "nation", nationColumns);
        Type nationNameType = nationTypes.get(1);
        PipelinePlan nation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.empty(),
                identityProjections(nationTypes),
                nationTypes,
                "q09.scan.nation",
                "q09.sink.nation");

        List<String> lineitemColumns = List.of("l_partkey", "l_orderkey", "l_suppkey", "l_quantity", "l_extendedprice", "l_discount");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        // Match Trino's SQL physical shape. First retain the six lineitem values plus s_nationkey
        // consumed downstream, then use that stream as the probe for partsupp and orders.
        List<Type> lineitemWithNationTypes = concatTypes(lineitemTypes, List.of(supplierTypes.get(1)));
        PipelinePlan filteredLineitem = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q09.scan.lineitem"),
                List.of(
                        namedHashJoinStep("q09.join.part", new HashJoinSpec(9_0, lineitemTypes, List.of(0), part, partTypes, List.of(0))),
                        namedFactoryStep("q09.project.after_part", filterAndProjectFactory(
                                9_1,
                                Optional.empty(),
                                identityProjections(lineitemTypes),
                                lineitemTypes)),
                        namedHashJoinStep("q09.join.supplier", new HashJoinSpec(9_2, lineitemTypes, List.of(2), supplier, supplierTypes, List.of(0))),
                        namedFactoryStep("q09.project.after_supplier", filterAndProjectFactory(
                                9_3,
                                Optional.empty(),
                                List.of(
                                        field(0, lineitemTypes.get(0)),
                                        field(1, lineitemTypes.get(1)),
                                        field(2, lineitemTypes.get(2)),
                                        field(3, lineitemTypes.get(3)),
                                        field(4, lineitemTypes.get(4)),
                                        field(5, lineitemTypes.get(5)),
                                        field(7, supplierTypes.get(1))),
                                lineitemWithNationTypes))),
                "q09.sink.filtered_lineitem",
                lineitemWithNationTypes);

        // The filtered-lineitem stream probes the 8M-row partsupp build. The projected
        // layout is [extendedprice, discount, quantity, orderkey, nationkey, supplycost].
        List<Type> withPartsuppTypes = List.of(
                lineitemTypes.get(4),
                lineitemTypes.get(5),
                lineitemTypes.get(3),
                lineitemTypes.get(1),
                supplierTypes.get(1),
                partsuppTypes.get(2));
        PipelinePlan partsuppJoined = appendPlan(
                filteredLineitem,
                List.of(
                        namedHashJoinStep("q09.join.partsupp", new HashJoinSpec(
                                9_4,
                                lineitemWithNationTypes,
                                List.of(2, 0),
                                partsupp,
                                partsuppTypes,
                                List.of(1, 0))),
                        namedFactoryStep("q09.project.after_partsupp", filterAndProjectFactory(
                                9_5,
                                Optional.empty(),
                                List.of(
                                        field(4, lineitemTypes.get(4)),
                                        field(5, lineitemTypes.get(5)),
                                        field(3, lineitemTypes.get(3)),
                                        field(1, lineitemTypes.get(1)),
                                        field(6, supplierTypes.get(1)),
                                        field(9, partsuppTypes.get(2))),
                                withPartsuppTypes))),
                "q09.sink.partsupp_joined",
                withPartsuppTypes);

        // That intermediate probes Trino's 15M-row orders build and retains the SQL plan's six live values.
        List<Type> withOrdersTypes = List.of(
                lineitemTypes.get(4),
                lineitemTypes.get(5),
                lineitemTypes.get(3),
                supplierTypes.get(1),
                partsuppTypes.get(2),
                orderDateType);
        // [nation, o_year, amount]
        List<Type> groupedTypes = List.of(nationNameType, BIGINT, DOUBLE);
        List<Type> outputTypes = query09OutputTypes(tables);
        return appendPlan(
                partsuppJoined,
                List.of(
                        namedHashJoinStep("q09.join.orders", new HashJoinSpec(9_6, withPartsuppTypes, List.of(3), orders, ordersTypes, List.of(0))),
                        namedFactoryStep("q09.project.after_orders", filterAndProjectFactory(
                                9_7,
                                Optional.empty(),
                                List.of(
                                        field(0, lineitemTypes.get(4)),
                                        field(1, lineitemTypes.get(5)),
                                        field(2, lineitemTypes.get(3)),
                                        field(4, supplierTypes.get(1)),
                                        field(5, partsuppTypes.get(2)),
                                        field(7, orderDateType)),
                                withOrdersTypes)),
                        namedHashJoinStep("q09.join.nation", new HashJoinSpec(9_8, withOrdersTypes, List.of(3), nation, nationTypes, List.of(0))),
                        // amount = disc_price - ps_supplycost * l_quantity
                        namedFactoryStep("q09.project.amount", filterAndProjectFactory(
                                9_9,
                                Optional.empty(),
                                List.of(
                                        field(7, nationNameType),
                                        year(field(5, orderDateType), orderDateType),
                                        subtract(discountedPrice(0, 1), multiply(field(4, DOUBLE), field(2, DOUBLE), DOUBLE), DOUBLE)),
                                groupedTypes)),
                        namedFactoryStep("q09.group", hashAggregationFactory(
                                9_10,
                                List.of(nationNameType, BIGINT),
                                List.of(0, 1),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        namedFactoryStep("q09.order_by", orderByFactory(
                                9_11,
                                outputTypes,
                                List.of(0, 1),
                                List.of(ASC_NULLS_LAST, DESC_NULLS_LAST)))),
                "q09.sink.final",
                outputTypes);
    }

    private List<Type> query09OutputTypes(TpchParquetTables tables)
    {
        Type nationNameType = tableColumnTypes(tables, "nation", List.of("n_name")).getFirst();
        return List.of(nationNameType, BIGINT, DOUBLE);
    }

    private PipelinePlan query13Plan(TpchParquetTables tables)
    {
        List<String> ordersColumns = List.of("o_orderkey", "o_custkey", "o_comment");
        List<Type> ordersScanTypes = tableColumnTypes(tables, "orders", ordersColumns);
        List<Type> ordersTypes = List.of(ordersScanTypes.get(0), ordersScanTypes.get(1));
        PipelinePlan orders = relationPlan(
                tables,
                "orders",
                ordersColumns,
                Optional.of(not(like(field(2, ordersScanTypes.get(2)), "%special%requests%"))),
                List.of(field(0, ordersScanTypes.get(0)), field(1, ordersScanTypes.get(1))),
                ordersTypes,
                "q13.filter.orders",
                "q13.sink.orders");

        List<String> customerColumns = List.of("c_custkey");
        List<Type> customerTypes = tableColumnTypes(tables, "customer", customerColumns);
        List<Type> outputTypes = query13OutputTypes();
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("customer"), customerColumns, "q13.scan.customer"),
                List.of(
                        // [c_custkey, o_orderkey, o_custkey] with nulls on the order side for unmatched customers
                        namedHashJoinStep("q13.join.orders", new HashJoinSpec(13_0, customerTypes, List.of(0), orders, ordersTypes, List.of(1), JoinType.LEFT)),
                        // [c_custkey, c_count]; count(o_orderkey) counts only matches
                        namedFactoryStep("q13.group.per_customer", hashAggregationFactory(
                                13_1,
                                List.of(customerTypes.get(0)),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(BIGINT)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        namedFactoryStep("q13.project.distribution", filterAndProjectFactory(
                                13_2,
                                Optional.empty(),
                                List.of(field(0, customerTypes.get(0)), field(1, BIGINT)),
                                List.of(customerTypes.get(0), BIGINT))),
                        // [c_count, custdist]
                        namedFactoryStep("q13.group.distribution", hashAggregationFactory(
                                13_3,
                                List.of(BIGINT),
                                List.of(1),
                                COUNT_ALL.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        namedFactoryStep("q13.order_by", orderByFactory(
                                13_4,
                                outputTypes,
                                List.of(1, 0),
                                List.of(DESC_NULLS_LAST, DESC_NULLS_LAST)))),
                "q13.sink.final",
                outputTypes);
    }

    private List<Type> query13OutputTypes()
    {
        return List.of(BIGINT, BIGINT);
    }

    private PipelinePlan query16Plan(TpchParquetTables tables)
    {
        List<String> partColumns = List.of("p_partkey", "p_brand", "p_type", "p_size");
        List<Type> partTypes = tableColumnTypes(tables, "part", partColumns);
        Type brandType = partTypes.get(1);
        Type typeType = partTypes.get(2);
        Type sizeType = partTypes.get(3);
        PipelinePlan part = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.of(and(
                        not(equalUtf8(1, "Brand#45", brandType)),
                        not(like(field(2, typeType), "MEDIUM POLISHED%")),
                        or(
                                equal(field(3, sizeType), constant(3L, sizeType), sizeType),
                                equal(field(3, sizeType), constant(9L, sizeType), sizeType),
                                equal(field(3, sizeType), constant(14L, sizeType), sizeType),
                                equal(field(3, sizeType), constant(19L, sizeType), sizeType),
                                equal(field(3, sizeType), constant(23L, sizeType), sizeType),
                                equal(field(3, sizeType), constant(36L, sizeType), sizeType),
                                equal(field(3, sizeType), constant(45L, sizeType), sizeType),
                                equal(field(3, sizeType), constant(49L, sizeType), sizeType)))),
                identityProjections(partTypes),
                partTypes,
                "q16.filter.part",
                "q16.sink.part");

        List<String> supplierColumns = List.of("s_suppkey", "s_comment");
        List<Type> supplierScanTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        PipelinePlan complainingSuppliers = relationPlan(
                tables,
                "supplier",
                supplierColumns,
                Optional.of(like(field(1, supplierScanTypes.get(1)), "%Customer%Complaints%")),
                List.of(field(0, supplierScanTypes.get(0))),
                List.of(supplierScanTypes.get(0)),
                "q16.filter.supplier",
                "q16.sink.supplier");

        List<String> partsuppColumns = List.of("ps_partkey", "ps_suppkey");
        List<Type> partsuppTypes = tableColumnTypes(tables, "partsupp", partsuppColumns);
        Type suppkeyType = partsuppTypes.get(1);
        // [ps_partkey, ps_suppkey, p_partkey, p_brand, p_type, p_size]
        List<Type> joinedTypes = concatTypes(partsuppTypes, partTypes);
        List<Type> outputTypes = query16OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("partsupp"), partsuppColumns, "q16.scan.partsupp"),
                List.of(
                        namedHashJoinStep("q16.join.part", new HashJoinSpec(16_0, partsuppTypes, List.of(0), part, partTypes, List.of(0))),
                        // anti join: keep rows whose suppkey is NOT in the complaining-supplier set
                        namedSemiJoinStep("q16.semi.supplier", new SemiJoinSpec(16_1, joinedTypes, 1, complainingSuppliers, supplierScanTypes.get(0), 0)),
                        namedFactoryStep("q16.filter.no_complaints", filterAndProjectFactory(
                                16_2,
                                Optional.of(not(field(joinedTypes.size(), BOOLEAN))),
                                identityProjections(joinedTypes),
                                joinedTypes)),
                        // distinct (brand, type, size, suppkey): grouped aggregation with no aggregates
                        namedFactoryStep("q16.group.distinct", hashAggregationFactory(
                                16_3,
                                List.of(brandType, typeType, sizeType, suppkeyType),
                                List.of(3, 4, 5, 1))),
                        // [p_brand, p_type, p_size, supplier_cnt]
                        namedFactoryStep("q16.group.count", hashAggregationFactory(
                                16_4,
                                List.of(brandType, typeType, sizeType),
                                List.of(0, 1, 2),
                                COUNT_ALL.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        namedFactoryStep("q16.order_by", orderByFactory(
                                16_5,
                                outputTypes,
                                List.of(3, 0, 1, 2),
                                List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                "q16.sink.final",
                outputTypes);
    }

    private List<Type> query16OutputTypes(TpchParquetTables tables)
    {
        List<Type> partTypes = tableColumnTypes(tables, "part", List.of("p_brand", "p_type", "p_size"));
        return List.of(partTypes.get(0), partTypes.get(1), partTypes.get(2), BIGINT);
    }

    private PipelinePlan query18Plan(TpchParquetTables tables)
    {
        List<String> lineitemColumns = List.of("l_orderkey", "l_quantity");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        // Retain quantity: Velox reuses this same aggregation as the outer sum instead of rescanning lineitem.
        List<Type> bigOrderTypes = List.of(lineitemTypes.get(0), DOUBLE);
        PipelinePlan bigOrders = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q18.scan.lineitem_quantity"),
                List.of(
                        namedFactoryStep("q18.group.order_quantity", hashAggregationFactory(
                                18_0,
                                List.of(lineitemTypes.get(0)),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        namedFactoryStep("q18.filter.big_orders", filterAndProjectFactory(
                                18_1,
                                Optional.of(greaterThan(field(1, DOUBLE), constant(300.0, DOUBLE), DOUBLE)),
                                List.of(field(0, lineitemTypes.get(0)), field(1, DOUBLE)),
                                bigOrderTypes))),
                "q18.sink.big_orders",
                bigOrderTypes);

        List<String> customerColumns = List.of("c_custkey", "c_name");
        List<Type> customerTypes = tableColumnTypes(tables, "customer", customerColumns);
        PipelinePlan customer = relationPlan(
                tables,
                "customer",
                customerColumns,
                Optional.empty(),
                identityProjections(customerTypes),
                customerTypes,
                "q18.scan.customer",
                "q18.sink.customer");

        List<String> ordersColumns = List.of("o_orderkey", "o_custkey", "o_orderdate", "o_totalprice");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        List<Type> ordersWithKeyTypes = concatTypes(ordersTypes, bigOrderTypes);
        // [o_orderkey, o_custkey, o_orderdate, o_totalprice, bigOrderKey, quantity, c_custkey, c_name]
        List<Type> ordersWithCustomerTypes = concatTypes(ordersWithKeyTypes, customerTypes);
        List<Type> outputTypes = query18OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("orders"), ordersColumns, "q18.scan.orders"),
                List.of(
                        namedHashJoinStep("q18.join.big_orders", new HashJoinSpec(18_2, ordersTypes, List.of(0), bigOrders, bigOrderTypes, List.of(0))),
                        namedHashJoinStep("q18.join.customer", new HashJoinSpec(18_3, ordersWithKeyTypes, List.of(1), customer, customerTypes, List.of(0))),
                        namedFactoryStep("q18.project.output", filterAndProjectFactory(
                                18_4,
                                Optional.empty(),
                                List.of(field(7, customerTypes.get(1)), field(1, ordersTypes.get(1)), field(0, ordersTypes.get(0)),
                                        field(2, ordersTypes.get(2)), field(3, ordersTypes.get(3)), field(5, DOUBLE)),
                                outputTypes)),
                        namedFactoryStep("q18.topn", topNFactory(
                                18_5,
                                outputTypes,
                                100,
                                List.of(4, 3),
                                List.of(DESC_NULLS_LAST, ASC_NULLS_LAST)))),
                "q18.sink.final",
                outputTypes);
    }

    private List<Type> query18OutputTypes(TpchParquetTables tables)
    {
        Type customerNameType = tableColumnTypes(tables, "customer", List.of("c_name")).getFirst();
        Type orderDateType = tableColumnTypes(tables, "orders", List.of("o_orderdate")).getFirst();
        return List.of(customerNameType, BIGINT, BIGINT, orderDateType, DOUBLE, DOUBLE);
    }

    private PipelinePlan query02Plan(TpchParquetTables tables)
    {
        // minimum EUROPE supplycost per partkey: the region -> nation -> supplier build chain keys partsupp
        List<String> regionColumns = List.of("r_regionkey", "r_name");
        List<Type> regionScanTypes = tableColumnTypes(tables, "region", regionColumns);
        Type regionKeyType = regionScanTypes.get(0);
        PipelinePlan minimumCostRegion = relationPlan(
                tables,
                "region",
                regionColumns,
                Optional.of(equalUtf8(1, "EUROPE", regionScanTypes.get(1))),
                List.of(field(0, regionKeyType)),
                List.of(regionKeyType),
                "q02.min.filter.region",
                "q02.min.sink.region");

        List<String> nationRegionColumns = List.of("n_nationkey", "n_regionkey");
        List<Type> nationRegionTypes = tableColumnTypes(tables, "nation", nationRegionColumns);
        // [n_nationkey, n_regionkey, r_regionkey] -> [n_nationkey]
        List<Type> minimumCostNationTypes = List.of(nationRegionTypes.get(0));
        PipelinePlan minimumCostNation = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("nation"), nationRegionColumns, "q02.min.scan.nation"),
                List.of(
                        namedHashJoinStep("q02.min.join.region", new HashJoinSpec(2_0, nationRegionTypes, List.of(1), minimumCostRegion, List.of(regionKeyType), List.of(0))),
                        namedFactoryStep("q02.min.project.nation", filterAndProjectFactory(
                                2_1,
                                Optional.empty(),
                                List.of(field(0, nationRegionTypes.get(0))),
                                minimumCostNationTypes))),
                "q02.min.sink.nation",
                minimumCostNationTypes);

        List<String> supplierKeyColumns = List.of("s_suppkey", "s_nationkey");
        List<Type> supplierKeyTypes = tableColumnTypes(tables, "supplier", supplierKeyColumns);
        // [s_suppkey, s_nationkey, n_nationkey] -> [s_suppkey]
        List<Type> minimumCostSupplierTypes = List.of(supplierKeyTypes.get(0));
        PipelinePlan minimumCostSupplier = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("supplier"), supplierKeyColumns, "q02.min.scan.supplier"),
                List.of(
                        namedHashJoinStep("q02.min.join.nation", new HashJoinSpec(2_2, supplierKeyTypes, List.of(1), minimumCostNation, minimumCostNationTypes, List.of(0))),
                        namedFactoryStep("q02.min.project.supplier", filterAndProjectFactory(
                                2_3,
                                Optional.empty(),
                                List.of(field(0, supplierKeyTypes.get(0))),
                                minimumCostSupplierTypes))),
                "q02.min.sink.supplier",
                minimumCostSupplierTypes);

        List<String> partsuppColumns = List.of("ps_partkey", "ps_suppkey", "ps_supplycost");
        List<Type> partsuppTypes = tableColumnTypes(tables, "partsupp", partsuppColumns);
        // [ps_partkey, min_cost]
        List<Type> minimumCostTypes = List.of(partsuppTypes.get(0), DOUBLE);
        PipelinePlan minimumCosts = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("partsupp"), partsuppColumns, "q02.min.scan.partsupp"),
                List.of(
                        // [ps_partkey, ps_suppkey, ps_supplycost, s_suppkey]
                        namedHashJoinStep("q02.min.join.supplier", new HashJoinSpec(2_4, partsuppTypes, List.of(1), minimumCostSupplier, minimumCostSupplierTypes, List.of(0))),
                        namedFactoryStep("q02.min.group", hashAggregationFactory(
                                2_5,
                                List.of(partsuppTypes.get(0)),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())))),
                "q02.min.sink",
                minimumCostTypes);

        List<String> partColumns = List.of("p_partkey", "p_mfgr", "p_size", "p_type");
        List<Type> partTypes = tableColumnTypes(tables, "part", partColumns);
        Type sizeType = partTypes.get(2);
        PipelinePlan part = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.of(and(
                        equal(field(2, sizeType), constant(15L, sizeType), sizeType),
                        like(field(3, partTypes.get(3)), "%BRASS"))),
                identityProjections(partTypes),
                partTypes,
                "q02.filter.part",
                "q02.sink.part");

        List<String> supplierColumns = List.of("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        PipelinePlan supplier = relationPlan(
                tables,
                "supplier",
                supplierColumns,
                Optional.empty(),
                identityProjections(supplierTypes),
                supplierTypes,
                "q02.scan.supplier",
                "q02.sink.supplier");

        List<String> nationColumns = List.of("n_nationkey", "n_name", "n_regionkey");
        List<Type> nationTypes = tableColumnTypes(tables, "nation", nationColumns);
        PipelinePlan nation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.empty(),
                identityProjections(nationTypes),
                nationTypes,
                "q02.scan.nation",
                "q02.sink.nation");

        PipelinePlan region = relationPlan(
                tables,
                "region",
                regionColumns,
                Optional.of(equalUtf8(1, "EUROPE", regionScanTypes.get(1))),
                List.of(field(0, regionKeyType)),
                List.of(regionKeyType),
                "q02.filter.region",
                "q02.sink.region");

        List<Type> withPartTypes = concatTypes(partsuppTypes, partTypes);
        List<Type> withSupplierTypes = concatTypes(withPartTypes, supplierTypes);
        List<Type> withNationTypes = concatTypes(withSupplierTypes, nationTypes);
        List<Type> withRegionTypes = concatTypes(withNationTypes, List.of(regionKeyType));
        List<Type> outputTypes = query02OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("partsupp"), partsuppColumns, "q02.scan.partsupp"),
                List.of(
                        // [ps_partkey, ps_suppkey, ps_supplycost, p_partkey, p_mfgr, p_size, p_type]
                        namedHashJoinStep("q02.join.part", new HashJoinSpec(2_6, partsuppTypes, List.of(0), part, partTypes, List.of(0))),
                        // + s x7 -> 7..13
                        namedHashJoinStep("q02.join.supplier", new HashJoinSpec(2_7, withPartTypes, List.of(1), supplier, supplierTypes, List.of(0))),
                        // + n x3 -> 14..16
                        namedHashJoinStep("q02.join.nation", new HashJoinSpec(2_8, withSupplierTypes, List.of(10), nation, nationTypes, List.of(0))),
                        // + r_regionkey -> 17
                        namedHashJoinStep("q02.join.region", new HashJoinSpec(2_9, withNationTypes, List.of(16), region, List.of(regionKeyType), List.of(0))),
                        // + [m_partkey, minCost] -> 18,19
                        namedHashJoinStep("q02.join.min_cost", new HashJoinSpec(2_10, withRegionTypes, List.of(0), minimumCosts, minimumCostTypes, List.of(0))),
                        // ps_supplycost = min; SQL order: s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
                        namedFactoryStep("q02.filter.min_cost", filterAndProjectFactory(
                                2_11,
                                Optional.of(equal(field(2, DOUBLE), field(19, DOUBLE), DOUBLE)),
                                List.of(
                                        field(12, supplierTypes.get(5)),
                                        field(8, supplierTypes.get(1)),
                                        field(15, nationTypes.get(1)),
                                        field(0, partsuppTypes.get(0)),
                                        field(4, partTypes.get(1)),
                                        field(9, supplierTypes.get(2)),
                                        field(11, supplierTypes.get(4)),
                                        field(13, supplierTypes.get(6))),
                                outputTypes)),
                        namedFactoryStep("q02.topn", topNFactory(
                                2_12,
                                outputTypes,
                                100,
                                List.of(0, 2, 1, 3),
                                List.of(DESC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST, ASC_NULLS_LAST)))),
                "q02.sink.final",
                outputTypes);
    }

    private List<Type> query02OutputTypes(TpchParquetTables tables)
    {
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", List.of("s_name", "s_address", "s_phone", "s_comment"));
        Type nationNameType = tableColumnTypes(tables, "nation", List.of("n_name")).getFirst();
        Type manufacturerType = tableColumnTypes(tables, "part", List.of("p_mfgr")).getFirst();
        return List.of(DOUBLE, supplierTypes.get(0), nationNameType, BIGINT, manufacturerType, supplierTypes.get(1), supplierTypes.get(2), supplierTypes.get(3));
    }

    private PipelinePlan query11Plan(TpchParquetTables tables)
    {
        Type partKeyType = tableColumnTypes(tables, "partsupp", List.of("ps_partkey")).getFirst();
        // [ps_partkey, value]
        List<Type> perPartTypes = List.of(partKeyType, DOUBLE);

        // the global value * 0.00001 threshold, a single-row scalar (the value subplan assembles again)
        PipelinePlan scaledTotal = appendPlan(
                query11GermanValuePlan(tables, false),
                List.of(
                        namedFactoryStep("q11.aggregate.total", hashAggregationFactory(
                                11_5,
                                List.of(),
                                List.of(),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        namedFactoryStep("q11.project.threshold", filterAndProjectFactory(
                                11_6,
                                Optional.empty(),
                                List.of(multiply(field(0, DOUBLE), constant(0.00001, DOUBLE), DOUBLE)),
                                List.of(DOUBLE)))),
                "q11.sink.threshold",
                List.of(DOUBLE));

        List<Type> outputTypes = query11OutputTypes(tables);
        return appendPlan(
                query11GermanValuePlan(tables, true),
                List.of(
                        // [ps_partkey, value, threshold]
                        namedNestedLoopJoinStep("q11.join.threshold", new NestedLoopJoinSpec(11_7, perPartTypes, scaledTotal, List.of(DOUBLE))),
                        namedFactoryStep("q11.filter.threshold", filterAndProjectFactory(
                                11_8,
                                Optional.of(greaterThan(field(1, DOUBLE), field(2, DOUBLE), DOUBLE)),
                                List.of(field(0, partKeyType), field(1, DOUBLE)),
                                outputTypes)),
                        namedFactoryStep("q11.order_by", orderByFactory(11_9, outputTypes, List.of(1), List.of(DESC_NULLS_LAST)))),
                "q11.sink.final",
                outputTypes);
    }

    private PipelinePlan query11GermanValuePlan(TpchParquetTables tables, boolean grouped)
    {
        List<String> nationColumns = List.of("n_nationkey", "n_name");
        List<Type> nationScanTypes = tableColumnTypes(tables, "nation", nationColumns);
        Type nationKeyType = nationScanTypes.get(0);
        PipelinePlan nation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.of(equalUtf8(1, "GERMANY", nationScanTypes.get(1))),
                List.of(field(0, nationKeyType)),
                List.of(nationKeyType),
                "q11.filter.nation",
                "q11.sink.nation");

        List<String> supplierColumns = List.of("s_suppkey", "s_nationkey");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        // [s_suppkey, s_nationkey, n_nationkey] -> [s_suppkey]
        List<Type> germanSupplierTypes = List.of(supplierTypes.get(0));
        PipelinePlan germanSuppliers = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("supplier"), supplierColumns, "q11.scan.supplier"),
                List.of(
                        namedHashJoinStep("q11.join.nation", new HashJoinSpec(11_0, supplierTypes, List.of(1), nation, List.of(nationKeyType), List.of(0))),
                        namedFactoryStep("q11.project.supplier", filterAndProjectFactory(
                                11_1,
                                Optional.empty(),
                                List.of(field(0, supplierTypes.get(0))),
                                germanSupplierTypes))),
                "q11.sink.supplier",
                germanSupplierTypes);

        List<String> partsuppColumns = List.of("ps_partkey", "ps_suppkey", "ps_supplycost", "ps_availqty");
        List<Type> partsuppTypes = tableColumnTypes(tables, "partsupp", partsuppColumns);
        Type availableQuantityType = partsuppTypes.get(3);
        // [ps_partkey, value = ps_supplycost * cast(ps_availqty as double)]
        List<Type> valueTypes = List.of(partsuppTypes.get(0), DOUBLE);
        List<PipelineStep> steps = new ArrayList<>();
        // [ps_partkey, ps_suppkey, ps_supplycost, ps_availqty, s_suppkey]
        steps.add(namedHashJoinStep("q11.join.supplier", new HashJoinSpec(11_2, partsuppTypes, List.of(1), germanSuppliers, germanSupplierTypes, List.of(0))));
        steps.add(namedFactoryStep("q11.project.value", filterAndProjectFactory(
                11_3,
                Optional.empty(),
                List.of(
                        field(0, partsuppTypes.get(0)),
                        multiply(field(2, DOUBLE), cast(field(3, availableQuantityType), availableQuantityType, DOUBLE), DOUBLE)),
                valueTypes)));
        if (grouped) {
            steps.add(namedFactoryStep("q11.group", hashAggregationFactory(
                    11_4,
                    List.of(partsuppTypes.get(0)),
                    List.of(0),
                    FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))));
        }
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("partsupp"), partsuppColumns, "q11.scan.partsupp"),
                List.copyOf(steps),
                "q11.sink.value",
                valueTypes);
    }

    private List<Type> query11OutputTypes(TpchParquetTables tables)
    {
        Type partKeyType = tableColumnTypes(tables, "partsupp", List.of("ps_partkey")).getFirst();
        return List.of(partKeyType, DOUBLE);
    }

    private PipelinePlan query15Plan(TpchParquetTables tables)
    {
        Type supplierNumberType = tableColumnTypes(tables, "lineitem", List.of("l_suppkey")).getFirst();
        // [supplier_no, total_revenue]
        List<Type> revenueTypes = List.of(supplierNumberType, DOUBLE);

        // max(total_revenue): the revenue view assembles again (the SQL references it twice)
        PipelinePlan maxRevenue = appendPlan(
                query15RevenuePlan(tables),
                List.of(namedFactoryStep("q15.aggregate.max", hashAggregationFactory(
                        15_2,
                        List.of(),
                        List.of(),
                        FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))),
                "q15.sink.max",
                List.of(DOUBLE));

        // [supplier_no, total_revenue, max_revenue], filtered to total = max
        List<Type> bestTypes = concatTypes(revenueTypes, List.of(DOUBLE));
        PipelinePlan best = appendPlan(
                query15RevenuePlan(tables),
                List.of(
                        namedNestedLoopJoinStep("q15.join.max", new NestedLoopJoinSpec(15_3, revenueTypes, maxRevenue, List.of(DOUBLE))),
                        namedFactoryStep("q15.filter.best", filterAndProjectFactory(
                                15_4,
                                Optional.of(equal(field(1, DOUBLE), field(2, DOUBLE), DOUBLE)),
                                identityProjections(bestTypes),
                                bestTypes))),
                "q15.sink.best",
                bestTypes);

        List<String> supplierColumns = List.of("s_suppkey", "s_name", "s_address", "s_phone");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        List<Type> outputTypes = query15OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("supplier"), supplierColumns, "q15.scan.supplier"),
                List.of(
                        // [s_suppkey, s_name, s_address, s_phone, supplier_no, total_revenue, max_revenue]
                        namedHashJoinStep("q15.join.best", new HashJoinSpec(15_5, supplierTypes, List.of(0), best, bestTypes, List.of(0))),
                        namedFactoryStep("q15.project.final", filterAndProjectFactory(
                                15_6,
                                Optional.empty(),
                                List.of(
                                        field(0, supplierTypes.get(0)),
                                        field(1, supplierTypes.get(1)),
                                        field(2, supplierTypes.get(2)),
                                        field(3, supplierTypes.get(3)),
                                        field(5, DOUBLE)),
                                outputTypes)),
                        namedFactoryStep("q15.order_by", orderByFactory(15_7, outputTypes, List.of(0), List.of(ASC_NULLS_LAST)))),
                "q15.sink.final",
                outputTypes);
    }

    /** The quarter's revenue per supplier (the Q15 CTE): 1996-Q1 lineitem, sum of disc price by suppkey. */
    private PipelinePlan query15RevenuePlan(TpchParquetTables tables)
    {
        List<String> lineitemColumns = List.of("l_suppkey", "l_extendedprice", "l_discount", "l_shipdate");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        Type shipDateType = lineitemTypes.get(3);
        Type supplierNumberType = lineitemTypes.get(0);
        // [supplier_no, total_revenue]
        List<Type> revenueTypes = List.of(supplierNumberType, DOUBLE);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q15.scan.lineitem"),
                List.of(
                        namedFactoryStep("q15.filter.lineitem", filterAndProjectFactory(
                                15_0,
                                Optional.of(and(
                                        greaterThanOrEqual(field(3, shipDateType), constant(LocalDate.of(1996, 1, 1).toEpochDay(), shipDateType), shipDateType),
                                        lessThan(field(3, shipDateType), constant(LocalDate.of(1996, 4, 1).toEpochDay(), shipDateType), shipDateType))),
                                List.of(field(0, supplierNumberType), discountedPrice(1, 2)),
                                revenueTypes)),
                        namedFactoryStep("q15.group.revenue", hashAggregationFactory(
                                15_1,
                                List.of(supplierNumberType),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))),
                "q15.sink.revenue",
                revenueTypes);
    }

    private List<Type> query15OutputTypes(TpchParquetTables tables)
    {
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", List.of("s_suppkey", "s_name", "s_address", "s_phone"));
        return List.of(supplierTypes.get(0), supplierTypes.get(1), supplierTypes.get(2), supplierTypes.get(3), DOUBLE);
    }

    private PipelinePlan query17Plan(TpchParquetTables tables)
    {
        List<String> partColumns = List.of("p_partkey", "p_brand", "p_container");
        List<Type> partScanTypes = tableColumnTypes(tables, "part", partColumns);
        List<Type> partTypes = List.of(partScanTypes.get(0));
        PipelinePlan part = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.of(and(
                        equalUtf8(1, "Brand#23", partScanTypes.get(1)),
                        equalUtf8(2, "MED BOX", partScanTypes.get(2)))),
                List.of(field(0, partScanTypes.get(0))),
                partTypes,
                "q17.filter.part",
                "q17.sink.part");

        // per-partkey threshold = 0.2 * avg(l_quantity) over ALL lineitems of the part
        List<String> quantityColumns = List.of("l_partkey", "l_quantity");
        List<Type> quantityTypes = tableColumnTypes(tables, "lineitem", quantityColumns);
        List<Type> thresholdTypes = List.of(quantityTypes.get(0), DOUBLE);
        PipelinePlan thresholds = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), quantityColumns, "q17.scan.lineitem_quantity"),
                List.of(
                        namedFactoryStep("q17.group.average_quantity", hashAggregationFactory(
                                17_0,
                                List.of(quantityTypes.get(0)),
                                List.of(0),
                                FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        namedFactoryStep("q17.project.threshold", filterAndProjectFactory(
                                17_1,
                                Optional.empty(),
                                List.of(field(0, quantityTypes.get(0)), multiply(constant(0.2, DOUBLE), field(1, DOUBLE), DOUBLE)),
                                thresholdTypes))),
                "q17.sink.thresholds",
                thresholdTypes);

        List<String> lineitemColumns = List.of("l_partkey", "l_quantity", "l_extendedprice");
        List<Type> lineitemTypes = tableColumnTypes(tables, "lineitem", lineitemColumns);
        List<Type> withPartTypes = concatTypes(lineitemTypes, partTypes);
        List<Type> outputTypes = query17OutputTypes();
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lineitemColumns, "q17.scan.lineitem"),
                List.of(
                        // [l_partkey, l_quantity, l_extendedprice, p_partkey]
                        namedHashJoinStep("q17.join.part", new HashJoinSpec(17_2, lineitemTypes, List.of(0), part, partTypes, List.of(0))),
                        // + [t_partkey, threshold] -> 4,5
                        namedHashJoinStep("q17.join.thresholds", new HashJoinSpec(17_3, withPartTypes, List.of(0), thresholds, thresholdTypes, List.of(0))),
                        namedFactoryStep("q17.filter.below_threshold", filterAndProjectFactory(
                                17_4,
                                Optional.of(lessThan(field(1, DOUBLE), field(5, DOUBLE), DOUBLE)),
                                List.of(field(2, DOUBLE)),
                                List.of(DOUBLE))),
                        namedFactoryStep("q17.aggregate.final", hashAggregationFactory(
                                17_5,
                                List.of(),
                                List.of(),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()))),
                        // avg_yearly = sum / 7.0
                        namedFactoryStep("q17.project.final", filterAndProjectFactory(
                                17_6,
                                Optional.empty(),
                                List.of(divide(field(0, DOUBLE), constant(7.0, DOUBLE), DOUBLE)),
                                outputTypes))),
                "q17.sink.final",
                outputTypes);
    }

    private List<Type> query17OutputTypes()
    {
        return List.of(DOUBLE);
    }

    private PipelinePlan query20Plan(TpchParquetTables tables)
    {
        List<String> partColumns = List.of("p_partkey", "p_name");
        List<Type> partScanTypes = tableColumnTypes(tables, "part", partColumns);
        List<Type> forestPartTypes = List.of(partScanTypes.get(0));
        PipelinePlan forestParts = relationPlan(
                tables,
                "part",
                partColumns,
                Optional.of(like(field(1, partScanTypes.get(1)), "forest%")),
                List.of(field(0, partScanTypes.get(0))),
                forestPartTypes,
                "q20.filter.part",
                "q20.sink.part");

        // half the 1994 shipped quantity per (partkey, suppkey)
        List<String> shippedColumns = List.of("l_partkey", "l_suppkey", "l_quantity", "l_shipdate");
        List<Type> shippedScanTypes = tableColumnTypes(tables, "lineitem", shippedColumns);
        Type shipDateType = shippedScanTypes.get(3);
        List<Type> thresholdTypes = List.of(shippedScanTypes.get(0), shippedScanTypes.get(1), DOUBLE);
        PipelinePlan thresholds = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), shippedColumns, "q20.scan.lineitem"),
                List.of(
                        namedFactoryStep("q20.filter.lineitem", filterAndProjectFactory(
                                20_0,
                                Optional.of(and(
                                        greaterThanOrEqual(field(3, shipDateType), constant(LocalDate.of(1994, 1, 1).toEpochDay(), shipDateType), shipDateType),
                                        lessThan(field(3, shipDateType), constant(LocalDate.of(1995, 1, 1).toEpochDay(), shipDateType), shipDateType))),
                                identityProjections(shippedScanTypes),
                                shippedScanTypes)),
                        namedFactoryStep("q20.group.shipped", hashAggregationFactory(
                                20_1,
                                List.of(shippedScanTypes.get(0), shippedScanTypes.get(1)),
                                List.of(0, 1),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()))),
                        namedFactoryStep("q20.project.threshold", filterAndProjectFactory(
                                20_2,
                                Optional.empty(),
                                List.of(
                                        field(0, shippedScanTypes.get(0)),
                                        field(1, shippedScanTypes.get(1)),
                                        multiply(constant(0.5, DOUBLE), field(2, DOUBLE), DOUBLE)),
                                thresholdTypes))),
                "q20.sink.thresholds",
                thresholdTypes);

        List<String> partsuppColumns = List.of("ps_partkey", "ps_suppkey", "ps_availqty");
        List<Type> partsuppTypes = tableColumnTypes(tables, "partsupp", partsuppColumns);
        Type availableQuantityType = partsuppTypes.get(2);
        List<Type> forestPartsuppTypes = concatTypes(partsuppTypes, forestPartTypes);
        List<Type> qualifiedSupplierTypes = List.of(partsuppTypes.get(1));
        PipelinePlan qualifiedSuppliers = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("partsupp"), partsuppColumns, "q20.scan.partsupp"),
                List.of(
                        // [ps_partkey, ps_suppkey, ps_availqty, p_partkey]
                        namedHashJoinStep("q20.join.part", new HashJoinSpec(20_3, partsuppTypes, List.of(0), forestParts, forestPartTypes, List.of(0))),
                        // + [t_partkey, t_suppkey, threshold] -> 4,5,6
                        namedHashJoinStep("q20.join.thresholds", new HashJoinSpec(20_4, forestPartsuppTypes, List.of(0, 1), thresholds, thresholdTypes, List.of(0, 1))),
                        // [ps_suppkey, cast(ps_availqty as double), threshold]
                        namedFactoryStep("q20.project.availqty", filterAndProjectFactory(
                                20_5,
                                Optional.empty(),
                                List.of(
                                        field(1, partsuppTypes.get(1)),
                                        cast(field(2, availableQuantityType), availableQuantityType, DOUBLE),
                                        field(6, DOUBLE)),
                                List.of(partsuppTypes.get(1), DOUBLE, DOUBLE))),
                        // [ps_suppkey] with availqty > threshold
                        namedFactoryStep("q20.filter.qualified", filterAndProjectFactory(
                                20_6,
                                Optional.of(greaterThan(field(1, DOUBLE), field(2, DOUBLE), DOUBLE)),
                                List.of(field(0, partsuppTypes.get(1))),
                                qualifiedSupplierTypes))),
                "q20.sink.qualified",
                qualifiedSupplierTypes);

        List<String> nationColumns = List.of("n_nationkey", "n_name");
        List<Type> nationScanTypes = tableColumnTypes(tables, "nation", nationColumns);
        Type nationKeyType = nationScanTypes.get(0);
        PipelinePlan canadaNation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.of(equalUtf8(1, "CANADA", nationScanTypes.get(1))),
                List.of(field(0, nationKeyType)),
                List.of(nationKeyType),
                "q20.filter.nation",
                "q20.sink.nation");

        List<String> supplierColumns = List.of("s_suppkey", "s_name", "s_address", "s_nationkey");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        List<Type> canadaSupplierTypes = concatTypes(supplierTypes, List.of(nationKeyType));
        List<Type> outputTypes = query20OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("supplier"), supplierColumns, "q20.scan.supplier"),
                List.of(
                        // [s_suppkey, s_name, s_address, s_nationkey, n_nationkey]
                        namedHashJoinStep("q20.join.nation", new HashJoinSpec(20_7, supplierTypes, List.of(3), canadaNation, List.of(nationKeyType), List.of(0))),
                        namedSemiJoinStep("q20.semi.qualified", new SemiJoinSpec(20_8, canadaSupplierTypes, 0, qualifiedSuppliers, partsuppTypes.get(1), 0)),
                        namedFactoryStep("q20.filter.matched", filterAndProjectFactory(
                                20_9,
                                Optional.of(field(canadaSupplierTypes.size(), BOOLEAN)),
                                List.of(field(1, supplierTypes.get(1)), field(2, supplierTypes.get(2))),
                                outputTypes)),
                        namedFactoryStep("q20.order_by", orderByFactory(20_10, outputTypes, List.of(0), List.of(ASC_NULLS_LAST)))),
                "q20.sink.final",
                outputTypes);
    }

    private List<Type> query20OutputTypes(TpchParquetTables tables)
    {
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", List.of("s_name", "s_address"));
        return List.of(supplierTypes.get(0), supplierTypes.get(1));
    }

    private PipelinePlan query21Plan(TpchParquetTables tables)
    {
        // EXISTS(other supplier) is equivalent to a non-singleton supplier range for the order.
        List<String> pairColumns = List.of("l_orderkey", "l_suppkey");
        List<Type> pairTypes = tableColumnTypes(tables, "lineitem", pairColumns);
        List<Type> rangeTypes = List.of(pairTypes.get(0), BIGINT, BIGINT);
        PipelinePlan supplierRange = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), pairColumns, "q21.scan.lineitem_suppliers"),
                List.of(namedFactoryStep("q21.group.supplier_range", hashAggregationFactory(
                        21_0,
                        List.of(pairTypes.get(0)),
                        List.of(0),
                        BIGINT_MIN.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                        BIGINT_MAX.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))),
                "q21.sink.supplier_range",
                rangeTypes);

        // Since l1 is late, NOT EXISTS(other late supplier) is a singleton late-supplier range.
        List<String> lateColumns = List.of("l_orderkey", "l_suppkey", "l_commitdate", "l_receiptdate");
        List<Type> lateScanTypes = tableColumnTypes(tables, "lineitem", lateColumns);
        Type commitDateType = lateScanTypes.get(2);
        RowExpression lateFilter = lessThan(field(2, commitDateType), field(3, lateScanTypes.get(3)), commitDateType);
        PipelinePlan lateSupplierRange = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lateColumns, "q21.scan.lineitem_late"),
                List.of(
                        namedFactoryStep("q21.filter.late", filterAndProjectFactory(
                                21_2,
                                Optional.of(lateFilter),
                                identityProjections(lateScanTypes),
                                lateScanTypes)),
                        namedFactoryStep("q21.group.late_supplier_range", hashAggregationFactory(
                                21_3,
                                List.of(pairTypes.get(0)),
                                List.of(0),
                                BIGINT_MIN.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                BIGINT_MAX.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))),
                "q21.sink.late_supplier_range",
                rangeTypes);

        List<String> ordersColumns = List.of("o_orderkey", "o_orderstatus");
        List<Type> ordersScanTypes = tableColumnTypes(tables, "orders", ordersColumns);
        List<Type> ordersTypes = List.of(ordersScanTypes.get(0));
        PipelinePlan orders = relationPlan(
                tables,
                "orders",
                ordersColumns,
                Optional.of(equalUtf8(1, "F", ordersScanTypes.get(1))),
                List.of(field(0, ordersScanTypes.get(0))),
                ordersTypes,
                "q21.filter.orders",
                "q21.sink.orders");

        List<String> nationColumns = List.of("n_nationkey", "n_name");
        List<Type> nationScanTypes = tableColumnTypes(tables, "nation", nationColumns);
        Type nationKeyType = nationScanTypes.get(0);
        PipelinePlan saudiNation = relationPlan(
                tables,
                "nation",
                nationColumns,
                Optional.of(equalUtf8(1, "SAUDI ARABIA", nationScanTypes.get(1))),
                List.of(field(0, nationKeyType)),
                List.of(nationKeyType),
                "q21.filter.nation",
                "q21.sink.nation");

        List<String> supplierColumns = List.of("s_suppkey", "s_name", "s_nationkey");
        List<Type> supplierTypes = tableColumnTypes(tables, "supplier", supplierColumns);
        Type supplierNameType = supplierTypes.get(1);
        // [s_suppkey, s_name, s_nationkey, n_nationkey]
        List<Type> saudiSupplierTypes = concatTypes(supplierTypes, List.of(nationKeyType));
        PipelinePlan saudiSuppliers = new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("supplier"), supplierColumns, "q21.scan.supplier"),
                List.of(namedHashJoinStep("q21.join.nation", new HashJoinSpec(21_5, supplierTypes, List.of(2), saudiNation, List.of(nationKeyType), List.of(0)))),
                "q21.sink.supplier",
                saudiSupplierTypes);

        List<Type> withOrdersTypes = concatTypes(lateScanTypes, ordersTypes);
        List<Type> withSuppliersTypes = concatTypes(withOrdersTypes, saudiSupplierTypes);
        List<Type> withSupplierRangeTypes = concatTypes(withSuppliersTypes, rangeTypes);
        List<Type> outputTypes = query21OutputTypes(tables);
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("lineitem"), lateColumns, "q21.scan.lineitem"),
                List.of(
                        namedFactoryStep("q21.filter.lineitem", filterAndProjectFactory(
                                21_6,
                                Optional.of(lateFilter),
                                identityProjections(lateScanTypes),
                                lateScanTypes)),
                        // [l1 x4, o_orderkey]
                        namedHashJoinStep("q21.join.orders", new HashJoinSpec(21_7, lateScanTypes, List.of(0), orders, ordersTypes, List.of(0))),
                        // + [s_suppkey, s_name, s_nationkey, n_nationkey] -> 5..8
                        namedHashJoinStep("q21.join.supplier", new HashJoinSpec(21_8, withOrdersTypes, List.of(1), saudiSuppliers, saudiSupplierTypes, List.of(0))),
                        // + [orderkey, minSupplier, maxSupplier] -> 9..11
                        namedHashJoinStep("q21.join.supplier_range", new HashJoinSpec(21_9, withSuppliersTypes, List.of(0), supplierRange, rangeTypes, List.of(0))),
                        // + [orderkey, minLateSupplier, maxLateSupplier] -> 12..14
                        namedHashJoinStep("q21.join.late_supplier_range", new HashJoinSpec(21_10, withSupplierRangeTypes, List.of(0), lateSupplierRange, rangeTypes, List.of(0))),
                        namedFactoryStep("q21.filter.qualified", filterAndProjectFactory(
                                21_11,
                                Optional.of(and(
                                        lessThan(field(10, BIGINT), field(11, BIGINT), BIGINT),
                                        equal(field(13, BIGINT), field(14, BIGINT), BIGINT))),
                                List.of(field(6, supplierNameType)),
                                List.of(supplierNameType))),
                        namedFactoryStep("q21.group", hashAggregationFactory(
                                21_12,
                                List.of(supplierNameType),
                                List.of(0),
                                COUNT_ALL.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                        namedFactoryStep("q21.topn", topNFactory(
                                21_13,
                                outputTypes,
                                100,
                                List.of(1, 0),
                                List.of(DESC_NULLS_LAST, ASC_NULLS_LAST)))),
                "q21.sink.final",
                outputTypes);
    }

    private List<Type> query21OutputTypes(TpchParquetTables tables)
    {
        Type supplierNameType = tableColumnTypes(tables, "supplier", List.of("s_name")).getFirst();
        return List.of(supplierNameType, BIGINT);
    }

    private PipelinePlan query22Plan(TpchParquetTables tables)
    {
        List<String> codes = List.of("13", "31", "23", "29", "30", "18", "17");
        Type codeType = tableColumnTypes(tables, "customer", List.of("c_phone")).getFirst();
        Type customerKeyType = tableColumnTypes(tables, "customer", List.of("c_custkey")).getFirst();
        // [cntrycode, c_acctbal, c_custkey]
        List<Type> codedTypes = List.of(codeType, DOUBLE, customerKeyType);

        // the positive-balance average over the seven codes (the coded-customer subplan assembles again)
        PipelinePlan average = appendPlan(
                query22CodedCustomersPlan(tables, codes),
                List.of(
                        namedFactoryStep("q22.filter.positive", filterAndProjectFactory(
                                22_2,
                                Optional.of(greaterThan(field(1, DOUBLE), constant(0.0, DOUBLE), DOUBLE)),
                                identityProjections(codedTypes),
                                codedTypes)),
                        namedFactoryStep("q22.aggregate.average", hashAggregationFactory(
                                22_3,
                                List.of(),
                                List.of(),
                                FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))),
                "q22.sink.average",
                List.of(DOUBLE));

        List<String> ordersColumns = List.of("o_custkey");
        List<Type> ordersTypes = tableColumnTypes(tables, "orders", ordersColumns);
        PipelinePlan orders = relationPlan(
                tables,
                "orders",
                ordersColumns,
                Optional.empty(),
                identityProjections(ordersTypes),
                ordersTypes,
                "q22.scan.orders",
                "q22.sink.orders");

        // + avg -> 3
        List<Type> withAverageTypes = concatTypes(codedTypes, List.of(DOUBLE));
        List<Type> outputTypes = query22OutputTypes(tables);
        return appendPlan(
                query22CodedCustomersPlan(tables, codes),
                List.of(
                        namedNestedLoopJoinStep("q22.join.average", new NestedLoopJoinSpec(22_4, codedTypes, average, List.of(DOUBLE))),
                        namedFactoryStep("q22.filter.above_average", filterAndProjectFactory(
                                22_5,
                                Optional.of(greaterThan(field(1, DOUBLE), field(3, DOUBLE), DOUBLE)),
                                identityProjections(withAverageTypes),
                                withAverageTypes)),
                        // anti join: keep customers with no orders
                        namedSemiJoinStep("q22.semi.orders", new SemiJoinSpec(22_6, withAverageTypes, 2, orders, ordersTypes.getFirst(), 0)),
                        namedFactoryStep("q22.filter.no_orders", filterAndProjectFactory(
                                22_7,
                                Optional.of(not(field(withAverageTypes.size(), BOOLEAN))),
                                identityProjections(withAverageTypes),
                                withAverageTypes)),
                        // [cntrycode, numcust, totacctbal]
                        namedFactoryStep("q22.group", hashAggregationFactory(
                                22_8,
                                List.of(codeType),
                                List.of(0),
                                COUNT_ALL.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE)).createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()))),
                        namedFactoryStep("q22.order_by", orderByFactory(22_9, outputTypes, List.of(0), List.of(ASC_NULLS_LAST)))),
                "q22.sink.final",
                outputTypes);
    }

    /** Customers projected to (substring(c_phone, 1, 2), c_acctbal, c_custkey), restricted to the code list. */
    private PipelinePlan query22CodedCustomersPlan(TpchParquetTables tables, List<String> codes)
    {
        List<String> customerColumns = List.of("c_phone", "c_acctbal", "c_custkey");
        List<Type> customerTypes = tableColumnTypes(tables, "customer", customerColumns);
        Type codeType = customerTypes.get(0);
        // [cntrycode, c_acctbal, c_custkey]
        List<Type> codedTypes = List.of(codeType, DOUBLE, customerTypes.get(2));
        RowExpression inCodes = or(
                equalUtf8(0, codes.get(0), codeType),
                equalUtf8(0, codes.get(1), codeType),
                codes.subList(2, codes.size()).stream()
                        .map(code -> equalUtf8(0, code, codeType))
                        .toArray(RowExpression[]::new));
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("customer"), customerColumns, "q22.scan.customer"),
                List.of(
                        namedFactoryStep("q22.project.code", filterAndProjectFactory(
                                22_0,
                                Optional.empty(),
                                List.of(
                                        substring(field(0, codeType), 1, 2, codeType),
                                        field(1, DOUBLE),
                                        field(2, customerTypes.get(2))),
                                codedTypes)),
                        namedFactoryStep("q22.filter.codes", filterAndProjectFactory(
                                22_1,
                                Optional.of(inCodes),
                                identityProjections(codedTypes),
                                codedTypes))),
                "q22.sink.coded",
                codedTypes);
    }

    private List<Type> query22OutputTypes(TpchParquetTables tables)
    {
        Type codeType = tableColumnTypes(tables, "customer", List.of("c_phone")).getFirst();
        return List.of(codeType, BIGINT, DOUBLE);
    }

    private MaterializedResult executePipelinePlan(PipelinePlan plan, List<Type> outputTypes)
    {
        List<Page> outputPages = executePipelinePlan(plan);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (Page page : outputPages) {
            result.page(page);
        }
        return result.build();
    }

    private List<Page> executePipelinePlan(PipelinePlan plan)
    {
        List<Page> outputPages = new ArrayList<>();
        TaskContext taskContext = taskContext();
        DriverContext driverContext = newDriverContext(taskContext);
        List<Operator> operators = createOperators(taskContext, driverContext, plan, outputPages::add);

        try (Driver driver = Driver.createDriver(driverContext, operators)) {
            processDriver(driver);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to execute Trino TPC-H parquet pipeline plan", exception);
        }

        return outputPages;
    }

    private TaskContext taskContext()
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TestingSession.testSessionBuilder().build())
                .setQueryMaxMemory(DataSize.of(16, GIGABYTE))
                .setMemoryPoolSize(DataSize.of(16, GIGABYTE))
                .build();
    }

    private void processDriver(Driver driver)
            throws Exception
    {
        while (!driver.isFinished()) {
            ListenableFuture<Void> blocked = driver.processUntilBlocked();
            if (!blocked.isDone()) {
                waitForBlocked(blocked);
            }
        }
    }

    private DriverContext newDriverContext(TaskContext taskContext)
    {
        return taskContext.addPipelineContext(nextDynamicPipelineId.getAndIncrement(), true, true, false).addDriverContext();
    }

    private List<Operator> createOperators(TaskContext taskContext, DriverContext driverContext, PipelinePlan plan, Consumer<Page> pageConsumer)
    {
        List<Operator> operators = new ArrayList<>();
        operators.add(createSourceOperator(driverContext, plan.source()));

        for (PipelineStep step : plan.steps()) {
            OperatorFactory factory = step.createOperatorFactory(taskContext, this);
            operators.add(profiled(step.profileName(), factory.createOperator(driverContext)));
            factory.noMoreOperators();
        }

        operators.add(profiled(plan.sinkName(), new PageConsumerOperator(
                driverContext.addOperatorContext(1000, new PlanNodeId("sink"), PageConsumerOperator.class.getSimpleName()),
                pageConsumer,
                Function.identity())));
        return operators;
    }

    private Operator createSourceOperator(DriverContext driverContext, PipelineSource source)
    {
        if (source instanceof FilesPipelineSource filesSource) {
            return profiled(
                    filesSource.profileName(),
                    new ParquetPageSourceOperator(
                            driverContext.addOperatorContext(0, new PlanNodeId("parquet-source-" + Math.abs(filesSource.profileName().hashCode())), ParquetPageSourceOperator.class.getSimpleName()),
                            filesSource.files(),
                            filesSource.columns()));
        }
        throw new IllegalArgumentException("Unsupported pipeline source: " + source);
    }

    private PipelinePlan relationPlan(TpchParquetTables tables, String tableName, List<String> columns, Optional<RowExpression> filter, List<RowExpression> projections, List<Type> outputTypes, String operatorName, String sinkName)
    {
        int operatorId = nextDynamicOperatorId.getAndIncrement();
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles(tableName), columns, operatorName + ".source"),
                List.of(namedFactoryStep(operatorName, filterAndProjectFactory(operatorId, filter, projections, outputTypes))),
                sinkName,
                outputTypes);
    }

    private List<Type> tableColumnTypes(TpchParquetTables tables, String tableName, List<String> columns)
    {
        return TrinoClickBenchPageReader.columnTypes(tables.tableFiles(tableName).getFirst(), columns);
    }

    private OperatorFactory hashAggregationFactory(int operatorId, List<Type> groupTypes, List<Integer> groupChannels, io.trino.operator.aggregation.AggregatorFactory... aggregators)
    {
        if (groupChannels.isEmpty()) {
            return new AggregationOperatorFactory(
                    operatorId,
                    new PlanNodeId("aggregation-" + operatorId),
                    List.of(aggregators));
        }
        List<Type> normalizedGroupTypes = (groupTypes.size() == groupChannels.size()) ? groupTypes : groupTypes.subList(0, groupChannels.size());
        return new HashAggregationOperatorFactory(
                operatorId,
                new PlanNodeId("grouped-aggregation-" + operatorId),
                normalizedGroupTypes,
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

    private OperatorFactory orderByFactory(int operatorId, List<Type> types, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        return new OrderByOperator.OrderByOperatorFactory(
                operatorId,
                new PlanNodeId("order-by-" + operatorId),
                types,
                rangeList(types.size()),
                10_000,
                sortChannels,
                sortOrders,
                new PagesIndex.TestingFactory(false),
                false,
                Optional.empty(),
                orderingCompiler);
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

    private OperatorFactory createHashJoinFactory(TaskContext taskContext, HashJoinSpec hashJoinSpec)
    {
        List<Type> buildTypes = hashJoinSpec.buildPlan().outputTypes().isEmpty() ? hashJoinSpec.buildTypes() : hashJoinSpec.buildPlan().outputTypes();
        io.trino.operator.join.unspilled.PartitionedLookupSourceFactory lookupSourceFactory = new io.trino.operator.join.unspilled.PartitionedLookupSourceFactory(
                buildTypes,
                buildTypes,
                hashJoinSpec.buildHashChannels().stream()
                        .map(buildTypes::get)
                        .toList(),
                1,
                false,
                new TypeOperators());
        JoinBridgeManager<io.trino.operator.join.unspilled.PartitionedLookupSourceFactory> joinBridgeManager = new JoinBridgeManager<>(
                false,
                lookupSourceFactory,
                lookupSourceFactory.getOutputTypes());
        OperatorFactory joinFactory = io.trino.operator.OperatorFactories.join(
                io.trino.operator.JoinOperatorType.ofJoinNodeType(hashJoinSpec.joinType(), false, false),
                hashJoinSpec.operatorId(),
                new PlanNodeId("join-" + hashJoinSpec.operatorId()),
                joinBridgeManager,
                false,
                hashJoinSpec.probeTypes(),
                hashJoinSpec.probeJoinChannels(),
                Optional.empty());
        io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory buildOperatorFactory = new io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory(
                9_000 + hashJoinSpec.operatorId(),
                new PlanNodeId("build-" + hashJoinSpec.operatorId()),
                joinBridgeManager,
                rangeList(buildTypes.size()),
                hashJoinSpec.buildHashChannels(),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                100,
                new PagesIndex.TestingFactory(false),
                HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier(taskContext.getSession()));

        DriverContext buildDriverContext = newDriverContext(taskContext);
        List<Operator> buildOperators = new ArrayList<>(createOperators(taskContext, buildDriverContext, hashJoinSpec.buildPlan(), ignoredPage -> {}));
        buildOperators.removeLast();
        buildOperators.add(profiled(hashJoinSpec.profileName() + ".build", buildOperatorFactory.createOperator(buildDriverContext)));
        try (Driver buildDriver = Driver.createDriver(buildDriverContext, buildOperators)) {
            buildOperatorFactory.noMoreOperators();
            java.util.concurrent.Future<io.trino.operator.join.LookupSource> lookupSource = joinBridgeManager.getJoinBridge().createLookupSource();
            while (!lookupSource.isDone()) {
                buildDriver.processForNumberOfIterations(1);
            }
            lookupSource.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to build Trino hash-join lookup source for " + hashJoinSpec.profileName() + " with build types " + buildTypes, exception);
        }

        return joinFactory;
    }

    private OperatorFactory createSemiJoinFactory(TaskContext taskContext, SemiJoinSpec semiJoinSpec)
    {
        SetBuilderOperator.SetBuilderOperatorFactory buildOperatorFactory = new SetBuilderOperator.SetBuilderOperatorFactory(
                9_500 + semiJoinSpec.operatorId(),
                new PlanNodeId("set-build-" + semiJoinSpec.operatorId()),
                semiJoinSpec.buildType(),
                semiJoinSpec.buildChannel(),
                100,
                joinCompiler,
                new TypeOperators());

        DriverContext buildDriverContext = newDriverContext(taskContext);
        List<Operator> buildOperators = new ArrayList<>(createOperators(taskContext, buildDriverContext, semiJoinSpec.buildPlan(), ignoredPage -> {}));
        buildOperators.removeLast();
        buildOperators.add(profiled(semiJoinSpec.profileName() + ".build", buildOperatorFactory.createOperator(buildDriverContext)));
        try (Driver buildDriver = Driver.createDriver(buildDriverContext, buildOperators)) {
            buildOperatorFactory.noMoreOperators();
            processDriver(buildDriver);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to build Trino semi-join set", exception);
        }

        return HashSemiJoinOperator.createOperatorFactory(
                semiJoinSpec.operatorId(),
                new PlanNodeId("semi-join-" + semiJoinSpec.operatorId()),
                buildOperatorFactory.getSetProvider(),
                semiJoinSpec.probeTypes(),
                semiJoinSpec.probeJoinChannel());
    }

    private OperatorFactory createNestedLoopJoinFactory(TaskContext taskContext, NestedLoopJoinSpec nestedLoopJoinSpec)
    {
        JoinBridgeManager<NestedLoopJoinBridge> joinBridgeManager = new JoinBridgeManager<>(
                false,
                new NestedLoopJoinPagesSupplier(),
                nestedLoopJoinSpec.buildTypes());
        OperatorFactory joinFactory = new io.trino.operator.join.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory(
                nestedLoopJoinSpec.operatorId(),
                new PlanNodeId("nested-loop-join-" + nestedLoopJoinSpec.operatorId()),
                joinBridgeManager,
                rangeList(nestedLoopJoinSpec.probeTypes().size()),
                rangeList(nestedLoopJoinSpec.buildTypes().size()));
        io.trino.operator.join.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory buildOperatorFactory =
                new io.trino.operator.join.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory(
                        9_700 + nestedLoopJoinSpec.operatorId(),
                        new PlanNodeId("nested-loop-build-" + nestedLoopJoinSpec.operatorId()),
                        joinBridgeManager);

        DriverContext buildDriverContext = newDriverContext(taskContext);
        List<Operator> buildOperators = new ArrayList<>(createOperators(taskContext, buildDriverContext, nestedLoopJoinSpec.buildPlan(), ignoredPage -> {}));
        buildOperators.removeLast();
        buildOperators.add(profiled(nestedLoopJoinSpec.profileName() + ".build", buildOperatorFactory.createOperator(buildDriverContext)));
        try (Driver buildDriver = Driver.createDriver(buildDriverContext, buildOperators)) {
            buildOperatorFactory.noMoreOperators();
            ListenableFuture<?> buildFinished = joinBridgeManager.getJoinBridge().whenBuildFinishes();
            while (!buildFinished.isDone()) {
                buildDriver.processForNumberOfIterations(1);
            }
            buildFinished.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to build Trino nested-loop pages", exception);
        }

        return joinFactory;
    }

    private static PipelineStep namedFactoryStep(String name, OperatorFactory factory)
    {
        return new FactoryStep(name, factory);
    }

    private static PipelineStep namedHashJoinStep(String name, HashJoinSpec spec)
    {
        return new HashJoinStep(name, spec.withProfileName(name));
    }

    private static PipelineStep namedSemiJoinStep(String name, SemiJoinSpec spec)
    {
        return new SemiJoinStep(name, spec.withProfileName(name));
    }

    private static PipelineStep namedNestedLoopJoinStep(String name, NestedLoopJoinSpec spec)
    {
        return new NestedLoopJoinStep(name, spec.withProfileName(name));
    }

    private static PipelinePlan appendPlan(PipelinePlan plan, List<PipelineStep> additionalSteps, String sinkName, List<Type> outputTypes)
    {
        List<PipelineStep> steps = new ArrayList<>(plan.steps().size() + additionalSteps.size());
        steps.addAll(plan.steps());
        steps.addAll(additionalSteps);
        return new PipelinePlan(plan.source(), List.copyOf(steps), sinkName, outputTypes);
    }

    private static List<Type> concatTypes(List<Type> left, List<Type> right)
    {
        List<Type> types = new ArrayList<>(left.size() + right.size());
        types.addAll(left);
        types.addAll(right);
        return types;
    }

    private static List<RowExpression> identityProjections(List<Type> types)
    {
        List<RowExpression> projections = new ArrayList<>(types.size());
        for (int index = 0; index < types.size(); index++) {
            projections.add(field(index, types.get(index)));
        }
        return projections;
    }

    /** extendedprice * (1 - discount), the revenue term shared across the queries. */
    private static RowExpression discountedPrice(int extendedPriceChannel, int discountChannel)
    {
        return multiply(
                field(extendedPriceChannel, DOUBLE),
                subtract(constant(1.0, DOUBLE), field(discountChannel, DOUBLE), DOUBLE),
                DOUBLE);
    }

    private static RowExpression equal(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(left, right));
    }

    private static RowExpression equalUtf8(int inputChannel, String constantValue, Type type)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(field(inputChannel, type), constant(Slices.utf8Slice(constantValue), type)));
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

    private static RowExpression ifExpression(RowExpression condition, RowExpression whenTrue, RowExpression whenFalse, Type outputType)
    {
        return new SpecialForm(SpecialForm.Form.IF, outputType, List.of(condition, whenTrue, whenFalse), List.of());
    }

    private static RowExpression like(RowExpression expression, String pattern)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                List.of(expression, constant(LikePattern.compile(pattern, Optional.empty()), LIKE_PATTERN)));
    }

    private static RowExpression not(RowExpression expression)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)), List.of(expression));
    }

    private static RowExpression substring(RowExpression expression, long start, long length, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveFunction("substr", fromTypes(type, BIGINT, BIGINT)), List.of(expression, constant(start, BIGINT), constant(length, BIGINT)));
    }

    /** extract(YEAR FROM date), a BIGINT. */
    private static RowExpression year(RowExpression expression, Type dateType)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(dateType)), List.of(expression));
    }

    private static RowExpression greaterThan(RowExpression left, RowExpression right, Type type)
    {
        return lessThan(right, left, type);
    }

    private static RowExpression greaterThanOrEqual(RowExpression left, RowExpression right, Type type)
    {
        return lessThanOrEqual(right, left, type);
    }

    private static RowExpression lessThan(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression lessThanOrEqual(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression add(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression subtract(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression multiply(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression divide(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression cast(RowExpression expression, Type fromType, Type toType)
    {
        return new CallExpression(FUNCTION_RESOLUTION.getCoercion(fromType, toType), List.of(expression));
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
            throw new IllegalStateException("Timed out waiting for blocked Trino TPC-H driver", exception);
        }
    }

    private sealed interface PipelineSource
            permits FilesPipelineSource
    {
        String profileName();
    }

    private record FilesPipelineSource(List<Path> files, List<String> columns, String profileName)
            implements PipelineSource
    {
    }

    private record PipelinePlan(PipelineSource source, List<PipelineStep> steps, String sinkName, List<Type> outputTypes)
    {
    }

    private record HashJoinSpec(
            int operatorId,
            List<Type> probeTypes,
            List<Integer> probeJoinChannels,
            PipelinePlan buildPlan,
            List<Type> buildTypes,
            List<Integer> buildHashChannels,
            JoinType joinType,
            String profileName)
    {
        private HashJoinSpec(int operatorId, List<Type> probeTypes, List<Integer> probeJoinChannels, PipelinePlan buildPlan, List<Type> buildTypes, List<Integer> buildHashChannels)
        {
            this(operatorId, probeTypes, probeJoinChannels, buildPlan, buildTypes, buildHashChannels, JoinType.INNER, "join-" + operatorId);
        }

        private HashJoinSpec(int operatorId, List<Type> probeTypes, List<Integer> probeJoinChannels, PipelinePlan buildPlan, List<Type> buildTypes, List<Integer> buildHashChannels, JoinType joinType)
        {
            this(operatorId, probeTypes, probeJoinChannels, buildPlan, buildTypes, buildHashChannels, joinType, "join-" + operatorId);
        }

        private HashJoinSpec withProfileName(String profileName)
        {
            return new HashJoinSpec(operatorId, probeTypes, probeJoinChannels, buildPlan, buildTypes, buildHashChannels, joinType, profileName);
        }
    }

    private record SemiJoinSpec(
            int operatorId,
            List<Type> probeTypes,
            int probeJoinChannel,
            PipelinePlan buildPlan,
            Type buildType,
            int buildChannel,
            String profileName)
    {
        private SemiJoinSpec(int operatorId, List<Type> probeTypes, int probeJoinChannel, PipelinePlan buildPlan, Type buildType, int buildChannel)
        {
            this(operatorId, probeTypes, probeJoinChannel, buildPlan, buildType, buildChannel, "semi-join-" + operatorId);
        }

        private SemiJoinSpec withProfileName(String profileName)
        {
            return new SemiJoinSpec(operatorId, probeTypes, probeJoinChannel, buildPlan, buildType, buildChannel, profileName);
        }
    }

    private record NestedLoopJoinSpec(
            int operatorId,
            List<Type> probeTypes,
            PipelinePlan buildPlan,
            List<Type> buildTypes,
            String profileName)
    {
        private NestedLoopJoinSpec(int operatorId, List<Type> probeTypes, PipelinePlan buildPlan, List<Type> buildTypes)
        {
            this(operatorId, probeTypes, buildPlan, buildTypes, "nested-loop-join-" + operatorId);
        }

        private NestedLoopJoinSpec withProfileName(String profileName)
        {
            return new NestedLoopJoinSpec(operatorId, probeTypes, buildPlan, buildTypes, profileName);
        }
    }

    private static final class ParquetPageSourceOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private final TrinoClickBenchPageReader reader;
        private boolean finished;

        private ParquetPageSourceOperator(OperatorContext operatorContext, List<Path> files, List<String> columns)
        {
            this.operatorContext = operatorContext;
            this.reader = new TrinoClickBenchPageReader(files, columns);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public void finish()
        {
            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return finished || !reader.hasNext();
        }

        @Override
        public boolean needsInput()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getOutput()
        {
            if (finished || !reader.hasNext()) {
                return null;
            }
            Page page = reader.nextPage();
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
            return page;
        }

        @Override
        public void close()
        {
            reader.close();
        }
    }

    private Operator profiled(String name, Operator operator)
    {
        TrinoOperatorCpuProfile profile = CURRENT_OPERATOR_CPU_PROFILE.get();
        if (profile == null || name == null) {
            return operator;
        }
        return profile.wrap(name, operator);
    }

    private sealed interface PipelineStep
            permits FactoryStep, HashJoinStep, SemiJoinStep, NestedLoopJoinStep
    {
        OperatorFactory createOperatorFactory(TaskContext taskContext, TrinoTpchParquetSupport support);

        String profileName();
    }

    private record FactoryStep(String profileName, OperatorFactory factory)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(TaskContext taskContext, TrinoTpchParquetSupport support)
        {
            return factory;
        }
    }

    private record HashJoinStep(String profileName, HashJoinSpec spec)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(TaskContext taskContext, TrinoTpchParquetSupport support)
        {
            return support.createHashJoinFactory(taskContext, spec);
        }
    }

    private record SemiJoinStep(String profileName, SemiJoinSpec spec)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(TaskContext taskContext, TrinoTpchParquetSupport support)
        {
            return support.createSemiJoinFactory(taskContext, spec);
        }
    }

    private record NestedLoopJoinStep(String profileName, NestedLoopJoinSpec spec)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(TaskContext taskContext, TrinoTpchParquetSupport support)
        {
            return support.createNestedLoopJoinFactory(taskContext, spec);
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

    @Override
    public void close()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }
}
