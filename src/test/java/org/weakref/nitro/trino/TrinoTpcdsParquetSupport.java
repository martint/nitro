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
import io.airlift.units.DataSize;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.TopNOperator;
import io.trino.operator.TopNRankingOperator;
import io.trino.operator.ValuesOperator;
import io.trino.operator.WindowFunctionDefinition;
import io.trino.operator.WindowOperator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.window.AggregationWindowFunctionSupplier;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.RegularPartitionerSupplier;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SpillerFactory;
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
import org.weakref.nitro.tpcds.TpcdsParquetTables;

import java.nio.file.Path;
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
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.operator.WindowFunctionDefinition.window;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
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
    private static final ThreadLocal<TrinoOperatorCpuProfile> CURRENT_OPERATOR_CPU_PROFILE = new ThreadLocal<>();
    private static final String TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY = "nitro.clickbench.trino.blockedWaitTimeoutSeconds";
    private static final int DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS = 5;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
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

    private Path currentRootDirectory;
    private String currentSchema;
    private TrinoTpcdsParquetSqlSupport sqlSupport;
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TrinoTpcdsParquetSupport"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TrinoTpcdsParquetSupport-scheduled"));

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

    private final OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());
    private final int blockedWaitTimeoutSeconds = blockedWaitTimeoutSeconds();

    public static boolean supportsOperatorAssembly(String queryId)
    {
        return "57".equals(queryId);
    }

    public MaterializedResult query01(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("01");
    }

    public MaterializedResult query02(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("02");
    }

    public MaterializedResult query03(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("03");
    }

    public MaterializedResult query04(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("04");
    }

    public MaterializedResult query05(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("05");
    }

    public MaterializedResult query06(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("06");
    }

    public MaterializedResult query07(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("07");
    }

    public MaterializedResult query08(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("08");
    }

    public MaterializedResult query09(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("09");
    }

    public MaterializedResult query10(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("10");
    }

    public MaterializedResult query11(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("11");
    }

    public MaterializedResult query12(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("12");
    }

    public MaterializedResult query13(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("13");
    }

    public MaterializedResult query14(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("14");
    }

    public MaterializedResult query15(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("15");
    }

    public MaterializedResult query16(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("16");
    }

    public MaterializedResult query17(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("17");
    }

    public MaterializedResult query18(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("18");
    }

    public MaterializedResult query19(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("19");
    }

    public MaterializedResult query20(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("20");
    }

    public MaterializedResult query21(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("21");
    }

    public MaterializedResult query22(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("22");
    }

    public MaterializedResult query23(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("23");
    }

    public MaterializedResult query24(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("24");
    }

    public MaterializedResult query25(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("25");
    }

    public MaterializedResult query26(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("26");
    }

    public MaterializedResult query27(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("27");
    }

    public MaterializedResult query28(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("28");
    }

    public MaterializedResult query29(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("29");
    }

    public MaterializedResult query30(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("30");
    }

    public MaterializedResult query31(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("31");
    }

    public MaterializedResult query32(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("32");
    }

    public MaterializedResult query33(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("33");
    }

    public MaterializedResult query34(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("34");
    }

    public MaterializedResult query35(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("35");
    }

    public MaterializedResult query36(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("36");
    }

    public MaterializedResult query37(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("37");
    }

    public MaterializedResult query38(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("38");
    }

    public MaterializedResult query39(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("39");
    }

    public MaterializedResult query40(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("40");
    }

    public MaterializedResult query41(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("41");
    }

    public MaterializedResult query42(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("42");
    }

    public MaterializedResult query43(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("43");
    }

    public MaterializedResult query44(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("44");
    }

    public MaterializedResult query45(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("45");
    }

    public MaterializedResult query46(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("46");
    }

    public MaterializedResult query47(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("47");
    }

    public MaterializedResult query48(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("48");
    }

    public MaterializedResult query49(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("49");
    }

    public MaterializedResult query50(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("50");
    }

    public MaterializedResult query51(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("51");
    }

    public MaterializedResult query52(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("52");
    }

    public MaterializedResult query53(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("53");
    }

    public MaterializedResult query54(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("54");
    }

    public MaterializedResult query55(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("55");
    }

    public MaterializedResult query56(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("56");
    }

    public MaterializedResult query57(TpcdsParquetTables tables)
    {
        Type sumType = query57SumType(tables);
        Type averageType = query57AverageType(tables);
        List<Type> currentTypes = query57CurrentTypes(tables);
        List<Type> adjacentTypes = List.of(currentTypes.get(0), currentTypes.get(1), currentTypes.get(2), sumType, BIGINT);
        List<Type> afterPreviousTypes = concatTypes(currentTypes, adjacentTypes);
        List<Type> afterNextTypes = concatTypes(afterPreviousTypes, adjacentTypes);
        List<Type> sortedTypes = concatTypes(
                List.of(currentTypes.get(0), currentTypes.get(1), currentTypes.get(2), currentTypes.get(3), currentTypes.get(4), currentTypes.get(5), currentTypes.get(6), sumType, sumType),
                List.of(DOUBLE));
        List<Type> outputTypes = List.of(currentTypes.get(0), currentTypes.get(1), currentTypes.get(2), currentTypes.get(3), currentTypes.get(4), currentTypes.get(5), currentTypes.get(6), sumType, sumType);

        PipelinePlan currentPlan = query57CurrentPlan(tables);
        PipelinePlan previousPlan = query57PreviousPlan(tables);
        PipelinePlan nextPlan = query57NextPlan(tables);

        PipelinePlan queryPlan = appendPlan(
                currentPlan,
                List.of(
                        namedHashJoinStep("q57.join.previous", new HashJoinSpec(57_20, currentTypes, List.of(0, 1, 2, 7), previousPlan, adjacentTypes, List.of(0, 1, 2, 4))),
                        namedHashJoinStep("q57.join.next", new HashJoinSpec(57_21, afterPreviousTypes, List.of(0, 1, 2, 7), nextPlan, adjacentTypes, List.of(0, 1, 2, 4))),
                        namedFactoryStep("q57.filter.project.deviation", filterAndProjectFactory(
                                57_22,
                                Optional.of(queryRelativeDeviationPredicate(6, 5, sumType, averageType)),
                                List.of(
                                        field(0, currentTypes.get(0)),
                                        field(1, currentTypes.get(1)),
                                        field(2, currentTypes.get(2)),
                                        field(3, currentTypes.get(3)),
                                        field(4, currentTypes.get(4)),
                                        field(5, currentTypes.get(5)),
                                        field(6, currentTypes.get(6)),
                                        field(11, sumType),
                                        field(16, sumType),
                                        subtract(cast(field(6, sumType), sumType, DOUBLE), cast(field(5, averageType), averageType, DOUBLE), DOUBLE)),
                                sortedTypes)),
                        namedFactoryStep("q57.topn", topNFactory(57_23, sortedTypes, 100, List.of(9, 2), List.of(ASC_NULLS_LAST, ASC_NULLS_LAST))),
                        namedFactoryStep("q57.project.final", filterAndProjectFactory(
                                57_24,
                                Optional.empty(),
                                List.of(
                                        field(0, outputTypes.get(0)),
                                        field(1, outputTypes.get(1)),
                                        field(2, outputTypes.get(2)),
                                        field(3, outputTypes.get(3)),
                                        field(4, outputTypes.get(4)),
                                        field(5, outputTypes.get(5)),
                                        field(6, outputTypes.get(6)),
                                        field(7, outputTypes.get(7)),
                                        field(8, outputTypes.get(8))),
                                outputTypes))),
                "q57.sink.final");

        return executePipelinePlan(queryPlan, outputTypes);
    }

    public MaterializedResult query58(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("58");
    }

    public MaterializedResult query59(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("59");
    }

    public MaterializedResult query60(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("60");
    }

    public MaterializedResult query61(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("61");
    }

    public MaterializedResult query62(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("62");
    }

    public MaterializedResult query63(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("63");
    }

    public MaterializedResult query64(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("64");
    }

    public MaterializedResult query65(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("65");
    }

    public MaterializedResult query66(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("66");
    }

    public MaterializedResult query67(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("67");
    }

    public MaterializedResult query68(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("68");
    }

    public MaterializedResult query69(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("69");
    }

    public MaterializedResult query70(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("70");
    }

    public MaterializedResult query71(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("71");
    }

    public MaterializedResult query72(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("72");
    }

    public MaterializedResult query73(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("73");
    }

    public MaterializedResult query74(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("74");
    }

    public MaterializedResult query75(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("75");
    }

    public MaterializedResult query76(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("76");
    }

    public MaterializedResult query77(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("77");
    }

    public MaterializedResult query78(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("78");
    }

    public MaterializedResult query79(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("79");
    }

    public MaterializedResult query80(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("80");
    }

    public MaterializedResult query81(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("81");
    }

    public MaterializedResult query82(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("82");
    }

    public MaterializedResult query83(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("83");
    }

    public MaterializedResult query84(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("84");
    }

    public MaterializedResult query85(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("85");
    }

    public MaterializedResult query86(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("86");
    }

    public MaterializedResult query87(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("87");
    }

    public MaterializedResult query88(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("88");
    }

    public MaterializedResult query89(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("89");
    }

    public MaterializedResult query90(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("90");
    }

    public MaterializedResult query91(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("91");
    }

    public MaterializedResult query92(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("92");
    }

    public MaterializedResult query93(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("93");
    }

    public MaterializedResult query94(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("94");
    }

    public MaterializedResult query95(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("95");
    }

    public MaterializedResult query96(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("96");
    }

    public MaterializedResult query97(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("97");
    }

    public MaterializedResult query98(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("98");
    }

    public MaterializedResult query99(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("99");
    }

    public MaterializedResult query57JoinedFacts(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = query57JoinedFactTypes(tables);
        return executePipelinePlan(query57JoinedFactsPlan(tables), outputTypes);
    }

    public MaterializedResult query57MonthlyGroupedSales(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = query57MonthlyGroupedTypes(tables);
        return executePipelinePlan(query57MonthlyGroupedSalesPlan(tables), outputTypes);
    }

    public MaterializedResult query57MonthlyRankedSales(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = query57MonthlyRankedTypes(tables);
        return executePipelinePlan(query57MonthlyRankedSalesPlan(tables), outputTypes);
    }

    public MaterializedResult query57CurrentRows(TpcdsParquetTables tables)
    {
        List<Type> outputTypes = query57CurrentTypes(tables);
        return executePipelinePlan(query57CurrentPlan(tables), outputTypes);
    }

    public MaterializedResult query57PreviousRows(TpcdsParquetTables tables)
    {
        Type sumType = query57SumType(tables);
        List<Type> outputTypes = List.of(query57CategoryType(tables), query57BrandType(tables), query57CallCenterNameType(tables), sumType, BIGINT);
        return executePipelinePlan(query57PreviousPlan(tables), outputTypes);
    }

    public MaterializedResult query57NextRows(TpcdsParquetTables tables)
    {
        Type sumType = query57SumType(tables);
        List<Type> outputTypes = List.of(query57CategoryType(tables), query57BrandType(tables), query57CallCenterNameType(tables), sumType, BIGINT);
        return executePipelinePlan(query57NextPlan(tables), outputTypes);
    }

    private PipelinePlan query57JoinedFactsPlan(TpcdsParquetTables tables)
    {
        List<String> factColumns = List.of("cs_sold_date_sk", "cs_call_center_sk", "cs_item_sk", "cs_sales_price");
        List<Type> factTypes = tableColumnTypes(tables, "catalog_sales", factColumns);
        Type salesType = factTypes.get(3);
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_brand", "i_category"));
        List<Type> dateTypes = tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_year", "d_moy"));
        List<Type> callCenterTypes = tableColumnTypes(tables, "call_center", List.of("cc_call_center_sk", "cc_name"));
        List<Type> projectedTypes = query57JoinedFactTypes(tables);

        PipelinePlan itemKeys = relationPlan(
                tables,
                "item",
                List.of("i_item_sk", "i_brand", "i_category"),
                Optional.empty(),
                identityProjections(itemTypes),
                itemTypes,
                "q57.scan.item",
                "q57.sink.item");
        PipelinePlan allowedDates = relationPlan(
                tables,
                "date_dim",
                List.of("d_date_sk", "d_year", "d_moy"),
                Optional.of(query57DatePredicate()),
                identityProjections(dateTypes),
                dateTypes,
                "q57.scan.date_dim",
                "q57.sink.date_dim");
        PipelinePlan callCenters = relationPlan(
                tables,
                "call_center",
                List.of("cc_call_center_sk", "cc_name"),
                Optional.empty(),
                identityProjections(callCenterTypes),
                callCenterTypes,
                "q57.scan.call_center",
                "q57.sink.call_center");

        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles("catalog_sales"), factColumns, "q57.scan.catalog_sales"),
                List.of(
                        namedHashJoinStep("q57.join.item", new HashJoinSpec(57_0, factTypes, List.of(2), itemKeys, itemTypes, List.of(0))),
                        namedHashJoinStep("q57.join.date_dim", new HashJoinSpec(57_1, concatTypes(factTypes, itemTypes), List.of(0), allowedDates, dateTypes, List.of(0))),
                        namedHashJoinStep("q57.join.call_center", new HashJoinSpec(57_2, concatTypes(concatTypes(factTypes, itemTypes), dateTypes), List.of(1), callCenters, callCenterTypes, List.of(0))),
                        namedFactoryStep("q57.project.joined_facts", filterAndProjectFactory(
                                57_3,
                                Optional.empty(),
                                List.of(
                                        field(6, itemTypes.get(2)),
                                        field(5, itemTypes.get(1)),
                                        field(11, callCenterTypes.get(1)),
                                        field(8, dateTypes.get(1)),
                                        field(9, dateTypes.get(2)),
                                        field(3, salesType)),
                                projectedTypes))),
                "q57.sink.joined_facts");
    }

    private PipelinePlan query57MonthlyGroupedSalesPlan(TpcdsParquetTables tables)
    {
        List<Type> joinedTypes = query57JoinedFactTypes(tables);
        Type salesType = joinedTypes.get(5);
        TestingAggregationFunction salesSum = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType));
        List<Type> groupedTypes = query57MonthlyGroupedTypes(tables);
        return appendPlan(
                query57JoinedFactsPlan(tables),
                List.of(namedFactoryStep("q57.group.monthly_sales", hashAggregationFactory(
                        57_4,
                        groupedTypes.subList(0, 5),
                        List.of(0, 1, 2, 3, 4),
                        salesSum.createAggregatorFactory(Step.SINGLE, List.of(5), OptionalInt.empty())))),
                "q57.sink.monthly_grouped");
    }

    private PipelinePlan query57MonthlyRankedSalesPlan(TpcdsParquetTables tables)
    {
        List<Type> groupedTypes = query57MonthlyGroupedTypes(tables);
        return appendPlan(
                query57MonthlyGroupedSalesPlan(tables),
                List.of(namedFactoryStep("q57.rank.monthly_sales", topNRankingFactory(
                        57_5,
                        groupedTypes,
                        List.of(0, 1, 2, 3, 4, 5),
                        List.of(0, 1, 2),
                        List.of(3, 4),
                        List.of(ASC_NULLS_LAST, ASC_NULLS_LAST),
                        32))),
                "q57.sink.monthly_ranked");
    }

    private PipelinePlan query57CurrentPlan(TpcdsParquetTables tables)
    {
        List<Type> rankedTypes = query57MonthlyRankedTypes(tables);
        Type averageType = query57AverageType(tables);
        Type sumType = query57SumType(tables);
        List<Type> currentTypes = query57CurrentTypes(tables);
        return appendPlan(
                query57MonthlyRankedSalesPlan(tables),
                List.of(
                        namedFactoryStep("q57.filter.current_year", filterAndProjectFactory(
                                57_10,
                                Optional.of(equal(3, 1999, INTEGER)),
                                identityProjections(rankedTypes),
                                rankedTypes)),
                        namedFactoryStep("q57.window.current_avg", windowFactory(
                                57_11,
                                rankedTypes,
                                List.of(0, 1, 2, 3, 4, 5, 6),
                                List.of(0, 1, 2, 3),
                                List.of(),
                                List.of(),
                                List.of(aggregateWindowFunction("avg", List.of(sumType), averageType, PARTITION_ROWS_FRAME, 5)))),
                        namedFactoryStep("q57.project.current", filterAndProjectFactory(
                                57_12,
                                Optional.empty(),
                                List.of(
                                        field(0, currentTypes.get(0)),
                                        field(1, currentTypes.get(1)),
                                        field(2, currentTypes.get(2)),
                                        field(3, currentTypes.get(3)),
                                        field(4, currentTypes.get(4)),
                                        field(7, averageType),
                                        field(5, sumType),
                                        field(6, BIGINT)),
                                currentTypes))),
                "q57.sink.current");
    }

    private PipelinePlan query57PreviousPlan(TpcdsParquetTables tables)
    {
        Type sumType = query57SumType(tables);
        List<Type> rankedTypes = query57MonthlyRankedTypes(tables);
        List<Type> adjacentTypes = List.of(query57CategoryType(tables), query57BrandType(tables), query57CallCenterNameType(tables), sumType, BIGINT);
        return appendPlan(
                query57MonthlyRankedSalesPlan(tables),
                List.of(namedFactoryStep("q57.project.previous", filterAndProjectFactory(
                        57_13,
                        Optional.empty(),
                        List.of(
                                field(0, rankedTypes.get(0)),
                                field(1, rankedTypes.get(1)),
                                field(2, rankedTypes.get(2)),
                                field(5, sumType),
                                add(field(6, BIGINT), constant(1L, BIGINT), BIGINT)),
                        adjacentTypes))),
                "q57.sink.previous");
    }

    private PipelinePlan query57NextPlan(TpcdsParquetTables tables)
    {
        Type sumType = query57SumType(tables);
        List<Type> rankedTypes = query57MonthlyRankedTypes(tables);
        List<Type> adjacentTypes = List.of(query57CategoryType(tables), query57BrandType(tables), query57CallCenterNameType(tables), sumType, BIGINT);
        return appendPlan(
                query57MonthlyRankedSalesPlan(tables),
                List.of(namedFactoryStep("q57.project.next", filterAndProjectFactory(
                        57_14,
                        Optional.empty(),
                        List.of(
                                field(0, rankedTypes.get(0)),
                                field(1, rankedTypes.get(1)),
                                field(2, rankedTypes.get(2)),
                                field(5, sumType),
                                subtract(field(6, BIGINT), constant(1L, BIGINT), BIGINT)),
                        adjacentTypes))),
                "q57.sink.next");
    }

    private List<Type> query57JoinedFactTypes(TpcdsParquetTables tables)
    {
        List<Type> itemTypes = tableColumnTypes(tables, "item", List.of("i_item_sk", "i_brand", "i_category"));
        List<Type> dateTypes = tableColumnTypes(tables, "date_dim", List.of("d_date_sk", "d_year", "d_moy"));
        List<Type> callCenterTypes = tableColumnTypes(tables, "call_center", List.of("cc_call_center_sk", "cc_name"));
        Type salesType = tableColumnTypes(tables, "catalog_sales", List.of("cs_sales_price")).getFirst();
        return List.of(itemTypes.get(2), itemTypes.get(1), callCenterTypes.get(1), dateTypes.get(1), dateTypes.get(2), salesType);
    }

    private List<Type> query57MonthlyGroupedTypes(TpcdsParquetTables tables)
    {
        List<Type> joinedTypes = query57JoinedFactTypes(tables);
        return List.of(joinedTypes.get(0), joinedTypes.get(1), joinedTypes.get(2), joinedTypes.get(3), joinedTypes.get(4), query57SumType(tables));
    }

    private List<Type> query57MonthlyRankedTypes(TpcdsParquetTables tables)
    {
        return concatTypes(query57MonthlyGroupedTypes(tables), List.of(BIGINT));
    }

    private List<Type> query57CurrentTypes(TpcdsParquetTables tables)
    {
        List<Type> groupedTypes = query57MonthlyGroupedTypes(tables);
        return List.of(groupedTypes.get(0), groupedTypes.get(1), groupedTypes.get(2), groupedTypes.get(3), groupedTypes.get(4), query57AverageType(tables), groupedTypes.get(5), BIGINT);
    }

    private Type query57CategoryType(TpcdsParquetTables tables)
    {
        return tableColumnTypes(tables, "item", List.of("i_category")).getFirst();
    }

    private Type query57BrandType(TpcdsParquetTables tables)
    {
        return tableColumnTypes(tables, "item", List.of("i_brand")).getFirst();
    }

    private Type query57CallCenterNameType(TpcdsParquetTables tables)
    {
        return tableColumnTypes(tables, "call_center", List.of("cc_name")).getFirst();
    }

    private Type query57SumType(TpcdsParquetTables tables)
    {
        Type salesType = tableColumnTypes(tables, "catalog_sales", List.of("cs_sales_price")).getFirst();
        return FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(salesType)).getFinalType();
    }

    private Type query57AverageType(TpcdsParquetTables tables)
    {
        return FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(query57SumType(tables))).getFinalType();
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

    private MaterializedResult executePipelinePlan(PipelinePlan plan, List<Type> outputTypes)
    {
        List<Page> outputPages = executePipelinePlan(plan);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (Page page : outputPages) {
            result.page(page);
        }
        return result.build();
    }

    private MaterializedResult executePagesPipeline(List<Page> inputPages, List<PipelineStep> steps, List<Type> outputTypes, String sourceName, String sinkName)
    {
        List<Page> outputPages = executePipelinePages(inputPages, steps, sourceName, sinkName);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(taskContext().getSession(), outputTypes);
        for (Page page : outputPages) {
            result.page(page);
        }
        return result.build();
    }

    private List<Page> executePipelinePages(List<Path> files, List<String> columns, List<PipelineStep> steps)
    {
        try (TrinoClickBenchPageReader reader = new TrinoClickBenchPageReader(files, columns)) {
            return executePipelinePages(readPages(reader), steps, "values-source", "sink");
        }
    }

    private List<Page> executePipelinePages(List<Page> inputPages, List<PipelineStep> steps)
    {
        return executePipelinePages(inputPages, steps, "values-source", "sink");
    }

    private List<Page> executePipelinePages(List<Path> files, List<String> columns, List<PipelineStep> steps, String sourceName, String sinkName)
    {
        try (TrinoClickBenchPageReader reader = new TrinoClickBenchPageReader(files, columns)) {
            return executePipelinePages(readPages(reader), steps, sourceName, sinkName);
        }
    }

    private List<Page> executePipelinePages(List<Page> inputPages, List<PipelineStep> steps, String sourceName, String sinkName)
    {
        List<Page> outputPages = new ArrayList<>();
        io.trino.operator.TaskContext taskContext = taskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        List<Operator> operators = new ArrayList<>();
        ValuesOperator.ValuesOperatorFactory sourceFactory = new ValuesOperator.ValuesOperatorFactory(0, new PlanNodeId("values-source"), inputPages);
        operators.add(profiled(sourceName, sourceFactory.createOperator(driverContext)));
        sourceFactory.noMoreOperators();

        for (PipelineStep step : steps) {
            OperatorFactory factory = step.createOperatorFactory(taskContext, this);
            operators.add(profiled(step.profileName(), factory.createOperator(driverContext)));
            factory.noMoreOperators();
        }

        operators.add(profiled(sinkName, new PageConsumerOperator(
                driverContext.addOperatorContext(1000, new PlanNodeId("sink"), PageConsumerOperator.class.getSimpleName()),
                outputPages::add,
                java.util.function.Function.identity())));

        try (Driver driver = Driver.createDriver(driverContext, operators)) {
            processDriver(driver);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to execute Trino TPC-DS parquet pages pipeline", exception);
        }

        return outputPages;
    }

    private List<Page> executePipelinePlan(PipelinePlan plan)
    {
        List<Page> outputPages = new ArrayList<>();
        io.trino.operator.TaskContext taskContext = taskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        List<Operator> operators = createOperators(taskContext, driverContext, plan, outputPages::add);

        try (Driver driver = Driver.createDriver(driverContext, operators)) {
            processDriver(driver);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to execute Trino TPC-DS parquet pipeline plan", exception);
        }

        return outputPages;
    }

    private io.trino.operator.TaskContext taskContext()
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TestingSession.testSessionBuilder().build())
                .setQueryMaxMemory(DataSize.of(4, GIGABYTE))
                .setMemoryPoolSize(DataSize.of(4, GIGABYTE))
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

    private List<Operator> createOperators(io.trino.operator.TaskContext taskContext, DriverContext driverContext, PipelinePlan plan, java.util.function.Consumer<Page> pageConsumer)
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
                java.util.function.Function.identity())));
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

    private PipelinePlan relationPlan(TpcdsParquetTables tables, String tableName, List<String> columns, Optional<RowExpression> filter, List<RowExpression> projections, List<Type> outputTypes, String operatorName, String sinkName)
    {
        return new PipelinePlan(
                new FilesPipelineSource(tables.tableFiles(tableName), columns, operatorName + ".source"),
                List.of(namedFactoryStep(operatorName, filterAndProjectFactory(7_000 + Math.abs(tableName.hashCode() % 1_000), filter, projections, outputTypes))),
                sinkName);
    }

    private List<Type> tableColumnTypes(TpcdsParquetTables tables, String tableName, List<String> columns)
    {
        return TrinoClickBenchPageReader.columnTypes(tables.tableFiles(tableName).getFirst(), columns);
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

    private OperatorFactory createHashJoinFactory(io.trino.operator.TaskContext taskContext, HashJoinSpec hashJoinSpec)
    {
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
                io.trino.operator.JoinOperatorType.innerJoin(false, false),
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
                rangeList(hashJoinSpec.buildTypes().size()),
                hashJoinSpec.buildHashChannels(),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                100,
                new PagesIndex.TestingFactory(false),
                HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier(taskContext.getSession()));

        DriverContext buildDriverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
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
            throw new RuntimeException("Unable to build Trino hash-join lookup source", exception);
        }

        return joinFactory;
    }

    private static PipelineStep factoryStep(OperatorFactory factory)
    {
        return new FactoryStep(null, factory);
    }

    private static PipelineStep namedFactoryStep(String name, OperatorFactory factory)
    {
        return new FactoryStep(name, factory);
    }

    private static PipelineStep hashJoinStep(HashJoinSpec spec)
    {
        return new HashJoinStep(null, spec);
    }

    private static PipelineStep namedHashJoinStep(String name, HashJoinSpec spec)
    {
        return new HashJoinStep(name, spec.withProfileName(name));
    }

    private static PipelinePlan appendPlan(PipelinePlan plan, List<PipelineStep> additionalSteps, String sinkName)
    {
        List<PipelineStep> steps = new ArrayList<>(plan.steps().size() + additionalSteps.size());
        steps.addAll(plan.steps());
        steps.addAll(additionalSteps);
        return new PipelinePlan(plan.source(), List.copyOf(steps), sinkName);
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

    private static List<Page> readPages(TrinoClickBenchPageReader reader)
    {
        List<Page> pages = new ArrayList<>();
        while (reader.hasNext()) {
            pages.add(reader.nextPage());
        }
        return pages;
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

    private static RowExpression equal(int inputChannel, long constantValue, Type type)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(field(inputChannel, type), constant(constantValue, type)));
    }

    private static RowExpression and(RowExpression first, RowExpression second)
    {
        return new SpecialForm(SpecialForm.Form.AND, BOOLEAN, List.of(first, second), List.of());
    }

    private static RowExpression or(RowExpression first, RowExpression second, RowExpression... rest)
    {
        RowExpression result = new SpecialForm(SpecialForm.Form.OR, BOOLEAN, List.of(first, second), List.of());
        for (RowExpression expression : rest) {
            result = new SpecialForm(SpecialForm.Form.OR, BOOLEAN, List.of(result, expression), List.of());
        }
        return result;
    }

    private static RowExpression greaterThan(RowExpression left, RowExpression right, Type type)
    {
        return lessThan(right, left, type);
    }

    private static RowExpression lessThan(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, List.of(type, type)), List.of(left, right));
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

    private static RowExpression cast(RowExpression expression, Type fromType, Type toType)
    {
        return new CallExpression(FUNCTION_RESOLUTION.getCoercion(fromType, toType), List.of(expression));
    }

    private static RowExpression ifExpression(RowExpression condition, RowExpression whenTrue, RowExpression whenFalse, Type outputType)
    {
        return new SpecialForm(SpecialForm.Form.IF, outputType, List.of(condition, whenTrue, whenFalse), List.of());
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

    private record HashJoinSpec(
            int operatorId,
            List<Type> probeTypes,
            List<Integer> probeJoinChannels,
            PipelinePlan buildPlan,
            List<Type> buildTypes,
            List<Integer> buildHashChannels,
            String profileName)
    {
        private HashJoinSpec(int operatorId, List<Type> probeTypes, List<Integer> probeJoinChannels, PipelinePlan buildPlan, List<Type> buildTypes, List<Integer> buildHashChannels)
        {
            this(operatorId, probeTypes, probeJoinChannels, buildPlan, buildTypes, buildHashChannels, "join-" + operatorId);
        }

        private HashJoinSpec withProfileName(String profileName)
        {
            return new HashJoinSpec(operatorId, probeTypes, probeJoinChannels, buildPlan, buildTypes, buildHashChannels, profileName);
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

    private record PipelinePlan(PipelineSource source, List<PipelineStep> steps, String sinkName)
    {
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
            permits FactoryStep, HashJoinStep
    {
        OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support);

        String profileName();
    }

    private record FactoryStep(String profileName, OperatorFactory factory)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support)
        {
            return factory;
        }
    }

    private record HashJoinStep(String profileName, HashJoinSpec spec)
            implements PipelineStep
    {
        @Override
        public OperatorFactory createOperatorFactory(io.trino.operator.TaskContext taskContext, TrinoTpcdsParquetSupport support)
        {
            return support.createHashJoinFactory(taskContext, spec);
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
        if (sqlSupport != null) {
            sqlSupport.close();
            sqlSupport = null;
        }
        currentRootDirectory = null;
        currentSchema = null;
    }

    private TrinoTpcdsParquetSqlSupport support(TpcdsParquetTables tables)
    {
        Path requestedRootDirectory = tables.rootDirectory().toAbsolutePath().normalize();
        String requestedSchema = tables.schema();
        if (sqlSupport == null || !requestedRootDirectory.equals(currentRootDirectory) || !requestedSchema.equals(currentSchema)) {
            close();
            sqlSupport = new TrinoTpcdsParquetSqlSupport(tables);
            currentRootDirectory = requestedRootDirectory;
            currentSchema = requestedSchema;
        }
        return sqlSupport;
    }
}
