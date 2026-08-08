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
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.LimitOperator.LimitOperatorFactory;
import io.trino.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.TopNOperator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
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
import io.trino.type.LikePattern;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public final class TrinoClickBenchSupport
        implements AutoCloseable
{
    private static final ThreadLocal<TrinoOperatorCpuProfile> CURRENT_OPERATOR_CPU_PROFILE = new ThreadLocal<>();
    private static final String TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY = "nitro.clickbench.trino.blockedWaitTimeoutSeconds";
    private static final String TRINO_QUERY_MAX_MEMORY_PROPERTY = "nitro.clickbench.trino.queryMaxMemoryGigabytes";
    private static final int DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_TRINO_QUERY_MAX_MEMORY_GIGABYTES = 8;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", ImmutableList.of());
    private static final TestingAggregationFunction BIGINT_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_AVG = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(BIGINT));
    private static final TestingAggregationFunction INTEGER_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(INTEGER));
    private static final TestingAggregationFunction INTEGER_AVG = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(INTEGER));
    private static final TestingAggregationFunction INTEGER_MIN = FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(INTEGER));
    private static final TestingAggregationFunction INTEGER_MAX = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(INTEGER));
    private static final TestingAggregationFunction VARCHAR_MIN = FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(VARCHAR));

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TrinoClickBenchSupport"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TrinoClickBenchSupport-scheduled"));
    private final OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());
    private final int blockedWaitTimeoutSeconds = blockedWaitTimeoutSeconds();
    private final DataSize queryMaxMemory = queryMaxMemory();

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

    public Path requiredActualHitsPath()
    {
        return TrinoClickBenchPageReader.requiredActualHitsPath();
    }

    public void consumeQuery00(Path input)
    {
        List<String> columns = TrinoClickBenchPageReader.allColumns(input);
        consume(input, columns, List.of(), TrinoClickBenchPageReader.columnTypes(input, columns));
    }

    public MaterializedResult query01(Path input)
    {
        return materialize(
                input,
                List.of("AdvEngineID"),
                List.of(new AggregationOperatorFactory(
                        1,
                        new PlanNodeId("aggregation"),
                        List.of(COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())))),
                List.of(BIGINT));
    }

    public MaterializedResult query03(Path input)
    {
        List<Type> projectedTypes = List.of(BIGINT, BIGINT);
        return materialize(
                input,
                List.of("AdvEngineID", "ResolutionWidth"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, INTEGER),
                                Optional.empty(),
                                List.of(castField(0, INTEGER, BIGINT), castField(1, INTEGER, BIGINT)),
                                projectedTypes),
                        new AggregationOperatorFactory(
                                2,
                                new PlanNodeId("aggregation"),
                                List.of(
                                        BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()),
                                        COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                        BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty())))),
                List.of(BIGINT_SUM.getFinalType(), COUNT.getFinalType(), BIGINT_AVG.getFinalType()));
    }

    public MaterializedResult query07(Path input)
    {
        return materialize(
                input,
                List.of("EventDate"),
                List.of(new AggregationOperatorFactory(
                        1,
                        new PlanNodeId("aggregation"),
                        List.of(
                                INTEGER_MIN.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()),
                                INTEGER_MAX.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty())))),
                List.of(INTEGER_MIN.getFinalType(), INTEGER_MAX.getFinalType()));
    }

    public MaterializedResult query08(Path input)
    {
        List<Type> outputTypes = List.of(INTEGER, BIGINT);
        return materialize(
                input,
                List.of("AdvEngineID"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER),
                                Optional.of(notEqual(0, INTEGER, 0L)),
                                List.of(field(0, INTEGER)),
                                List.of(INTEGER)),
                        new HashAggregationOperatorFactory(
                                2,
                                new PlanNodeId("grouped-aggregation"),
                                List.of(INTEGER),
                                List.of(0),
                                List.of(),
                                Step.SINGLE,
                                List.of(COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                                OptionalInt.empty(),
                                100_000,
                                Optional.of(DataSize.of(16, MEGABYTE)),
                                hashStrategyCompiler,
                                Optional.empty()),
                        topNFactory(3, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query13(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("SearchPhrase"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR),
                                Optional.of(notEqual(0, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)),
                        new HashAggregationOperatorFactory(
                                2,
                                new PlanNodeId("grouped-aggregation"),
                                List.of(VARCHAR),
                                List.of(0),
                                List.of(),
                                Step.SINGLE,
                                List.of(COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                                OptionalInt.empty(),
                                100_000,
                                Optional.of(DataSize.of(16, MEGABYTE)),
                                hashStrategyCompiler,
                                Optional.empty()),
                        topNFactory(3, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query34(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("URL"),
                List.of(
                        new HashAggregationOperatorFactory(
                                1,
                                new PlanNodeId("grouped-aggregation"),
                                List.of(VARCHAR),
                                List.of(0),
                                List.of(),
                                Step.SINGLE,
                                List.of(COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                                OptionalInt.empty(),
                                100_000,
                                Optional.of(DataSize.of(16, MEGABYTE)),
                                hashStrategyCompiler,
                                Optional.empty()),
                        topNFactory(2, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query02(Path input)
    {
        return materialize(
                input,
                List.of("AdvEngineID"),
                List.of(
                        filterAndProjectFactory(1, List.of(INTEGER), Optional.of(notEqual(0, INTEGER, 0L)), List.of(), List.of()),
                        aggregationFactory(2, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                List.of(BIGINT));
    }

    public MaterializedResult query04(Path input)
    {
        return materialize(
                input,
                List.of("UserID"),
                List.of(aggregationFactory(1, BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(0), OptionalInt.empty()))),
                List.of(BIGINT_AVG.getFinalType()));
    }

    public MaterializedResult query05(Path input)
    {
        return sqlShapedCountDistinct(input, "UserID", BIGINT);
    }

    public MaterializedResult query06(Path input)
    {
        return sqlShapedCountDistinct(input, "SearchPhrase", VARCHAR);
    }

    public MaterializedResult query09(Path input)
    {
        return sqlShapedGroupedCountDistinct(input, "RegionID", INTEGER, "UserID", BIGINT);
    }

    public MaterializedResult query10(Path input)
    {
        List<Type> projectedTypes = List.of(INTEGER, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(INTEGER, BIGINT_SUM.getFinalType(), BIGINT, BIGINT_AVG.getFinalType(), BIGINT);
        return materialize(
                input,
                List.of("RegionID", "AdvEngineID", "ResolutionWidth", "UserID"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, INTEGER, INTEGER, BIGINT),
                                Optional.empty(),
                                List.of(field(0, INTEGER), castField(1, INTEGER, BIGINT), castField(2, INTEGER, BIGINT), field(3, BIGINT)),
                                projectedTypes),
                        markDistinctFactory(2, projectedTypes, List.of(0, 3)),
                        hashAggregationFactory(
                                3,
                                List.of(INTEGER),
                                List.of(0),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.of(4))),
                        topNFactory(4, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query11(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("MobilePhoneModel", "UserID"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR, BIGINT),
                                Optional.of(notEqual(0, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, VARCHAR), field(1, BIGINT)),
                                List.of(VARCHAR, BIGINT)),
                        markDistinctFactory(2, List.of(VARCHAR, BIGINT), List.of(0, 1)),
                        hashAggregationFactory(
                                3,
                                List.of(VARCHAR),
                                List.of(0),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.of(2))),
                        topNFactory(4, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query12(Path input)
    {
        List<Type> outputTypes = List.of(INTEGER, VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("MobilePhone", "MobilePhoneModel", "UserID"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, VARCHAR, BIGINT),
                                Optional.of(notEqual(1, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, INTEGER), field(1, VARCHAR), field(2, BIGINT)),
                                List.of(INTEGER, VARCHAR, BIGINT)),
                        markDistinctFactory(2, List.of(INTEGER, VARCHAR, BIGINT), List.of(0, 1, 2)),
                        hashAggregationFactory(
                                3,
                                List.of(INTEGER, VARCHAR),
                                List.of(0, 1),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.of(3))),
                        topNFactory(4, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query14(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("SearchPhrase", "UserID"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR, BIGINT),
                                Optional.of(notEqual(0, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, VARCHAR), field(1, BIGINT)),
                                List.of(VARCHAR, BIGINT)),
                        markDistinctFactory(2, List.of(VARCHAR, BIGINT), List.of(0, 1)),
                        hashAggregationFactory(
                                3,
                                List.of(VARCHAR),
                                List.of(0),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.of(2))),
                        topNFactory(4, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query15(Path input)
    {
        List<Type> outputTypes = List.of(INTEGER, VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("SearchEngineID", "SearchPhrase"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, VARCHAR),
                                Optional.of(notEqual(1, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, INTEGER), field(1, VARCHAR)),
                                List.of(INTEGER, VARCHAR)),
                        hashAggregationFactory(
                                2,
                                List.of(INTEGER, VARCHAR),
                                List.of(0, 1),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query16(Path input)
    {
        List<Type> outputTypes = List.of(BIGINT, BIGINT);
        return materialize(
                input,
                List.of("UserID"),
                List.of(
                        hashAggregationFactory(1, List.of(BIGINT), List.of(0), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(2, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query17(Path input)
    {
        List<Type> outputTypes = List.of(BIGINT, VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("UserID", "SearchPhrase"),
                List.of(
                        hashAggregationFactory(1, List.of(BIGINT, VARCHAR), List.of(0, 1), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(2, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query18(Path input)
    {
        List<Type> outputTypes = List.of(BIGINT, VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("UserID", "SearchPhrase"),
                List.of(
                        hashAggregationFactory(1, List.of(BIGINT, VARCHAR), List.of(0, 1), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        limitFactory(2, 10)),
                outputTypes);
    }

    public MaterializedResult query19(Path input)
    {
        List<Type> projectedTypes = List.of(BIGINT, BIGINT, VARCHAR);
        List<Type> outputTypes = List.of(BIGINT, BIGINT, VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("UserID", "EventTime", "SearchPhrase"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(BIGINT, BIGINT, VARCHAR),
                                Optional.empty(),
                                List.of(field(0, BIGINT), minuteOfHour(field(1, BIGINT)), field(2, VARCHAR)),
                                projectedTypes),
                        hashAggregationFactory(2, projectedTypes, List.of(0, 1, 2), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(3), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query20(Path input)
    {
        return materialize(
                input,
                List.of("UserID"),
                List.of(filterAndProjectFactory(
                        1,
                        List.of(BIGINT),
                        Optional.of(equal(0, BIGINT, 435_090_932_899_640_449L)),
                        List.of(field(0, BIGINT)),
                        List.of(BIGINT))),
                List.of(BIGINT));
    }

    public MaterializedResult query21(Path input)
    {
        return materialize(
                input,
                List.of("URL"),
                List.of(
                        filterAndProjectFactory(1, List.of(VARCHAR), Optional.of(like(0, "%google%")), List.of(), List.of()),
                        aggregationFactory(2, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                List.of(BIGINT));
    }

    public MaterializedResult query22(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR_MIN.getFinalType(), BIGINT);
        return materialize(
                input,
                List.of("SearchPhrase", "URL"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR, VARCHAR),
                                Optional.of(and(notEqual(0, VARCHAR, Slices.utf8Slice("")), like(1, "%google%"))),
                                List.of(field(0, VARCHAR), field(1, VARCHAR)),
                                List.of(VARCHAR, VARCHAR)),
                        hashAggregationFactory(
                                2,
                                List.of(VARCHAR),
                                List.of(0),
                                VARCHAR_MIN.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query23(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, VARCHAR_MIN.getFinalType(), VARCHAR_MIN.getFinalType(), BIGINT, BIGINT);
        return materialize(
                input,
                List.of("SearchPhrase", "URL", "Title", "UserID"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT),
                                Optional.of(and(
                                        like(2, "%Google%"),
                                        notLike(1, "%.google.%"),
                                        notEqual(0, VARCHAR, Slices.utf8Slice("")))),
                                List.of(field(0, VARCHAR), field(1, VARCHAR), field(2, VARCHAR), field(3, BIGINT)),
                                List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT)),
                        markDistinctFactory(2, List.of(VARCHAR, VARCHAR, VARCHAR, BIGINT), List.of(0, 3)),
                        hashAggregationFactory(
                                3,
                                List.of(VARCHAR),
                                List.of(0),
                                VARCHAR_MIN.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                VARCHAR_MIN.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.of(4))),
                        topNFactory(4, outputTypes, 10, List.of(3), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query24(Path input)
    {
        List<String> columns = TrinoClickBenchPageReader.allColumns(input);
        List<Type> types = TrinoClickBenchPageReader.columnTypes(input, columns);
        int eventTimeIndex = columns.indexOf("EventTime");
        int watchIdIndex = columns.indexOf("WatchID");
        int clientIpIndex = columns.indexOf("ClientIP");
        int urlIndex = columns.indexOf("URL");
        int searchPhraseIndex = columns.indexOf("SearchPhrase");
        List<Integer> sortChannels = List.of(eventTimeIndex, watchIdIndex, clientIpIndex, urlIndex, searchPhraseIndex);
        List<SortOrder> sortOrders = List.of(ascending(), ascending(), ascending(), ascending(), ascending());

        List<Page> partialRows = new ArrayList<>();
        for (Path split : TrinoClickBenchPageReader.resolveFiles(input)) {
            partialRows.addAll(executePipeline(
                    "q24.partial",
                    driverContext -> new ParquetPageSourceOperator(
                            driverContext.addOperatorContext(0, new PlanNodeId("partial-source"), ParquetPageSourceOperator.class.getSimpleName()),
                            split,
                            columns),
                    List.of(
                            filterAndProjectFactory(1, types, Optional.of(like(urlIndex, "%google%")), identityProjections(types), types),
                            topNFactory(2, types, 10, sortChannels, sortOrders)),
                    true).pages());
        }

        PipelineOutput result = executePipeline(
                "q24.final",
                driverContext -> new PagesSourceOperator(
                        driverContext.addOperatorContext(0, new PlanNodeId("topn-exchange"), PagesSourceOperator.class.getSimpleName()),
                        partialRows),
                List.of(topNFactory(1, types, 10, sortChannels, sortOrders)),
                true);
        partialRows.clear();

        MaterializedResult.Builder materialized = MaterializedResult.resultBuilder(result.driverContext().getSession(), types);
        result.pages().forEach(materialized::page);
        return materialized.build();
    }

    public MaterializedResult query25(Path input)
    {
        return materialize(
                input,
                List.of("EventTime", "SearchPhrase"),
                List.of(
                        filterAndProjectFactory(1, List.of(BIGINT, VARCHAR), Optional.of(notEqual(1, VARCHAR, Slices.utf8Slice(""))), identityProjections(List.of(BIGINT, VARCHAR)), List.of(BIGINT, VARCHAR)),
                        topNFactory(2, List.of(BIGINT, VARCHAR), 10, List.of(0), List.of(ascending())),
                        filterAndProjectFactory(3, List.of(BIGINT, VARCHAR), Optional.empty(), List.of(field(1, VARCHAR)), List.of(VARCHAR))),
                List.of(VARCHAR));
    }

    public MaterializedResult query26(Path input)
    {
        return materialize(
                input,
                List.of("SearchPhrase"),
                List.of(
                        filterAndProjectFactory(1, List.of(VARCHAR), Optional.of(notEqual(0, VARCHAR, Slices.utf8Slice(""))), List.of(field(0, VARCHAR)), List.of(VARCHAR)),
                        topNFactory(2, List.of(VARCHAR), 10, List.of(0), List.of(ascending()))),
                List.of(VARCHAR));
    }

    public MaterializedResult query27(Path input)
    {
        return materialize(
                input,
                List.of("EventTime", "SearchPhrase"),
                List.of(
                        filterAndProjectFactory(1, List.of(BIGINT, VARCHAR), Optional.of(notEqual(1, VARCHAR, Slices.utf8Slice(""))), identityProjections(List.of(BIGINT, VARCHAR)), List.of(BIGINT, VARCHAR)),
                        topNFactory(2, List.of(BIGINT, VARCHAR), 10, List.of(0, 1), List.of(ascending(), ascending())),
                        filterAndProjectFactory(3, List.of(BIGINT, VARCHAR), Optional.empty(), List.of(field(1, VARCHAR)), List.of(VARCHAR))),
                List.of(VARCHAR));
    }

    public MaterializedResult query28(Path input)
    {
        List<Type> outputTypes = List.of(INTEGER, DOUBLE, BIGINT);
        return materialize(
                input,
                List.of("CounterID", "URL"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, VARCHAR),
                                Optional.of(notEqual(1, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, INTEGER), length(field(1, VARCHAR))),
                                List.of(INTEGER, BIGINT)),
                        hashAggregationFactory(
                                2,
                                List.of(INTEGER),
                                List.of(0),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        filterAndProjectFactory(
                                3,
                                outputTypes,
                                Optional.of(greaterThan(2, BIGINT, 100_000L)),
                                identityProjections(outputTypes),
                                outputTypes),
                        topNFactory(4, outputTypes, 25, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query29(Path input)
    {
        List<Type> projectedTypes = List.of(VARCHAR, BIGINT, VARCHAR);
        List<Type> outputTypes = List.of(VARCHAR, DOUBLE, BIGINT, VARCHAR);
        return materialize(
                input,
                List.of("Referer"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR),
                                Optional.of(notEqual(0, VARCHAR, Slices.utf8Slice(""))),
                                List.of(regexpReplace(field(0, VARCHAR), "^https?://(?:www\\.)?([^/]+)/.*$", "$1"), length(field(0, VARCHAR)), field(0, VARCHAR)),
                                projectedTypes),
                        hashAggregationFactory(
                                2,
                                List.of(VARCHAR),
                                List.of(0),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(1), OptionalInt.empty()),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                VARCHAR_MIN.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty())),
                        filterAndProjectFactory(
                                3,
                                outputTypes,
                                Optional.of(greaterThan(2, BIGINT, 100_000L)),
                                identityProjections(outputTypes),
                                outputTypes),
                        topNFactory(4, outputTypes, 25, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query30(Path input)
    {
        List<RowExpression> projections = new ArrayList<>();
        List<Type> projectedTypes = new ArrayList<>();
        List<io.trino.operator.aggregation.AggregatorFactory> partialAggregators = new ArrayList<>();
        List<io.trino.operator.aggregation.AggregatorFactory> finalAggregators = new ArrayList<>();
        for (int offset = 0; offset < 90; offset++) {
            projections.add(add(castField(0, INTEGER, BIGINT), constant((long) offset, BIGINT), BIGINT));
            projectedTypes.add(BIGINT);
            partialAggregators.add(BIGINT_SUM.createAggregatorFactory(Step.PARTIAL, List.of(offset), OptionalInt.empty()));
            finalAggregators.add(BIGINT_SUM.createAggregatorFactory(Step.FINAL, List.of(offset), OptionalInt.empty()));
        }

        List<Page> partialRows = new ArrayList<>();
        List<OperatorFactory> partialFactories = List.of(
                filterAndProjectFactory(1, List.of(INTEGER), Optional.empty(), projections, projectedTypes),
                new AggregationOperatorFactory(2, new PlanNodeId("partial-aggregation"), partialAggregators));
        for (Path split : TrinoClickBenchPageReader.resolveFiles(input)) {
            partialRows.addAll(executePipeline(
                    "q30.partial",
                    driverContext -> new ParquetPageSourceOperator(
                            driverContext.addOperatorContext(0, new PlanNodeId("partial-source"), ParquetPageSourceOperator.class.getSimpleName()),
                            split,
                            List.of("ResolutionWidth")),
                    partialFactories,
                    true,
                    false).pages());
        }
        partialFactories.forEach(OperatorFactory::noMoreOperators);

        PipelineOutput result = executePipeline(
                "q30.final",
                driverContext -> new PagesSourceOperator(
                        driverContext.addOperatorContext(0, new PlanNodeId("aggregation-exchange"), PagesSourceOperator.class.getSimpleName()),
                        partialRows),
                List.of(new AggregationOperatorFactory(1, new PlanNodeId("final-aggregation"), finalAggregators)),
                true);
        partialRows.clear();

        MaterializedResult.Builder materialized = MaterializedResult.resultBuilder(result.driverContext().getSession(), projectedTypes);
        result.pages().forEach(materialized::page);
        return materialized.build();
    }

    public MaterializedResult query31(Path input)
    {
        List<Type> projectedTypes = List.of(INTEGER, INTEGER, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(INTEGER, INTEGER, BIGINT, BIGINT_SUM.getFinalType(), BIGINT_AVG.getFinalType());
        return materialize(
                input,
                List.of("SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR),
                                Optional.of(notEqual(4, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, INTEGER), field(1, INTEGER), castField(2, INTEGER, BIGINT), castField(3, INTEGER, BIGINT)),
                                projectedTypes),
                        hashAggregationFactory(
                                2,
                                List.of(INTEGER, INTEGER),
                                List.of(0, 1),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query32(Path input)
    {
        List<Type> projectedTypes = List.of(BIGINT, INTEGER, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(BIGINT, INTEGER, BIGINT, BIGINT_SUM.getFinalType(), BIGINT_AVG.getFinalType());
        return materialize(
                input,
                List.of("WatchID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(BIGINT, INTEGER, INTEGER, INTEGER, VARCHAR),
                                Optional.of(notEqual(4, VARCHAR, Slices.utf8Slice(""))),
                                List.of(field(0, BIGINT), field(1, INTEGER), castField(2, INTEGER, BIGINT), castField(3, INTEGER, BIGINT)),
                                projectedTypes),
                        hashAggregationFactory(
                                2,
                                List.of(BIGINT, INTEGER),
                                List.of(0, 1),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query33(Path input)
    {
        List<Type> projectedTypes = List.of(BIGINT, INTEGER, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(BIGINT, INTEGER, BIGINT, BIGINT_SUM.getFinalType(), BIGINT_AVG.getFinalType());
        return materialize(
                input,
                List.of("WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(BIGINT, INTEGER, INTEGER, INTEGER),
                                Optional.empty(),
                                List.of(field(0, BIGINT), field(1, INTEGER), castField(2, INTEGER, BIGINT), castField(3, INTEGER, BIGINT)),
                                projectedTypes),
                        hashAggregationFactory(
                                2,
                                List.of(BIGINT, INTEGER),
                                List.of(0, 1),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()),
                                BIGINT_SUM.createAggregatorFactory(Step.SINGLE, List.of(2), OptionalInt.empty()),
                                BIGINT_AVG.createAggregatorFactory(Step.SINGLE, List.of(3), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query35(Path input)
    {
        List<Type> projectedTypes = List.of(BIGINT, VARCHAR);
        List<Type> outputTypes = List.of(BIGINT, VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("URL"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR),
                                Optional.empty(),
                                List.of(constant(1L, BIGINT), field(0, VARCHAR)),
                                projectedTypes),
                        hashAggregationFactory(2, projectedTypes, List.of(0, 1), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query36(Path input)
    {
        List<Type> projectedTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT);
        List<Type> outputTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT);
        return materialize(
                input,
                List.of("ClientIP"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER),
                                Optional.empty(),
                                List.of(
                                        castField(0, INTEGER, BIGINT),
                                        subtract(castField(0, INTEGER, BIGINT), constant(1L, BIGINT), BIGINT),
                                        subtract(castField(0, INTEGER, BIGINT), constant(2L, BIGINT), BIGINT),
                                        subtract(castField(0, INTEGER, BIGINT), constant(3L, BIGINT), BIGINT)),
                                projectedTypes),
                        hashAggregationFactory(2, projectedTypes, List.of(0, 1, 2, 3), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(4), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query37(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("URL", "CounterID", "EventDate", "DontCountHits", "IsRefresh"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER),
                                Optional.of(and(
                                        equal(1, INTEGER, 62L),
                                        equal(3, INTEGER, 0L),
                                        equal(4, INTEGER, 0L),
                                        dateRange(input, 2, LocalDate.of(2013, 7, 1), LocalDate.of(2013, 8, 1)),
                                        notEqual(0, VARCHAR, Slices.utf8Slice("")))),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)),
                        hashAggregationFactory(2, List.of(VARCHAR), List.of(0), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query38(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, BIGINT);
        return materialize(
                input,
                List.of("Title", "CounterID", "EventDate", "DontCountHits", "IsRefresh"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER),
                                Optional.of(and(
                                        equal(1, INTEGER, 62L),
                                        equal(3, INTEGER, 0L),
                                        equal(4, INTEGER, 0L),
                                        dateRange(input, 2, LocalDate.of(2013, 7, 1), LocalDate.of(2013, 8, 1)),
                                        notEqual(0, VARCHAR, Slices.utf8Slice("")))),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)),
                        hashAggregationFactory(2, List.of(VARCHAR), List.of(0), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes);
    }

    public MaterializedResult query39(Path input)
    {
        List<Type> outputTypes = List.of(VARCHAR, BIGINT);
        return sliceResult(materialize(
                input,
                List.of("URL", "CounterID", "EventDate", "IsRefresh", "IsLink", "IsDownload"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER),
                                Optional.of(and(
                                        equal(1, INTEGER, 62L),
                                        equal(3, INTEGER, 0L),
                                        notEqual(4, INTEGER, 0L),
                                        equal(5, INTEGER, 0L),
                                        dateRange(input, 2, LocalDate.of(2013, 7, 1), LocalDate.of(2013, 8, 1)))),
                                List.of(field(0, VARCHAR)),
                                List.of(VARCHAR)),
                        hashAggregationFactory(2, List.of(VARCHAR), List.of(0), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 1_010, List.of(1), List.of(DESC_NULLS_LAST))),
                outputTypes), outputTypes, 1_000, 10);
    }

    public MaterializedResult query40(Path input)
    {
        List<Type> projectedTypes = List.of(INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR);
        List<Type> outputTypes = List.of(INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, BIGINT);
        return sliceResult(materialize(
                input,
                List.of("TraficSourceID", "SearchEngineID", "AdvEngineID", "Referer", "URL", "CounterID", "EventDate", "IsRefresh"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER),
                                Optional.of(and(
                                        equal(5, INTEGER, 62L),
                                        equal(7, INTEGER, 0L),
                                        dateRange(input, 6, LocalDate.of(2013, 7, 1), LocalDate.of(2013, 8, 1)))),
                                List.of(
                                        field(0, INTEGER),
                                        field(1, INTEGER),
                                        field(2, INTEGER),
                                        trafficSourceCase(field(1, INTEGER), field(2, INTEGER), field(3, VARCHAR)),
                                        field(4, VARCHAR)),
                                projectedTypes),
                        hashAggregationFactory(2, projectedTypes, List.of(0, 1, 2, 3, 4), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 1_010, List.of(5), List.of(DESC_NULLS_LAST))),
                outputTypes), outputTypes, 1_000, 10);
    }

    public MaterializedResult query41(Path input)
    {
        List<Type> outputTypes = List.of(BIGINT, INTEGER, BIGINT);
        return sliceResult(materialize(
                input,
                List.of("URLHash", "EventDate", "CounterID", "IsRefresh", "TraficSourceID", "RefererHash"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(BIGINT, INTEGER, INTEGER, INTEGER, INTEGER, BIGINT),
                                Optional.of(and(
                                        equal(2, INTEGER, 62L),
                                        equal(3, INTEGER, 0L),
                                        or(equal(4, INTEGER, -1L), equal(4, INTEGER, 6L)),
                                        equal(5, BIGINT, 3_594_120_000_172_545_465L),
                                        dateRange(input, 1, LocalDate.of(2013, 7, 1), LocalDate.of(2013, 8, 1)))),
                                List.of(field(0, BIGINT), field(1, INTEGER)),
                                List.of(BIGINT, INTEGER)),
                        hashAggregationFactory(2, List.of(BIGINT, INTEGER), List.of(0, 1), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 110, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes), outputTypes, 100, 10);
    }

    public MaterializedResult query42(Path input)
    {
        List<Type> outputTypes = List.of(INTEGER, INTEGER, BIGINT);
        return sliceResult(materialize(
                input,
                List.of("WindowClientWidth", "WindowClientHeight", "CounterID", "EventDate", "IsRefresh", "DontCountHits", "URLHash"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, BIGINT),
                                Optional.of(and(
                                        equal(2, INTEGER, 62L),
                                        equal(4, INTEGER, 0L),
                                        equal(5, INTEGER, 0L),
                                        equal(6, BIGINT, 2_868_770_270_353_813_622L),
                                        dateRange(input, 3, LocalDate.of(2013, 7, 1), LocalDate.of(2013, 8, 1)))),
                                List.of(field(0, INTEGER), field(1, INTEGER)),
                                List.of(INTEGER, INTEGER)),
                        hashAggregationFactory(2, List.of(INTEGER, INTEGER), List.of(0, 1), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 10_010, List.of(2), List.of(DESC_NULLS_LAST))),
                outputTypes), outputTypes, 10_000, 10);
    }

    public MaterializedResult query43(Path input)
    {
        List<Type> projectedTypes = List.of(BIGINT);
        List<Type> outputTypes = List.of(BIGINT, BIGINT);
        return sliceResult(materialize(
                input,
                List.of("EventTime", "CounterID", "EventDate", "DontCountHits", "IsRefresh"),
                List.of(
                        filterAndProjectFactory(
                                1,
                                List.of(BIGINT, INTEGER, INTEGER, INTEGER, INTEGER),
                                Optional.of(and(
                                        equal(1, INTEGER, 62L),
                                        equal(3, INTEGER, 0L),
                                        equal(4, INTEGER, 0L),
                                        dateRange(input, 2, LocalDate.of(2013, 7, 14), LocalDate.of(2013, 7, 16)))),
                                List.of(minuteBucket(field(0, BIGINT))),
                                projectedTypes),
                        hashAggregationFactory(2, projectedTypes, List.of(0), COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, outputTypes, 1_010, List.of(0), List.of(ascending()))),
                outputTypes), outputTypes, 1_000, 10);
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private void consume(Path input, List<String> columns, List<OperatorFactory> factories, List<Type> outputTypes)
    {
        execute(input, columns, factories, outputTypes, false);
    }

    private MaterializedResult materialize(Path input, List<String> columns, List<OperatorFactory> factories, List<Type> outputTypes)
    {
        return execute(input, columns, factories, outputTypes, true);
    }

    private MaterializedResult execute(Path input, List<String> columns, List<OperatorFactory> factories, List<Type> outputTypes, boolean collectOutput)
    {
        PipelineOutput output = executePipeline(
                driverContext -> new ParquetPageSourceOperator(
                        driverContext.addOperatorContext(0, new PlanNodeId("source"), ParquetPageSourceOperator.class.getSimpleName()),
                        input,
                        columns),
                factories,
                collectOutput);

        MaterializedResult.Builder result = MaterializedResult.resultBuilder(output.driverContext().getSession(), outputTypes);
        for (Page page : output.pages()) {
            result.page(page);
        }
        return result.build();
    }

    private PipelineOutput executePipeline(SourceFactory sourceFactory, List<OperatorFactory> factories, boolean collectOutput)
    {
        return executePipeline("pipeline", sourceFactory, factories, collectOutput);
    }

    private PipelineOutput executePipeline(String profilePrefix, SourceFactory sourceFactory, List<OperatorFactory> factories, boolean collectOutput)
    {
        return executePipeline(profilePrefix, sourceFactory, factories, collectOutput, true);
    }

    private PipelineOutput executePipeline(
            String profilePrefix,
            SourceFactory sourceFactory,
            List<OperatorFactory> factories,
            boolean collectOutput,
            boolean closeFactories)
    {
        List<Page> outputPages = new ArrayList<>();
        DriverContext driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TestingSession.testSessionBuilder().build())
                .setQueryMaxMemory(queryMaxMemory)
                .setMemoryPoolSize(queryMaxMemory)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        List<Operator> operators = new ArrayList<>();
        Operator source = sourceFactory.create(driverContext);
        operators.add(profiled(profilePrefix + ".source", source));

        int operatorIndex = 1;
        for (OperatorFactory factory : factories) {
            Operator operator = factory.createOperator(driverContext);
            operators.add(profiled(profilePrefix + ".operator-" + operatorIndex + "." + operator.getClass().getSimpleName(), operator));
            if (closeFactories) {
                factory.noMoreOperators();
            }
            operatorIndex++;
        }

        operators.add(profiled(profilePrefix + ".sink", new PageConsumerOperator(
                driverContext.addOperatorContext(1000, new PlanNodeId("sink"), PageConsumerOperator.class.getSimpleName()),
                page -> {
                    if (collectOutput) {
                        outputPages.add(page);
                    }
                },
                java.util.function.Function.identity())));

        try (Driver driver = Driver.createDriver(driverContext, operators)) {
            while (!driver.isFinished()) {
                var blocked = driver.processUntilBlocked();
                if (!blocked.isDone()) {
                    waitForBlocked(driver, operators, blocked);
                }
            }
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to execute Trino ClickBench pipeline", exception);
        }
        return new PipelineOutput(driverContext, outputPages);
    }

    /** Executes the same local/final aggregation topology selected for distributed single-DISTINCT SQL. */
    private MaterializedResult sqlShapedCountDistinct(Path input, String column, Type type)
    {
        List<Page> partialKeys = new ArrayList<>();
        for (Path split : TrinoClickBenchPageReader.resolveFiles(input)) {
            partialKeys.addAll(executePipeline(
                    driverContext -> new ParquetPageSourceOperator(
                            driverContext.addOperatorContext(0, new PlanNodeId("partial-source"), ParquetPageSourceOperator.class.getSimpleName()),
                            split,
                            List.of(column)),
                    List.of(hashAggregationFactory(1, List.of(type), List.of(0))),
                    true).pages());
        }

        List<Page> finalKeys = executePipeline(
                driverContext -> new PagesSourceOperator(
                        driverContext.addOperatorContext(0, new PlanNodeId("distinct-exchange"), PagesSourceOperator.class.getSimpleName()),
                        partialKeys),
                List.of(hashAggregationFactory(1, List.of(type), List.of(0))),
                true).pages();
        PipelineOutput count = executePipeline(
                driverContext -> new PagesSourceOperator(
                        driverContext.addOperatorContext(0, new PlanNodeId("count-exchange"), PagesSourceOperator.class.getSimpleName()),
                        finalKeys),
                List.of(aggregationFactory(1, COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty()))),
                true);

        MaterializedResult.Builder result = MaterializedResult.resultBuilder(count.driverContext().getSession(), List.of(BIGINT));
        count.pages().forEach(result::page);
        return result.build();
    }

    /** Executes the split-local/final pair grouping and grouped count selected for distributed grouped DISTINCT. */
    private MaterializedResult sqlShapedGroupedCountDistinct(
            Path input,
            String groupColumn,
            Type groupType,
            String distinctColumn,
            Type distinctType)
    {
        List<Type> pairTypes = List.of(groupType, distinctType);
        List<Page> partialKeys = new ArrayList<>();
        for (Path split : TrinoClickBenchPageReader.resolveFiles(input)) {
            partialKeys.addAll(executePipeline("q09.partial",
                    driverContext -> new ParquetPageSourceOperator(
                            driverContext.addOperatorContext(0, new PlanNodeId("partial-source"), ParquetPageSourceOperator.class.getSimpleName()),
                            split,
                            List.of(groupColumn, distinctColumn)),
                    List.of(hashAggregationFactory(1, pairTypes, List.of(0, 1))),
                    true).pages());
        }

        PipelineOutput count = executePipeline("q09.final",
                driverContext -> new PagesSourceOperator(
                        driverContext.addOperatorContext(0, new PlanNodeId("distinct-exchange"), PagesSourceOperator.class.getSimpleName()),
                        partialKeys),
                List.of(
                        hashAggregationFactory(1, pairTypes, List.of(0, 1)),
                        hashAggregationFactory(
                                2,
                                List.of(groupType),
                                List.of(0),
                                COUNT.createAggregatorFactory(Step.SINGLE, List.of(), OptionalInt.empty())),
                        topNFactory(3, List.of(groupType, BIGINT), 10, List.of(1), List.of(DESC_NULLS_LAST))),
                true);
        partialKeys.clear();

        List<Type> outputTypes = List.of(groupType, BIGINT);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(count.driverContext().getSession(), outputTypes);
        count.pages().forEach(result::page);
        return result.build();
    }

    @FunctionalInterface
    private interface SourceFactory
    {
        Operator create(DriverContext driverContext);
    }

    private record PipelineOutput(DriverContext driverContext, List<Page> pages) {}

    private static Operator profiled(String name, Operator operator)
    {
        TrinoOperatorCpuProfile profile = CURRENT_OPERATOR_CPU_PROFILE.get();
        return profile == null ? operator : profile.wrap(name, operator);
    }

    private static final class ParquetPageSourceOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private final TrinoClickBenchPageReader reader;
        private boolean finished;

        private ParquetPageSourceOperator(OperatorContext operatorContext, Path input, List<String> columns)
        {
            this.operatorContext = operatorContext;
            this.reader = new TrinoClickBenchPageReader(input, columns);
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

    private static final class PagesSourceOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private final List<Page> pages;
        private int index;
        private boolean finished;

        private PagesSourceOperator(OperatorContext operatorContext, List<Page> pages)
        {
            this.operatorContext = operatorContext;
            this.pages = List.copyOf(pages);
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
            return finished || index == pages.size();
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
            if (isFinished()) {
                return null;
            }
            Page page = pages.get(index++);
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
            return page;
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

    private static OperatorFactory limitFactory(int operatorId, long limit)
    {
        return new LimitOperatorFactory(operatorId, new PlanNodeId("limit-" + operatorId), limit);
    }

    private OperatorFactory markDistinctFactory(int operatorId, List<Type> types, List<Integer> distinctChannels)
    {
        return new MarkDistinctOperatorFactory(operatorId, new PlanNodeId("mark-distinct-" + operatorId), types, distinctChannels, hashStrategyCompiler);
    }

    private OperatorFactory filterAndProjectFactory(int operatorId, List<Type> inputTypes, Optional<RowExpression> filter, List<RowExpression> projections, List<Type> outputTypes)
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

    private MaterializedResult sliceResult(MaterializedResult source, List<Type> outputTypes, int offset, int limit)
    {
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(TestingSession.testSessionBuilder().build(), outputTypes);
        List<io.trino.testing.MaterializedRow> rows = source.getMaterializedRows();
        int end = Math.min(rows.size(), offset + limit);
        for (int index = offset; index < end; index++) {
            result.row(rows.get(index).getFields());
        }
        return result.build();
    }

    private static RowExpression castField(int inputChannel, Type fromType, Type toType)
    {
        return new CallExpression(FUNCTION_RESOLUTION.getCoercion(fromType, toType), List.of(field(inputChannel, fromType)));
    }

    private static RowExpression equal(int inputChannel, Type type, Object constantValue)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(field(inputChannel, type), constant(constantValue, type)));
    }

    private static RowExpression greaterThan(int inputChannel, Type type, Object constantValue)
    {
        return lessThan(constant(constantValue, type), field(inputChannel, type), type);
    }

    private static RowExpression notEqual(int inputChannel, Type type, Object constantValue)
    {
        return not(equal(inputChannel, type, constantValue));
    }

    private static RowExpression add(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression subtract(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression divide(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression modulus(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.MODULUS, List.of(type, type)), List.of(left, right));
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

    private static RowExpression not(RowExpression expression)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)), List.of(expression));
    }

    private static RowExpression like(int inputChannel, String pattern)
    {
        return like(field(inputChannel, VARCHAR), pattern);
    }

    private static RowExpression like(RowExpression expression, String pattern)
    {
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                List.of(expression, constant(LikePattern.compile(pattern, Optional.empty()), LIKE_PATTERN)));
    }

    private static RowExpression notLike(int inputChannel, String pattern)
    {
        return not(like(inputChannel, pattern));
    }

    private static RowExpression length(RowExpression expression)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveFunction("length", fromTypes(VARCHAR)), List.of(expression));
    }

    private static RowExpression regexpReplace(RowExpression expression, String pattern, String replacement)
    {
        RowExpression compiledPattern = new CallExpression(
                FUNCTION_RESOLUTION.getCoercion(VARCHAR, JONI_REGEXP),
                List.of(constant(Slices.utf8Slice(pattern), VARCHAR)));
        return new CallExpression(
                FUNCTION_RESOLUTION.resolveFunction("regexp_replace", fromTypes(VARCHAR, JONI_REGEXP, VARCHAR)),
                List.of(expression, compiledPattern, constant(Slices.utf8Slice(replacement), VARCHAR)));
    }

    private static RowExpression minuteOfHour(RowExpression eventTime)
    {
        RowExpression sixty = constant(60L, BIGINT);
        return modulus(divide(eventTime, sixty, BIGINT), sixty, BIGINT);
    }

    private static RowExpression minuteBucket(RowExpression eventTime)
    {
        RowExpression sixty = constant(60L, BIGINT);
        return multiply(divide(eventTime, sixty, BIGINT), sixty, BIGINT);
    }

    private static RowExpression multiply(RowExpression left, RowExpression right, Type type)
    {
        return new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(type, type)), List.of(left, right));
    }

    private static RowExpression trafficSourceCase(RowExpression searchEngineId, RowExpression advEngineId, RowExpression referer)
    {
        RowExpression bothZero = and(
                new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(INTEGER, INTEGER)), List.of(searchEngineId, constant(0L, INTEGER))),
                new CallExpression(FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(INTEGER, INTEGER)), List.of(advEngineId, constant(0L, INTEGER))));
        return new SpecialForm(SpecialForm.Form.IF, VARCHAR, List.of(bothZero, referer, constant(Slices.utf8Slice(""), VARCHAR)), List.of());
    }

    private static SortOrder ascending()
    {
        return SortOrder.ASC_NULLS_LAST;
    }

    private static RowExpression dateRange(Path file, int inputChannel, LocalDate inclusiveLowerBound, LocalDate exclusiveUpperBound)
    {
        int lowerBound = eventDateLiteral(file, inclusiveLowerBound);
        int upperBound = eventDateLiteral(file, exclusiveUpperBound);
        return and(
                not(lessThan(field(inputChannel, INTEGER), constant((long) lowerBound, INTEGER), INTEGER)),
                lessThan(field(inputChannel, INTEGER), constant((long) upperBound, INTEGER), INTEGER));
    }

    private static int eventDateLiteral(Path file, LocalDate date)
    {
        if (eventDateUsesEpochDays(file)) {
            return toIntExact(date.toEpochDay());
        }
        return (date.getYear() * 10_000) + (date.getMonthValue() * 100) + date.getDayOfMonth();
    }

    private static boolean eventDateUsesEpochDays(Path file)
    {
        Path schemaFile = TrinoClickBenchPageReader.resolveFiles(file).getFirst();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(schemaFile))) {
            var field = reader.getFooter().getFileMetaData().getSchema().getType("EventDate").asPrimitiveType();
            return field.getLogicalTypeAnnotation() instanceof org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation logicalType
                    && !logicalType.isSigned()
                    && logicalType.getBitWidth() == 16;
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect ClickBench EventDate encoding for " + schemaFile, exception);
        }
    }

    private static Type inferType(String columnName)
    {
        return switch (columnName) {
            case "AdvEngineID", "ResolutionWidth", "EventDate", "RegionID", "MobilePhone", "CounterID", "ClientIP", "IsRefresh", "TraficSourceID", "SearchEngineID", "WindowClientWidth", "WindowClientHeight", "IsLink", "IsDownload", "DontCountHits" -> INTEGER;
            case "UserID", "WatchID", "EventTime", "RefererHash", "URLHash" -> BIGINT;
            default -> VARCHAR;
        };
    }

    private static ThreadFactory daemonThreadsNamed(String baseName)
    {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, baseName + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static DataSize queryMaxMemory()
    {
        String configured = System.getProperty(TRINO_QUERY_MAX_MEMORY_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return DataSize.of(DEFAULT_TRINO_QUERY_MAX_MEMORY_GIGABYTES, io.airlift.units.DataSize.Unit.GIGABYTE);
        }

        int gigabytes = Integer.parseInt(configured);
        if (gigabytes <= 0) {
            throw new IllegalArgumentException(TRINO_QUERY_MAX_MEMORY_PROPERTY + " must be positive");
        }
        return DataSize.of(gigabytes, io.airlift.units.DataSize.Unit.GIGABYTE);
    }

    private static int blockedWaitTimeoutSeconds()
    {
        String configured = System.getProperty(TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS;
        }

        int seconds = Integer.parseInt(configured);
        if (seconds <= 0) {
            throw new IllegalArgumentException(TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY + " must be positive");
        }
        return seconds;
    }

    private void waitForBlocked(Driver driver, List<Operator> operators, com.google.common.util.concurrent.ListenableFuture<Void> blocked)
            throws Exception
    {
        try {
            blocked.get(blockedWaitTimeoutSeconds, TimeUnit.SECONDS);
        }
        catch (TimeoutException exception) {
            throw new IllegalStateException(describeBlockedDriver(driver, operators, blocked), exception);
        }
    }

    private static String describeBlockedDriver(Driver driver, List<Operator> operators, com.google.common.util.concurrent.ListenableFuture<Void> blocked)
    {
        StringBuilder message = new StringBuilder("Timed out waiting for Trino driver blocked future to complete");
        message.append(" [driverFinished=").append(driver.isFinished()).append(']');
        for (int index = 0; index < operators.size(); index++) {
            Operator operator = operators.get(index);
            com.google.common.util.concurrent.ListenableFuture<Void> operatorBlocked = operator.isBlocked();
            com.google.common.util.concurrent.ListenableFuture<Void> waitingForMemory = operator.getOperatorContext().isWaitingForMemory();
            com.google.common.util.concurrent.ListenableFuture<Void> waitingForRevocableMemory = operator.getOperatorContext().isWaitingForRevocableMemory();
            message.append(System.lineSeparator())
                    .append("operator[").append(index).append("]=").append(operator.getClass().getSimpleName())
                    .append(" finished=").append(operator.isFinished())
                    .append(" needsInput=").append(operator.needsInput())
                    .append(" blockedDone=").append(operatorBlocked.isDone())
                    .append(" matchesDriverBlocked=").append(operatorBlocked == blocked)
                    .append(" waitingForMemoryDone=").append(waitingForMemory.isDone())
                    .append(" waitingForRevocableMemoryDone=").append(waitingForRevocableMemory.isDone())
                    .append(" waitingForMemoryMatchesDriverBlocked=").append(waitingForMemory == blocked)
                    .append(" waitingForRevocableMatchesDriverBlocked=").append(waitingForRevocableMemory == blocked);
        }
        return message.toString();
    }
}
