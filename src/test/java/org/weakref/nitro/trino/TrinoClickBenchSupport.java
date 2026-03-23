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
import io.trino.operator.Operator;
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
import io.trino.testing.MaterializedResult;
import io.trino.testing.PageConsumerOperator;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTaskContext;

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

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public final class TrinoClickBenchSupport
        implements AutoCloseable
{
    private static final String TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS_PROPERTY = "nitro.clickbench.trino.blockedWaitTimeoutSeconds";
    private static final String TRINO_QUERY_MAX_MEMORY_PROPERTY = "nitro.clickbench.trino.queryMaxMemoryGigabytes";
    private static final int DEFAULT_TRINO_BLOCKED_WAIT_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_TRINO_QUERY_MAX_MEMORY_GIGABYTES = 4;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", ImmutableList.of());
    private static final TestingAggregationFunction BIGINT_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
    private static final TestingAggregationFunction BIGINT_AVG = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(BIGINT));
    private static final TestingAggregationFunction INTEGER_MIN = FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(INTEGER));
    private static final TestingAggregationFunction INTEGER_MAX = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(INTEGER));

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TrinoClickBenchSupport"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TrinoClickBenchSupport-scheduled"));
    private final OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());
    private final int blockedWaitTimeoutSeconds = blockedWaitTimeoutSeconds();
    private final DataSize queryMaxMemory = queryMaxMemory();

    public Path requiredActualHitsPath()
    {
        return TrinoClickBenchPageReader.requiredActualHitsPath();
    }

    public void consumeQuery0SelectAll(Path input)
    {
        List<String> columns = TrinoClickBenchPageReader.allColumns(input);
        consume(input, columns, List.of(), columns.stream().map(TrinoClickBenchSupport::inferType).toList());
    }

    public MaterializedResult query1CountAll(Path input)
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

    public MaterializedResult query3SumAdvEngineAndAvgResolutionWidth(Path input)
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

    public MaterializedResult query7MinAndMaxEventDate(Path input)
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

    public MaterializedResult query8GroupByAdvEngineId(Path input)
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

    public MaterializedResult query13TopSearchPhrases(Path input)
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

    public MaterializedResult query34TopUrls(Path input)
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
        List<Page> outputPages = collectOutput ? new ArrayList<>() : null;
        try (TrinoClickBenchPageReader reader = new TrinoClickBenchPageReader(input, columns)) {
            DriverContext driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TestingSession.testSessionBuilder().build())
                    .setQueryMaxMemory(queryMaxMemory)
                    .setMemoryPoolSize(queryMaxMemory)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();

            List<Operator> operators = new ArrayList<>();
            TrinoPageSequenceSourceOperator.Factory sourceFactory = new TrinoPageSequenceSourceOperator.Factory(0, new PlanNodeId("source"), reader);
            operators.add(sourceFactory.createOperator(driverContext));

            for (OperatorFactory factory : factories) {
                operators.add(factory.createOperator(driverContext));
                factory.noMoreOperators();
            }

            operators.add(new PageConsumerOperator(
                    driverContext.addOperatorContext(1000, new PlanNodeId("sink"), PageConsumerOperator.class.getSimpleName()),
                    page -> {
                        if (collectOutput) {
                            outputPages.add(page);
                        }
                    },
                    java.util.function.Function.identity()));

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

            MaterializedResult.Builder result = MaterializedResult.resultBuilder(driverContext.getSession(), outputTypes);
            if (collectOutput) {
                for (Page page : outputPages) {
                    result.page(page);
                }
            }
            return result.build();
        }
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

    private static RowExpression castField(int inputChannel, Type fromType, Type toType)
    {
        return new CallExpression(FUNCTION_RESOLUTION.getCoercion(fromType, toType), List.of(field(inputChannel, fromType)));
    }

    private static RowExpression notEqual(int inputChannel, Type type, Object constantValue)
    {
        RowExpression equals = new CallExpression(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, List.of(type, type)),
                List.of(field(inputChannel, type), constant(constantValue, type)));
        return new CallExpression(FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)), List.of(equals));
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
