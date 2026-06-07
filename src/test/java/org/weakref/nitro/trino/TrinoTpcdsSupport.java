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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.OutputFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.TaskContext;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.server.protocol.spooling.QueryDataEncoders;
import io.trino.server.protocol.spooling.SpoolingEnabledConfig;
import io.trino.split.SplitSource;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.PartitionFunctionProvider;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.weakref.nitro.tpcds.TpcdsQueryCatalog;
import org.weakref.nitro.trino.tpcds.LocalTpcdsPlugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.node.TestingInternalNodeManager.CURRENT_NODE;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.trino.spiller.SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class TrinoTpcdsSupport
        implements AutoCloseable
{
    private static final DataSize TASK_QUERY_MAX_MEMORY = DataSize.of(2, GIGABYTE);
    private static final DataSize TASK_MEMORY_POOL_SIZE = DataSize.of(4, GIGABYTE);

    private final PlanTester planTester;

    public TrinoTpcdsSupport(String schema)
    {
        planTester = PlanTester.create(sessionForSchema(schema));
        planTester.installPlugin(new LocalTpcdsPlugin());
        planTester.createCatalog("tpcds", "tpcds", java.util.Map.of());
    }

    public MaterializedResult executeBenchmarkQuery(String queryId, String schema)
    {
        return executeSql(TpcdsQueryCatalog.benchmarkQuerySql(queryId, "tpcds", schema));
    }

    public String explainBenchmarkQuery(String queryId, String schema)
    {
        return explainSql(TpcdsQueryCatalog.benchmarkQuerySql(queryId, "tpcds", schema));
    }

    /** Optimized logical plan for an arbitrary SQL string (used to audit ported-query computation trees against Trino's plan). */
    public String explainQuery(String sql)
    {
        return explainSql(sql);
    }

    public MaterializedResult executeReferenceQuery(String queryId)
    {
        return executeSql(TpcdsQueryCatalog.referenceQuerySql(queryId));
    }

    @Override
    public void close()
    {
        planTester.close();
    }

    private MaterializedResult executeSql(String sql)
    {
        return executePlanWithLargeTaskMemory(
                session -> planTester.createPlan(session, sql),
                new PlanTester.MaterializedResultOutput());
    }

    private String explainSql(String sql)
    {
        planTester.getAccessControl().checkCanExecuteQuery(planTester.getDefaultSession().getIdentity(), planTester.getDefaultSession().getQueryId());
        return planTester.inTransaction(planTester.getDefaultSession(), session -> {
            Plan plan = planTester.createPlan(session, sql);
            return textLogicalPlan(
                    plan.getRoot(),
                    planTester.getPlannerContext().getMetadata(),
                    planTester.getPlannerContext().getFunctionManager(),
                    plan.getStatsAndCosts(),
                    session,
                    0,
                    false);
        });
    }

    private <T> T executePlanWithLargeTaskMemory(Function<Session, Plan> planFactory, PlanTester.Output<T> output)
    {
        planTester.getAccessControl().checkCanExecuteQuery(planTester.getDefaultSession().getIdentity(), planTester.getDefaultSession().getQueryId());

        return planTester.inTransaction(planTester.getDefaultSession(), session -> {
            try (Closer closer = Closer.create()) {
                Plan plan = planFactory.apply(session);
                List<Driver> drivers = createDriversWithLargeTaskMemory(session, plan, output.outputFactory());
                drivers.forEach(closer::register);

                boolean done = false;
                while (!done) {
                    boolean processed = false;
                    for (Driver driver : drivers) {
                        if (!driver.isFinished()) {
                            driver.processForNumberOfIterations(1);
                            processed = true;
                        }
                    }
                    done = !processed;
                }

                return output.result(((OutputNode) plan.getRoot()).getColumnNames());
            }
            catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        });
    }

    private List<Driver> createDriversWithLargeTaskMemory(Session session, Plan plan, OutputFactory outputFactory)
    {
        SubPlan subplan = planTester.createSubPlans(session, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected sub-plan to have no children");
        }

        TaskContext taskContext = TestingTaskContext.builder(notificationExecutor(), yieldExecutor(), session)
                .setQueryMaxMemory(TASK_QUERY_MAX_MEMORY)
                .setMemoryPoolSize(TASK_MEMORY_POOL_SIZE)
                .build();

        TableExecuteContextManager tableExecuteContextManager = new TableExecuteContextManager();
        tableExecuteContextManager.registerTableExecuteContextForQuery(taskContext.getQueryContext().getQueryId());

        PlannerContext plannerContext = planTester.getPlannerContext();
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                plannerContext,
                java.util.Optional.empty(),
                planTester.getPageSourceManager(),
                indexManager(),
                partitionFunctionProvider(),
                pageSinkManager(),
                unsupportedDirectExchangeClientSupplier(),
                expressionCompiler(),
                pageFunctionCompiler(),
                joinFilterFunctionCompiler(),
                new IndexJoinLookupStats(),
                taskManagerConfig(),
                new io.trino.spiller.GenericSpillerFactory(unsupportedSingleStreamSpillerFactory()),
                new QueryDataEncoders(new SpoolingEnabledConfig(), Set.of()),
                java.util.Optional.empty(),
                unsupportedSingleStreamSpillerFactory(),
                unsupportedPartitioningSpillerFactory(),
                new PagesIndex.TestingFactory(false),
                joinCompiler(),
                hashStrategyCompiler(),
                new OrderingCompiler(plannerContext.getTypeOperators()),
                new DynamicFilterConfig(),
                blockTypeOperators(),
                typeOperators(),
                tableExecuteContextManager,
                exchangeManagerRegistry(),
                CURRENT_NODE.getNodeVersion(),
                new CompilerConfig());

        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getOutputPartitioningScheme().getOutputLayout(),
                subplan.getFragment().getPartitionedSources(),
                outputFactory);

        List<SplitAssignment> splitAssignments = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableHandle table = tableScan.getTable();
            SplitSource splitSource = getSplitSource(session, table);

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getNextBatch(splitSource)) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
                }
            }

            splitAssignments.add(new SplitAssignment(tableScan.getId(), scheduledSplits.build(), true));
        }

        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, DriverFactory> driverFactoriesBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            for (int index = 0; index < driverFactory.getDriverInstances().orElse(1); index++) {
                if (driverFactory.getSourceId().isPresent()) {
                    checkState(driverFactoriesBySource.put(driverFactory.getSourceId().orElseThrow(), driverFactory) == null);
                }
                else {
                    DriverContext driverContext = taskContext
                            .addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false)
                            .addDriverContext();
                    drivers.add(driverFactory.createDriver(driverContext));
                }
            }
        }

        Set<PlanNodeId> partitionedSources = ImmutableSet.copyOf(subplan.getFragment().getPartitionedSources());
        for (SplitAssignment splitAssignment : splitAssignments) {
            DriverFactory driverFactory = driverFactoriesBySource.get(splitAssignment.getPlanNodeId());
            checkState(driverFactory != null);
            boolean partitioned = partitionedSources.contains(driverFactory.getSourceId().orElseThrow());
            for (ScheduledSplit split : splitAssignment.getSplits()) {
                DriverContext driverContext = taskContext
                        .addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned)
                        .addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                driver.updateSplitAssignment(new SplitAssignment(split.getPlanNodeId(), ImmutableSet.of(split), true));
                drivers.add(driver);
            }
        }

        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            driverFactory.noMoreDrivers();
        }

        return ImmutableList.copyOf(drivers);
    }

    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        try {
            return splitSource.getNextBatch(1000).get().getSplits();
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
        catch (ExecutionException exception) {
            throw new RuntimeException(exception);
        }
    }

    private SplitSource getSplitSource(Session session, TableHandle table)
    {
        try {
            Method method = planTester.getSplitManager().getClass().getMethod(
                    "getSplits",
                    Session.class,
                    Class.forName("io.opentelemetry.api.trace.Span"),
                    TableHandle.class,
                    Class.forName("io.trino.spi.connector.DynamicFilter"),
                    Class.forName("io.trino.spi.connector.Constraint"));
            Object invalidSpan = Class.forName("io.opentelemetry.api.trace.Span")
                    .getMethod("getInvalid")
                    .invoke(null);
            return (SplitSource) method.invoke(planTester.getSplitManager(), session, invalidSpan, table, EMPTY, alwaysTrue());
        }
        catch (ReflectiveOperationException exception) {
            throw new RuntimeException("Unable to fetch TPC-DS splits", exception);
        }
    }

    private io.trino.operator.DirectExchangeClientSupplier unsupportedDirectExchangeClientSupplier()
    {
        return (io.trino.operator.DirectExchangeClientSupplier) Proxy.newProxyInstance(
                io.trino.operator.DirectExchangeClientSupplier.class.getClassLoader(),
                new Class<?>[] {io.trino.operator.DirectExchangeClientSupplier.class},
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                });
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll().stream()
                .map(TableScanNode.class::cast)
                .collect(toImmutableList());
    }

    private Executor notificationExecutor()
    {
        return reflectiveField("notificationExecutor", Executor.class);
    }

    private ScheduledExecutorService yieldExecutor()
    {
        return reflectiveField("yieldExecutor", ScheduledExecutorService.class);
    }

    private io.trino.operator.index.IndexManager indexManager()
    {
        return reflectiveField("indexManager", io.trino.operator.index.IndexManager.class);
    }

    private PartitionFunctionProvider partitionFunctionProvider()
    {
        return reflectiveField("partitionFunctionProvider", PartitionFunctionProvider.class);
    }

    private io.trino.split.PageSinkManager pageSinkManager()
    {
        return reflectiveField("pageSinkManager", io.trino.split.PageSinkManager.class);
    }

    private ExpressionCompiler expressionCompiler()
    {
        return reflectiveField("expressionCompiler", ExpressionCompiler.class);
    }

    private PageFunctionCompiler pageFunctionCompiler()
    {
        return reflectiveField("pageFunctionCompiler", PageFunctionCompiler.class);
    }

    private JoinFilterFunctionCompiler joinFilterFunctionCompiler()
    {
        return reflectiveField("joinFilterFunctionCompiler", JoinFilterFunctionCompiler.class);
    }

    private io.trino.execution.TaskManagerConfig taskManagerConfig()
    {
        return reflectiveField("taskManagerConfig", io.trino.execution.TaskManagerConfig.class);
    }

    private JoinCompiler joinCompiler()
    {
        return reflectiveField("joinCompiler", JoinCompiler.class);
    }

    private FlatHashStrategyCompiler hashStrategyCompiler()
    {
        return reflectiveField("hashStrategyCompiler", FlatHashStrategyCompiler.class);
    }

    private BlockTypeOperators blockTypeOperators()
    {
        return reflectiveField("blockTypeOperators", BlockTypeOperators.class);
    }

    private io.trino.spi.type.TypeOperators typeOperators()
    {
        return reflectiveField("typeOperators", io.trino.spi.type.TypeOperators.class);
    }

    private io.trino.exchange.ExchangeManagerRegistry exchangeManagerRegistry()
    {
        return reflectiveField("exchangeManagerRegistry", io.trino.exchange.ExchangeManagerRegistry.class);
    }

    private <T> T reflectiveField(String fieldName, Class<T> type)
    {
        try {
            Field field = PlanTester.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return type.cast(field.get(planTester));
        }
        catch (ReflectiveOperationException exception) {
            throw new RuntimeException("Unable to access PlanTester field: " + fieldName, exception);
        }
    }

    private static Session sessionForSchema(String schema)
    {
        return testSessionBuilder()
                .setSource("nitro")
                .setCatalog("tpcds")
                .setSchema(schema)
                .setSystemProperty("query_max_memory", "4GB")
                .setSystemProperty("query_max_memory_per_node", "2GB")
                .build();
    }
}
