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
package org.weakref.nitro.data;

import org.weakref.nitro.operator.AggregationOperatorResources;
import org.weakref.nitro.operator.DistinctKeySetPolicy;
import org.weakref.nitro.operator.FilterOperatorPolicy;
import org.weakref.nitro.operator.FlatKeyTablePolicy;
import org.weakref.nitro.operator.FullJoinOperatorPolicy;
import org.weakref.nitro.operator.GroupIdOperatorPolicy;
import org.weakref.nitro.operator.GroupingStateResources;
import org.weakref.nitro.operator.HashJoinOperatorResources;
import org.weakref.nitro.operator.OperatorCodeGenerationResources;
import org.weakref.nitro.operator.OperatorResources;
import org.weakref.nitro.operator.ProjectOperatorResources;
import org.weakref.nitro.operator.TopNRankingOperatorPolicy;

import static java.util.Objects.requireNonNull;

/**
 * Stateful resources owned by a Nitro engine integration.
 *
 * <p>The embedding engine chooses this object's lifetime and passes it to every allocator that should share retained
 * storage. Nothing in Nitro discovers an ambient process-wide pool. Separate resource owners are therefore isolated,
 * and an integration can bind retention to a worker, task, query, or test fixture as appropriate.
 */
public final class EngineResources
        implements AutoCloseable
{
    private final AllocationResources allocationResources;
    private final OperatorResources operatorResources;
    private boolean closed;

    public EngineResources(
            PrimitiveArrayPool primitiveArrays,
            PrimitiveArrayPool nativeBuffers,
            OperatorCodeGenerationResources operatorCodeGeneration,
            ProjectOperatorResources projectOperator,
            AggregationOperatorResources aggregationOperator,
            HashJoinOperatorResources hashJoinOperator,
            GroupingStateResources groupingState)
    {
        this(
                new AllocationResources(primitiveArrays, nativeBuffers),
                new OperatorResources(
                        operatorCodeGeneration,
                        FlatKeyTablePolicy.fromSystemProperties(),
                        DistinctKeySetPolicy.fromSystemProperties(),
                        FilterOperatorPolicy.fromSystemProperties(),
                        FullJoinOperatorPolicy.fromSystemProperties(),
                        GroupIdOperatorPolicy.fromSystemProperties(),
                        projectOperator,
                        aggregationOperator,
                        hashJoinOperator,
                        groupingState,
                        TopNRankingOperatorPolicy.fromSystemProperties()));
    }

    public EngineResources(
            AllocationResources allocationResources,
            OperatorCodeGenerationResources operatorCodeGeneration,
            ProjectOperatorResources projectOperator,
            AggregationOperatorResources aggregationOperator,
            HashJoinOperatorResources hashJoinOperator,
            GroupingStateResources groupingState)
    {
        this(
                allocationResources,
                new OperatorResources(
                        operatorCodeGeneration,
                        FlatKeyTablePolicy.fromSystemProperties(),
                        DistinctKeySetPolicy.fromSystemProperties(),
                        FilterOperatorPolicy.fromSystemProperties(),
                        FullJoinOperatorPolicy.fromSystemProperties(),
                        GroupIdOperatorPolicy.fromSystemProperties(),
                        projectOperator,
                        aggregationOperator,
                        hashJoinOperator,
                        groupingState,
                        TopNRankingOperatorPolicy.fromSystemProperties()));
    }

    public EngineResources(AllocationResources allocationResources, OperatorResources operatorResources)
    {
        this.allocationResources = requireNonNull(allocationResources, "allocationResources is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
    }

    /**
     * Constructs a new, isolated resource owner using Nitro's standalone retention defaults.
     *
     * <p>This is a factory for a fresh owner, not a shared resource accessor.
     */
    public static EngineResources createDefault()
    {
        return new EngineResources(
                AllocationResources.createDefault(),
                OperatorResources.createDefault());
    }

    public PrimitiveArrayPool primitiveArrays()
    {
        checkOpen();
        return allocationResources.primitiveArrays();
    }

    public PrimitiveArrayPool nativeBuffers()
    {
        checkOpen();
        return allocationResources.nativeBuffers();
    }

    public AllocationResources allocationResources()
    {
        checkOpen();
        return allocationResources;
    }

    public OperatorCodeGenerationResources operatorCodeGeneration()
    {
        checkOpen();
        return operatorResources.codeGeneration();
    }

    public ProjectOperatorResources projectOperator()
    {
        checkOpen();
        return operatorResources.project();
    }

    public AggregationOperatorResources aggregationOperator()
    {
        checkOpen();
        return operatorResources.aggregation();
    }

    public HashJoinOperatorResources hashJoinOperator()
    {
        checkOpen();
        return operatorResources.hashJoin();
    }

    public GroupingStateResources groupingState()
    {
        checkOpen();
        return operatorResources.grouping();
    }

    public OperatorResources operatorResources()
    {
        checkOpen();
        return operatorResources;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        allocationResources.close();
        operatorResources.close();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Engine resources are closed");
        }
    }
}
