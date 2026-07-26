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
package org.weakref.nitro.operator;

import static java.util.Objects.requireNonNull;

/**
 * Typed, engine-owned operator and generated-code resources.
 *
 * <p>This owner is deliberately separate from vector/storage allocation. It is passed by the engine composition root
 * and contains no connector-facing resources or untyped service lookup.
 */
public final class OperatorResources
        implements AutoCloseable
{
    private final OperatorCodeGenerationResources codeGeneration;
    private final FilterOperatorResources filter;
    private final FullJoinOperatorPolicy fullJoinPolicy;
    private final GroupIdOperatorPolicy groupIdPolicy;
    private final ProjectOperatorResources project;
    private final AggregationOperatorResources aggregation;
    private final HashJoinOperatorResources hashJoin;
    private final GroupingStateResources grouping;
    private boolean closed;

    public OperatorResources(
            OperatorCodeGenerationResources codeGeneration,
            FilterOperatorPolicy filterPolicy,
            FullJoinOperatorPolicy fullJoinPolicy,
            GroupIdOperatorPolicy groupIdPolicy,
            ProjectOperatorResources project,
            AggregationOperatorResources aggregation,
            HashJoinOperatorResources hashJoin,
            GroupingStateResources grouping)
    {
        this.codeGeneration = requireNonNull(codeGeneration, "codeGeneration is null");
        this.filter = new FilterOperatorResources(codeGeneration.projectionMask(), requireNonNull(filterPolicy, "filterPolicy is null"));
        this.fullJoinPolicy = requireNonNull(fullJoinPolicy, "fullJoinPolicy is null");
        this.groupIdPolicy = requireNonNull(groupIdPolicy, "groupIdPolicy is null");
        this.project = requireNonNull(project, "project is null");
        this.aggregation = requireNonNull(aggregation, "aggregation is null");
        this.hashJoin = requireNonNull(hashJoin, "hashJoin is null");
        this.grouping = requireNonNull(grouping, "grouping is null");
    }

    /**
     * Constructs a fresh, isolated owner using Nitro's standalone policies.
     */
    public static OperatorResources createDefault()
    {
        return createDefault(null);
    }

    /**
     * Constructs a fresh, isolated owner with engine-selected hash-join diagnostics.
     */
    public static OperatorResources createDefault(HashJoinMaterializationListener hashJoinMaterializationListener)
    {
        return new OperatorResources(
                new OperatorCodeGenerationResources(),
                FilterOperatorPolicy.fromSystemProperties(),
                FullJoinOperatorPolicy.fromSystemProperties(),
                GroupIdOperatorPolicy.fromSystemProperties(),
                new ProjectOperatorResources(ProjectOperatorPolicy.fromSystemProperties()),
                new AggregationOperatorResources(AggregationOperatorPolicy.fromSystemProperties()),
                new HashJoinOperatorResources(Boolean.parseBoolean(
                        System.getProperty("nitro.hash.join.shareBufferPoolAcrossOperators", "true")),
                        hashJoinMaterializationListener),
                new GroupingStateResources(Boolean.parseBoolean(
                        System.getProperty("nitro.group.zeroedLongDirectIdsPool", "true"))));
    }

    public OperatorCodeGenerationResources codeGeneration()
    {
        checkOpen();
        return codeGeneration;
    }

    public ProjectOperatorResources project()
    {
        checkOpen();
        return project;
    }

    public FilterOperatorResources filter()
    {
        checkOpen();
        return filter;
    }

    public GroupIdOperatorPolicy groupIdPolicy()
    {
        checkOpen();
        return groupIdPolicy;
    }

    public FullJoinOperatorPolicy fullJoinPolicy()
    {
        checkOpen();
        return fullJoinPolicy;
    }

    public AggregationOperatorResources aggregation()
    {
        checkOpen();
        return aggregation;
    }

    public HashJoinOperatorResources hashJoin()
    {
        checkOpen();
        return hashJoin;
    }

    public GroupingStateResources grouping()
    {
        checkOpen();
        return grouping;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        codeGeneration.close();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Operator resources are closed");
        }
    }
}
