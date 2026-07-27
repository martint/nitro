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

import org.weakref.nitro.jit.ProjectionCodeGenerationPolicy;

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
    private final AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy;
    private final FlatKeyTablePolicy flatKeyTablePolicy;
    private final DistinctKeySetPolicy distinctKeySetPolicy;
    private final FilterOperatorResources filter;
    private final FullJoinOperatorPolicy fullJoinPolicy;
    private final GroupIdOperatorPolicy groupIdPolicy;
    private final ProjectOperatorResources project;
    private final AggregationOperatorResources aggregation;
    private final BufferedJoinInputPolicy bufferedJoinInputPolicy;
    private final JoinBufferPolicy joinBufferPolicy;
    private final NestedLoopJoinPolicy nestedLoopJoinPolicy;
    private final SemiJoinOperatorPolicy semiJoinPolicy;
    private final DynamicFilterPolicy dynamicFilterPolicy;
    private final HashJoinOperatorResources hashJoin;
    private final GenericJoinIndexFactory genericJoinIndexes;
    private final GroupingStateResources grouping;
    private final SortOperatorPolicy sortPolicy;
    private final TopNRankingOperatorPolicy topNRankingPolicy;
    private final WindowOperatorPolicy windowPolicy;
    private boolean closed;

    public OperatorResources(
            OperatorCodeGenerationResources codeGeneration,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy,
            DistinctKeySetPolicy distinctKeySetPolicy,
            FilterOperatorPolicy filterPolicy,
            FullJoinOperatorPolicy fullJoinPolicy,
            GroupIdOperatorPolicy groupIdPolicy,
            ProjectOperatorResources project,
            AggregationOperatorResources aggregation,
            BufferedJoinInputPolicy bufferedJoinInputPolicy,
            JoinBufferPolicy joinBufferPolicy,
            NestedLoopJoinPolicy nestedLoopJoinPolicy,
            SemiJoinOperatorPolicy semiJoinPolicy,
            DynamicFilterPolicy dynamicFilterPolicy,
            HashJoinOperatorResources hashJoin,
            GroupingStateResources grouping,
            SortOperatorPolicy sortPolicy,
            TopNRankingOperatorPolicy topNRankingPolicy,
            WindowOperatorPolicy windowPolicy)
    {
        this.codeGeneration = requireNonNull(codeGeneration, "codeGeneration is null");
        this.adaptiveLongGroupingPolicy = requireNonNull(adaptiveLongGroupingPolicy, "adaptiveLongGroupingPolicy is null");
        this.flatKeyTablePolicy = requireNonNull(flatKeyTablePolicy, "flatKeyTablePolicy is null");
        this.distinctKeySetPolicy = requireNonNull(distinctKeySetPolicy, "distinctKeySetPolicy is null");
        this.project = requireNonNull(project, "project is null");
        this.filter = new FilterOperatorResources(
                codeGeneration.projectionMask(),
                project.evaluationPolicy(),
                requireNonNull(filterPolicy, "filterPolicy is null"));
        this.fullJoinPolicy = requireNonNull(fullJoinPolicy, "fullJoinPolicy is null");
        this.groupIdPolicy = requireNonNull(groupIdPolicy, "groupIdPolicy is null");
        this.aggregation = requireNonNull(aggregation, "aggregation is null");
        this.bufferedJoinInputPolicy = requireNonNull(bufferedJoinInputPolicy, "bufferedJoinInputPolicy is null");
        this.joinBufferPolicy = requireNonNull(joinBufferPolicy, "joinBufferPolicy is null");
        this.nestedLoopJoinPolicy = requireNonNull(nestedLoopJoinPolicy, "nestedLoopJoinPolicy is null");
        this.semiJoinPolicy = requireNonNull(semiJoinPolicy, "semiJoinPolicy is null");
        this.dynamicFilterPolicy = requireNonNull(dynamicFilterPolicy, "dynamicFilterPolicy is null");
        this.hashJoin = requireNonNull(hashJoin, "hashJoin is null");
        this.genericJoinIndexes = new GenericJoinIndexFactory(
                codeGeneration,
                flatKeyTablePolicy,
                hashJoin.buildPolicy(),
                hashJoin.indexPolicy(),
                hashJoin.executionPolicy());
        this.grouping = requireNonNull(grouping, "grouping is null");
        this.sortPolicy = requireNonNull(sortPolicy, "sortPolicy is null");
        this.topNRankingPolicy = requireNonNull(topNRankingPolicy, "topNRankingPolicy is null");
        this.windowPolicy = requireNonNull(windowPolicy, "windowPolicy is null");
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
        EvaluationOperatorPolicy evaluationPolicy = EvaluationOperatorPolicy.fromSystemProperties();
        FlatKeyTablePolicy flatKeyTablePolicy = FlatKeyTablePolicy.fromSystemProperties();
        return new OperatorResources(
                new OperatorCodeGenerationResources(ProjectionCodeGenerationPolicy.fromSystemProperties()),
                AdaptiveLongGroupingPolicy.fromSystemProperties(),
                flatKeyTablePolicy,
                DistinctKeySetPolicy.fromSystemProperties(),
                FilterOperatorPolicy.fromSystemProperties(),
                FullJoinOperatorPolicy.fromSystemProperties(),
                GroupIdOperatorPolicy.fromSystemProperties(),
                new ProjectOperatorResources(ProjectOperatorPolicy.fromSystemProperties(), evaluationPolicy),
                new AggregationOperatorResources(AggregationOperatorPolicy.fromSystemProperties()),
                BufferedJoinInputPolicy.fromSystemProperties(),
                JoinBufferPolicy.fromSystemProperties(),
                NestedLoopJoinPolicy.fromSystemProperties(),
                SemiJoinOperatorPolicy.fromSystemProperties(),
                DynamicFilterPolicy.fromSystemProperties(),
                new HashJoinOperatorResources(Boolean.parseBoolean(
                        System.getProperty("nitro.hash.join.shareBufferPoolAcrossOperators", "true")),
                        hashJoinMaterializationListener,
                        HashJoinIndexPolicy.fromSystemProperties(),
                        HashJoinDynamicFilterPolicy.fromSystemProperties(),
                        HashJoinBuildPolicy.fromSystemProperties(),
                        HashJoinOutputPolicy.fromSystemProperties(),
                        HashJoinFilterPolicy.fromSystemProperties(),
                        HashJoinExecutionPolicy.fromSystemProperties()),
                createDefaultGroupingResources(),
                SortOperatorPolicy.fromSystemProperties(),
                TopNRankingOperatorPolicy.fromSystemProperties(),
                WindowOperatorPolicy.fromSystemProperties());
    }

    private static GroupingStateResources createDefaultGroupingResources()
    {
        boolean poolZeroedLongDirectIds = Boolean.parseBoolean(
                System.getProperty("nitro.group.zeroedLongDirectIdsPool", "true"));
        return new GroupingStateResources(
                poolZeroedLongDirectIds,
                LongGroupingPolicy.fromSystemProperties(poolZeroedLongDirectIds),
                CompositeGroupingPolicy.fromSystemProperties());
    }

    public OperatorCodeGenerationResources codeGeneration()
    {
        checkOpen();
        return codeGeneration;
    }

    public FlatKeyTablePolicy flatKeyTablePolicy()
    {
        checkOpen();
        return flatKeyTablePolicy;
    }

    public AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy()
    {
        checkOpen();
        return adaptiveLongGroupingPolicy;
    }

    public DistinctKeySetPolicy distinctKeySetPolicy()
    {
        checkOpen();
        return distinctKeySetPolicy;
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

    GenericJoinIndexFactory genericJoinIndexes()
    {
        checkOpen();
        return genericJoinIndexes;
    }

    public BufferedJoinInputPolicy bufferedJoinInputPolicy()
    {
        checkOpen();
        return bufferedJoinInputPolicy;
    }

    public JoinBufferPolicy joinBufferPolicy()
    {
        checkOpen();
        return joinBufferPolicy;
    }

    public NestedLoopJoinPolicy nestedLoopJoinPolicy()
    {
        checkOpen();
        return nestedLoopJoinPolicy;
    }

    public SemiJoinOperatorPolicy semiJoinPolicy()
    {
        checkOpen();
        return semiJoinPolicy;
    }

    public DynamicFilterPolicy dynamicFilterPolicy()
    {
        checkOpen();
        return dynamicFilterPolicy;
    }

    public GroupingStateResources grouping()
    {
        checkOpen();
        return grouping;
    }

    public SortOperatorPolicy sortPolicy()
    {
        checkOpen();
        return sortPolicy;
    }

    public TopNRankingOperatorPolicy topNRankingPolicy()
    {
        checkOpen();
        return topNRankingPolicy;
    }

    public WindowOperatorPolicy windowPolicy()
    {
        checkOpen();
        return windowPolicy;
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
