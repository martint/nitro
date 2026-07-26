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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestOperatorResources
{
    @Test
    void testOwnersAreIsolatedAndExplicitlyClosed()
    {
        OperatorResources first = OperatorResources.createDefault();
        OperatorResources second = OperatorResources.createDefault();

        assertThat(first.codeGeneration()).isNotSameAs(second.codeGeneration());
        assertThat(first.filter()).isNotSameAs(second.filter());
        assertThat(first.fullJoinPolicy()).isNotSameAs(second.fullJoinPolicy());
        assertThat(first.groupIdPolicy()).isNotSameAs(second.groupIdPolicy());
        assertThat(first.project()).isNotSameAs(second.project());
        assertThat(first.aggregation()).isNotSameAs(second.aggregation());
        assertThat(first.hashJoin()).isNotSameAs(second.hashJoin());
        assertThat(first.grouping()).isNotSameAs(second.grouping());
        assertThat(first.topNRankingPolicy()).isNotSameAs(second.topNRankingPolicy());

        first.close();
        assertThatIllegalStateException()
                .isThrownBy(first::codeGeneration)
                .withMessage("Operator resources are closed");
        assertThatIllegalStateException()
                .isThrownBy(first::filter)
                .withMessage("Operator resources are closed");
        assertThatIllegalStateException()
                .isThrownBy(first::fullJoinPolicy)
                .withMessage("Operator resources are closed");
        assertThatIllegalStateException()
                .isThrownBy(first::groupIdPolicy)
                .withMessage("Operator resources are closed");
        assertThatIllegalStateException()
                .isThrownBy(first::topNRankingPolicy)
                .withMessage("Operator resources are closed");
        assertThat(second.codeGeneration()).isNotNull();
        second.close();
    }

    @Test
    void testProjectConstructionDoesNotDiscoverServicesThroughAllocator()
    {
        try (AllocationResources allocationResources = AllocationResources.createDefault();
                OperatorResources operatorResources = OperatorResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                Operator source = new ConstantTableOperator(allocator, 0, List.of());
                Operator project = new ProjectOperator(
                        allocator,
                        new EvaluationPlan(List.of(), List.of()),
                        new PrimitiveRegistry(),
                        source,
                        operatorResources)) {
            assertThat(project.outputCount()).isZero();
        }
    }

    @Test
    void testGroupingAndAggregationConstructionDoNotDiscoverServicesThroughAllocator()
    {
        try (AllocationResources allocationResources = AllocationResources.createDefault();
                OperatorResources operatorResources = OperatorResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                Operator groupSource = new ConstantTableOperator(allocator, 0, List.of());
                Operator filterSource = new ConstantTableOperator(allocator, 0, List.of());
                Operator aggregationSource = new ConstantTableOperator(allocator, 0, List.of());
                Operator groupedAggregationSource = new ConstantTableOperator(allocator, 0, List.of());
                Operator distinctSource = new ConstantTableOperator(allocator, 0, List.of());
                Operator markerSource = new ConstantTableOperator(allocator, 0, List.of());
                Operator semiOuter = new ConstantTableOperator(allocator, 0, List.of());
                Operator semiInner = new ConstantTableOperator(allocator, 0, List.of());
                Operator joinOuter = new ConstantTableOperator(allocator, 0, List.of());
                Operator joinInner = new ConstantTableOperator(allocator, 0, List.of());
                Operator group = new GroupOperator(allocator, 0, groupSource, operatorResources);
                Operator filter = new FilterOperator(
                        filterSource,
                        new EvaluationPlan(List.of(), List.of()),
                        new PrimitiveRegistry(),
                        AllMask.ALL,
                        allocator,
                        operatorResources.filter());
                Operator aggregation = new AggregationOperator(allocator, List.of(), aggregationSource, operatorResources);
                Operator groupedAggregation = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        List.of(),
                        groupedAggregationSource,
                        operatorResources);
                Operator distinct = new MarkDistinctOperator(allocator, new int[] {0}, distinctSource, false, operatorResources);
                Operator marker = new MarkDistinctMarkerOperator(allocator, new int[] {0}, markerSource, true, operatorResources);
                Operator semiJoin = new SemiJoinOperator(
                        allocator,
                        semiOuter,
                        0,
                        semiInner,
                        0,
                        true,
                        false,
                        operatorResources);
                Operator hashJoin = new HashJoinOperator(operatorResources, allocator, joinOuter, 0, joinInner, 0)) {
            assertThat(group.outputCount()).isEqualTo(1);
            assertThat(filter.outputCount()).isZero();
            assertThat(aggregation.outputCount()).isZero();
            assertThat(groupedAggregation.outputCount()).isEqualTo(1);
            assertThat(distinct.outputCount()).isZero();
            assertThat(marker.outputCount()).isEqualTo(1);
            assertThat(semiJoin.outputCount()).isZero();
            assertThat(hashJoin.outputCount()).isZero();

            Allocator.Context accumulatorContext = new Allocator.Context("resource-aware-accumulator");
            Streams state = new DistinctCount(0).allocate(
                    new AggregationExecutionContext(
                            allocator,
                            accumulatorContext,
                            operatorResources.codeGeneration(),
                            operatorResources.pooledLongHashSetPolicy()),
                    1);
            assertThat(state.values()).isNotNull();
            allocator.release(accumulatorContext);
        }
    }

    @Test
    void testHashJoinDiagnosticsAreOwnedByInjectedResources()
    {
        int[] materializations = new int[1];
        try (AllocationResources allocationResources = AllocationResources.createDefault();
                OperatorResources operatorResources = OperatorResources.createDefault(
                        (operatorName, outputIndex, streams, rowCount, nanos) -> materializations[0]++);
                Allocator allocator = new Allocator(allocationResources);
                Operator outer = new ConstantTableOperator(allocator, 1, List.of(Row.row(1L)));
                Operator inner = new ConstantTableOperator(allocator, 1, List.of(Row.row(1L)));
                Operator join = new HashJoinOperator(operatorResources, allocator, outer, 0, inner, 0)) {
            assertThat(join.hasNext()).isTrue();
            try (Batch batch = join.next()) {
                assertThat(batch.output(0).borrow(Stream.VALUES).length()).isEqualTo(1);
            }
            assertThat(materializations[0]).isEqualTo(1);
        }
    }
}
