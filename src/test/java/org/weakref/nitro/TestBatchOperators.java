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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddBigint;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.BatchOperator;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.data.Row.row;

public class TestBatchOperators
{
    @Test
    void testConstantTableOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L)));

        Batch batch = operator.nextBatch();
        assertThat(((I64VectorWithNulls) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testGeneratorOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(10)));

        Batch batch = operator.nextBatch();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(10L, 11L, 12L, 13L, 14L);
    }

    @Test
    void testProjectOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(scalarRegistry.register(AddBigint.class));

        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        new org.weakref.nitro.operator.evaluator.ir.Variable(0),
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(new org.weakref.nitro.operator.evaluator.ir.Variable(0), Stream.VALUES)));

        BatchOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L), row(2L, 20L))));

        Batch batch = operator.nextBatch();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(11L, 22L);
    }

    @Test
    void testFilterOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new FilterOperator(
                new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(0))),
                List.of(new org.weakref.nitro.operator.evaluator.functions.InputReference(0), new org.weakref.nitro.operator.evaluator.functions.I64Predicate(0, value -> value < 3)),
                1,
                allocator);

        Batch batch = operator.nextBatch();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(batch.borrowMask().count()).isEqualTo(3);
    }

    @Test
    void testAggregationOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountAll()),
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))));

        Batch batch = operator.nextBatch();
        assertThat(((I64VectorWithNulls) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(6L);
        assertThat(((I64VectorWithNulls) batch.output(1).borrow(Stream.VALUES)).values()).containsExactly(3L);
    }

    @Test
    void testGroupedAggregationOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(new Sum(2), new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(10L, 1L),
                                row(10L, 2L),
                                row(20L, 3L)))));

        Batch batch = operator.nextBatch();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64VectorWithNulls) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(3L, 3L);
        assertThat(Arrays.copyOf(((I64VectorWithNulls) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testLimitOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new LimitOperator(3, new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(0))));

        Batch batch = operator.nextBatch();
        assertThat(batch.borrowMask().count()).isEqualTo(3);
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    void testGroupOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new GroupOperator(
                allocator,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(10L), row(10L), row(20L))));

        Batch batch = operator.nextBatch();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(0L, 0L, 1L);
    }

    @Test
    void testTopNOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new TopNOperator(
                allocator,
                2,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(5L), row(3L))));

        Batch batch = operator.nextBatch();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(5L, 3L);
    }

    @Test
    void testNestedLoopJoinOperatorExposesBatchApi()
    {
        Allocator allocator = new Allocator();
        BatchOperator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L))),
                new ConstantTableOperator(allocator, 1, List.of(row(10L), row(20L))));

        Batch batch = operator.nextBatch();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64VectorWithNulls) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(1L, 2L);
        assertThat(Arrays.copyOf(((I64VectorWithNulls) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(10L, 10L);
    }

    @Test
    void testTableOperatorExposesBatchApi()
    {
        BatchOperator operator = new TableOperator(
                1,
                List.of(new TableOperator.Page(
                        2,
                        new org.weakref.nitro.data.Vector[] {new I64Vector(new long[] {7L, 8L})},
                        org.weakref.nitro.data.Mask.all(2))));

        Batch batch = operator.nextBatch();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(7L, 8L);
    }
}
