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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.Streams;
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
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.data.Row.row;

public class TestOperatorBatches
{
    @Test
    void testConstantTableOperatorProducesBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L)));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testGeneratorOperatorProducesBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(10)));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(10L, 11L, 12L, 13L, 14L);
    }

    @Test
    void testProjectOperatorProducesProjectedBatch()
    {
        Allocator allocator = new Allocator();
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();

        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        new Variable(0),
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(new Variable(0), Stream.VALUES)));

        Operator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L), row(2L, 20L))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(11L, 22L);
    }

    @Test
    void testFilterOperatorProducesFilteredBatch()
    {
        Allocator allocator = new Allocator();
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable threshold = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(
                        new Assignment(threshold, new Literal(3L), AllMask.ALL),
                        new Assignment(
                                predicate,
                                new Call("lt", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(threshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        Operator operator = new FilterOperator(
                new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(0))),
                evaluationPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator);

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(batch.borrowMask().count()).isEqualTo(3);
    }

    @Test
    void testAggregationOperatorProducesAggregateBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountAll()),
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(6L);
        assertThat(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values()).containsExactly(3L);
    }

    @Test
    void testGroupedAggregationOperatorProducesGroupedAggregateBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
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

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(3L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testLimitOperatorProducesLimitedBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new LimitOperator(allocator, 3, new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(0))));

        Batch batch = operator.next();
        assertThat(batch.borrowMask().count()).isEqualTo(3);
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    void testGroupOperatorProducesGroupBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupOperator(
                allocator,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(10L), row(10L), row(20L))));

        Batch batch = operator.next();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(0L, 0L, 1L);
    }

    @Test
    void testTopNOperatorProducesTopNBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new TopNOperator(
                allocator,
                2,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(5L), row(3L))));

        Batch batch = operator.next();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(5L, 3L);
    }

    @Test
    void testTopNOperatorPreservesBinaryOutputColumn()
    {
        BinaryVector names = new BinaryVector(4, 19);
        names.addTrait(BinaryVector.Trait.UTF8_STRING);
        names.addTrait(BinaryVector.Trait.ASCII_ONLY);
        names.setBytes(0, "alpha".getBytes(UTF_8));
        names.setBytes(1, "beta".getBytes(UTF_8));
        names.setBytes(2, "gamma".getBytes(UTF_8));
        names.setBytes(3, "delta".getBytes(UTF_8));

        Operator operator = new TopNOperator(
                new Allocator(),
                2,
                0,
                new TableOperator(
                        2,
                        List.of(TableOperator.Page.values(
                                4,
                                new Vector[] {
                                        new I64Vector(new long[] {1L, 5L, 3L, 4L}),
                                        names,
                                },
                                org.weakref.nitro.data.Mask.all(4)))));

        Batch batch = operator.next();
        I64Vector ranks = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector resultNames = (BinaryVector) batch.output(1).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(ranks.values(), batch.borrowMask().count())).containsExactly(5L, 4L);
        assertThat(resultNames.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
        assertThat(resultNames.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
        assertThat(resultNames.utf8Value(0)).isEqualTo("beta");
        assertThat(resultNames.utf8Value(1)).isEqualTo("delta");
    }

    @Test
    void testNestedLoopJoinOperatorProducesJoinBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L))),
                new ConstantTableOperator(allocator, 1, List.of(row(10L), row(20L))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(1L, 2L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(10L, 10L);
    }

    @Test
    void testNestedLoopJoinOperatorPreservesBinaryPayloadColumn()
    {
        BinaryVector names = new BinaryVector(2, 9);
        names.addTrait(BinaryVector.Trait.UTF8_STRING);
        names.addTrait(BinaryVector.Trait.ASCII_ONLY);
        names.setBytes(0, "red".getBytes(UTF_8));
        names.setBytes(1, "blue".getBytes(UTF_8));

        Operator operator = new NestedLoopJoinOperator(
                new Allocator(),
                new TableOperator(
                        1,
                        List.of(TableOperator.Page.values(
                                2,
                                new Vector[] {new I64Vector(new long[] {1L, 2L})},
                                org.weakref.nitro.data.Mask.all(2)))),
                new TableOperator(
                        1,
                        List.of(TableOperator.Page.values(
                                2,
                                new Vector[] {names},
                                org.weakref.nitro.data.Mask.all(2)))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector ids = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector payload = (BinaryVector) batch.output(1).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(ids.values(), rowCount)).containsExactly(1L, 2L);
        assertThat(payload.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
        assertThat(payload.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
        assertThat(payload.utf8Value(0)).isEqualTo("red");
        assertThat(payload.utf8Value(1)).isEqualTo("red");
    }

    @Test
    void testTableOperatorProducesBatch()
    {
        Operator operator = new TableOperator(
                1,
                List.of(TableOperator.Page.values(
                        2,
                        new org.weakref.nitro.data.Vector[] {new I64Vector(new long[] {7L, 8L})},
                        org.weakref.nitro.data.Mask.all(2))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(7L, 8L);
    }

    @Test
    void testTableOperatorProducesExplicitStreams()
    {
        Operator operator = new TableOperator(
                1,
                List.of(new TableOperator.Page(
                        2,
                        new Streams[] {Streams.ofValuesAndNulls(
                                new I64Vector(new long[] {7L, 8L}),
                                new BooleanVector(new boolean[] {false, true}))},
                        org.weakref.nitro.data.Mask.all(2))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(7L, 8L);
        assertThat(((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values()).containsExactly(false, true);
    }
}
