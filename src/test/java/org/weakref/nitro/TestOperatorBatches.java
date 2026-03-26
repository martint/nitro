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
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.OffsetOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

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
    void testConstantTableOperatorProducesI32Batch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new ConstantTableOperator(allocator, 1, List.of(row(1), row(2), row(3)));

        Batch batch = operator.next();
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
        assertThat(((I32Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(1, 2, 3);
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
    void testAggregationOperatorAcceptsI32Input()
    {
        Allocator allocator = new Allocator();
        Operator operator = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountAll()),
                new ConstantTableOperator(allocator, 1, List.of(row(1), row(2), row(3))));

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
    void testGroupedAggregationOperatorCanFuseGroupingAndAggregation()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountAll()),
                new ConstantTableOperator(allocator, 2, List.of(
                        row(10L, 1L),
                        row(10L, 2L),
                        row(20L, 3L))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(10L, 20L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(3L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(2).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanExposeGroupingKeyBatch()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(
                                row("alpha"),
                                row("alpha"),
                                row("beta")))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(keys.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
        assertThat(keys.utf8Value(0)).isEqualTo("alpha");
        assertThat(keys.utf8Value(1)).isEqualTo("beta");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanFuseMultipleGroupingKeys()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                new ConstantTableOperator(allocator, 2, List.of(
                        row(10L, "alpha"),
                        row(10L, "alpha"),
                        row(10L, "beta"),
                        row(20L, "alpha"))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector leftKeys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector rightKeys = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(leftKeys.values(), rowCount)).containsExactly(10L, 10L, 20L);
        assertThat(rightKeys.utf8Value(0)).isEqualTo("alpha");
        assertThat(rightKeys.utf8Value(1)).isEqualTo("beta");
        assertThat(rightKeys.utf8Value(2)).isEqualTo("alpha");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanFuseThreeLongGroupingKeys()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new CountAll()),
                new ConstantTableOperator(allocator, 3, List.of(
                        row(10L, 100L, 1000L),
                        row(10L, 100L, 1000L),
                        row(10L, 100L, 2000L),
                        row(20L, 100L, 1000L))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector firstKeys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        I64Vector secondKeys = (I64Vector) batch.output(1).borrow(Stream.VALUES);
        I64Vector thirdKeys = (I64Vector) batch.output(2).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(3).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(firstKeys.values(), rowCount)).containsExactly(10L, 10L, 20L);
        assertThat(Arrays.copyOf(secondKeys.values(), rowCount)).containsExactly(100L, 100L, 100L);
        assertThat(Arrays.copyOf(thirdKeys.values(), rowCount)).containsExactly(1000L, 2000L, 1000L);
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorPreservesEmptyUtf8GroupingKeys()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(
                                row(""),
                                row(""),
                                row("alpha")))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
        I64Vector counts = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(2);
        assertThat(nulls.values()).containsExactly(false, false);
        assertThat(keys.utf8Value(0)).isEqualTo("");
        assertThat(keys.endOffset(0)).isZero();
        assertThat(keys.utf8Value(1)).isEqualTo("alpha");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanExposeMultipleGroupingKeys()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1, 2),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        new int[] {0, 1},
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(10L, "alpha"),
                                row(10L, "alpha"),
                                row(10L, "beta"),
                                row(20L, "alpha")))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector leftKeys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector rightKeys = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(leftKeys.values(), rowCount)).containsExactly(10L, 10L, 20L);
        assertThat(rightKeys.utf8Value(0)).isEqualTo("alpha");
        assertThat(rightKeys.utf8Value(1)).isEqualTo("beta");
        assertThat(rightKeys.utf8Value(2)).isEqualTo("alpha");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorMaterializesOnlyConstrainedBinaryKeys()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(
                                row("alpha"),
                                row("alpha"),
                                row("beta")))));

        Batch batch = operator.next();
        batch.constrain(Mask.sparse(new int[] {1}, 2));

        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);

        assertThat(nulls.values()).containsExactly(true, false);
        assertThat(keys.utf8Value(1)).isEqualTo("beta");
        assertThat(keys.endOffset(1)).isEqualTo("beta".length());
    }

    @Test
    void testGroupedAggregationOperatorSupportsRetainedBatchesForTopNPayloadDeferral()
    {
        Operator operator = new GroupedAggregationOperator(
                new Allocator(),
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        new Allocator(),
                        0,
                        new ConstantTableOperator(new Allocator(), 1, List.of(
                                row("alpha"),
                                row("alpha"),
                                row("beta")))));

        assertThat(operator.supportsRetainedBatches()).isTrue();
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
    void testTopNOperatorSupportsI32OrderingAndPayloadColumns()
    {
        Allocator allocator = new Allocator();
        Operator operator = new TopNOperator(
                allocator,
                2,
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1, 10),
                        row(5, 20),
                        row(3, 30))));

        Batch batch = operator.next();
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
        assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
        assertThat(Arrays.copyOf(((I32Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(5, 3);
        assertThat(Arrays.copyOf(((I32Vector) batch.output(1).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(20, 30);
    }

    @Test
    void testHashJoinOperatorSupportsI32EquiJoin()
    {
        Allocator allocator = new Allocator();
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1, 10),
                        row(2, 20),
                        row(3, 30))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2, 200),
                        row(3, 300),
                        row(4, 400))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
        assertThat(Arrays.copyOf(((I32Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2, 3);
        assertThat(Arrays.copyOf(((I32Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(20, 30);
        assertThat(Arrays.copyOf(((I32Vector) batch.output(3).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(200, 300);
    }

    @Test
    void testHashJoinOperatorSupportsI64EquiJoinWithDuplicateMatches()
    {
        Allocator allocator = new Allocator();
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(2L, 20L),
                        row(3L, 30L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2L, 200L),
                        row(2L, 201L),
                        row(3L, 300L),
                        row(4L, 400L))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 2L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(20L, 20L, 30L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(3).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(200L, 201L, 300L);
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
    void testTopNOperatorPreservesArrayPayloadColumn()
    {
        ArrayVector payload = new ArrayVector(4);
        payload.offsets()[0] = 0;
        payload.offsets()[1] = 2;
        payload.offsets()[2] = 2;
        payload.offsets()[3] = 3;
        payload.offsets()[4] = 6;
        payload.setElements(Streams.ofValues(new I64Vector(new long[] {10L, 11L, 20L, 30L, 31L, 32L})));

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
                                        payload,
                                },
                                org.weakref.nitro.data.Mask.all(4)))));

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(
                        row(5L, List.of()),
                        row(4L, List.of(30L, 31L, 32L)));
    }

    @Test
    void testTopNOperatorOrdersUtf8Keys()
    {
        Operator operator = new TopNOperator(
                new Allocator(),
                2,
                0,
                new ConstantTableOperator(
                        new Allocator(),
                        2,
                        List.of(
                                row("apple", 1L),
                                row("pear", 2L),
                                row("banana", 3L))));

        Batch batch = operator.next();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(keys.utf8Value(0)).isEqualTo("pear");
        assertThat(keys.utf8Value(1)).isEqualTo("banana");
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(2L, 3L);
    }

    @Test
    void testTopNOperatorOrdersDoubles()
    {
        Operator operator = new TopNOperator(
                new Allocator(),
                2,
                0,
                new ConstantTableOperator(
                        new Allocator(),
                        2,
                        List.of(
                                row(1.5, 10L),
                                row(3.25, 20L),
                                row(2.5, 30L))));

        Batch batch = operator.next();
        org.weakref.nitro.data.F64Vector values = (org.weakref.nitro.data.F64Vector) batch.output(0).borrow(Stream.VALUES);
        I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(values.values(), batch.borrowMask().count())).containsExactly(3.25, 2.5);
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(20L, 30L);
    }

    @Test
    void testTopNOperatorSupportsMultiKeyOrdering()
    {
        Operator operator = new TopNOperator(
                new Allocator(),
                3,
                new int[] {0, 1},
                new boolean[] {false, false},
                new ConstantTableOperator(
                        new Allocator(),
                        3,
                        List.of(
                                row(20L, "pear", 1L),
                                row(10L, "pear", 2L),
                                row(10L, "apple", 3L),
                                row(10L, "banana", 4L))));

        Batch batch = operator.next();
        I64Vector first = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector second = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector payload = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(first.values(), batch.borrowMask().count())).containsExactly(10L, 10L, 10L);
        assertThat(second.utf8Value(0)).isEqualTo("apple");
        assertThat(second.utf8Value(1)).isEqualTo("banana");
        assertThat(second.utf8Value(2)).isEqualTo("pear");
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(3L, 4L, 2L);
    }

    @Test
    void testTopNOperatorDefersPayloadBorrowUntilOutputIsRequested()
    {
        TrackingOperator source = new TrackingOperator(new TableOperator(
                2,
                List.of(TableOperator.Page.values(
                        4,
                        new Vector[] {
                                new I64Vector(new long[] {1L, 5L, 3L, 4L}),
                                new I64Vector(new long[] {10L, 20L, 30L, 40L}),
                        },
                        org.weakref.nitro.data.Mask.all(4)))));

        Operator operator = new TopNOperator(new Allocator(), 2, 0, source);

        Batch batch = operator.next();

        assertThat(source.borrowCount(0)).isGreaterThan(0);
        assertThat(source.borrowCount(1)).isZero();

        I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(20L, 40L);
        assertThat(source.borrowCount(1)).isGreaterThan(0);
    }

    @Test
    void testTopNOperatorMaterializesOnlyConstrainedPayloadRows()
    {
        BinaryVector payloads = new BinaryVector(3, 16);
        payloads.addTrait(BinaryVector.Trait.UTF8_STRING);
        payloads.addTrait(BinaryVector.Trait.ASCII_ONLY);
        payloads.setBytes(0, "ccc".getBytes(UTF_8));
        payloads.setBytes(1, "bb".getBytes(UTF_8));
        payloads.setBytes(2, "a".getBytes(UTF_8));

        Operator operator = new TopNOperator(
                new Allocator(),
                3,
                0,
                new TableOperator(
                        2,
                        List.of(TableOperator.Page.values(
                                3,
                                new Vector[] {
                                        new I64Vector(new long[] {3L, 2L, 1L}),
                                        payloads,
                                },
                                org.weakref.nitro.data.Mask.all(3)))));

        Batch batch = operator.next();
        batch.constrain(Mask.sparse(new int[] {2}, 3));

        BinaryVector payload = (BinaryVector) batch.output(1).borrow(Stream.VALUES);

        assertThat(payload.length()).isEqualTo(3);
        assertThat(payload.length(0)).isZero();
        assertThat(payload.length(1)).isZero();
        assertThat(payload.utf8Value(2)).isEqualTo("a");
    }

    @Test
    void testOffsetOperatorReusesPrefetchedTopNBatch()
    {
        CountingNextOperator source = new CountingNextOperator(new TopNOperator(
                new Allocator(),
                3,
                0,
                new ConstantTableOperator(
                        new Allocator(),
                        1,
                        List.of(
                                row(5L),
                                row(4L),
                                row(3L),
                                row(2L)))));

        Operator operator = new OffsetOperator(new Allocator(), 1, source);

        assertThat(operator.hasNext()).isTrue();
        Batch batch = operator.next();
        Mask mask = batch.borrowMask();
        I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);

        assertThat(source.nextCount()).isEqualTo(1);
        assertThat(mask.selectedCount()).isEqualTo(2);
        assertThat(mask.position(0)).isEqualTo(1);
        assertThat(mask.position(1)).isEqualTo(2);
        assertThat(values.values()[mask.position(0)]).isEqualTo(4L);
        assertThat(values.values()[mask.position(1)]).isEqualTo(3L);
    }

    @Test
    void testOperatorAssertionsToRowsClosesOffsetBatches()
    {
        Operator operator = new OffsetOperator(
                new Allocator(),
                1,
                new TopNOperator(
                        new Allocator(),
                        3,
                        0,
                        new ConstantTableOperator(
                                new Allocator(),
                                1,
                                List.of(
                                        row(5L),
                                        row(4L),
                                        row(3L),
                                        row(2L)))));

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(row(4L), row(3L));
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
    void testNestedLoopJoinOperatorSupportsI64EquiJoin()
    {
        Allocator allocator = new Allocator();
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(2L, 20L),
                        row(3L, 30L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2L, 200L),
                        row(3L, 300L),
                        row(4L, 400L))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(20L, 30L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(2).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(3).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(200L, 300L);
    }

    @Test
    void testNestedLoopJoinOperatorSupportsUtf8EquiJoin()
    {
        Allocator allocator = new Allocator();
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("alpha", 10L),
                        row("beta", 20L),
                        row((Object) null, 30L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("beta", 200L),
                        row("alpha", 100L),
                        row((Object) null, 300L))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        I64Vector leftPayload = (I64Vector) batch.output(1).borrow(Stream.VALUES);
        I64Vector rightPayload = (I64Vector) batch.output(3).borrow(Stream.VALUES);

        assertThat(keys.utf8Value(0)).isEqualTo("alpha");
        assertThat(keys.utf8Value(1)).isEqualTo("beta");
        assertThat(Arrays.copyOf(leftPayload.values(), rowCount)).containsExactly(10L, 20L);
        assertThat(Arrays.copyOf(rightPayload.values(), rowCount)).containsExactly(100L, 200L);
    }

    @Test
    void testNestedLoopJoinOperatorSupportsMultiKeyEquiJoin()
    {
        Allocator allocator = new Allocator();
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "alpha", 10L),
                        row(1L, "beta", 20L),
                        row(2L, "alpha", 30L))),
                new int[] {0, 1},
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "beta", 200L),
                        row(1L, "alpha", 100L),
                        row(2L, "beta", 300L))),
                new int[] {0, 1});

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector leftIds = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector leftNames = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector leftPayload = (I64Vector) batch.output(2).borrow(Stream.VALUES);
        I64Vector rightPayload = (I64Vector) batch.output(5).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(leftIds.values(), rowCount)).containsExactly(1L, 1L);
        assertThat(leftNames.utf8Value(0)).isEqualTo("alpha");
        assertThat(leftNames.utf8Value(1)).isEqualTo("beta");
        assertThat(Arrays.copyOf(leftPayload.values(), rowCount)).containsExactly(10L, 20L);
        assertThat(Arrays.copyOf(rightPayload.values(), rowCount)).containsExactly(100L, 200L);
    }

    @Test
    void testHashJoinOperatorSupportsMultiKeyEquiJoin()
    {
        Allocator allocator = new Allocator();
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "alpha", 10L),
                        row(1L, "beta", 20L),
                        row(2L, "alpha", 30L))),
                new int[] {0, 1},
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "beta", 200L),
                        row(1L, "alpha", 100L),
                        row(2L, "beta", 300L))),
                new int[] {0, 1});

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector leftIds = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector leftNames = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector leftPayload = (I64Vector) batch.output(2).borrow(Stream.VALUES);
        I64Vector rightPayload = (I64Vector) batch.output(5).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(leftIds.values(), rowCount)).containsExactly(1L, 1L);
        assertThat(leftNames.utf8Value(0)).isEqualTo("alpha");
        assertThat(leftNames.utf8Value(1)).isEqualTo("beta");
        assertThat(Arrays.copyOf(leftPayload.values(), rowCount)).containsExactly(10L, 20L);
        assertThat(Arrays.copyOf(rightPayload.values(), rowCount)).containsExactly(100L, 200L);
    }

    @Test
    void testHashJoinOperatorPreservesArrayPayloadColumn()
    {
        ArrayVector payload = new ArrayVector(3);
        payload.offsets()[0] = 0;
        payload.offsets()[1] = 2;
        payload.offsets()[2] = 3;
        payload.offsets()[3] = 3;
        payload.setElements(Streams.ofValues(new I64Vector(new long[] {100L, 101L, 200L})));

        Operator operator = new HashJoinOperator(
                new Allocator(),
                new TableOperator(
                        1,
                        List.of(TableOperator.Page.values(
                                2,
                                new Vector[] {new I64Vector(new long[] {1L, 2L})},
                                org.weakref.nitro.data.Mask.all(2)))),
                0,
                new TableOperator(
                        2,
                        List.of(TableOperator.Page.values(
                                3,
                                new Vector[] {
                                        new I64Vector(new long[] {2L, 1L, 3L}),
                                        payload,
                                },
                                org.weakref.nitro.data.Mask.all(3)))),
                0);

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(
                        row(1L, 1L, List.of(200L)),
                        row(2L, 2L, List.of(100L, 101L)));
    }

    @Test
    void testHashJoinOperatorSupportsMultipleOuterPages()
    {
        BinaryVector firstPageKeys = new BinaryVector(2, 4);
        firstPageKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        firstPageKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
        firstPageKeys.setBytes(0, "aa".getBytes(UTF_8));
        firstPageKeys.setBytes(1, "bb".getBytes(UTF_8));

        BinaryVector secondPageKeys = new BinaryVector(2, 4);
        secondPageKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        secondPageKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
        secondPageKeys.setBytes(0, "aa".getBytes(UTF_8));
        secondPageKeys.setBytes(1, "bb".getBytes(UTF_8));

        Allocator allocator = new Allocator();
        Operator operator = new HashJoinOperator(
                allocator,
                new TableOperator(
                        2,
                        List.of(
                                TableOperator.Page.values(
                                        2,
                                        new Vector[] {
                                                firstPageKeys,
                                                new I64Vector(new long[] {10L, 20L}),
                                        },
                                        org.weakref.nitro.data.Mask.all(2)),
                                TableOperator.Page.values(
                                        2,
                                        new Vector[] {
                                                secondPageKeys,
                                                new I64Vector(new long[] {30L, 40L}),
                                        },
                                        org.weakref.nitro.data.Mask.all(2)))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("aa", 100L),
                        row("bb", 200L))),
                0);

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(
                        row("aa", 10L, "aa", 100L),
                        row("bb", 20L, "bb", 200L),
                        row("aa", 30L, "aa", 100L),
                        row("bb", 40L, "bb", 200L));
    }

    @Test
    void testHashJoinOperatorPreservesLazyNonRetainedOuterPayloadAcrossOutputBatches()
    {
        Allocator allocator = new Allocator();
        List<org.weakref.nitro.data.Row> innerRows = new ArrayList<>();
        for (int index = 0; index < 2_500; index++) {
            innerRows.add(row(1L, 1000L + index));
        }
        for (int index = 0; index < 2_500; index++) {
            innerRows.add(row(2L, 2000L + index));
        }
        innerRows.add(row(3L, 3000L));

        try (Operator join = new HashJoinOperator(
                allocator,
                new LazyNonRetainedOuterOperator(
                        new long[] {1L, 2L, 3L},
                        new long[] {10L, 20L, 30L}),
                0,
                new ConstantTableOperator(allocator, 2, innerRows),
                0)) {
            try (Batch first = join.next()) {
                assertThat(first.borrowMask().count()).isEqualTo(4_096);
                assertThat(((I64Vector) first.output(1).borrow(Stream.VALUES)).values()[4_095]).isEqualTo(20L);
            }

            try (Batch second = join.next()) {
                assertThat(second.borrowMask().count()).isEqualTo(905);
                I64Vector outerPayloads = (I64Vector) second.output(1).borrow(Stream.VALUES);
                I64Vector innerPayloads = (I64Vector) second.output(3).borrow(Stream.VALUES);

                for (int index = 0; index < 904; index++) {
                    assertThat(outerPayloads.values()[index]).isEqualTo(20L);
                }
                assertThat(outerPayloads.values()[904]).isEqualTo(30L);
                assertThat(innerPayloads.values()[904]).isEqualTo(3000L);
            }
        }
    }

    @Test
    void testHashJoinOperatorPreservesObservedSchemaOnEmptyResult()
    {
        Allocator allocator = new Allocator();
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"))),
                0,
                new ConstantTableOperator(allocator, 1, List.of()),
                0);

        Batch batch = operator.next();
        assertThat(batch.borrowMask().count()).isZero();
        assertThat(batch.output(0).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(1).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
        assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
    }

    @Test
    void testNestedLoopJoinOperatorPreservesObservedSchemaOnEmptyResult()
    {
        Allocator allocator = new Allocator();
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"))),
                0,
                new ConstantTableOperator(allocator, 1, List.of()),
                0);

        Batch batch = operator.next();
        assertThat(batch.borrowMask().count()).isZero();
        assertThat(batch.output(0).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(1).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
        assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
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

    private static final class TrackingOperator
            implements Operator
    {
        private final Operator delegate;
        private final int[] borrowCounts;

        private TrackingOperator(Operator delegate)
        {
            this.delegate = delegate;
            this.borrowCounts = new int[delegate.outputCount()];
        }

        @Override
        public int outputCount()
        {
            return delegate.outputCount();
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public Batch next()
        {
            Batch batch = delegate.next();
            Output[] outputs = new Output[delegate.outputCount()];
            for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
                int trackedOutput = outputIndex;
                Output output = batch.output(outputIndex);
                outputs[outputIndex] = new Output(
                        output.streams(),
                        stream -> {
                            borrowCounts[trackedOutput]++;
                            return output.borrow(stream);
                        },
                        (stream, vector) -> output.take(stream));
            }
            return new Batch(batch.borrowMask(), Function.identity(), outputs);
        }

        @Override
        public void constrain(org.weakref.nitro.data.Mask mask)
        {
            delegate.constrain(mask);
        }

        @Override
        public boolean supportsRetainedBatches()
        {
            return delegate.supportsRetainedBatches();
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        private int borrowCount(int output)
        {
            return borrowCounts[output];
        }
    }

    private static final class CountingNextOperator
            implements Operator
    {
        private final Operator delegate;
        private int nextCount;

        private CountingNextOperator(Operator delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public int outputCount()
        {
            return delegate.outputCount();
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public Batch next()
        {
            nextCount++;
            return delegate.next();
        }

        @Override
        public void constrain(Mask mask)
        {
            delegate.constrain(mask);
        }

        @Override
        public boolean supportsRetainedBatches()
        {
            return delegate.supportsRetainedBatches();
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        private int nextCount()
        {
            return nextCount;
        }
    }

    private static final class LazyNonRetainedOuterOperator
            implements Operator
    {
        private final I64Vector keys;
        private final long[] payloadValues;
        private Mask currentMask;
        private boolean emitted;

        private LazyNonRetainedOuterOperator(long[] keys, long[] payloadValues)
        {
            this.keys = new I64Vector(keys);
            this.payloadValues = Arrays.copyOf(payloadValues, payloadValues.length);
            this.currentMask = Mask.all(keys.length);
        }

        @Override
        public int outputCount()
        {
            return 2;
        }

        @Override
        public boolean hasNext()
        {
            return !emitted;
        }

        @Override
        public Batch next()
        {
            emitted = true;
            return new Batch(
                    currentMask,
                    this::constrain,
                    Function.identity(),
                    new Output(Set.of(Stream.VALUES), ignored -> keys),
                    new Output(Set.of(Stream.VALUES), ignored -> lazyPayload()));
        }

        @Override
        public void constrain(Mask mask)
        {
            currentMask = mask;
        }

        @Override
        public void close()
        {
        }

        private I64Vector lazyPayload()
        {
            I64Vector payload = new I64Vector(payloadValues.length);
            if (currentMask.all()) {
                System.arraycopy(payloadValues, 0, payload.values(), 0, payloadValues.length);
                return payload;
            }
            for (int index = 0; index < currentMask.count(); index++) {
                int position = currentMask.position(index);
                payload.values()[position] = payloadValues[position];
            }
            return payload;
        }
    }
}
